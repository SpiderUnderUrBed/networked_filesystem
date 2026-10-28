use networked_filesystem::length_delimited_v2::length_delimited::{MspcFile, TcpFsBidirectional};
use networked_filesystem::{Direction, LocalState, RemoteFileSystem, StreamSender};

use std::sync::{Arc};
use tokio::sync::{RwLock, mpsc};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};


struct AppState {
    filesystem: Arc<RwLock<RemoteFileSystem<TcpFsBidirectional, MspcFile>>>,
}
#[tokio::main]
async fn main() -> std::io::Result<()> {
    println!("Hello, world!");
    let listener = TcpListener::bind("127.0.0.1:8011").await?;
    loop {
        let (stream, addr) = listener.accept().await?;
        let (fs_tx, fs_rx) = mpsc::channel(32);
        let mut receiver = TcpFsBidirectional::new(fs_tx.clone(), fs_rx);
        let mut filesystem = RemoteFileSystem::<TcpFsBidirectional, MspcFile>::new(receiver);
        filesystem.set_direction(Direction::Server);
        filesystem.create_state(
            0,
            LocalState {
                location:"/".to_string(), 
                starting_cursor_position: None, 
                ending_cursor_position: None 
            },
        );
        let arc_filesystem = Arc::new(RwLock::new(filesystem));
        let state = AppState {
            filesystem: arc_filesystem.clone(),
        };
        let (reply_tx, mut reply_rx) = flume::bounded::<Vec<u8>>(32);

        tokio::spawn(async move {
            let (mut read_half, mut write_half) = stream.into_split();
            let mut temp_buf = [0u8; 4096];
            let mut accum: Vec<u8> = Vec::new();

            let mut filesystem_for_read_task = arc_filesystem.write().await;
            loop {
                tokio::select! {
                    Ok(out) = reply_rx.recv_async() => {
                        println!("writing back");
                        if let Err(e) = write_half.write_all(&out).await {
                            eprintln!("write error: {e}");
                            break;
                        }
                    },
                    result = read_half.read(&mut temp_buf) => {
                        match result {
                            Ok(0) => {
                                println!("{} disconnected", addr);
                            break;
                            }
                            Ok(n) => {
                                let data = &temp_buf[..n];
                                // println!("read {} bytes: {:?}", n, data);

                                let _ = filesystem_for_read_task.inner_mut().send(data.to_vec()).await;
                            }
                            Err(e) => {
                                eprintln!("read error: {e}");
                                break;
                            }
                        }
                    },
                    _ = filesystem_for_read_task.receive_operation(0, None) => {
                        println!("finished receiving");
                    }
                }
            }
        });
    }

    Ok(())
}