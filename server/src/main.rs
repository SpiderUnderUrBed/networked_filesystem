use networked_filesystem::{Direction, LocalState, RemoteFileSystem, TcpFsReceiver};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::{Mutex, broadcast},
};

#[derive(Deserialize, Serialize)]
struct PingRequest {}

struct AppState {
    filesystem: Arc<RwLock<RemoteFileSystem<TcpFsReceiver>>>,
}
#[tokio::main]
async fn main() -> std::io::Result<()> {
    println!("Hello, world!");
    let listener = TcpListener::bind("127.0.0.1:8011").await?;
    loop {
        let (stream, addr) = listener.accept().await?;
        let (fs_tx, fs_rx) = flume::bounded(32);
        let mut receiver = TcpFsReceiver::new(fs_tx.clone(), fs_rx);
        receiver.set_start_delimiter(r"\\\\f".as_bytes().to_vec());
        receiver.set_end_delimiter("////f".as_bytes().to_vec());
        let mut filesystem = RemoteFileSystem::<TcpFsReceiver>::new(receiver);
        filesystem.set_direction(Direction::Server);
        filesystem.create_state(
            "main".to_owned(),
            LocalState {
                location: "/".to_string(),
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
                    }
                    result = read_half.read(&mut temp_buf) => {
                        match result {
                            Ok(0) => {
                                println!("{} disconnected", addr);
                                break;
                            }
                            Ok(n) => {
                                let data = &temp_buf[..n];
                                println!("read {} bytes: {:?}", n, data);

                                filesystem_for_read_task.inner_mut().send(data.to_vec());
                            }
                            Err(e) => {
                                eprintln!("read error: {e}");
                                break;
                            }
                        }
                    }
                    _ = filesystem_for_read_task.receive_operation("main".to_string()) => {
                        println!("finished receiving");
                    }
                }
            }
        });
    }

    Ok(())
}