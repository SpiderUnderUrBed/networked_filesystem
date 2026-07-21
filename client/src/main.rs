use std::{fs::File as StdFile, io::{BufReader, Read}, path::Path, sync::{Arc, atomic::AtomicBool}};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use input_macro::input;
use networked_filesystem::{
    Codec, Direction, File, LocalState, Operation, RemoteFileSystem, TcpFsSender,
};
use std::sync::atomic::Ordering;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tokio::sync::broadcast;
struct AppState {
    filesystem: Arc<RwLock<RemoteFileSystem<TcpFsSender>>>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("Welcome to the networked filesystem POC!");
    let (tx, mut rx) = broadcast::channel::<Vec<u8>>(32);

    let (tx_clone, mut rx_clone) = (tx.clone(), rx.resubscribe());
    tokio::spawn(async move {
        //let listener = TcpListener::bind("127.0.0.1:80")?;
        let mut stream = TcpStream::connect("127.0.0.1:8011").await.unwrap();
        println!("Connected to stream at 127.0.0.1:8011");
        let (mut read_half, mut write_half) = stream.into_split();
        let mut temp_buf = [0u8; 4096];
        let mut accum: Vec<u8> = Vec::new();
        loop {
            tokio::select! {
                Ok(out) = rx_clone.recv() => {
                    if let Err(e) = write_half.write_all(&out).await {
                        eprintln!("write error: {e}");
                        break;
                    }
                }
                result = read_half.read(&mut temp_buf) => {
                    match result {
                        Ok(0) => {
                            println!("disconnected");
                            break;
                        }
                        Ok(n) => {
                            let data = &temp_buf[..n];
                           // println!("read {} bytes: {:?}", n, data);

                        }
                        Err(e) => {
                            eprintln!("read error: {e}");
                            break;
                        }
                    }
                }

            }
        }
    });

    let mut tcp_fs = TcpFsSender::new(rx.resubscribe(), tx.clone());
    tcp_fs.set_start_delimiter(r"\\f".as_bytes().to_vec());
    tcp_fs.set_end_delimiter("//f".as_bytes().to_vec());
    let mut filesystem = RemoteFileSystem::<TcpFsSender>::new(tcp_fs);
    filesystem.set_direction(Direction::Local);
    filesystem.set_codec(Codec::RawContinues);
    // filesystem.create_state("main".to_owned(), LocalState { location: "/".to_string() });
    let state = Arc::new(RwLock::new(AppState {
        filesystem: Arc::new(RwLock::new(filesystem)),
    }));

    loop {
        let operation: String = input!("Enter an operation: ").parse().unwrap();
        let (tx, rx) = broadcast::channel::<Vec<u8>>(32);
        let arc_state = state.clone();
        if operation.starts_with("send") {
            {
                println!("sending file");
                let inner_state = arc_state.write().await;

                let mut filesystem = inner_state.filesystem.write().await;
                filesystem.create_state(
                    "main".to_string(),
                    LocalState {
                        // TODO: set back to /
                        location: "/"
                            .to_string(),
                    },
                );

                let _ = filesystem.set_operation(Operation::Move);

                filesystem.clear_files();
                let file = File {
                    original_location: None,
                    final_location:
                        "/home/spiderunderurbed/projects/tcp_fs_poc/test-output.txt".to_owned(),
                    content_stream: Some(rx.resubscribe()),
                };
                filesystem.append_files(file);
            }
            let arc_state_clone = arc_state.clone();
            let handle = tokio::spawn(async move {
                let state = arc_state_clone.write().await;
                let mut filesystem = state.filesystem.write().await;
                let _ = filesystem.execute_operation("main".to_string()).await;
            });

            let file_path = Path::new("/home/spiderunderurbed/projects/tcp_fs_poc/test.txt");
            // let file = StdFile::open(file_path)?;
            // let reader = BufReader::new(file);

            // for byte_result in reader.bytes() {
            //     match byte_result {
            //         Ok(byte) => {
            //             tx.send(vec![byte])?;
            //         },
            //         Err(e) => {
            //             eprintln!("\nError reading byte: {}", e);
            //             return Err(e.into());
            //         }
            //     }
            // }

            let file = StdFile::open(file_path)?;
            let mut reader = BufReader::new(file);
            let mut chunk = vec![0u8; 1000];

            loop {
                let n = reader.read(&mut chunk)?;
                if n == 0 {
                    break;
                }
                tx.send(chunk[..n].to_vec())?;
            }

            // let payload: String = (0..=10000).map(|i| format!("{} ", i)).collect();
            // let bytes = payload.as_bytes();
            // tx.send(bytes.to_vec())?;
            println!("going to drop tx");
            drop(tx);
            if let Err(e) = handle.await {
                eprintln!("task panicked: {e:?}");
            }
            //break;
        } else if operation.starts_with("ls") {
            {
                println!("sending dir");
                let inner_state = arc_state.write().await;

                let mut filesystem = inner_state.filesystem.write().await;
                filesystem.create_state(
                    "main".to_string(),
                    LocalState {
                        // TODO: set back to /
                        location: "/home/spiderunderurbed/projects/tcp_fs_poc/".to_string(),
                    },
                );

                let _ = filesystem.set_operation(Operation::Ls);
            }
            let arc_state_clone = arc_state.clone();
            let handle = tokio::spawn(async move {
                let state = arc_state_clone.write().await;
                let mut filesystem = state.filesystem.write().await;
                let _ = filesystem.execute_operation("main".to_string()).await;
            });
            drop(tx);
            //tx_option = None;
            if let Err(e) = handle.await {
                eprintln!("task panicked: {e:?}");
            }
        }
    }
    Ok(())
}
