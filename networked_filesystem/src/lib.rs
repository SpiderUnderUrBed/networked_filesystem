use std::{
    collections::{HashMap, VecDeque}, default, io::Write, marker::PhantomData, os::unix::ffi::OsStrExt, pin::Pin, sync::{
        Arc, RwLock,
        atomic::{AtomicBool, Ordering},
        mpsc::Receiver,
    }, task::{Context, Poll}, vec,
};

use multer::bytes::{self, buf};
use multipeek::{IteratorExt, MultiPeek};
use num_enum::{IntoPrimitive, TryFromPrimitive};
use tokio::sync::{Mutex, Notify, broadcast};
use tokio::{io::AsyncWriteExt, sync::watch};

use crate::subsequence::{SubsequenceStatus, find_subsequence_by_windows_iter};
//use tokio_util::sync::CancellationToken;

mod subsequence;

#[derive(Clone, TryFromPrimitive)]
#[repr(u8)]
pub enum Direction {
    Local = 0,
    Server = 1,
    Unknown = 2,
}
// impl Direction {
//     fn to_byte_header(&self) -> Option<u8> {
//         match self {
//             Direction::Local => Some(0),
//             Direction::Server => Some(1),
//             Direction::Unknown => None,
//         }
//     }
//     fn from_byte_header(byte: u8) -> Option<Direction> {
//         match byte {
//             0 => Some(Direction::Local),
//             1 => Some(Direction::Server),
//             _ => None,
//         }
//     }
// }

#[derive(Clone, TryFromPrimitive)]
#[repr(u8)]
pub enum Operation {
    Move = 0,
    Set = 1,
    Ls = 3,
    // LsWithRange { start: u64, end: u64 },
    None = 4,
}
// impl Operation {
//     fn to_byte_header(&self) -> Option<u8> {
//         match self {
//             Operation::Move => Some(1),
//             Operation::None => None,
//             Operation::Set => Some(2),
//             Operation::Ls => Some(3),
//         }
//     }
//     fn from_byte_header(byte: u8) -> Option<Operation> {
//         match byte {
//             1 => Some(Operation::Move),
//             2 => Some(Operation::Set),
//             3 => Some(Operation::Ls),
//             _ => None,
//         }
//     }
// }

#[derive(Clone, TryFromPrimitive)]
#[repr(u8)]
pub enum Codec {
    Raw = 0,
    RawContinues = 1,
    Multipart = 2,
    Unknown = 4,
}
// impl Codec {
//     fn to_byte_header(&self) -> Option<u8> {
//         match self {
//             Codec::Raw => Some(0),
//             Codec::Multipart => Some(1),
//             Codec::RawContinues => Some(2),
//             Codec::Unknown => None,
//         }
//     }
//     fn from_byte_header(byte: u8) -> Option<Codec> {
//         match byte {
//             0 => Some(Codec::Raw),
//             1 => Some(Codec::Multipart),
//             2 => Some(Codec::RawContinues),
//             _ => None,
//         }
//     }
// }
#[derive(Clone, Debug, TryFromPrimitive)]
#[repr(u8)]
pub enum ChunkingStatus {
    Continues = 0,
    End = 1,
}
// impl ChunkingStatus {
//     fn to_byte_header(&self) -> u8 {
//         match self {
//             ChunkingStatus::Continues => 0,
//             ChunkingStatus::End => 1,
//         }
//     }
//     fn from_byte_header(byte: u8) -> Option<ChunkingStatus> {
//         match byte {
//             0 => Some(ChunkingStatus::Continues),
//             1 => Some(ChunkingStatus::End),
//             _ => None,
//         }
//     }
// }

#[derive(Default, Clone)]
struct FileFrame {
    escape_byte: Option<u8>,
    starting_delimiter: Option<Vec<u8>>,
    ending_delimiter: Option<Vec<u8>>,
    // starting_position: Option<u8>,
    direction: Option<Direction>,
    operation: Option<Operation>,
    codec: Option<Codec>,
    chunking_status: Option<ChunkingStatus>,
    // file_chunks: [u8; 4076],
    file_chunks: Vec<u8>,
    remainder: Vec<u8>,
    collect_buffer: bool,
    complete_frame: bool,
}
enum FileFrameStatus {
    NotValidFrame,
    Pending,
    Unknown,
}
static STUFF_BYTES: bool = true;
impl FileFrame {
    fn new(
        starting_delimiter: Option<Vec<u8>>,
        ending_delimiter: Option<Vec<u8>>,
        direction: Direction,
        operation: Operation,
        codec: Codec,
        chunking_status: ChunkingStatus,
        escape_byte: Option<u8>
    ) -> Self {
        FileFrame {
            starting_delimiter,
            ending_delimiter,
            direction: Some(direction),
            operation: Some(operation),
            codec: Some(codec),
            chunking_status: Some(chunking_status),
            file_chunks: Vec::new(),
            remainder: Vec::new(),
            collect_buffer: true,
            complete_frame: false,
            escape_byte,
        }
    }
    fn append_bytes_send(&mut self, bytes: Vec<u8>) {
        self.file_chunks.extend(bytes);
    }
    fn flush(&mut self) {
        self.file_chunks = Vec::new();
    }
    fn to_bytes(&self) -> Result<Vec<u8>, FileFrameStatus> {
        let mut bytes_frame = Vec::new();
        if let Some(ref starting_delimiter) = self.starting_delimiter {
            bytes_frame.extend(starting_delimiter);
        }
        if let Some(ref direction) = self.direction {
            bytes_frame.push(direction.clone() as u8);
        } else {
            return Err(FileFrameStatus::NotValidFrame);
        }
        if let Some(ref operation) = self.operation {
            bytes_frame.push(operation.clone() as u8);
        } else {
            return Err(FileFrameStatus::NotValidFrame);
        }
        if let Some(ref codec) = self.codec {
            bytes_frame.push(codec.clone() as u8);
        } else {
            return Err(FileFrameStatus::NotValidFrame);
        }
        if let Some(ref chunking_status) = self.chunking_status {
            bytes_frame.push(chunking_status.clone() as u8);
        } else {
            return Err(FileFrameStatus::NotValidFrame);
        }
        //if STUFF_BYTES {
            if let (Some(ref ending_delims), Some(ref starting_delims), Some(escape_byte)) = (self.ending_delimiter.clone(), self.starting_delimiter.clone(), self.escape_byte) {
                let starting_byte_to_escape = starting_delims.get(0).unwrap();
                let ending_byte_to_escape = ending_delims.get(0).unwrap();
                // let mut current_byte_iter = self.file_chunks.iter();
                //while let Some(byte) = current_byte_iter.next()  {
                for (_, byte) in self.file_chunks.iter().enumerate() {
                    if *byte == *starting_byte_to_escape || *byte == *ending_byte_to_escape {
                        bytes_frame.push(escape_byte);
                    }
                    bytes_frame.push(*byte);
                }
            } else {
                bytes_frame.extend(self.file_chunks.clone());
            }
        // } else {
        //     // bytes_frame.extend(self.file_chunks.clone());
        // }
        if let Some(ref ending_delimiter) = self.ending_delimiter {
            println!("adding ending delimiter");
            //println!("Adding the end delimiter");
            bytes_frame.extend(ending_delimiter);
        }
        Ok(bytes_frame)
    }
    //&mut
    //previous_bytes: &mut Vec<u8>, 
    fn recursively_handle_bytes(
        &mut self,
        bytes: Vec<u8>,
    ) -> Result<VecDeque<Self>, FileFrameStatus> {
        let mut subframes: VecDeque<Self> = VecDeque::new();
        let mut total_bytes = self.remainder.clone();
        total_bytes.extend(bytes.clone());

        if self.collect_buffer {
            self.collect_buffer = false;
            //if !self.complete_frame {
            if let Some(end_delimiter) = self.ending_delimiter.clone() {
                let mut bytes_iter = self.file_chunks.clone().into_iter().multipeek();
                let mut end_pos = 0;
                let delimiter_iter = end_delimiter.iter();
                // TODO: see if I could for the subsequence matching 
                // handle partial matches at the end of the iterator, signify that and start a remainder
                // waiting for the next amount of bytes from a read 
                'end_delims: loop {
                    // let mut matches = true;
                    match find_subsequence_by_windows_iter(delimiter_iter.clone(), bytes_iter.clone()){
                        SubsequenceStatus::Pending => {
                            
                        },
                        SubsequenceStatus::NotMatched => {
                            return Err(FileFrameStatus::NotValidFrame);
                        },
                        SubsequenceStatus::Continue => {
                            bytes_iter.next();
                            end_pos += 1;
                            continue 'end_delims;
                        },
                        SubsequenceStatus::FoundAt(pos) => end_pos = pos,
                    }
                    println!("self remainder: {:?}", self.remainder);
                    self.remainder = self.file_chunks[end_pos..self.file_chunks.len()].to_vec();
                    println!("new remainder: {:?}", self.remainder);
                    self.file_chunks = self.file_chunks[0..end_pos].to_vec();
                    println!("own chunks: {:?}", self.file_chunks);
                    subframes.push_front(self.clone());
                    let mut inner_file_frame = FileFrame::default();
                    inner_file_frame.starting_delimiter = self.starting_delimiter.clone();
                    inner_file_frame.ending_delimiter = self.ending_delimiter.clone();
                    inner_file_frame.file_chunks = self.remainder.clone();
                    inner_file_frame.escape_byte = self.escape_byte;
                    return Ok(subframes);
                }
            }
            subframes.push_front(self.clone());
            let mut inner_file_frame = FileFrame::default();
            inner_file_frame.starting_delimiter = self.starting_delimiter.clone();
            inner_file_frame.ending_delimiter = self.ending_delimiter.clone();
            inner_file_frame.escape_byte = self.escape_byte;
            if let Ok(frames) = inner_file_frame.recursively_handle_bytes(self.remainder.clone()) {
                subframes.extend(frames);
            }
            return Ok(subframes);
        } else {
            if self.starting_delimiter.is_none() {
                self.collect_buffer = true;
                return self.recursively_handle_bytes(bytes);
            } else {
                let mut starting_offset = 0;
                let mut bytes_iter = total_bytes.clone().into_iter().multipeek();
                let starting_delimiter = self.starting_delimiter.clone().unwrap();
                starting_offset = 0;
                'start_delims: loop {
                    let delimiter_iter = starting_delimiter.iter();
                    // for (i, expected) in delimiter_iter.clone().enumerate() {
                    match find_subsequence_by_windows_iter(delimiter_iter.clone(), bytes_iter.clone()){
                        SubsequenceStatus::Pending => {
                            
                        },
                        SubsequenceStatus::NotMatched => {
                            return Err(FileFrameStatus::NotValidFrame);
                        },
                        SubsequenceStatus::Continue => {
                            bytes_iter.next();
                            starting_offset += 1;
                            continue 'start_delims;
                        },
                        SubsequenceStatus::FoundAt(pos) => starting_offset = pos,
                    }
                    //}
                    self.remainder = total_bytes[..starting_offset].to_vec();

                    self.file_chunks =
                        total_bytes[starting_offset + starting_delimiter.len()..].to_vec();
                    self.collect_buffer = true;
                    //subframes.push_front(self.clone());
                    let mut inner_file_frame = FileFrame::default();
                    inner_file_frame.starting_delimiter = self.starting_delimiter.clone();
                    inner_file_frame.ending_delimiter = self.ending_delimiter.clone();
                    inner_file_frame.escape_byte = self.escape_byte;
                    //inner_file_frame.remainder = self.remainder.clone();
                    inner_file_frame.file_chunks = self.file_chunks.clone();
                    inner_file_frame.collect_buffer = true;
                    if let Ok(frames) = inner_file_frame.recursively_handle_bytes(Vec::new()) {
                        subframes.extend(frames);
                    } else {
                        println!("pushing a subframe {:?}", self.file_chunks);
                        println!("remainder at subframe {:?}", self.remainder);
                        if let Some(ref ending_delimiter) = self.ending_delimiter {
                            if total_bytes.len() - starting_delimiter.len() == self.file_chunks.len()
                                || self.remainder.len() == ending_delimiter.len()
                            {
                                self.file_chunks = self.file_chunks
                                    [0..self.file_chunks.len() - ending_delimiter.len()]
                                    .to_vec();
                            }
                        } else {
                            println!("no ending delimiter");
                        }
                        subframes.push_front(self.clone());
                        //println!("got errors one level down");
                    }
                    return Ok(subframes);
                    //}
                }
            }
        }
        //todo!()
    }
    //&mut

}

pub struct TcpFsReceiver {
    // tx: broadcast::Sender<Vec<u8>>,
    // rx: broadcast::Receiver<Vec<u8>>,
    tx: flume::Sender<Vec<u8>>,
    rx: flume::Receiver<Vec<u8>>,
    start_delimiter: Option<Vec<u8>>,
    end_delimiter: Option<Vec<u8>>,
}
impl TcpFsReceiver {
    pub fn new(tx: flume::Sender<Vec<u8>>, rx: flume::Receiver<Vec<u8>>) -> TcpFsReceiver {
        TcpFsReceiver {
            tx,
            rx,
            start_delimiter: None,
            end_delimiter: None,
        }
    }
    pub fn set_start_delimiter(&mut self, delim: Vec<u8>) {
        self.start_delimiter = Some(delim);
    }
    pub fn set_end_delimiter(&mut self, delim: Vec<u8>) {
        self.end_delimiter = Some(delim);
    }
    pub fn send(&mut self, bytes: Vec<u8>) {
        let res = self.tx.send(bytes);
        println!("{:#?}", res);
    }
}
impl Clone for TcpFsReceiver {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            rx: self.rx.clone(),
            start_delimiter: self.start_delimiter.clone(),
            end_delimiter: self.start_delimiter.clone(),
        }
    }
}
impl Default for TcpFsReceiver {
    fn default() -> Self {
        let (tx, rx) = flume::bounded(32);
        Self {
            tx,
            rx,
            start_delimiter: None,
            end_delimiter: None,
        }
    }
}
pub struct TcpFsSender {
    tx: flume::Sender<Vec<u8>>,
    rx: flume::Receiver<Vec<u8>>,
    byte_array: Vec<u8>,
    start_delimiter: Option<Vec<u8>>,
    end_delimiter: Option<Vec<u8>>,
}
// impl Clone for TcpFsSender {
//     fn clone(&self) -> Self {
//         Self {
//             tx: self.tx.clone(),
//             // rx: self.rx.resubscribe(),
//             rx: std::mem::swap(self.rx),
//             byte_array: self.byte_array.clone(),
//             start_delimiter: self.start_delimiter.clone(),
//             end_delimiter: self.end_delimiter.clone(),
//         }
//     }
// }

impl TcpFsSender {
    pub fn new(rx: flume::Receiver<Vec<u8>>, tx: flume::Sender<Vec<u8>>) -> TcpFsSender {
        TcpFsSender {
            tx,
            rx,
            byte_array: Vec::new(),
            start_delimiter: None,
            end_delimiter: None,
        }
    }
    pub fn set_start_delimiter(&mut self, delim: Vec<u8>) {
        self.start_delimiter = Some(delim);
    }
    pub fn set_end_delimiter(&mut self, delim: Vec<u8>) {
        self.end_delimiter = Some(delim);
    }
    pub fn feed_bytes(&mut self, bytes: Vec<u8>) {
        self.byte_array.extend(bytes);
    }

    pub async fn send(&mut self, bytes: Vec<u8>) {
        let res = self.tx.send_async(bytes).await;
        println!("{:#?}", res);
    }
}
pub struct File {
    pub original_location: Option<String>,
    pub final_location: String,
    pub content_stream: Option<flume::Receiver<Vec<u8>>>,
}
impl Clone for File {
    fn clone(&self) -> Self {
        Self {
            original_location: self.original_location.clone(),
            final_location: self.final_location.clone(),
            content_stream: self.content_stream.as_ref().map(|stream| stream.clone()),
        }
    }
}

pub struct RemoteFileSystem<S> {
    state: S,
    local_state: HashMap<String, LocalState>,
    direction: Direction,
    operation: Operation,
    codec: Codec,
    files: Vec<File>,
    task: Mutex<()>, //in_operation: bool,
}
impl<S: Default> Default for RemoteFileSystem<S> {
    fn default() -> Self {
        Self {
            state: S::default(),
            local_state: HashMap::new(),
            direction: Direction::Unknown,
            operation: Operation::None,
            codec: Codec::Unknown,
            files: Vec::new(),
            task: Mutex::new(()),
            //in_operation: false,
        }
    }
}

#[derive(Debug)]
pub struct LocalState {
    pub location: String,
}


impl RemoteFileSystem<TcpFsReceiver> {
    pub fn new(state: TcpFsReceiver) -> Self {
        RemoteFileSystem {
            state,
            ..Default::default()
        }
    }
    pub fn inner_mut(&mut self) -> &mut TcpFsReceiver {
        &mut self.state
    }
    pub fn set_codec(&mut self, codec: Codec) {
        self.codec = codec;
    }
    pub fn set_direction(&mut self, direction: Direction) {
        self.direction = direction;
    }
    pub fn create_state(&mut self, state_name: String, state: LocalState) {
        self.local_state.insert(state_name, state);
    }
    pub fn remove_state(&mut self, state_name: String) {
        self.local_state.remove(&state_name);
    }
    fn patch_state_location_path(&mut self, state_name: &str) {
        if let Some(state) = self.local_state.get_mut(state_name) {
            if state.location.starts_with("//") {
                state.location = state.location[1..].to_string();
            }
            if state.location.starts_with("/~") {
                state.location = state.location[1..].to_string();
            }
        }
    }
    pub async fn receive_operation(
        &mut self,
        fs_state_name: String,
    ) {
        println!("re receiving");
        //Box::pin(async move {
            let mut frame = FileFrame::default();
            frame.starting_delimiter = self.state.start_delimiter.clone();
            frame.ending_delimiter = self.state.end_delimiter.clone();
            println!("receiving in here");
            let new_location = "/home/spiderunderurbed/projects/tcp_fs_poc/output.jar";
            let mut file_handle: Option<std::fs::File> = None;

            let mut temp_handle = std::fs::OpenOptions::new().append(false).open(new_location);
            //.open(&mut *location);
            if let Err(_) = temp_handle {
                let _ = std::fs::File::create(
                    //&mut *location,
                    new_location,
                );
                temp_handle = std::fs::OpenOptions::new().append(true).open(new_location)
                //.open(&mut *location);
            }
            file_handle = Some(temp_handle.unwrap());

            let mut remainder: Vec<u8> = Vec::new();
            let mut all_frames: VecDeque<FileFrame> = VecDeque::new();
            loop {
                match self.state.rx.recv_async().await {
                        Ok(bytes) => {
                           //println!("{:?}", bytes);
                            let mut frame = FileFrame::default();
                            frame.remainder = remainder.clone();
                            frame.starting_delimiter = self.state.start_delimiter.clone();
                            frame.ending_delimiter = self.state.end_delimiter.clone();
                            match frame.recursively_handle_bytes(bytes) {
                                Ok(frames) => {
                                    all_frames.extend(frames.clone());
                                    //println!("got frames: {}", frames.len());
                                    for (i, frame) in all_frames.iter().enumerate() {
                                        //println!("{:#?}", frame);
                                        println!("{}: frame chunks: {:?}", i, frame.file_chunks);
                                        //println!("{}: frame remainder: {:?}", i, frame.remainder);
                                        //remainder = frame.remainder.clone();
                                    }
                                    remainder = frames.iter().last().unwrap().remainder.clone();
                                },
                                Err(e) => match e {
                                    FileFrameStatus::NotValidFrame => {
                                        println!("got a non valid frame");
                                        // println!("err here");
                                        // println!("not valid frame");
                                    },
                                    FileFrameStatus::Pending => {

                                    },
                                    FileFrameStatus::Unknown => {

                                    },
                                },
                            }
                        }, 
                        Err(e) => {
                            // for (i, frame) in all_frames.iter().enumerate() {
                            //     println!("{}: frame chunks: {:?}", i, frame.file_chunks);
                            //     //println!("{}: frame remainder: {:?}", i, frame.remainder);
                            // }
                            break;
                            //println!("got a flume error");
                        }
                }
            }
        //})
    }
}
// fn log_head_tail(bytes: &[u8]) {
//     let n = bytes.len();
//     let head_n = n.min(10);
//     let tail_n = n.min(10);

//     let head = &bytes[..head_n];
//     let tail = &bytes[n - tail_n..];

//     println!(
//         "sending {} bytes | first {}: {:?} | last {}: {:?}",
//         n, head_n, head, tail_n, tail
//     );
// }
static ESCAPE_BYTE: u8 = 22;
impl RemoteFileSystem<TcpFsSender> {
    pub fn new(state: TcpFsSender) -> Self {
        Self {
            // _marker: PhantomData,
            state,
            local_state: HashMap::new(),
            direction: Direction::Unknown,
            operation: Operation::None,
            codec: Codec::Unknown,
            files: Vec::new(),
            task: Mutex::new(()), //in_operation: false,
        }
    }
    pub fn inner_mut(&mut self) -> &mut TcpFsSender {
        &mut self.state
    }
    pub fn set_codec(&mut self, codec: Codec) {
        self.codec = codec;
    }
    pub fn set_direction(&mut self, direction: Direction) {
        self.direction = direction;
    }
    pub fn set_operation(
        &mut self,
        operation: Operation,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.operation = operation;
        Ok(())
    }
    pub fn create_state(&mut self, state_name: String, state: LocalState) {
        self.local_state.insert(state_name, state);
    }
    pub fn remove_state(&mut self, state_name: String) {
        self.local_state.remove(&state_name);
    }
    pub fn clear_files(&mut self) {
        self.files = vec![];
    }
    pub fn append_files(&mut self, file: File) {
        self.files.push(file);
    }

    pub fn execute_operation(
        &mut self,
        fs_state_name: String,
    ) -> Pin<
        Box<dyn Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send + '_>,
    > {
        match self.operation {
            Operation::Move => {
                return Box::pin(async move {
                    let mut files = std::mem::take(&mut self.files);
                    for mut file in files.drain(..) {
                        self.operation = Operation::Set;
                        let _ = self.execute_operation(file.final_location.clone()).await;
                        self.operation = Operation::Move;
                        match self.codec {
                            Codec::Raw => {
                                todo!()
                            }
                            // Codec::RawContinues => {
                            // }
                            Codec::Multipart | Codec::RawContinues => {
                                //let mut temp_buf: Vec<u8> = Vec::with_capacity(4080);
                                //let mut position: u16 = 20;
                                // let direction_header = self.direction.to_byte_header().unwrap();
                                // let operation_header = self.operation.to_byte_header().unwrap();
                                // let codec_header = self.codec.to_byte_header().unwrap();

                                // let mut previous_buf: Vec<u8> = Vec::with_capacity(4096);
                                //let mut delimiter_offset = 0;

                                // let direction_header = self.direction.clone() as u8;
                                // let operation_header = self.operation.clone() as u8;
                                // let codec_header = self.codec.clone() as u8;
                                let mut frame = FileFrame::new(
                                    self.state.start_delimiter.clone(),
                                    self.state.end_delimiter.clone(),
                                    self.direction.clone(),
                                    self.operation.clone(),
                                    self.codec.clone(),
                                    ChunkingStatus::Continues,
                                    Some(ESCAPE_BYTE)
                                );

                                loop {
                                    let mut file_content_stream =
                                        file.content_stream.take().unwrap();

                                    match file_content_stream.recv_async().await {
                                        Ok(bytes) => {
                                            // frame.append_bytes_recv(bytes);
                                            //let guard = self.task.lock().await;
                                            frame.append_bytes_send(bytes.clone());
                                            // println!("sending bytes {:#?}", bytes);
                                            if frame.file_chunks.len() >= 1000 {
                                                println!("got a bunch of bytes");
                                                if let Ok(bytes) = frame.to_bytes() {
                                                    println!(
                                                        "{:?}",
                                                        bytes[0..bytes.len().clamp(0, 10)].to_vec()
                                                    );
                                                    self.state.send(bytes).await;
                                                    println!("sent the bytes");
                                                    frame.flush();
                                                    // break;
                                                } else {
                                                    println!("err");
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            match e {
                                                // broadcast::error::RecvError::Closed => {
                                                //     if let Ok(bytes) = frame.to_bytes() {
                                                //         //println!("{:#?}", bytes);
                                                //         self.state.send(bytes).await;
                                                //         //println!("sent the bytes after closing");
                                                //         frame.flush();
                                                //     } else {
                                                //         println!("err");
                                                //     }
                                                // }
                                                // broadcast::error::RecvError::Lagged(n) => {
                                                //     println!("lagged here at {}", n);
                                                // }
                                                flume::RecvError::Disconnected => {
                                                    //println!("disconnected");
                                                }
                                            }
                                        }
                                    }
                                    file.content_stream = Some(file_content_stream);
                                }
                            }
                            _ => return Err("unimplimented".into()),
                        }
                    }
                    Ok(())
                });
            }
            Operation::None => return Box::pin(async { Err("unimplimented".into()) }),
            Operation::Set => {
                return Box::pin(async move {
                    //todo!()
                    Ok(())
                    // println!("setting");
                    // if let Some(state) = self.local_state.get(&fs_state_name) {
                    //     let mut temp_buf: Vec<u8> = Vec::with_capacity(4080);
                    //     if let Some(start_delims) = &self.state.start_delimiter {
                    //         temp_buf.extend(start_delims);
                    //     }
                    //     temp_buf.push(self.direction.to_byte_header().unwrap());
                    //     temp_buf.push(self.operation.to_byte_header().unwrap());
                    //     temp_buf.extend_from_slice(state.location.as_bytes().into());
                    //     if let Some(end_delims) = &self.state.end_delimiter {
                    //         temp_buf.extend(end_delims);
                    //     }
                    //     self.state.send(temp_buf).await;
                    //     Ok(())
                    // } else {
                    //     Err("err".into())
                    // }
                });
                //return Box::pin(async { Err("unimplimented".into()) })
            }
            Operation::Ls => {
                return Box::pin(async move {
                    todo!()
                    // if let Some(state) = self.local_state.get(&fs_state_name) {
                    //     let mut temp_buf: Vec<u8> = Vec::with_capacity(4080);
                    //     if let Some(start_delims) = &self.state.start_delimiter {
                    //         temp_buf.extend(start_delims);
                    //     }
                    //     temp_buf.push(self.direction.to_byte_header().unwrap());
                    //     temp_buf.push(self.operation.to_byte_header().unwrap());
                    //     temp_buf.extend_from_slice(state.location.as_bytes().into());
                    //     if let Some(end_delims) = &self.state.end_delimiter {
                    //         temp_buf.extend(end_delims);
                    //     }
                    //     self.state.send(temp_buf).await;
                    //     Ok(())
                    // } else {
                    //     Err("err".into())
                    // }
                });
            }
        }
    }
    // pub fn has_operation(&self) -> bool {
    //     true
    // }
}

// pub trait FsType: Clone + Send + Sync {}
// impl FsType for TcpFsSender {}
// impl FsType for TcpFsReceiver {}

// temp_buf.push(direction_header);
// temp_buf.push(operation_header);
// // If it cannot fill the buffer change this to 1 to say it continues
// temp_buf.push(codec_header);
// // This says it does not continue, this will change if there is more to the buffer
// temp_buf.push(1);

// loop {
//     let mut file_content_stream =
//         file.content_stream.take().unwrap();
//     match file_content_stream.recv().await {
//         Ok(bytes) => {
//                 if (bytes.len() as u16 + temp_buf.len() as u16) >= 4076 {
//                     println!("continuing");
//                     temp_buf[delimiter_offset + 3] = 0;
//                     // self.state.send(temp_buf.clone()).await;
//                     let previous_len = temp_buf.len();
//                     temp_buf.extend_from_slice(&bytes);
//                     loop {
//                         let mut new_buf: Vec<u8> = temp_buf.drain(..previous_bytes_len).collect();
//                         if let Some(end_delims) = &self.state.end_delimiter {
//                             new_buf.extend(end_delims);
//                         }
//                         // temp_buf = Vec::new();
//                         self.state.send(new_buf.clone()).await;
//                         if let Some(start_delims) = &self.state.start_delimiter {
//                             delimiter_offset = start_delims.len();
//                             temp_buf.extend(start_delims);
//                         }
//                         temp_buf.push(direction_header);
//                         temp_buf.push(operation_header);
//                         temp_buf.push(codec_header);
//                         temp_buf.push(1);
//                         println!("byte len {}", bytes.len());
//                         if (bytes.len() >= 4096)  || ((bytes.len() as u16 + temp_buf.len() as u16) < 4076) {
//                             break;
//                         }
//                     }
//                     // temp_buf.extend_from_slice(&bytes);
//                     // if (bytes.len() as u16 + temp_buf.len() as u16) < 4076 {
//                     //     break;
//                     // }
//                     break;
//                     //break;
//                 } else {
//                     // Add the current data onto the buffer
//                     println!("extending");
//                     //position = bytes.len() as u16;
//                     temp_buf.extend_from_slice(&bytes);
//                     break;
//                 }
//             //}
//         }
//         Err(e) => {
//             match e {
//                 broadcast::error::RecvError::Closed => {
//                     println!("closed");
//                     break;
//                 }
//                 broadcast::error::RecvError::Lagged(_) => {
//                     println!("lagged");
//                 }
//             }
//         }
//     }
//     file.content_stream = Some(file_content_stream);
// }
// if let Some(end_delims) = &self.state.end_delimiter {
//     temp_buf.extend(end_delims);
// }
//self.state.send(temp_buf).await;
// self.file.content_stream = Some(stream);
// self.recv_future = None;
// if bytes.len() >= 4076 {
//     println!("too long");
//     return Err(
//         "cannot send over 4076 bytes as a segment"
//             .into(),
//     );
// }
// This will continue on to the next message
// so signify it continues then return the current buffer
//println!("{} {}", bytes.len(), position);
//loop {
// pub struct RemoteFileSystem<S: FsType> {
//     state: S,
//     local_state: HashMap<String, LocalState>,
//     direction: Direction,
//     operation: Operation,
//     codec: Codec,
//     files: Vec<File>,
// }
// pub struct RemoteFileSystem<TcpFsSender> {
//     state: S,
//     local_state: HashMap<String, LocalState>,
//     direction: Direction,
//     operation: Operation,
//     codec: Codec,
//     files: Vec<File>,
// }
// pub struct RemoteFileSystem<TcpFsReceiver> {
//     state: S,
// }

// impl<S: FsType> RemoteFileSystem<S> {
//     pub fn new(_state: S) -> Self {
//         Self {
//             _marker: PhantomData,
//             local_state: HashMap::new(),
//         }
//     }
// }
// trait WorkingFs: Clone + Send + Sync {
// }