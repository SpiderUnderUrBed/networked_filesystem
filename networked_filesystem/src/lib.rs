use std::{
    collections::HashMap,
    io::Write,
    marker::PhantomData,
    os::unix::ffi::OsStrExt,
    pin::Pin,
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, Ordering},
        mpsc::Receiver,
    },
    task::{Context, Poll},
};

use multer::bytes::buf;
use multipeek::IteratorExt;
use tokio::sync::{Mutex, Notify, broadcast};
use tokio::{io::AsyncWriteExt, sync::watch};
//use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub enum Direction {
    Local,
    Server,
    Unknown,
}
impl Direction {
    fn to_byte_header(&self) -> Option<u8> {
        match self {
            Direction::Local => Some(0),
            Direction::Server => Some(1),
            Direction::Unknown => None,
        }
    }
    fn from_byte_header(byte: u8) -> Option<Direction> {
        match byte {
            0 => Some(Direction::Local),
            1 => Some(Direction::Server),
            _ => None,
        }
    }
}

#[derive(Clone)]
pub enum Operation {
    Move,
    Set,
    Ls,
    // LsWithRange { start: u64, end: u64 },
    None,
}
impl Operation {
    fn to_byte_header(&self) -> Option<u8> {
        match self {
            Operation::Move => Some(1),
            Operation::None => None,
            Operation::Set => Some(2),
            Operation::Ls => Some(3),
        }
    }
    fn from_byte_header(byte: u8) -> Option<Operation> {
        match byte {
            1 => Some(Operation::Move),
            2 => Some(Operation::Set),
            3 => Some(Operation::Ls),
            _ => None,
        }
    }
}

#[derive(Clone)]
pub enum Codec {
    Raw,
    RawContinues,
    Multipart,
    Unknown,
}
impl Codec {
    fn to_byte_header(&self) -> Option<u8> {
        match self {
            Codec::Raw => Some(0),
            Codec::Multipart => Some(1),
            Codec::RawContinues => Some(2),
            Codec::Unknown => None,
        }
    }
    fn from_byte_header(byte: u8) -> Option<Codec> {
        match byte {
            0 => Some(Codec::Raw),
            1 => Some(Codec::Multipart),
            2 => Some(Codec::RawContinues),
            _ => None,
        }
    }
}
#[derive(Clone, Debug)]
pub enum ChunkingStatus {
    Continues,
    End,
}
impl ChunkingStatus {
    fn to_byte_header(&self) -> u8 {
        match self {
            ChunkingStatus::Continues => 1,
            ChunkingStatus::End => 0,
        }
    }
    fn from_byte_header(byte: u8) -> Option<ChunkingStatus> {
        match byte {
            0 => Some(ChunkingStatus::Continues),
            1 => Some(ChunkingStatus::End),
            _ => None,
        }
    }
}

pub struct TcpFsReceiver {
    tx: broadcast::Sender<Vec<u8>>,
    rx: broadcast::Receiver<Vec<u8>>,
    start_delimiter: Option<Vec<u8>>,
    end_delimiter: Option<Vec<u8>>,
}
impl TcpFsReceiver {
    pub fn new(tx: broadcast::Sender<Vec<u8>>, rx: broadcast::Receiver<Vec<u8>>) -> TcpFsReceiver {
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
    }
}
impl Clone for TcpFsReceiver {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            rx: self.rx.resubscribe(),
            start_delimiter: self.start_delimiter.clone(),
            end_delimiter: self.start_delimiter.clone(),
        }
    }
}
impl Default for TcpFsReceiver {
    fn default() -> Self {
        let (tx, rx) = broadcast::channel::<Vec<u8>>(32);
        Self {
            tx,
            rx,
            start_delimiter: None,
            end_delimiter: None,
        }
    }
}
pub struct TcpFsSender {
    tx: broadcast::Sender<Vec<u8>>,
    rx: broadcast::Receiver<Vec<u8>>,
    byte_array: Vec<u8>,
    start_delimiter: Option<Vec<u8>>,
    end_delimiter: Option<Vec<u8>>,
}
impl Clone for TcpFsSender {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            rx: self.rx.resubscribe(),
            byte_array: self.byte_array.clone(),
            start_delimiter: self.start_delimiter.clone(),
            end_delimiter: self.end_delimiter.clone(),
        }
    }
}

impl TcpFsSender {
    pub fn new(rx: broadcast::Receiver<Vec<u8>>, tx: broadcast::Sender<Vec<u8>>) -> TcpFsSender {
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
        let _ = self.tx.send(bytes);
    }
}
pub struct File {
    pub original_location: Option<String>,
    pub final_location: String,
    pub content_stream: Option<broadcast::Receiver<Vec<u8>>>,
}
impl Clone for File {
    fn clone(&self) -> Self {
        Self {
            original_location: self.original_location.clone(),
            final_location: self.final_location.clone(),
            content_stream: self
                .content_stream
                .as_ref()
                .map(|stream| stream.resubscribe()),
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
    pub fn receive_operation(
        &mut self,
        fs_state_name: String,
    ) -> Pin<
        Box<dyn Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send + '_>,
    > {
        Box::pin(async move {
            let mut raw_current_buffer: Vec<u8> = Vec::with_capacity(4096);

            let mut file_buffer: Vec<u8> = Vec::with_capacity(4096);
            let mut direction: Option<Direction> = None;
            let mut operation: Option<Operation> = None;
            let mut codec: Option<Codec> = None;
            let mut continues: Option<ChunkingStatus> = None;

            let mut file_handle = None;

            let mut location = {
                match self.local_state.get(&fs_state_name) {
                    Some(fs_state) => fs_state.location.clone(),
                    None => "/tmp/fserror".to_string(),
                }
            };

            'end: loop {
                let mut start_deliter_offset = 0;
                let mut end_deliter_offset;
                match self.state.rx.recv().await {
                    Ok(bytes) => {
                        raw_current_buffer.extend(bytes.clone());
                        end_deliter_offset = bytes.len();
                        let mut new_bytes = Vec::with_capacity(bytes.len() + 1);
                        new_bytes.push(0);
                        new_bytes.extend(bytes);
                        let mut bytes_iter = new_bytes.into_iter().multipeek();
                        //'searching: while let Some(_) = bytes_iter.peek() {
                        // 'searching_start_delims: while let Some(_) = bytes_iter.next() {
                        //     'start_delim: {
                        //         if let Some(start_delimiter) = &self.state.start_delimiter {
                        //             //deliter_offset = start_delimiter.len();
                        //             for (i, delimiter_byte) in start_delimiter.iter().enumerate() {
                        //                 if let Some(future_byte) = bytes_iter.peek_nth(i) {
                        //                     if future_byte != delimiter_byte {
                        //                         start_deliter_offset += 1;
                        //                         break 'start_delim;
                        //                     }
                        //                 } else {
                        //                     break 'end;
                        //                 }
                        //             }
                        //             start_deliter_offset = start_deliter_offset + start_delimiter.len();
                        //             break 'searching_start_delims;
                        //         } else {
                        //             break 'searching_start_delims;
                        //         }
                        //     }
                        // }
                        start_deliter_offset = {
                            if let Some(start_delimiter) = &self.state.start_delimiter {
                                find_subsequence(&raw_current_buffer, start_delimiter).unwrap_or(0)
                            } else {
                                0
                            }
                        };
                        for _ in 0..start_deliter_offset {
                            bytes_iter.next();
                        };
                        println!("start:{}\nend:{}", start_deliter_offset, end_deliter_offset);
                        println!(
                            "{:?}\n{:?}\n{:?}\noffset: {}\nend_offset: {}",
                            String::from_utf8_lossy(&raw_current_buffer[0..10]),
                            String::from_utf8_lossy(&raw_current_buffer[start_deliter_offset..start_deliter_offset + 10]),
                            String::from_utf8_lossy(&raw_current_buffer[start_deliter_offset..end_deliter_offset]),
                            start_deliter_offset,
                            end_deliter_offset
                        );
                        if bytes_iter.peek_nth(2).is_none() {
                            println!("err cannot pick any new operations or so on");
                            break;
                        }
                        let current_byte = bytes_iter.next().unwrap();
                        println!("current byte: {:#?}", current_byte.clone());
                        if direction.is_none() {
                            // direction = Direction::from_byte_header(current_byte);
                            match Direction::from_byte_header(*bytes_iter.peek_nth(1).unwrap()) {
                                Some(new_direction) => {
                                    direction = Some(new_direction);
                                },
                                None => {
                                    if direction.is_none() {
                                        println!("no direction");
                                        return Err("invalid, no direction was processed".into());
                                    }
                                },
                            }
                            bytes_iter.next();
                            //break 'end;
                        }
                        // Ensures an operation was set
                        if operation.is_none() {
                            match Operation::from_byte_header(*bytes_iter.peek_nth(1).unwrap()) {
                                Some(new_operation) => {
                                    operation = Some(new_operation);
                                },
                                None => {
                                    if operation.is_none() {
                                        println!("no operation");
                                        return Err("invalid, no operation was processed".into());
                                    }
                                },
                            }
                            bytes_iter.next();
                            if matches!(operation.clone().unwrap(), Operation::Set)
                                || matches!(operation.clone().unwrap(), Operation::Ls)
                            {
                                location.clear();
                            } else if matches!(operation.clone().unwrap(), Operation::Move) {
                                if bytes_iter.peek_nth(2).is_none() {
                                    println!("err cannot pick the operations for move");
                                    break;
                                }
                                if codec.is_none() {
                                    match Codec::from_byte_header(*bytes_iter.peek_nth(1).unwrap()) {
                                        Some(new_codec) => {
                                            codec = Some(new_codec);
                                        },
                                        None => {},
                                    }
                                    if codec.is_none() {
                                        println!("still no codec");
                                        return Err("invalid, no codec was processed".into());
                                    }
                                    bytes_iter.next();
                                }

                                if continues.clone().is_none() {
                                    match ChunkingStatus::from_byte_header(
                                        *bytes_iter.peek_nth(1).unwrap(),
                                    ) {
                                        Some(new_continues) => {
                                            continues = Some(new_continues);
                                        },
                                        None => {},
                                    }
                                    println!("this is continues: {:#?}", continues);
                                    if continues.is_none() {
                                        println!("still no continues");
                                        return Err("invalid, no continuation was processed".into());
                                    }
                                }
                            }
                        }

                        // 'searching_end_delims: while let Some(_) = bytes_iter.next() {
                        //     'end_delim: {
                        //         if let Some(end_delimiter) = &self.state.end_delimiter {
                        //             //deliter_offset = start_delimiter.len();
                        //             for (i, delimiter_byte) in end_delimiter.iter().enumerate() {
                        //                 if let Some(future_byte) = bytes_iter.peek_nth(i) {
                        //                     if future_byte != delimiter_byte {
                        //                         bytes_iter.nth(i);
                        //                         break 'end_delim;
                        //                     }
                        //                 } else {
                        //                     break 'end;
                        //                 }
                        //             }
                        //         } else {
                        //             break 'searching_end_delims;
                        //         }
                        //     }
                        // }

                        end_deliter_offset = {
                            if let Some(end_delimiter) = &self.state.end_delimiter {
                                find_subsequence(&raw_current_buffer, end_delimiter).unwrap_or(raw_current_buffer.len())
                            } else {
                                raw_current_buffer.len()
                            }
                        };
                        if matches!(operation.clone().unwrap(), Operation::Set){
                            let buffer = (&raw_current_buffer
                                [start_deliter_offset + 2..end_deliter_offset])
                                .to_vec();         
                            location = String::from_utf8(buffer).unwrap();
                        } if matches!(operation.clone().unwrap(), Operation::Move) {
                            println!("will connect buffer");
                            // file_buffer = (&raw_current_buffer
                            //     [start_deliter_offset + 4..end_deliter_offset])
                            //     .to_vec();
                            let buffer = (&raw_current_buffer
                                [start_deliter_offset + 4..end_deliter_offset])
                                .to_vec();
                            let new_location = "/home/spiderunderurbed/projects/tcp_fs_poc/filesystem_demo.txt";
                            if file_handle.is_none() {
                                println!("{:#?}", new_location);
                                let mut temp_handle = std::fs::OpenOptions::new()
                                    .append(true)
                                    .open(&mut *location);
                                if let Err(_) = temp_handle {
                                    let _ = std::fs::File::create(new_location);
                                    temp_handle = std::fs::OpenOptions::new()
                                        .append(true)
                                        .open(new_location);
                                }
                                file_handle = Some(temp_handle.unwrap());
                            }
                            println!("writing to");
                            let mut local_handle = file_handle.take().unwrap();
                            let _ = local_handle.write_all(&buffer);
                            if matches!(continues.clone().unwrap(), ChunkingStatus::Continues) {
                                file_handle = Some(local_handle);
                            } else {
                                let _ = local_handle.flush();
                            }
                        }
                        raw_current_buffer.clear();
                    }
                    Err(e) => match e {
                        broadcast::error::RecvError::Closed => {
                            println!("closed")
                        }
                        broadcast::error::RecvError::Lagged(_) => {
                            println!("lagged");
                        }
                    },
                }
                // match self.state.rx.recv().await {
                //     Ok(bytes) => todo!(),
                //     Err(e) => {
                //         match e {
                //             broadcast::error::RecvError::Closed => {
                //                 println!("closed")
                //             },
                //             broadcast::error::RecvError::Lagged(_) => {
                //                 println!("lagged");
                //             },
                //         }
                //     },
                // }
            }
            Ok(())
        })
    }
}
fn find_subsequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|window| window == needle)
}
fn log_head_tail(bytes: &[u8]) {
    let n = bytes.len();
    let head_n = n.min(10);
    let tail_n = n.min(10);

    let head = &bytes[..head_n];
    let tail = &bytes[n - tail_n..];

    println!(
        "sending {} bytes | first {}: {:?} | last {}: {:?}",
        n, head_n, head, tail_n, tail
    );
}
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
                        let _ = self.execute_operation(fs_state_name.clone()).await;
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
                                let direction_header = self.direction.to_byte_header().unwrap();
                                let operation_header = self.operation.to_byte_header().unwrap();
                                let codec_header = self.codec.to_byte_header().unwrap();

                                //let mut delimiter_offset = 0;

                                loop {
                                    let mut file_content_stream =
                                        file.content_stream.take().unwrap();

                                    match file_content_stream.recv().await {
                                        Ok(bytes) => {
                                            // TODO: consider if i need to fill and therfore allocate
                                            // the capacity of the max size this buffer could be
                                            let mut temp_buf: Vec<u8> = Vec::with_capacity(4096);

                                            let chunks: Vec<&[u8]> = bytes.chunks(4076).collect();
                                            let chunks_length = chunks.len();
                                            for (i, chunk) in chunks.into_iter().enumerate() {
                                                let mut temp_buf: Vec<u8> =
                                                    Vec::with_capacity(4096);
                                                if let Some(start_delims) =
                                                    &self.state.start_delimiter
                                                {
                                                    temp_buf.extend(start_delims);
                                                }
                                                temp_buf.push(direction_header);
                                                temp_buf.push(operation_header);
                                                temp_buf.push(codec_header);
                                                temp_buf.push(if i == chunks_length - 1 {
                                                    1
                                                } else {
                                                    0
                                                });
                                                temp_buf.extend_from_slice(chunk);
                                                if let Some(end_delims) = &self.state.end_delimiter
                                                {
                                                    temp_buf.extend(end_delims);
                                                }
                                                self.state.send(temp_buf).await;
                                            }
                                            // self.state.send(temp_buf).await;
                                            file.content_stream = Some(file_content_stream);
                                        }
                                        Err(e) => match e {
                                            broadcast::error::RecvError::Closed => {
                                                println!("Closed here");
                                                file.content_stream = Some(file_content_stream);
                                                break;
                                            }
                                            broadcast::error::RecvError::Lagged(e) => {
                                                file.content_stream = Some(file_content_stream);
                                                println!("lagged: {:#?}", e);
                                            }
                                        },
                                    }
                                    //file.content_stream = Some(file_content_stream);
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
                    println!("setting");
                    if let Some(state) = self.local_state.get(&fs_state_name) {
                        let mut temp_buf: Vec<u8> = Vec::with_capacity(4080);
                        if let Some(start_delims) = &self.state.start_delimiter {
                            temp_buf.extend(start_delims);
                        }
                        temp_buf.push(self.direction.to_byte_header().unwrap());
                        temp_buf.push(self.operation.to_byte_header().unwrap());
                        temp_buf.extend_from_slice(state.location.as_bytes().into());
                        if let Some(end_delims) = &self.state.end_delimiter {
                            temp_buf.extend(end_delims);
                        }
                        self.state.send(temp_buf).await;
                        Ok(())
                    } else {
                        Err("err".into())
                    }
                });
                //return Box::pin(async { Err("unimplimented".into()) })
            }
            Operation::Ls => {
                return Box::pin(async move {
                    if let Some(state) = self.local_state.get(&fs_state_name) {
                        let mut temp_buf: Vec<u8> = Vec::with_capacity(4080);
                        if let Some(start_delims) = &self.state.start_delimiter {
                            temp_buf.extend(start_delims);
                        }
                        temp_buf.push(self.direction.to_byte_header().unwrap());
                        temp_buf.push(self.operation.to_byte_header().unwrap());
                        temp_buf.extend_from_slice(state.location.as_bytes().into());
                        if let Some(end_delims) = &self.state.end_delimiter {
                            temp_buf.extend(end_delims);
                        }
                        self.state.send(temp_buf).await;
                        Ok(())
                    } else {
                        Err("err".into())
                    }
                });
            }
        }
    }
    // pub fn has_operation(&self) -> bool {
    //     true
    // }
}

pub trait FsType: Clone + Send + Sync {}
impl FsType for TcpFsSender {}
impl FsType for TcpFsReceiver {}

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
//                     let previous_bytes_len = temp_buf.len();
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