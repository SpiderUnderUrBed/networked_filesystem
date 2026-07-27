use std::{
    collections::{HashMap, VecDeque},
    default,
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
    vec,
};

use multer::bytes::{self, buf};
use multipeek::{IteratorExt, MultiPeek};
use num_enum::{IntoPrimitive, TryFromPrimitive};
use tokio::sync::{Mutex, Notify, broadcast};
use tokio::{io::AsyncWriteExt, sync::watch};
use std::fs::File as StdFile;

use crate::subsequence::{SubsequenceStatus, find_subsequence_by_windows_iter};


mod subsequence;

#[derive(Clone, TryFromPrimitive)]
#[repr(u8)]
pub enum Direction {
    Local = 0,
    Server = 1,
    Unknown = 2,
}


#[derive(Clone, TryFromPrimitive)]
#[repr(u8)]
pub enum Operation {
    Move = 0,
    Set = 1,
    Ls = 3,
    // LsWithRange { start: u64, end: u64 },
    None = 4,
}


#[derive(Clone, TryFromPrimitive)]
#[repr(u8)]
pub enum Codec {
    Raw = 0,
    RawContinues = 1,
    Multipart = 2,
    Unknown = 4,
}

#[derive(Clone, Debug, TryFromPrimitive)]
#[repr(u8)]
pub enum ChunkingStatus {
    Continues = 0,
    End = 1,
}

#[derive(Default, Clone)]
pub struct FrameEncoder {
    escape_byte: Option<u8>,
    starting_delimiter: Option<Vec<u8>>,
    ending_delimiter: Option<Vec<u8>>,
    file_chunks: Vec<u8>,
    remainder: Vec<u8>,
    collect_buffer: bool,
}
pub trait FrameHandler {
    type FrameOutput;
    fn append_bytes_recv(
        &mut self,
        bytes: &Vec<u8>,
        _: &mut u64
    ) -> Result<VecDeque<Self::FrameOutput>, FileFrameStatus>;
    // fn set_remainder(&mut self, remainder: Vec<u8>);
    fn set_chunks(&mut self, chunks: Vec<u8>);
    fn get_remainder(&self) -> Vec<u8>;
    fn get_chunks(&self) -> Vec<u8>;
}
impl FrameEncoder {
    fn new(
        starting_delimiter: Option<Vec<u8>>,
        ending_delimiter: Option<Vec<u8>>,
        escape_byte: Option<u8>,
    ) -> Self {
        FrameEncoder {
            starting_delimiter,
            ending_delimiter,
            file_chunks: Vec::new(),
            remainder: Vec::new(),
            collect_buffer: false,
            escape_byte,
        }
    }
    fn encode_bytes(&self, headers: Vec<u8>, content: Vec<u8>) -> Vec<u8> {
        let mut bytes_frame = Vec::new();
        if let Some(ref starting_delimiter) = self.starting_delimiter {
            bytes_frame.extend(starting_delimiter);
        }
        bytes_frame.extend(headers);
        if let (Some(ref ending_delims), Some(ref starting_delims), Some(escape_byte)) = (
            self.ending_delimiter.clone(),
            self.starting_delimiter.clone(),
            self.escape_byte,
        ) {
            let starting_byte_to_escape = starting_delims.get(0).unwrap();
            let ending_byte_to_escape = ending_delims.get(0).unwrap();
            for (_, byte) in content.iter().enumerate() {
                if *byte == *starting_byte_to_escape || *byte == *ending_byte_to_escape {
                    bytes_frame.push(escape_byte);
                }
                bytes_frame.push(*byte);
            }
        } else {
            bytes_frame.extend(content.clone());
        }
        if let Some(ref ending_delimiter) = self.ending_delimiter {
            bytes_frame.extend(ending_delimiter);
        }
        bytes_frame
    }

}
impl FrameHandler for FrameEncoder {
    type FrameOutput = Self;
    fn append_bytes_recv(
        &mut self,
        bytes: &Vec<u8>,
        _: &mut u64
    ) -> Result<VecDeque<Self>, FileFrameStatus> {
        // println!("{:#?}, {:#?}, {:#?}", self.escape_byte, self.starting_delimiter, self.ending_delimiter);
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
                    match find_subsequence_by_windows_iter(
                        delimiter_iter.clone(),
                        bytes_iter.clone(),
                    ) {
                        SubsequenceStatus::Pending => {}
                        SubsequenceStatus::NotMatched => {
                            println!("no ending delims found for {:?}", self.file_chunks.clone());
                            return Err(FileFrameStatus::FrameNoEnds);
                        }
                        SubsequenceStatus::Continue => {
                            bytes_iter.next();
                            end_pos += 1;
                            continue 'end_delims;
                        }
                        SubsequenceStatus::FoundAt(pos) => end_pos = pos,
                    }
                    println!("self remainder: {:?}", self.remainder);
                    self.remainder = self.file_chunks[end_pos..self.file_chunks.len()].to_vec();
                    println!("new remainder: {:?}", self.remainder);
                    self.file_chunks = self.file_chunks[0..end_pos].to_vec();
                    println!("own chunks: {:?}", self.file_chunks);
                    //  subframes.push_front(self.clone());
                    let mut inner_file_frame = FrameEncoder::default();
                    inner_file_frame.starting_delimiter = self.starting_delimiter.clone();
                    inner_file_frame.ending_delimiter = self.ending_delimiter.clone();
                    inner_file_frame.file_chunks = self.remainder.clone();
                    inner_file_frame.escape_byte = self.escape_byte;
                    //return Ok(subframes);
                    break 'end_delims;
                }
            }
            println!("is handling remainder");
            subframes.push_front(self.clone());
            let mut inner_file_frame = FrameEncoder::default();
            inner_file_frame.starting_delimiter = self.starting_delimiter.clone();
            inner_file_frame.ending_delimiter = self.ending_delimiter.clone();
            inner_file_frame.escape_byte = self.escape_byte;
            match inner_file_frame.append_bytes_recv(&self.remainder.clone(), &mut 0) {
                Ok(frames) => {
                    // println!("frames len: {}", frames.len());
                    // if frames.len() > 1 {
                    //     // frames.pop_back();
                    // }
                    // if let Some(first_frame) = frames.pop_front() {
                    //     if first_frame.file_chunks != self.file_chunks {
                    //         subframes.push_back(first_frame);
                    //     }
                    // }
                    subframes.extend(frames);
                }
                Err(_) => {
                    println!("got an err handling remainders");
                }
            }
            return Ok(subframes);
        } else {
            if self.starting_delimiter.is_none() {
                self.collect_buffer = true;
                return self.append_bytes_recv(bytes, &mut 0);
            } else {
                let mut starting_offset = 0;
                let mut bytes_iter = total_bytes.clone().into_iter().multipeek();
                let starting_delimiter = self.starting_delimiter.clone().unwrap();
                starting_offset = 0;
                'start_delims: loop {
                    let delimiter_iter = starting_delimiter.iter();
                    // for (i, expected) in delimiter_iter.clone().enumerate() {
                    match find_subsequence_by_windows_iter(
                        delimiter_iter.clone(),
                        bytes_iter.clone(),
                    ) {
                        SubsequenceStatus::Pending => {}
                        SubsequenceStatus::NotMatched => {
                            return Err(FileFrameStatus::FrameNoBegins);
                        }
                        SubsequenceStatus::Continue => {
                            bytes_iter.next();
                            starting_offset += 1;
                            continue 'start_delims;
                        }
                        SubsequenceStatus::FoundAt(pos) => starting_offset = pos,
                    }
                    //}
                    self.remainder = total_bytes[..starting_offset].to_vec();

                    self.file_chunks =
                        total_bytes[starting_offset + starting_delimiter.len()..].to_vec();
                    self.collect_buffer = true;
                    //subframes.push_front(self.clone());
                    let mut inner_file_frame = FrameEncoder::default();
                    inner_file_frame.starting_delimiter = self.starting_delimiter.clone();
                    inner_file_frame.ending_delimiter = self.ending_delimiter.clone();
                    inner_file_frame.escape_byte = self.escape_byte;
                    //inner_file_frame.remainder = self.remainder.clone();
                    inner_file_frame.file_chunks = self.file_chunks.clone();
                    inner_file_frame.collect_buffer = true;
                    if let Ok(frames) = inner_file_frame.append_bytes_recv(&Vec::new(), &mut 0) {
                        subframes.extend(frames);
                    } else {
                        println!("pushing a subframe {:?}", self.file_chunks);
                        println!("remainder at subframe {:?}", self.remainder);
                        if let Some(ref ending_delimiter) = self.ending_delimiter {
                            if total_bytes.len() - starting_delimiter.len()
                                == self.file_chunks.len()
                                || self.remainder.len() == ending_delimiter.len()
                            {
                                if self.file_chunks.len() > ending_delimiter.len() {
                                    self.file_chunks = self.file_chunks
                                        [0..self.file_chunks.len() - ending_delimiter.len()]
                                        .to_vec();
                                } else {
                                    println!("throwing an error and cant return the subframe");
                                }
                            }
                        } else {
                            println!("no ending delimiter");
                        }
                        return Err(FileFrameStatus::FrameNoEnds)
                    }
                    return Ok(subframes);
                    //}
                }
            }
        }
        //todo!()
    }
    // fn set_remainder(&mut self, remainder: Vec<u8>){
    //     self.remainder = remainder;
    // }
    fn set_chunks(&mut self, chunks: Vec<u8>){
        println!("{}", chunks.len());
        self.remainder = chunks;
    }
    fn get_remainder(&self) -> Vec<u8> {
        self.remainder.clone()
    }
    
    fn get_chunks(&self) -> Vec<u8> {
        self.file_chunks.clone()
    }
}

pub trait HandleWithLength {
    fn to_bytes(&self) -> Result<Vec<u8>, FileFrameStatus>;
}
pub trait HandleWithDelims {
    // type FrameOutput;
    fn to_bytes(
        &self,
        escape_byte: Option<u8>,
        starting_delimiter: Option<Vec<u8>>,
        ending_delimiter: Option<Vec<u8>>,
    ) -> Result<Vec<u8>, FileFrameStatus>;
    //    fn create_frame_handler() -> Self::FrameOutput;

}
impl HandleWithDelims for FileFrame {
    // type FrameOutput = FrameEncoder;
    fn to_bytes(
        &self,
        escape_byte: Option<u8>,
        starting_delimiter: Option<Vec<u8>>,
        ending_delimiter: Option<Vec<u8>>,
    ) -> Result<Vec<u8>, FileFrameStatus> {
        let mut bytes_frame = Vec::new();
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
        let encoder = FrameEncoder::new(starting_delimiter, ending_delimiter, escape_byte);
        bytes_frame = encoder.encode_bytes(bytes_frame, self.file_chunks.clone());

        Ok(bytes_frame)
    }

}

pub struct WithDelims;
pub struct WithLength;

pub trait Handle<With> {
    fn to_bytes(
        &self,
        escape_byte: Option<u8>,
        starting_delimiter: Option<Vec<u8>>,
        ending_delimiter: Option<Vec<u8>>,
    ) -> Result<Vec<u8>, FileFrameStatus>;
}

impl<T: HandleWithDelims> Handle<WithDelims> for T {
    fn to_bytes(
        &self,
        escape_byte: Option<u8>,
        starting_delimiter: Option<Vec<u8>>,
        ending_delimiter: Option<Vec<u8>>,
    ) -> Result<Vec<u8>, FileFrameStatus> {
        HandleWithDelims::to_bytes(self, escape_byte, starting_delimiter, ending_delimiter)
    }
}

impl<T: HandleWithLength> Handle<WithLength> for T {
    fn to_bytes(
        &self,
        _escape_byte: Option<u8>,
        _starting_delimiter: Option<Vec<u8>>,
        _ending_delimiter: Option<Vec<u8>>,
    ) -> Result<Vec<u8>, FileFrameStatus> {
        HandleWithLength::to_bytes(self)
    }
}
#[derive(Default, Clone)]
struct FileFrame {
    direction: Option<Direction>,
    operation: Option<Operation>,
    codec: Option<Codec>,
    chunking_status: Option<ChunkingStatus>,
    file_chunks: Vec<u8>,
    remainder: Vec<u8>,
    collect_buffer: bool,
    complete_frame: bool,
}
pub enum FileFrameStatus {
    FrameNoBegins,
    FrameNoEnds,
    NotValidFrame,
    NoFrameDecoding, 
}
// static STUFF_BYTES: bool = true;
impl FileFrame {
    fn new(
        // starting_delimiter: Option<Vec<u8>>,
        // ending_delimiter: Option<Vec<u8>>,
        direction: Direction,
        operation: Operation,
        codec: Codec,
        chunking_status: ChunkingStatus,
        //escape_byte: Option<u8>
    ) -> Self {
        FileFrame {
            direction: Some(direction),
            operation: Some(operation),
            codec: Some(codec),
            chunking_status: Some(chunking_status),
            file_chunks: Vec::new(),
            remainder: Vec::new(),
            collect_buffer: true,
            complete_frame: false,
        }
    }
    fn decode(&self, bytes: Vec<u8>) -> Result<(), FileFrameStatus> {
        todo!()
    }
    fn append_bytes_send(&mut self, bytes: Vec<u8>) {
        self.file_chunks.extend(bytes);
    }
    fn flush(&mut self) {
        self.file_chunks = Vec::new();
    }
    //&mut
    //previous_bytes: &mut Vec<u8>,

    //&mut
}

pub trait StreamReceiver {
    //type FrameOutput: FrameHandler;
    type FrameOutput: FrameHandler<FrameOutput = Self::FrameOutput>;
    fn create_frame_handler(&self) -> Self::FrameOutput;
    async fn get_chunk(&self) -> Result<Vec<u8>, FileStreamError>;
}

#[derive(Debug)]
pub enum FileStreamError {
    Disconnect,
}

pub struct TcpFsReceiver {
    // tx: broadcast::Sender<Vec<u8>>,
    // rx: broadcast::Receiver<Vec<u8>>,
    tx: flume::Sender<Vec<u8>>,
    rx: flume::Receiver<Vec<u8>>,
    start_delimiter: Option<Vec<u8>>,
    end_delimiter: Option<Vec<u8>>,
    escape_byte: Option<u8>,
}
impl StreamReceiver for TcpFsReceiver {
    type FrameOutput = FrameEncoder;
    fn create_frame_handler(&self) -> FrameEncoder {
        FrameEncoder::new(self.start_delimiter.clone(), self.end_delimiter.clone(), self.escape_byte)
    }
    async fn get_chunk(&self) -> Result<Vec<u8>, FileStreamError> {
        match self.rx.recv_async().await {
            Ok(byes) => Ok(byes),
            Err(_) => Err(FileStreamError::Disconnect),
        }
    }
}
impl TcpFsReceiver {
    pub fn new(tx: flume::Sender<Vec<u8>>, rx: flume::Receiver<Vec<u8>>) -> TcpFsReceiver {
        TcpFsReceiver {
            tx,
            rx,
            start_delimiter: None,
            end_delimiter: None,
            escape_byte: None,
        }
    }
    pub fn set_start_delimiter(&mut self, delim: Vec<u8>) {
        self.start_delimiter = Some(delim);
    }
    pub fn set_end_delimiter(&mut self, delim: Vec<u8>) {
        self.end_delimiter = Some(delim);
    }
    pub fn set_escape_byte(&mut self, escape_byte: u8) {
        self.escape_byte = Some(escape_byte);
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
            escape_byte: self.escape_byte.clone(),
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
            escape_byte: None,
        }
    }
}

pub struct TcpFsSender {
    tx: flume::Sender<Vec<u8>>,
    rx: flume::Receiver<Vec<u8>>,
    byte_array: Vec<u8>,
    start_delimiter: Option<Vec<u8>>,
    end_delimiter: Option<Vec<u8>>,
    escape_byte: Option<u8>,
}
impl Default for TcpFsSender {
    fn default() -> Self {
        let (tx, rx) = flume::unbounded();
        Self { tx, rx, byte_array: Default::default(), start_delimiter: Default::default(), end_delimiter: Default::default(), escape_byte: Default::default() }
    }
}
pub trait StreamSender {
    async fn encode_frame<S, W>(&self, frame: S) -> Result<Vec<u8>, FileFrameStatus>
    where
        S: Handle<W>;
    async fn send(&mut self, bytes: Vec<u8>);
}

impl StreamSender for TcpFsSender {
    async fn encode_frame<S, W>(&self, frame: S) -> Result<Vec<u8>, FileFrameStatus>
    where
        S: Handle<W>,
    {
        frame.to_bytes(
            self.escape_byte,
            self.start_delimiter.clone(),
            self.end_delimiter.clone(),
        )
    }
    async fn send(&mut self, bytes: Vec<u8>) {
        let res = self.tx.send_async(bytes).await;
        println!("{:#?}", res);
    }
}
impl TcpFsSender {
    pub fn new(rx: flume::Receiver<Vec<u8>>, tx: flume::Sender<Vec<u8>>) -> TcpFsSender {
        TcpFsSender {
            tx,
            rx,
            byte_array: Vec::new(),
            start_delimiter: None,
            end_delimiter: None,
            escape_byte: None,
        }
    }
    pub fn set_start_delimiter(&mut self, delim: Vec<u8>) {
        self.start_delimiter = Some(delim);
    }
    pub fn set_end_delimiter(&mut self, delim: Vec<u8>) {
        self.end_delimiter = Some(delim);
    }
    pub fn set_escape_byte(&mut self, escape_byte: u8) {
        self.escape_byte = Some(escape_byte);
    }
    pub fn feed_bytes(&mut self, bytes: Vec<u8>) {
        self.byte_array.extend(bytes);
    }
    // pub async fn get_chunk(&self) -> Result<Vec<u8>, FileStreamError> {
    //     match self.rx.recv_async().await {
    //         Ok(byes) => Ok(byes),
    //         Err(_) => Err(FileStreamError::Disconnect),
    //     }
    // }

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

pub enum StreamableFileSystemErrors {
    None
}

pub struct RemoteFileSystem<S> {
    state: S,
    local_state: HashMap<String, LocalState>,
    direction: Direction,
    operation: Operation,
    codec: Codec,
    files: Vec<File>,
    file_handle: Option<StdFile>,
    remainder: Vec<u8>, 
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
            file_handle: None,
            remainder: Vec::new(), 
        }
    }
}

#[derive(Debug)]
pub struct LocalState {
    pub location: String,
}

impl <S: Default>RemoteFileSystem<S>{
    pub fn new(state: S) -> Self {
        RemoteFileSystem {
            state,
            ..Default::default()
        }
    }
    pub fn inner_mut(&mut self) -> &mut S {
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
 
    pub fn set_operation(
        &mut self,
        operation: Operation,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.operation = operation;
        Ok(())
    }
    pub fn clear_files(&mut self) {
        self.files = vec![];
    }
    pub fn append_files(&mut self, file: File) {
        self.files.push(file);
    }
}
impl <S: Default + StreamReceiver>RemoteFileSystem<S> {

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
    pub async fn receive_operation(&mut self, fs_state_name: String) -> Result<(), StreamableFileSystemErrors>  {
        println!("receiving in here");
        let new_location = "/home/spiderunderurbed/projects/tcp_fs_poc/output.jar";
        let mut file_handle: Option<std::fs::File> = None;

        let mut temp_handle = std::fs::OpenOptions::new().append(true).open(new_location);
        if let Err(_) = temp_handle {
            let _ = std::fs::File::create(
                new_location,
            );
            temp_handle = std::fs::OpenOptions::new().append(true).open(new_location)
        }
        file_handle = Some(temp_handle.unwrap());
        let remainder = &mut 0;

        loop {
            match self.state.get_chunk().await {
                Ok(bytes) => {
                    let mut total_bytes = Vec::new();
                    total_bytes.extend(self.remainder.clone());
                    self.remainder = Vec::new();
                    total_bytes.extend(bytes);
                    let mut frame = self.state.create_frame_handler();
                    frame.set_chunks(self.remainder.clone());
                    // let mut frame = FrameEncoder::default();
                    // frame.remainder = self.remainder.clone();
                    // frame.starting_delimiter = self.state.start_delimiter.clone();
                    // frame.ending_delimiter = self.state.end_delimiter.clone();
                    
                    match frame.append_bytes_recv(&total_bytes, remainder) {
                        Ok(frames) => {
                            for frame in &frames {
                                if let Some(mut handle) = file_handle.take() {
                                    //let owned_frame: S::FrameOutput = frame.to_frame_output();
                                    let chunks = &frame.get_chunks()[4..frame.get_chunks().len()];
                                    let _ = handle.write_all(chunks);
                                    let _ = handle.flush();
                                    let _ = handle.sync_all();
                                    file_handle = Some(handle);
                                } else {
                                    println!("do not have handle");
                                }
                            }
                            if let Some(last_frame) = frames.iter().last() {
                                self.remainder.extend(last_frame.get_remainder().clone());
                            }
                        }
                        Err(e) => match e {
                            FileFrameStatus::NotValidFrame => {}
                            FileFrameStatus::NoFrameDecoding => {}
                            FileFrameStatus::FrameNoBegins => {
                            }
                            FileFrameStatus::FrameNoEnds => {
                                let remainder = total_bytes;
                                self.remainder = remainder.to_vec();
                                println!("this frame does not end");
                            }
                        },
                    }
                }
                Err(e) => {
                    break Ok(());
                }
            }
        }
    }
}


impl <S: StreamSender + Default>RemoteFileSystem<S> 
// where S: StreamSender
{

    pub async fn execute_operation(
        &mut self,
        fs_state_name: String,
    ) -> Result<(), StreamableFileSystemErrors> {
        match self.operation {
            Operation::Move => {
               // return Box::pin(async move {
                    let mut files = std::mem::take(&mut self.files);
                    for mut file in files.drain(..) {
                        self.operation = Operation::Set;
                        let _ = Box::pin(self.execute_operation(file.final_location.clone())).await;
                        self.operation = Operation::Move;
                        match self.codec {
                            Codec::Raw => {
                                todo!()
                            }
                            // Codec::RawContinues => {
                            // }
                            Codec::Multipart | Codec::RawContinues => {
                                loop {
                                    let mut file_content_stream =
                                        file.content_stream.take().unwrap();

                                    match file_content_stream.recv_async().await {
                                        Ok(bytes) => {
                                            println!("got a bunch of bytes");
                                            for chunk in bytes.chunks(4096) {
                                                let mut frame = FileFrame::new(
                                                    self.direction.clone(),
                                                    self.operation.clone(),
                                                    self.codec.clone(),
                                                    ChunkingStatus::Continues,
                                                    // Some(ESCAPE_BYTE)
                                                );
                                                frame.append_bytes_send(chunk.to_vec());


                                                if let Ok(bytes) =
                                                    self.state.encode_frame(frame.clone()).await
                                                {
                                                    println!(
                                                        "{:?}",
                                                        bytes[0..bytes.len().clamp(0, 10)].to_vec()
                                                    );
                                                    self.state.send(bytes).await;
                                                    println!("sent the bytes");
                                                    // frame.flush();
                                                    // break;
                                                } else {
                                                    println!("err");
                                                }
                                            }
                                            //frame.flush();
                                            //}
                                        }
                                        Err(_) => {
                                            //println!("disconnect");
                                            // match e {
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
                                            // flume::RecvError::Disconnected => {
                                            //     //println!("disconnected");
                                            // }
                                            // }
                                        }
                                    }
                                    file.content_stream = Some(file_content_stream);
                                }
                            }
                            _ => return  Err(StreamableFileSystemErrors::None),
                        }
                    }
                    Ok(())
                //});
            }
            Operation::None => Err(StreamableFileSystemErrors::None),
            //Err("unimplimented".into()),
            Operation::Set => {
                //return Box::pin(async move {
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
                //});
                //return Box::pin(async { Err("unimplimented".into()) })
            }
            Operation::Ls => {
                return Err(StreamableFileSystemErrors::None);
                // return Box::pin(async move {
                    //todo!()
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
                //});
            }
        }
    }
    // pub fn has_operation(&self) -> bool {
    //     true
    // }
}
