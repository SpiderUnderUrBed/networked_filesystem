use async_trait::async_trait;
use futures::Stream;
use futures::StreamExt;
use futures::future::{Either, pending};
use futures::io::Cursor;
use num_enum::TryFromPrimitive;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use std::fs::File;
use std::fs::File as StdFile;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;

use std::{
    collections::{HashMap, VecDeque},
    error::Error,
    fmt::Debug,
    io::Write,
    sync::Arc,
    vec,
};

use tokio::sync::watch;

// pub use flume_delmited_v1::*;
// use length_delimited_v2::*;

use crate::flume_delmited_v1::flume_delimited::ConvertWithDelims;
use crate::length_delimited_v2::length_delimited::ConvertWithLength;
use crate::length_delimited_v2::length_delimited::WithLength;

pub mod chain;
mod delimited_commons;
pub mod flume_delmited_v1;
pub mod length_delimited_v2;

// use flume_delmited_v1::flume_delimited::*;
#[derive(Clone, TryFromPrimitive, Debug, PartialEq)]
#[repr(u8)]
pub enum Direction {
    Local = 0,
    Server = 1,
    Unknown = 2,
}

#[derive(Clone, TryFromPrimitive, Debug, PartialEq)]
#[repr(u8)]
pub enum Operation {
    Move = 0,
    Set = 1,
    Ls = 2,
    Eof = 3,
    Drain = 4,
    None = 5,
    Acknowlage = 6,
    Cursor = 7,
    Error = 9
}

#[derive(Clone, TryFromPrimitive, Debug)]
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

pub trait Decodable {
    type Output: std::fmt::Debug + FrameCommons;
    // async fn handle<S, F>(output: Self::Output, state_id: u8, fs: &mut RemoteFileSystem<S, F>) -> Result<(), FileHandleStatus>;
    fn decode(bytes: Vec<u8>) -> Result<Self::Output, FileFrameStatus>;
}
pub trait FrameCommons {
    fn get_direction(&self) -> Result<Direction, FileFrameStatus>;
    fn get_operation(&self) -> Result<Operation, FileFrameStatus>;
}

pub trait FrameHandler {
    type FrameOutput: FrameCommons;
    fn encode_bytes(&self, headers: Vec<u8>, content: Vec<u8>) -> Vec<u8>;
    fn append_bytes_recv(
        &mut self,
        bytes: &Vec<u8>,
        _: &mut u64,
    ) -> Result<VecDeque<Self::FrameOutput>, FileFrameStatus>;
    // fn set_remainder(&mut self, remainder: Vec<u8>);
    fn set_chunks(&mut self, chunks: Vec<u8>);
    fn get_remainder(&self) -> Vec<u8>;
    fn get_chunks(&self) -> Vec<u8>;
}



pub trait Convert<W> {
    fn to_bytes(&self, options: W) -> Result<Vec<u8>, FileFrameStatus>;
}


#[derive(Default, Clone, Debug)]
pub struct FileFrame {
    direction: Option<Direction>,
    operation: Option<Operation>,
    codec: Option<Codec>,
    pub chunking_status: Option<ChunkingStatus>,
    pub chunks: Vec<u8>,
}
impl FrameCommons for FileFrame {
    fn get_direction(&self) -> Result<Direction, FileFrameStatus> {
        Ok(self.direction.clone().unwrap())
    }
    fn get_operation(&self) -> Result<Operation, FileFrameStatus> {
        Ok(self.operation.clone().unwrap())
    }
}

#[derive(Debug)]
pub enum TransportRecvError {
    Disconnected,
    Lagged(usize),
    NoStream,
}

#[derive(Debug)]
pub enum FileHandleStatus {
    NoStateGiven,
    IncorrectData,
    IncorrectStateAsked,
    Any(Box<dyn Error + Send + Sync>),
}

#[derive(Debug)]
pub enum FileFrameStatus {
    FrameNoBegins,
    FrameNoEnds,
    NotValidFrame,
    NotCorrectFrame,
    NoFrameDecoding,
}
impl FileFrame {
    fn new(
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
            chunks: Vec::new(),
        }
    }
    pub fn write_at_location<S, F>(
        &mut self,
        fs: &mut RemoteFileSystem<S, F>,
        location: String,
        starting_cursor_position: Option<u64>,
        ending_cursor_position: Option<u64>
    ) -> Result<(), FileHandleStatus> {
        if fs.file_handle.is_none() {
            let mut temp_handle = std::fs::OpenOptions::new()
                .truncate(true)
                .append(true)
                .open(&location);
            if let Err(e) = temp_handle {
                let _ = std::fs::File::create(&location);
                temp_handle = std::fs::OpenOptions::new().append(true).open(location)
            }
            fs.file_handle = Some(temp_handle.unwrap());
        }
        let mut handle = fs.file_handle.take().unwrap();
        let _ = handle.write_all(&self.chunks);
        let _ = handle.flush();
        let _ = handle.sync_all();
        fs.file_handle = Some(handle);
        Ok(())
    }
    pub fn handle<S, F>(
        mut output: FileFrame,
        state_id: u8,
        fs: &mut RemoteFileSystem<S, F>,
    ) -> Result<(), FileHandleStatus> {
        if let Some(state) = fs.local_state.get(&state_id) {
            let location = &state.location;
            let starting_cursor_position = state.starting_cursor_position;
            let ending_cursor_position = state.ending_cursor_position;
            output.write_at_location(fs, location.to_string(), starting_cursor_position, ending_cursor_position)?;
        } else {
            return Err(FileHandleStatus::NoStateGiven);
        }

        Ok(())
    }
    fn append_bytes_send(&mut self, bytes: Vec<u8>) {
        self.chunks.extend(bytes);
    }
    fn flush(&mut self) {
        self.chunks = Vec::new();
    }
}
impl Decodable for FileFrame {
    type Output = FileFrame;

    fn decode(mut bytes: Vec<u8>) -> Result<FileFrame, FileFrameStatus> {
        let mut frame = FileFrame::default();
        if let Some(byte) = bytes.get(0) {
            if let Ok(direction) = Direction::try_from_primitive(*byte) {
                frame.direction = Some(direction);
            } else {
                return Err(FileFrameStatus::NotValidFrame);
            }
            bytes.remove(0);
        } else {
            return Err(FileFrameStatus::NotValidFrame);
        }
        if let Some(byte) = bytes.get(0) {
            if let Ok(operation) = Operation::try_from_primitive(*byte) {
                if !matches!(operation, Operation::Move) {
                    return Err(FileFrameStatus::NotCorrectFrame);
                }
                frame.operation = Some(operation);
            } else {
                return Err(FileFrameStatus::NotValidFrame);
            }
            bytes.remove(0);
        } else {
            return Err(FileFrameStatus::NotValidFrame);
        }
        if let Some(byte) = bytes.get(0) {
            if let Ok(codec) = Codec::try_from_primitive(*byte) {
                frame.codec = Some(codec);
            } else {
                return Err(FileFrameStatus::NotValidFrame);
            }
            bytes.remove(0);
        } else {
            return Err(FileFrameStatus::NotValidFrame);
        }
        if let Some(byte) = bytes.get(0) {
            if let Ok(continues) = ChunkingStatus::try_from_primitive(*byte) {
                frame.chunking_status = Some(continues);
            } else {
                return Err(FileFrameStatus::NotValidFrame);
            }
            bytes.remove(0);
        } else {
            return Err(FileFrameStatus::NotValidFrame);
        }
        frame.chunks.extend(bytes);
        Ok(frame)
    }
}

pub trait StateDelims {
    fn get_escape_byte(&self) -> Option<u8>;
    fn get_starting_delims(&self) -> Option<Vec<u8>>;
    fn get_ending_delims(&self) -> Option<Vec<u8>>;
}

#[async_trait]
pub trait StreamReceiver {
    //type FrameOutput: FrameHandler;
    type FrameOutput: FrameHandler<FrameOutput = Self::FrameOutput> + FrameCommons;
    fn create_frame_handler(&self) -> Self::FrameOutput;
    async fn recv(&mut self) -> Result<Vec<u8>, FileStreamError>;
}

#[derive(Debug)]
pub enum FileStreamError {
    Disconnect,
}

#[async_trait]
pub trait StreamSender {
    type Opts;
    async fn encode_frame<S>(&self, frame: S) -> Result<Vec<u8>, FileFrameStatus>
    where
        S: Convert<Self::Opts> + Send;
    async fn send(&mut self, bytes: Vec<u8>) -> Result<(), Box<dyn Error + Send + Sync>>;
}


pub trait BidirectionalStream: StreamSender + StreamReceiver {}
// pub trait BidirectionalStream: StreamSender {
//     type FrameOutput: FrameHandler<FrameOutput = Self::FrameOutput>;
//     // async fn encode_frame<S, W>(&self, frame: S) -> Result<Vec<u8>, FileFrameStatus>
//     // where
//     //     S: Handle<W>;
//     // async fn send(&mut self, bytes: Vec<u8>);
//     fn create_frame_handler(&self) -> Self::FrameOutput;
//     async fn get_chunk(&self) -> Result<Vec<u8>, FileStreamError>;
// }

pub trait FileSender {
    async fn recv(&mut self) -> Result<Vec<u8>, TransportRecvError>;
    fn get_location(&self) -> String;
    fn get_state(&self) -> u8;
}

#[derive(Debug)]
pub enum StreamableFileSystemErrors {
    None,
    NoStateGiven,
    IncorrectStateAsked,
    IncorrectData,
    Unknown,
    // NoFrameFound<S>(VecDeque<<S as StreamReceiver>::FrameOutput>),
    NoFrameFound(Vec<u8>),
    TransportRecvError(TransportRecvError),
    Any(Box<dyn Error + Send + Sync>),
}

#[derive(Debug, Default, Clone)]
pub struct LocalState {
    pub location: String,
    pub starting_cursor_position: Option<u64>,
    pub ending_cursor_position: Option<u64>,
}

pub struct RemoteFileSystem<S, F> {
    state: S,
    local_state: HashMap<u8, LocalState>,
    direction: Direction,
    operation: Operation,
    unique_operation_event: (watch::Sender<Operation>, watch::Receiver<Operation>),
    codec: Codec,
    files: Vec<F>,
    file_handle: Option<StdFile>,
    remainder: Vec<u8>,
}
impl<S: Default, F> Default for RemoteFileSystem<S, F> {
    fn default() -> Self {
        let (watch_tx, watch_rx) = watch::channel(Operation::None);
        Self {
            state: S::default(),
            local_state: HashMap::new(),
            direction: Direction::Unknown,
            operation: Operation::None,
            codec: Codec::Unknown,
            files: Vec::new(),
            file_handle: None,
            remainder: Vec::new(),
            unique_operation_event: (watch_tx, watch_rx),
        }
    }
}
impl<S: Clone, F: Clone> Clone for RemoteFileSystem<S, F> {
    fn clone(&self) -> Self {
        let (watch_tx, watch_rx) = watch::channel(Operation::None);
        Self {
            state: self.state.clone(),
            local_state: self.local_state.clone(),
            direction: self.direction.clone(),
            operation: self.operation.clone(),
            codec: self.codec.clone(),
            files: self.files.clone(),
            file_handle: None,
            remainder: self.remainder.clone(),
            unique_operation_event: (watch_tx, watch_rx),
        }
    }
}

impl<S: Default, F> RemoteFileSystem<S, F> {
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
    pub fn get_state(&self) -> &S {
        &self.state
    }
    pub fn set_direction(&mut self, direction: Direction) {
        self.direction = direction;
    }
    pub fn get_direction(&self) -> Direction {
        self.direction.clone()
    }
    pub fn create_state(&mut self, state_id: u8, state: LocalState) {
        self.local_state.insert(state_id, state);
    }
    pub fn remove_state(&mut self, state_id: u8) {
        self.local_state.remove(&state_id);
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
    pub fn append_files(&mut self, file: F) {
        self.files.push(file);
    }
    pub fn get_operation_event(&self) -> watch::Receiver<Operation> {
        self.unique_operation_event.1.clone()
    }
}
impl<S: Default + StreamReceiver, F> RemoteFileSystem<S, F> {
    pub async fn receive_operation(
        &mut self,
        fs_state_id: u8,
    ) -> Result<(), StreamableFileSystemErrors> 
        where <S as StreamReceiver>::FrameOutput: Debug
    {
        let remainder = &mut 0;

        loop {
            match self.state.recv().await {
                Ok(bytes) => {
                    let mut total_bytes = Vec::new();
                    total_bytes.extend(self.remainder.clone());
                    self.remainder = Vec::new();
                    total_bytes.extend(bytes);
                    let mut frame = self.state.create_frame_handler();
                    frame.set_chunks(self.remainder.clone());

                    match frame.append_bytes_recv(&total_bytes, remainder) {
                        Ok(frames) => {
                            for frame in &frames {
                                if let Ok(direction) = frame.get_direction() {
                                    if direction == self.direction {
                                        continue;
                                    }
                                } else {
                                    continue;
                                }
                                let chunks = frame.get_chunks();
                                if let Ok(file_frame) = FileFrame::decode(chunks.clone()) {
                                    FileFrame::handle(file_frame, fs_state_id, self)
                                        .map_err(|e| match e {
                                            FileHandleStatus::NoStateGiven => {
                                                StreamableFileSystemErrors::NoStateGiven
                                            }
                                            FileHandleStatus::IncorrectData => {
                                                StreamableFileSystemErrors::IncorrectData
                                            }
                                            FileHandleStatus::IncorrectStateAsked => {
                                                StreamableFileSystemErrors::IncorrectStateAsked
                                            }
                                            FileHandleStatus::Any(e) => {
                                                StreamableFileSystemErrors::Any(e)
                                            }
                                        })?;
                                } else if let Ok(set_frame) = SetFrame::decode(chunks.clone()) {
                                    SetFrame::handle(set_frame, fs_state_id, self)
                                        .map_err(|e| match e {
                                            FileHandleStatus::NoStateGiven => {
                                                StreamableFileSystemErrors::NoStateGiven
                                            }
                                            FileHandleStatus::IncorrectData => {
                                                StreamableFileSystemErrors::IncorrectData
                                            }
                                            FileHandleStatus::IncorrectStateAsked => {
                                                StreamableFileSystemErrors::IncorrectStateAsked
                                            }
                                            FileHandleStatus::Any(e) => {
                                                StreamableFileSystemErrors::Any(e)
                                            }
                                        })?;
                                } else if let Ok(eof_frame) = EofFrame::decode(chunks.clone()) {
                                    EofFrame::handle(eof_frame, fs_state_id, self)
                                        .map_err(|e| match e {
                                            FileHandleStatus::NoStateGiven => {
                                                StreamableFileSystemErrors::NoStateGiven
                                            }
                                            FileHandleStatus::IncorrectData => {
                                                StreamableFileSystemErrors::IncorrectData
                                            }
                                            FileHandleStatus::IncorrectStateAsked => {
                                                StreamableFileSystemErrors::IncorrectStateAsked
                                            }
                                            FileHandleStatus::Any(e) => {
                                                StreamableFileSystemErrors::Any(e)
                                            }
                                        })?;
                                } else if let Ok(cursor_frame) = CursorFrame::decode(chunks.clone()) {
                                    CursorFrame::handle(cursor_frame, fs_state_id, self)
                                        .map_err(|e| match e {
                                            FileHandleStatus::NoStateGiven => {
                                                StreamableFileSystemErrors::NoStateGiven
                                            }
                                            FileHandleStatus::IncorrectData => {
                                                StreamableFileSystemErrors::IncorrectData
                                            }
                                            FileHandleStatus::IncorrectStateAsked => {
                                                StreamableFileSystemErrors::IncorrectStateAsked
                                            }
                                            FileHandleStatus::Any(e) => {
                                                StreamableFileSystemErrors::Any(e)
                                            }
                                        })?;
                                } else if let Ok(_) = ErrorFrame::decode(chunks){
                                    return Err(StreamableFileSystemErrors::Unknown);
                                }

                            }
                            if let Some(last_frame) = frames.iter().last() {
                                self.remainder.extend(last_frame.get_remainder().clone());
                            }
                        }
                        Err(e) => match e {
                            FileFrameStatus::NotValidFrame => {}
                            FileFrameStatus::NoFrameDecoding => {}
                            FileFrameStatus::FrameNoBegins => {}
                            FileFrameStatus::FrameNoEnds => {
                                let remainder = total_bytes;
                                self.remainder = remainder.to_vec();
                            }
                            FileFrameStatus::NotCorrectFrame => {}
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

impl<S: StreamSender + Default, F: FileSender> RemoteFileSystem<S, F> {
    pub async fn send_file(&mut self, mut file: F) -> Result<(), StreamableFileSystemErrors> 
        where
            FileFrame: Convert<S::Opts>,
            SetFrame: Convert<S::Opts>,
            EofFrame: Convert<S::Opts>,
            CursorFrame: Convert<S::Opts>
    {
        match self.codec {
            Codec::Multipart => {
                todo!()
            }
            Codec::Raw | Codec::RawContinues => {
                loop {
                    match file.recv().await {
                        Ok(bytes) => {
                            for chunk in bytes.chunks(4096) {
                                let mut frame = FileFrame::new(
                                    self.direction.clone(),
                                    Operation::Move,
                                    self.codec.clone(),
                                    ChunkingStatus::Continues,
                                    // Some(ESCAPE_BYTE)
                                );
                                frame.append_bytes_send(chunk.to_vec());

                                if let Ok(bytes) = self.state.encode_frame(frame.clone()).await {
                                    if let Err(e) = self.state.send(bytes).await {
                                        println!("{:#?}", e);
                                    }
                                }
                            }
                        }
                        Err(e) => return Err(StreamableFileSystemErrors::TransportRecvError(e)),
                    }
                }
            }
            _ => return Err(StreamableFileSystemErrors::None),
        }
    }
    pub async fn set_location(
        &mut self,
        state_id: u8,
        location: String,
    ) -> Result<(), StreamableFileSystemErrors> 
        where 
            FileFrame: Convert<S::Opts>,
            SetFrame: Convert<S::Opts>,
            EofFrame: Convert<S::Opts>,
    {
        let frame = SetFrame::new(
            Some(self.direction.clone()),
            state_id,
            location.as_bytes().to_vec(),
        );
        if let Ok(bytes) = self.state.encode_frame(frame.clone()).await {
            if let Err(e) = self.state.send(bytes).await {
                println!("{:#?}", e);
            }
        }
        Ok(())
    }
    pub async fn execute_operation(
        &mut self,
        state_id: u8,
    ) -> Result<(), StreamableFileSystemErrors> 
        where 
            FileFrame: Convert<S::Opts>,
            SetFrame: Convert<S::Opts>,
            EofFrame: Convert<S::Opts>,
            CursorFrame: Convert<S::Opts>
    {
        let _ = self.unique_operation_event.0.send(self.operation.clone());
        match self.operation {
            Operation::Move => {
                let (extracted, kept): (Vec<F>, Vec<F>) = std::mem::take(&mut self.files)
                    .into_iter()
                    .partition(|f| f.get_state() == state_id);

                self.files = kept;

                for file in extracted {
                    self.operation = Operation::Set;
                    if let Some(state) = self.local_state.get_mut(&state_id) {
                        state.location = file.get_location();
                    } else {
                        return Err(StreamableFileSystemErrors::NoStateGiven);
                    }
                    let _ = Box::pin(self.execute_operation(state_id)).await;
                    self.operation = Operation::Move;
                    if let Err(e) = self.send_file(file).await {
                        if matches!(
                            e,
                            StreamableFileSystemErrors::TransportRecvError(
                                TransportRecvError::Disconnected
                            )
                        ) {
                            self.operation = Operation::Eof;
                            let _ = Box::pin(self.execute_operation(state_id)).await;
                        }
                    }
                }
                Ok(())
            }
            Operation::None => Err(StreamableFileSystemErrors::None),
            Operation::Set => {
                if let Some(state) = self.local_state.get(&state_id) {
                    self.set_location(state_id, state.location.clone()).await?;
                    Ok(())
                } else {
                    Err(StreamableFileSystemErrors::NoStateGiven)
                }
            }
            Operation::Ls => {
                return Err(StreamableFileSystemErrors::None);
            }
            Operation::Eof => {
                let frame = EofFrame::new(Some(self.direction.clone()));
                if let Ok(bytes) = self.state.encode_frame(frame.clone()).await {
                    if let Err(e) = self.state.send(bytes).await {
                        println!("{:#?}", e);
                    }
                }
                Ok(())
            }
            Operation::Cursor => {
                if let Some(state) = self.local_state.get(&state_id) {
                    let frame = CursorFrame::new(Some(self.direction.clone()), state.starting_cursor_position, state.ending_cursor_position);
                    if let Ok(bytes) = self.state.encode_frame(frame.clone()).await {
                        if let Err(e) = self.state.send(bytes).await {
                            println!("{:#?}", e);
                        }
                    }
                    Ok(())
                } else {
                    Err(StreamableFileSystemErrors::NoStateGiven)
                }
            },
            Operation::Drain => return Err(StreamableFileSystemErrors::None),
            Operation::Acknowlage => return Err(StreamableFileSystemErrors::None),
            Operation::Error => return Err(StreamableFileSystemErrors::None),
        }
    }
}

#[async_trait]
pub trait ChannelSend<T: Send + 'static> {
    type Error: std::fmt::Debug;

    async fn send(&self, item: T) -> Result<(), Self::Error>;
}

#[async_trait]
impl<T: Send + 'static> ChannelSend<T> for std::sync::mpsc::Sender<T> {
    type Error = std::sync::mpsc::SendError<T>;

    async fn send(&self, item: T) -> Result<(), Self::Error> {
        self.send(item)
    }
}

#[async_trait]
impl<T: Send + 'static> ChannelSend<T> for tokio::sync::mpsc::Sender<T> {
    type Error = tokio::sync::mpsc::error::SendError<T>;

    async fn send(&self, item: T) -> Result<(), Self::Error> {
        self.send(item).await
    }
}

#[async_trait]
impl<T: Send + 'static> ChannelSend<T> for tokio::sync::mpsc::UnboundedSender<T> {
    type Error = tokio::sync::mpsc::error::SendError<T>;

    async fn send(&self, item: T) -> Result<(), Self::Error> {
        self.send(item)
    }
}

#[async_trait]
impl<T: Send + Clone + 'static + Debug> ChannelSend<T> for tokio::sync::broadcast::Sender<T> {
    type Error = tokio::sync::broadcast::error::SendError<T>;

    async fn send(&self, item: T) -> Result<(), Self::Error> {
        self.send(item).map(|_| ())
    }
}

#[async_trait]
impl<T: Send + 'static> ChannelSend<T> for flume::Sender<T> {
    type Error = flume::SendError<T>;

    async fn send(&self, item: T) -> Result<(), Self::Error> {
        self.send_async(item).await
    }
}

impl<S, F> RemoteFileSystem<S, F>
where
    S: BidirectionalStream + Default + Send,
    F: FileSender + Send,
{
    pub async fn receive_operation_bidirectionally<W>(
        &mut self,
        fs_state_id: u8,
        mut rx: Option<&mut (dyn Stream<Item = Vec<u8>> + Unpin + Send)>,
    ) -> Result<(), StreamableFileSystemErrors>
    where
        S: FrameWrite<W, FileFrame> + FrameWrite<W, EofFrame>,
    {
        let remainder = &mut 0;

        loop {
            // let rx_ref = rx.as_deref_mut();
            match self.state.recv().await {
                Ok(bytes) => {
                    let mut total_bytes = Vec::new();
                    total_bytes.extend(self.remainder.clone());
                    self.remainder = Vec::new();
                    total_bytes.extend(bytes);
                    let mut frame = self.state.create_frame_handler();
                    frame.set_chunks(self.remainder.clone());

                    match frame.append_bytes_recv(&total_bytes, remainder) {
                        Ok(frames) => {
                            for frame in &frames {
                                if let Ok(direction) = frame.get_direction() {
                                    if direction == self.direction {
                                        continue;
                                    }
                                } else {
                                    continue;
                                }
                                let chunks = frame.get_chunks();
                                if let Ok(file_frame) = FileFrame::decode(chunks.clone()) {
                                    FileFrame::handle(file_frame, fs_state_id, self)
                                        .map_err(|e| match e {
                                            FileHandleStatus::NoStateGiven => {
                                                StreamableFileSystemErrors::NoStateGiven
                                            }
                                            FileHandleStatus::IncorrectData => {
                                                StreamableFileSystemErrors::IncorrectData
                                            }
                                            FileHandleStatus::IncorrectStateAsked => {
                                                StreamableFileSystemErrors::IncorrectStateAsked
                                            }
                                            FileHandleStatus::Any(e) => {
                                                StreamableFileSystemErrors::Any(e)
                                            }
                                        })?;
                                } else if let Ok(set_frame) = SetFrame::decode(chunks.clone()) {
                                    SetFrame::handle(set_frame, fs_state_id, self)
                                        .map_err(|e| match e {
                                            FileHandleStatus::NoStateGiven => {
                                                StreamableFileSystemErrors::NoStateGiven
                                            }
                                            FileHandleStatus::IncorrectData => {
                                                StreamableFileSystemErrors::IncorrectData
                                            }
                                            FileHandleStatus::IncorrectStateAsked => {
                                                StreamableFileSystemErrors::IncorrectStateAsked
                                            }
                                            FileHandleStatus::Any(e) => {
                                                StreamableFileSystemErrors::Any(e)
                                            }
                                        })?;
                                } else if let Ok(eof_frame) = EofFrame::decode(chunks.clone()) {
                                    EofFrame::handle(eof_frame, fs_state_id, self)
                                        .map_err(|e| match e {
                                            FileHandleStatus::NoStateGiven => {
                                                StreamableFileSystemErrors::NoStateGiven
                                            }
                                            FileHandleStatus::IncorrectData => {
                                                StreamableFileSystemErrors::IncorrectData
                                            }
                                            FileHandleStatus::IncorrectStateAsked => {
                                                StreamableFileSystemErrors::IncorrectStateAsked
                                            }
                                            FileHandleStatus::Any(e) => {
                                                StreamableFileSystemErrors::Any(e)
                                            }
                                        })?;
                                    // return Err(StreamableFileSystemErrors::NoFrameFound(chunks));
                                } else if let Ok(drain_frame) = DrainFrame::decode(chunks.clone()) {
                                    if let Some(rx) = rx.take() {
                                        DrainFrame::handle(drain_frame, fs_state_id, self, rx)
                                            .await
                                            .map_err(|e| match e {
                                                FileHandleStatus::NoStateGiven => {
                                                    StreamableFileSystemErrors::NoStateGiven
                                                }
                                                FileHandleStatus::IncorrectData => {
                                                    StreamableFileSystemErrors::IncorrectData
                                                }
                                                FileHandleStatus::IncorrectStateAsked => {
                                                    StreamableFileSystemErrors::IncorrectStateAsked
                                                }
                                                FileHandleStatus::Any(e) => {
                                                    StreamableFileSystemErrors::Any(e)
                                                }
                                            })?;
                                    }
                                } else if let Ok(cursor_frame) = CursorFrame::decode(chunks.clone()) {
                                    CursorFrame::handle(cursor_frame, fs_state_id, self)
                                        .map_err(|e| match e {
                                            FileHandleStatus::NoStateGiven => {
                                                StreamableFileSystemErrors::NoStateGiven
                                            }
                                            FileHandleStatus::IncorrectData => {
                                                StreamableFileSystemErrors::IncorrectData
                                            }
                                            FileHandleStatus::IncorrectStateAsked => {
                                                StreamableFileSystemErrors::IncorrectStateAsked
                                            }
                                            FileHandleStatus::Any(e) => {
                                                StreamableFileSystemErrors::Any(e)
                                            }
                                        })?;
                                } else if let Ok(_) = ErrorFrame::decode(chunks){
                                    return Err(StreamableFileSystemErrors::Unknown);
                                }
                            }
                            if let Some(last_frame) = frames.iter().last() {
                                self.remainder.extend(last_frame.get_remainder().clone());
                            }
                        }
                        Err(e) => match e {
                            FileFrameStatus::NotValidFrame => {}
                            FileFrameStatus::NoFrameDecoding => {}
                            FileFrameStatus::FrameNoBegins => {}
                            FileFrameStatus::FrameNoEnds => {
                                let remainder = total_bytes;
                                self.remainder = remainder.to_vec();
                            }
                            FileFrameStatus::NotCorrectFrame => {}
                        },
                    }
                }
                Err(e) => {
                    break Ok(());
                }
            }
        }
    }

    pub async fn create_bidirectional_handler(
        this: Arc<tokio::sync::Mutex<Self>>,
        fs_state_id: u8,
        mut rx: Option<&mut (dyn Stream<Item = Vec<u8>> + Unpin + Send)>,
    ) where
        S: FrameWrite<F, FileFrame> + FrameWrite<F, EofFrame>,
        FileFrame: Convert<S::Opts>,
        SetFrame: Convert<S::Opts>,
        EofFrame: Convert<S::Opts>,
        DrainFrame: Convert<S::Opts>,
        AcknowlageFrame: Convert<S::Opts>,
        CursorFrame: Convert<S::Opts>
    {
        loop {
            let sender_future = {
                let this = Arc::clone(&this);
                async move {
                    let mut guard = this.lock().await;
                    guard.execute_operation_bidirectionally(fs_state_id).await
                }
            };

            let receiver_future = if let Some(rx_ref) = rx.as_mut() {
                Either::Left({
                    let this = Arc::clone(&this);
                    async move {
                        let mut guard = this.lock().await;
                        guard
                            .receive_operation_bidirectionally(fs_state_id, Some(rx_ref))
                            .await
                    }
                })
            } else {
                Either::Right(pending())
            };

            tokio::select! {
                send_result = sender_future => {
                    if let Err(e) = send_result {
                        println!("{:#?}", e);
                    }
                }
                receiver_result = receiver_future => {
                    if let Err(e) = receiver_result {
                        println!("{:#?}", e);
                    }
                }
            }
        }
    }
    pub async fn execute_operation_bidirectionally(
        &mut self,
        state_id: u8,
        // mut rx: &impl Iterator<Item = Vec<u8>>,
    ) -> Result<(), StreamableFileSystemErrors> 
        where 
            FileFrame: Convert<S::Opts>,
            SetFrame: Convert<S::Opts>,
            EofFrame: Convert<S::Opts>,
            DrainFrame: Convert<S::Opts>,
            AcknowlageFrame: Convert<S::Opts>,
            CursorFrame: Convert<S::Opts>
    {
        if self.operation == *self.unique_operation_event.1.borrow() {
            return Ok(());
        }
        let _ = self.unique_operation_event.0.send(self.operation.clone());
        match self.operation {
            Operation::Move => {
                // return Box::pin(async move {
                let mut files = std::mem::take(&mut self.files);
                for file in files.drain(..) {
                    self.operation = Operation::Set;
                    if let Some(state) = self.local_state.get_mut(&state_id) {
                        state.location = file.get_location();
                    } else {
                        return Err(StreamableFileSystemErrors::NoStateGiven);
                    }
                    let _ = Box::pin(self.execute_operation(state_id)).await;
                    self.operation = Operation::Move;
                    if let Err(e) = self.send_file(file).await {
                        if matches!(
                            e,
                            StreamableFileSystemErrors::TransportRecvError(
                                TransportRecvError::Disconnected
                            )
                        ) {
                            self.operation = Operation::Eof;
                            let _ = Box::pin(self.execute_operation(state_id)).await;
                        }
                    }
                }
                Ok(())
            }
            Operation::None => Err(StreamableFileSystemErrors::None),
            Operation::Set => {
                if let Some(state) = self.local_state.get(&state_id) {
                    let frame = SetFrame::new(
                        Some(self.direction.clone()),
                        state_id,
                        state.location.as_bytes().to_vec(),
                    );
                    if let Ok(bytes) = self.state.encode_frame(frame.clone()).await {
                        let _ = self.state.send(bytes).await;
                    }
                    Ok(())
                } else {
                    Err(StreamableFileSystemErrors::NoStateGiven)
                }
            }
            Operation::Ls => {
                return Err(StreamableFileSystemErrors::None);
            }
            Operation::Eof => {
                let frame = EofFrame::new(Some(self.direction.clone()));
                if let Ok(bytes) = self.state.encode_frame(frame.clone()).await {
                    let _ = self.state.send(bytes).await;
                }
                Ok(())
            }
            Operation::Drain => {
                let frame = DrainFrame::new(Some(self.direction.clone()));
                if let Ok(bytes) = self.state.encode_frame(frame.clone()).await {
                    let _ = self.state.send(bytes).await;
                }
                Ok(())
            }
            Operation::Acknowlage => {
                let frame = AcknowlageFrame::new(Some(self.direction.clone()));
                if let Ok(bytes) = self.state.encode_frame(frame.clone()).await {
                    let _ = self.state.send(bytes).await;
                }
                Ok(())
            }
            Operation::Cursor => {
                if let Some(state) = self.local_state.get(&state_id) {
                    let frame = CursorFrame::new(Some(self.direction.clone()), state.starting_cursor_position, state.ending_cursor_position);
                    if let Ok(bytes) = self.state.encode_frame(frame.clone()).await {
                        if let Err(e) = self.state.send(bytes).await {
                            println!("{:#?}", e);
                        }
                    }
                    Ok(())
                } else {
                    Err(StreamableFileSystemErrors::NoStateGiven)
                }
            },
            Operation::Error => return Err(StreamableFileSystemErrors::None),
        }
    }
}

#[derive(Clone, Default, Debug)]
pub struct SetFrame {
    direction: Option<Direction>,
    operation: Option<Operation>,
    state_id: u8,
    pub chunks: Vec<u8>,
}
impl FrameCommons for SetFrame {
    fn get_direction(&self) -> Result<Direction, FileFrameStatus> {
        Ok(self.direction.clone().unwrap())
    }
    fn get_operation(&self) -> Result<Operation, FileFrameStatus> {
        Ok(self.operation.clone().unwrap())
    }
}
impl Decodable for SetFrame {
    type Output = SetFrame;

    fn decode(mut bytes: Vec<u8>) -> Result<SetFrame, FileFrameStatus> {
        let mut frame = SetFrame::default();
        if let Some(byte) = bytes.get(0) {
            if let Ok(direction) = Direction::try_from_primitive(*byte) {
                frame.direction = Some(direction);
            } else {
                return Err(FileFrameStatus::NotValidFrame);
            }
            bytes.remove(0);
        } else {
            return Err(FileFrameStatus::NotValidFrame);
        }
        if let Some(byte) = bytes.get(0) {
            if let Ok(operation) = Operation::try_from_primitive(*byte) {
                if !matches!(operation, Operation::Set) {
                    return Err(FileFrameStatus::NotCorrectFrame);
                }
                frame.operation = Some(operation);
            } else {
                return Err(FileFrameStatus::NotValidFrame);
            }
            bytes.remove(0);
        } else {
            return Err(FileFrameStatus::NotValidFrame);
        }
        if let Some(byte) = bytes.get(0) {
            frame.state_id = *byte;
            bytes.remove(0);
        } else {
            return Err(FileFrameStatus::NotValidFrame);
        }
        frame.chunks = bytes;
        Ok(frame)
    }
}

impl SetFrame {
    fn new(direction: Option<Direction>, state_id: u8, chunks: Vec<u8>) -> SetFrame {
        SetFrame {
            direction,
            operation: Some(Operation::Set),
            state_id,
            chunks,
        }
    }
    pub fn handle<S, F>(
        output: SetFrame,
        state_id: u8,
        fs: &mut RemoteFileSystem<S, F>,
    ) -> Result<(), FileHandleStatus> {
        if let Some(state) = fs.local_state.get_mut(&state_id) {
            if let Ok(location) = String::from_utf8(output.chunks.clone()) {
                state.location = location;
            } else {
                return Err(FileHandleStatus::IncorrectData);
            }
        } else {
            return Err(FileHandleStatus::IncorrectStateAsked);
        }
        Ok(())
    }
}

#[derive(Debug, Default, Clone)]
pub struct AcknowlageFrame {
    direction: Option<Direction>,
    operation: Option<Operation>,
}

impl AcknowlageFrame {
    fn new(direction: Option<Direction>) -> AcknowlageFrame {
        AcknowlageFrame {
            direction,
            operation: Some(Operation::Acknowlage),
        }
    }
    pub fn to_bytes<S, F>(filesystem: RemoteFileSystem<S, F>) -> Result<Vec<u8>, FileFrameStatus>
    where
        S: StateDelims,
    {
        let state = filesystem.state;
        ConvertWithDelims::to_bytes(
            &AcknowlageFrame::new(Some(filesystem.direction)),
            state.get_escape_byte(),
            state.get_starting_delims(),
            state.get_ending_delims(),
        )
    }
}
impl FrameCommons for AcknowlageFrame {
    fn get_direction(&self) -> Result<Direction, FileFrameStatus> {
        Ok(self.direction.clone().unwrap())
    }
    fn get_operation(&self) -> Result<Operation, FileFrameStatus> {
        Ok(self.operation.clone().unwrap())
    }
}

impl Decodable for AcknowlageFrame {
    type Output = Self;

    fn decode(mut bytes: Vec<u8>) -> Result<Self::Output, FileFrameStatus> {
        let mut frame = AcknowlageFrame::default();
        if let Some(byte) = bytes.get(0) {
            if let Ok(direction) = Direction::try_from_primitive(*byte) {
                frame.direction = Some(direction);
            } else {
                return Err(FileFrameStatus::NotValidFrame);
            }
            bytes.remove(0);
        } else {
            return Err(FileFrameStatus::NotValidFrame);
        }
        if let Some(byte) = bytes.get(0) {
            if let Ok(operation) = Operation::try_from_primitive(*byte) {
                if !matches!(operation, Operation::Acknowlage) {
                    return Err(FileFrameStatus::NotCorrectFrame);
                }
                frame.operation = Some(operation);
            } else {
                return Err(FileFrameStatus::NotValidFrame);
            }
            bytes.remove(0);
        } else {
            return Err(FileFrameStatus::NotValidFrame);
        }
        Ok(frame)
    }
}

#[derive(Debug, Default, Clone)]
pub struct ErrorFrame {
    direction: Option<Direction>,
    operation: Option<Operation>,
}

impl ErrorFrame {
    fn new(direction: Option<Direction>) -> ErrorFrame {
        ErrorFrame {
            direction,
            operation: Some(Operation::Error),
        }
    }
    pub fn to_bytes<S, F>(filesystem: RemoteFileSystem<S, F>) -> Result<Vec<u8>, FileFrameStatus>
    where
        S: StateDelims,
    {
        let state = filesystem.state;
        ConvertWithDelims::to_bytes(
            &ErrorFrame::new(Some(filesystem.direction)),
            state.get_escape_byte(),
            state.get_starting_delims(),
            state.get_ending_delims(),
        )
    }
    pub async fn handle<S, F, W>(mut filesystem: RemoteFileSystem<S, F>) 
    where
        S: FrameWrite<W, ErrorFrame>,
        F: Send,
    {
        let frame = ErrorFrame::new(Some(filesystem.direction.clone()));
        if let Err(e) = <S as FrameWrite<W, ErrorFrame>>::frame_write(&mut filesystem, &frame).await {
            println!("{:#?}", e);
        }
    }
}
impl FrameCommons for ErrorFrame {
    fn get_direction(&self) -> Result<Direction, FileFrameStatus> {
        Ok(self.direction.clone().unwrap())
    }
    fn get_operation(&self) -> Result<Operation, FileFrameStatus> {
        Ok(self.operation.clone().unwrap())
    }
}

impl Decodable for ErrorFrame {
    type Output = Self;

    fn decode(mut bytes: Vec<u8>) -> Result<Self::Output, FileFrameStatus> {
        let mut frame = ErrorFrame::default();
        if let Some(byte) = bytes.get(0) {
            if let Ok(direction) = Direction::try_from_primitive(*byte) {
                frame.direction = Some(direction);
            } else {
                return Err(FileFrameStatus::NotValidFrame);
            }
            bytes.remove(0);
        } else {
            return Err(FileFrameStatus::NotValidFrame);
        }
        if let Some(byte) = bytes.get(0) {
            if let Ok(operation) = Operation::try_from_primitive(*byte) {
                if !matches!(operation, Operation::Error) {
                    return Err(FileFrameStatus::NotCorrectFrame);
                }
                frame.operation = Some(operation);
            } else {
                return Err(FileFrameStatus::NotValidFrame);
            }
            bytes.remove(0);
        } else {
            return Err(FileFrameStatus::NotValidFrame);
        }
        Ok(frame)
    }
}


#[derive(Debug, Default, Clone)]
pub struct CursorFrame {
    direction: Option<Direction>,
    operation: Option<Operation>,
    starting_cursor: Option<u64>,
    ending_cursor: Option<u64>
}

impl CursorFrame {
    fn new(direction: Option<Direction>, starting_cursor: Option<u64>, ending_cursor: Option<u64>) -> CursorFrame {
        CursorFrame {
            direction,
            operation: Some(Operation::Eof),
            starting_cursor,
            ending_cursor
        }
    }
    fn handle<S, F>(
        output: CursorFrame,
        state_id: u8,
        fs: &mut RemoteFileSystem<S, F>,
    ) -> Result<(), FileHandleStatus> {
        
        Ok(())
    }
}
impl FrameCommons for CursorFrame {
    fn get_direction(&self) -> Result<Direction, FileFrameStatus> {
        Ok(self.direction.clone().unwrap())
    }
    fn get_operation(&self) -> Result<Operation, FileFrameStatus> {
        Ok(self.operation.clone().unwrap())
    }
}
impl Decodable for CursorFrame {
    type Output = Self;

    fn decode(mut bytes: Vec<u8>) -> Result<Self::Output, FileFrameStatus> {
        let mut frame = CursorFrame::default();
        if let Some(byte) = bytes.get(0) {
            if let Ok(direction) = Direction::try_from_primitive(*byte) {
                frame.direction = Some(direction);
            } else {
                return Err(FileFrameStatus::NotValidFrame);
            }
            bytes.remove(0);
        } else {
            return Err(FileFrameStatus::NotValidFrame);
        }
        if let Some(byte) = bytes.get(0) {
            if let Ok(operation) = Operation::try_from_primitive(*byte) {
                if !matches!(operation, Operation::Eof) {
                    return Err(FileFrameStatus::NotCorrectFrame);
                }
                frame.operation = Some(operation);
            } else {
                return Err(FileFrameStatus::NotValidFrame);
            }
            bytes.remove(0);
        } else {
            return Err(FileFrameStatus::NotValidFrame);
        }
        if let Some(byte) = bytes.get(0) {
            if *byte == 1 {
                bytes.remove(0);
                if bytes.len() < std::mem::size_of::<u64>() { 
                    return Err(FileFrameStatus::NotValidFrame);  
                };
                let amount_bytes = bytes[..std::mem::size_of::<u64>()]
                    .try_into()
                    .map_err(|_| FileFrameStatus::NotValidFrame)?;
                let amount = u64::from_le_bytes(amount_bytes);
                frame.starting_cursor = Some(amount);
                bytes = bytes[amount as usize..].to_vec();
            } else if *byte != 0 {
                return Err(FileFrameStatus::NotValidFrame);
            }
        } else {
            return Err(FileFrameStatus::NotValidFrame);
        }
        if let Some(byte) = bytes.get(0) {
            if *byte == 1 {
                bytes.remove(0);
                if bytes.len() < std::mem::size_of::<u64>() { 
                    return Err(FileFrameStatus::NotValidFrame);  
                };
                let amount_bytes = bytes[..std::mem::size_of::<u64>()]
                    .try_into()
                    .map_err(|_| FileFrameStatus::NotValidFrame)?;
                let amount = u64::from_le_bytes(amount_bytes);
                frame.ending_cursor = Some(amount);
                bytes = bytes[amount as usize..].to_vec();
            } else if *byte != 0 {
                return Err(FileFrameStatus::NotValidFrame);
            }
        } else {
            return Err(FileFrameStatus::NotValidFrame);
        }
        Ok(frame)
    }
}

#[derive(Debug, Default, Clone)]
pub struct EofFrame {
    direction: Option<Direction>,
    operation: Option<Operation>,
}
impl EofFrame {
    fn new(direction: Option<Direction>) -> EofFrame {
        EofFrame {
            direction,
            operation: Some(Operation::Eof),
        }
    }
    pub fn handle<S, F>(
        _output: EofFrame,
        _state_id: u8,
        fs: &mut RemoteFileSystem<S, F>,
    ) -> Result<(), FileHandleStatus> {
        if let Some(mut file_handle) = fs.file_handle.take() {
            let _ = file_handle.flush();
            let _ = file_handle.sync_all();
        }
        Ok(())
    }
    pub fn raw_output_with_delims<S: StateDelims + StreamSender, F>(
        fs: &mut RemoteFileSystem<S, F>,
    ) -> Vec<u8> {
        let frame = EofFrame::new(Some(fs.direction.clone()));
        let bytes = ConvertWithDelims::to_bytes(
            &frame,
            fs.state.get_escape_byte(),
            fs.state.get_starting_delims(),
            fs.state.get_ending_delims(),
        )
        .unwrap();
        bytes
    }
    pub async fn output_with_delims<S: StateDelims + StreamSender, F>(
        fs: &mut RemoteFileSystem<S, F>,
    ) -> Result<(), FileHandleStatus> {
        let bytes = EofFrame::raw_output_with_delims(fs);
        let _ = fs.state.send(bytes).await;
        Ok(())
    }
}
impl FrameCommons for EofFrame {
    fn get_direction(&self) -> Result<Direction, FileFrameStatus> {
        Ok(self.direction.clone().unwrap())
    }
    fn get_operation(&self) -> Result<Operation, FileFrameStatus> {
        Ok(self.operation.clone().unwrap())
    }
}
impl Decodable for EofFrame {
    type Output = EofFrame;

    fn decode(mut bytes: Vec<u8>) -> Result<EofFrame, FileFrameStatus> {
        let mut frame = EofFrame::default();
        if let Some(byte) = bytes.get(0) {
            if let Ok(direction) = Direction::try_from_primitive(*byte) {
                frame.direction = Some(direction);
            } else {
                return Err(FileFrameStatus::NotValidFrame);
            }
            bytes.remove(0);
        } else {
            return Err(FileFrameStatus::NotValidFrame);
        }
        if let Some(byte) = bytes.get(0) {
            if let Ok(operation) = Operation::try_from_primitive(*byte) {
                if !matches!(operation, Operation::Eof) {
                    return Err(FileFrameStatus::NotCorrectFrame);
                }
                frame.operation = Some(operation);
            } else {
                return Err(FileFrameStatus::NotValidFrame);
            }
            bytes.remove(0);
        } else {
            return Err(FileFrameStatus::NotValidFrame);
        }
        Ok(frame)
    }
}

pub struct WithoutDelims;
pub struct WithDelims;

#[async_trait]
pub trait FrameWrite<W, Fr>: BidirectionalStream + Sized + Send {
    async fn frame_write<F: Send>(
        fs: &mut RemoteFileSystem<Self, F>,
        frame: &Fr,
    ) -> Result<(), FileHandleStatus>;
}

#[async_trait]
impl<S, Fr> FrameWrite<WithDelims, Fr> for S
where
    S: BidirectionalStream + StateDelims + Send,
    Fr: ConvertWithDelims + Sync,
{
    async fn frame_write<F: Send>(
        fs: &mut RemoteFileSystem<S, F>,
        frame: &Fr,
    ) -> Result<(), FileHandleStatus> {
        let bytes = ConvertWithDelims::to_bytes(
            frame,
            fs.state.get_escape_byte(),
            fs.state.get_starting_delims(),
            fs.state.get_ending_delims(),
        )
        .map_err(|e| {
            FileHandleStatus::IncorrectData
        })?;
        fs.state.send(bytes).await.map_err(|e| {
            FileHandleStatus::Any(e)
        })
    }
}

#[async_trait]
impl<S, Fr> FrameWrite<WithLength, Fr> for S
where
    S: BidirectionalStream + Send,
    Fr: ConvertWithLength + Sync,
{
    async fn frame_write<F: Send>(
        fs: &mut RemoteFileSystem<S, F>,
        frame: &Fr,
    ) -> Result<(), FileHandleStatus> {
        let bytes =
            ConvertWithLength::to_bytes(frame).map_err(|_| FileHandleStatus::IncorrectData)?;
        fs.state.send(bytes).await.map_err(FileHandleStatus::Any)
    }
}

#[derive(Debug, Default, Clone)]
pub struct DrainFrame {
    direction: Option<Direction>,
    operation: Option<Operation>,
}

impl FrameCommons for DrainFrame {
    fn get_direction(&self) -> Result<Direction, FileFrameStatus> {
        Ok(self.direction.clone().unwrap())
    }
    fn get_operation(&self) -> Result<Operation, FileFrameStatus> {
        Ok(self.operation.clone().unwrap())
    }
}

impl DrainFrame {
    fn new(direction: Option<Direction>) -> DrainFrame {
        DrainFrame {
            direction,
            operation: Some(Operation::Drain),
        }
    }

    pub async fn handle<W, S, F>(
        _output: DrainFrame,
        state_id: u8,
        fs: &mut RemoteFileSystem<S, F>,
        rx: &mut (dyn Stream<Item = Vec<u8>> + Unpin + Send),
    ) -> Result<(), FileHandleStatus>
    where
        S: FrameWrite<W, FileFrame> + FrameWrite<W, EofFrame>,
        F: Send,
    {
        if let Some(state) = fs.local_state.get(&state_id) {
            let location = &state.location;
            let starting_cursor_position = state.starting_cursor_position;
            let ending_cursor_position = state.ending_cursor_position;

            let (raw_tx, raw_rx) = mpsc::channel(32);
            let mut rx = Box::pin(ReceiverStream::new(raw_rx)).boxed();

            let mut file = File::open(location)
                .map_err(|e| FileHandleStatus::Any(Box::new(e)))?;
            if let Some(starting_cursor_position) = starting_cursor_position {
                let _ = file.seek(SeekFrom::Start(starting_cursor_position.try_into().unwrap()));
            }
            if let Some(ending_cursor_position) = ending_cursor_position {
                let _ = file.seek(SeekFrom::End(ending_cursor_position.try_into().unwrap()));
            }

            tokio::spawn(async move {
                loop {
                    let mut chunk = vec![0u8; 1000];
                    let Ok(n) = file.read(&mut chunk) else {
                        break;
                    };
                    if n == 0 {
                        break;
                    }
                    let _ = raw_tx.send(chunk).await;
                }
            });

            Self::write_from_stream::<W, S, F>(fs, &mut rx).await
        } else {
            return Err(FileHandleStatus::NoStateGiven);
        }
    }

    pub async fn write_from_stream<W, S, F>(
        fs: &mut RemoteFileSystem<S, F>,
        rx: &mut (dyn Stream<Item = Vec<u8>> + Unpin + Send),
    ) -> Result<(), FileHandleStatus>
    where
        S: FrameWrite<W, FileFrame> + FrameWrite<W, EofFrame>,
        F: Send,
    {
        loop {
            match rx.next().await {
                Some(bytes) => {
                    let mut frame = FileFrame::new(
                        fs.direction.clone(),
                        Operation::Move,
                        Codec::RawContinues,
                        ChunkingStatus::Continues,
                    );
                    frame.append_bytes_send(bytes);
                    if let Err(e) = <S as FrameWrite<W, FileFrame>>::frame_write(fs, &frame).await {
                        println!("{:#?}", e);
                    }
                }
                None => {
                    let frame = EofFrame::new(Some(fs.direction.clone()));
                    if let Err(e) = <S as FrameWrite<W, EofFrame>>::frame_write(fs, &frame).await {
                        println!("{:#?}", e);
                    }
                    return Ok(());
                }
            }
        }
    }
}

impl Decodable for DrainFrame {
    type Output = DrainFrame;

    fn decode(mut bytes: Vec<u8>) -> Result<DrainFrame, FileFrameStatus> {
        let mut frame = DrainFrame::default();
        if let Some(byte) = bytes.get(0) {
            if let Ok(direction) = Direction::try_from_primitive(*byte) {
                frame.direction = Some(direction);
            } else {
                return Err(FileFrameStatus::NotValidFrame);
            }
            bytes.remove(0);
        } else {
            return Err(FileFrameStatus::NotValidFrame);
        }
        if let Some(byte) = bytes.get(0) {
            if let Ok(operation) = Operation::try_from_primitive(*byte) {
                if !matches!(operation, Operation::Drain) {
                    return Err(FileFrameStatus::NotCorrectFrame);
                }
                frame.operation = Some(operation);
            } else {
                return Err(FileFrameStatus::NotValidFrame);
            }
            bytes.remove(0);
        } else {
            return Err(FileFrameStatus::NotValidFrame);
        }
        Ok(frame)
    }
}
