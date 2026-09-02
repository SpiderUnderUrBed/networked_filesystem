use std::{
    collections::{HashMap, VecDeque}, default, error::Error, io::Write, marker::PhantomData, os::unix::ffi::OsStrExt, pin::Pin, process::Output, sync::{
        atomic::{AtomicBool, Ordering}, mpsc::Receiver, Arc, RwLock
    }, task::{Context, Poll}, time::Duration, vec
};

use multer::bytes::{self, buf};
use multipeek::{IteratorExt, MultiPeek};
use num_enum::{IntoPrimitive, TryFromPrimitive};
use std::fs::File as StdFile;
use tokio::{
    fs::File,
    sync::{Mutex, Notify, broadcast},
};
use tokio::{io::AsyncWriteExt, sync::watch};

pub use flume_delmited_v1::*;

use crate::flume_delmited_v1::flume_delimited::FrameEncoder;

pub mod chain;
mod delimited_commons;
mod flume_delmited_v1;

// use flume_delmited_v1::flume_delimited::*;
#[derive(Clone, TryFromPrimitive, Debug)]
#[repr(u8)]
pub enum Direction {
    Local = 0,
    Server = 1,
    Unknown = 2,
}

#[derive(Clone, TryFromPrimitive, Debug)]
#[repr(u8)]
pub enum Operation {
    Move = 0,
    Set = 1,
    Ls = 2,
    Eof = 3,
    // LsWithRange { start: u64, end: u64 },
    None = 4,
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

pub trait FrameCommons {
    type Output: std::fmt::Debug;
    async fn handle<S, F>(output: Self::Output, state_id: u8, fs: &mut RemoteFileSystem<S, F>) -> Result<(), FileHandleStatus>;
    fn decode(bytes: Vec<u8>) -> Result<Self::Output, FileFrameStatus>;
}

pub trait FrameHandler {
    type FrameOutput;
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

pub trait HandleWithLength {
    fn to_bytes(&self) -> Result<Vec<u8>, FileFrameStatus>;
}

pub struct WithLength;

pub trait Handle<W> {
    fn to_bytes(
        &self,
        escape_byte: Option<u8>,
        starting_delimiter: Option<Vec<u8>>,
        ending_delimiter: Option<Vec<u8>>,
    ) -> Result<Vec<u8>, FileFrameStatus>;
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
#[derive(Default, Clone, Debug)]
pub struct FileFrame {
    direction: Option<Direction>,
    operation: Option<Operation>,
    codec: Option<Codec>,
    pub chunking_status: Option<ChunkingStatus>,
    pub chunks: Vec<u8>,
}


#[derive(Debug)]
pub enum TransportRecvError {
    Disconnected,
    Lagged(usize),
    NoStream
}

#[derive(Debug)]
pub enum FileHandleStatus {
    NoStateGiven,
    IncorrectData,
    IncorrectStateAsked
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
    pub fn write_at_location<S, F>(&mut self, fs: &mut RemoteFileSystem<S, F>, location: String) -> Result<(), FileHandleStatus>{
        if fs.file_handle.is_none() {
            let mut temp_handle = std::fs::OpenOptions::new()
                .truncate(true)
                .append(true)
                .open(&location);
            if let Err(e) = temp_handle {
                let _ = std::fs::File::create(&location);
                temp_handle = std::fs::OpenOptions::new()
                    .append(true)
                    .open(location)
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
    fn append_bytes_send(&mut self, bytes: Vec<u8>) {
        self.chunks.extend(bytes);
    }
    fn flush(&mut self) {
        self.chunks = Vec::new();
    }
}
impl FrameCommons for FileFrame {
    type Output = FileFrame;
    async fn handle<S, F>(mut output: FileFrame, state_id: u8, fs: &mut RemoteFileSystem<S, F>) -> Result<(), FileHandleStatus>{
        if let Some(state) = fs.local_state.get(&state_id) {
            let location = &state.location;
            output.write_at_location(fs, location.to_string())?;
        } else {
            return Err(FileHandleStatus::NoStateGiven);
        }

        Ok(())
    }
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
                if !matches!(operation, Operation::Move){
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

pub trait StreamSender {
    async fn encode_frame<S, W>(&self, frame: S) -> Result<Vec<u8>, FileFrameStatus>
    where
        S: Handle<W>;
    async fn send(&mut self, bytes: Vec<u8>);
}

pub trait FileSender {
    async fn get_chunk(&mut self) -> Result<Vec<u8>, TransportRecvError>;
    fn get_location(&self) -> String;
}

#[derive(Debug)]
pub enum StreamableFileSystemErrors {
    None,
    NoStateGiven,
    IncorrectStateAsked,
    IncorrectData,
    Unknown,
    TransportRecvError(TransportRecvError),
    Any(Box<dyn Error + Send + Sync>)
}

#[derive(Debug, Default, Clone)]
pub struct LocalState {
    pub location: String,
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
impl<S: Clone, F: Clone> Clone for RemoteFileSystem<S, F>{
    fn clone(&self) -> Self {
        let (watch_tx, watch_rx) = watch::channel(Operation::None);
        Self { 
            state: self.state.clone(), 
            local_state: self.local_state.clone(), 
            direction: self.direction.clone(), 
            operation: self.operation.clone(), 
            codec: self.codec.clone(), files: self.files.clone(), 
            file_handle: None,
            remainder: self.remainder.clone(),
            unique_operation_event: (watch_tx, watch_rx)
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
    pub fn set_direction(&mut self, direction: Direction) {
        self.direction = direction;
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
    ) -> Result<(), StreamableFileSystemErrors> {
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

                    match frame.append_bytes_recv(&total_bytes, remainder) {
                        Ok(frames) => {
                            for frame in &frames {
                                let chunks = frame.get_chunks();
                                if let Ok(file_frame) = FileFrame::decode(chunks.clone()) {
                                    FileFrame::handle(file_frame, fs_state_id, self).await
                                        .map_err(|e| match e {
                                            FileHandleStatus::NoStateGiven => StreamableFileSystemErrors::NoStateGiven,
                                            FileHandleStatus::IncorrectData => StreamableFileSystemErrors::IncorrectData,
                                            FileHandleStatus::IncorrectStateAsked => StreamableFileSystemErrors::IncorrectStateAsked,
                                        })?;
                                } else if let Ok(set_frame) = SetFrame::decode(chunks) {
                                    SetFrame::handle(set_frame, fs_state_id, self).await
                                        .map_err(|e| match e {
                                            FileHandleStatus::NoStateGiven => StreamableFileSystemErrors::NoStateGiven,
                                            FileHandleStatus::IncorrectData => StreamableFileSystemErrors::IncorrectData,
                                            FileHandleStatus::IncorrectStateAsked => StreamableFileSystemErrors::IncorrectStateAsked,
                                        })?;
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
                                                    }
                            FileFrameStatus::NotCorrectFrame => {},
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

impl<S: StreamSender + Default, F: FileSender> RemoteFileSystem<S, F>
{
    pub async fn send_file(&mut self, mut file: F) -> Result<(), StreamableFileSystemErrors> {
        match self.codec {
            Codec::Multipart => {
                todo!()
            },
            Codec::Raw | Codec::RawContinues => {
                loop {
                    match file.get_chunk().await {
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

                                if let Ok(bytes) =
                                    self.state.encode_frame(frame.clone()).await
                                {
                                    self.state.send(bytes).await;
                                }
                            }
                        }
                        Err(e) => {
                            return Err(StreamableFileSystemErrors::TransportRecvError(e))
                        }
                    }
                }
            }
            _ => return Err(StreamableFileSystemErrors::None),
        }
    }
    pub async fn execute_operation(
        &mut self,
        state_id: u8,
    ) -> Result<(), StreamableFileSystemErrors> {
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
                    println!("before sending file");
                    if let Err(e) = self.send_file(file).await {
                        println!("done sending file");
                        if matches!(e, StreamableFileSystemErrors::TransportRecvError(TransportRecvError::Disconnected)){
                            self.operation = Operation::Eof;
                            let _ = Box::pin(self.execute_operation(state_id)).await;
                            println!("done sending eof");
                        }
                    }
                }
                Ok(())
            },
            Operation::None => Err(StreamableFileSystemErrors::None),
            //Err("unimplimented".into()),
            Operation::Set => {
                if let Some(state) = self.local_state.get(&state_id) {
                    let frame = SetFrame::new(
                        Some(self.direction.clone()),
                        state_id,
                        state.location.as_bytes().to_vec(),
                    );
                    if let Ok(bytes) = self.state.encode_frame(frame.clone()).await {
                        self.state.send(bytes).await;
                    }
                    Ok(())
                } else {
                    Err(StreamableFileSystemErrors::NoStateGiven)
                }
            }
            Operation::Ls => {
                return Err(StreamableFileSystemErrors::None);
            },
            Operation::Eof => {
                let frame = EofFrame::new(
                    Some(self.direction.clone())
                );
                if let Ok(bytes) = self.state.encode_frame(frame.clone()).await {
                    self.state.send(bytes).await;
                }
                Ok(())
            }
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
    type Output = SetFrame;
    async fn handle<S, F>(output: SetFrame, state_id: u8, fs: &mut RemoteFileSystem<S, F>) -> Result<(), FileHandleStatus>{
        if let Some(state) =
            fs.local_state.get_mut(&state_id)
        {
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
                if !matches!(operation, Operation::Set){
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
    fn new(
        direction: Option<Direction>,
        state_id: u8,
        chunks: Vec<u8>,
    ) -> SetFrame {
        SetFrame {
            direction,
            operation: Some(Operation::Set),
            state_id,
            chunks,
        }
    }

}

#[derive(Debug, Default, Clone)]
pub struct EofFrame {
    direction: Option<Direction>,
    operation: Option<Operation>,
}
impl EofFrame {
    fn new(direction: Option<Direction>) -> EofFrame {
        EofFrame { direction, operation: Some(Operation::Eof) }
    }
}
impl FrameCommons for EofFrame {
    type Output = EofFrame;
    async fn handle<S, F>(_output: EofFrame, _state_id: u8, fs: &mut RemoteFileSystem<S, F>) -> Result<(), FileHandleStatus> {
        let mut file_handle = fs.file_handle.take().unwrap();
        let _ = file_handle.flush();
        let _ = file_handle.sync_all();
        Ok(())
    }
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
                if !matches!(operation, Operation::Eof){
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