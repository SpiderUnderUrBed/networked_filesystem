use std::{
    collections::{HashMap, VecDeque}, default, error::Error, io::Write, marker::PhantomData, os::unix::ffi::OsStrExt, pin::Pin, process::Output, sync::{
        atomic::{AtomicBool, Ordering}, mpsc::Receiver, Arc, RwLock
    }, task::{Context, Poll}, vec
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

mod delimited_commons;
mod flume_delmited_v1;

// use flume_delmited_v1::flume_delimited::*;
#[derive(Clone, Debug, TryFromPrimitive)]
#[repr(u8)]
pub enum Direction {
    Local = 0,
    Server = 1,
    Unknown = 2,
}

#[derive(Clone, Debug, TryFromPrimitive)]
#[repr(u8)]
pub enum Operation {
    Move = 0,
    Set = 1,
    Ls = 3,
    // LsWithRange { start: u64, end: u64 },
    None = 4,
}

#[derive(Clone, Debug, TryFromPrimitive)]
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
pub trait DecodableFrame {
    type Output;
    fn decode(bytes: Vec<u8>) -> Result<Self::Output, FileFrameStatus>;
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
    chunking_status: Option<ChunkingStatus>,
    chunks: Vec<u8>,
}

#[derive(Debug)]
pub enum FileFrameStatus {
    FrameNoBegins,
    FrameNoEnds,
    NotValidFrame,
    NotCorrectFrame,
    NoFrameDecoding,
    FileStreamError(FileStreamError)
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

    fn append_bytes_send(&mut self, bytes: Vec<u8>) {
        self.chunks.extend(bytes);
    }
    fn flush(&mut self) {
        self.chunks = Vec::new();
    }
}
impl DecodableFrame for FileFrame {
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
    async fn get_chunk(&mut self) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>>;
    fn get_location(&self) -> String;
}

#[derive(Debug)]
pub enum StreamableFileSystemErrors {
    None,
    NoStateGiven,
    IncorrectStateAsked,
    IncorrectData,
    IsADir,
    FileFrameError(FileFrameStatus),
    Any(Box<dyn Error + Send + Sync>)
}

pub struct RemoteFileSystem<S, F> {
    state: S,
    local_state: HashMap<u8, LocalState>,
    direction: Direction,
    operation: Operation,
    codec: Codec,
    files: Vec<F>,
    file_handle: Option<StdFile>,
    remainder: Vec<u8>,
}
impl<S: Default, F> Default for RemoteFileSystem<S, F> {
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

#[derive(Debug, Default, Clone)]
pub struct LocalState {
    pub location: String,
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
    pub fn get_state_mut(&mut self, state_id: u8) -> Option<&mut LocalState> {
        self.local_state.get_mut(&state_id)
    }
    pub fn get_state(&mut self, state_id: u8) -> Option<&LocalState>  {
        self.local_state.get(&state_id)
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
}
impl<S: Clone, F: Clone> Clone for RemoteFileSystem<S, F> {
    fn clone(&self) -> Self {
        RemoteFileSystem {
            state: self.state.clone(),
            local_state: self.local_state.clone(),
            direction: self.direction.clone(),
            operation: self.operation.clone(),
            codec: self.codec.clone(),
            files: self.files.clone(),
            file_handle: None,
            remainder: self.remainder.clone(),
        }
    }
}
impl<S: Default + StreamReceiver, F> RemoteFileSystem<S, F> {
    pub async fn decode_into<T: DecodableFrame<Output = T>>(&mut self) -> Result<(Vec<T>, Vec<Vec<u8>>), FileFrameStatus> {
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
                    let mut decoded_frames = Vec::new();
                    let mut regular_frames = Vec::new();
                    match frame.append_bytes_recv(&total_bytes, remainder) {
                        Ok(frames) => {
                            for frame in &frames {
                                let chunks = frame.get_chunks();
                                match T::decode(chunks.clone()){
                                    Ok(decoded_frame) => decoded_frames.push(decoded_frame),
                                    Err(_) => regular_frames.push(chunks),
                                }
                            }
                            return Ok((decoded_frames, regular_frames))
                        }
                        Err(e) => {
                            match e {
                                FileFrameStatus::NotValidFrame => {
                                                                                        }
                                FileFrameStatus::NoFrameDecoding => {
                                                                                        }
                                FileFrameStatus::FrameNoBegins => {
                                                                                        }
                                FileFrameStatus::FrameNoEnds => {
                                                                                            let remainder = total_bytes;
                                                                                            self.remainder = remainder.to_vec();
                                                                                        }
                                FileFrameStatus::FileStreamError(_) => {},
                                FileFrameStatus::NotCorrectFrame => {},
                            };
                            return Err(e);
                        },
                    }
                }
                Err(e) => {
                    match e {
                        FileStreamError::Disconnect => {},
                    }
                    break Err(FileFrameStatus::FileStreamError(e))
                }
            }
        }
    }
    pub async fn feed_bytes(
        &mut self,
        bytes: Vec<u8>,
        remainder: &mut u64,
        fs_state_id: u8
    ) -> Result<(), StreamableFileSystemErrors> {
        let mut total_bytes = Vec::new();
        total_bytes.extend(self.remainder.clone());
        self.remainder = Vec::new();
        total_bytes.extend(bytes);
        let mut frame = self.state.create_frame_handler();
        frame.set_chunks(self.remainder.clone());
        println!("about to receive");
        match frame.append_bytes_recv(&total_bytes, remainder) {
            Ok(frames) => {
                println!("got a few frames");
                for frame in &frames {
                    let chunks = frame.get_chunks();
                    if let Ok(file_frame) = FileFrame::decode(chunks.clone()) {
                        println!("chunks: {:?}", file_frame.chunks);
                        if self.file_handle.is_none() {
                            if let Some(state) = self.local_state.get(&fs_state_id) {
                                let location = &state.location;
                                println!("location: {:#?}", location);
                                let mut temp_handle = std::fs::OpenOptions::new()
                                    .truncate(true)
                                    .append(true)
                                    .open(location);
                                if let Err(_) = temp_handle {
                                    let _ = std::fs::File::create(location);
                                    temp_handle = std::fs::OpenOptions::new()
                                        .append(true)
                                        .open(location);
                                }
                                if let Ok(file) = temp_handle {
                                    self.file_handle = Some(file);
                                } else {
                                    return Err(StreamableFileSystemErrors::IsADir);
                                }
                            } else {
                                return Err(StreamableFileSystemErrors::NoStateGiven);
                            }
                        }

                        // if let Some(mut handle) = self.file_handle.take() {
                        let mut handle = self.file_handle.take().unwrap();
                        let _ = handle.write_all(&file_frame.chunks);
                        let _ = handle.flush();
                        let _ = handle.sync_all();

                        self.file_handle = Some(handle);
                    } else if let Ok(set_frame) = SetFrame::decode(chunks) {
                        println!("got a set frame");
                        if let Some(state) =
                            self.local_state.get_mut(&set_frame.state_id)
                        {
                            println!("got the state to edit");
                            if let Ok(location) = String::from_utf8(set_frame.chunks) {
                                println!("new location: {}", location);
                                state.location = location;
                            } else {
                                return Err(StreamableFileSystemErrors::IncorrectData);
                            }
                        } else {
                            return Err(StreamableFileSystemErrors::IncorrectStateAsked);
                        }
                    }
                }
                if let Some(last_frame) = frames.iter().last() {
                    self.remainder.extend(last_frame.get_remainder().clone());
                }
                Ok(())
            }
            Err(e) => {
                match e {
                    FileFrameStatus::NotValidFrame => {
                                                                }
                    FileFrameStatus::NoFrameDecoding => {
                                                                }
                    FileFrameStatus::FrameNoBegins => {
                                                                }
                    FileFrameStatus::FrameNoEnds => {
                                                                    let remainder = total_bytes;
                                                                    self.remainder = remainder.to_vec();
                                                                }
                    FileFrameStatus::FileStreamError(_) => {},
                    FileFrameStatus::NotCorrectFrame => {},
                };
                return Err(StreamableFileSystemErrors::FileFrameError(e))
            }
        }
    }
    pub async fn receive_operation(
        &mut self,
        fs_state_id: u8,
    ) -> Result<(), StreamableFileSystemErrors> {
        let remainder = &mut 0;

        loop {
            match self.state.get_chunk().await {
                Ok(bytes) => {
                    self.feed_bytes(bytes, remainder, fs_state_id).await?;
                }
                Err(_) => {
                    break Ok(());
                }
            }
        }
    }
}

impl<S: StreamSender + Default, F: FileSender> RemoteFileSystem<S, F>
// where S: StreamSender
{
    pub async fn send_file(&mut self, mut file: F) -> Result<(), StreamableFileSystemErrors> {
        match self.codec {
            Codec::Multipart => {
                todo!()
            }
            // Codec::RawContinues => {
            // }
            Codec::Raw | Codec::RawContinues => {
                loop {
                    // let file_content_stream =
                    //     file.content_stream.take().unwrap();

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
                            return Err(StreamableFileSystemErrors::Any(e))
                        }
                    }
                    // file.content_stream = Some(file_content_stream);
                }
            }
            _ => return Err(StreamableFileSystemErrors::None),
        }
    }
    pub async fn execute_operation(
        &mut self,
        state_id: u8,
    ) -> Result<(), StreamableFileSystemErrors> {
        match self.operation {
            Operation::Move => {
                // return Box::pin(async move {
                let mut files = std::mem::take(&mut self.files);
                for mut file in files.drain(..) {
                    self.operation = Operation::Set;
                    if let Some(state) = self.local_state.get_mut(&state_id) {
                        state.location = file.get_location();
                    } else {
                        return Err(StreamableFileSystemErrors::NoStateGiven);
                    }
                    let _ = Box::pin(self.execute_operation(state_id)).await;
                    self.operation = Operation::Move;
                    self.send_file(file).await?;
                }
                Ok(())
                //});
            }
            Operation::None => Err(StreamableFileSystemErrors::None),
            //Err("unimplimented".into()),
            Operation::Set => {
                if let Some(state) = self.local_state.get(&state_id) {
                    let frame = SetFrame::new(
                        Some(self.direction.clone()),
                        Some(Operation::Set),
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
            }
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct SetFrame {
    direction: Option<Direction>,
    operation: Option<Operation>,
    state_id: u8,
    chunks: Vec<u8>,
}
impl SetFrame {
    fn new(
        direction: Option<Direction>,
        operation: Option<Operation>,
        state_id: u8,
        chunks: Vec<u8>,
    ) -> SetFrame {
        SetFrame {
            direction,
            operation,
            state_id,
            chunks,
        }
    }

}
impl DecodableFrame for SetFrame {
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
