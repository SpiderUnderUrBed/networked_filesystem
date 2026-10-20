use std::{collections::VecDeque, error::Error};

use async_trait::async_trait;
use futures::sink::With;
use tokio::sync::mpsc;

use crate::{AcknowlageFrame, Convert, CursorFrame, DrainFrame, EofFrame, FileFrame, FileFrameStatus, FileSender, FileStreamError, FrameCommons, FrameHandler, SetFrame, StreamReceiver, StreamSender, TransportRecvError};

pub struct MspcFile {
    pub state_id: u8,
    pub original_location: Option<String>,
    pub final_location: String,
    pub content_stream: Option<mpsc::Receiver<Vec<u8>>>,
}

impl FileSender for MspcFile {
    async fn recv(&mut self) -> Result<Vec<u8>, TransportRecvError> {
        if let Some(mut stream) = self.content_stream.take() {
            let result;
            match stream.recv().await {
                Some(bytes) => {
                    result = bytes;
                }
                None => {
                    return Err(TransportRecvError::Disconnected);
                }
            }
            self.content_stream = Some(stream);
            return Ok(result);
        } else {
            Err(TransportRecvError::NoStream)
        }
    }

    fn get_location(&self) -> String {
        self.final_location.clone()
    }

    fn get_state(&self) -> u8 {
        self.state_id
    }
}


pub struct WithLength;

pub trait ConvertWithLength {
    fn to_bytes(
        &self,
    ) -> Result<Vec<u8>, FileFrameStatus>;
}

impl ConvertWithLength for FileFrame {
    fn to_bytes(
        &self,
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
        let encoder = FrameEncoder::new();
        bytes_frame = encoder.encode_bytes(bytes_frame, self.chunks.clone());
        Ok(bytes_frame)
    }
}

impl ConvertWithLength for SetFrame {
    fn to_bytes(
        &self,
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
        bytes_frame.push(self.state_id);
        let encoder = FrameEncoder::new();
        bytes_frame = encoder.encode_bytes(bytes_frame, self.chunks.clone());
        Ok(bytes_frame)
    }
}

impl ConvertWithLength for EofFrame {
    fn to_bytes(
        &self,
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
        let encoder = FrameEncoder::new();
        bytes_frame = encoder.encode_bytes(bytes_frame, Vec::new());
        Ok(bytes_frame)
    }
}

impl ConvertWithLength for DrainFrame {
    fn to_bytes(
        &self,
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
        let encoder = FrameEncoder::new();
        bytes_frame = encoder.encode_bytes(bytes_frame, Vec::new());
        Ok(bytes_frame)
    }
}

impl ConvertWithLength for AcknowlageFrame {
    fn to_bytes(
        &self,
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
        let encoder = FrameEncoder::new();
        bytes_frame = encoder.encode_bytes(bytes_frame, Vec::new());
        Ok(bytes_frame)
    }
}

impl ConvertWithLength for CursorFrame {
    fn to_bytes(
        &self,
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
        if let Some(starting_cursor) = self.starting_cursor {
            bytes_frame.push(1);
            bytes_frame.extend(starting_cursor.to_le_bytes());
        } else {
            bytes_frame.push(0);
        }
        if let Some(ending_cursor) = self.ending_cursor {
            bytes_frame.push(1);
            bytes_frame.extend(ending_cursor.to_le_bytes());
        } else {
            bytes_frame.push(0);
        }


        let encoder = FrameEncoder::new();
        bytes_frame = encoder.encode_bytes(bytes_frame, Vec::new());
        Ok(bytes_frame)
    }
}

impl<T: ConvertWithLength> Convert<WithLength> for T {
    fn to_bytes(
        &self,
        _opts: WithLength
    ) -> Result<Vec<u8>, FileFrameStatus> {
        ConvertWithLength::to_bytes(self)
    }
}

pub struct TcpFsBidirectional {
    pub tx: mpsc::Sender<Vec<u8>>,
    pub rx: mpsc::Receiver<Vec<u8>>
}
impl TcpFsBidirectional {
    pub fn new(tx: mpsc::Sender<Vec<u8>>, rx: mpsc::Receiver<Vec<u8>>) -> TcpFsBidirectional {
        TcpFsBidirectional {
            tx,
            rx,
        }
    }
}
impl Default for TcpFsBidirectional {
    fn default() -> Self {
        let (tx, rx) = mpsc::channel(32);
        Self {
            tx,
            rx,
        }
    }
}


#[async_trait]
impl StreamSender for TcpFsBidirectional {
    type Opts = WithLength;
    async fn encode_frame<S>(&self, frame: S) -> Result<Vec<u8>, FileFrameStatus>
    where
        S: Convert<WithLength> + Send,
    {
        frame.to_bytes(WithLength {})
    }
    async fn send(&mut self, bytes: Vec<u8>) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.tx.send(bytes).await?;
        Ok(())
    }
}

#[derive(Clone, Default, Debug)]
pub struct FrameEncoder {
    file_chunks: Vec<u8>,
    remainder: Vec<u8>,
}

impl FrameCommons for FrameEncoder {
    fn get_direction(&self) -> Result<crate::Direction, FileFrameStatus> {
        let direction = self
            .file_chunks
            .get(0)
            .map(|v| (*v).try_into().map_err(|_| FileFrameStatus::NotValidFrame))
            .ok_or_else(|| FileFrameStatus::NotValidFrame)??;
        Ok(direction)
    }
    fn get_operation(&self) -> Result<crate::Operation, FileFrameStatus> {
        let operation = self
            .file_chunks
            .get(0)
            .map(|v| (*v).try_into().map_err(|_| FileFrameStatus::NotValidFrame))
            .ok_or_else(|| FileFrameStatus::NotValidFrame)??;
        Ok(operation)
    }
}


impl FrameHandler for FrameEncoder { 
    type FrameOutput = Self;

    fn encode_bytes(&self, headers: Vec<u8>, content: Vec<u8>) -> Vec<u8> {
        let mut bytes_frame = Vec::new();
        bytes_frame.extend(TryInto::<u64>::try_into(headers.len() + content.len()).unwrap().to_le_bytes());
        bytes_frame.extend(headers);
        bytes_frame.extend(content);
        bytes_frame
    }

    fn append_bytes_recv(
        &mut self,
        bytes: &Vec<u8>,
        _: &mut u64,
    ) -> Result<std::collections::VecDeque<Self::FrameOutput>, FileFrameStatus> {
        // let mut total_chunks = self.remainder;
        // total_chunks.extend(self.file_chunks);
        let mut subframes: VecDeque<Self> = VecDeque::new();
        self.file_chunks.extend(bytes);
        let total_chunks = &self.file_chunks;

        if total_chunks.len() < std::mem::size_of::<u64>() {
            return Err(FileFrameStatus::FrameNoBegins);
        }
        let amount_bytes = total_chunks[..std::mem::size_of::<u64>()]
            .try_into()
            .map_err(|_| FileFrameStatus::NotValidFrame)?;
        let amount = u64::from_le_bytes(amount_bytes);
        if total_chunks.len() >= std::mem::size_of::<u64>() + amount as usize {
            let remainder = total_chunks[std::mem::size_of::<u64>() + amount as usize..].to_vec();
            self.file_chunks = total_chunks[std::mem::size_of::<u64>()..std::mem::size_of::<u64>() + amount as usize].to_vec();
            subframes.push_front(self.clone());
            let mut inner_file_frame = FrameEncoder::default();
            inner_file_frame.file_chunks = remainder;
            match inner_file_frame.append_bytes_recv(&Vec::new(), &mut 0) {
                Ok(frames) => {
                    subframes.extend(frames);
                }
                Err(_) => {}
            }
            Ok(subframes)
        } else {
            Err(FileFrameStatus::FrameNoEnds)
            // self.remainder = self.file_chunks;
        }
    }

    fn set_chunks(&mut self, chunks: Vec<u8>) {
        self.remainder = chunks;
    }
    fn get_remainder(&self) -> Vec<u8> {
        self.remainder.clone()
    }

    fn get_chunks(&self) -> Vec<u8> {
        self.file_chunks.clone()
    }
}
impl FrameEncoder {
    fn new() -> FrameEncoder { 
        FrameEncoder { file_chunks: Vec::new(), remainder: Vec::new()  }
    }
}

#[async_trait]
impl StreamReceiver for TcpFsBidirectional {
    type FrameOutput = FrameEncoder;
    fn create_frame_handler(&self) -> FrameEncoder {
        FrameEncoder::new()
    }
    async fn recv(&mut self) -> Result<Vec<u8>, FileStreamError> {
        match self.rx.recv().await {
            Some(bytes) => Ok(bytes),
            None => Err(FileStreamError::Disconnect),
        }
    }
}

