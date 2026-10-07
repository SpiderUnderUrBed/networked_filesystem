use std::error::Error;

use async_trait::async_trait;
use futures::sink::With;
use tokio::sync::mpsc;

use crate::{AcknowlageFrame, Convert, ConvetWithLength, EofFrame, FileFrame, FileFrameStatus, FileSender, FileStreamError, FrameCommons, FrameHandler, SetFrame, StreamReceiver, StreamSender};

pub struct MspcFile {
    pub state_id: u8,
    pub original_location: Option<String>,
    pub final_location: String,
    pub content_stream: Option<mpsc::Receiver<Vec<u8>>>,
}

impl FileSender for MspcFile {
    async fn recv(&mut self) -> Result<Vec<u8>, crate::TransportRecvError> {
        todo!()
    }

    fn get_location(&self) -> String {
        todo!()
    }

    fn get_state(&self) -> u8 {
        todo!()
    }
}

pub struct WithLength;

pub trait ConvertWithLength {
    // type FrameOutput;
    fn to_bytes(
        &self,
    ) -> Result<Vec<u8>, FileFrameStatus>;
    //    fn create_frame_handler() -> Self::FrameOutput;
}

impl ConvertWithLength for FileFrame {
    fn to_bytes(
        &self,
    ) -> Result<Vec<u8>, FileFrameStatus> {
        todo!()
    }
}

impl ConvertWithLength for SetFrame {
    fn to_bytes(
        &self,
    ) -> Result<Vec<u8>, FileFrameStatus> {
        todo!()
    }
}

impl ConvertWithLength for EofFrame {
    fn to_bytes(
        &self,
    ) -> Result<Vec<u8>, FileFrameStatus> {
        todo!()
    }
}

impl ConvertWithLength for AcknowlageFrame {
    fn to_bytes(
        &self,
    ) -> Result<Vec<u8>, FileFrameStatus> {
        todo!()
    }
}


impl<T: ConvetWithLength> Convert<WithLength> for T {
    fn to_bytes(
        &self,
        _opts: WithLength
    ) -> Result<Vec<u8>, FileFrameStatus> {
        ConvetWithLength::to_bytes(self)
    }
}

pub struct TcpFsBidirectional {
    pub tx: mpsc::Sender<Vec<u8>>,
    pub rx: mpsc::Receiver<Vec<u8>>
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
pub struct FrameEncoder {
}

impl FrameCommons for FrameEncoder {
    fn get_direction(&self) -> Result<crate::Direction, FileFrameStatus> {
        todo!()
    }

    fn get_operation(&self) -> Result<crate::Operation, FileFrameStatus> {
        todo!()
    }
}

impl FrameHandler for FrameEncoder {
    type FrameOutput = Self;

    fn encode_bytes(&self, headers: Vec<u8>, content: Vec<u8>) -> Vec<u8> {
        todo!()
    }

    fn append_bytes_recv(
        &mut self,
        bytes: &Vec<u8>,
        _: &mut u64,
    ) -> Result<std::collections::VecDeque<Self::FrameOutput>, FileFrameStatus> {
        todo!()
    }

    fn set_chunks(&mut self, chunks: Vec<u8>) {
        todo!()
    }

    fn get_remainder(&self) -> Vec<u8> {
        todo!()
    }

    fn get_chunks(&self) -> Vec<u8> {
        todo!()
    }
}
impl FrameEncoder {
    fn new() -> FrameEncoder { 
        FrameEncoder {  }
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