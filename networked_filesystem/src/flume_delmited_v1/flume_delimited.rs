use std::collections::VecDeque;

use multipeek::IteratorExt;

use crate::{
    FileFrame, FileFrameStatus, FileSender, FileStreamError, FrameHandler, Handle, SetFrame,
    StreamReceiver, StreamSender,
    delimited_commons::subsequence::{SubsequenceStatus, find_subsequence_by_windows_iter},
};

pub struct FlumeFile {
    pub original_location: Option<String>,
    pub final_location: String,
    pub content_stream: Option<flume::Receiver<Vec<u8>>>,
}
impl Clone for FlumeFile {
    fn clone(&self) -> Self {
        Self {
            original_location: self.original_location.clone(),
            final_location: self.final_location.clone(),
            content_stream: self.content_stream.as_ref().map(|stream| stream.clone()),
        }
    }
}
impl FileSender for FlumeFile {
    async fn get_chunk(&mut self) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
        if let Some(stream) = self.content_stream.take() {
            let result;
            match stream.recv_async().await {
                Ok(bytes) => {
                    result = bytes;
                }
                Err(e) => {
                    return Err(Box::new(e));
                }
            }
            self.content_stream = Some(stream);
            return Ok(result);
        } else {
            Err("no stream".into())
        }
    }

    fn get_location(&self) -> String {
        self.final_location.clone()
    }
}

pub struct WithDelims;

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
        bytes_frame = encoder.encode_bytes(bytes_frame, self.chunks.clone());

        Ok(bytes_frame)
    }
}

impl HandleWithDelims for SetFrame {
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
        let mut chunks_with_state = Vec::new();
        chunks_with_state.push(self.state_id);
        chunks_with_state.extend(self.chunks.clone());
        let encoder = FrameEncoder::new(starting_delimiter, ending_delimiter, escape_byte);
        bytes_frame = encoder.encode_bytes(bytes_frame, chunks_with_state);
        Ok(bytes_frame)
    }
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

pub struct TcpFsReceiver {
    pub tx: flume::Sender<Vec<u8>>,
    pub rx: flume::Receiver<Vec<u8>>,
    start_delimiter: Option<Vec<u8>>,
    end_delimiter: Option<Vec<u8>>,
    escape_byte: Option<u8>,
}
impl StreamReceiver for TcpFsReceiver {
    type FrameOutput = FrameEncoder;
    fn create_frame_handler(&self) -> FrameEncoder {
        FrameEncoder::new(
            self.start_delimiter.clone(),
            self.end_delimiter.clone(),
            self.escape_byte,
        )
    }
    async fn get_chunk(&self) -> Result<Vec<u8>, FileStreamError> {
        match self.rx.recv_async().await {

            Ok(bytes) => { Ok(bytes) },
            Err(e) => { Err(FileStreamError::Disconnect) },
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
        let _ = self.tx.send(bytes);
    }
}
impl Clone for TcpFsReceiver {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            rx: self.rx.clone(),
            start_delimiter: self.start_delimiter.clone(),
            end_delimiter: self.end_delimiter.clone(),
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
    pub tx: flume::Sender<Vec<u8>>,
    pub rx: flume::Receiver<Vec<u8>>,
    byte_array: Vec<u8>,
    start_delimiter: Option<Vec<u8>>,
    end_delimiter: Option<Vec<u8>>,
    escape_byte: Option<u8>,
}
impl Clone for TcpFsSender {
    fn clone(&self) -> Self {
        Self { 
            tx: self.tx.clone(), rx: self.rx.clone(), byte_array: self.byte_array.clone(), start_delimiter: self.start_delimiter.clone(), end_delimiter: self.end_delimiter.clone(), escape_byte: self.escape_byte.clone() 
        }
    }
}

impl Default for TcpFsSender {
    fn default() -> Self {
        let (tx, rx) = flume::unbounded();
        Self {
            tx,
            rx,
            byte_array: Default::default(),
            start_delimiter: Default::default(),
            end_delimiter: Default::default(),
            escape_byte: Default::default(),
        }
    }
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
impl FrameEncoder {
    pub fn new(
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
}
impl FrameHandler for FrameEncoder {
    type FrameOutput = Self;
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
    fn append_bytes_recv(
        &mut self,
        bytes: &Vec<u8>,
        _: &mut u64,
    ) -> Result<VecDeque<Self>, FileFrameStatus> {
        let mut subframes: VecDeque<Self> = VecDeque::new();
        let mut total_bytes = self.remainder.clone();
        total_bytes.extend(bytes.clone());

        if self.collect_buffer {
            self.collect_buffer = false;
            if let Some(end_delimiter) = self.ending_delimiter.clone() {
                let mut bytes_iter = self.file_chunks.clone().into_iter().multipeek();
                let mut end_pos = 0;
                let delimiter_iter = end_delimiter.iter();
                // TODO: see if I could for the subsequence matching
                // handle partial matches at the end of the iterator, signify that and start a remainder
                // waiting for the next amount of bytes from a read
                'end_delims: loop {
                    match find_subsequence_by_windows_iter(
                        delimiter_iter.clone(),
                        bytes_iter.clone(),
                    ) {
                        SubsequenceStatus::Pending => {}
                        SubsequenceStatus::NotMatched => {
                            return Err(FileFrameStatus::FrameNoEnds);
                        }
                        SubsequenceStatus::Continue => {
                            bytes_iter.next();
                            end_pos += 1;
                            continue 'end_delims;
                        }
                        SubsequenceStatus::FoundAt(pos) => {
                            end_pos = pos
                        },
                    }
                    self.remainder = self.file_chunks[end_pos..self.file_chunks.len()].to_vec();
                    self.file_chunks = self.file_chunks[0..end_pos].to_vec();
                    let mut inner_file_frame = FrameEncoder::default();
                    inner_file_frame.starting_delimiter = self.starting_delimiter.clone();
                    inner_file_frame.ending_delimiter = self.ending_delimiter.clone();
                    inner_file_frame.file_chunks = self.remainder.clone();
                    inner_file_frame.escape_byte = self.escape_byte;
                    //return Ok(subframes);
                    break 'end_delims;
                }
            }
            subframes.push_front(self.clone());
            let mut inner_file_frame = FrameEncoder::default();
            inner_file_frame.starting_delimiter = self.starting_delimiter.clone();
            inner_file_frame.ending_delimiter = self.ending_delimiter.clone();
            inner_file_frame.escape_byte = self.escape_byte;
            match inner_file_frame.append_bytes_recv(&self.remainder.clone(), &mut 0) {
                Ok(frames) => {
                    subframes.extend(frames);
                }
                Err(_) => {}
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
                        SubsequenceStatus::FoundAt(pos) => {
                            starting_offset = pos
                        },
                    }
                    self.remainder = total_bytes[..starting_offset].to_vec();

                    self.file_chunks =
                        total_bytes[starting_offset + starting_delimiter.len()..].to_vec();

                    self.collect_buffer = true;
                    let mut inner_file_frame = FrameEncoder::default();
                    inner_file_frame.starting_delimiter = self.starting_delimiter.clone();
                    inner_file_frame.ending_delimiter = self.ending_delimiter.clone();
                    inner_file_frame.escape_byte = self.escape_byte;
                    inner_file_frame.file_chunks = self.file_chunks.clone();
                    inner_file_frame.collect_buffer = true;
                    if let Ok(frames) = inner_file_frame.append_bytes_recv(&Vec::new(), &mut 0) {
                        subframes.extend(frames);
                    } else {
                        if let Some(ref ending_delimiter) = self.ending_delimiter {
                            if total_bytes.len() - starting_delimiter.len()
                                == self.file_chunks.len()
                                || self.remainder.len() == ending_delimiter.len()
                            {
                                if self.file_chunks.len() > ending_delimiter.len() {
                                    self.file_chunks = self.file_chunks
                                        [0..self.file_chunks.len() - ending_delimiter.len()]
                                        .to_vec();
                                }
                            }
                        } else {
                        }
                        return Err(FileFrameStatus::FrameNoEnds);
                    }
                    return Ok(subframes);
                }
            }
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
