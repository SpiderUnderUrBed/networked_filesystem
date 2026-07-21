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
    task::{Context, Poll}, vec,
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
            loop {
                match self.state.rx.recv().await {
                    Ok(bytes) => {
                        // I push one new byte to the start of the array
                        // as the very first byte is glossed over for starting delimiter
                        // detection and this is better than special case handling
                        let mut new_bytes = Vec::with_capacity(bytes.len() + 1);
                        new_bytes.push(0);
                        new_bytes.extend(bytes);
                        // I create a multipeak array which lets me to see
                        // some amount of bytes into the future somewhat efficently
                        let mut bytes_iter = new_bytes.into_iter().multipeek();
                        // Defines whether or not im collecting the next few bytes until the end of the buffer
                        // and/or the end delimiter is detected
                        let mut collecting_buffer = false;

                        // A file handle will be kept for file transfer operations which has not finished
                        let mut file_handle = None;
                        while let Some(_) = bytes_iter.peek() {
                            let mut direction: Option<Direction> = None;
                            let mut operation: Option<Operation> = None;
                            let mut codec: Option<Codec> = None;
                            let mut continues: Option<ChunkingStatus> = None;
                            let location = {
                                match self.local_state.get(&fs_state_name) {
                                    Some(fs_state) => &mut fs_state.location.clone(),
                                    None => &mut "/tmp/fserror".to_string(),
                                }
                            };

                            'end: while let Some(byte) = bytes_iter.next() {
                                'start_delim: {
                                    let mut deliter_offset = 0;
                                    if let Some(start_delimiter) = &self.state.start_delimiter {
                                        deliter_offset = start_delimiter.len();
                                        if !collecting_buffer {
                                            // Go through the starting delimiter bytes and see if
                                            // a matching pattern has been detected
                                            // if no breaks occur then it
                                            // will start collecting the buffer
                                            for (i, delimiter_byte) in
                                                start_delimiter.iter().enumerate()
                                            {
                                                // look ahead for the specific starting delimiter byte
                                                if let Some(future_byte) = bytes_iter.peek_nth(i) {
                                                    if future_byte != delimiter_byte {
                                                        // the next byte does not match with the delimiter byte, go to
                                                        // next loop
                                                        break 'start_delim;
                                                    }
                                                } else {
                                                    // there is nothing more to check for when looking for a starting delimiter
                                                    // just end it here
                                                    break 'end;
                                                }
                                            }
                                        }
                                        // Nothing broke back to the loop
                                        // therfore the starting delimiter
                                        // pattern has been found next
                                        // start collecting the buffer
                                        collecting_buffer = true;
                                    } else {
                                        // The user did not set a starting delimiter, immediately try to collect from
                                        // the buffer
                                        collecting_buffer = true;
                                    }
                                    // start collecting the buffer
                                    if collecting_buffer {
                                        // If an end delimiter is set, always keep looking for it
                                        if let Some(end_delimiter) = &self.state.end_delimiter {
                                            // if what was seen is not apart of a end delimiter it will break to this
                                            // loop and continue
                                            'end_delim: {
                                                // Go through all the end delimiters bytes, if there is another
                                                // byte left in the buffer or the sequence of potential end delimiters is
                                                // terminated end it
                                                for (i, delimiter_byte) in
                                                    end_delimiter.iter().enumerate()
                                                {
                                                    if let Some(future_byte) =
                                                        bytes_iter.peek_nth(end_delimiter.len() + i)
                                                    {
                                                        if future_byte != delimiter_byte {
                                                            break 'end_delim;
                                                        }
                                                    } else {
                                                        break 'end_delim;
                                                    }
                                                }
                                                // This part handles the end of operations, right
                                                // after the end delimiter

                                                // if the location was completely set, and its the end of the
                                                // set operation, it will set the local states location to what
                                                // location
                                                if let Some(final_operation) = operation {
                                                    // TODO: consider if i want to use matches! or match
                                                    if matches!(final_operation, Operation::Set) {
                                                        if let Some(local_state) =
                                                            self.local_state.get_mut(&fs_state_name)
                                                        {
                                                            local_state.location =
                                                                location.to_string();
                                                        } else {
                                                            return Err("no state added".into());
                                                        }
                                                        self.patch_state_location_path(
                                                            &fs_state_name,
                                                        );
                                                    }
                                                    if matches!(final_operation, Operation::Ls) {
                                                        let mut temp_buf = Vec::new();
                                                        // start pushing the direction and operation
                                                        temp_buf.push(
                                                            Direction::to_byte_header(
                                                                &self.direction,
                                                            )
                                                            .unwrap(),
                                                        );
                                                        temp_buf.push(
                                                            Operation::to_byte_header(
                                                                &Operation::Ls,
                                                            )
                                                            .unwrap(),
                                                        );
                                                        // Assume it will not continue
                                                        temp_buf.push(0);
                                                        let files = std::fs::read_dir(location)
                                                            .expect("invalid directory");
                                                        for file_result in files {
                                                            if let Ok(file) = file_result {
                                                                temp_buf.push(file.file_name().len().try_into().expect("this system seems to allow filenames over 4096, this is not supported"));
                                                                temp_buf.extend_from_slice(
                                                                    file.file_name().as_bytes(),
                                                                );
                                                            }
                                                        }
                                                        println!("about to send dir back");
                                                        self.state.send(temp_buf);
                                                    }
                                                }
                                                collecting_buffer = false;
                                                break 'end;
                                            }
                                        }

                                        // Looks ahead to see past the delimiter
                                        if let Some(byte) = bytes_iter.peek_nth(deliter_offset) {
                                            // Ensures direction is set
                                            if direction.is_none() {
                                                direction = Direction::from_byte_header(*byte);
                                                if direction.is_none() {
                                                    println!("no direction");
                                                    return Err(
                                                        "invalid, no direction was processed"
                                                            .into(),
                                                    );
                                                }
                                                break 'start_delim;
                                            }
                                            // Ensures an operation was set
                                            if operation.is_none() {
                                                operation = Operation::from_byte_header(*byte);
                                                if operation.is_none() {
                                                    println!("no operation");
                                                    return Err(
                                                        "invalid, no operation was processed"
                                                            .into(),
                                                    );
                                                }
                                                if matches!(
                                                    operation.clone().unwrap(),
                                                    Operation::Set
                                                ) || matches!(
                                                    operation.clone().unwrap(),
                                                    Operation::Ls
                                                ) {
                                                    location.clear();
                                                }
                                                break 'start_delim;
                                            }
                                            match operation.clone().unwrap() {
                                                Operation::Move => {
                                                    if codec.is_none() {
                                                        codec = Codec::from_byte_header(*byte);
                                                        if codec.is_none() {
                                                            return Err(
                                                                "invalid, no codec was processed"
                                                                    .into(),
                                                            );
                                                        }
                                                        break 'start_delim;
                                                    }

                                                    if continues.clone().is_none() {
                                                        continues =
                                                            ChunkingStatus::from_byte_header(*byte);
                                                        println!(
                                                            "this is continues: {:#?}",
                                                            continues
                                                        );
                                                        if continues.is_none() {
                                                            return Err("invalid, no continuation was processed".into());
                                                        }
                                                        break 'start_delim;
                                                    }
                                                    let new_location = "/home/spiderunderurbed/projects/tcp_fs_poc/test-output.txt";
                                                    if file_handle.is_none() {
                                                        println!("{:?}", location);
                                                        let mut temp_handle =
                                                            std::fs::OpenOptions::new()
                                                                .append(true)
                                                                .open(new_location);
                                                                //.open(&mut *location);
                                                        if let Err(_) = temp_handle {
                                                            let _ = std::fs::File::create(
                                                                //&mut *location,
                                                                new_location
                                                            );
                                                            temp_handle =
                                                                std::fs::OpenOptions::new()
                                                                    .append(true)
                                                                    .open(new_location)
                                                                    //.open(&mut *location);
                                                        }
                                                        file_handle = Some(temp_handle.unwrap());
                                                    }
                                                    let mut local_handle =
                                                        file_handle.take().unwrap();
                                                    let _ = local_handle.write_all(&[*byte]);
                                                    if matches!(
                                                        continues.clone().unwrap(),
                                                        ChunkingStatus::Continues
                                                    ) {
                                                        file_handle = Some(local_handle);
                                                    }
                                                    // location.clear();
                                                }
                                                Operation::Set => {
                                                    location.push(char::from(*byte));
                                                }
                                                Operation::Ls => {
                                                    location.push(char::from(*byte));
                                                }
                                                Operation::None => todo!(),
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => match e {
                        broadcast::error::RecvError::Closed => {
                            println!("closed");
                        }
                        broadcast::error::RecvError::Lagged(_) => {
                            println!("lagged");
                        }
                    },
                }
            }
            Ok(())
        })
    }
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
                                let direction_header = self.direction.to_byte_header().unwrap();
                                let operation_header = self.operation.to_byte_header().unwrap();
                                let codec_header = self.codec.to_byte_header().unwrap();

                                let mut previous_buf: Vec<u8> =  Vec::with_capacity(4096);
                                //let mut delimiter_offset = 0;

                                loop {
                                    let mut file_content_stream =
                                        file.content_stream.take().unwrap();

                                    match file_content_stream.recv().await {
                                        Ok(bytes) => {
                                            previous_buf.extend(bytes.clone());
                                            if previous_buf.len() < 1000 {
                                                file.content_stream = Some(file_content_stream);
                                                continue;
                                            }
                                            // temp_buf = vec![];
                                            // temp_buf = previous_buf.clone();
                                            let mut temp_buf: Vec<u8> = Vec::with_capacity(4096);

                                            // TODO: consider if i need to fill and therfore allocate
                                            // the capacity of the max size this buffer could be
                                            let chunks: Vec<&[u8]> = previous_buf.chunks(1000).collect();
                                            let chunks_length = chunks.len();
                                            for (i, chunk) in chunks.into_iter().enumerate() {
                                                if let Some(start_delims) =
                                                    &self.state.start_delimiter
                                                {
                                                    //delimiter_offset = start_delims.len();
                                                    temp_buf.extend(start_delims);
                                                }
                                                temp_buf.push(direction_header);
                                                temp_buf.push(operation_header);
                                                temp_buf.push(codec_header);
                                                if chunks_length == i {
                                                    temp_buf.push(1);
                                                } else {
                                                    temp_buf.push(0);
                                                }
                                                temp_buf.extend_from_slice(chunk);
                                                if let Some(end_delims) = &self.state.end_delimiter
                                                {
                                                    temp_buf.extend(end_delims);
                                                }
                                            }
                                            self.state.send(temp_buf).await;

                                        }
                                        Err(e) => {
                                            match e {
                                                broadcast::error::RecvError::Closed => {
                                                    // println!("closing");
                                                    let mut temp_buf: Vec<u8> = Vec::with_capacity(4096);
                                                    if let Some(start_delims) =
                                                        &self.state.start_delimiter
                                                    {
                                                        //delimiter_offset = start_delims.len();
                                                        temp_buf.extend(start_delims);
                                                    }
                                                    temp_buf.push(direction_header);
                                                    temp_buf.push(operation_header);
                                                    temp_buf.push(codec_header);
                                                    temp_buf.push(1);
                                                    temp_buf.extend(previous_buf.clone());
                                                    if let Some(end_delims) = &self.state.end_delimiter
                                                    {
                                                        temp_buf.extend(end_delims);
                                                    }
                                                    println!("{:#?}", temp_buf.len());
                                                    self.state.send(temp_buf.clone()).await;
                                                    break;
                                                },
                                                broadcast::error::RecvError::Lagged(_) => {

                                                },
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
