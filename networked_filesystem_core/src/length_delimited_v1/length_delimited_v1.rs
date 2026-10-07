#![feature(buf_read_has_data_left)]
use std::fs::File;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::cmp::min;
use std::thread;
use arrayvec::ArrayVec;


const MAX_CHUNK_SIZE: usize = 4076;
const NW: &'static str = "127.0.0.1:1987";

const LENGTH_OFFSET: usize = size_of::<MessageKind>();
const LENGTH_SIZE: usize = size_of::<u64>();
const DATA_OFFSET: usize = LENGTH_OFFSET + LENGTH_SIZE;

#[repr(u8)]
enum MessageKind {
	FileChunk = 1,
	Eof = 2,
}

impl MessageKind {
	fn from(val: u8) -> MessageKind {
		match val {
			1 => Self::FileChunk,
			2 => Self::Eof,
			_ => panic!("invalid message kind {val}"),
		}
	}
}

enum FileSenderResult<'a> {
	Ok(&'a [u8]),
	NoData,
	Eof,
	Err(io::Error),
}

enum FileReceiverResult {
	Ok,
	Finished,
	Err(io::Error),
}

struct FileSender {
	buf: [u8; MAX_CHUNK_SIZE],
	input: Box<dyn BufRead>,
	reached_eof: bool,
}

impl FileSender {
	fn new(input: Box<dyn BufRead>) -> Self {
		Self {buf: [0; _], input: input, reached_eof: false}
	}

	fn get_chunk(&mut self) -> FileSenderResult<'_> {
		let has_data_left = self.input.has_data_left();
		match has_data_left {
			Err(err) => return FileSenderResult::Err(err),
			Ok(false) => match self.reached_eof {
				true => return FileSenderResult::Eof,
				false => {
					self.reached_eof = true;
					self.buf[0] = MessageKind::Eof as u8;
					return FileSenderResult::Ok(&self.buf[..LENGTH_OFFSET]);
				}
			}
			Ok(true) => {},
		}

		let read_amount = match self.input.read(&mut self.buf[DATA_OFFSET..]) {
			Ok(val) => val,
			Err(err) =>  return FileSenderResult::Err(err),
		};

		assert!(read_amount <= MAX_CHUNK_SIZE - DATA_OFFSET);

		if read_amount == 0 {
			return FileSenderResult::NoData;
		}

		self.buf[0] = MessageKind::FileChunk as u8;
		self.buf[LENGTH_OFFSET..DATA_OFFSET].clone_from_slice(&read_amount.to_le_bytes());

		FileSenderResult::Ok(&self.buf[..DATA_OFFSET + read_amount])
	}
}

struct FileReceiver {
	output: Box<dyn Write>,
	state: FileReceiverState,
}

enum FileReceiverState {
	ReadingKind,
	ReadingSize(ArrayVec<u8, LENGTH_SIZE>),
	ReadingChunkData(u64),
	Done,
}

impl FileReceiver {
	fn new(out: Box<dyn Write>) -> Self {
		Self {output: out, state: FileReceiverState::ReadingKind}
	}

	fn receive_chunk(&mut self, data: &[u8]) -> FileReceiverResult {
		use FileReceiverState::*;
		let mut data_offset = 0;
		while data.len() - data_offset > 0 {
			match &mut self.state {
				ReadingKind => match MessageKind::from(data[data_offset]) {
					MessageKind::Eof => self.state = Done,
					MessageKind::FileChunk => {
						data_offset += 1;
						self.state = ReadingSize(ArrayVec::new());
					}
				},
				ReadingSize(size_bytes) => {
					let read_bytes = min(size_bytes.remaining_capacity(), data.len() - data_offset);

					size_bytes.try_extend_from_slice(&data[data_offset..data_offset + read_bytes]).unwrap();
					data_offset += read_bytes;

					if size_bytes.is_full() {
						let chunk_size = u64::from_le_bytes(size_bytes.as_slice().try_into().unwrap());
						assert!(chunk_size <= (MAX_CHUNK_SIZE - DATA_OFFSET) as u64);
						self.state = ReadingChunkData(chunk_size);
					}
				},
				ReadingChunkData(remaining_bytes) => {
					let read_bytes = min(data.len() - data_offset, *remaining_bytes as usize);
					if let Err(err) = self.output.write_all(&data[data_offset..data_offset + read_bytes]) { return FileReceiverResult::Err(err); }
					*remaining_bytes -= read_bytes as u64;
					data_offset += read_bytes;
					if *remaining_bytes == 0 {
						self.state = ReadingKind;
					}
				},
				Done => return FileReceiverResult::Finished,
			}
		}
		FileReceiverResult::Ok
	}
}


fn main() {
	let sender_thread = thread::spawn(|| {
		let mut sender = FileSender::new(Box::new(BufReader::new(File::open("in.txt").unwrap())));

		let mut stream: TcpStream = loop {
			match TcpStream::connect(NW) {
				Ok(s) => break s,
				_ => {}
			}
		};

		while match sender.get_chunk() {
			FileSenderResult::Ok(chunk) => {
				stream.write_all(chunk).unwrap();
				true
			}
			FileSenderResult::NoData => true,
			FileSenderResult::Eof => false,
			FileSenderResult::Err(err) => panic!("{:?}", err),
		} {};

		println!("Sender done!");
	});
	let receiver_thread = thread::spawn(|| {
		let mut receiver = FileReceiver::new(Box::new(File::create("out.txt").unwrap()));

		let listener = TcpListener::bind(NW).unwrap();

		let mut read_buf = [0u8; MAX_CHUNK_SIZE];
		for mut stream in listener.incoming() {
			loop {
				match stream.as_mut().unwrap().read(&mut read_buf) {
					Ok(count) => match receiver.receive_chunk(&read_buf[..count]) {
						FileReceiverResult::Ok => {},
						FileReceiverResult::Finished => break,
						FileReceiverResult::Err(_) => panic!("!"),
					}
					Err(_) => panic!("error handling or something"),
				}
			}
			break;
		}

		println!("Receiver done!");

	});

	sender_thread.join().unwrap();
	receiver_thread.join().unwrap();
}