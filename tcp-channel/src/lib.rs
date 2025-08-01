use std::{
	io::{self, prelude::*},
	net::{SocketAddr, TcpListener, TcpStream},
};
use utils::{u64_from_bytes, u64_to_bytes, Duration, Instant};

pub struct TcpTxListener {
	listener: TcpListener,
}

pub use io::Error;

impl TcpTxListener {
	pub fn new(addr: &str) -> Self {
		let listener = TcpListener::bind(addr).unwrap();
		Self { listener }
	}

	pub fn new_tx(&self) -> TcpTx {
		let stream = self.listener.incoming().next().unwrap().unwrap();
		stream.set_nodelay(true).unwrap();
		TcpTx { stream }
	}
}

pub struct TcpTx {
	stream: TcpStream,
}

impl TcpTx {
	pub fn send(&mut self, bytes: &[u8]) -> Result<(), Error> {
		let _ = self.stream.write_all(&bytes.len().to_be_bytes())?;
		let _ = self.stream.write_all(&bytes)?;
		Ok(())
	}

	pub fn set_write_timeout(&self, dur: Duration) -> Result<(), Error> {
		self.stream.set_write_timeout(Some(dur))
	}

	pub fn wait_for_rx(&mut self) {
		let mut bytes = [0u8; 8];
		loop {
			self.stream.read_exact(&mut bytes[..]).unwrap();
			println!("Got something from client");
			if u64_from_bytes(&bytes[..]) == 1234 {
				println!("Acking client");
				let _ = self.stream.write_all(&bytes[..]).unwrap();
				println!("Returning");
				return;
			}
		}
	}
}

pub struct TcpRx {
	stream: TcpStream,
	buf: Vec<u8>,
}

impl TcpRx {
	pub fn connect(addr: &str) -> Self {
		let stream = TcpStream::connect(addr).unwrap();
		stream.set_nodelay(true).unwrap();
		Self {
			stream,
			buf: Vec::new(),
		}
	}

	pub fn connect_timeout(
		addr: &str,
		timeout: Duration,
	) -> Result<Self, Error> {
		let addr: SocketAddr = addr.parse().unwrap();
		let now = Instant::now();
		loop {
			let e = now.elapsed();
			if e > timeout {
				break;
			}
			let t = timeout - e;
			if let Ok(stream) = TcpStream::connect_timeout(&addr, t) {
				stream.set_nodelay(true).unwrap();
				return Ok(Self {
					stream,
					buf: Vec::new(),
				});
			}
		}
		Err(io::Error::new(io::ErrorKind::TimedOut, "failed to connect"))
	}

	pub fn recv(&mut self) -> Result<&[u8], Error> {
		self.buf.clear();
		let mut msg_sz = [0u8; 8];
		self.stream.read_exact(&mut msg_sz[..])?;
		let sz = usize::from_be_bytes(msg_sz);
		self.buf.resize(sz, 0);
		self.stream.read_exact(&mut self.buf[..sz])?;
		Ok(self.buf.as_slice())
	}

	pub fn start_tx(&mut self) -> Result<(), Error> {
		let mut bytes = [0u8; 8];
		u64_to_bytes(1234u64, &mut bytes[..]);
		self.stream.write_all(&bytes[..])?;

		let mut bytes_response = [0u8; 8];
		self.stream.read_exact(&mut bytes_response[..])?;
		Ok(())
	}
}
