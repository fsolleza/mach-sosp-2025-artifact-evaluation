use std::{
	fs::{self, File},
	io::prelude::*,
	//	os::fd::AsRawFd,
	path::*,
	sync::{
		atomic::{AtomicU64, Ordering::SeqCst},
		Arc,
	},
	time::{self, Duration},
};

use crate::{
	constants::{BLOCKS_PER_FILE, BLOCK_SZ, FILE_SZ},
	utils,
};

// use io_uring::{opcode, types, IoUring};

//pub struct IoUringWriter {
//	file: File,
//	ring: IoUring,
//	dir: PathBuf,
//	id: usize,
//	block_counter: usize,
//	completion_counter: usize,
//}
//
//impl IoUringWriter {
//	pub fn write(&mut self, data: *const u8) {
//		let op = opcode::Write::new(
//			types::Fd(self.file.as_raw_fd()),
//			data,
//			BLOCK_SZ as u32,
//		)
//		.offset((self.block_counter * BLOCK_SZ) as u64)
//		.build()
//		.user_data(self.block_counter as u64);
//
//		unsafe {
//			self.ring
//				.submission()
//				.push(&op)
//				.expect("submission queue is full");
//		}
//		self.ring.submit();
//
//		self.block_counter += 1;
//
//		if self.block_counter % BLOCKS_PER_FILE == 0 {
//			self.reset();
//		}
//		//let last_count = self.block_counter.fetch_add(1, SeqCst);
//		//if last_count > 0 && (last_count + 1) % BLOCKS_PER_FILE as u64 == 0 {
//		//	self.reset();
//		//}
//	}
//
//	pub fn new(dir: PathBuf, replace: bool) -> Self {
//		if replace {
//			let _ = fs::remove_dir_all(&dir);
//			fs::create_dir_all(&dir).unwrap();
//		}
//
//		let mut id = 0;
//		let path = dir.join(format!("data-{}", id));
//		let file = utils::new_direct_file(&path, FILE_SZ);
//		let ring = IoUring::new(2).unwrap();
//		let block_counter = 0;
//		let completion_counter = 0;
//		Self {
//			file,
//			ring,
//			dir,
//			id,
//			block_counter,
//			completion_counter,
//		}
//	}
//
//	pub fn reset(&mut self) {
//		self.id += 1;
//		let path = self.dir.join(format!("data-{}", self.id));
//		self.file = utils::new_direct_file(&path, FILE_SZ);
//	}
//
//	pub fn completed(&mut self) {
//		self.ring.submit_and_wait(1);
//		let mut completion = self.ring.completion();
//		completion.next().expect("completion queue is empty");
//	}
//}

pub struct StorageWriter {
	dir: PathBuf,
	id: usize,
	block_counter: Arc<AtomicU64>,
	file: File,
	last_new_file: time::Instant,
	last_flush: time::Instant,
}

impl StorageWriter {
	pub fn new(dir: PathBuf, replace: bool) -> Self {
		if replace {
			let _ = fs::remove_dir_all(&dir);
			fs::create_dir_all(&dir).unwrap();
		}

		let mut id = 0;
		let path = dir.join(format!("data-{}", id));
		let file = utils::new_file(&path, FILE_SZ);
		//let file = utils::new_direct_file(&path, FILE_SZ);
		//let file = utils::null_file(&path, FILE_SZ);
		id += 1;

		Self {
			dir,
			id,
			file,
			last_new_file: time::Instant::now(),
			last_flush: time::Instant::now(),
			block_counter: Arc::new(AtomicU64::new(0)),
		}
	}

	pub fn write(&mut self, data: &[u8; BLOCK_SZ]) {
		self.file.write_all(data).unwrap();
		//self.file.sync_all();
		let last_count = self.block_counter.fetch_add(1, SeqCst);
		if last_count > 0 && (last_count + 1) % BLOCKS_PER_FILE as u64 == 0 {
			self.reset();
		}
	}

	pub fn reset(&mut self) {
		let path = self.dir.join(format!("data-{}", self.id));
		self.file = utils::new_file(&path, FILE_SZ);
		//self.file = utils::new_direct_file(&path, FILE_SZ);
		//self.file = utils::null_file(&path, FILE_SZ);
		self.id += 1;
		self.last_new_file = time::Instant::now();
		self.last_flush = time::Instant::now();
	}

	pub fn config(&self) -> StorageConfig {
		StorageConfig {
			dir: self.dir.clone(),
		}
	}
}

#[derive(Debug, Default, Copy, Clone, serde::Serialize, serde::Deserialize)]
pub struct StorageReaderStats {
	pub bytes_read: usize,
	pub blocks_read: usize,
	pub total_read_time: Duration,
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct StorageConfig {
	pub dir: PathBuf,
}

impl StorageConfig {
	pub fn write_to_vec(&self, vec: &mut Vec<u8>) {
		let path_bytes = self.dir.to_str().unwrap().as_bytes();
		vec.extend_from_slice(&(path_bytes.len() as u64).to_be_bytes());
		vec.extend_from_slice(path_bytes);
	}

	pub fn read_from_slice(slice: &[u8]) -> (Self, usize) {
		let sz = u64::from_be_bytes(slice[..8].try_into().unwrap()) as usize;
		let str = std::str::from_utf8(&slice[8..8 + sz]).unwrap();
		let dir = PathBuf::from(str);
		(Self { dir }, 8 + sz)
	}
}

//impl StorageConfig {
//	pub fn new_with_path(dir: PathBuf) -> Self {
//		StorageConfig { dir }
//	}
//}
