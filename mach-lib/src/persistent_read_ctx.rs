use crate::constants::{CHUNK_SZ, FILE_SZ};
use crate::storage2::StorageConfig;
use crossbeam::channel::{unbounded, Sender};
use dashmap::DashMap;
use libc::*;
use memmap2::Mmap;
use std::{
	fs::{File, OpenOptions},
	os::unix::prelude::*,
	//io::{prelude::*},
	path::Path,
	sync::Arc,
	//time::Duration,
};

fn read_only_file(path: &Path) -> File {
	let flags = O_RDONLY;
	let s = format!("Reading file: {}", path.display());
	let file = OpenOptions::new()
		.custom_flags(flags)
		.read(true)
		.open(path)
		.expect(&s);
	file
}

struct FileMap {
	chunks: Arc<DashMap<u64, Arc<Vec<u8>>>>,
	files: Arc<DashMap<u64, Arc<Mmap>>>,
	prefetcher: Sender<(Arc<Mmap>, u64)>,
	read_ahead_cnt: u64,
	cfg: StorageConfig,
}

impl Clone for FileMap {
	fn clone(&self) -> Self {
		FileMap {
			chunks: self.chunks.clone(),
			files: self.files.clone(),
			read_ahead_cnt: self.read_ahead_cnt,
			prefetcher: self.prefetcher.clone(),
			cfg: self.cfg.clone(),
		}
	}
}

impl FileMap {
	fn new(read_ahead_cnt: u64, cfg: StorageConfig) -> Self {
		let (prefetcher, _rx) = unbounded();
		let this = FileMap {
			chunks: Arc::new(DashMap::new()),
			files: Arc::new(DashMap::new()),
			read_ahead_cnt,
			prefetcher,
			cfg,
		};

		let _chunks = this.chunks.clone();

		this
	}

	fn read(&mut self, chunk_addr: u64) -> Arc<Vec<u8>> {
		let file_sz = FILE_SZ as u64;
		let _chunk_sz = CHUNK_SZ as u64;
		let fid = chunk_addr / file_sz;
		use std::io::prelude::*;
		use std::io::SeekFrom;
		let file_path = self.cfg.dir.join(format!("data-{}", fid));
		let offset = chunk_addr % file_sz;
		let mut result_buf = vec![0u8; CHUNK_SZ];
		let mut fd = read_only_file(&file_path);
		let _ = fd.seek(SeekFrom::Start(offset));
		fd.read_exact(&mut result_buf).unwrap();
		Arc::new(result_buf)
	}
}

#[derive(Clone)]
struct Persistent {
	data: Arc<Vec<u8>>,
	id: u64,
}

#[derive(Clone)]
pub struct PersistentCtx {
	storage_config: StorageConfig,
	persistent: Option<Persistent>,
	file_map: FileMap,
}

impl PersistentCtx {
	pub fn new(storage_config: StorageConfig) -> Self {
		let file_map = FileMap::new(3, storage_config.clone());
		Self {
			storage_config,
			persistent: None,
			file_map,
		}
	}

	pub fn storage_config(&self) -> StorageConfig {
		self.storage_config.clone()
	}

	pub fn read_chunk_addr(&mut self, address: u64, buf: &mut [u8]) {
		let chunk_addr = address - address % CHUNK_SZ as u64;

		match &mut self.persistent {
			None => {
				let v = self.file_map.read(chunk_addr);
				self.persistent = Some(Persistent {
					data: v,
					id: chunk_addr,
				});
			}
			Some(ref mut r) => {
				if r.id != chunk_addr {
					let v = self.file_map.read(chunk_addr);
					r.id = chunk_addr;
					r.data = v;
				}
			}
		}

		let persistent = self.persistent.as_mut().unwrap();
		assert_eq!(persistent.id, chunk_addr);
		buf.copy_from_slice(&persistent.data[..]);

		//let addr_offset = address % BLOCK_SZ as u64;
		//let chunk_offset = addr_offset - addr_offset % CHUNK_SZ as u64;
		//let s = chunk_offset as usize;
		//let e = s + CHUNK_SZ;
		//buf.copy_from_slice(&persistent.data[s..e]);
	}
}
