use std::{
	collections::{BinaryHeap, HashSet},
	io::{prelude::*, SeekFrom},
	mem,
	path::PathBuf,
	sync::{
		atomic::{AtomicU64, Ordering::SeqCst},
		Arc,
	},
	thread, time,
};

use crossbeam::channel::{bounded, Receiver, Sender};

//use thread_priority::{set_current_thread_priority, ThreadPriority};
use crate::{
	block::{BlockReader, BlockWriter, Entry, EntryHeader, INITIAL_ADDRESS},
	constants::{CHUNK_SZ, FILE_SZ},
	error::MachError,
	persistent_read_ctx::PersistentCtx,
	read_only_block::RoBlock,
	read_only_chunk::RoChunk,
	storage2::{StorageConfig, StorageWriter},
	utils::read_direct_file,
};

pub fn secs_now() -> u64 {
	time::SystemTime::now()
		.duration_since(time::UNIX_EPOCH)
		.unwrap()
		.as_secs()
}

fn storage_worker(
	to_rx: Receiver<BlockWriter>,
	from_tx: Sender<BlockWriter>,
	mut storage: StorageWriter,
) {
	//let mut total_bytes = 0;
	//let mut total_time: f64 = 0.;

	while let Ok(block) = to_rx.recv() {
		let bl = block.borrow_block();
		let slice = bl.full_block();
		//let now = Instant::now();
		storage.write(slice);
		//total_time += now.elapsed().as_secs_f64();
		//total_bytes += slice.len();
		//if total_bytes > 1024 * 1024 * 1024 {
		//	//let mbps = (total_bytes / (1024 * 1024)) as f64 / total_time;
		//	//println!("Writer MBPS {}mbps", mbps);
		//	total_time = 0.;
		//	total_bytes = 0;
		//}
		if from_tx.send(block).is_err() {
			println!("Writing thread exited, storage worker exiting");
			break;
		}
	}
}

pub struct AppendResult {
	pub addr: u64,
	pub new_chunk: bool,
	pub new_block: bool,
}

pub struct TSL {
	block_writer: BlockWriter,
	last_returned_address: u64,
	committed_address: Arc<AtomicU64>,
	last_flush: Arc<AtomicU64>,
	from_storage: Receiver<BlockWriter>,
	to_storage: Sender<BlockWriter>,
	block_version: usize,
	reader: TSLReader,
}

impl TSL {
	pub fn new(dir: PathBuf, replace_dir: bool) -> Self {
		// Setup blocks
		let block_writer1 = BlockWriter::new();
		let block_writer2 = BlockWriter::new();
		let block_reader1 = block_writer1.reader();
		let block_reader2 = block_writer2.reader();

		// Setup storage thread
		let (to_tx, to_rx) = bounded::<BlockWriter>(1);
		let (from_tx, from_rx) = bounded::<BlockWriter>(1);
		let storage = StorageWriter::new(dir.clone(), replace_dir);
		let storage_config = storage.config();
		//let storage_reader = storage.reader();

		{
			let to_rx = to_rx.clone();
			let from_tx = from_tx.clone();
			thread::Builder::new()
				.name("storage".to_string())
				.spawn(move || {
					storage_worker(to_rx, from_tx, storage);
				})
				.unwrap();
		}

		from_tx.send(block_writer2).unwrap(); // enqueue next block
		let last_flush = Arc::new(AtomicU64::new(0));

		// Shared committed address counter
		let committed_address = Arc::new(AtomicU64::new(0));

		let reader = TSLReader {
			block_reader1,
			block_reader2,
			storage_config,
			committed_address: committed_address.clone(),
		};

		let from_storage = from_rx;
		let to_storage = to_tx;
		let block_writer = block_writer1;

		Self {
			block_writer,
			//storage,
			last_returned_address: 0,
			block_version: 0,
			committed_address,
			last_flush,
			from_storage,
			to_storage,
			reader,
		}
	}

	pub fn reader(&self) -> TSLReader {
		self.reader.clone()
	}

	pub fn expected_next_addr(&self) -> u64 {
		self.block_writer.expected_next_addr()
	}

	// return the address of this sample
	pub fn append(
		&mut self,
		source: u64,
		timestamp: u64,
		data: &[u8],
		last_addr: u64,
	) -> AppendResult {
		let mut new_block = false;
		let push_result =
			self.block_writer.push(source, last_addr, timestamp, data);

		let result = if let Ok(r) = push_result {
			r
		} else {
			new_block = true;
			self.block_writer.sync();
			self.flush();
			self.block_writer
				.push(source, last_addr, timestamp, data)
				.unwrap()
		};

		if new_block {
			assert!(result.new_chunk);
		}
		self.last_returned_address = result.addr;

		AppendResult {
			addr: result.addr,
			new_chunk: result.new_chunk,
			new_block,
		}
	}

	pub fn flush(&mut self) {
		self.block_version += 1;
		loop {
			if let Ok(mut next_block) = self.from_storage.recv() {
				next_block.reset(self.block_version);

				mem::swap(&mut next_block, &mut self.block_writer);
				self.to_storage.try_send(next_block).unwrap();
				self.last_flush.store(secs_now(), SeqCst);
				break;
			}
		}
	}

	pub fn sync(&self) -> u64 {
		self.block_writer.sync();
		self.committed_address
			.store(self.last_returned_address, SeqCst);
		self.last_returned_address
	}
}

#[derive(Clone)]
pub struct TSLReader {
	pub(crate) block_reader1: BlockReader,
	pub(crate) block_reader2: BlockReader,
	pub(crate) storage_config: StorageConfig,
	pub(crate) committed_address: Arc<AtomicU64>,
}

impl TSLReader {
	pub fn snapshot(&self) -> TslSnapshot {
		let committed_address = self.committed_address.load(SeqCst);

		let mut snapshot1 = self.block_reader1.force_snapshot();
		let mut snapshot2 = self.block_reader2.force_snapshot();
		let empty = snapshot1.is_empty() || snapshot2.is_empty();

		if snapshot2
			.address_range()
			.is_before(&snapshot1.address_range())
		{
			mem::swap(&mut snapshot1, &mut snapshot2);
		} else if !empty
			&& !snapshot2
				.address_range()
				.excludes(&snapshot1.address_range())
		{
			let r1 = snapshot1.address_range();
			let r2 = snapshot2.address_range();
			panic!("TslSnapshots overlap!!! {:?} {:?}", r1, r2);
		}

		TslSnapshot {
			snapshot1,
			snapshot2,
			persistent_chunk: RoChunk::new_empty(),
			committed_address,
			storage_config: self.storage_config.clone(),
			block_reads: 0,
		}
	}

	// Currently this is just a duplicate of snapshot
	pub fn tsl_address_reader(&self) -> TslAddressReader {
		let committed_address = self.committed_address.load(SeqCst);

		//println!("Getting address reader");
		let mut snapshot1 = self.block_reader1.force_snapshot();
		let mut snapshot2 = self.block_reader2.force_snapshot();
		let empty = snapshot1.is_empty() || snapshot2.is_empty();

		if snapshot2
			.address_range()
			.is_before(&snapshot1.address_range())
		{
			mem::swap(&mut snapshot1, &mut snapshot2);
		} else if !empty
			&& !snapshot2
				.address_range()
				.excludes(&snapshot1.address_range())
		{
			let r1 = snapshot1.address_range();
			let r2 = snapshot2.address_range();
			panic!("TslSnapshots overlap!!! {:?} {:?}", r1, r2);
		}

		let persistent_ctx = PersistentCtx::new(self.storage_config.clone());

		TslAddressReader {
			snapshot1,
			snapshot2,
			persistent_chunk: RoChunk::new_empty(),
			committed_address,
			persistent_chunks_read: 0,
			persistent_addrs_read: 0,
			tmp_buf: Vec::with_capacity(1024),
			persistent_ctx,
		}
	}
}

#[derive(Clone)]
pub struct TslAddressReader {
	snapshot1: RoBlock,
	snapshot2: RoBlock,
	persistent_chunk: RoChunk,
	committed_address: u64,
	tmp_buf: Vec<u8>,

	pub persistent_chunks_read: u64,
	pub persistent_addrs_read: u64,
	pub persistent_ctx: PersistentCtx,
}

impl TslAddressReader {
	pub fn reset_read_counters(&mut self) {
		self.persistent_chunks_read = 0;
		self.persistent_addrs_read = 0;
	}

	pub fn read_addr(
		&mut self,
		addr: u64,
		entry: &mut Entry,
	) -> Result<(), MachError> {
		if addr == u64::MAX {
			return Err(MachError::MaxAddress);
		} else if addr > self.committed_address {
			return Err(MachError::UncommittedAddress);
		}

		let bytes: &[u8] = if self.snapshot1.address_range().contains(addr) {
			//self.in_memory_addresses_read += 1;
			self.snapshot1.read_unchecked_entry_bytes(addr)?
		} else if self.snapshot2.address_range().contains(addr) {
			//self.in_memory_addresses_read += 1;
			self.snapshot2.read_unchecked_entry_bytes(addr)?
		} else {
			self.read_from_persistent_chunk(addr)?
		};

		let _ = Entry::read_from_bytes(bytes, addr, entry)?;
		Ok(())
	}

	pub fn read_addr_direct(
		&mut self,
		addr: u64,
		entry: &mut Entry,
	) -> Result<(), MachError> {
		if addr == u64::MAX {
			return Err(MachError::MaxAddress);
		} else if addr > self.committed_address {
			return Err(MachError::UncommittedAddress);
		}

		if self.snapshot1.address_range().contains(addr) {
			let bytes = self.snapshot1.read_unchecked_entry_bytes(addr)?;
			Entry::read_from_bytes(bytes, addr, entry)?;
		} else if self.snapshot2.address_range().contains(addr) {
			let bytes = self.snapshot2.read_unchecked_entry_bytes(addr)?;
			Entry::read_from_bytes(bytes, addr, entry)?;
		} else {
			let address = addr;
			let file_id = (address - address % FILE_SZ as u64) / FILE_SZ as u64;
			let addr_offset = address % FILE_SZ as u64;
			let file_path = self
				.persistent_ctx
				.storage_config()
				.dir
				.join(format!("data-{}", file_id));
			let mut file = read_direct_file(&file_path);
			file.seek(SeekFrom::Start(addr_offset as u64)).unwrap();

			self.tmp_buf.clear();
			self.tmp_buf.resize(EntryHeader::SIZE, 0);
			file.read_exact(&mut self.tmp_buf[..]).unwrap();

			let header =
				EntryHeader::from_bytes(self.tmp_buf.as_slice(), addr)?;
			let len = header.len;

			self.tmp_buf.clear();
			self.tmp_buf.resize(len, 0u8);
			file.read_exact(&mut self.tmp_buf[..]).unwrap();
			entry.with_header_and_data(header, self.tmp_buf.as_slice());
			self.persistent_addrs_read += 1;
		}

		Ok(())
	}

	fn read_from_persistent_chunk(
		&mut self,
		addr: u64,
	) -> Result<&[u8], MachError> {
		let addr_range = self.persistent_chunk.address_range();
		if addr >= addr_range.max || addr < addr_range.min {
			self.load_persistent_chunk(addr);
		}
		self.persistent_chunk.read_unchecked_entry_bytes(addr)
	}

	fn load_persistent_chunk(&mut self, addr: u64) {
		self.persistent_chunk
			.load_persistent_with_addr(addr, &mut self.persistent_ctx);
		self.persistent_chunks_read += 1;
	}

	pub fn read_chunk_with_id(&mut self, chunk_id: u64) -> Option<RoChunk> {
		let addr = chunk_id * CHUNK_SZ as u64;
		self.read_chunk_with_address(addr)
	}

	pub fn read_chunk_with_address(&mut self, addr: u64) -> Option<RoChunk> {
		if addr == u64::MAX {
			return None;
		}
		let chunk = if self.snapshot1.address_range().contains(addr) {
			self.snapshot1.read_chunk_with_address(addr)
		} else if self.snapshot2.address_range().contains(addr) {
			self.snapshot2.read_chunk_with_address(addr)
		} else {
			self.load_persistent_chunk(addr);
			self.persistent_chunk.clone()
		};
		Some(chunk)
	}
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct TslSnapshot {
	snapshot1: RoBlock,
	snapshot2: RoBlock,
	persistent_chunk: RoChunk,
	committed_address: u64,
	storage_config: StorageConfig,
	block_reads: usize,
}

impl TslSnapshot {
	pub fn write_to_vec(&self, vec: &mut Vec<u8>) {
		self.snapshot1.write_to_vec(vec);
		self.snapshot2.write_to_vec(vec);
		self.persistent_chunk.write_to_vec(vec);
		vec.extend_from_slice(&self.committed_address.to_be_bytes());
		self.storage_config.write_to_vec(vec);
		vec.extend_from_slice(&self.block_reads.to_be_bytes());
	}

	pub fn read_from_slice(data: &[u8]) -> (Self, usize) {
		let mut idx = 0;

		let (snapshot1, sz) = RoBlock::read_from_slice(&data[idx..]);
		idx += sz;

		let (snapshot2, sz) = RoBlock::read_from_slice(&data[idx..]);
		idx += sz;

		let (persistent_chunk, sz) = RoChunk::read_from_slice(&data[idx..]);
		idx += sz;

		let committed_address =
			u64::from_be_bytes(data[idx..idx + 8].try_into().unwrap());
		idx += 8;

		let (storage_config, sz) = StorageConfig::read_from_slice(&data[idx..]);
		idx += sz;

		let block_reads =
			usize::from_be_bytes(data[idx..idx + 8].try_into().unwrap());
		idx += 8;

		(
			TslSnapshot {
				snapshot1,
				snapshot2,
				persistent_chunk,
				committed_address,
				storage_config,
				block_reads,
			},
			idx,
		)
	}

	pub fn read_addr_bytes(
		&mut self,
		addr: u64,
		ctx: &mut PersistentCtx,
	) -> Result<&[u8], MachError> {
		if addr == u64::MAX {
			return Err(MachError::MaxAddress);
		}

		if self.snapshot1.address_range().contains(addr) {
			self.snapshot1.read_unchecked_entry_bytes(addr)
		} else if self.snapshot2.address_range().contains(addr) {
			self.snapshot2.read_unchecked_entry_bytes(addr)
		} else {
			self.read_bytes_from_persistent_block(addr, ctx)
		}
	}

	fn load_persistent_chunk(&mut self, addr: u64, ctx: &mut PersistentCtx) {
		self.persistent_chunk.load_persistent_with_addr(addr, ctx);
		self.block_reads += 1;
	}

	fn read_bytes_from_persistent_block(
		&mut self,
		addr: u64,
		ctx: &mut PersistentCtx,
	) -> Result<&[u8], MachError> {
		let addr_range = self.persistent_chunk.address_range();
		if addr >= addr_range.max || addr < addr_range.min {
			loop {
				self.load_persistent_chunk(addr, ctx);
				let addr_range = self.persistent_chunk.address_range();
				if addr <= addr_range.max && addr >= addr_range.min {
					break;
				}
			}
		}
		self.persistent_chunk.read_unchecked_entry_bytes(addr)
	}

	pub fn iterator(
		&self,
		start_addresses: HashSet<SourceAddress>,
	) -> TslSnapshotIterator {
		TslSnapshotIterator::new(self.clone(), start_addresses)
	}
}

#[derive(Copy, Clone, Debug, Hash, PartialOrd, Ord, Eq, PartialEq)]
pub struct SourceAddress {
	pub source: u64,
	pub address: u64,
}

pub struct TslSnapshotIterator {
	addresses: BinaryHeap<(u64, u64)>, // next address and source
	snapshot: TslSnapshot,
	tmp_entry: Entry,
}

impl TslSnapshotIterator {
	pub fn new(
		snapshot: TslSnapshot,
		start_addresses: HashSet<SourceAddress>,
	) -> TslSnapshotIterator {
		//println!("Start addresses: {:?}", start_addresses);
		let addresses: BinaryHeap<(u64, u64)> = start_addresses
			.iter()
			.map(|x| (x.address, x.source))
			.collect();
		let tmp_entry = Entry::new_empty();
		TslSnapshotIterator {
			addresses,
			snapshot,
			tmp_entry,
		}
	}

	pub fn next_entry(&mut self, ctx: &mut PersistentCtx) -> Option<&[u8]> {
		let (addr, source) = self.addresses.pop()?;
		if addr == INITIAL_ADDRESS {
			return self.next_entry(ctx);
		}
		let bytes: &[u8] = match self.snapshot.read_addr_bytes(addr, ctx) {
			Ok(x) => x,
			Err(MachError::MaxAddress) => return None,
			_ => panic!("Unexpected internal error"),
		};
		let _ =
			Entry::read_from_bytes(bytes, addr, &mut self.tmp_entry).unwrap();
		self.addresses.push((self.tmp_entry.last_addr, source));
		Some(bytes)
	}

	pub fn current_entry(&self) -> &Entry {
		&self.tmp_entry
	}
}
