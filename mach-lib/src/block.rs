use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering::SeqCst};

use crate::{
	aligned::AlignedBlock,
	constants::{BLOCK_SZ, CHUNK_SZ},
	error::MachError,
	read_only_block::RoBlock,
	utils::manually_boxed_zeroed,
};

const ENTRY_HEADER: usize = 8 * 5;
pub const CHUNK_TAIL_VALUE: u64 = u64::MAX - 1;
const CHUNK_TAIL: [u8; 8] = CHUNK_TAIL_VALUE.to_be_bytes();

pub const INITIAL_ADDRESS: u64 = u64::MAX;

//pub fn is_chunk_tail(bytes: &[u8]) -> bool {
//	&bytes[..8] == &CHUNK_TAIL[..]
//}

#[derive(Debug, Eq, PartialEq)]
pub enum BlockError {
	Full,
	ConcurrentUpdate,
	NewUnsyncedBlock,
}

#[derive(Copy, Clone, Debug)]
pub struct EntryHeader {
	pub address: u64,
	pub source: u64,
	pub last_addr: u64,
	pub timestamp: u64,
	pub len: usize,
}

impl EntryHeader {
	pub const SIZE: usize = ENTRY_HEADER;

	pub fn new_empty() -> Self {
		EntryHeader {
			address: u64::MAX,
			source: u64::MAX,
			last_addr: u64::MAX,
			timestamp: u64::MAX,
			len: 0,
		}
	}

	pub fn from_bytes(b: &[u8], addr: u64) -> Result<Self, MachError> {
		assert_eq!(ENTRY_HEADER, 40);
		let address = u64::from_be_bytes(b[0..8].try_into().unwrap());

		if address == CHUNK_TAIL_VALUE {
			return Err(MachError::ChunkTail);
		}

		if address != addr {
			panic!(
				"Invalid expected address parsed address: {} expected: {}",
				address, addr
			);
		}

		let source = u64::from_be_bytes(b[8..16].try_into().unwrap());
		let last_addr = u64::from_be_bytes(b[16..24].try_into().unwrap());
		let timestamp = u64::from_be_bytes(b[24..32].try_into().unwrap());
		let len = usize::from_be_bytes(b[32..40].try_into().unwrap());

		Ok(Self {
			address,
			source,
			last_addr,
			timestamp,
			len,
		})
	}

	pub fn write(&self, b: &mut [u8]) {
		assert_eq!(ENTRY_HEADER, 40);
		b[0..8].copy_from_slice(&self.address.to_be_bytes());
		b[8..16].copy_from_slice(&self.source.to_be_bytes());
		b[16..24].copy_from_slice(&self.last_addr.to_be_bytes());
		b[24..32].copy_from_slice(&self.timestamp.to_be_bytes());
		b[32..40].copy_from_slice(&self.len.to_be_bytes());
	}
}

#[derive(Clone)]
pub struct Entry {
	pub header: EntryHeader,
	pub data: Vec<u8>,
	pub source: u64,
	pub timestamp: u64,
	pub last_addr: u64,
}

impl Entry {
	pub fn copy_entry(&mut self, other: &Entry) {
		self.data.clear();
		self.data.extend_from_slice(other.data.as_slice());
		self.source = other.source;
		self.timestamp = other.timestamp;
		self.last_addr = other.last_addr;
	}

	pub fn header(&self) -> &EntryHeader {
		&self.header
	}

	pub fn new_empty() -> Self {
		Self {
			header: EntryHeader::new_empty(),
			data: Vec::new(),
			source: u64::MAX,
			timestamp: u64::MAX,
			last_addr: u64::MAX,
		}
	}

	pub fn with_header_and_data(&mut self, header: EntryHeader, data: &[u8]) {
		self.source = header.source;
		self.timestamp = header.timestamp;
		self.last_addr = header.last_addr;
		self.header = header;
		self.data.clear();
		self.data.extend_from_slice(data);
	}

	pub fn read_from_bytes(
		data: &[u8],
		addr: u64,
		entry: &mut Self,
	) -> Result<usize, MachError> {
		let header_bytes = &data[..ENTRY_HEADER];
		let header = EntryHeader::from_bytes(header_bytes, addr)?;

		if header.address == CHUNK_TAIL_VALUE {
			Err(MachError::ChunkTail)
		} else if header.address != addr {
			panic!("Internal error");
		} else {
			let start = ENTRY_HEADER;
			let end = start + header.len;

			entry.header = header;
			entry.data.clear();
			entry.data.extend_from_slice(&data[start..end]);
			entry.source = header.source;
			entry.timestamp = header.timestamp;
			entry.last_addr = header.last_addr;
			Ok(end)
		}
	}
}

//pub struct Entries(Vec<u8>);
//
//impl Entries {
//	pub fn _from_bytes(bytes: &[u8]) -> Self {
//		let len = usize::from_be_bytes(bytes[0..8].try_into().unwrap());
//		Entries(bytes[8..8 + len].into())
//	}
//
//	pub fn _as_bytes(&self, buffer: &mut Vec<u8>) {
//		buffer.extend_from_slice(&self.0.len().to_be_bytes());
//		buffer.extend_from_slice(self.0.as_slice());
//	}
//
//	pub fn _bytes_size(&self) -> usize {
//		8 + self.0.len()
//	}
//}
//
//impl From<Vec<u8>> for Entries {
//	fn from(x: Vec<u8>) -> Self {
//		Entries(x)
//	}
//}

pub struct BorrowedBlock<'a> {
	data: &'a [u8; BLOCK_SZ],
	_max_addr: u64,
}

impl<'a> BorrowedBlock<'a> {
	pub fn full_block(&self) -> &[u8; BLOCK_SZ] {
		self.data
	}
}

pub struct BlockReader {
	inner: *mut Inner,
	ref_count: *mut AtomicUsize,
}

impl Drop for BlockReader {
	fn drop(&mut self) {
		let count = unsafe { (*self.ref_count).fetch_sub(1, SeqCst) };

		// This would be the last one, so deallocate the box
		if count == 1 {
			let boxed = unsafe { Box::from_raw(self.inner) };
			drop(boxed);
			let boxed = unsafe { Box::from_raw(self.ref_count) };
			drop(boxed);
		}
	}
}

impl Clone for BlockReader {
	fn clone(&self) -> Self {
		unsafe {
			(*self.ref_count).fetch_add(1, SeqCst);
		}
		Self {
			inner: self.inner,
			ref_count: self.ref_count,
		}
	}
}

impl BlockReader {
	pub fn force_snapshot(&self) -> RoBlock {
		loop {
			match self.snapshot() {
				Ok(x) => return x,
				Err(BlockError::ConcurrentUpdate) => {
					return RoBlock::new_empty_snapshot()
				}
				Err(BlockError::NewUnsyncedBlock) => {
					return RoBlock::new_empty_snapshot()
				}
				_ => unreachable!(),
			}
		}
	}

	pub fn snapshot(&self) -> Result<RoBlock, BlockError> {
		let (data, max_addr) = unsafe { (*self.inner).read()? };
		assert!(data.len() > 0);
		Ok(RoBlock::from_block_and_max_addr(data, max_addr))
	}
}

unsafe impl Send for BlockReader {}
unsafe impl Sync for BlockReader {}

pub struct BlockWriter {
	inner: *mut Inner,
	ref_count: *mut AtomicUsize,
}

impl Drop for BlockWriter {
	fn drop(&mut self) {
		let count = unsafe { (*self.ref_count).fetch_sub(1, SeqCst) };
		// This would be the last one, so deallocate the box
		if count == 1 {
			let boxed = unsafe { Box::from_raw(self.inner) };
			drop(boxed);
			let boxed = unsafe { Box::from_raw(self.ref_count) };
			drop(boxed);
		}
	}
}

unsafe impl Send for BlockWriter {}
unsafe impl Sync for BlockWriter {}

impl BlockWriter {
	pub fn expected_next_addr(&self) -> u64 {
		unsafe { (*self.inner).expected_next_addr() }
	}

	pub fn new() -> Self {
		let mut inner = manually_boxed_zeroed::<Inner>();
		inner.init();
		let ptr = Box::into_raw(inner);
		let ref_count = Box::into_raw(Box::new(AtomicUsize::new(1)));
		Self {
			inner: ptr,
			ref_count,
		}
	}

	pub fn reader(&self) -> BlockReader {
		unsafe { (*self.ref_count).fetch_add(1, SeqCst) };
		BlockReader {
			inner: self.inner,
			ref_count: self.ref_count,
		}
	}

	#[inline(always)]
	pub fn push(
		&mut self,
		source: u64,
		last_addr: u64,
		timestamp: u64,
		data: &[u8],
	) -> Result<BlockPushResult, BlockError> {
		unsafe { (*self.inner).push(source, last_addr, timestamp, data) }
	}

	pub fn reset(&mut self, version: usize) {
		unsafe { (*self.inner).reset(version) }
	}

	#[inline]
	pub fn borrow_block(&self) -> BorrowedBlock {
		unsafe { (*self.inner).borrow_block() }
	}

	#[inline]
	pub fn sync(&self) -> u64 {
		unsafe { (*self.inner).sync() }
	}
}

#[derive(Debug)]
pub struct BlockPushResult {
	pub addr: u64,
	pub new_chunk: bool,
}

#[repr(C, align(512))]
struct Inner {
	// Contains actual data. This needs to be at the top so it maintains 512
	// byte alignment
	data: AlignedBlock,

	// Id of this block. Initially set to 0, then updated on reset.
	id: usize,

	// Keeps track of next offset in this block to write to
	offset: usize,

	// Keeps track of address of last item written into this block
	last_address: u64,
	sync_last_address: AtomicU64,

	// Versioning for this block, used when recycling
	sync_version: AtomicUsize,

	tmp_just_flushed: bool,
	tmp_item_count: usize,
}

impl Inner {
	fn expected_next_addr(&self) -> u64 {
		// calculate the address based on the last offset
		self.base_addr() + self.offset as u64
	}

	#[inline]
	fn init(&mut self) {
		self.offset = 0;
		self.id = 0;
		self.last_address = u64::MAX;
		self.tmp_just_flushed = false;
		self.tmp_item_count = 0;
		//self.data[..8].copy_from_slice(&self.offset.to_be_bytes());
		//self.data[8..16].copy_from_slice(&self.id.to_be_bytes());
	}

	#[inline]
	fn push_unchecked(
		&mut self,
		source: u64,
		last_addr: u64,
		timestamp: u64,
		data: &[u8],
	) -> u64 {
		// calculate the address based on the last offset
		let addr = self.base_addr() + self.offset as u64;

		let mut offset = self.offset;

		// Write entry header information
		let header = EntryHeader {
			address: addr,
			source,
			last_addr,
			timestamp,
			len: data.len(),
		};

		let header_bytes = &mut self.data[offset..offset + ENTRY_HEADER];
		header.write(header_bytes);

		offset += ENTRY_HEADER;

		// Write data
		self.data[offset..offset + data.len()].copy_from_slice(data);
		offset += data.len();

		// Write the address of this entry to the block header
		//self.data[..8].copy_from_slice(&addr.to_be_bytes());

		// Update struct metadata for next push or sync
		self.last_address = addr;
		self.offset = offset;
		self.tmp_item_count += 1;

		addr
	}

	#[inline]
	fn base_addr(&self) -> u64 {
		(self.id * BLOCK_SZ) as u64
	}

	#[inline(always)]
	fn push(
		&mut self,
		source: u64,
		last_addr: u64,
		timestamp: u64,
		data: &[u8],
	) -> Result<BlockPushResult, BlockError> {
		let total_data_len = data.len() + ENTRY_HEADER;
		let mut new_chunk = self.offset == 0;

		// Check to see if this fits in this chunk of data or should it be
		// aligned to the next chunk.
		let mut offset = self.offset;
		let last_chunk_offset = offset - (offset % CHUNK_SZ);
		let next_chunk_offset = last_chunk_offset + CHUNK_SZ;

		let required = offset + total_data_len + CHUNK_TAIL.len();

		if required >= BLOCK_SZ || required >= next_chunk_offset {
			// Close chunk with tail
			//println!("chunk count {}", self.tmp_item_count);
			self.tmp_item_count = 0;
			self.data[offset..offset + CHUNK_TAIL.len()]
				.copy_from_slice(&CHUNK_TAIL);

			// We add block header here so that all chunks have the same header
			// (including the first chunk at the start of the block)
			offset = next_chunk_offset;
			new_chunk = true;
		}

		//if offset + total_data_len + CHUNK_TAIL.len() >= next_chunk_offset {
		//	println!("chunk count {}", self.tmp_item_count);
		//	self.tmp_item_count = 0;

		//	// Close chunk with tail
		//	self.data[offset..offset + CHUNK_TAIL.len()]
		//		.copy_from_slice(&CHUNK_TAIL);

		//	// We add block header here so that all chunks have the same header
		//	// (including the first chunk at the start of the block)
		//    offset = next_chunk_offset;
		//    new_chunk = true;
		//}

		// if the offset and data now exceeds the block sz, this block is full;
		if required >= BLOCK_SZ {
			Err(BlockError::Full)
		} else {
			//if self.tmp_just_flushed {
			//	println!("offset just flushed self.offset {} offset {}", self.offset, offset);
			//}
			self.offset = offset;
			let addr = self.push_unchecked(source, last_addr, timestamp, data);
			let r = BlockPushResult { addr, new_chunk };
			//if self.tmp_just_flushed {
			//	println!("Address: {}", addr);
			//}
			self.tmp_just_flushed = false;
			Ok(r)
		}
	}

	#[inline]
	fn sync(&self) -> u64 {
		let addr = self.last_address;
		self.sync_last_address.store(addr, SeqCst);
		addr as u64
	}

	fn read(&self) -> Result<(Box<AlignedBlock>, u64), BlockError> {
		let version = self.sync_version.load(SeqCst);
		let addr = self.sync_last_address.load(SeqCst);
		if addr == 0 {
			return Err(BlockError::NewUnsyncedBlock);
		}
		if addr == u64::MAX {
			return Err(BlockError::NewUnsyncedBlock);
		}

		// Copy until the last entry
		let mut offset: usize = addr as usize % BLOCK_SZ;

		let header =
			EntryHeader::from_bytes(&self.data[offset..], addr).unwrap();
		let data_len = header.len;
		offset += ENTRY_HEADER + data_len;

		//println!("Last synced address {}, Copying until offset {}", addr, offset);

		let mut result = AlignedBlock::new_boxed(); //aligned_block_vec();
		result[..offset].copy_from_slice(&self.data[..offset]);

		// Add in tail to tell readers that they should stop reading here
		result[offset..offset + CHUNK_TAIL.len()]
			.copy_from_slice(&CHUNK_TAIL[..]);

		if self.sync_version.load(SeqCst) == version {
			Ok((result, addr as u64))
		} else {
			println!("Failed copy");
			Err(BlockError::ConcurrentUpdate)
		}
	}

	#[inline]
	fn reset(&mut self, id: usize) {
		// version control for lock-free sync with read
		self.sync_version.fetch_add(1, SeqCst);

		self.id = id;
		self.offset = 0;
		self.tmp_just_flushed = true;
		//println!("Flushing, new id: {}", id);

		// set this to max so read knows its a new block empty
		self.last_address = u64::MAX;
		self.sync_last_address.store(u64::MAX, SeqCst);

		// version control for lock-free sync with read
		self.sync_version.fetch_add(1, SeqCst);
	}

	fn borrow_block(&self) -> BorrowedBlock {
		let data = &self.data;
		let _max_addr = (self.offset + BLOCK_SZ * self.id) as u64;
		BorrowedBlock { data, _max_addr }
	}
}

#[cfg(test)]
mod test {
	use super::*;

	#[test]
	fn empty_no_sync() {
		let mut block = BlockWriter::new();
		let block_reader = block.reader();
		let snapshot = block_reader.snapshot();
		match snapshot {
			Err(BlockError::NewUnsyncedBlock) => {}
			_ => panic!("Wrong stuff"),
		}
	}

	#[test]
	fn empty_sync() {
		let mut block = BlockWriter::new();
		let block_reader = block.reader();
		block.sync();
		let snapshot = block_reader.snapshot();
		match snapshot {
			Err(BlockError::NewUnsyncedBlock) => {}
			_ => panic!("Wrong stuff"),
		}
	}

	#[test]
	fn empty_increment_version() {
		let mut block = BlockWriter::new();
		let block_reader = block.reader();
		let bytes = [0u8; 512];
		let mut last_addr = u64::MAX;
		for i in 0..3 {
			let last_addr = block.push(0, last_addr, i, &bytes).unwrap();
			println!("LAST_ADDR: {}", last_addr.addr);
		}
		block.sync();
		block.reset(2);
		println!("Snyching");
		block.sync();
		let snapshot = block_reader.snapshot();
		match snapshot {
			Err(BlockError::NewUnsyncedBlock) => {}
			_ => panic!("Wrong stuff"),
		}
	}

	#[test]
	fn end_to_end() {
		let mut block = BlockWriter::new();
		let block_reader = block.reader();
		let bytes = [0u8; 512];
		let mut last_addr = u64::MAX;
		for i in 0..3 {
			let last_addr = block.push(0, last_addr, i, &bytes).unwrap();
			println!("LAST_ADDR: {}", last_addr.addr);
		}
		block.sync();
		block.reset(2);
		for i in 0..3 {
			let last_addr = block.push(0, last_addr, i, &bytes).unwrap();
			println!("LAST_ADDR: {}", last_addr.addr);
		}
		block.sync();
		let snapshot = block_reader.snapshot().unwrap();
		println!("Snapshot range: {:?}", snapshot.address_range());
	}
}
