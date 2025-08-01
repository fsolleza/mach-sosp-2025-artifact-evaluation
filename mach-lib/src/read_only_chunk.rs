use crate::{
	block::EntryHeader, constants::CHUNK_SZ, error::MachError,
	persistent_read_ctx::PersistentCtx, utils::manually_boxed_zeroed,
	utils::AddressRange,
};
use serde::*;
use std::{
	ops::{Deref, DerefMut},
	rc::Rc,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RoChunkArray {
	#[serde(with = "serde_arrays")]
	data: [u8; CHUNK_SZ],
}

impl RoChunkArray {
	pub fn write_to_vec(&self, vec: &mut Vec<u8>) {
		vec.extend_from_slice(&self.data[..]);
	}

	pub fn read_from_slice(slice: &[u8]) -> (Box<Self>, usize) {
		let mut this = Self::new_boxed();
		this.data[..].copy_from_slice(&slice[..CHUNK_SZ]);
		(this, CHUNK_SZ)
	}

	pub fn new_boxed() -> Box<Self> {
		manually_boxed_zeroed::<Self>()
	}
}

impl Deref for RoChunkArray {
	type Target = [u8; CHUNK_SZ];
	fn deref(&self) -> &Self::Target {
		&self.data
	}
}

impl DerefMut for RoChunkArray {
	fn deref_mut(&mut self) -> &mut Self::Target {
		&mut self.data
	}
}

#[derive(Clone)]
pub struct RoChunkRef {
	inner: Rc<RoChunk>,
}

impl std::ops::Deref for RoChunkRef {
	type Target = RoChunk;
	fn deref(&self) -> &Self::Target {
		&*self.inner
	}
}

#[derive(Clone, Serialize, Deserialize)]
pub struct RoChunk {
	data: Box<RoChunkArray>,
	min_addr: u64,
	max_addr: u64,
}

impl RoChunk {
	//pub fn parse(&self) -> Vec<RoChunkEntry> {
	//	RoChunkEntry::parse_chunk(self)
	//}

	//pub fn clone_to_chunk_ref(&self) -> RoChunkRef {
	//	let inner = Rc::new(self.clone());
	//	RoChunkRef { inner }
	//}

	pub fn from_slice(slice: &[u8], min_addr: u64) -> Self {
		assert!(min_addr % CHUNK_SZ as u64 == 0);
		let mut data = RoChunkArray::new_boxed();
		data.copy_from_slice(&slice[..CHUNK_SZ]);
		RoChunk {
			data,
			min_addr,
			max_addr: min_addr + CHUNK_SZ as u64,
		}
	}

	pub fn new_empty() -> Self {
		RoChunk {
			data: RoChunkArray::new_boxed(),
			min_addr: u64::MAX,
			max_addr: u64::MAX,
		}
	}

	pub fn load_persistent_with_addr(
		&mut self,
		address: u64,
		ctx: &mut PersistentCtx,
	) {
		ctx.read_chunk_addr(address, &mut self.data[..]);

		let min_addr = address - address % CHUNK_SZ as u64;
		let max_addr = min_addr + CHUNK_SZ as u64;

		self.min_addr = min_addr;
		self.max_addr = max_addr;
	}

	//pub fn load_persistent_with_id(
	//	&mut self,
	//	chunk_id: u64,
	//	storage_config: StorageConfig,
	//) {
	//	let addr = chunk_id * CHUNK_SZ as u64;
	//	self.load_persistent_with_addr(addr, storage_config);
	//}

	//#[inline]
	//pub fn id_from_addr(addr: u64) -> u64 {
	//	todo!()
	//}

	pub fn address_range(&self) -> AddressRange {
		AddressRange {
			min: self.min_addr,
			max: self.max_addr,
		}
	}

	pub fn contains(&self, addr: u64) -> bool {
		self.min_addr <= addr && addr < self.max_addr
	}

	// Given and address, returns the slice (including header) that contains
	// data from that address, and the slice's start and end offsets in this
	// block.
	fn read_addr(&self, addr: u64) -> Result<(&[u8], usize, usize), MachError> {
		let offset = addr as usize % CHUNK_SZ;

		let header = &self.data[offset..offset + EntryHeader::SIZE];
		let header = EntryHeader::from_bytes(header, addr)?;
		let len = header.len;

		let start = offset + EntryHeader::SIZE;
		let end = start + len;
		Ok((&self.data[offset..end], offset, end))
	}

	pub fn read_unchecked_entry_bytes(
		&self,
		addr: u64,
	) -> Result<&[u8], MachError> {
		Ok(self.read_addr(addr)?.0)
	}

	pub fn is_empty(&self) -> bool {
		self.max_addr == u64::MAX
	}

	pub fn write_to_vec(&self, vec: &mut Vec<u8>) {
		vec.extend_from_slice(&self.min_addr.to_be_bytes());
		vec.extend_from_slice(&self.max_addr.to_be_bytes());
		self.data.write_to_vec(vec);
	}

	pub fn read_from_slice(slice: &[u8]) -> (Self, usize) {
		let min_addr = u64::from_be_bytes(slice[..8].try_into().unwrap());
		let max_addr = u64::from_be_bytes(slice[8..16].try_into().unwrap());
		let (data, sz) = RoChunkArray::read_from_slice(&slice[8..]);
		(
			Self {
				data,
				min_addr,
				max_addr,
			},
			sz + 16,
		)
	}
}
