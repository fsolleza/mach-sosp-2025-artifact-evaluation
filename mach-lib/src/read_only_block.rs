use serde::{Deserialize, Serialize};

use crate::{
	aligned::AlignedBlock, block::EntryHeader, block::CHUNK_TAIL_VALUE,
	constants::*, error::MachError, read_only_chunk::RoChunk, utils::*,
};

#[derive(Serialize, Deserialize)]
pub struct RoBlock {
	data: Box<AlignedBlock>,
	max_addr: u64,
}

impl Clone for RoBlock {
	fn clone(&self) -> Self {
		let mut new = RoBlock::new_empty();
		new.data[..].copy_from_slice(&self.data[..]);
		new.max_addr = self.max_addr;
		new
	}
}

impl RoBlock {
	pub fn new_empty() -> Self {
		RoBlock {
			data: AlignedBlock::new_boxed(),
			max_addr: u64::MAX,
		}
	}

	pub fn new_empty_snapshot() -> Self {
		RoBlock {
			data: AlignedBlock::new_boxed(),
			max_addr: u64::MAX,
		}
	}

	pub fn read_chunk_with_address(&self, addr: u64) -> RoChunk {
		let addr_range = self.address_range();
		assert!(addr_range.min <= addr && addr <= addr_range.max);

		let addr = addr as usize;
		let offset_in_block = addr % BLOCK_SZ;

		// align to the chunk
		let chunk_offset_in_block =
			offset_in_block - offset_in_block % CHUNK_SZ;
		let chunk_start = chunk_offset_in_block;
		let chunk_end = chunk_offset_in_block + CHUNK_SZ;

		// get min address of this chunk
		let chunk_min_addr = addr - addr % CHUNK_SZ;

		//println!("Reading chunk from RoBlock.");
		//println!("{:?}", &self.data[chunk_start..chunk_end]);
		RoChunk::from_slice(
			&self.data[chunk_start..chunk_end],
			chunk_min_addr as u64,
		)
	}

	pub fn from_block_and_max_addr(
		data: Box<AlignedBlock>,
		max_addr: u64,
	) -> Self {
		Self { data, max_addr }
	}

	// #[inline]
	// pub fn id_from_addr(addr: u64) -> u64 {
	// 	addr / (BLOCK_SZ as u64)
	// }

	pub fn address_range(&self) -> AddressRange {
		let base_block_addr = BLOCK_SZ * (self.max_addr as usize / BLOCK_SZ);
		let min_addr = base_block_addr;
		AddressRange {
			min: min_addr as u64,
			max: self.max_addr,
		}
	}

	// Given and address, returns the slice (including header) that contains
	// data from that address, and the slice's start and end offsets in this
	// block.
	fn read_addr(&self, addr: u64) -> Result<(&[u8], usize, usize), MachError> {
		let offset = addr as usize % BLOCK_SZ;

		let header = &self.data[offset..offset + EntryHeader::SIZE];
		let header = EntryHeader::from_bytes(header, addr)?;
		if header.address == CHUNK_TAIL_VALUE {
			Err(MachError::ChunkTail)
		} else {
			let len = header.len;
			let start = offset + EntryHeader::SIZE;
			let end = start + len;
			Ok((&self.data[offset..end], offset, end))
		}
	}

	pub fn read_unchecked_entry_bytes(
		&self,
		addr: u64,
	) -> Result<&[u8], MachError> {
		let (slice, _, _) = self.read_addr(addr)?;
		Ok(slice)
	}

	pub fn is_empty(&self) -> bool {
		self.max_addr == u64::MAX
	}

	pub fn write_to_vec(&self, vec: &mut Vec<u8>) {
		vec.extend_from_slice(&self.max_addr.to_be_bytes());
		self.data.write_to_vec(vec);
	}

	pub fn read_from_slice(slice: &[u8]) -> (Self, usize) {
		let max_addr = u64::from_be_bytes(slice[..8].try_into().unwrap());
		let (data, sz) = AlignedBlock::read_from_slice(&slice[8..]);
		(Self { data, max_addr }, sz + 8)
	}
}
