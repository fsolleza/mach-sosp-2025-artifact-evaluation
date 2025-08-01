use std::ops::{Deref, DerefMut};

use serde::{Deserialize, Serialize};

use crate::{constants::BLOCK_SZ, utils::manually_boxed_zeroed};

#[repr(C, align(512))]
#[derive(Clone, Serialize, Deserialize)]
pub struct AlignedBlock {
	#[serde(with = "serde_arrays")]
	inner: [u8; BLOCK_SZ],
}

impl Deref for AlignedBlock {
	type Target = [u8; BLOCK_SZ];

	fn deref(&self) -> &Self::Target {
		&self.inner
	}
}

impl DerefMut for AlignedBlock {
	fn deref_mut(&mut self) -> &mut Self::Target {
		&mut self.inner
	}
}

impl AlignedBlock {
	pub fn new_boxed() -> Box<Self> {
		manually_boxed_zeroed::<AlignedBlock>()
	}

	pub fn write_to_vec(&self, vec: &mut Vec<u8>) {
		vec.extend_from_slice(&self.inner[..]);
	}

	pub fn read_from_slice(slice: &[u8]) -> (Box<Self>, usize) {
		let mut this = Self::new_boxed();
		this.inner[..].copy_from_slice(&slice[..BLOCK_SZ]);
		(this, BLOCK_SZ)
	}
}
