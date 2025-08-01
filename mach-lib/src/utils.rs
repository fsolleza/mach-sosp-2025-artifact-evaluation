use std::{
	alloc::{alloc_zeroed, Layout},
	fs::{File, OpenOptions},
	os::unix::prelude::*,
	path::Path,
};

//static PRINTED_WARNING: AtomicBool = AtomicBool::new(false);

// pub fn null_file(path: &Path, size: usize) -> File {
// 	use libc::*;
//
// 	let file = OpenOptions::new()
// 		.write(true)
// 		.open("/dev/null")
// 		.unwrap();
// 	file
// }

pub fn new_file(path: &Path, size: usize) -> File {
	let f = new_direct_file(path, size);
	f
}

fn new_direct_file(path: &Path, size: usize) -> File {
	use libc::*;
	let flags = O_LARGEFILE | O_TRUNC | O_DIRECT | O_SYNC | O_RDWR | O_CREAT;

	let file = OpenOptions::new()
		.custom_flags(flags)
		.write(true)
		.mode(S_IRWXU)
		.open(path)
		.unwrap();
	file.set_len(size as u64).unwrap();
	file
}

pub fn read_direct_file(path: &Path) -> File {
	use libc::*;
	let flags = O_RDONLY;
	let file = OpenOptions::new()
		.custom_flags(flags)
		.read(true)
		.open(path)
		.unwrap();
	file
}

pub fn manually_boxed_zeroed<T>() -> Box<T> {
	let layout = Layout::new::<T>();
	unsafe {
		let ptr = alloc_zeroed(layout) as *mut T;
		Box::from_raw(ptr)
	}
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub struct AddressRange {
	pub min: u64,
	pub max: u64,
}

impl AddressRange {
	pub fn contains(&self, addr: u64) -> bool {
		addr >= self.min && addr <= self.max
	}

	pub fn excludes(&self, other: &Self) -> bool {
		assert!(self.min <= self.max);
		(self.min >= other.max) || (self.max <= other.min)
	}

	pub fn is_before(&self, other: &Self) -> bool {
		self.max < other.min
	}
}
