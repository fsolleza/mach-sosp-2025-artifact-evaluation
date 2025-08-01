use std::fs::*;

use libc::*;
use minstant::*;
use rand::prelude::*;
use std::io::prelude::*;
use std::path::{Path, PathBuf};
use std::time::Duration;
use std::{
	alloc::{alloc_zeroed, Layout},
	os::unix::prelude::*,
};

pub fn new_direct_file(path: &Path, size: usize) -> File {
	let flags = O_LARGEFILE | O_TRUNC | O_DIRECT | O_SYNC | O_RDWR | O_CREAT;

	//if !PRINTED_WARNING.swap(true, SeqCst) {
	//	println!("WARNING: THIS FILE IS NOT O_DIRECT");
	//}
	//let flags = O_LARGEFILE | O_TRUNC | O_RDWR | O_CREAT;
	let file = OpenOptions::new()
		.custom_flags(flags)
		.write(true)
		.mode(S_IRWXU)
		.open(path)
		.unwrap();
	file.set_len(size as u64).unwrap();
	file
}

#[repr(C, align(512))]
pub struct AlignedBlock {
	inner: [u8; 64 * 1024 * 1024],
}

pub fn manually_boxed_zeroed<T>() -> Box<T> {
	let layout = Layout::new::<T>();
	unsafe {
		let ptr = alloc_zeroed(layout) as *mut T;
		Box::from_raw(ptr)
	}
}

fn main() {
	let mut rng = rand::thread_rng();
	let mut data: Box<AlignedBlock> = manually_boxed_zeroed();
	for i in &mut data.inner[..] {
		*i = rng.gen();
	}

	let path = PathBuf::from("/nvme/data/tmp/can_delete/test_io");
	let mut file = new_direct_file(&path, 10 * 1024 * 1024 * 1024);

	let mut now = Instant::now();
	let mut counter = 0;
	loop {
		file.write(&data.inner[..]).unwrap();
		counter += data.inner.len();
		if now.elapsed() >= Duration::from_secs(1) {
			println!("{} Gib/s", counter as f64 / (1024. * 1024. * 1024.));
			counter = 0;
			now = Instant::now();
		}
	}
}
