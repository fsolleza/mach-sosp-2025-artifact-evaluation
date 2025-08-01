
use crate::block::BlockWriter;
use crossbeam::channel::{Sender, Receiver};

fn io_uring_writer(rx: Receiver<(BlockWriter, Sender<BlockWriter>)>) {

	let mut ring = IoUring::new(8).unwrap();
	while let Some(b) = rx.recv() {
		let op = opcode::Write::new(
			types::Fd(self.file.as_raw_fd()),
			data,
			BLOCK_SZ as u32,
		)
		.build()
		.user_data(self.block_counter as u64);

		unsafe {
			self.ring
				.submission()
				.push(&op)
				.expect("submission queue is full");
		}
		self.ring.submit();
	}
}

fn io_uring_consumer(
