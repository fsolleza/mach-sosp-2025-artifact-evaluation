use utils::{u32_from_bytes, u32_to_bytes, u64_from_bytes, u64_to_bytes};

#[derive(Copy, Clone, Debug, Default)]
pub struct SyscallEvent {
	pub pid: u32,
	pub tid: u32,
	pub syscall_number: u64,
	pub start_time: u64,
	pub duration_us: u64,
}

impl SyscallEvent {
	pub const BYTES_SIZE: usize = 32;

	pub(crate) fn to_bytes(&self, data: &mut [u8]) -> usize {
		u32_to_bytes(self.pid, &mut data[0..4]);
		u32_to_bytes(self.tid, &mut data[4..8]);
		u64_to_bytes(self.syscall_number, &mut data[8..16]);
		u64_to_bytes(self.start_time, &mut data[16..24]);
		u64_to_bytes(self.duration_us, &mut data[24..32]);
		assert_eq!(Self::BYTES_SIZE, 32);
		32
	}

	pub(crate) fn from_bytes(bytes: &[u8]) -> (Self, usize) {
		let pid = u32_from_bytes(&bytes[..4]);
		let tid = u32_from_bytes(&bytes[4..8]);
		let syscall_number = u64_from_bytes(&bytes[8..16]);
		let start_time = u64_from_bytes(&bytes[16..24]);
		let duration_us = u64_from_bytes(&bytes[24..32]);
		assert_eq!(Self::BYTES_SIZE, 32);
		let result = Self {
			pid,
			tid,
			syscall_number,
			start_time,
			duration_us,
		};
		(result, 32)
	}

	pub(crate) fn to_byte_vec(&self, data: &mut Vec<u8>) {
		let l = data.len();
		data.resize(l + 32, 0);
		assert_eq!(Self::BYTES_SIZE, self.to_bytes(&mut data[l..]));
	}
}
