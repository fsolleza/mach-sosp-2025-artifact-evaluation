#[derive(Copy, Clone, Debug, Default)]
pub struct PageCacheEvent {
	pub pid: u32,
	pub tid: u32,
	pub timestamp_us: u64,
	pub pfn: i64,
	pub i_ino: i64,
	pub index: i64,
	pub s_dev: i32,
}

impl PageCacheEvent {
	pub const BYTES_SIZE: usize = 44;

	pub(crate) fn to_bytes(&self, data: &mut [u8]) -> usize {
		data[0..4].copy_from_slice(&self.pid.to_be_bytes());
		data[4..8].copy_from_slice(&self.tid.to_be_bytes());
		data[8..16].copy_from_slice(&self.timestamp_us.to_be_bytes());
		data[16..24].copy_from_slice(&self.pfn.to_be_bytes());
		data[24..32].copy_from_slice(&self.i_ino.to_be_bytes());
		data[32..40].copy_from_slice(&self.index.to_be_bytes());
		data[40..44].copy_from_slice(&self.s_dev.to_be_bytes());
		assert_eq!(44, Self::BYTES_SIZE);
		Self::BYTES_SIZE
	}

	pub(crate) fn to_byte_vec(&self, data: &mut Vec<u8>) {
		let l = data.len();
		data.resize(l + 44, 0);
		assert_eq!(Self::BYTES_SIZE, self.to_bytes(&mut data[l..]));
	}

	pub(crate) fn from_bytes(data: &[u8]) -> (Self, usize) {
		let pid = u32::from_be_bytes(data[..4].try_into().unwrap());
		let tid = u32::from_be_bytes(data[4..8].try_into().unwrap());
		let timestamp_us = u64::from_be_bytes(data[8..16].try_into().unwrap());
		let pfn = i64::from_be_bytes(data[16..24].try_into().unwrap());
		let i_ino = i64::from_be_bytes(data[24..32].try_into().unwrap());
		let index = i64::from_be_bytes(data[32..40].try_into().unwrap());
		let s_dev = i32::from_be_bytes(data[40..44].try_into().unwrap());
		let read = 44;
		assert_eq!(read, Self::BYTES_SIZE);

		let result = Self {
			pid,
			tid,
			timestamp_us,
			pfn,
			i_ino,
			index,
			s_dev,
		};
		(result, read)
	}
}
