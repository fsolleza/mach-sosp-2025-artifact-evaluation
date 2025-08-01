use utils::{u64_from_bytes, u64_to_bytes};

#[derive(Debug, Copy, Clone)]
pub struct PacketCapture {
	pub dst_port: u64,
	pub src_port: u64,
	pub dst_addr: u64,
	pub src_addr: u64,
	pub tot_len: u64,
	pub timestamp: u64,
}

impl PacketCapture {
	pub const BYTES_SIZE: usize = 48;

	pub(crate) fn to_bytes(&self, data: &mut [u8]) -> usize {
		u64_to_bytes(self.dst_port, &mut data[..8]);
		u64_to_bytes(self.src_port, &mut data[8..16]);
		u64_to_bytes(self.dst_addr, &mut data[16..24]);
		u64_to_bytes(self.src_addr, &mut data[24..32]);
		u64_to_bytes(self.tot_len, &mut data[32..40]);
		u64_to_bytes(self.timestamp, &mut data[40..48]);
		assert_eq!(48, Self::BYTES_SIZE);
		Self::BYTES_SIZE
	}

	pub(crate) fn from_bytes(bytes: &[u8]) -> (Self, usize) {
		let dst_port = u64_from_bytes(&bytes[..8]);
		let src_port = u64_from_bytes(&bytes[8..16]);
		let dst_addr = u64_from_bytes(&bytes[16..24]);
		let src_addr = u64_from_bytes(&bytes[24..32]);
		let tot_len = u64_from_bytes(&bytes[32..40]);
		let timestamp = u64_from_bytes(&bytes[40..48]);
		assert_eq!(48, Self::BYTES_SIZE);

		let result = Self {
			dst_port,
			src_port,
			dst_addr,
			src_addr,
			tot_len,
			timestamp,
		};
		(result, 48)
	}

	pub(crate) fn to_byte_vec(&self, data: &mut Vec<u8>) {
		let l = data.len();
		data.resize(l + 48, 0);
		assert_eq!(Self::BYTES_SIZE, self.to_bytes(&mut data[l..]));
	}
}
