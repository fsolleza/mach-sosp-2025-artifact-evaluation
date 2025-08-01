use utils::{u64_from_bytes, u64_to_bytes};

#[derive(Default, Copy, Clone, Debug)]
pub struct KVLog {
	pub tid: u64,
	pub op: KVOp,
	pub timestamp: u64,
	pub duration_us: u64,
}

impl KVLog {
	pub const BYTES_SIZE: usize = 32;

	pub(crate) fn to_bytes(&self, data: &mut [u8]) -> usize {
		let op_val: u64 = match self.op {
			KVOp::Read => 0u64,
			KVOp::Write => 1u64,
		};

		u64_to_bytes(self.tid, &mut data[..8]);
		u64_to_bytes(op_val, &mut data[8..16]);
		u64_to_bytes(self.timestamp, &mut data[16..24]);
		u64_to_bytes(self.duration_us, &mut data[24..32]);
		assert_eq!(32, Self::BYTES_SIZE);
		32
	}

	pub(crate) fn from_bytes(bytes: &[u8]) -> (Self, usize) {
		let tid = u64_from_bytes(&bytes[..8]);
		let op_val = u64_from_bytes(&bytes[8..16]);
		let ts = u64_from_bytes(&bytes[16..24]);
		let dur = u64_from_bytes(&bytes[24..32]);
		assert_eq!(32, Self::BYTES_SIZE);

		let op = match op_val {
			0 => KVOp::Read,
			1 => KVOp::Write,
			_ => panic!("Unimplemented!"),
		};

		let result = Self {
			tid,
			op,
			timestamp: ts,
			duration_us: dur,
		};
		(result, 32)
	}

	pub(crate) fn to_byte_vec(&self, data: &mut Vec<u8>) {
		let l = data.len();
		data.resize(l + 32, 0);
		assert_eq!(Self::BYTES_SIZE, self.to_bytes(&mut data[l..]));
	}
}

#[derive(Default, Copy, Clone, Debug, Hash, Eq, PartialEq)]
pub enum KVOp {
	#[default]
	Read,
	Write,
}

impl KVOp {
	pub fn as_val(&self) -> u64 {
		match self {
			KVOp::Read => 0,
			KVOp::Write => 1,
		}
	}
}
