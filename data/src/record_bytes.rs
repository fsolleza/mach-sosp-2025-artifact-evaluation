use crate::{
	kv_op::*, packet::*, page_cache::*, record::RecordKind, syscall::*,
};
use utils::{
	micros_since_epoch, u32_from_bytes, u64_from_bytes, u64_to_byte_vec,
};

pub struct OwnedRecordBatchBytes {
	pub bytes: Vec<u8>,
}

pub struct RecordBatchBytes<'a> {
	pub bytes: &'a [u8],
}

impl<'a> RecordBatchBytes<'a> {
	pub fn new(bytes: &'a [u8]) -> Self {
		Self { bytes }
	}

	pub fn iterator(&self) -> RecordBatchBytesIterator {
		RecordBatchBytesIterator::new(self.bytes)
	}

	pub fn arrival_us(&self) -> u64 {
		u64_from_bytes(&self.bytes[..8])
	}

	pub fn data_len(&self) -> u64 {
		u64_from_bytes(&self.bytes[8..16])
	}

	pub fn record_count(&self) -> u64 {
		self.data_len()
	}

	pub fn prep_for_tcp(&self, fishstore: bool) -> (Vec<u8>, Vec<u8>) {
		if fishstore {
			return self.prep_for_tcp_fishstore();
		}

		let mut iterator = self.iterator();

		let mut kvlog = 0;
		let mut syscall_event = 0;
		let mut page_cache_event = 0;
		let mut packet_capture = 0;
		while let Some(bytes) = iterator.next_record_bytes() {
			match bytes.kind() {
				RecordKind::KVLog => kvlog += 1,
				RecordKind::SyscallEvent => syscall_event += 1,
				RecordKind::PageCacheEvent => page_cache_event += 1,
				RecordKind::PacketCapture => packet_capture += 1,
			}
		}

		let mut header = Vec::new();
		u64_to_byte_vec(kvlog, &mut header);
		u64_to_byte_vec(syscall_event, &mut header);
		u64_to_byte_vec(page_cache_event, &mut header);
		u64_to_byte_vec(packet_capture, &mut header);
		u64_to_byte_vec(self.bytes.len() as u64, &mut header);

		let bytes: Vec<u8> = self.bytes.into();
		(header, bytes)
	}

	fn prep_for_tcp_fishstore(&self) -> (Vec<u8>, Vec<u8>) {
		let arrival_us = self.arrival_us();
		let data_len = self.data_len();
		let mut iterator = self.iterator();
		let timestamp_now = micros_since_epoch();

		let mut modified_bytes = Vec::new();
		let mut kvlog = 0;
		let mut syscall_event = 0;
		let mut page_cache_event = 0;
		let mut packet_capture = 0;

		u64_to_byte_vec(arrival_us, &mut modified_bytes);
		u64_to_byte_vec(data_len, &mut modified_bytes);

		// NOTE: This mimics the format for a batch of bytes as specified in
		// in this struct. But, we modify record format to include the
		// arrival timestamp as the first 8 bytes of the record.
		while let Some(bytes) = iterator.next_record_bytes() {
			match bytes.kind() {
				RecordKind::KVLog => kvlog += 1,
				RecordKind::SyscallEvent => syscall_event += 1,
				RecordKind::PageCacheEvent => page_cache_event += 1,
				RecordKind::PacketCapture => packet_capture += 1,
			}
			u64_to_byte_vec(timestamp_now, &mut modified_bytes);
			modified_bytes.extend_from_slice(bytes.bytes);
		}

		let mut header = Vec::new();
		u64_to_byte_vec(kvlog, &mut header);
		u64_to_byte_vec(syscall_event, &mut header);
		u64_to_byte_vec(page_cache_event, &mut header);
		u64_to_byte_vec(packet_capture, &mut header);
		u64_to_byte_vec(modified_bytes.len() as u64, &mut header);

		(header, modified_bytes)
	}
}

pub struct RecordBatchBytesIterator<'a> {
	bytes: &'a [u8],
	offset: usize,
}

impl<'a> RecordBatchBytesIterator<'a> {
	pub fn new(bytes: &'a [u8]) -> Self {
		Self {
			bytes,

			// start at 16 since first 16 bytes are arrival time and data length
			offset: 16,
		}
	}

	pub fn next_record_bytes(&mut self) -> Option<RecordBytes> {
		if self.offset >= self.bytes.len() {
			None
		} else {
			let start = self.offset;

			// Get RawRecord kind value
			let x = u64_from_bytes(&self.bytes[start + 8..start + 16]);

			let mut end = start + 16;
			match x {
				0 => end += KVLog::BYTES_SIZE,
				1 => end += SyscallEvent::BYTES_SIZE,
				2 => end += PageCacheEvent::BYTES_SIZE,
				3 => end += PacketCapture::BYTES_SIZE,
				_ => panic!("Unhandled value"),
			}
			self.offset = end;

			Some(RecordBytes {
				bytes: &self.bytes[start..end],
			})
		}
	}
}

#[derive(Copy, Clone)]
pub struct RecordBytes<'a> {
	pub bytes: &'a [u8],
}

impl<'a> RecordBytes<'a> {
	pub fn source_id(&self) -> u64 {
		let source_id = u64_from_bytes(&self.bytes[..8]);
		source_id
	}

	pub fn kind(&self) -> RecordKind {
		let x = u64_from_bytes(&self.bytes[8..16]);
		match x {
			0 => RecordKind::KVLog,
			1 => RecordKind::SyscallEvent,
			2 => RecordKind::PageCacheEvent,
			3 => RecordKind::PacketCapture,
			_ => panic!("Unhandled value"),
		}
	}

	pub fn as_kv_log(&self) -> Option<KVLog> {
		let x = u64_from_bytes(&self.bytes[8..16]);
		match x {
			0 => {
				let (x, _sz) = KVLog::from_bytes(&self.bytes[16..]);
				Some(x)
			}
			1..4 => None,
			_ => panic!("Unhandled value"),
		}
	}

	pub fn is_kv_log(&self) -> bool {
		let x = u64_from_bytes(&self.bytes[8..16]);
		match x {
			0 => true,
			1..4 => false,
			_ => panic!("Unhandled value"),
		}
	}

	pub fn kv_log_tid(&self) -> Option<u64> {
		if self.is_kv_log() {
			Some(u64_from_bytes(&self.bytes[16..24]))
		} else {
			None
		}
	}

	pub fn kv_log_op(&self) -> Option<KVOp> {
		if self.is_kv_log() {
			let op_val = u64_from_bytes(&self.bytes[24..32]);
			let op = match op_val {
				0 => KVOp::Read,
				1 => KVOp::Write,
				_ => panic!("Unimplemented!"),
			};
			Some(op)
		} else {
			None
		}
	}

	pub fn kv_log_timestamp(&self) -> Option<u64> {
		if self.is_kv_log() {
			Some(u64_from_bytes(&self.bytes[32..40]))
		} else {
			None
		}
	}

	pub fn kv_log_duration_us(&self) -> Option<u64> {
		if self.is_kv_log() {
			Some(u64_from_bytes(&self.bytes[40..48]))
		} else {
			None
		}
	}

	pub fn as_syscall_event(&self) -> Option<SyscallEvent> {
		let x = u64_from_bytes(&self.bytes[8..16]);
		match x {
			0 => None,
			1 => {
				let (x, _sz) = SyscallEvent::from_bytes(&self.bytes[16..]);
				Some(x)
			}
			2..4 => None,
			_ => panic!("Unhandled value"),
		}
	}

	pub fn is_syscall_event(&self) -> bool {
		let x = u64_from_bytes(&self.bytes[8..16]);
		match x {
			0 => false,
			1 => true,
			2..4 => false,
			_ => panic!("Unhandled value"),
		}
	}

	pub fn syscall_event_pid(&self) -> Option<u32> {
		if self.is_syscall_event() {
			Some(u32_from_bytes(&self.bytes[16..20]))
		} else {
			None
		}
	}

	pub fn syscall_event_tid(&self) -> Option<u32> {
		if self.is_syscall_event() {
			Some(u32_from_bytes(&self.bytes[20..24]))
		} else {
			None
		}
	}

	pub fn syscall_event_syscall_number(&self) -> Option<u64> {
		if self.is_syscall_event() {
			Some(u64_from_bytes(&self.bytes[24..32]))
		} else {
			None
		}
	}

	pub fn syscall_event_start_time(&self) -> Option<u64> {
		if self.is_syscall_event() {
			Some(u64_from_bytes(&self.bytes[32..40]))
		} else {
			None
		}
	}

	pub fn syscall_event_duration_us(&self) -> Option<u64> {
		if self.is_syscall_event() {
			Some(u64_from_bytes(&self.bytes[40..48]))
		} else {
			None
		}
	}

	pub fn as_page_cache_event(&self) -> Option<PageCacheEvent> {
		let x = u64_from_bytes(&self.bytes[8..16]);
		match x {
			0..2 => None,
			2 => {
				let (x, _sz) = PageCacheEvent::from_bytes(&self.bytes[16..]);
				Some(x)
			}
			3 => None,
			_ => panic!("Unhandled value"),
		}
	}

	pub fn is_page_cache_event(&self) -> bool {
		let x = u64_from_bytes(&self.bytes[8..16]);
		match x {
			0..2 => false,
			2 => true,
			3 => false,
			_ => panic!("Unhandled value"),
		}
	}

	pub fn is_packet_capture(&self) -> bool {
		let x = u64_from_bytes(&self.bytes[8..16]);
		match x {
			0..3 => false,
			3 => true,
			_ => panic!("Unhandled value"),
		}
	}

	pub fn as_packet_capture(&self) -> Option<PacketCapture> {
		let x = u64_from_bytes(&self.bytes[8..16]);
		match x {
			0..3 => None,
			3 => {
				let (x, _sz) = PacketCapture::from_bytes(&self.bytes[16..]);
				Some(x)
			}
			_ => panic!("Unhandled value"),
		}
	}
}
