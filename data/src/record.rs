use crate::{kv_op::*, packet::*, page_cache::*, syscall::*};
use lzzzz::lz4;
use numtoa::*;
use std::hash::Hash;
use utils::*;
use utils::{hash64, HashMap};

#[derive(Debug, Clone, Hash, Copy, Eq, PartialEq)]
pub enum SourceInformation {
	KVLog { op: KVOp },
	SyscallEvent { syscall_number: u64 },
	PageCacheEvent,
	PacketCapture,
}

impl SourceInformation {
	pub fn as_id(&self) -> u64 {
		hash64(self)
	}
}

#[derive(Debug, Clone, Copy)]
pub enum RecordKind {
	KVLog = 0,
	SyscallEvent = 1,
	PageCacheEvent = 2,
	PacketCapture = 3,
}

#[derive(Debug, Clone)]
pub enum RawRecord {
	KVLog(KVLog),
	SyscallEvent(SyscallEvent),
	PageCacheEvent(PageCacheEvent),
	PacketCapture(PacketCapture),
}

#[derive(Clone)]
pub struct Record {
	pub source_id: u64,
	pub data: RawRecord,
}

#[derive(Clone)]
pub struct RecordBatch {
	pub arrival_us: u64,
	pub data: Vec<Record>,
}

pub struct RecordBatchSerializer {
	data: Vec<u8>,
	tmp: Vec<u8>,
	compressed: Vec<u8>,
}

pub struct RecordBatchesDeserializer {
	data: Vec<u8>,
	offset: usize,
	tmp: Vec<u8>,
}

impl RecordBatchesDeserializer {
	pub fn reset(&mut self) {
		self.offset = 0;
	}

	pub fn new(data: Vec<u8>) -> Self {
		Self {
			data,
			offset: 0,
			tmp: Vec::new(),
		}
	}

	pub fn next_batch_bytes(&mut self) -> Option<&[u8]> {
		if self.offset == self.data.len() {
			return None;
		}

		let mut o = self.offset;

		let bytes_len = u64_from_bytes(&self.data[o..o + 8]) as usize;

		o += 8;
		let compressed_len = u64_from_bytes(&self.data[o..o + 8]) as usize;

		o += 8;
		let compressed_bytes = &self.data[o..o + compressed_len];

		o += compressed_len;
		self.offset = o;

		// Decompress
		self.tmp.clear();
		self.tmp.resize(bytes_len, 0);
		assert_eq!(
			lz4::decompress(compressed_bytes, &mut self.tmp[..]).unwrap(),
			bytes_len
		);

		Some(self.tmp.as_slice())
	}

	pub fn next_batch(&mut self) -> Option<RecordBatch> {
		let bytes = self.next_batch_bytes()?;
		// Deserialize
		let (batch, sz) = RecordBatch::from_bytes(bytes);
		assert_eq!(sz, bytes.len());
		Some(batch)
	}
}

impl RecordBatchSerializer {
	pub fn new() -> Self {
		Self {
			data: Vec::new(),
			tmp: Vec::new(),
			compressed: Vec::new(),
		}
	}

	pub fn into_vec(self) -> Vec<u8> {
		self.data
	}

	pub fn push_batch(&mut self, batch: &RecordBatch) {
		// serialize to bytes
		self.tmp.clear();
		batch.to_byte_vec(&mut self.tmp);
		let bytes_len = self.tmp.len();
		let max_len = lz4::max_compressed_size(bytes_len);

		// compress
		self.compressed.clear();
		self.compressed.resize(max_len, 0);
		let compressed_len = lz4::compress(
			self.tmp.as_slice(),
			self.compressed.as_mut(),
			lz4::ACC_LEVEL_DEFAULT,
		)
		.unwrap();

		// write into result
		let l = self.data.len();
		self.data.resize(l + 16, 0);

		// Write bytes len, compressed len, compressed bytes
		u64_to_bytes(bytes_len as u64, &mut self.data[l..l + 8]);
		u64_to_bytes(compressed_len as u64, &mut self.data[l + 8..l + 16]);
		self.data
			.extend_from_slice(&self.compressed[..compressed_len]);
	}
}

impl RecordBatch {
	pub fn as_questdb_batch(
		&self,
		first_ts_us: u64,
	) -> questdb::ingress::Buffer {
		let ts_ns = first_ts_us * 1_000;
		let mut duplicate_check = HashMap::new();
		let mut buf = [0u8; 20];

		let mut buffer = questdb::ingress::Buffer::new();
		for record in self.data.iter() {
			// increment timestamp for this source
			let this_ts = {
				let ts =
					duplicate_check.entry(record.source_id).or_insert(ts_ns);
				let this_ts = *ts;
				*ts += 1;
				this_ts
			};

			match record.data {
				RawRecord::KVLog(x) => {
					buffer
						.table("kvlog")
						.unwrap()
						.symbol(
							"source",
							record.source_id.numtoa_str(10, &mut buf),
						)
						.unwrap()
						.column_i64("tid", x.tid as i64)
						.unwrap()
						.column_i64("op", x.op.as_val() as i64)
						.unwrap()
						.column_i64("item_ts", x.timestamp as i64)
						.unwrap()
						.column_i64("duration_us", x.duration_us as i64)
						.unwrap()
						.at(questdb::ingress::TimestampNanos::new(
							this_ts as i64,
						))
						.unwrap();
				}

				RawRecord::SyscallEvent(_) => {
					todo!()
				}

				RawRecord::PageCacheEvent(_) => {
					todo!()
				}

				RawRecord::PacketCapture(_) => {
					todo!()
				}
			}
		}

		buffer
	}

	pub fn as_line_protocol(&self) -> String {
		let arrival_us = self.arrival_us;
		let mut _buf = [0u8; 20];
		let ts_ns = arrival_us * 1_000;
		let mut duplicate_check = HashMap::new();

		let mut strings = Vec::new();
		for record in self.data.iter() {
			// increment timestamp for this source
			let this_ts = {
				let ts =
					duplicate_check.entry(record.source_id).or_insert(ts_ns);
				let this_ts = *ts;
				*ts += 1;
				this_ts
			};

			match record.data {
				RawRecord::KVLog(x) => {
					strings.push(format!(
						"kvlog,source=id-{},tid={},op={} ts={},duration_us={} {}",
						record.source_id,
						x.tid,
						x.op.as_val(),
						x.timestamp,
						x.duration_us,
						this_ts,
					));
				}

				RawRecord::SyscallEvent(x) => {
					strings.push(format!(
						"syscall_event,source=id-{} pid={},tid={},syscall_number={},start_time={},duration_us={} {}",
						record.source_id,
						x.pid,
						x.tid,
						x.syscall_number,
						x.start_time,
						x.duration_us,
						this_ts,
					));
				}

				RawRecord::PageCacheEvent(x) => {
					strings.push(format!(
						"page_cache,source=id-{} pid={},tid={},timestamp_us={},pfn={},i_ino={},index={},s_dev={} {}",
						record.source_id,
						x.pid,
						x.tid,
						x.timestamp_us,
						x.pfn,
						x.i_ino,
						x.index,
						x.s_dev,
						this_ts,
					));
				}

				RawRecord::PacketCapture(x) => {
					strings.push(format!(
						"packet,source=id-{} dst_port={},src_port={},dst_addr={},src_addr={},tot_len={},timestamp_us={} {}",
						record.source_id,
						x.dst_port,
						x.src_port,
						x.dst_addr,
						x.src_addr,
						x.tot_len,
						x.timestamp,
						this_ts,
					));
				}
			}
		}

		//build.build()
		strings.join("\n")
	}

	pub fn to_byte_vec(&self, data: &mut Vec<u8>) {
		u64_to_byte_vec(self.arrival_us, data);
		u64_to_byte_vec(self.data.len() as u64, data);
		for d in self.data.iter() {
			d.to_byte_vec(data);
		}
	}

	pub fn from_bytes(bytes: &[u8]) -> (Self, usize) {
		let arrival_us = u64_from_bytes(&bytes[..8]);
		let data_len = u64_from_bytes(&bytes[8..16]);
		let mut offset = 16;

		let mut data = Vec::new();
		for _ in 0..data_len {
			let (item, sz) = Record::from_bytes(&bytes[offset..]);
			offset += sz;
			data.push(item);
		}

		let this = RecordBatch { arrival_us, data };
		(this, offset)
	}
}

impl Record {
	pub fn source_information(&self) -> SourceInformation {
		self.data.source_information()
	}

	pub fn source_id(&self) -> u64 {
		self.source_id
	}

	pub fn from_packet_capture(item: PacketCapture) -> Self {
		let data = RawRecord::PacketCapture(item);
		let source_id = data.source_id();
		Self { data, source_id }
	}

	pub fn from_kvlog(item: KVLog) -> Self {
		let data = RawRecord::KVLog(item);
		let source_id = data.source_id();
		Self { data, source_id }
	}

	pub fn from_syscall_event(item: SyscallEvent) -> Self {
		let data = RawRecord::SyscallEvent(item);
		let source_id = data.source_id();
		Self { data, source_id }
	}

	pub fn from_page_cache_event(item: PageCacheEvent) -> Self {
		let data = RawRecord::PageCacheEvent(item);
		let source_id = data.source_id();
		Self { data, source_id }
	}

	pub fn as_kvlog(&self) -> Option<&KVLog> {
		match &self.data {
			RawRecord::KVLog(x) => Some(x),
			_ => None,
		}
	}

	pub fn as_syscall_event(&self) -> Option<&SyscallEvent> {
		match &self.data {
			RawRecord::SyscallEvent(x) => Some(x),
			_ => None,
		}
	}

	pub fn as_page_cache_event(&self) -> Option<&PageCacheEvent> {
		match &self.data {
			RawRecord::PageCacheEvent(x) => Some(x),
			_ => None,
		}
	}

	pub fn from_kv_log(item: KVLog) -> Self {
		let data = RawRecord::KVLog(item);
		let source_id = data.source_id();
		Self { data, source_id }
	}

	pub fn to_byte_vec(&self, data: &mut Vec<u8>) {
		u64_to_byte_vec(self.source_id, data);
		self.data.to_byte_vec(data);
	}

	pub fn from_bytes(data: &[u8]) -> (Self, usize) {
		let source_id = u64_from_bytes(&data[..8]);
		let (data, sz) = RawRecord::from_bytes(&data[8..]);
		let this = Self { source_id, data };
		(this, sz + 8)
	}
}

impl RawRecord {
	fn source_id(&self) -> u64 {
		self.source_information().as_id()
	}

	fn source_information(&self) -> SourceInformation {
		match self {
			Self::KVLog(x) => SourceInformation::KVLog { op: x.op },

			Self::SyscallEvent(x) => SourceInformation::SyscallEvent {
				syscall_number: x.syscall_number,
			},

			Self::PageCacheEvent(_) => SourceInformation::PageCacheEvent,
			Self::PacketCapture(_) => SourceInformation::PacketCapture,
		}
	}

	fn to_byte_vec(&self, data: &mut Vec<u8>) {
		match self {
			Self::KVLog(x) => {
				u64_to_byte_vec(0, data);
				x.to_byte_vec(data);
			}
			Self::SyscallEvent(x) => {
				u64_to_byte_vec(1, data);
				x.to_byte_vec(data);
			}
			Self::PageCacheEvent(x) => {
				u64_to_byte_vec(2, data);
				x.to_byte_vec(data);
			}
			Self::PacketCapture(x) => {
				u64_to_byte_vec(3, data);
				x.to_byte_vec(data);
			}
		}
	}

	fn from_bytes(data: &[u8]) -> (Self, usize) {
		let x = u64_from_bytes(&data[..8]);
		let (r, sz) = match x {
			0 => {
				let (x, sz) = KVLog::from_bytes(&data[8..]);
				(RawRecord::KVLog(x), sz)
			}
			1 => {
				let (x, sz) = SyscallEvent::from_bytes(&data[8..]);
				(RawRecord::SyscallEvent(x), sz)
			}
			2 => {
				let (x, sz) = PageCacheEvent::from_bytes(&data[8..]);
				(RawRecord::PageCacheEvent(x), sz)
			}
			3 => {
				let (x, sz) = PacketCapture::from_bytes(&data[8..]);
				(RawRecord::PacketCapture(x), sz)
			}
			_ => panic!("Unhandled value"),
		};
		(r, sz + 8)
	}
}
