//use dashmap::DashMap;
use fxhash::FxBuildHasher;
use minstant::Instant;
use seqlock::SeqLock;
use serde::{Deserialize, Serialize};
use std::{
	fs,
	hash::Hash,
	mem::MaybeUninit,
	ops::{Deref, DerefMut},
	path,
	rc::Rc,
	sync::{Arc, Mutex},
	time::Duration,
};

type HashMap<K, V> = std::collections::HashMap<K, V, FxBuildHasher>;
type HashSet<T> = std::collections::HashSet<T, FxBuildHasher>;
type DashMap<K, V> = dashmap::DashMap<K, V, FxBuildHasher>;

fn new_dashmap<K, V>() -> DashMap<K, V>
where
	K: Eq + Hash,
{
	DashMap::with_hasher(FxBuildHasher::default())
}

fn new_hashmap<K, V>() -> HashMap<K, V> {
	HashMap::with_hasher(FxBuildHasher::default())
}

//fn new_hashmap_with_capacity<K, V>(capacity: usize) -> HashMap<K, V> {
//	HashMap::with_capacity_and_hasher(capacity, FxBuildHasher::default())
//}

fn new_hashset<T>() -> HashSet<T> {
	HashSet::with_hasher(FxBuildHasher::default())
}

use crate::{
	block::{self, Entry, INITIAL_ADDRESS},
	constants::{CHUNK_SZ, PARTITIONS},
	error::MachError,
	//tsl_iouring as tsl,
	tsl,
};

#[derive(Clone)]
pub enum IndexSpecification {
	RangeIndexU64 {
		index_id: u64,
		func: fn(&[u8]) -> u64,
		min: u64,
		max: u64,
		interval: u64,
		chunk: u64,
		low_bin: u32,
		high_bin: u32,
		bins: Vec<u32>, // low, high, bitmap_id
	},
	None,
}

impl IndexSpecification {
	const NONE: Self = Self::None;

	// returns sorted by min_bound
	fn get_bin_min_bounds_u64(&self) -> Vec<(u32, u64)> {
		let mut result = Vec::new();
		match self {
			IndexSpecification::RangeIndexU64 {
				min,
				max,
				interval,
				low_bin,
				high_bin,
				bins,
				..
			} => {
				result.push((*low_bin, 0));

				let mut curr_min = *min;
				for bin in bins.iter() {
					result.push((*bin, curr_min));
					curr_min += *interval;
				}

				result.push((*high_bin, *max));
				result
			}
			_ => panic!("Internal error"),
		}
	}

	fn get_value_u64(&self, bytes: &[u8]) -> u64 {
		match self {
			IndexSpecification::RangeIndexU64 { func, .. } => func(bytes),
			_ => panic!("Internal error"),
		}
	}

	#[inline(always)]
	fn get_bitmap_id_u64(&self, value: u64) -> u32 {
		match self {
			IndexSpecification::RangeIndexU64 {
				min,
				max,
				interval,
				low_bin,
				high_bin,
				bins,
				..
			} => {
				if value < *min {
					*low_bin
				} else if value >= *max {
					*high_bin
				} else {
					let bin = ((value - *min) / interval) as usize;
					bins[bin]
				}
			}
			_ => panic!("Internal error"),
		}
	}

	fn get_bitmap_ids_for_range_u64(
		&self,
		min_field: u64,
		max_field: u64,
	) -> HashSet<u32> {
		let mut v = new_hashset();
		match self {
			IndexSpecification::RangeIndexU64 {
				min,
				max,
				interval,
				low_bin,
				high_bin,
				bins,
				..
			} => {
				println!("Index spec: min {} interval {}", min, interval);
				println!(
					"low bin: {}, bins: {:?}, high bin: {}",
					low_bin, bins, high_bin
				);

				if min_field >= *max {
					v.insert(*high_bin);
				} else if max_field < *min {
					v.insert(*low_bin);
				} else {
					let mut start_bin =
						((min_field - *min) / interval) as usize;
					let mut end_bin = ((max_field - *min) / interval) as usize;

					if min_field < *min {
						start_bin = 0;
						v.insert(*low_bin);
					}

					if max_field >= *max {
						end_bin = bins.len() - 1;
						v.insert(*high_bin);
					}

					for bin in &bins[start_bin..=end_bin] {
						v.insert(*bin);
					}
				}
			}
			_ => panic!("Internal error"),
		}
		v
	}

	fn new_range_index_u64(
		index_id: u64,
		func: fn(&[u8]) -> u64,
		min: u64,
		max: u64,
		low_bin: u32,
		high_bin: u32,
		bins: &[u32],
	) -> Self {
		let interval = (max - min) / bins.len() as u64;
		Self::RangeIndexU64 {
			index_id,
			func,
			min,
			max,
			interval,
			chunk: u64::MAX,
			low_bin,
			high_bin,
			bins: bins.to_vec(),
		}
	}
}

struct LocalMapValue {
	address: u64,
	counter: usize,
	timer: Instant,
	ts_address: u64,
	timestamp: u64,

	indexes: [IndexSpecification; 128],
	n_indexes: usize,
}

impl LocalMapValue {
	// TODO: This does not support removing (yet)
	fn add_index(&mut self, spec: IndexSpecification) {
		let idx = self.n_indexes;
		self.indexes[idx] = spec;
		self.n_indexes += 1;
	}

	fn indexes(&self) -> &[IndexSpecification] {
		&self.indexes[..self.n_indexes]
	}
}

#[derive(Copy, Clone, Debug)]
struct InnerSharedAddresses {
	address: u64,
	ts_address: u64,
	timestamp: u64,
}

#[derive(Clone)]
struct InnerSharedIndexData {
	indexes: [IndexSpecification; 128],
	n_indexes: usize,
}

struct SharedMapValue {
	inner_shared_addresses: SeqLock<InnerSharedAddresses>,
	inner_shared_indexed_data: Mutex<InnerSharedIndexData>,
}

impl SharedMapValue {
	fn update_addresses(&self, local: &LocalMapValue) {
		let mut guard = self.inner_shared_addresses.lock_write();
		guard.address = local.address;
		guard.ts_address = local.ts_address;
		guard.timestamp = local.timestamp;
	}

	fn add_index(&self, spec: IndexSpecification) {
		let mut guard = self.inner_shared_indexed_data.lock().unwrap();
		let idx = guard.n_indexes;
		guard.indexes[idx] = spec;
		guard.n_indexes += 1;
	}

	//fn read(&self

	fn new() -> Self {
		let inner_shared_addresses = SeqLock::new(InnerSharedAddresses {
			address: u64::MAX,
			ts_address: u64::MAX,
			timestamp: u64::MAX,
		});

		let inner_shared_indexed_data = Mutex::new(InnerSharedIndexData {
			indexes: [IndexSpecification::NONE; 128],
			n_indexes: 0,
		});

		SharedMapValue {
			inner_shared_addresses,
			inner_shared_indexed_data,
		}
	}
}

struct LocalMap {
	inner: HashMap<(u64, u64), LocalMapValue>,
}

impl Deref for LocalMap {
	type Target = HashMap<(u64, u64), LocalMapValue>;
	fn deref(&self) -> &Self::Target {
		&self.inner
	}
}

impl DerefMut for LocalMap {
	fn deref_mut(&mut self) -> &mut Self::Target {
		&mut self.inner
	}
}

impl LocalMap {
	fn new() -> Self {
		Self {
			inner: new_hashmap(),
		}
	}
}

#[derive(Clone)]
struct SharedMap {
	inner: Arc<DashMap<(u64, u64), SharedMapValue>>,
}

impl Deref for SharedMap {
	type Target = DashMap<(u64, u64), SharedMapValue>;
	fn deref(&self) -> &Self::Target {
		&*self.inner
	}
}

impl SharedMap {
	fn new() -> Self {
		Self {
			inner: Arc::new(new_dashmap()),
		}
	}

	fn add_source(&self, source: u64, partition: u64) {
		self.inner
			.insert((source, partition), SharedMapValue::new());
	}

	fn update_or_insert(
		&self,
		source: u64,
		partition: u64,
		entry: &LocalMapValue,
	) {
		let shared_item = self.inner.get(&(source, partition)).unwrap();
		shared_item.update_addresses(entry);
	}

	fn add_index(&self, source: u64, partition: u64, spec: IndexSpecification) {
		self.inner
			.get(&(source, partition))
			.unwrap()
			.add_index(spec);
	}
}

#[derive(Copy, Clone, Default)]
enum IndexEntry {
	U64 {
		source_id: u64,
		key: u32,
		min: u64,
		max: u64,
		count: u64,
		sum: u64,
		min_ts: u64,
		max_ts: u64,
		max_addr: u64,
	},

	#[default]
	None,
}

impl IndexEntry {
	//fn source_id(&self) -> u64 {
	//	match self {
	//		IndexEntry::U64 { source_id, .. } => *source_id,
	//		IndexEntry::None => panic!("Internal error"),
	//	}
	//}

	fn max_addr(&self) -> u64 {
		match self {
			IndexEntry::U64 { max_addr, .. } => *max_addr,
			//IndexEntry::F64 { max_addr, .. } => *max_addr,
			IndexEntry::None => panic!("Internal error"),
		}
	}

	fn get_sum_u64(&self) -> u64 {
		match self {
			IndexEntry::U64 { sum, .. } => *sum,
			//IndexEntry::F64 { .. } => panic!("Internal error"),
			IndexEntry::None => panic!("Internal error"),
		}
	}

	fn get_count(&self) -> u64 {
		match self {
			IndexEntry::U64 { count, .. } => *count,
			//IndexEntry::F64 { count, .. } => *count,
			IndexEntry::None => panic!("Internal error"),
		}
	}

	fn get_time_range(&self) -> (u64, u64) {
		match self {
			IndexEntry::U64 { min_ts, max_ts, .. } => (*min_ts, *max_ts),
			//IndexEntry::F64 { min_ts, max_ts, .. } => (*min_ts, *max_ts),
			IndexEntry::None => panic!("Internal error"),
		}
	}

	fn get_value_range_u64(&self) -> (u64, u64) {
		match self {
			IndexEntry::U64 { min, max, .. } => (*min, *max),
			//IndexEntry::F64 { .. } => panic!("Internal error"),
			IndexEntry::None => panic!("Internal error"),
		}
	}
}

struct RoIndex {
	pub map: HashMap<u32, IndexEntry>, // min, max, count, sum
	pub chunk_id: u64,
	//pub partition: u64,
}

struct Index {
	timeranges: Vec<(u64, usize)>,
	timeranges_vec: Vec<IndexEntry>, // key, min, max, count, sum
	timeranges_vec_len: usize,
	last_addr: u64,

	chunk_id: u64,
	partition: u64,
	reader: IndexReader,

	time_tsl_head: u64,
	last_indexed_timestamp: u64,
}

impl Index {
	fn clear(&mut self) {
		self.timeranges_vec_len = 0; //.clear();
	}

	fn new(partition: u64) -> Self {
		Self {
			timeranges: vec![(0, usize::MAX); 1024 * 1024],
			timeranges_vec: vec![IndexEntry::default(); 1024 * 1024],
			timeranges_vec_len: 0,

			last_addr: u64::MAX,

			chunk_id: 0,
			partition,
			reader: IndexReader::new(),

			time_tsl_head: u64::MAX,
			last_indexed_timestamp: u64::MAX,
		}
	}

	fn update_reader(
		&self,
		index_tsl_addr: u64,
		chunk_id: u64,
		time_tsl_addr: u64,
	) {
		self.reader.update(index_tsl_addr, chunk_id, time_tsl_addr);
	}

	fn reader(&self) -> IndexReader {
		self.reader.clone()
	}

	fn deserialize_ro_index_with_bitmap_values_into(
		bytes: &[u8],
		bitmap_values: &HashSet<u32>,
		index: &mut RoIndex,
	) {
		let mut s = 0;
		let mut e = 8;
		let chunk_id = u64::from_be_bytes(bytes[s..e].try_into().unwrap());

		s = e;
		e = s + 8;
		let _partition = u64::from_be_bytes(bytes[s..e].try_into().unwrap());

		s = e;
		e = s + 8;
		let timeranges_len =
			u64::from_be_bytes(bytes[s..e].try_into().unwrap());

		index.map.clear();
		index.map.reserve(timeranges_len as usize);
		let map = &mut index.map;
		for _ in 0..timeranges_len {
			s = e;
			e = s + 8;
			let kind = u64::from_be_bytes(bytes[s..e].try_into().unwrap());
			match kind {
				0 => {
					s = e;
					e = s + 8;
					let source_id =
						u64::from_be_bytes(bytes[s..e].try_into().unwrap());

					s = e;
					e = s + 4;
					let key =
						u32::from_be_bytes(bytes[s..e].try_into().unwrap());
					if !bitmap_values.contains(&key) {
						e += 56;
						continue;
					}

					s = e;
					e = s + 8;
					let min =
						u64::from_be_bytes(bytes[s..e].try_into().unwrap());

					s = e;
					e = s + 8;
					let max =
						u64::from_be_bytes(bytes[s..e].try_into().unwrap());

					s = e;
					e = s + 8;
					let count =
						u64::from_be_bytes(bytes[s..e].try_into().unwrap());

					s = e;
					e = s + 8;
					let sum =
						u64::from_be_bytes(bytes[s..e].try_into().unwrap());

					s = e;
					e = s + 8;
					let min_ts =
						u64::from_be_bytes(bytes[s..e].try_into().unwrap());

					s = e;
					e = s + 8;
					let max_ts =
						u64::from_be_bytes(bytes[s..e].try_into().unwrap());

					s = e;
					e = s + 8;
					let max_addr =
						u64::from_be_bytes(bytes[s..e].try_into().unwrap());

					map.insert(
						key,
						IndexEntry::U64 {
							source_id,
							key,
							min,
							max,
							count,
							sum,
							min_ts,
							max_ts,
							max_addr,
						},
					);
				}
				_ => panic!("Internal error"),
			}
		}

		index.chunk_id = chunk_id;
	}

	//fn deserialize_ro_index_with_bitmap_values(
	//	bytes: &[u8],
	//	bitmap_values: &HashSet<u32>,
	//) -> RoIndex {
	//	let mut idx = RoIndex { map: new_hashmap(), chunk_id: 0 };
	//	Self::deserialize_ro_index_with_bitmap_values_into(bytes, bitmap_values, &mut idx);
	//	idx
	//}

	fn deserialize_ro_index(bytes: &[u8]) -> RoIndex {
		let mut s = 0;
		let mut e = 8;
		let chunk_id = u64::from_be_bytes(bytes[s..e].try_into().unwrap());

		s = e;
		e = s + 8;
		let _partition = u64::from_be_bytes(bytes[s..e].try_into().unwrap());

		s = e;
		e = s + 8;
		let timeranges_len =
			u64::from_be_bytes(bytes[s..e].try_into().unwrap());

		let mut map = new_hashmap();
		for _ in 0..timeranges_len {
			s = e;
			e = s + 8;
			let kind = u64::from_be_bytes(bytes[s..e].try_into().unwrap());
			match kind {
				0 => {
					s = e;
					e = s + 8;
					let source_id =
						u64::from_be_bytes(bytes[s..e].try_into().unwrap());

					s = e;
					e = s + 4;
					let key =
						u32::from_be_bytes(bytes[s..e].try_into().unwrap());

					s = e;
					e = s + 8;
					let min =
						u64::from_be_bytes(bytes[s..e].try_into().unwrap());

					s = e;
					e = s + 8;
					let max =
						u64::from_be_bytes(bytes[s..e].try_into().unwrap());

					s = e;
					e = s + 8;
					let count =
						u64::from_be_bytes(bytes[s..e].try_into().unwrap());

					s = e;
					e = s + 8;
					let sum =
						u64::from_be_bytes(bytes[s..e].try_into().unwrap());

					s = e;
					e = s + 8;
					let min_ts =
						u64::from_be_bytes(bytes[s..e].try_into().unwrap());

					s = e;
					e = s + 8;
					let max_ts =
						u64::from_be_bytes(bytes[s..e].try_into().unwrap());

					s = e;
					e = s + 8;
					let max_addr =
						u64::from_be_bytes(bytes[s..e].try_into().unwrap());

					map.insert(
						key,
						IndexEntry::U64 {
							source_id,
							key,
							min,
							max,
							count,
							sum,
							min_ts,
							max_ts,
							max_addr,
						},
					);
				}
				_ => panic!("Internal index deserializeation error"),
			}
		}

		RoIndex { map, chunk_id }
	}

	fn serialize_timeranges_into(&self, data: &mut Vec<u8>) {
		data.extend_from_slice(&self.chunk_id.to_be_bytes());
		data.extend_from_slice(&self.partition.to_be_bytes());
		data.extend_from_slice(&self.timeranges_vec_len.to_be_bytes());
		for v in self.timeranges_vec[..self.timeranges_vec_len].iter() {
			match v {
				IndexEntry::U64 {
					source_id,
					key,
					min,
					max,
					count,
					sum,
					min_ts,
					max_ts,
					max_addr,
				} => {
					data.extend_from_slice(&0u64.to_be_bytes());
					data.extend_from_slice(&source_id.to_be_bytes());
					data.extend_from_slice(&key.to_be_bytes()); // 4 byte key
					data.extend_from_slice(&min.to_be_bytes());
					data.extend_from_slice(&max.to_be_bytes());
					data.extend_from_slice(&count.to_be_bytes());
					data.extend_from_slice(&sum.to_be_bytes());
					data.extend_from_slice(&min_ts.to_be_bytes());
					data.extend_from_slice(&max_ts.to_be_bytes());
					data.extend_from_slice(&max_addr.to_be_bytes());
				}
				IndexEntry::None => {
					panic!("Internal index serialization error")
				}
			}
		}
	}

	fn index_item(
		&mut self,
		source: u64,
		ts: u64,
		addr: u64,
		bytes: &[u8],
		index_spec: &IndexSpecification,
	) {
		self.last_indexed_timestamp = ts;
		match index_spec {
			IndexSpecification::None => {
				panic!("Internal error, got an index none")
			}
			IndexSpecification::RangeIndexU64 { func, .. } => {
				let value: u64 = func(bytes);

				let bitmap_id = index_spec.get_bitmap_id_u64(value);

				let idx = {
					let entry = &mut self.timeranges[bitmap_id as usize];
					if entry.0 != self.chunk_id || entry.1 == usize::MAX {
						let offset_id = self.timeranges_vec_len;
						self.timeranges_vec_len += 1;
						let e = IndexEntry::U64 {
							source_id: source,
							key: bitmap_id,
							min: u64::MAX,
							max: 0,
							count: 0,
							sum: 0,
							min_ts: u64::MAX,
							max_ts: 0,
							max_addr: u64::MAX,
						};
						self.timeranges_vec[offset_id] = e;
						*entry = (self.chunk_id, offset_id)
					}
					entry.1
				};

				let map_entry = &mut self.timeranges_vec[idx];
				match map_entry {
					IndexEntry::U64 {
						min,
						max,
						count,
						sum,
						min_ts,
						max_ts,
						max_addr,
						..
					} => {
						*min = (*min).min(value);
						*max = (*max).max(value);
						*count += 1;
						*sum += value;
						*min_ts = (*min_ts).min(ts);
						*max_ts = (*max_ts).max(ts);
						*max_addr = addr;
					}
					//IndexEntry::F64 { .. } => panic!("Internal error"),
					IndexEntry::None { .. } => panic!("Internal error"),
				}
			}
		}
	}
}

#[derive(Serialize, Deserialize, Copy, Clone)]
struct IndexSnapshot {
	addr: u64,
	chunk_id: u64,
	time_tsl_head: u64,
}

#[derive(Clone)]
struct IndexReader {
	inner: Arc<SeqLock<IndexSnapshot>>, // time addr,  chunk id
}

impl IndexReader {
	fn new() -> Self {
		Self {
			inner: Arc::new(SeqLock::new(IndexSnapshot {
				addr: u64::MAX,
				chunk_id: u64::MAX,
				time_tsl_head: u64::MAX,
			})),
		}
	}

	fn snapshot(&self) -> IndexSnapshot {
		self.inner.read()
	}

	fn update(&self, addr: u64, chunk_id: u64, time_tsl_head: u64) {
		let mut guard = self.inner.lock_write();
		guard.addr = addr;
		guard.chunk_id = chunk_id;
		guard.time_tsl_head = time_tsl_head;
	}
}

pub struct Partitions {
	//dir: PathBuf,
	partitions: [tsl::TSL; PARTITIONS],
	local_map: LocalMap,
	shared_map: SharedMap,

	index_tsl: tsl::TSL,
	time_tsl: tsl::TSL,

	//space_tsl: tsl::TSL,
	indexes: Vec<Index>,        // one index per partition
	index_byte_buffer: Vec<u8>, // buffer for index tsl

	index_id: u64,

	// Measuring arrival time. Shared timer and timer are the same instant.
	// Timer is used locally, shared is cloned by the reader.
	timer: Instant,
}

impl Partitions {
	pub fn micros_now(&self) -> u64 {
		self.timer.elapsed().as_micros() as u64
	}

	pub fn define_source(&mut self, s: u64, p: u64) {
		let local_map_value = LocalMapValue {
			address: INITIAL_ADDRESS,
			counter: 0,
			timer: Instant::now(),
			ts_address: 0,
			timestamp: u64::MAX,
			indexes: [IndexSpecification::NONE; 128],
			n_indexes: 0,
		};

		self.local_map.insert((s, p), local_map_value);
		self.shared_map.add_source(s, p);
	}

	pub fn define_range_index_u64(
		&mut self,
		source: u64,
		partition: u64,
		func: fn(&[u8]) -> u64,
		min: u64,
		interval: u64,
		bins: u64,
	) -> u64 {
		let max = min + bins * interval;
		self.index_id += 1;
		let index_id = self.index_id;

		let mut bin_ids = Vec::new();
		{
			//let idx = &mut self.indexes[partition as usize];
			for _ in 0..bins {
				self.index_id += 1;
				bin_ids.push(self.index_id as u32);
			}
		}

		self.index_id += 1;
		let low_bin = self.index_id as u32;

		self.index_id += 1;
		let high_bin = self.index_id as u32;

		println!(
			"Bin_ids for this range index: {low_bin}, {:?}, {high_bin}",
			bin_ids
		);

		let spec = IndexSpecification::new_range_index_u64(
			index_id, func, min, max, low_bin, high_bin, &bin_ids,
		);

		self.local_map
			.get_mut(&(source, partition))
			.unwrap()
			.add_index(spec.clone());

		self.shared_map.add_index(source, partition, spec);

		index_id
	}

	pub fn new(dir: path::PathBuf) -> Self {
		let replace_dir = true;
		if replace_dir {
			let _ = fs::remove_dir_all(&dir);
			fs::create_dir_all(&dir).unwrap();
		}

		let partitions = unsafe {
			type Arr = [tsl::TSL; PARTITIONS];
			let mut arr: MaybeUninit<Arr> = MaybeUninit::uninit();
			let mut ptr = arr.as_mut_ptr() as *mut tsl::TSL;
			for i in 0..PARTITIONS {
				let mut dir = dir.clone();
				dir.push(format!("partition_{}", i));
				ptr.write(tsl::TSL::new(dir, replace_dir));
				ptr = ptr.add(1);
			}
			arr.assume_init()
		};

		let local_map = LocalMap::new();
		let shared_map = SharedMap::new();

		let index_tsl = {
			let mut dir = dir.clone();
			dir.push("index");
			tsl::TSL::new(dir, replace_dir)
		};

		let time_tsl = {
			let mut dir = dir.clone();
			dir.push("time");
			tsl::TSL::new(dir, replace_dir)
		};

		let indexes = (0..PARTITIONS).map(|p| Index::new(p as u64)).collect();
		let timer = Instant::now();

		Self {
			//dir,
			partitions,
			local_map,
			shared_map,

			index_tsl,
			time_tsl,

			//space_tsl,
			indexes,
			index_byte_buffer: Vec::new(),
			index_id: 0,
			timer,
		}
	}

	pub fn push(
		&mut self,
		source: u64,
		partition: u64,
		data: &[u8],
	) -> (u64, u64) {
		let sp = (source, partition);
		let map_entry = self.local_map.get_mut(&sp).unwrap();
		let timestamp = self.timer.elapsed().as_micros() as u64;
		//let timestamp = 0;

		let last_address = map_entry.address;

		let p = partition as usize;

		let result =
			self.partitions[p].append(source, timestamp, data, last_address);

		// If there's a new chunk for this partition, the item we just wrote is
		// in a new chunk! So, we write the old chunk index into the metadata,
		// then index the new item in the new chunk's index
		if result.addr > 0 && (result.new_chunk || result.new_block) {
			let index = &mut self.indexes[p];
			let index_ts = index.last_indexed_timestamp;

			// Serialize the time information
			let index_tsl_addr = {
				self.index_byte_buffer.clear();
				index.serialize_timeranges_into(&mut self.index_byte_buffer);
				let buf = self.index_byte_buffer.as_slice();
				let result = self.index_tsl.append(
					partition,
					index_ts,
					buf,
					index.last_addr,
				);
				result.addr
			};

			let time_tsl_addr = {
				let result = self.time_tsl.append(
					u64::MAX - 1,
					index_ts,
					&index_tsl_addr.to_be_bytes(),
					index.time_tsl_head,
				);
				if result.new_block {
					println!("New timeline block");
				}
				result.addr
			};

			// Sequence here: make the metadata sync first before exposing the
			// actual address to readers
			self.partitions[p].sync();
			self.index_tsl.sync();
			self.time_tsl.sync();
			index.update_reader(index_tsl_addr, index.chunk_id, time_tsl_addr);

			// And now we can prep the index for the next chunk
			index.last_addr = index_tsl_addr;
			index.time_tsl_head = time_tsl_addr;
			index.chunk_id += 1;
			index.clear();
		}

		// Index the data item for this partition
		let partition_idx = &mut self.indexes[p];

		for idx in map_entry.indexes() {
			partition_idx.index_item(source, timestamp, result.addr, data, idx);
		}

		if map_entry.counter > 1024 * 128 {
			if map_entry.timer.elapsed() > Duration::from_secs(1) {
				let result = self.time_tsl.append(
					source,
					timestamp,
					&map_entry.address.to_be_bytes(),
					map_entry.ts_address,
				);
				if result.new_block {
					println!("New timeline block");
				}
				// Sync the relevant TSLs
				self.partitions[partition as usize].sync();
				self.time_tsl.sync();

				// Sync the per source information
				// Sequence here: store the last valid address first then store the
				// ts index address.
				self.shared_map
					.update_or_insert(source, partition, map_entry);

				// And reset the counters
				map_entry.ts_address = result.addr;
				map_entry.timer = Instant::now();
			}
			map_entry.counter = 0;
		}

		map_entry.address = result.addr;
		map_entry.counter += 1;
		(result.addr, timestamp)
	}

	pub fn sync(&self, source: u64, partition: u64) {
		let sp = (source, partition);

		// Sync the relevant TSLs
		self.partitions[partition as usize].sync();

		// Sync the per source information
		let map_entry = self.local_map.get(&sp).unwrap();
		self.shared_map
			.update_or_insert(source, partition, map_entry);

		// Index and time range TSL are synced during push operations
	}

	pub fn reader(&self) -> PartitionsReader {
		let partitions = unsafe {
			type Arr = [tsl::TSLReader; PARTITIONS];
			let mut arr: MaybeUninit<Arr> = MaybeUninit::uninit();
			let mut ptr = arr.as_mut_ptr() as *mut tsl::TSLReader;
			for p in self.partitions.iter() {
				ptr.write(p.reader());
				ptr = ptr.add(1);
			}
			arr.assume_init()
		};

		let shared_map = self.shared_map.clone();

		//let space_tsl = self.space_tsl.reader();
		let index_tsl = self.index_tsl.reader();
		let time_tsl = self.time_tsl.reader();

		let indexes = self.indexes.iter().map(|x| x.reader()).collect();

		PartitionsReader {
			partitions,
			shared_map,
			indexes,
			index_tsl,
			time_tsl,

			timer: self.timer,
		}
	}
}

#[derive(Clone)]
pub struct PartitionsReader {
	partitions: [tsl::TSLReader; PARTITIONS],
	shared_map: SharedMap,

	index_tsl: tsl::TSLReader,
	time_tsl: tsl::TSLReader,

	#[allow(dead_code)]
	indexes: Vec<IndexReader>,

	timer: Instant,
}

impl PartitionsReader {
	pub fn now_micros(&self) -> u64 {
		self.timer.elapsed().as_micros() as u64
	}

	pub fn indexed_query_ctx(
		&self,
		sps: &[(u64, u64)],
	) -> Option<QueryContext> {
		Some(QueryContext::new(self.indexed_reader(sps)?))
	}

	fn indexed_reader(
		&self,
		sps: &[(u64, u64)],
	) -> Option<PartitionsIndexedReader> {
		let mut local_map = new_hashmap();
		let mut partition_ids = new_hashset();

		let mut partitions: Vec<Option<tsl::TslAddressReader>> = Vec::new();
		let mut metadata: Vec<Option<tsl::TslAddressReader>> = Vec::new();
		let mut indexes: Vec<Option<IndexSnapshot>> = Vec::new();
		for _ in 0..PARTITIONS {
			partitions.push(None);
			indexes.push(None);
			metadata.push(None);
		}

		// Collect unique partitions and sources
		for sp in sps {
			partition_ids.insert(sp.1);
			if let Some(x) = self.shared_map.get(sp) {
				let address = x.inner_shared_addresses.read();
				let indexes: Vec<Rc<IndexSpecification>> = {
					let guard = x.inner_shared_indexed_data.lock().unwrap();
					guard.indexes[..guard.n_indexes]
						.iter()
						.map(|x| Rc::new(x.clone()))
						.collect()
				};

				let x = IndexedReaderSourceData {
					address: address.address,
					ts_address: address.ts_address,
					timestamp: address.timestamp,
					indexes,
				};

				local_map.insert(*sp, Rc::new(x));
			}
		}

		// Get partitions and build partitions map
		for p in partition_ids {
			let p = p as usize;
			let tsl_reader = self.partitions[p].tsl_address_reader();
			partitions[p] = Some(tsl_reader);
			indexes[p] = Some(self.indexes[p].snapshot());
		}

		let index_tsl = self.index_tsl.tsl_address_reader();
		let time_tsl = self.time_tsl.tsl_address_reader();
		let sources = local_map;
		let timer = self.timer;

		Some(PartitionsIndexedReader {
			index_tsl,
			time_tsl,
			timer,
			partitions,
			sources,
			indexes,
			tmp_entry: Entry::new_empty(),
		})
	}
}

struct IndexedReaderSourceData {
	address: u64,
	ts_address: u64,
	timestamp: u64,
	indexes: Vec<Rc<IndexSpecification>>,
}

#[derive(Clone)]
struct PartitionsIndexedReader {
	index_tsl: tsl::TslAddressReader,
	time_tsl: tsl::TslAddressReader,
	timer: Instant,

	partitions: Vec<Option<tsl::TslAddressReader>>,
	sources: HashMap<(u64, u64), Rc<IndexedReaderSourceData>>,
	indexes: Vec<Option<IndexSnapshot>>,
	tmp_entry: block::Entry,
}

impl PartitionsIndexedReader {
	pub fn now_micros(&self) -> u64 {
		self.timer.elapsed().as_micros() as u64
	}

	fn reset_read_counters(&mut self) {
		self.time_tsl.reset_read_counters();
		self.index_tsl.reset_read_counters();

		for i in self.partitions.iter_mut() {
			if let Some(p) = i.as_mut() {
				p.reset_read_counters();
			}
		}
	}

	fn persistent_chunks_read_time_tsl(&self) -> u64 {
		self.time_tsl.persistent_chunks_read
	}

	fn persistent_chunks_read_index_tsl(&self) -> u64 {
		self.index_tsl.persistent_chunks_read
	}

	fn persistent_chunks_read_data_tsls(&self) -> u64 {
		let mut total = 0;
		for i in self.partitions.iter() {
			if let Some(p) = i.as_ref() {
				total += p.persistent_chunks_read;
			}
		}
		total
	}

	fn persistent_addrs_read_time_tsl(&self) -> u64 {
		self.time_tsl.persistent_addrs_read
	}

	fn persistent_addrs_read_index_tsl(&self) -> u64 {
		self.index_tsl.persistent_addrs_read
	}

	fn persistent_addrs_read_data_tsls(&self) -> u64 {
		let mut total = 0;
		for i in self.partitions.iter() {
			if let Some(p) = i.as_ref() {
				total += p.persistent_addrs_read;
			}
		}
		total
	}

	// fn in_memory_metadata_addresses_read(&self) -> u64 {
	// 	let mut total = 0;
	// 	total += self.time_tsl.in_memory_chunks_read;
	// 	total += self.index_tsl.in_memory_addresses_read;
	// 	total
	// }

	// fn in_memory_metadata_chunks_read(&self) -> u64 {
	// 	let mut total = 0;
	// 	total += self.time_tsl.in_memory_chunks_read;
	// 	total += self.index_tsl.in_memory_chunks_read;
	// 	total
	// }

	// fn persistent_metadata_chunks_read(&self) -> u64 {
	// 	let mut total = 0;
	// 	total += self.time_tsl.persistent_chunks_read;
	// 	total += self.index_tsl.persistent_chunks_read;
	// 	total
	// }

	// fn in_memory_data_addresses_read(&self) -> u64 {
	// 	let mut total = 0;
	// 	for i in self.partitions.iter() {
	// 		if let Some(p) = i.as_ref() {
	// 			total += p.in_memory_addresses_read;
	// 		}
	// 	}
	// 	total
	// }

	// fn in_memory_data_chunks_read(&self) -> u64 {
	// 	let mut total = 0;
	// 	for i in self.partitions.iter() {
	// 		if let Some(p) = i.as_ref() {
	// 			total += p.in_memory_chunks_read;
	// 		}
	// 	}
	// 	total
	// }

	// fn persistent_data_chunks_read(&self) -> u64 {
	// 	let mut total = 0;
	// 	for i in self.partitions.iter() {
	// 		if let Some(p) = i.as_ref() {
	// 			total += p.persistent_chunks_read;
	// 		}
	// 	}
	// 	total
	// }

	fn get_max_timestamp(&self, s: u64, p: u64) -> Option<u64> {
		let value = self.sources.get(&(s, p))?;
		Some(value.timestamp)
	}

	fn get_data_head_addr(&self, s: u64, p: u64) -> Option<u64> {
		let value = self.sources.get(&(s, p))?;
		Some(value.address)
	}

	fn get_time_head_addr(&self, s: u64, p: u64) -> Option<u64> {
		let value = self.sources.get(&(s, p))?;
		Some(value.ts_address)
	}

	fn get_entry(&mut self, addr: u64, p: u64) -> Result<&Entry, MachError> {
		match self.partitions[p as usize].as_mut() {
			None => return Err(MachError::PartitionNotInReader),
			Some(x) => x.read_addr(addr, &mut self.tmp_entry)?,
		}
		Ok(&self.tmp_entry)
	}

	fn get_index(
		&self,
		s: u64,
		p: u64,
		id: u64,
	) -> Option<Rc<IndexSpecification>> {
		let source_data = self.sources.get(&(s, p))?;
		for index in source_data.indexes.iter() {
			match &(**index) {
				IndexSpecification::RangeIndexU64 { index_id, .. } => {
					if *index_id == id {
						return Some(index.clone());
					}
				}
				IndexSpecification::None => panic!("None index"),
			}
		}
		None
	}

	// For a given source, get the bitmap_ids that captures field in the range
	// min_field to max_field (inclusive)
	fn get_bitmap_ids_for_range_u64(
		&self,
		source: u64,
		partition: u64,
		id: u64,
		min_field: u64,
		max_field: u64,
	) -> HashSet<u32> {
		//let mut v = new_hashset();

		let source_data = match self.sources.get(&(source, partition)) {
			Some(x) => x,
			None => {
				return new_hashset();
			}
		};

		for index in source_data.indexes.iter() {
			match &(**index) {
				IndexSpecification::None => panic!("Intenral error - found a None index where there shouldnt be one"),
				IndexSpecification::RangeIndexU64 {
					index_id,
					..
				} => {
					if *index_id == id {
						return index.get_bitmap_ids_for_range_u64(min_field, max_field);
					}
				}
			}
		}

		new_hashset()
	}

	fn get_entry_on_or_before_timestamp(
		&mut self,
		s: u64,
		p: u64,
		ts: u64,
	) -> Option<&block::Entry> {
		let mut time_tsl_addr = self.get_time_head_addr(s, p)?;
		let mut data_addr = u64::MAX;
		loop {
			self.time_tsl
				.read_addr(time_tsl_addr, &mut self.tmp_entry)
				.unwrap();
			if self.tmp_entry.header.timestamp < ts {
				println!(
					"Found entry ts < ts: {} < {}",
					self.tmp_entry.header.timestamp, ts
				);
				break;
			}
			data_addr =
				u64::from_be_bytes(self.tmp_entry.data[..].try_into().unwrap());
			time_tsl_addr = self.tmp_entry.last_addr;
		}

		if data_addr == u64::MAX {
			println!("HERE");
			None
		} else {
			self.partitions[p as usize]
				.as_mut()
				.unwrap()
				.read_addr(data_addr, &mut self.tmp_entry)
				.unwrap();
			Some(&self.tmp_entry)
		}
	}
}

#[derive(Clone)]
pub struct QueryContext {
	reader: PartitionsIndexedReader,
	//entry_cache: HashMap<(u64, u64), RoChunkEntry>,
}

impl QueryContext {
	fn new(reader: PartitionsIndexedReader) -> Self {
		Self {
			reader,
			//entry_cache: new_hashmap(),
		}
	}

	pub fn persistent_chunks_read(&self) -> (u64, u64, u64) {
		let a = self.reader.persistent_chunks_read_time_tsl();
		let b = self.reader.persistent_chunks_read_index_tsl();
		let c = self.reader.persistent_chunks_read_data_tsls();
		(a, b, c)
	}

	pub fn persistent_addrs_read(&self) -> (u64, u64, u64) {
		let a = self.reader.persistent_addrs_read_time_tsl();
		let b = self.reader.persistent_addrs_read_index_tsl();
		let c = self.reader.persistent_addrs_read_data_tsls();
		(a, b, c)
	}

	// pub fn print_read_counters(&self) {
	// 	println!(">> Read counters");
	// 	println!(
	// 		">> In memory metadata addresses read: {}",
	// 		self.in_memory_metadata_addresses_read()
	// 	);
	// 	println!(
	// 		">> In memory metadata chunks read: {}",
	// 		self.in_memory_metadata_chunks_read()
	// 	);
	// 	println!(
	// 		">> Persistent metadata chunks read: {}",
	// 		self.persistent_metadata_chunks_read()
	// 	);
	// 	println!(
	// 		">> In memory data addresses read: {}",
	// 		self.in_memory_data_addresses_read()
	// 	);
	// 	println!(
	// 		">> In memory data chunks read: {}",
	// 		self.in_memory_data_chunks_read()
	// 	);
	// 	println!(
	// 		">> Persistent data chunks read: {}",
	// 		self.persistent_data_chunks_read()
	// 	);
	// }

	pub fn reset_read_counters(&mut self) {
		self.reader.reset_read_counters();
	}

	//pub fn in_memory_metadata_addresses_read(&self) -> u64 {
	//	self.reader.in_memory_metadata_addresses_read()
	//}

	//pub fn in_memory_metadata_chunks_read(&self) -> u64 {
	//	self.reader.in_memory_metadata_chunks_read()
	//}

	//pub fn persistent_metadata_chunks_read(&self) -> u64 {
	//	self.reader.persistent_metadata_chunks_read()
	//}

	//pub fn in_memory_data_addresses_read(&self) -> u64 {
	//	self.reader.in_memory_data_addresses_read()
	//}

	//pub fn in_memory_data_chunks_read(&self) -> u64 {
	//	self.reader.in_memory_data_chunks_read()
	//}

	//pub fn persistent_data_chunks_read(&self) -> u64 {
	//	self.reader.persistent_data_chunks_read()
	//}

	pub fn get_max_timestamp(&self, s: u64, p: u64) -> Option<u64> {
		self.reader.get_max_timestamp(s, p)
	}

	fn get_bitmap_ids_for_range_u64(
		&mut self,
		s: u64,
		p: u64,
		index_id: u64,
		field_min: u64,
		field_max: u64,
	) -> HashSet<u32> {
		self.reader
			.get_bitmap_ids_for_range_u64(s, p, index_id, field_min, field_max)
	}

	fn get_index(
		&self,
		s: u64,
		p: u64,
		id: u64,
	) -> Option<Rc<IndexSpecification>> {
		self.reader.get_index(s, p, id)
	}

	fn get_data_head_addr(&self, s: u64, p: u64) -> u64 {
		match self.reader.get_data_head_addr(s, p) {
			Some(x) => x,
			None => u64::MAX,
		}
	}

	pub fn get_time_head_addr(&self, s: u64, p: u64) -> u64 {
		match self.reader.sources.get(&(s, p)) {
			Some(x) => x.ts_address,
			None => u64::MAX,
		}
	}

	// Get entry directly, don't read the chunk to save some I/O and processing
	// time on index access
	fn get_time_tsl_entry(&mut self, addr: u64) -> &Entry {
		let entry = &mut self.reader.tmp_entry;
		self.reader.time_tsl.read_addr(addr, entry).unwrap();
		entry
	}

	// Returns the index, the timestamp the index was written, and the address
	// of the next index
	fn get_index_tsl_entry_with_timestamp(
		&mut self,
		p: u64,
		ts: u64,
	) -> (RoIndex, u64, u64) {
		let index_snapshot = self.reader.indexes[p as usize].as_ref().unwrap();
		let index_addr_head = index_snapshot.addr;

		let mut addr = index_snapshot.time_tsl_head;
		let mut index_addr = u64::MAX;

		if ts == u64::MAX {
			let entry = self.get_time_tsl_entry(addr); // note this one might be slow
			index_addr = u64::from_be_bytes(entry.data[..].try_into().unwrap());
		} else {
			// traverse the time tsl
			loop {
				let entry = self.get_time_tsl_entry(addr); // note this one might be slow
				addr = entry.last_addr;
				if entry.timestamp < ts {
					break;
				}
				index_addr =
					u64::from_be_bytes(entry.data[..].try_into().unwrap());
			}
		}

		// This can happen if ts is after last indexed timestamp
		if index_addr == u64::MAX {
			index_addr = index_addr_head;
		}

		assert!(index_addr != u64::MAX);
		self.get_index_tsl_entry(index_addr)
	}

	// Get entry directly, don't read the chunk to save some I/O and processing
	// time on index access.
	// Adjusts the address internally if we get an address that is the chunk
	// tail.
	// Returns the index, the timestamp the index was written, and the address
	// of the next index
	fn get_index_tsl_entry(&mut self, addr: u64) -> (RoIndex, u64, u64) {
		let mut idx_addr = addr;
		let tmp_entry = &mut self.reader.tmp_entry;
		match self.reader.index_tsl.read_addr(addr, tmp_entry) {
			Ok(_) => {}
			Err(MachError::ChunkTail) => {
				idx_addr =
					idx_addr - (idx_addr % CHUNK_SZ as u64) + CHUNK_SZ as u64;
				self.reader
					.index_tsl
					.read_addr(idx_addr, tmp_entry)
					.unwrap();
			}
			_ => panic!("Unhandled internal error"),
		}
		let index: RoIndex = Index::deserialize_ro_index(&tmp_entry.data[..]);
		(index, tmp_entry.timestamp, tmp_entry.last_addr)
	}

	fn get_index_tsl_entry_with_bitmap_values2(
		&mut self,
		addr: u64,
		bitmap_values: &HashSet<u32>,
		index: &mut RoIndex,
	) -> (u64, u64) {
		let mut idx_addr = addr;
		let tmp_entry = &mut self.reader.tmp_entry;
		match self.reader.index_tsl.read_addr(addr, tmp_entry) {
			Ok(_) => {}
			Err(MachError::ChunkTail) => {
				idx_addr =
					idx_addr - (idx_addr % CHUNK_SZ as u64) + CHUNK_SZ as u64;
				self.reader
					.index_tsl
					.read_addr(idx_addr, tmp_entry)
					.unwrap();
			}
			_ => panic!("Unhandled internal error"),
		}

		Index::deserialize_ro_index_with_bitmap_values_into(
			&tmp_entry.data[..],
			bitmap_values,
			index,
		);
		(tmp_entry.timestamp, tmp_entry.last_addr)
	}

	//fn get_index_tsl_entry_with_bitmap_values(
	//	&mut self,
	//	addr: u64,
	//	bitmap_values: &HashSet<u32>,
	//) -> (RoIndex, u64, u64) {
	//	let mut idx_addr = addr;
	//	let tmp_entry = &mut self.reader.tmp_entry;
	//	match self.reader.index_tsl.read_addr(addr, tmp_entry) {
	//		Ok(_) => {}
	//		Err(MachError::ChunkTail) => {
	//			idx_addr =
	//				idx_addr - (idx_addr % CHUNK_SZ as u64) + CHUNK_SZ as u64;
	//			self.reader
	//				.index_tsl
	//				.read_addr(idx_addr, tmp_entry)
	//				.unwrap();
	//		}
	//		_ => panic!("Unhandled internal error"),
	//	}
	//	let index: RoIndex = Index::deserialize_ro_index_with_bitmap_values(
	//		&tmp_entry.data[..],
	//		bitmap_values,
	//	);
	//	(index, tmp_entry.timestamp, tmp_entry.last_addr)
	//}

	fn get_entry_on_or_before_timestamp(
		&mut self,
		s: u64,
		p: u64,
		ts: u64,
	) -> Option<&block::Entry> {
		self.reader.get_entry_on_or_before_timestamp(s, p, ts)
	}

	//fn cache_entries_in_chunk_with_addr(&mut self, p: u64, addr: u64) {
	//	let chunk = self.reader.get_chunk_with_addr(p, addr).unwrap();
	//	let entries = chunk.parse();
	//	for entry in entries {
	//		let addr = entry.address;
	//		self.entry_cache.insert((p, addr), entry);
	//	}
	//}

	//fn get_cached_data_entry(&mut self, p: u64, addr: u64) -> RoChunkEntry {
	//	let entry = match self.entry_cache.get(&(p, addr)) {
	//		Some(x) => x,
	//		None => {
	//			self.cache_entries_in_chunk_with_addr(p, addr);
	//			self.entry_cache.get(&(p, addr)).unwrap()
	//		}
	//	};
	//	entry.clone()
	//}
}

#[inline(always)]
fn in_range_u64(min: u64, max: u64, value: u64) -> bool {
	min <= value && value <= max
}

struct IndexesIterator<'a> {
	partition: u64,
	ts_min: u64,
	ts_max: u64,
	bitmap_values: &'a HashSet<u32>,
	ctx: &'a mut QueryContext,
	buf: RoIndex,
	next_addr: u64,
	done: bool,
}

impl<'a> IndexesIterator<'a> {
	fn new(
		partition: u64,
		ts_min: u64,
		ts_max: u64,
		bitmap_values: &'a HashSet<u32>,
		ctx: &'a mut QueryContext,
	) -> Self {
		let buf = RoIndex {
			map: new_hashmap(),
			chunk_id: 0,
		};
		let next_addr = u64::MAX;
		let done = false;
		Self {
			partition,
			ts_min,
			ts_max,
			bitmap_values,
			ctx,
			buf,
			next_addr,
			done,
		}
	}

	fn next_index(&mut self) -> Option<(&RoIndex, u64)> {
		self.buf.map.clear();
		if self.done {
			return None;
		}

		if self.next_addr == u64::MAX {
			let (idx, ts, next_addr) =
				self.ctx.get_index_tsl_entry_with_timestamp(
					self.partition,
					self.ts_max,
				);
			self.buf = idx;
			self.next_addr = next_addr;
			return Some((&self.buf, ts));
		}

		let (ts, next_addr) = self.ctx.get_index_tsl_entry_with_bitmap_values2(
			self.next_addr,
			self.bitmap_values,
			&mut self.buf,
		);

		if ts < self.ts_min {
			self.done = true;
			return None;
		}

		if next_addr == u64::MAX {
			self.done = true;
		} else {
			assert!(self.next_addr > next_addr);
		}
		self.next_addr = next_addr;
		Some((&self.buf, ts))
	}
}

pub fn indexed_percentile_u64(
	source: u64,
	partition: u64,
	ts_min: u64,
	ts_max: u64,
	index_id: u64,
	percentile: f64, // between 0 and 1
	ctx: &mut QueryContext,
) -> u64 {
	println!("Query source: {}", source);

	assert!(percentile >= 0. && percentile <= 1.);

	let index_spec: Rc<IndexSpecification> =
		ctx.get_index(source, partition, index_id).unwrap();

	let now = Instant::now();
	let bitmap_ids = ctx.get_bitmap_ids_for_range_u64(
		source,
		partition,
		index_id,
		0,
		u64::MAX,
	);
	println!("Bitmap ids: {:?}", bitmap_ids);

	println!(">> Query prelude {:?}", now.elapsed());

	//let index_scan_start = Instant::now();

	let mut indexes_in_time_iterator =
		IndexesIterator::new(partition, ts_min, ts_max, &bitmap_ids, ctx);

	// for each index, check if its chunk might contain data that qualifies
	let mut data_chunks = Vec::new();

	let mut bin_map = new_hashmap();
	bitmap_ids.iter().for_each(|x| {
		bin_map.insert(*x, 0u64);
	});
	//let mut result_count = 0;

	let mut tmp_bin_map = new_hashmap();

	//let now = Instant::now();
	let mut index_scan_max_time = u64::MAX;

	let mut bin_to_data_chunks = new_hashmap();

	let now = Instant::now();
	while let Some((index, ts)) = indexes_in_time_iterator.next_index() {
		if index_scan_max_time == u64::MAX {
			index_scan_max_time = ts;
		}

		tmp_bin_map.clear();
		let mut max_addr = 0;
		let min_addr = index.chunk_id * CHUNK_SZ as u64;
		let mut covered = true;
		let mut contains = false;

		for bitmap_id in bitmap_ids.iter() {
			let mut data_chunk_has_bin = false;
			if let Some(index_entry) = index.map.get(bitmap_id) {
				// Check if the data in this bucket is all within the requested
				// time range
				let (idx_min_ts, idx_max_ts) = index_entry.get_time_range();
				let min_in = ts_min <= idx_min_ts && idx_min_ts <= ts_max;
				let max_in = ts_min <= idx_max_ts && idx_max_ts <= ts_max;
				if !min_in || !max_in {
					covered = false;
				}

				// We ignore these results if not this index is no longer
				// covered
				if covered {
					let count = tmp_bin_map.entry(bitmap_id).or_insert(0u64);
					let c = index_entry.get_count();
					*count += c;
					//result_count += c;
				}

				// We still want to collect this in case the index will not be
				// covered in a later bitmap
				if min_in || max_in {
					data_chunk_has_bin = true;
					contains = true;
					max_addr = max_addr.max(index_entry.max_addr());
				}
			}

			// We build this for later scan
			if data_chunk_has_bin {
				bin_to_data_chunks
					.entry(bitmap_id)
					.or_insert_with(Vec::new)
					.push((max_addr, min_addr));
			}
		}

		if !covered && contains {
			data_chunks.push((max_addr, min_addr));
		} else if covered {
			for (k, v) in tmp_bin_map.iter_mut() {
				*bin_map.get_mut(k).unwrap() += *v;
			}
		}
	}
	println!("1. Scanned all indexes in {:?}", now.elapsed());

	let now = Instant::now();
	for (starting_addr, min_addr) in data_chunks {
		let mut addr = starting_addr;
		while addr >= min_addr {
			let entry = ctx.reader.get_entry(addr, partition).unwrap();
			addr = entry.last_addr;

			if entry.source != source {
				println!("entry source {} != source {}", entry.source, source);
				assert!(entry.source == source);
			}

			let ts = entry.timestamp;
			let in_time = ts_min <= ts && ts <= ts_max;

			if in_time {
				let field = index_spec.get_value_u64(&entry.data[..]);
				let bitmap_id = index_spec.get_bitmap_id_u64(field);
				*bin_map.get_mut(&bitmap_id).unwrap() += 1;
				//result_count += 1;
			}
		}
	}
	println!("2. Scanned data chunks that were partially covered by an index in {:?}", now.elapsed());

	let now = Instant::now();
	let mut addr = u64::MAX;
	let mut scanned_items = 0;

	// We scan until the largest address we got from indexing. If we didnt get
	// anything from the index can, we need to scan the entire thing so we set
	// first addr to 0. When we check if we should break the loop, we check addr
	// agains first addr, or in the case it's always going to be greater (e.g.,
	// > 0), we stop when u64::MAX
	if index_scan_max_time == u64::MAX {
		index_scan_max_time = 0;
	}

	let mut unindexed_fields_by_bin = new_hashmap();
	bitmap_ids.iter().for_each(|x| {
		unindexed_fields_by_bin.insert(*x, Vec::new());
	});

	loop {
		let entry = if addr == u64::MAX {
			let e = ctx
				.get_entry_on_or_before_timestamp(source, partition, ts_max)
				.unwrap();
			e
		} else {
			ctx.reader.get_entry(addr, partition).unwrap()
		};

		if entry.timestamp <= index_scan_max_time {
			//println!("Breaking because we are now past the first element of the chunk scan");
			break;
		}

		if entry.timestamp < ts_min {
			break;
		}
		scanned_items += 1;
		addr = entry.last_addr;

		if in_range_u64(ts_min, ts_max, entry.timestamp) {
			let field = index_spec.get_value_u64(&entry.data[..]);
			let bitmap_id = index_spec.get_bitmap_id_u64(field);
			*bin_map.get_mut(&bitmap_id).unwrap() += 1;
			//result_count += 1;

			// also store unindexed records for later use so we dont have to
			// traverse unindexed records again.
			unindexed_fields_by_bin
				.get_mut(&bitmap_id)
				.unwrap()
				.push(field);
		}
	}
	println!("3. Scanned {scanned_items} unindexed records after last indexed timestamp in {:?}", now.elapsed());

	let now = Instant::now();

	//let percentile_calculation_start = Instant::now();

	// Calculate the percentiles for every bucket to find out which bucket the
	// requested percentile falls in
	let bins = index_spec.get_bin_min_bounds_u64();
	let mut bin_vec: Vec<(u32, u64)> = Vec::new();
	let mut total = 0;
	for item in bins.iter() {
		match bin_map.get(&item.0) {
			Some(x) => {
				bin_vec.push((item.0, *x));
				total += *x;
			}
			None => bin_vec.push((item.0, 0)),
		};
	}

	let mut curr_percentile = 0.0;
	let mut curr_bin = 0;
	//let mut curr_bin_idx = 0;
	//let bin_count = bin_vec.len();
	for (bin, count) in bin_vec {
		let perc = count as f64 / total as f64;
		curr_bin = bin;

		// If this bin adds enough items s.t. we exceed the percentile, we exit
		if curr_percentile + perc > percentile {
			break;
		}
		curr_percentile += perc;
		//curr_bin_idx += 1;
	}

	println!("Curr percentile: {}", curr_percentile);
	println!("Curr bin: {}", curr_bin);

	// Percentile adjustments
	let remaining = percentile - curr_percentile;
	let remaining_count = (total as f64 * remaining).floor() as u64;
	println!(
		"Remaining tile points {:?} Remaining count: {}",
		remaining, remaining_count
	);

	// Scan every entry in that bucket to find the right value
	let mut records = Vec::new();
	records.extend_from_slice(
		unindexed_fields_by_bin.get(&curr_bin).unwrap().as_slice(),
	);

	let data_chunks = bin_to_data_chunks.get(&curr_bin).unwrap();

	println!("Number of data chunks to scan: {}", data_chunks.len());
	let mut percentile_calculation_scanned_items = 0;
	for (max_addr, min_addr) in data_chunks {
		let max_addr = *max_addr;
		let min_addr = *min_addr;
		let mut addr = max_addr;
		let mut count = 0;
		while addr >= min_addr && addr != u64::MAX {
			count += 1;

			let entry = ctx.reader.get_entry(addr, partition).unwrap();
			addr = entry.last_addr;

			if entry.source != source {
				println!(
					"entry source {} != source {} addr: {} count: {}",
					entry.source, source, addr, count
				);
				assert!(entry.source == source);
			}

			let ts = entry.timestamp;
			let in_time = ts_min <= ts && ts <= ts_max;

			if in_time {
				let field = index_spec.get_value_u64(&entry.data[..]);
				if index_spec.get_bitmap_id_u64(field) == curr_bin {
					records.push(field);
					percentile_calculation_scanned_items += 1;
				}
			}
		}
	}
	records.sort();
	let records_in_vec = records.len();
	println!("4. Scanned {percentile_calculation_scanned_items} records in bucket {curr_bin} to find percentile in {:?}. Records in memory {records_in_vec}", now.elapsed());

	records[remaining_count as usize]
}

#[derive(Debug, Copy, Clone)]
pub struct AggregationU64 {
	pub sum: u64,
	pub count: u64,
	pub min: u64,
	pub max: u64,
}

pub fn indexed_aggregation_u64(
	source: u64,
	partition: u64,
	ts_min: u64,
	ts_max: u64,
	index_id: u64,
	ctx: &mut QueryContext,
) -> AggregationU64 {
	//let now = Instant::now();

	let mut result_sum = 0;
	let mut result_count = 0;
	let mut result_min = u64::MAX;
	let mut result_max = u64::MIN;

	let index_spec: Rc<IndexSpecification> =
		ctx.get_index(source, partition, index_id).unwrap();

	let bitmap_ids = ctx.get_bitmap_ids_for_range_u64(
		source,
		partition,
		index_id,
		0,
		u64::MAX,
	);
	println!("Bitmap ids: {:?}", bitmap_ids);

	let mut indexes_in_time_iterator =
		IndexesIterator::new(partition, ts_min, ts_max, &bitmap_ids, ctx);

	// for each index, check if its chunk might contain data that qualifies
	let mut index_scan_max_time = u64::MAX;

	let mut data_chunks = Vec::new();

	let now = Instant::now();
	while let Some((index, ts)) = indexes_in_time_iterator.next_index() {
		if index_scan_max_time == u64::MAX {
			println!("Indexed timestamp: {}", ts);
			index_scan_max_time = ts;
		}
		assert!(ts <= index_scan_max_time);

		// chunk specific aggregations used if the chunk is covered entirely
		let mut chunk_sum = 0;
		let mut chunk_min = u64::MAX;
		let mut chunk_max = 0;
		let mut chunk_count = 0;

		let mut max_addr = 0;
		let min_addr = index.chunk_id * CHUNK_SZ as u64;
		let mut covered = true;
		let mut contains = false;

		for bitmap_id in bitmap_ids.iter() {
			if let Some(index_entry) = index.map.get(bitmap_id) {
				let (idx_min_ts, idx_max_ts) = index_entry.get_time_range();
				//let (idx_min_v, idx_max_v) = index_entry.get_value_range_u64();
				//let idx_count = index_entry.get_count();
				//let idx_sum = index_entry.get_sum_u64();

				let min_in = ts_min <= idx_min_ts && idx_min_ts <= ts_max;
				let max_in = ts_min <= idx_max_ts && idx_max_ts <= ts_max;

				if !min_in || !max_in {
					covered = false;
				}

				// We ignore these results if not this index is no longer
				// covered
				if covered {
					let (idx_min_v, idx_max_v) =
						index_entry.get_value_range_u64();
					let idx_count = index_entry.get_count();
					let idx_sum = index_entry.get_sum_u64();
					chunk_count += idx_count;
					chunk_sum += idx_sum;
					chunk_min = chunk_min.min(idx_min_v);
					chunk_max = chunk_max.max(idx_max_v);
				}

				// We still want to collect this in case the index will not be
				// covered in a later bitmap
				if min_in || max_in {
					contains = true;
					max_addr = max_addr.max(index_entry.max_addr());
				}
			}
		}

		if covered {
			result_count += chunk_count;
			result_sum += chunk_sum;
			result_min = result_min.min(chunk_min);
			result_max = result_max.max(chunk_max);
		} else if contains {
			data_chunks.push((max_addr, min_addr));
		}
	}
	println!("1. Scanned indexes in {:?}", now.elapsed());

	let now = Instant::now();
	let mut count = 0;
	let len = data_chunks.len();
	for (starting_addr, min_addr) in data_chunks {
		let mut addr = starting_addr;
		while addr >= min_addr {
			count += 1;
			let entry = ctx.reader.get_entry(addr, partition).unwrap();
			addr = entry.last_addr;

			if entry.source != source {
				println!("entry source {} != source {}", entry.source, source);
				assert!(entry.source == source);
			}

			let ts = entry.timestamp;
			let in_time = ts_min <= ts && ts <= ts_max;

			let field = index_spec.get_value_u64(&entry.data[..]);

			if in_time {
				result_count += 1;
				result_sum += field;
				result_min = result_min.min(field);
				result_max = result_max.max(field);
			}
		}
	}
	println!("2. Scanned {count} records in {len} indexed chunks that may contain a record {:?}", now.elapsed());

	let now = Instant::now();
	let mut addr = u64::MAX;
	let mut scanned_items = 0;

	// We scan until the largest address we got from indexing. If we didnt get
	// anything from the index can, we need to scan the entire thing so we set
	// first addr to 0. When we check if we should break the loop, we check addr
	// agains first addr, or in the case it's always going to be greater (e.g.,
	// > 0), we stop when u64::MAX
	if index_scan_max_time == u64::MAX {
		index_scan_max_time = 0;
	}

	loop {
		let entry = if addr == u64::MAX {
			//let now = Instant::now();
			let e = ctx
				.get_entry_on_or_before_timestamp(source, partition, ts_max)
				.unwrap();
			e
		} else {
			ctx.reader.get_entry(addr, partition).unwrap()
		};

		if entry.timestamp <= index_scan_max_time {
			println!("Scan breaking because we've reached indexed timestamp");
			println!(
				"Entry timestamp {} index scan max time {}",
				entry.timestamp, index_scan_max_time
			);
			break;
		}

		if entry.timestamp < ts_min {
			break;
		}
		scanned_items += 1;
		addr = entry.last_addr;

		if in_range_u64(ts_min, ts_max, entry.timestamp) {
			let field = index_spec.get_value_u64(&entry.data[..]);
			result_count += 1;
			result_sum += field;
			result_min = result_min.min(field);
			result_max = result_max.max(field);
		}
	}
	println!("3. Scanned {scanned_items} unindexed records after last indexed timestamp {:?}", now.elapsed());

	AggregationU64 {
		count: result_count,
		sum: result_sum,
		min: result_min,
		max: result_max,
	}
}

// pub fn multi_indexed_range_scan_u64(
// 	spi: &[(u64, u64, u64)],
// 	ts_min: u64,
// 	ts_max: u64,
// 	field_min: u64, // inclusive
// 	field_max: u64, // inclusive
// 	ctx: &mut QueryContext,
// ) -> Vec<Entry> {
// 	let mut entries = Vec::new();
// 	for item in spi.iter() {
// 		let (source, partition, index) = *item;
// 		let mut tmp = indexed_range_scan_u64(
// 			source, partition, ts_min, ts_max, index, field_min, field_max, ctx,
// 		);
// 		entries.append(&mut tmp);
// 	}
// 	entries
// }

pub fn indexed_range_scan_u64<F: FnMut(u64, u64, &[u8])>(
	source: u64,
	partition: u64,
	ts_min: u64,
	ts_max: u64,

	index_id: u64,
	field_min: u64, // inclusive
	field_max: u64, // inclusive

	mut payload_func: F,

	ctx: &mut QueryContext,
) {
	println!("Source: {}", source);

	let index_spec: Rc<IndexSpecification> =
		ctx.get_index(source, partition, index_id).unwrap();

	let bitmap_ids = ctx.get_bitmap_ids_for_range_u64(
		source, partition, index_id, field_min, field_max,
	);

	println!("Bitmap id len: {}", bitmap_ids.len());

	let mut indexes_in_time_iterator =
		IndexesIterator::new(partition, ts_min, ts_max, &bitmap_ids, ctx);

	// for each index, check if its chunk might contain data that qualifies
	let mut index_scan_max_time = u64::MAX;

	let mut data_chunks = Vec::new();

	let now = Instant::now();
	while let Some((index, ts)) = indexes_in_time_iterator.next_index() {
		if index_scan_max_time == u64::MAX {
			index_scan_max_time = ts;
		}
		let mut max_addr = 0;
		let min_addr = index.chunk_id * CHUNK_SZ as u64;
		let mut contains = false;
		for bitmap_id in bitmap_ids.iter() {
			if let Some(index_entry) = index.map.get(bitmap_id) {
				//assert!(source == index_entry.source_id());

				let (idx_min_ts, idx_max_ts) = index_entry.get_time_range();
				let (idx_min_v, idx_max_v) = index_entry.get_value_range_u64();

				let in_time_range = (ts_min <= idx_min_ts
					&& idx_min_ts <= ts_max)
					|| (ts_min <= idx_max_ts && idx_max_ts <= ts_max);

				let in_value_range = (field_min <= idx_min_v
					&& idx_min_v <= field_max)
					|| (field_min <= idx_max_v && idx_max_v <= field_max);

				if in_time_range && in_value_range {
					max_addr = max_addr.max(index_entry.max_addr());
					contains = true;
				}
			}
		}
		if contains {
			assert!(max_addr > 0);
			assert!(max_addr < u64::MAX);
			data_chunks.push((max_addr, min_addr));
		}
	}

	println!("1. Scanned indexes in {:?}", now.elapsed());

	let mut result_count = 0;

	let now = Instant::now();
	let mut addr = u64::MAX;
	let mut scanned_items = 0;

	// We scan until the largest address we got from indexing. If we didnt get
	// anything from the index can, we need to scan the entire thing so we set
	// first addr to 0. When we check if we should break the loop, we check addr
	// agains first addr, or in the case it's always going to be greater (e.g.,
	// > 0), we stop when u64::MAX
	if index_scan_max_time == u64::MAX {
		index_scan_max_time = 0;
	}

	println!("Index scan max timestamp: {index_scan_max_time}, requested timestamp: {ts_max}");
	if index_scan_max_time < ts_max {
		println!("Scanning items with ts > indexed timestamp");
		loop {
			let entry = if addr == u64::MAX {
				match ctx
					.get_entry_on_or_before_timestamp(source, partition, ts_max)
				{
					Some(x) => x,
					None => {
						// Could be none if ts_max > time
						// index's last index
						let a = ctx.get_data_head_addr(source, partition);
						let e = ctx.reader.get_entry(a, partition).unwrap();
						e
					}
				}
			} else {
				let e = ctx.reader.get_entry(addr, partition).unwrap();
				e
			};

			if entry.timestamp <= index_scan_max_time {
				println!("Stopping scanning because entry timestamp {} <= {index_scan_max_time}", entry.timestamp);
				break;
			}

			if entry.timestamp < ts_min {
				println!("Stopping scanning because of ts_min {ts_min} > entry timestamp {}", entry.timestamp);
				break;
			}
			scanned_items += 1;
			addr = entry.last_addr;

			if in_range_u64(ts_min, ts_max, entry.timestamp) {
				let field = index_spec.get_value_u64(&entry.data[..]);
				if in_range_u64(field_min, field_max, field) {
					payload_func(entry.timestamp, field, &entry.data[..]);
					result_count += 1;
					//result.push(entry.clone());
				}
			}
		}
	}
	println!("2. Scanned {scanned_items} unindexed records after last indexed timestamp {:?}", now.elapsed());

	let mut count = 0;
	let now = Instant::now();
	let len = data_chunks.len();
	println!("Scanning {} data chunks", len);
	for (starting_addr, min_addr) in data_chunks {
		let mut addr = starting_addr;
		//let mut count = 0;
		while addr >= min_addr {
			count += 1;
			let entry = ctx.reader.get_entry(addr, partition).unwrap();
			addr = entry.last_addr;

			assert!(entry.source == source);
			//if entry.source != source {
			//	println!(
			//		"entry source {} != source {}, addr: {}, count: {}",
			//		entry.source, source, addr, count
			//	);
			//}

			let ts = entry.timestamp;
			let in_time = ts_min <= ts && ts <= ts_max;

			let field = index_spec.get_value_u64(&entry.data[..]);
			let in_field = field_min <= field && field <= field_max;
			//chunk_scan_max_time = chunk_scan_max_time.max(ts);

			if in_time && in_field {
				payload_func(ts, field, &entry.data[..]);
				result_count += 1;
			}
		}
	}

	println!("3. Scanned {count} records in {len} indexed chunks that may contain a record {:?}",
			 now.elapsed());

	//result.append(&mut chunk_scan_entries);
	println!("Index scan result: {result_count}");
}

pub fn raw_percentile_u64(
	s: u64,
	p: u64,
	ts_min: u64,
	ts_max: u64,
	field_func: fn(&[u8]) -> u64,
	percentile: f64,
	ctx: &mut QueryContext,
) -> u64 {
	assert!(percentile >= 0. && percentile <= 1.);

	let mut result_fields = Vec::new();
	let mut last_addr = ctx.get_data_head_addr(s, p);
	let mut scanned_items = 0;
	while last_addr < u64::MAX {
		let entry = ctx.reader.get_entry(last_addr, p).unwrap();
		last_addr = entry.last_addr;

		let ts = entry.timestamp;

		if ts > ts_max {
			continue;
		}

		if ts < ts_min {
			break;
		}

		scanned_items += 1;
		let in_time = ts_min <= ts && ts <= ts_max;
		if in_time {
			let indexed_value = field_func(&entry.data[..]);
			result_fields.push(indexed_value);
		}
	}
	result_fields.sort();
	let idx = (result_fields.len() as f64 * percentile).floor() as usize;
	println!(">> Raw scan scanned items {}", scanned_items);
	result_fields[idx]
}

pub fn raw_scan_u64<F: FnMut(u64, u64, &[u8])>(
	s: u64,
	p: u64,
	ts_min: u64,
	ts_max: u64,
	field_func: fn(&[u8]) -> u64,
	field_min: u64, // inclusive
	field_max: u64, // inclusive
	mut payload_func: F,
	ctx: &mut QueryContext,
) {
	let mut last_addr = ctx.get_data_head_addr(s, p);
	let mut scanned_items = 0;
	//let mut result_count = 0;
	let mut found = false;
	let mut now = Instant::now();
	while last_addr < u64::MAX {
		let entry = ctx.reader.get_entry(last_addr, p).unwrap();
		last_addr = entry.last_addr;

		let ts = entry.timestamp;

		if ts > ts_max {
			continue;
		}
		if !found {
			found = true;
			println!("Found first ts in {:?}", now.elapsed());
			now = Instant::now();
		}

		if ts < ts_min {
			println!("Found last ts in {:?}", now.elapsed());
			break;
		}

		scanned_items += 1;
		let in_time = ts_min <= ts && ts <= ts_max;
		if in_time {
			if field_min == 0 && field_max == u64::MAX {
				let indexed_value = field_func(&entry.data[..]);
				payload_func(ts, indexed_value, &entry.data[..]);
			} else {
				let indexed_value = field_func(&entry.data[..]);
				if field_min <= indexed_value && indexed_value <= field_max {
					payload_func(ts, indexed_value, &entry.data[..]);
				}
			}
		}
	}
	println!("Scanned items: {scanned_items}");
}

pub fn range_index_only_scan_u64<F: FnMut(u64, u64, &[u8])>(
	source: u64,
	partition: u64,
	ts_min: u64,
	ts_max: u64,

	index_id: u64,
	field_min: u64, // inclusive
	field_max: u64, // inclusive

	mut payload_func: F,

	ctx: &mut QueryContext,
) {
	let new_ts_max = ctx.reader.now_micros() - 1000000;
	let now = Instant::now();
	let mut first = false;
	// This func only executes when the ts is between 0 and ts_max
	// (inclusive) per idnexed_range_scan_u64 so...
	let new_payload_func = |ts: u64, x: u64, b: &[u8]| {
		// ... we only have to check for ts_min
		if ts < ts_max {
			if first {
				println!("Found first in {:?}", now.elapsed());
				first = true;
			}
			payload_func(ts, x, b);
		}
	};

	// Effectively, this would cause the range scan to start at earliest
	// range index
	indexed_range_scan_u64(
		source,
		partition,
		ts_min,
		new_ts_max,
		index_id,
		field_min,
		field_max,
		new_payload_func,
		ctx,
	)
}

pub fn time_index_only_scan_u64<F: FnMut(u64, u64, &[u8])>(
	source: u64,
	partition: u64,
	ts_min: u64,
	ts_max: u64,

	field_func: fn(&[u8]) -> u64,
	field_min: u64, // inclusive
	field_max: u64, // inclusive

	mut payload_func: F,

	ctx: &mut QueryContext,
) {
	let now = Instant::now();
	let mut entry = ctx
		.get_entry_on_or_before_timestamp(source, partition, ts_max)
		.unwrap();
	println!("Found first entry in {:?}", now.elapsed());

	let mut scanned_items = 0;
	loop {
		scanned_items += 1;
		let last_addr = entry.last_addr;
		let ts = entry.timestamp;

		if ts < ts_min {
			break;
		}

		let in_time = ts_min <= ts && ts <= ts_max;
		if in_time {
			if field_min == 0 && field_max == u64::MAX {
				let indexed_value = field_func(&entry.data[..]);
				payload_func(ts, indexed_value, &entry.data[..]);
			} else {
				let indexed_value = field_func(&entry.data[..]);
				if field_min <= indexed_value && indexed_value <= field_max {
					payload_func(ts, indexed_value, &entry.data[..]);
				}
			}
		}

		if last_addr == u64::MAX {
			break;
		}
		entry = ctx.reader.get_entry(last_addr, partition).unwrap();
	}
	println!("Scanned items: {scanned_items}");
}
