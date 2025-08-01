//mod durability;
//mod server;
mod aligned;
mod block;
mod error;
mod flat_bytes;
mod item;
mod partitions;
mod read_only_block;
//mod schema;
mod persistent_read_ctx;
mod storage2;
mod tsl;
mod utils;
//mod persistent_write_ctx;
//mod tsl_iouring;

mod read_only_chunk;

//mod range_index;
//use block::*;
//use constants::*;
//use durability::*;

pub mod constants;
pub use partitions::*;
//pub use schema::*;
pub use tsl::*;

#[cfg(test)]
mod end_to_end {
	use super::*;
	use crate::read_only_chunk::*;
	use rand::prelude::*;
	use std::{
		collections::HashMap,
		time::{Duration, Instant},
	};

	//fn gen_data() -> Vec<u8> {
	//    let mut rng = thread_rng();
	//    let mut v = vec![0u8; 4096];
	//    for i in v.iter_mut() {
	//        *i = rng.gen();
	//    }
	//    v
	//}

	fn gen_slices(data: &[DataItem], n_slices: usize) -> Vec<&[u8]> {
		let mut rng = thread_rng();
		let mut v = Vec::new();
		for _ in 0..n_slices {
			let idx: usize = rng.gen_range(0..data.len());
			v.push(&data[idx].bytes[..]);
		}
		v
	}

	fn gen_source_seq(n_sources: usize, len: usize) -> Vec<u64> {
		assert!(len > n_sources);
		let mut rng = thread_rng();
		let v: Vec<u64> = (0..n_sources as u64).collect();
		let mut res = Vec::new();

		while res.len() < len {
			res.extend_from_slice(&v);
		}

		res.shuffle(&mut rng);
		res[0..len].into()
	}

	struct DataItem {
		bytes: [u8; 128],
	}

	impl DataItem {
		fn new_index_target() -> Self {
			let mut rng = thread_rng();
			let to_index: u64 = rng.gen_range(20..30);
			let mut bytes = [0u8; 128];
			bytes[0..8].copy_from_slice(&to_index.to_be_bytes());
			for i in bytes[8..].iter_mut() {
				*i = rng.gen();
			}
			Self { bytes }
		}

		fn new_random() -> Self {
			let mut rng = thread_rng();
			let to_index: u64 = {
				let a = rng.gen_range(0..20);
				let b = rng.gen_range(30..100);
				let pick: bool = rng.gen();
				if pick {
					a
				} else {
					b
				}
			};

			let mut bytes = [0u8; 128];

			bytes[0..8].copy_from_slice(&to_index.to_be_bytes());
			for i in bytes[8..].iter_mut() {
				*i = rng.gen();
			}
			Self { bytes }
		}
	}

	/// Returns n_items DataItem
	fn gen_data(n_items: u64) -> Vec<DataItem> {
		(0..n_items).map(|_| DataItem::new_random()).collect()
	}

	fn gen_index_targets(n_items: u64) -> Vec<DataItem> {
		(0..n_items).map(|_| DataItem::new_index_target()).collect()
	}

	fn mach_write<'a>(
		mach: &'a mut Partitions,
		sources: &'a [u64],
		data: &'a [&'a [u8]],
		n_records: usize,
	) -> HashMap<u64, Vec<(u64, &'a [u8])>> {
		let mut t = Instant::now();
		let partitions = constants::PARTITIONS;
		let mut pushed_data = HashMap::new();

		let start = Instant::now();
		let mut counter = 0;
		loop {
			let source: u64 = sources[counter % sources.len()];
			let data: &[u8] = data[counter % data.len()];
			let partition: u64 = source % partitions as u64;
			mach.push(source, partition, counter as u64, data);
			pushed_data
				.entry(source)
				.or_insert_with(Vec::new)
				.push((counter as u64, data));
			counter += 1;
			if start.elapsed() > Duration::from_secs(10) {
				break;
			}
		}

		let mut synced = HashMap::new();
		for source in sources {
			if !synced.contains_key(source) {
				synced.insert(*source, true);
				mach.sync(*source, source % partitions as u64);
			}
		}

		pushed_data
	}

	fn mach_write_with_index_targets<'a>(
		mach: &'a mut Partitions,
		sources: &'a [u64],
		data: &'a [&'a [u8]],
		index_targets: &'a [&'a [u8]],
		n_records: usize,
	) -> HashMap<u64, Vec<(u64, u64, &'a [u8])>> {
		let mut t = Instant::now();
		let partitions = constants::PARTITIONS;
		let mut pushed_data = HashMap::new();

		let start = Instant::now();
		let mut counter = 0;
		let mut indexed_counters: HashMap<u64, u64> = HashMap::new();
		loop {
			let source: u64 = sources[counter % sources.len()];
			let partition: u64 = source % partitions as u64;

			let index_record: f64 = thread_rng().gen();
			let indexed_count = indexed_counters.entry(source).or_insert(0);
			let data: &[u8] = if index_record < 0.01 {
				*indexed_count += 1;
				index_targets[counter % index_targets.len()]
			} else {
				data[counter % data.len()]
			};

			let addr = mach.push(source, partition, counter as u64, data);
			pushed_data.entry(source).or_insert_with(Vec::new).push((
				counter as u64,
				addr,
				data,
			));
			counter += 1;
			if start.elapsed() > Duration::from_secs(10) {
				break;
			}
		}

		let mut synced = HashMap::new();
		for source in sources {
			if !synced.contains_key(source) {
				synced.insert(*source, true);
				mach.sync(*source, source % partitions as u64);
			}
		}

		pushed_data
	}

	//#[test]
	//fn iterator_scan() {
	//	let data = gen_data(4096);
	//	let slices = gen_slices(&data, 1024 * 32);
	//	let nsources = 10;
	//	let sources = gen_source_seq(nsources, 4 * nsources);
	//	let n_records = 1024 * 1024 * 8;

	//	println!("Writing to mach");
	//	let mut mach = Partitions::new(
	//		"/nvme/fsolleza/data/can_delete/test_iterator_scan".into(),
	//	);
	//	let reader = mach.reader();
	//	let expected = mach_write(&mut mach, &sources, &slices, n_records);

	//	println!("Reading and verifying");
	//	let partitions = constants::PARTITIONS as u64;
	//	for source in 0..nsources as u64 {
	//		println!("Doing source {}", source);
	//		let snapshot =
	//			reader.snapshot(&[(source, source % partitions)]).unwrap();
	//		let mut iter = snapshot.iterator();
	//		let mut result: Vec<(u64, Vec<u8>)> = Vec::new();
	//		while let Some(x) = iter.next_entry() {
	//			result.push((x.timestamp, x.data.as_slice().into()));
	//		}

	//		let result_iter = result.iter().rev();
	//		let expected = expected.get(&source).unwrap();

	//		for (a, b) in result_iter.zip(expected.iter()) {
	//			if a.1 != b.1 {
	//				panic!("Failed");
	//			}
	//		}
	//	}
	//}

	#[test]
	fn full_index_scan() {
		let data = gen_data(4096);
		let slices = gen_slices(&data, 1024 * 32);
		let nsources = 10;
		let sources = gen_source_seq(nsources, 4 * nsources);
		let n_records = 1024 * 1024 * 8;

		//println!("Generating expected results");
		//let expected = expected_result(&sources, &slices, n_records);

		println!("Writing to mach");
		let mut mach = Partitions::new(
			"/nvme/fsolleza/data/can_delete/test_full_index".into(),
		);
		let reader = mach.reader();
		let expected = mach_write(&mut mach, &sources, &slices, n_records);

		println!("Reading and verifying");
		let partitions = constants::PARTITIONS as u64;
		for source in 0..nsources as u64 {
			let s = source;
			let p = source % partitions;

			println!("Doing source {}", source);

			let mut reader = reader
				.indexed_reader(&[(source, source % partitions)])
				.unwrap();

			let mut result: Vec<(u64, Vec<u8>)> = Vec::new();
			let mut last_addr = reader.get_head_addr(s, p).unwrap();
			while last_addr < u64::MAX {
				let entry = reader.get_entry(last_addr, p).unwrap();
				result.push((entry.timestamp, entry.data.as_slice().into()));
				last_addr = entry.last_addr;
			}

			let result_iter = result.iter().rev();
			let expected = expected.get(&source).unwrap();

			for (a, b) in result_iter.zip(expected.iter()) {
				if a.1 != b.1 {
					panic!("Failed at timestamps {} {}", a.0, b.0);
				}
			}
		}
	}

	#[test]
	fn time_skip() {
		let data = gen_data(4096);
		let slices = gen_slices(&data, 1024 * 32);
		let nsources = 10;
		let sources = gen_source_seq(nsources, 4 * nsources);
		let n_records = 1024 * 1024 * 8;

		println!("Writing to mach");
		let mut mach = Partitions::new(
			"/nvme/fsolleza/data/can_delete/test_time_skip".into(),
		);
		let reader = mach.reader();
		let data = mach_write(&mut mach, &sources, &slices, n_records);

		println!("Reading and verifying");
		let partitions = constants::PARTITIONS as u64;
		for source in 0..nsources as u64 {
			let records = data.get(&source).unwrap();
			let (ts, data) = records[records.len() / 2];

			let s = source;
			let p = source % partitions;

			println!("Doing source {}", source);

			let mut reader = reader
				.indexed_reader(&[(source, source % partitions)])
				.unwrap();

			let entry = reader.get_entry_at_timestamp(s, p, ts).unwrap();
			assert_eq!(entry.timestamp, ts);
			assert_eq!(entry.data.as_slice(), data);
		}
	}

	#[test]
	fn index_filter() {
		let data = gen_data(4096);
		let slices = gen_slices(&data, 1024 * 32);

		let index_data = gen_index_targets(4096);
		let index_slices = gen_slices(&index_data, 4096);

		let nsources = 10;
		let sources = gen_source_seq(nsources, 4 * nsources);
		let n_records = 1024 * 1024 * 8;

		println!("Writing to mach");
		let mut mach = Partitions::new(
			"/nvme/fsolleza/data/can_delete/test_index_filter".into(),
		);
		let reader = mach.reader();
		let data = mach_write_with_index_targets(
			&mut mach,
			&sources,
			&slices,
			&index_slices,
			n_records,
		);

		println!("Reading and verifying");
		let partitions = constants::PARTITIONS as u64;
		for source in 0..nsources as u64 {
			println!("Doing source {}", source);

			let records = data.get(&source).unwrap();

			let p = source % partitions;
			let idx_id = source * 100 + 2;

			// Build expected count;
			let mut expected_count = 0;
			let mut expected_addrs: Vec<u64> = Vec::new();
			for (ts, addr, data) in records {
				let indexed_value =
					u64::from_be_bytes(data[..8].try_into().unwrap());
				if 20 <= indexed_value && indexed_value < 30 {
					expected_count += 1;
					expected_addrs.push(*addr);
				}
			}

			// Indexed access
			let mut chunks = 0;
			let mut indexed_count = 0;
			let mut reader = reader.indexed_reader(&[(source, p)]).unwrap();

			let mut index_addr = reader.get_index_head_addr(p).unwrap();
			let mut index = reader.get_index_entry(index_addr).unwrap();
			let addrs = index.indexed_addresses();
			println!("Indexed addresses: {:?}", addrs);

			// Read data until indexed chunk
			let mut last_addr = reader.get_head_addr(source, p).unwrap();
			let mut indexed_value = u64::MAX;
			let mut actual_addrs = Vec::new();
			while last_addr > addrs.max {
				let entry = reader.get_entry(last_addr, p).unwrap();
				indexed_value =
					u64::from_be_bytes(entry.data[..8].try_into().unwrap());
				if 20 <= indexed_value && indexed_value < 30 {
					actual_addrs.push(last_addr);
					indexed_count += 1;
				}
				last_addr = entry.last_addr;
			}
			println!("Indexed count: {}", indexed_count);

			let mut index_addr = reader.get_index_head_addr(p).unwrap();
			while index_addr < u64::MAX {
				let index = reader.get_index_entry(index_addr).unwrap();
				if index.bitmap.contains(idx_id as u32) {
					//println!("Parsing chunk: {}", index.chunk_id);
					chunks += 1;
					let chunk = reader.load_indexed_chunk(&index);
					let entries = RoChunkEntry::parse_chunk(&chunk);
					//println!("Parsed chunk: {}, got {} entries", index.chunk_id, entries.len());
					let mut found = false;
					for entry in entries {
						if source == entry.source {
							let indexed_value = u64::from_be_bytes(
								entry.data()[..8].try_into().unwrap(),
							);
							if 20 <= indexed_value && indexed_value < 30 {
								found = true;
								indexed_count += 1;
								actual_addrs.push(entry.address);
							}
						}
					}
					if !found {
						println!("Failed to find something where we expected in chunk id {}", index.chunk_id);
					}
				}
				index_addr = index.last_addr;
			}

			println!(
				"Read chunks: {} {} {}",
				chunks, indexed_count, expected_count
			);
			let expected_addrs: Vec<u64> =
				expected_addrs.iter().copied().rev().collect();
			for (i, (e, a)) in
				expected_addrs.iter().zip(actual_addrs.iter()).enumerate()
			{
				//println!("i: {}, e: {}, a: {}", i, e, a);
				if (e != a) {
					//println!("Failed at idx {}", i);
					//println!("{:?}", expected_addrs);
					//println!("{:?}", actual_addrs);
					assert_eq!(e, a);
				}
			}
			assert_eq!(indexed_count, expected_count);
		}
	}
}
