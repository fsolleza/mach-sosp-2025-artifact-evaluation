use mach_lib::*;

use rand::prelude::*;
use std::collections::HashMap;
use std::fs::remove_dir_all;
use std::time::Instant;
use std::sync::Arc;

fn gen_slices(data: &[DataItem]) -> Vec<&[u8]> {
	println!("Generating slices");
	let mut rng = thread_rng();
	let mut v = Vec::new();
	for _ in 0..32 * 1024 {
		let idx: usize = rng.gen_range(0..data.len());
		v.push(&data[idx].bytes[..]);
	}
	v
}

fn gen_source_seq(n_sources: usize, recs_per_source: usize) -> Vec<u64> {
	println!(
		"Generating source sequence list with {} and {} records per source",
		n_sources, recs_per_source
	);
	let mut rng = thread_rng();
	let mut v = Vec::new();
	for _ in 0..recs_per_source {
		for j in 0..n_sources {
			v.push(j as u64);
		}
	}
	v.shuffle(&mut rng);
	v
}

struct DataItem {
	bytes: [u8; 144],
}

impl DataItem {
	fn new_random() -> Self {
		let mut rng = thread_rng();
		let will_index: f64 = rng.gen();

		let to_index_u64: u64 = if will_index < 0.01f64 {
			rng.gen_range(2100..=2400)
		} else {
			let a = rng.gen_range(0..2000);
			let b = rng.gen_range(3000..10000);
			let pick: bool = rng.gen();
			if pick {
				a
			} else {
				b
			}
		};

		let to_index_f64: f64 = if will_index < 0.01f64 {
			0.22
		} else {
			let mut a: f64 = 0.22;
			while a >= 0.20 && a < 0.30 {
				a = rng.gen();
				if a < 0. {
					a *= -1.;
				}
			}
			a
		};

		let mut variable_length = [0u8; 128];
		for i in variable_length[..].iter_mut() {
			*i = rng.gen();
		}

		let mut bytes = [0u8; 144];
		bytes[..8].copy_from_slice(&to_index_u64.to_be_bytes()[..]);
		bytes[8..16].copy_from_slice(&to_index_f64.to_be_bytes()[..]);
		bytes[16..].copy_from_slice(&variable_length[..]);

		Self { bytes }
	}
}

/// Returns n_items DataItem
fn gen_data() -> Vec<DataItem> {
	println!("Generating data items");
	(0..4096).map(|_| DataItem::new_random()).collect()
}

//fn gen_index_targets() -> Vec<DataItem> {
//	println!("Generating index data items");
//	(0..4096).map(|_| DataItem::new_index_target()).collect()
//}

//fn mach_write_with_index_targets2<'a>(
//	mut mach: Partitions,
//	sources: &'a [u64],
//	data: &'a [&'a [u8]],
//	collect_expected_results: bool,
//) -> (Partitions, Option<HashMap<u64, Vec<(u64, u64, &'a [u8])>>>) {
//	// returns counter, address, data
//	let partitions = constants::PARTITIONS;
//	let mut pushed_data = HashMap::new();
//
//	let mut counter = 0;
//	let start_instant = Instant::now();
//	let mut payload_bytes_written = 0;
//	let mut total_bytes_written = 0;
//
//	let mut periodic_counter = 0;
//	let mut periodic_instant = Instant::now();
//
//	let now = Instant::now();
//	let mut loops = 0;
//	loop {
//		loops += 1;
//		for source in sources.iter() {
//			let source: u64 = *source;
//			let partition: u64 = source % partitions as u64;
//			let data: &[u8] = data[counter % data.len()];
//
//			payload_bytes_written += data.len();
//			total_bytes_written += data.len() + 64 * 3; // Includes source, time, partition
//
//			let (addr, ts) = mach.push(source, partition, data);
//			if collect_expected_results {
//				pushed_data
//					.entry(source)
//					.or_insert_with(Vec::new)
//					.push((ts, addr, data));
//			}
//			counter += 1;
//
//			if counter % (10 * 1024 * 1024) == 0 {
//				let n = counter - periodic_counter;
//				let dur: f64 = periodic_instant.elapsed().as_secs_f64();
//				println!("Samples per second: {:.2}", n as f64 / dur);
//
//				periodic_counter = counter;
//				periodic_instant = Instant::now();
//			}
//		}
//
//		if now.elapsed().as_secs() > 40 {
//			break;
//		}
//	}
//
//	println!("Done writing {} records, looped {} times", counter, loops);
//	let dur: f64 = start_instant.elapsed().as_secs_f64();
//	let mb = (1024 * 1024) as f64;
//	let payload_per_sec = (payload_bytes_written as f64 / dur) / mb;
//	let total_per_sec = (total_bytes_written as f64 / dur) / mb;
//	let records_per_sec = counter as f64 / dur;
//	println!("{} {} {}", payload_per_sec, total_per_sec, records_per_sec);
//
//	let mut synced = HashMap::new();
//	for source in sources {
//		if !synced.contains_key(source) {
//			synced.insert(*source, true);
//			mach.sync(*source, source % partitions as u64);
//		}
//	}
//
//	if collect_expected_results {
//		(mach, Some(pushed_data))
//	} else {
//		(mach, None)
//	}
//}


fn mach_write_with_index_targets<'a>(
	mach: &'a mut Partitions,
	sources: &'a [u64],
	data: &'a [&'a [u8]],
	collect_expected_results: bool,
) -> Option<HashMap<u64, Vec<(u64, u64, Vec<u8>)>>> {
	// returns counter, address, data
	let partitions = constants::PARTITIONS;
	let mut pushed_data = HashMap::new();

	let mut counter = 0;
	let start_instant = Instant::now();
	let mut payload_bytes_written = 0;
	let mut total_bytes_written = 0;

	let mut periodic_counter = 0;
	let mut periodic_instant = Instant::now();

	let now = Instant::now();
	let mut loops = 0;
	loop {
		loops += 1;
		for source in sources.iter() {
			let source: u64 = *source;
			let partition: u64 = source % partitions as u64;
			let data: &[u8] = data[counter % data.len()];

			payload_bytes_written += data.len();
			total_bytes_written += data.len() + 64 * 3; // Includes source, time, partition

			let (addr, ts) = mach.push(source, partition, data);
			if collect_expected_results {
				pushed_data
					.entry(source)
					.or_insert_with(Vec::new)
					.push((ts, addr, data.into()));
			}
			counter += 1;

			if counter % (10 * 1024 * 1024) == 0 {
				let n = counter - periodic_counter;
				let dur: f64 = periodic_instant.elapsed().as_secs_f64();
				println!("Samples per second: {:.2}", n as f64 / dur);

				periodic_counter = counter;
				periodic_instant = Instant::now();
			}
		}

		if now.elapsed().as_secs() > 40 {
			break;
		}
	}

	println!("Done writing {} records, looped {} times", counter, loops);
	let dur: f64 = start_instant.elapsed().as_secs_f64();
	let mb = (1024 * 1024) as f64;
	let payload_per_sec = (payload_bytes_written as f64 / dur) / mb;
	let total_per_sec = (total_bytes_written as f64 / dur) / mb;
	let records_per_sec = counter as f64 / dur;
	println!("{} {} {}", payload_per_sec, total_per_sec, records_per_sec);

	let mut synced = HashMap::new();
	for source in sources {
		if !synced.contains_key(source) {
			synced.insert(*source, true);
			mach.sync(*source, source % partitions as u64);
		}
	}

	if collect_expected_results {
		Some(pushed_data)
	} else {
		None
	}
}

fn index_u64_func(bytes: &[u8]) -> u64 {
	u64::from_be_bytes(bytes[..8].try_into().unwrap())
}

struct WriteEntry {
	ts: u64,
	addr: u64,
	data: Vec<u8>,
}

impl WriteEntry {
	fn as_slice(&self) -> &[u8] {
		self.data.as_slice()
	}
}

struct LoadedData {
	mach: Partitions,
	index_ids: HashMap<(u64, u64), u64>,
	written_data: HashMap<u64, Vec<WriteEntry>>,
}

struct LoadDataSpecification {
	path: &'static str,
	n_sources: usize,
	n_records_per_source: usize,
	index_min: u64,
	index_max: u64,
	index_bins: u64,
	verify: bool,
}

impl LoadDataSpecification {
	fn load_unindexed(&self) {
		let path = self.path;
		let n_sources = self.n_sources;
		let n_records_per_source = self.n_records_per_source;
		let data = gen_data();
		let slices = gen_slices(&data);
		let sources = gen_source_seq(n_sources, n_records_per_source);

		let mut mach = Partitions::new(path.into());

		for source in 0..n_sources {
			let source = source as u64;
			let partition = source % constants::PARTITIONS as u64;
			mach.define_source(source, partition);
		}

		let _ =
			mach_write_with_index_targets(&mut mach, &sources, &slices, false);
	}

	fn load_indexed(
		&self
	) -> Option<LoadedData> {
		let path = self.path;
		let n_sources = self.n_sources;
		let n_records_per_source = self.n_records_per_source;
		let index_min = self.index_min;
		let index_max = self.index_max;
		let index_bins = self.index_bins;
		let verify = self.verify;

		let data = gen_data();
		let slices = gen_slices(&data);
		let sources = gen_source_seq(n_sources, n_records_per_source);

		let mut mach = Partitions::new(path.into());

		let mut index_id_map: HashMap<(u64, u64), u64> = HashMap::new();
		for source in 0..n_sources {
			let source = source as u64;
			let partition = source % constants::PARTITIONS as u64;
			mach.define_source(source, partition);
			let index_id_u64 = mach.define_range_index_u64(
				source,
				partition,
				index_u64_func,
				index_min,
				index_max,
				index_bins,
				);
			println!("source {}, part: {}, index: {}", source, partition, index_id_u64);
			index_id_map.insert((source, partition), index_id_u64);
		}

		let tmp_written_data =
			mach_write_with_index_targets(&mut mach, &sources, &slices, verify);
		let tmp_written_data = tmp_written_data?;

		let mut written_data = HashMap::new();
		for (k, v) in tmp_written_data {
			let mut new_vec = Vec::new();
			for item in v {
				let w = WriteEntry {
					ts: item.0,
					addr: item.1,
					data: item.2,
				};
				new_vec.push(w);
			}
			written_data.insert(k, new_vec);
		}

		Some(LoadedData {
			mach,
			index_ids: index_id_map,
			written_data,
		})
	}
}

struct Foo {
	mach: Partitions,
	index_ids: HashMap<(u64, u64), u64>,
	written_data: HashMap<u64, Vec<WriteEntry>>,
}

fn index_scan_test2(
	path: &str,
	n_sources: usize,
	n_records_per_source: usize,
	verify: bool,
	bins_per_source: u64,
) {
	let data = gen_data();
	let slices = gen_slices(&data);
	let sources = gen_source_seq(n_sources, n_records_per_source);

	let mut mach = Partitions::new(path.into());
	//let reader = mach.reader();

	let mut index_id_map: HashMap<(u64, u64), u64> = HashMap::new();
	for source in 0..n_sources {
		let source = source as u64;
		let partition = source % constants::PARTITIONS as u64;
		mach.define_source(source, partition);

		let index_id_u64 = mach.define_range_index_u64(
			source,
			partition,
			index_u64_func,
			0,
			10_000,
			bins_per_source,
		);

		index_id_map.insert((source, partition), index_id_u64);
	}


	let tmp_written_data = mach_write_with_index_targets(&mut mach, &sources, &slices, verify);
	let tmp_written_data = tmp_written_data.unwrap();

	//let tmp_written_data = mach_write_with_index_targets(&mut mach, &sources, &slices, verify).unwrap();

	let mut written_data = HashMap::new();
	for (k, v) in tmp_written_data {
		let mut new_vec = Vec::new();
		for item in v {
			let w = WriteEntry {
				ts: item.0,
				addr: item.1,
				data: item.2,
			};
			new_vec.push(w);
		}
		written_data.insert(k, new_vec);
	}

	//let loaded_data = LoadedData {
	//	_data: data.into(),
	//	mach,
	//	index_ids: index_id_map,
	//	written_data,
	//};

	let foo = Foo { mach, index_ids: index_id_map, written_data };

	if !verify {
		return;
	}

	//let reader = loaded_data.mach.reader();
	//let data = tmp_written_data;
	let index_id_map = &foo.index_ids;
	let data = &foo.written_data;
	let reader = foo.mach.reader();

	let reader_timestamp = reader.now_micros();
	println!("TImestamp now: {}", reader_timestamp);
	let min_ts = reader_timestamp - 20 * 1_000_000;
	let max_ts = reader_timestamp - 10 * 1_000_000;

	let min_value = 2100;
	let max_value = 3500;

	println!("Reading and verifying");
	let partitions = constants::PARTITIONS as u64;
	for source in 0..n_sources as u64 {
		println!("\nDoing source {}", source);

		let records = data.get(&source).unwrap();
		println!("Records len: {}", records.len());

		let p = source % partitions;

		// Build expected count;
		let mut expected_count = 0;
		let mut expected_addrs: Vec<u64> = Vec::new();
		//for item in records {
		//	let ts = item.ts;
		//	let addr = item.addr;
		//	let data = item.as_slice();
		//	//let ts = item.0;
		//	//let addr = item.1;
		//	//let data = item.2;
		//	let indexed_value =
		//		u64::from_be_bytes(data[..8].try_into().unwrap());

		//	let in_time = min_ts <= ts && ts <= max_ts;
		//	let in_value =
		//		min_value <= indexed_value && indexed_value <= max_value;
		//	if in_time && in_value {
		//		expected_count += 1;
		//		expected_addrs.push(addr);
		//	}
		//}

		let expected_addrs: Vec<u64> =
			expected_addrs.iter().copied().rev().collect();
		println!("Total records: {}", records.len());
		println!("Expected addresses len: {}", expected_addrs.len());

		{
			println!("Doing u64 index scan");
			// Indexed access
			let index_id_u64 = *index_id_map.get(&(source, p)).unwrap();
			println!("Source {}. part: {}, index_id: {}", source, p, index_id_u64);

			let mut ctx = reader.indexed_query_ctx(&[(source, p)]).unwrap();

			let result = indexed_range_scan_u64(
				source,
				p,
				min_ts,
				max_ts,
				index_id_u64,
				min_value,
				max_value,
				&mut ctx,
			);

			println!("Result: {}", result.len());
			//assert_eq!(result.len(), expected_count);
			//for (e, r) in expected_addrs.iter().zip(result.iter()) {
			//	if *e != r.header.address {
			//		println!("Addresses don't line up");
			//		assert_eq!(*e, r.header.address);
			//	}
			//}
			ctx.print_read_counters();
		}
	}
}


fn index_scan_test(
	loaded_data: LoadedData,
	lookback_end_us: u64,
	lookback_start_us: u64,
	min_value: u64,
	max_value: u64,
) {

	let reader = loaded_data.mach.reader();

	let reader_timestamp = reader.now_micros();
	println!("TImestamp now: {}", reader_timestamp);
	let min_ts = reader_timestamp - lookback_end_us;
	let max_ts = reader_timestamp - lookback_start_us;
	println!("min ts: {}, max ts: {}", min_ts, max_ts);

	println!("Reading and verifying");
	let partitions = constants::PARTITIONS as u64;
	for source in 0..partitions as u64 {
		println!("\nDoing source {}", source);

		let records = loaded_data.written_data.get(&source).unwrap();

		let p = source % partitions;

		// Build expected count;
		let mut expected_count = 0;
		let mut expected_addrs: Vec<u64> = Vec::new();
		for rec in records {

			let ts = rec.ts;
			let addr = rec.addr;
			let data = rec.as_slice();

			let indexed_value =
				u64::from_be_bytes(data[..8].try_into().unwrap());

			let in_time = min_ts <= ts && ts <= max_ts;
			let in_value =
				min_value <= indexed_value && indexed_value <= max_value;
			if in_time && in_value {
				expected_count += 1;
				expected_addrs.push(addr);
			}
		}

		let expected_addrs: Vec<u64> =
			expected_addrs.iter().copied().rev().collect();
		println!("Total records: {}", records.len());
		println!("Expected addresses len: {}", expected_addrs.len());

		println!("Doing u64 index scan");
		// Indexed access
		let index_id_u64 = *loaded_data.index_ids.get(&(source, p)).unwrap();
			println!("Source {}. part: {}, index_id: {}", source, p, index_id_u64);

		let mut ctx = reader.indexed_query_ctx(&[(source, p)]).unwrap();

		let result = indexed_range_scan_u64(
			source,
			p,
			min_ts,
			max_ts,
			index_id_u64,
			min_value,
			max_value,
			&mut ctx,
			);

		assert_eq!(result.len(), expected_count);
		for (e, r) in expected_addrs.iter().zip(result.iter()) {
			if *e != r.header.address {
				println!("Addresses don't line up");
				assert_eq!(*e, r.header.address);
			}
		}
		ctx.print_read_counters();
	}
}

fn index_aggregation_test(
	loaded_data: LoadedData,
	lookback_end_ts: u64,
	lookback_start_ts: u64,
) {

	let reader = loaded_data.mach.reader();
	let data = &loaded_data.written_data;
	let index_id_map = &loaded_data.index_ids;

	let reader_timestamp = reader.now_micros();
	println!("TImestamp now: {}", reader_timestamp);
	let min_ts = reader_timestamp - lookback_end_ts;
	let max_ts = reader_timestamp - lookback_start_ts;

	println!("Reading and verifying");
	for source in 0..constants::PARTITIONS as u64 {
		println!("\nDoing source {}", source);

		let records = data.get(&source).unwrap();

		let p = source % constants::PARTITIONS as u64;

		// Build expected count;
		let mut expected_count = 0u64;
		let mut expected_sum = 0;
		for item in records {
			let ts = item.ts;
			let data = item.as_slice();
			let indexed_value = index_u64_func(data);
			let in_time = min_ts <= ts && ts <= max_ts;
			if in_time {
				expected_count += 1;
				expected_sum += indexed_value;
			}
		}

		println!("Expected count: {} sum: {}", expected_count, expected_sum);

		// Indexed access
		let index_id_u64 = *index_id_map.get(&(source, p)).unwrap();

		let mut ctx = reader.indexed_query_ctx(&[(source, p)]).unwrap();

		let result = indexed_aggregation_u64(
			source,
			p,
			min_ts,
			max_ts,
			index_id_u64,
			&mut ctx,
			);

		assert_eq!(result.count, expected_count);
		assert_eq!(result.sum, expected_sum);
		ctx.print_read_counters();
	}
}

fn multi_index_aggregation_test(
	loaded_data: LoadedData,
	lookback_end_us: u64,
	lookback_start_us: u64,
) {

	let reader = loaded_data.mach.reader();
	let data = &loaded_data.written_data;
	let index_id_map = &loaded_data.index_ids;

	let reader_timestamp = reader.now_micros();
	println!("TImestamp now: {}", reader_timestamp);
	let min_ts = reader_timestamp - lookback_end_us;
	let max_ts = reader_timestamp - lookback_start_us;

	println!("Reading and verifying");
	let mut spi: Vec<(u64, u64, u64)> = Vec::new();
	let mut sp: Vec<(u64, u64)> = Vec::new();

	for s in 0..4 {
		let p = s % constants::PARTITIONS as u64;
		let i = *index_id_map.get(&(s, p)).unwrap();
		sp.push((s, p));
		spi.push((s, p, i));
	}

	let mut records = Vec::new();
	let mut sum = 0;
	let mut count = 0;
	for (s, _, _) in spi.iter() {
		let source_records = data.get(s).unwrap();
		for item in source_records {
			let ts = item.ts;
			let data = item.as_slice();

			let indexed_value = index_u64_func(data);
			let in_time = min_ts <= ts && ts <= max_ts;
			if in_time {
				records.push(indexed_value);
				sum += indexed_value;
				count += 1;
			}
		}
	}

	records.sort();
	println!("Expected Count: {}, Sum: {}", count, sum);

	let mut reader = reader.indexed_query_ctx(&sp).unwrap();

	let now = Instant::now();
	let result = multi_indexed_aggregation_u64(
		&spi,
		min_ts,
		max_ts,
		&mut reader,
	);
	println!("Aggregation {:?}, duration: {:?}", result, now.elapsed());
}

fn multi_index_percentile_test(
	loaded_data: LoadedData,
	lookback_end_us: u64,
	lookback_start_us: u64,
	percentile: f64,
) {
	let reader = loaded_data.mach.reader();
	let data = &loaded_data.written_data;
	let index_id_map = &loaded_data.index_ids;

	let reader_timestamp = reader.now_micros();
	println!("TImestamp now: {}", reader_timestamp);
	let min_ts = reader_timestamp - lookback_end_us;
	let max_ts = reader_timestamp - lookback_start_us;

	println!("Reading and verifying");
	let mut spi: Vec<(u64, u64, u64)> = Vec::new();
	let mut sp: Vec<(u64, u64)> = Vec::new();
	for s in 0..4 {
		let p = s % constants::PARTITIONS as u64;
		let i = *index_id_map.get(&(s, p)).unwrap();
		sp.push((s, p));
		spi.push((s, p, i));
	}

	let mut records = Vec::new();
	let mut sum = 0;
	let mut count = 0;
	for (s, _, _) in spi.iter() {
		let source_records = data.get(s).unwrap();
		for item in source_records {
			let ts = item.ts;
			let data = item.as_slice();

			let indexed_value = index_u64_func(data);
			let in_time = min_ts <= ts && ts <= max_ts;
			if in_time {
				records.push(indexed_value);
				sum += indexed_value;
				count += 1;
			}
		}
	}

	records.sort();
	let idx = (records.len() as f64 * percentile).floor() as usize;
	let tile_value = records[idx];
	println!("Expected Count: {}, Sum: {}, Tile: {}", count, sum, tile_value);

	let mut reader = reader.indexed_query_ctx(&sp).unwrap();

	let now = Instant::now();
	let result = multi_indexed_percentile_u64(
		&spi,
		min_ts,
		max_ts,
		percentile,
		&mut reader,
	);
	println!("Percentile duration: {:?}", now.elapsed());
	assert_eq!(result, tile_value);
	println!("Tile result: {}", result);
}



fn index_percentile_test(
	loaded_data: LoadedData,
	lookback_end_us: u64,
	lookback_start_us: u64,
	percentile: f64,
) {
	let reader = loaded_data.mach.reader();
	let data = &loaded_data.written_data;
	let index_id_map = &loaded_data.index_ids;

	let reader_timestamp = reader.now_micros();
	println!("TImestamp now: {}", reader_timestamp);
	let min_ts = reader_timestamp - lookback_end_us;
	let max_ts = reader_timestamp - lookback_start_us;

	println!("Reading and verifying");
	for source in 0..constants::PARTITIONS as u64 {
		println!("\nDoing source {}", source);

		let records = data.get(&source).unwrap();

		let p = source % constants::PARTITIONS as u64;

		// Build expected count;
		let mut expected_values = Vec::new();
		for item in records {
			let ts = item.ts;
			let data = item.as_slice();
			let indexed_value = index_u64_func(data);
			let in_time = min_ts <= ts && ts <= max_ts;
			if in_time {
				expected_values.push(indexed_value);
			}
		}
		expected_values.sort();
		println!("Expected len: {}", expected_values.len());

		// And then get the percentile value
		let idx = (expected_values.len() as f64 * percentile).floor() as usize;
		println!("Index: {}", idx);
		let expected_percentile_value = expected_values[idx];

		// Indexed access
		let index_id_u64 = *index_id_map.get(&(source, p)).unwrap();

		let mut ctx = reader.indexed_query_ctx(&[(source, p)]).unwrap();

		let now = Instant::now();
		let result = indexed_percentile_u64(
			source,
			p,
			min_ts,
			max_ts,
			index_id_u64,
			percentile,
			&mut ctx,
			);

		println!("Indexed percentile duration: {:?}", now.elapsed());
		assert_eq!(result, expected_percentile_value);
		ctx.print_read_counters();
	}
}

#[allow(unused)]
enum TestType {
	IndexedIngestTest,
	UnIndexedIngestTest,
	RawScanTest,
	IndexScanTest,
	IndexAggregationTest,
	IndexPercentileTest,
	MultiIndexAggregationTest,
	MultiIndexPercentileTest,
}

fn main() {
	let n_sources = 20;
	let n_records_per_source = 5_000_000;

	//let test = TestType::IndexedIngestTest;
	//let test = TestType::UnIndexedIngestTest;
	//let test = TestType::IndexScanTest;
	//let test = TestType::IndexAggregationTest;
	//let test = TestType::IndexPercentileTest;
	let test = TestType::MultiIndexAggregationTest;

	let index_min: u64 = 0;
	let index_max: u64 = 10_000;
	let index_bins: u64 = 10;
	//let mach_path = "/home/fsolleza/ramdisk/can_delete/tmp_mach_index_filter";
	let mach_path = "/nvme/data/tmp/can_delete/mach-lib-bench";
	match remove_dir_all(mach_path) {
		_ => {}
	}

	let lookback_end_us = 20 * 1_000_000;
	let lookback_start_us = 10 * 1_000_000;
	let percentile = 0.999;
	let min_value = 2100;
	let max_value = 3500;


	match test {
		TestType::UnIndexedIngestTest => {
			let spec = LoadDataSpecification {
				path: mach_path,
				n_sources,
				n_records_per_source,
				index_min,     // ignored
				index_max,     // ignored
				index_bins,    // ignored
				verify: false, // ignored
			};
			let _ = spec.load_unindexed();
		}

		TestType::IndexedIngestTest => {
			let spec = LoadDataSpecification {
				path: mach_path,
				n_sources,
				n_records_per_source,
				index_min,
				index_max,
				index_bins,
				verify: false,
			};
			let _ = spec.load_indexed();
		}

		TestType::RawScanTest => {
			//raw_scan_test(
			//	mach_path,
			//	n_sources,
			//	n_records_per_source,
			//	verify,
			//	index_min,
			//	index_max,
			//	index_bins
			//);
		}

		TestType::IndexScanTest => {
			let spec = LoadDataSpecification {
				path: mach_path,
				n_sources,
				n_records_per_source,
				index_min,
				index_max,
				index_bins,
				verify: true,
			};
			let loaded_data = spec.load_indexed().unwrap();
			index_scan_test(
				loaded_data,
				lookback_end_us,
				lookback_start_us,
				min_value,
				max_value,
			);
		}

		TestType::IndexAggregationTest => {
			let spec = LoadDataSpecification {
				path: mach_path,
				n_sources,
				n_records_per_source,
				index_min,
				index_max,
				index_bins,
				verify: true,
			};
			let loaded_data = spec.load_indexed().unwrap();
			index_aggregation_test(
				loaded_data,
				lookback_end_us,
				lookback_start_us,
			);
		}

		TestType::IndexPercentileTest => {
			let spec = LoadDataSpecification {
				path: mach_path,
				n_sources,
				n_records_per_source,
				index_min,
				index_max,
				index_bins,
				verify: true,
			};
			let loaded_data = spec.load_indexed().unwrap();

			index_percentile_test(
				loaded_data,
				lookback_end_us,
				lookback_start_us,
				percentile,
			);
		}

		TestType::MultiIndexAggregationTest => {
			let spec = LoadDataSpecification {
				path: mach_path,
				n_sources,
				n_records_per_source,
				index_min,
				index_max,
				index_bins,
				verify: true,
			};
			let loaded_data = spec.load_indexed().unwrap();
			multi_index_aggregation_test(
				loaded_data,
				lookback_end_us,
				lookback_start_us,
			);
		}

		TestType::MultiIndexPercentileTest => {
			let spec = LoadDataSpecification {
				path: mach_path,
				n_sources,
				n_records_per_source,
				index_min,
				index_max,
				index_bins,
				verify: true,
			};
			let loaded_data = spec.load_indexed().unwrap();
			multi_index_percentile_test(
				loaded_data,
				lookback_end_us,
				lookback_start_us,
				percentile,
			);
		}

	}
	remove_dir_all(mach_path).unwrap();
}
