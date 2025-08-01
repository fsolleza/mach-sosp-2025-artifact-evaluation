use clap::{Parser, ValueEnum};
use crossbeam::channel::{bounded, Receiver, Sender};
use data::{
	kv_op::KVOp,
	record::{Record, SourceInformation},
	record_bytes::{RecordBatchBytes, RecordBytes},
};
use mach_lib::{
	self, indexed_aggregation_u64, indexed_percentile_u64,
	indexed_range_scan_u64, range_index_only_scan_u64, raw_scan_u64,
	time_index_only_scan_u64, Partitions, PartitionsReader, QueryContext,
};
use std::{
	fs::File,
	io::prelude::*,
	net::{TcpListener, TcpStream},
	sync::atomic::{AtomicBool, AtomicU64, Ordering::SeqCst},
	thread,
};
use utils::{u64_from_bytes, ConcurrentMap, Duration, HashSet, Instant};

static RECEIVED: AtomicU64 = AtomicU64::new(0);
static DROPPED: AtomicU64 = AtomicU64::new(0);
static STOP_INGESTING: AtomicBool = AtomicBool::new(false);

static ABLATION_RANGE: (u64, u64) = (80, u64::MAX);

fn counters() {
	let mut total_received = 0;
	let mut total_dropped = 0;

	loop {
		let r = RECEIVED.swap(0, SeqCst);
		let d = DROPPED.swap(0, SeqCst);
		total_received += r;
		total_dropped += d;
		println!(
			"Records received: {r}, dropped: {d}, total received \
			 {total_received} dropped {total_dropped}"
		);
		thread::sleep(Duration::from_secs(10));
	}
}

fn handler(_listener: TcpListener, mut stream: TcpStream, tx: Sender<Vec<u8>>) {
	// signal to client that this connection am ready

	stream.write_all(&1234u64.to_be_bytes()).unwrap();

	loop {
		// Read header
		let mut counts = [0u8; 40];
		if stream.read_exact(&mut counts[..]).is_err() {
			println!("Failed to read from stream, breaking");
			break;
		}

		let kv_log = u64_from_bytes(&counts[..8]);
		let syscall = u64_from_bytes(&counts[8..16]);
		let pagecache = u64_from_bytes(&counts[16..24]);
		let packet = u64_from_bytes(&counts[24..32]);
		let msg_sz = u64_from_bytes(&counts[32..40]);

		// Read payload
		let mut v = vec![0u8; msg_sz as usize];
		if stream.read_exact(&mut v[..]).is_err() {
			println!("Failed to read from stream, breaking");
			break;
		}

		let l = kv_log + syscall + pagecache + packet;

		if !STOP_INGESTING.load(SeqCst) {
			RECEIVED.fetch_add(l, SeqCst);
			if tx.try_send(v).is_err() {
				DROPPED.fetch_add(l, SeqCst);
			}
		}
	}
}

fn listener(addr: &str, tx: Sender<Vec<u8>>) {
	let listener = TcpListener::bind(addr).unwrap();
	let stream = listener.incoming().next().unwrap().unwrap();
	handler(listener, stream, tx);
	println!("Exiting handler thread");
}

#[inline(always)]
fn kv_indexer(bytes: &[u8]) -> u64 {
	let r = RecordBytes { bytes };
	r.kv_log_duration_us().unwrap()
}

#[inline(always)]
fn syscall_indexer(bytes: &[u8]) -> u64 {
	let r = RecordBytes { bytes };
	r.syscall_event_duration_us().unwrap()
}

#[inline(always)]
fn pagecache_indexer(_bytes: &[u8]) -> u64 {
	1
}

#[inline(always)]
fn packet_indexer(_bytes: &[u8]) -> u64 {
	1
}

#[inline(always)]
fn register_source(
	m: &mut Partitions,
	s: u64,
	p: u64,
	r: RecordBytes,
	d: &ConcurrentMap<u64, Option<u64>>,
	q: Query,
) {
	let idx_id = match q {
		Query::rocksdb_p1 => register_source_rocksdb(m, s, p, r),
		Query::rocksdb_p2 => register_source_rocksdb(m, s, p, r),
		Query::rocksdb_p3 => register_source_rocksdb(m, s, p, r),
		Query::rocksdb_p3_probe_effect => register_source_rocksdb(m, s, p, r),
		Query::correlate => register_source_rocksdb(m, s, p, r),
		Query::valkey_p1 => register_source_valkey(m, s, p, r),
		Query::valkey_p2 => register_source_valkey(m, s, p, r),
		Query::valkey_p3 => register_source_valkey(m, s, p, r),
		Query::ablation_noindex => register_source_rocksdb(m, s, p, r),
		Query::ablation_onlytime => register_source_rocksdb(m, s, p, r),
		Query::ablation_onlyrange => register_source_rocksdb(m, s, p, r),
		Query::ablation_timerange => register_source_rocksdb(m, s, p, r),
	};
	d.insert(s, idx_id);
}

fn register_source_valkey(
	mach: &mut Partitions,
	s: u64,
	p: u64,
	record_bytes: RecordBytes,
) -> Option<u64> {
	let (record, sz) = Record::from_bytes(record_bytes.bytes);
	assert_eq!(sz, record_bytes.bytes.len());
	assert_eq!(record.source_id(), record.source_information().as_id());

	mach.define_source(s, p);

	if let Some(op) = record_bytes.kv_log_op() {
		let idx_id = match op {
			KVOp::Read => {
				Some(mach.define_range_index_u64(s, p, kv_indexer, 0, 40, 10))
			}
			KVOp::Write => None,
		};
		return idx_id;
	}

	if let Some(num) = record_bytes.syscall_event_syscall_number() {
		let idx_id = match num {
			44 => Some(mach.define_range_index_u64(
				s,
				p,
				syscall_indexer,
				0,
				10,
				10,
			)),
			_ => None,
		};
		return idx_id;
	}

	if record_bytes.is_packet_capture() {
		let idx_id = mach.define_range_index_u64(s, p, packet_indexer, 0, 5, 1);
		return Some(idx_id);
	}

	None
}

fn register_source_rocksdb(
	mach: &mut Partitions,
	s: u64,
	p: u64,
	record_bytes: RecordBytes,
) -> Option<u64> {
	let (record, sz) = Record::from_bytes(record_bytes.bytes);
	assert_eq!(sz, record_bytes.bytes.len());
	assert_eq!(record.source_id(), record.source_information().as_id());

	mach.define_source(s, p);

	if let Some(op) = record_bytes.kv_log_op() {
		let idx_id = match op {
			KVOp::Read => {
				mach.define_range_index_u64(s, p, kv_indexer, 0, 10, 10)
			}
			_ => return None,
		};
		return Some(idx_id);
	}

	if let Some(num) = record_bytes.syscall_event_syscall_number() {
		let idx_id = match num {
			17 => Some(mach.define_range_index_u64(
				s,
				p,
				syscall_indexer,
				0,
				10,
				10,
			)),
			_ => None,
		};
		return idx_id;
	}

	if record_bytes.is_page_cache_event() {
		let idx_id =
			mach.define_range_index_u64(s, p, pagecache_indexer, 0, 5, 1);
		return Some(idx_id);
	}

	None
}

#[inline(always)]
fn ingest_record(
	m: &mut Partitions,
	b: RecordBytes,
	r: &mut HashSet<u64>,
	s: &ConcurrentMap<u64, Option<u64>>,
	q: Query,
) {
	let source_id = b.source_id();
	let partition = 0;
	if r.insert(source_id) {
		register_source(m, source_id, partition, b, s, q);
	}
	m.push(source_id, partition, b.bytes);
}

#[inline(always)]
fn ingest_batch_bytes(
	m: &mut Partitions,
	b: RecordBatchBytes,
	r: &mut HashSet<u64>,
	s: &ConcurrentMap<u64, Option<u64>>,
	q: Query,
) {
	let mut iterator = b.iterator();
	while let Some(b) = iterator.next_record_bytes() {
		ingest_record(m, b, r, s, q);
	}
}

fn ingest(
	mut mach: Partitions,
	rx: Receiver<Vec<u8>>,
	source_idx: ConcurrentMap<u64, Option<u64>>,
	query: Query,
) {
	let mut registered_sources = HashSet::new();
	while let Ok(item) = rx.recv() {
		let batch_bytes = RecordBatchBytes::new(item.as_slice());
		ingest_batch_bytes(
			&mut mach,
			batch_bytes,
			&mut registered_sources,
			&source_idx,
			query,
		);
	}
	println!("Exiting ingest thread");
}

fn valkey_p3_correlate(
	reader: &PartitionsReader,
	out_path: &str,
	source_idx: &ConcurrentMap<u64, Option<u64>>,
) {
	let p = 0;

	let kv_id = SourceInformation::KVLog { op: KVOp::Read }.as_id();
	let pcap_id = SourceInformation::PacketCapture.as_id();
	let reader_timestamp = reader.now_micros();

	// lookback
	let min_ts = reader_timestamp - 130 * 1_000_000;
	let max_ts = reader_timestamp - 10 * 1_000_000;
	let ctxs = [(kv_id, p), (pcap_id, p)];
	let mut ctx = reader.indexed_query_ctx(&ctxs).unwrap();

	let slow_reads = {
		let source_id = kv_id;
		println!("Getting p9999 read latency");
		let idx_id = source_idx.get(&source_id).unwrap().unwrap();
		let agg = indexed_percentile_u64(
			source_id, p, min_ts, max_ts, idx_id, 0.9999, &mut ctx,
		);

		let mut results = Vec::new();
		indexed_range_scan_u64(
			source_id,
			p,
			min_ts,
			max_ts,
			idx_id,
			agg,
			u64::MAX,
			|ts, x, _| {
				results.push((ts, x));
			},
			&mut ctx,
		);
		results
	};

	let packet_capture = {
		println!("Getting packet capture");
		let source_id = pcap_id;

		let mut results = Vec::new();
		raw_scan_u64(
			source_id,
			p,
			min_ts,
			max_ts,
			|bytes: &[u8]| {
				let bytes = RecordBytes { bytes };
				let pcap = bytes.as_packet_capture().unwrap();
				pcap.dst_port
			},
			0,
			u64::MAX,
			|ts, x, _| {
				if x == 5555 {
					println!("HERE");
					results.push((ts, 1));
				}
			},
			&mut ctx,
		);
		results
	};

	println!("Prepping output\n");
	let mut output = String::from("source,ts,value\n");
	for (ts, x) in slow_reads {
		output.push_str(&format!("kv,{},{}\n", ts, x));
	}
	for (ts, x) in packet_capture {
		output.push_str(&format!("packet_capture,{},{}\n", ts, x));
	}

	let mut f = File::create(out_path).unwrap();
	f.write_all(output.as_bytes()).unwrap();
	f.sync_all().unwrap();
}

fn rocksdb_p1_query(
	reader: &PartitionsReader,
	source_idx: &ConcurrentMap<u64, Option<u64>>,
) {
	let source_id = SourceInformation::KVLog { op: KVOp::Read }.as_id();
	let partition = 0;
	let idx_id = source_idx.get(&source_id).unwrap().unwrap();

	let reader_timestamp = reader.now_micros();

	// lookback
	let min_ts = reader_timestamp - 80 * 1_000_000;
	let max_ts = reader_timestamp - 20 * 1_000_000;
	let mut ctx = reader.indexed_query_ctx(&[(source_id, partition)]).unwrap();

	let s = Instant::now();
	let agg = indexed_aggregation_u64(
		source_id, partition, min_ts, max_ts, idx_id, &mut ctx,
	);
	let dur1 = s.elapsed();
	println!("Max Result: {:?}", agg);

	let s = Instant::now();
	let agg = indexed_percentile_u64(
		source_id, partition, min_ts, max_ts, idx_id, 0.9999, &mut ctx,
	);
	let dur2 = s.elapsed();
	println!("Percentile Result: {:?}", agg);
	println!("Query Latency (Application Max Latency): {:?}", dur1);
	println!("Query Latency (Application Tail Latency): {:?}", dur2);
}

fn rocksdb_p2_query(
	reader: &PartitionsReader,
	source_idx: &ConcurrentMap<u64, Option<u64>>,
) {
	let source_id =
		SourceInformation::SyscallEvent { syscall_number: 17 }.as_id();
	let partition = 0;
	let idx_id = source_idx.get(&source_id).unwrap().unwrap();

	let reader_timestamp = reader.now_micros();

	// lookback
	let min_ts = reader_timestamp - 80 * 1_000_000;
	let max_ts = reader_timestamp - 20 * 1_000_000;
	let mut ctx = reader.indexed_query_ctx(&[(source_id, partition)]).unwrap();

	let s = Instant::now();
	let agg = indexed_aggregation_u64(
		source_id, partition, min_ts, max_ts, idx_id, &mut ctx,
	);
	let dur1 = s.elapsed();
	println!("Q2 Max {:?}", s.elapsed());
	println!("Max Result: {:?}", agg);

	let s = Instant::now();
	let agg = indexed_percentile_u64(
		source_id, partition, min_ts, max_ts, idx_id, 0.9999, &mut ctx,
	);
	let dur2 = s.elapsed();
	println!("Percentile Result: {:?}", agg);
	println!("Query Latency (pread64 Max Latency): {:?}", dur1);
	println!("Query Latency (pread64 Tail Latency): {:?}", dur2);
}

fn rocksdb_p3_query(
	reader: &PartitionsReader,
	source_idx: &ConcurrentMap<u64, Option<u64>>,
) {
	let page_cache_id = SourceInformation::PageCacheEvent.as_id();
	let page_cache_idx = source_idx.get(&page_cache_id).unwrap().unwrap();
	let partition = 0;

	let reader_timestamp = reader.now_micros();

	// lookback
	let min_ts = reader_timestamp - 80 * 1_000_000;
	let max_ts = reader_timestamp - 20 * 1_000_000;
	let mut ctx = reader
		.indexed_query_ctx(&[(page_cache_id, partition)])
		.unwrap();

	let s = Instant::now();
	let pg_agg = indexed_aggregation_u64(
		page_cache_id,
		partition,
		min_ts,
		max_ts,
		page_cache_idx,
		&mut ctx,
	);
	let dur = s.elapsed();
	println!("Page Cache Event Count Query results {:?}", pg_agg);
	println!("Query Latency (Page Cache Event Count): {:?}", dur);
}

fn valkey_p1_query(
	reader: &PartitionsReader,
	source_idx: &ConcurrentMap<u64, Option<u64>>,
) {
	let read_id = SourceInformation::KVLog { op: KVOp::Read }.as_id();
	let write_id = SourceInformation::KVLog { op: KVOp::Write }.as_id();
	let mut ctx = reader
		.indexed_query_ctx(&[(read_id, 0), (write_id, 0)])
		.unwrap();
	let p = 0;

	// lookback
	let reader_timestamp = reader.now_micros();
	let min_ts = reader_timestamp - 80 * 1_000_000;
	let max_ts = reader_timestamp - 20 * 1_000_000;

	println!("Valkey P1 Query");
	{
		let idx_id = source_idx.get(&read_id).unwrap().unwrap();

		let now = Instant::now();
		let agg = indexed_percentile_u64(
			read_id, 0, min_ts, max_ts, idx_id, 0.9999, &mut ctx,
		);
		let mut count = 0;
		indexed_range_scan_u64(
			read_id,
			p,
			min_ts,
			max_ts,
			idx_id,
			agg,
			u64::MAX,
			|_ts, _x, _| {
				count += 1;
			},
			&mut ctx,
		);
		let d = now.elapsed();
		println!("Count: {}", count);
		println!("Query Latency (Slow Requests): {:?}", d);
	}
}

fn valkey_p2_query(
	reader: &PartitionsReader,
	source_idx: &ConcurrentMap<u64, Option<u64>>,
) {
	let kv_read_id = SourceInformation::KVLog { op: KVOp::Read }.as_id();
	let syscall_send_id =
		SourceInformation::SyscallEvent { syscall_number: 44 }.as_id();
	let mut ctx = reader
		.indexed_query_ctx(&[(kv_read_id, 0), (syscall_send_id, 0)])
		.unwrap();
	let p = 0;

	// lookback
	let reader_timestamp = reader.now_micros();
	let min_ts = reader_timestamp - 80 * 1_000_000;
	let max_ts = reader_timestamp - 20 * 1_000_000;

	println!("Valkey P2 Query");
	let dur1 = {
		let idx_id = source_idx.get(&kv_read_id).unwrap().unwrap();

		let now = Instant::now();
		let agg = indexed_percentile_u64(
			kv_read_id, 0, min_ts, max_ts, idx_id, 0.9999, &mut ctx,
		);
		let mut count = 0;
		indexed_range_scan_u64(
			kv_read_id,
			p,
			min_ts,
			max_ts,
			idx_id,
			agg,
			u64::MAX,
			|_, _, _| {
				count += 1;
			},
			&mut ctx,
		);
		now.elapsed()
	};

	let dur2 = {
		let idx_id = source_idx.get(&syscall_send_id).unwrap().unwrap();

		let now = Instant::now();
		let agg = indexed_percentile_u64(
			syscall_send_id,
			0,
			min_ts,
			max_ts,
			idx_id,
			0.9999,
			&mut ctx,
		);
		let mut count = 0;
		indexed_range_scan_u64(
			syscall_send_id,
			p,
			min_ts,
			max_ts,
			idx_id,
			agg,
			u64::MAX,
			|_, _, _| {
				count += 1;
			},
			&mut ctx,
		);
		now.elapsed()
	};

	println!("Query Latency Slow Requests: {:?}", dur1);
	println!("Query Latency Slow sendto Execution: {:?}", dur2);
}

fn valkey_p3_query(
	reader: &PartitionsReader,
	source_idx: &ConcurrentMap<u64, Option<u64>>,
) {
	let kv_read_id = SourceInformation::KVLog { op: KVOp::Read }.as_id();
	let packet_id = SourceInformation::PacketCapture.as_id();
	let mut ctx = reader
		.indexed_query_ctx(&[(kv_read_id, 0), (packet_id, 0)])
		.unwrap();
	let p = 0;

	// lookback
	let reader_timestamp = reader.now_micros();
	let min_ts = reader_timestamp - 80 * 1_000_000;
	let max_ts = reader_timestamp - 20 * 1_000_000;

	println!("Valkey P3 Query");
	let (dur1, target_ts) = {
		let idx_id = source_idx.get(&kv_read_id).unwrap().unwrap();

		let now = Instant::now();
		let agg = indexed_aggregation_u64(
			kv_read_id, 0, min_ts, max_ts, idx_id, &mut ctx,
		);
		let mut count = 0;
		let mut target_ts = 0;
		indexed_range_scan_u64(
			kv_read_id,
			p,
			min_ts,
			max_ts,
			idx_id,
			agg.max,
			u64::MAX,
			|ts, _, _| {
				count += 1;
				target_ts = ts;
			},
			&mut ctx,
		);
		let dur1 = now.elapsed();
		println!("Count: {}, ts: {}", count, target_ts);
		(dur1, target_ts)
	};

	let dur2 = {
		let low_ts = target_ts - 5000000;
		let high_ts = target_ts + 5000000;
		let idx_id = source_idx.get(&packet_id).unwrap().unwrap();
		let now = Instant::now();
		let mut count = 0;
		indexed_range_scan_u64(
			packet_id,
			p,
			low_ts,
			high_ts,
			idx_id,
			0,
			u64::MAX,
			|_, _, _| {
				count += 1;
			},
			&mut ctx,
		);
		let d = now.elapsed();
		println!("Count: {}", count);
		d
	};

	println!("Query Latency (Maximum Latency Request): {:?}", dur1);
	println!("Query Latency (TCP Packet scan): {:?}", dur2);
}

fn ablation_noindex_query(
	ctx: &mut QueryContext,
	source_id: u64,
	partition: u64,
	min_ts: u64,
	max_ts: u64,
	_idx_id: u64,
) {
	let mut min_ts_found = u64::MAX;
	let mut max_ts_found = 0;
	let mut count = 0;
	println!("Executing ablation no index");
	let now = Instant::now();
	raw_scan_u64(
		source_id,
		partition,
		min_ts,
		max_ts,
		kv_indexer,
		ABLATION_RANGE.0,
		ABLATION_RANGE.1,
		|ts, _, _| {
			count += 1;
			min_ts_found = min_ts_found.min(ts);
			max_ts_found = max_ts_found.max(ts);
		},
		ctx,
	);
	println!("Done executing in {:?}, {count}", now.elapsed());
	println!("Min ts found: {min_ts_found} Max ts fonud: {max_ts_found}");

	let chunks_read = ctx.persistent_chunks_read();
	println!("Time TSL chunks read: {}", chunks_read.0);
	println!("Index TSL chunks read: {}", chunks_read.1);
	println!("Data TSLs chunks read: {}", chunks_read.2);
}

fn ablation_onlytime_query(
	ctx: &mut QueryContext,
	source_id: u64,
	partition: u64,
	min_ts: u64,
	max_ts: u64,
	_idx_id: u64,
) {
	let mut count = 0;

	println!("Executing ablation time only");
	let now = Instant::now();
	time_index_only_scan_u64(
		source_id,
		partition,
		min_ts,
		max_ts,
		kv_indexer,
		ABLATION_RANGE.0,
		ABLATION_RANGE.1,
		|_, _, _| {
			count += 1;
		},
		ctx,
	);
	println!("Done executing in {:?}, {count}", now.elapsed());
	let chunks_read = ctx.persistent_chunks_read();
	println!("Time TSL chunks read: {}", chunks_read.0);
	println!("Index TSL chunks read: {}", chunks_read.1);
	println!("Data TSLs chunks read: {}", chunks_read.2);
}

fn ablation_onlyrange_query(
	ctx: &mut QueryContext,
	source_id: u64,
	partition: u64,
	min_ts: u64,
	max_ts: u64,
	idx_id: u64,
) {
	let mut count = 0;
	println!("Executing ablation range only");
	let now = Instant::now();
	range_index_only_scan_u64(
		source_id,
		partition,
		min_ts,
		max_ts,
		idx_id,
		ABLATION_RANGE.0,
		ABLATION_RANGE.1,
		|_, _, _| {
			count += 1;
		},
		ctx,
	);
	println!("Done executing in {:?}, {count}", now.elapsed());
	let chunks_read = ctx.persistent_chunks_read();
	println!("Time TSL chunks read: {}", chunks_read.0);
	println!("Index TSL chunks read: {}", chunks_read.1);
	println!("Data TSLs chunks read: {}", chunks_read.2);
}

fn ablation_timerange_query(
	ctx: &mut QueryContext,
	source_id: u64,
	partition: u64,
	min_ts: u64,
	max_ts: u64,
	idx_id: u64,
) {
	let mut count = 0;
	println!("Executing ablation time range");
	let now = Instant::now();
	indexed_range_scan_u64(
		source_id,
		partition,
		min_ts,
		max_ts,
		idx_id,
		ABLATION_RANGE.0,
		ABLATION_RANGE.1,
		|_, _, _| {
			count += 1;
		},
		ctx,
	);
	println!("Done executing in {:?}, {count}", now.elapsed());
	let chunks_read = ctx.persistent_chunks_read();
	println!("Time TSL chunks read: {}", chunks_read.0);
	println!("Index TSL chunks read: {}", chunks_read.1);
	println!("Data TSLs chunks read: {}", chunks_read.2);
}

#[derive(Copy, Clone, Debug, ValueEnum, Eq, PartialEq)]
#[allow(non_camel_case_types)]
enum Query {
	rocksdb_p1,
	rocksdb_p2,
	rocksdb_p3,
	correlate,
	valkey_p1,
	valkey_p2,
	valkey_p3,
	rocksdb_p3_probe_effect,
	ablation_noindex,
	ablation_onlytime,
	ablation_onlyrange,
	ablation_timerange,
}

impl Query {
	fn is_ablation(&self) -> bool {
		match self {
			Self::ablation_noindex => true,
			Self::ablation_onlytime => true,
			Self::ablation_onlyrange => true,
			Self::ablation_timerange => true,
			_ => false,
		}
	}

	fn is_query_workload(&self) -> bool {
		match self {
			Self::rocksdb_p1 => true,
			Self::rocksdb_p2 => true,
			Self::rocksdb_p3 => true,
			Self::valkey_p1 => true,
			Self::valkey_p2 => true,
			Self::valkey_p3 => true,
			Self::correlate => true,
			_ => false,
		}
	}

	fn is_probe_effect(&self) -> bool {
		match self {
			Self::rocksdb_p3_probe_effect => true,
			_ => false,
		}
	}
}

fn run_query(
	q: Query,
	correlation_path: Option<&String>,
	reader: &PartitionsReader,
	source_idx: &ConcurrentMap<u64, Option<u64>>,
) {
	match q {
		Query::rocksdb_p1 => rocksdb_p1_query(reader, source_idx),
		Query::rocksdb_p2 => rocksdb_p2_query(reader, source_idx),
		Query::rocksdb_p3 => rocksdb_p3_query(reader, source_idx),
		Query::valkey_p1 => valkey_p1_query(reader, source_idx),
		Query::valkey_p2 => valkey_p2_query(reader, source_idx),
		Query::valkey_p3 => valkey_p3_query(reader, source_idx),
		Query::correlate => {
			let correlation_path = correlation_path.unwrap();
			valkey_p3_correlate(reader, &*correlation_path, source_idx);
		}
		_ => unimplemented!(),
	};
}

fn run_ablation(
	q: Query,
	reader: &PartitionsReader,
	source_idx: &ConcurrentMap<u64, Option<u64>>,
	lookback_s: u64,
) {
	let wait_time = Duration::from_secs(lookback_s + 5 + 120);
	thread::sleep(wait_time);
	STOP_INGESTING.store(true, SeqCst);

	let source_id = SourceInformation::KVLog { op: KVOp::Read }.as_id();
	let partition = 0;
	let idx_id = source_idx.get(&source_id).unwrap().unwrap();
	let reader_ts = reader.now_micros();
	// lookback
	let max_ts = reader_ts - (5 + lookback_s) * 1_000_000;
	let min_ts = max_ts - 120 * 1_000_000; // two minute lookback

	assert!(max_ts < reader_ts);
	println!("Reader ts: {}", reader_ts);
	println!("Max ts: {}", max_ts);
	println!("Min ts: {}", min_ts);

	let mut ctx = reader.indexed_query_ctx(&[(source_id, partition)]).unwrap();

	let now = Instant::now();
	match q {
		Query::ablation_noindex => ablation_noindex_query(
			&mut ctx, source_id, partition, min_ts, max_ts, idx_id,
		),
		Query::ablation_onlytime => ablation_onlytime_query(
			&mut ctx, source_id, partition, min_ts, max_ts, idx_id,
		),
		Query::ablation_onlyrange => ablation_onlyrange_query(
			&mut ctx, source_id, partition, min_ts, max_ts, idx_id,
		),
		Query::ablation_timerange => ablation_timerange_query(
			&mut ctx, source_id, partition, min_ts, max_ts, idx_id,
		),
		_ => unimplemented!(),
	}
	//run_query(q, None, reader, source_idx);
	let d = now.elapsed();
	println!("Duraton: {:?}", d);
}

fn run_queries(
	q: Query,
	correlation_path: Option<String>,
	reader: &PartitionsReader,
	source_idx: &ConcurrentMap<u64, Option<u64>>,
) {
	let correlation_path_ref = correlation_path.as_ref();
	if q == Query::correlate {
		thread::sleep(Duration::from_secs(150));
		run_query(q, correlation_path_ref, reader, source_idx);
	} else {
		for i in 0..5 {
			thread::sleep(Duration::from_secs(120));
			println!("Running query {}", i);
			run_query(q, correlation_path_ref, reader, source_idx);
		}
	}
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
	#[arg(short, long)]
	mach_path: String,

	#[arg(short, long)]
	addr: String,

	#[arg(short, long)]
	query: Query,

	#[arg(short, long)]
	correlation_path: Option<String>,

	#[arg(short, long)]
	ablation_lookback: Option<u64>,
}

fn main() {
	let args = Args::parse();
	let addr = args.addr.clone();
	let mach_path = args.mach_path.clone();
	println!("Mach path: {:?}", mach_path);
	let query = args.query;
	let correlation_path = args.correlation_path.clone();
	let ablation_lookback = args.ablation_lookback;
	match (query, &correlation_path) {
		(Query::correlate, None) => {
			panic!(
				"Correlation requested but no path given. Set correlation-path"
			);
		}
		_ => {}
	}

	if query.is_ablation() && ablation_lookback.is_none() {
		panic!("wait time should be set when query is ablation");
	}

	println!("Initializing Mach");
	let mach = Partitions::new(mach_path.into());
	let reader = mach.reader();
	let source_idx = ConcurrentMap::new();

	let queue_size = 1024;
	let (tx, rx) = bounded(queue_size);
	println!("Queue size: {} batches", queue_size);

	println!("Launched listener thread");
	let ingest_handle = thread::spawn(move || {
		//let id = core_affinity::CoreId { id: 10 };
		//core_affinity::set_for_current(id);
		listener(addr.as_str(), tx)
	});

	println!("Launched ingest thread");
	{
		let source_idx = source_idx.clone();
		thread::spawn(move || ingest(mach, rx, source_idx, query));
	}

	//println!("Warming up for 2 minutes");
	//thread::sleep(Duration::from_secs(120));
	//println!("Done with warm up");

	thread::spawn(move || {
		//let id = core_affinity::CoreId { id: 10 };
		//core_affinity::set_for_current(id);
		counters();
	});

	// Register indices but don't fire queries
	if query.is_probe_effect() {
	} else if query.is_ablation() {
		run_ablation(query, &reader, &source_idx, ablation_lookback.unwrap());
		return;
	} else if query.is_query_workload() {
		run_queries(query, correlation_path, &reader, &source_idx);
		return;
	} else {
		unreachable!()
	}

	println!("Blocking on handler thread");
	ingest_handle.join().unwrap();
	return;
}
