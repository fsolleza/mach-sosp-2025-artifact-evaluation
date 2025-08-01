use chrono::prelude::*;
use clap::{Parser, ValueEnum};
use crossbeam::channel::{bounded, unbounded, Receiver, Sender};
use data::record::RecordBatch;
use data::{kv_op::KVOp, record::SourceInformation};
use serde_json::Value;
use std::{
	fs::File,
	io::prelude::*,
	net::{TcpListener, TcpStream},
	sync::atomic::{AtomicBool, AtomicU64, Ordering::SeqCst},
	thread,
};
use utils::{micros_since_epoch, u64_from_bytes, Duration, HashMap, Instant};

static RECEIVED: AtomicU64 = AtomicU64::new(0);
static DROPPED: AtomicU64 = AtomicU64::new(0);
static STOP_INGEST: AtomicBool = AtomicBool::new(false);
static CURRENT_INGEST_TS: AtomicU64 = AtomicU64::new(0);

fn counters() {
	let mut last_received = 0;
	//let mut last_dropped = 0;
	let mut last_written = 0;
	loop {
		// current
		let cr = RECEIVED.load(SeqCst);
		let cd = DROPPED.load(SeqCst);
		let cw = cr - cd;

		// diff
		let r = cr - last_received;
		//let d = cd - last_dropped;
		let w = cw - last_written;

		// replace last values
		last_received = cr;
		//last_dropped = cd;
		last_written = cw;

		//let current_ts = CURRENT_INGEST_TS.load(SeqCst);

		if !STOP_INGEST.load(SeqCst) {
			println!("Records received: {r}, written: {w}, total received {cr} dropped {cd}");
		}
		thread::sleep(Duration::from_secs(10));
	}
}

//fn lp_transform(rx: Receiver<(u64, Vec<u8>)>, tx: Sender<String>) {
//
//	let micros_now = micros_since_epoch();
//	while let Ok((count, bytes)) = rx.recv() {
//		let (mut record_batch, sz) = RecordBatch::from_bytes(&bytes[..]);
//		assert_eq!(sz, bytes.len());
//
//		let now = micros_since_epoch();
//		record_batch.arrival_us = now;
//		let lp = record_batch.as_line_protocol();
//
//		// And then update the last and CURRENT_INGESTED_TIMESTAMP
//		CURRENT_INGEST_TS.swap(now, SeqCst);
//
//		RECEIVED.fetch_add(count, SeqCst);
//		if tx.try_send(lp).is_err() {
//			DROPPED.fetch_add(count, SeqCst);
//		}
//	}
//}

fn handler(_l: TcpListener, mut stream: TcpStream, tx: Sender<IngestData>) {
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

		if STOP_INGEST.load(SeqCst) {
			continue;
		}

		let now = micros_since_epoch();
		RECEIVED.fetch_add(l, SeqCst);
		let x = IngestData {
			len: l,
			arrival: now,
			bytes: v,
		};
		if tx.try_send(x).is_err() {
			DROPPED.fetch_add(l, SeqCst);
		}
	}
}

fn listener(addr: &str, tx: Sender<IngestData>) {
	let listener = TcpListener::bind(addr).unwrap();
	let stream = listener.incoming().next().unwrap().unwrap();
	handler(listener, stream, tx);
	println!("Exiting handler thread");
}

struct IngestData {
	len: u64,
	arrival: u64,
	bytes: Vec<u8>,
}

async fn ingest(receiver: Receiver<IngestData>) {
	let url = "http://localhost:8086/write?db=mydb";
	let client = reqwest::Client::new();
	while let Ok(x) = receiver.recv() {
		let IngestData {
			len,
			arrival,
			bytes,
		} = x;

		let (mut record_batch, sz) = RecordBatch::from_bytes(&bytes[..]);
		assert_eq!(sz, bytes.len());

		record_batch.arrival_us = arrival;
		let lp = record_batch.as_line_protocol();

		match client.post(url).body(lp).send().await {
			Ok(resp) => match resp.error_for_status() {
				Ok(_) => {}
				Err(_) => {
					println!("Can't get error status");
					DROPPED.fetch_add(len, SeqCst);
				}
			},
			Err(x) => {
				println!("Error in ingest {:?}", x);
				DROPPED.fetch_add(len, SeqCst);
			}
		}

		CURRENT_INGEST_TS.fetch_max(arrival, SeqCst);
	}
}

fn valkey_p3_correlation(min: String, max: String, out_path: &str) {
	let kv_id = SourceInformation::KVLog { op: KVOp::Read }.as_id();
	let _syscall_id =
		SourceInformation::SyscallEvent { syscall_number: 17 }.as_id();
	let _page_cache_id = SourceInformation::PageCacheEvent.as_id();

	// KV LOG
	let slow_reads = {
		println!("Querying KV Percentile");
		let q = format!(
			"SELECT PERCENTILE(duration_us, 99.99)
			FROM kvlog
			WHERE source = 'id-{}'
			AND time >= '{}'
			AND time <= '{}'",
			kv_id, min, max
		);

		let result = do_query(q);
		let resp: Value = serde_json::from_str(&result).unwrap();
		println!("{}", serde_json::to_string_pretty(&resp).unwrap());

		println!("Querying KV Records");
		let val = resp["results"][0]["series"][0]["values"][0][1]
			.as_i64()
			.unwrap();
		println!("{}", val);
		let q = format!(
			"SELECT time,duration_us
			FROM kvlog
			WHERE source = 'id-{}'
			AND duration_us > {}
			AND time >= '{}'
			AND time <= '{}'",
			kv_id, val, min, max
		);
		let result = do_query(q);

		println!("Processing KV Records");
		let resp: Value = serde_json::from_str(&result).unwrap();
		let arr = resp["results"][0]["series"][0]["values"]
			.as_array()
			.unwrap();
		let mut slow_reads: Vec<(String, i64)> = Vec::new();
		for v in arr.iter() {
			let ts = String::from(v[0].as_str().unwrap());
			let x = v[1].as_i64().unwrap();
			slow_reads.push((ts, x));
		}
		slow_reads
	};

	// Packets
	let packets = {
		println!("Querying packets");
		let source_id = SourceInformation::PacketCapture.as_id();
		let q = format!(
			"SELECT dst_port
			FROM packet
			WHERE source = 'id-{}'
			AND dst_port = 5555
			AND time >= '{}'
			AND time <= '{}'",
			source_id, min, max
		);
		let result = do_query(q);

		println!("Processing ");
		let resp: Value = serde_json::from_str(&result).unwrap();
		let arr = resp["results"][0]["series"][0]["values"].as_array();
		let mut results: Vec<(String, i64)> = Vec::new();
		if let Some(arr) = arr {
			for v in arr.iter() {
				let ts = String::from(v[0].as_str().unwrap());
				let x = v[1].as_i64().unwrap();
				results.push((ts, x));
			}
		} else {
			println!("No records captured");
		}
		results
	};
	println!("Prepping output");

	let mut string_out = String::from("source,ts,value\n");
	for (ts, v) in slow_reads {
		string_out.push_str(&format!("kv,{},{}\n", ts, v));
	}

	for (ts, v) in packets {
		string_out.push_str(&format!("packets,{},{}\n", ts, v));
	}

	println!("Writing to file");
	let mut f = File::create(out_path).unwrap();
	f.write_all(string_out.as_bytes()).unwrap();
	f.sync_all().unwrap();
	println!("Done writing to file");
}

fn rocksdb_p1_query(min: String, max: String) {
	let source_id = SourceInformation::KVLog { op: KVOp::Read }.as_id();

	let s = Instant::now();
	let q = format!(
		"SELECT COUNT(duration_us), MAX(duration_us)
		FROM kvlog
		WHERE source = 'id-{}'
		AND time >= '{}'
		AND time <= '{}'",
		source_id, min, max
	);

	let result = do_query(q);
	let _resp: Value = serde_json::from_str(&result).unwrap();
	let dur1 = s.elapsed();

	let s = Instant::now();
	let q = format!(
		"SELECT PERCENTILE(duration_us, 99.99)
		FROM kvlog
		WHERE source = 'id-{}'
		AND time >= '{}'
		AND time <= '{}'",
		source_id, min, max
	);

	let result = do_query(q);
	let _resp: Value = serde_json::from_str(&result).unwrap();
	let dur2 = s.elapsed();
	println!("Query Latency (Application Max Latency): {:?}", dur1);
	println!("Query Latency (Application Tail Latency): {:?}", dur2);
}

fn rocksdb_p2_query(min: String, max: String) {
	let source_id =
		SourceInformation::SyscallEvent { syscall_number: 17 }.as_id();

	let s = Instant::now();
	let q = format!(
		"SELECT COUNT(duration_us), MAX(duration_us)
		FROM syscall_event
		WHERE source = 'id-{}'
		AND time >= '{}'
		AND time <= '{}' ",
		source_id, min, max
	);

	let result = do_query(q);
	let _resp: Value = serde_json::from_str(&result).unwrap();
	let dur1 = s.elapsed();

	let s = Instant::now();
	let q = format!(
		"SELECT PERCENTILE(duration_us, 99.99)
		FROM syscall_event
		WHERE source = 'id-{}'
		AND time >= '{}'
		AND time <= '{}'",
		source_id, min, max
	);

	let result = do_query(q);
	let dur2 = s.elapsed();
	let _resp: Value = serde_json::from_str(&result).unwrap();
	println!("Query Latency (pread64 Max Latency): {:?}", dur1);
	println!("Query Latency (pread64 Tail Latency): {:?}", dur2);
}

fn rocksdb_p3_query(min: String, max: String) {
	let page_cache_id = SourceInformation::PageCacheEvent.as_id();
	let q = format!(
		"SELECT COUNT(pid)
		FROM page_cache
		WHERE source = 'id-{}'
		AND time >= '{}'
		AND time <= '{}'",
		page_cache_id, min, max
	);
	let s = Instant::now();
	let result = do_query(q);
	let dur1 = s.elapsed();
	let _resp: Value = serde_json::from_str(&result).unwrap();
	println!("Query Latency (Page Cache Event Count): {:?}", dur1);
}

fn valkey_p1_query(min: String, max: String) {
	let source_id = SourceInformation::KVLog { op: KVOp::Read }.as_id();
	let q = format!(
		"SELECT PERCENTILE(duration_us, 99.99)
		FROM kvlog
		WHERE source = 'id-{}'
		AND time >= '{}'
		AND time <= '{}'",
		source_id, min, max
	);

	let now = Instant::now();
	let result = do_query(q);
	let resp: Value = serde_json::from_str(&result).unwrap();

	let val = resp["results"][0]["series"][0]["values"][0][1]
		.as_i64()
		.unwrap();
	let q = format!(
		"SELECT COUNT(pid)
		FROM kvlog
		WHERE source = 'id-{}'
		AND duration_us > {}
		AND time >= '{}'
		AND time <= '{}'",
		source_id, val, min, max
	);

	let _result = do_query(q);
	let dur = now.elapsed();
	println!("Query Latency (Slow Requests): {:?}", dur);
}

fn valkey_p2_query(min: String, max: String) {
	let dur1 = {
		let source_id = SourceInformation::KVLog { op: KVOp::Read }.as_id();
		let q = format!(
			"SELECT PERCENTILE(duration_us, 99.99)
			FROM kvlog
			WHERE source = 'id-{}'
			AND time >= '{}'
			AND time <= '{}'",
			source_id, min, max
		);

		let now = Instant::now();
		let result = do_query(q);
		let resp: Value = serde_json::from_str(&result).unwrap();

		let val = resp["results"][0]["series"][0]["values"][0][1]
			.as_i64()
			.unwrap();
		println!("{}", val);
		let q = format!(
			"SELECT COUNT(pid)
			FROM kvlog
			WHERE source = 'id-{}'
			AND duration_us > {}
			AND time >= '{}'
			AND time <= '{}'",
			source_id, val, min, max
		);

		let _result = do_query(q);
		now.elapsed()
	};

	let dur2 = {
		let source_id =
			SourceInformation::SyscallEvent { syscall_number: 44 }.as_id();
		let q = format!(
			"SELECT PERCENTILE(duration_us, 99.99)
			FROM syscall_event
			WHERE source = 'id-{}'
			AND time >= '{}'
			AND time <= '{}'",
			source_id, min, max
		);

		let now = Instant::now();
		let result = do_query(q);
		let resp: Value = serde_json::from_str(&result).unwrap();
		let val = resp["results"][0]["series"][0]["values"][0][1]
			.as_i64()
			.unwrap();
		let q = format!(
			"SELECT COUNT(pid)
			FROM syscall_event
			WHERE source = 'id-{}'
			AND duration_us > {}
			AND time >= '{}'
			AND time <= '{}'",
			source_id, val, min, max
		);

		let _result = do_query(q);
		now.elapsed()
	};

	println!("Query Latency (Slow Requests): {:?}", dur1);
	println!("Query Latency (Slow sendto Execution): {:?}", dur2);
}

fn valkey_p3_query(min: String, max: String) {
	let (dur1, resp) = {
		let source_id = SourceInformation::KVLog { op: KVOp::Read }.as_id();
		let q = format!(
			"SELECT MAX(duration_us), *
			FROM kvlog
			WHERE source = 'id-{}'
			AND time >= '{}'
			AND time <= '{}'",
			source_id, min, max
		);

		let now = Instant::now();
		let result = do_query(q);
		let resp: Value = serde_json::from_str(&result).unwrap();
		let dur = now.elapsed();
		(dur, resp)
	};

	let dur2 = {
		let val = resp["results"][0]["series"][0]["values"][0][0]
			.as_str()
			.unwrap();
		let source_id = SourceInformation::PacketCapture.as_id();
		let target_ts = chrono::DateTime::parse_from_rfc3339(val).unwrap();
		let low_ts = (target_ts - Duration::from_secs(5))
			.to_rfc3339_opts(SecondsFormat::Nanos, true);
		let high_ts = (target_ts + Duration::from_secs(5))
			.to_rfc3339_opts(SecondsFormat::Nanos, true);
		let q = format!(
			"SELECT COUNT(dst_port)
			FROM packet
			WHERE source = 'id-{}'
			AND time >= '{}'
			AND time <= '{}'",
			source_id, low_ts, high_ts
		);

		let now = Instant::now();
		let _result = do_query(q);
		now.elapsed()
	};

	println!("Query Latency (Maximum Latency Request): {:?}", dur1);
	println!("Query Latency (TCP Packet scan): {:?}", dur2);
}

fn do_query(query: String) -> String {
	println!("Doing query: {query}");
	let mut map = HashMap::new();
	map.insert("q", query);

	let url = "http://localhost:8086/query?db=mydb";
	let client = reqwest::blocking::Client::new();
	let resp = client
		.get(url)
		.query(&*map)
		.timeout(Duration::from_secs(50 * 30))
		.send()
		.unwrap();

	let json = resp.text().unwrap();
	json
}

fn setup_influxv1() {
	let url = "http://localhost:8086/query?q=CREATE DATABASE mydb";
	let client = reqwest::blocking::Client::new();
	client.post(url).send().unwrap();
}

#[derive(Copy, Clone, Debug, ValueEnum, PartialEq, Eq)]
#[allow(non_camel_case_types)]
enum Query {
	rocksdb_p1,
	rocksdb_p2,
	rocksdb_p3,
	correlate,
	valkey_p1,
	valkey_p2,
	valkey_p3,
}

fn run_query(q: Query) {
	for i in 0..5 {
		thread::sleep(Duration::from_secs(120));

		let now = CURRENT_INGEST_TS.load(SeqCst);
		let utc = Utc.with_ymd_and_hms(1970, 1, 1, 0, 0, 0).unwrap();
		let now_ts = utc + Duration::from_micros(now);
		let max_ts = now_ts - Duration::from_micros(20 * 1_000_000);
		let min_ts = now_ts - Duration::from_micros(80 * 1_000_000);

		let max_s = max_ts.to_rfc3339_opts(SecondsFormat::Nanos, true);
		let min_s = min_ts.to_rfc3339_opts(SecondsFormat::Nanos, true);

		println!("min {} max {}", min_s, max_s);

		println!("Executing query {}", i);
		match q {
			Query::rocksdb_p1 => rocksdb_p1_query(min_s, max_s),
			Query::rocksdb_p2 => rocksdb_p2_query(min_s, max_s),
			Query::rocksdb_p3 => rocksdb_p3_query(min_s, max_s),
			Query::valkey_p1 => valkey_p1_query(min_s, max_s),
			Query::valkey_p2 => valkey_p2_query(min_s, max_s),
			Query::valkey_p3 => valkey_p3_query(min_s, max_s),
			_ => panic!("Got correlate in run_query"),
		};
	}
}

fn run_correlate(correlation_path: Option<String>) {
	let now = CURRENT_INGEST_TS.load(SeqCst);

	let utc = Utc.with_ymd_and_hms(1970, 1, 1, 0, 0, 0).unwrap();
	let now_ts = utc + Duration::from_micros(now);
	let max_ts = now_ts - Duration::from_micros(10 * 1_000_000);
	let min_ts = now_ts - Duration::from_micros(130 * 1_000_000);

	let max_s = max_ts.to_rfc3339_opts(SecondsFormat::Nanos, true);
	let min_s = min_ts.to_rfc3339_opts(SecondsFormat::Nanos, true);

	println!("min {} max {}", min_s, max_s);

	STOP_INGEST.store(true, SeqCst);
	let corr_path = correlation_path.unwrap();
	valkey_p3_correlation(min_s, max_s, &corr_path);
	println!("Done with correlation");
}

fn complete_data_query(q: Query) {
	thread::sleep(Duration::from_secs(120));

	let now = micros_since_epoch();

	// we want to query data relative to now
	let max_us = now - 20 * 1_000_000;
	let min_us = now - 80 * 1_000_000;

	let epoch = Utc.with_ymd_and_hms(1970, 1, 1, 0, 0, 0).unwrap();
	let min_ts = epoch + Duration::from_micros(min_us);
	let max_ts = epoch + Duration::from_micros(max_us);

	let max_s = max_ts.to_rfc3339_opts(SecondsFormat::Nanos, true);
	let min_s = min_ts.to_rfc3339_opts(SecondsFormat::Nanos, true);

	// and then we wait until the current ingest timestamp is greater than
	// max_ts
	println!("Sleeping in complete data query done");
	println!("Waiting for complete data to be loaded");
	// We can now signal TCP to stop sending queries so that we don't
	// consume all RAM
	STOP_INGEST.store(true, SeqCst);

	let now = Instant::now();
	loop {
		let current = CURRENT_INGEST_TS.load(SeqCst);
		if current > max_us {
			break;
		}
		let diff = max_us - current;
		println!("Remaining: {diff}us to ingest");
		thread::sleep(Duration::from_secs(1));
	}
	println!("Done waiting. We waited for {:?}", now.elapsed());

	match q {
		Query::rocksdb_p1 => rocksdb_p1_query(min_s, max_s),
		Query::rocksdb_p2 => rocksdb_p2_query(min_s, max_s),
		Query::rocksdb_p3 => rocksdb_p3_query(min_s, max_s),
		Query::valkey_p1 => valkey_p1_query(min_s, max_s),
		Query::valkey_p2 => valkey_p2_query(min_s, max_s),
		Query::valkey_p3 => valkey_p3_query(min_s, max_s),
		_ => panic!("Got correlate in run_query"),
	};
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
	#[arg(short, long)]
	addr: String,

	#[arg(short, long)]
	runtime_threads: u64,

	#[arg(short, long)]
	influx_connections: u64,

	#[arg(short, long)]
	query: Option<Query>,

	#[arg(short, long)]
	correlation_path: Option<String>,

	#[arg(short, long)]
	complete_data: bool,
}

fn main() {
	let args = Args::parse();
	let addr = args.addr.clone();
	let runtime_threads = args.runtime_threads;
	let influx_connections = args.influx_connections;
	let query = args.query;
	let correlation_path = args.correlation_path.clone();
	let complete_data = args.complete_data;
	match (query, &correlation_path) {
		(Some(Query::correlate), None) => {
			panic!(
				"Correlation requested but no path given. Set correlation-path"
			);
		}
		_ => {}
	}

	if complete_data && query.is_none() {
		panic!("full data requested but query not set");
	}

	let (tx, rx) = if complete_data {
		unbounded()
	} else {
		bounded(8 * 1024)
	};

	println!("Launching listener thread");
	let ingest_handle = thread::spawn(move || listener(addr.as_str(), tx));

	println!("Establishing connection to influx and setting up database");
	setup_influxv1();

	let rt = tokio::runtime::Builder::new_multi_thread()
		.worker_threads(runtime_threads as usize)
		.enable_all()
		.build()
		.unwrap();

	println!("Initializing ingest tasks");
	for _ in 0..influx_connections {
		let receiver = rx.clone();
		rt.spawn(async move { ingest(receiver).await });
	}

	thread::spawn(counters);

	let durations: Vec<Duration> = Vec::new();
	if complete_data {
		let q = query.unwrap();
		complete_data_query(q);
		std::process::exit(0);
	}

	if let Some(q) = query {
		if q == Query::correlate {
			std::thread::sleep(Duration::from_secs(140));
			run_correlate(correlation_path);
		} else {
			run_query(q);
		}
		std::process::exit(0);
	}

	println!("Blocking on handler thread");
	ingest_handle.join().unwrap();

	let d: Vec<_> = durations.iter().map(|x| x.as_secs_f64()).collect();
	println!("Query Durations: {:?}", d);

	// Because sometimes there's some thread hanging out?
	std::process::exit(0);
}
