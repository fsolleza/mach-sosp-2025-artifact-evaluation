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

fn counters() {
	let mut total_received = 0;
	let mut total_dropped = 0;

	loop {
		let r = RECEIVED.swap(0, SeqCst);
		let d = DROPPED.swap(0, SeqCst);
		let w = r - d;
		total_received += r;
		total_dropped += d;
		println!(
			"Records received: {r}, dropped: {d}, written {w}, total received \
			 {total_received} dropped {total_dropped}"
		);
		thread::sleep(Duration::from_secs(10));
	}
}

fn register_source(mach: &mut Partitions, source_id: u64, index_bins: u64) {
	let kv_id = SourceInformation::KVLog { op: KVOp::Read }.as_id();
	let syscall_id =
		SourceInformation::SyscallEvent { syscall_number: 17 }.as_id();
	mach.define_source(source_id, 0);
	if source_id == kv_id {
		match index_bins {
			1 => mach.define_range_index_u64(kv_id, 0, kv_indexer, 0, 100, 1), // inlude > 100
			4 => mach.define_range_index_u64(kv_id, 0, kv_indexer, 0, 25, 4),
			10 => mach.define_range_index_u64(kv_id, 0, kv_indexer, 0, 10, 10),
			20 => mach.define_range_index_u64(kv_id, 0, kv_indexer, 0, 5, 20),
			_ => panic!("Invalid index bins, choose 1, 4, 10, 20"),
		};
	}
}

fn ingest(mut mach: Partitions, rx: Receiver<Vec<u8>>, index_bins: u64) {
	let mut registered_sources = HashSet::new();
	let mut records_written = 0u64;
	let mut bytes_written = 0u64;

	let duration = 240;
	let mut now = Instant::now();
	let mut first = true;
	while let Ok(item) = rx.recv() {
		if first {
			first = false;
			now = Instant::now();
		}

		let batch_bytes = RecordBatchBytes::new(item.as_slice());
		let mut iterator = batch_bytes.iterator();
		while let Some(b) = iterator.next_record_bytes() {
			let source_id = b.source_id();
			if registered_sources.insert(source_id) {
				register_source(&mut mach, source_id, index_bins);
			}
			mach.push(source_id, 0, b.bytes);
			bytes_written += b.bytes.len() as u64;
			records_written += 1;
		}

		if now.elapsed() > Duration::from_secs(duration) {
			break;
		}
	}

	let rps = records_written as f64 / duration as f64;
	let bps = bytes_written as f64 / duration as f64;
	println!("Exiting ingest thread, {rps} {bps}");
}

fn handler(_listener: TcpListener, mut stream: TcpStream, tx: Sender<Vec<u8>>) {
	// signal to client that this connection am ready

	println!("Sending signal ready to client");
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

		RECEIVED.fetch_add(l, SeqCst);
		if tx.try_send(v).is_err() {
			DROPPED.fetch_add(l, SeqCst);
		}
	}
}

fn listener(addr: &str, tx: Sender<Vec<u8>>) {
	let listener = TcpListener::bind(addr).unwrap();
	let stream = listener.incoming().next().unwrap().unwrap();
	handler(listener, stream, tx);
	println!("Exiting handler thread");
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
	#[arg(short, long)]
	mach_path: String,

	#[arg(short, long)]
	addr: String,

	#[arg(short, long)]
	index_bins: u64,
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

fn main() {
	let args = Args::parse();
	let addr = args.addr.clone();
	let index_bins = args.index_bins;
	let mach_path = args.mach_path.clone();

	let (tx, rx) = bounded(4096);

	let mut mach = Partitions::new(mach_path.into());

	thread::spawn(move || {
		counters();
	});

	println!("Launched ingest thread");
	let ingest_handle = thread::spawn(move || ingest(mach, rx, index_bins));

	println!("Launched listener thread");
	thread::spawn(move || listener(addr.as_str(), tx));

	let _ = ingest_handle.join();
}
