mod page_cache_bpf_lib;
mod rocksdb_lib;
mod syscall_bpf_lib;

use clap::Parser;
use crossbeam::channel::{bounded, Receiver};
use data::{
	record::{Record, RecordBatch, RecordBatchSerializer},
	record_bytes::RecordBatchBytes,
};
use rocksdb_lib::ROCKSDB_STARTED;
use std::{
	fs::File,
	io::prelude::*,
	net::TcpStream,
	path::PathBuf,
	sync::atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst},
	thread,
};
use utils::{micros_since_epoch, u64_from_bytes, Duration, Instant};

//static COUNT: AtomicUsize = AtomicUsize::new(0);
pub static DROPPED: AtomicUsize = AtomicUsize::new(0);
static APP_COUNT: AtomicUsize = AtomicUsize::new(0);
static SYSCALL_COUNT: AtomicUsize = AtomicUsize::new(0);
static PAGE_CACHE_COUNT: AtomicUsize = AtomicUsize::new(0);
static DONE: AtomicBool = AtomicBool::new(false);

fn counter() {
	while !ROCKSDB_STARTED.load(SeqCst) {
		thread::sleep(Duration::from_secs(1));
	}

	let mut count = 0;
	let mut op_count = 0;
	let mut start = Instant::now();
	loop {
		if DONE.load(SeqCst) {
			break;
		}
		let app_cnt = APP_COUNT.swap(0, SeqCst);
		let syscall = SYSCALL_COUNT.swap(0, SeqCst);
		let page_cache = PAGE_CACHE_COUNT.swap(0, SeqCst);
		let total = app_cnt + syscall + page_cache;
		let dropped = DROPPED.swap(0, SeqCst);

		if op_count == 0 {
			// to account for the fact that this is the first second of data,
			// subtract 1 second
			start = Instant::now() - Duration::from_secs(1);
		}
		op_count += app_cnt;
		op_count += dropped;

		println!(
			"Count: {} App: {}, Syscall: {}, Page cache: {}, Count: {}, Dropped: {}",
			count, app_cnt, syscall, page_cache, total, dropped
		);
		count += 1;
		thread::sleep(Duration::from_secs(1));
	}

	let average = op_count as f64 / start.elapsed().as_secs_f64();
	println!("Workload done, exiting print out");
	println!("Average rocksdb rate per sec: {:.2}", average);
}

pub(crate) enum SourceKind {
	Application,
	Syscall,
	PageCache,
}

fn receiver_loop(
	receiver: Receiver<(SourceKind, Vec<Record>)>,
	collect_syscalls: bool,
	collect_page_cache: bool,
	duration_secs: Option<u64>,
	mut handle_batch: impl FnMut(&RecordBatch),
) {
	//let mut batches = Vec::new();

	// Consume and discard while workload has not started
	while !ROCKSDB_STARTED.load(SeqCst) {
		let _ = receiver.recv();
	}

	println!("Warming up for 15 seconds");
	let warmup_start = Instant::now();
	while let Ok(_) = receiver.recv_timeout(Duration::from_secs(5)) {
		if warmup_start.elapsed() > Duration::from_secs(15) {
			break;
		}

		if DONE.load(SeqCst) {
			return;
		}
	}

	let start = Instant::now();
	let mut count = 0;
	while let Ok((kind, record)) = receiver.recv_timeout(Duration::from_secs(5))
	{
		if DONE.load(SeqCst) {
			break;
		}

		match duration_secs {
			Some(x) => {
				if start.elapsed() > Duration::from_secs(x) {
					DONE.store(true, SeqCst);
					break;
				}
			}
			None => {}
		};

		count += 1;
		let now = micros_since_epoch();
		let l = record.len();
		match kind {
			SourceKind::Application => {
				APP_COUNT.fetch_add(l, SeqCst);
			}
			SourceKind::Syscall => {
				if !collect_syscalls {
					continue;
				}
				SYSCALL_COUNT.fetch_add(l, SeqCst);
			}
			SourceKind::PageCache => {
				if !collect_page_cache {
					continue;
				}
				PAGE_CACHE_COUNT.fetch_add(l, SeqCst);
			}
		};

		let batch = RecordBatch {
			arrival_us: now,
			data: record,
		};
		//batches.push_batch(&batch);
		handle_batch(&batch);
	}

	println!("Workload done, saving data");
	println!("Batch count: {}", count);
}

fn do_nothing(
	receiver: Receiver<(SourceKind, Vec<Record>)>,
	collect_syscalls: bool,
	collect_page_cache: bool,
	duration_secs: Option<u64>,
) {
	let func = |_batch: &RecordBatch| {};

	receiver_loop(
		receiver,
		collect_syscalls,
		collect_page_cache,
		duration_secs,
		func,
	);

	println!("Receiver loop exited, doing nothing with data");
}

fn record_workload(
	out_path: PathBuf,
	receiver: Receiver<(SourceKind, Vec<Record>)>,
	collect_syscalls: bool,
	collect_page_cache: bool,
	duration_secs: Option<u64>,
) {
	let mut batches = RecordBatchSerializer::new();

	let func = |batch: &RecordBatch| {
		batches.push_batch(batch);
	};

	receiver_loop(
		receiver,
		collect_syscalls,
		collect_page_cache,
		duration_secs,
		func,
	);

	println!("Post processing recorded data");
	let bytes = batches.into_vec();

	let mut file = File::create(&out_path).unwrap();
	file.write_all(bytes.as_slice()).unwrap();
	file.sync_all().unwrap();
	println!(
		"Wrote {}mb to file: {:?}",
		bytes.len() / 1_000_000,
		out_path
	);
}

fn raw_to_file(
	out_path: PathBuf,
	receiver: Receiver<(SourceKind, Vec<Record>)>,
	collect_syscalls: bool,
	collect_page_cache: bool,
	duration_secs: Option<u64>,
	_flush_interval_mb: u64,
) {
	let mut file = File::create(&out_path).unwrap();
	let mut buf = Vec::new();
	let mut bytes_count = 0;
	//let mut since_last_flush = 0;

	let mut last_flush = Instant::now();
	let func = |batch: &RecordBatch| {
		buf.clear();
		batch.to_byte_vec(&mut buf);
		bytes_count += buf.len();
		(&mut file).write_all(buf.as_slice()).unwrap();
		if last_flush.elapsed() > Duration::from_secs(1) {
			(&mut file).sync_all().unwrap();
			last_flush = Instant::now();
		}
	};

	receiver_loop(
		receiver,
		collect_syscalls,
		collect_page_cache,
		duration_secs,
		func,
	);

	println!(
		"Wrote {}mb to file: {:?}",
		bytes_count / 1_000_000,
		out_path
	);
}

fn write_to_tcp(
	mut stream: TcpStream,
	receiver: Receiver<(SourceKind, Vec<Record>)>,
	collect_syscalls: bool,
	collect_page_cache: bool,
	duration_secs: Option<u64>,
	fishstore: bool,
) {
	println!("Entering client loop");

	let mut buf = Vec::new();

	let func = |batch: &RecordBatch| {
		buf.clear();
		batch.to_byte_vec(&mut buf);
		let batch_bytes = RecordBatchBytes::new(buf.as_slice());
		let (header, bytes) = batch_bytes.prep_for_tcp(fishstore);

		stream.write_all(&header[..]).unwrap();
		stream.write_all(&bytes[..]).unwrap();
	};

	receiver_loop(
		receiver,
		collect_syscalls,
		collect_page_cache,
		duration_secs,
		func,
	);
}

fn bump_memlock_rlimit() -> Result<(), &'static str> {
	let rlimit = libc::rlimit {
		rlim_cur: 128 << 20,
		rlim_max: 128 << 20,
	};

	if unsafe { libc::setrlimit(libc::RLIMIT_MEMLOCK, &rlimit) } != 0 {
		Err("Failed to increase rlimit")
	} else {
		Ok(())
	}
}

fn drop_caches_loop() {
	use std::process::Command;
	let mut comm = Command::new("sh");
	comm.arg("-c")
		.arg("echo 1 | sudo tee /proc/sys/vm/drop_caches");

	while !ROCKSDB_STARTED.load(SeqCst) {
		thread::sleep(Duration::from_secs(1));
	}

	loop {
		thread::sleep(Duration::from_secs(40));
		println!("Dropping caches");
		comm.status().expect("process failed to execute");
		if DONE.load(SeqCst) {
			break;
		}
	}
}

#[derive(Copy, Clone, Debug)]
enum AlternativeRecording {
	RawToFile,
	TCP,
	Noop,
}

impl std::fmt::Display for AlternativeRecording {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		let s = match self {
			Self::RawToFile => "raw to file",
			Self::Noop => "noop",
			Self::TCP => "tcp",
		};
		s.fmt(f)
	}
}

impl std::str::FromStr for AlternativeRecording {
	type Err = String;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match s {
			"raw-to-file" => Ok(Self::RawToFile),
			"tcp" => Ok(Self::TCP),
			"noop" => Ok(Self::Noop),
            _ => Err(format!("Unknown alternative recording: {s}. Valid opts are raw-to-file, noop, tcp")),
        }
	}
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
	#[arg(short, long)]
	output_path: Option<String>,

	#[arg(short, long)]
	syscalls: bool,

	#[arg(short, long)]
	page_cache: bool,

	#[arg(short, long)]
	rocksdb_path: String,

	#[arg(short, long)]
	duration_seconds: Option<u64>,

	#[arg(short, long)]
	alternative_recording: Option<AlternativeRecording>,

	#[arg(short, long)]
	flush_interval_mb: Option<u64>,

	#[arg(short, long)]
	no_drop_caches: bool,

	#[arg(short, long)]
	fishstore: bool,
}

fn setup_tcp() -> TcpStream {
	println!("Connecting to storage server");
	let mut stream = TcpStream::connect("127.0.0.1:7878").unwrap();
	stream.set_nodelay(true).unwrap();
	println!("Waiting for storage server to signal ready");
	let mut start = [0u8; 8];
	stream.read_exact(&mut start[..]).unwrap();
	let start_signal = u64_from_bytes(&start);
	if start_signal != 1234 {
		println!("Received unexpected start signal {start_signal}");
		std::process::exit(0);
	}
	stream
}

fn main() {
	let _ = ctrlc::set_handler(move || {
		DONE.store(true, SeqCst);
	});

	let args = Args::parse();

	let collect_syscalls = args.syscalls;
	let collect_page_caches = args.page_cache;
	println!("Collect page caches: {}", collect_page_caches);
	let file_out_path = args.output_path.clone();
	let rocksdb_path = args.rocksdb_path.clone();
	let duration_seconds = args.duration_seconds;
	let alternative_recording = args.alternative_recording;
	let flush_interval_mb = args.flush_interval_mb;
	let drop_caches = !args.no_drop_caches;
	let fishstore = args.fishstore;

	// let rocksdb_path = "/nvme/data/tmp/can_delete/rocksdb-app";
	//let rocksdb_path = "/home/ubuntu/rocksdb-app";
	let rocksdb_instances = 3;
	let rocksdb_threads_per_instance = 16;
	let rocksdb_read_ratio = 0.90;
	let rocksdb_key_low = 0;
	let rocksdb_key_max_read = 128;
	let rocksdb_key_high = 1024 * 1024;

	let stream = match alternative_recording {
		Some(AlternativeRecording::TCP) => Some(setup_tcp()),
		_ => None,
	};

	let (tx, rx) = bounded(4096);

	if drop_caches {
		thread::spawn(drop_caches_loop);
	}

	thread::spawn(counter);

	let handle = thread::spawn(move || match alternative_recording {
		None => {
			let file_out_path = file_out_path.unwrap();
			record_workload(
				file_out_path.into(),
				rx,
				collect_syscalls,
				collect_page_caches,
				duration_seconds,
			);
		}

		Some(AlternativeRecording::TCP) => {
			let stream = stream.unwrap();
			write_to_tcp(
				stream,
				rx,
				collect_syscalls,
				collect_page_caches,
				duration_seconds,
				fishstore,
			)
		}

		Some(AlternativeRecording::RawToFile) => {
			let flush_interval_mb = flush_interval_mb.unwrap();
			let file_out_path = file_out_path.unwrap();
			raw_to_file(
				file_out_path.into(),
				rx,
				collect_syscalls,
				collect_page_caches,
				duration_seconds,
				flush_interval_mb,
			)
		}

		Some(AlternativeRecording::Noop) => do_nothing(
			rx,
			collect_syscalls,
			collect_page_caches,
			duration_seconds,
		),
	});

	{
		let tx = tx.clone();
		thread::spawn(move || {
			rocksdb_lib::run_workload(
				rocksdb_instances,
				rocksdb_threads_per_instance,
				rocksdb_read_ratio,
				rocksdb_key_low,
				rocksdb_key_high,
				rocksdb_key_max_read,
				rocksdb_path.into(),
				tx,
			);
		});
	}

	bump_memlock_rlimit().unwrap();

	{
		let tx = tx.clone();
		thread::spawn(move || {
			syscall_bpf_lib::run_workload(tx);
		});
	}

	{
		let tx = tx.clone();
		thread::spawn(move || {
			page_cache_bpf_lib::run_workload(tx);
		});
	}

	while !DONE.load(SeqCst) {
		thread::sleep(Duration::from_secs(1));
	}

	println!("Waiting for records receiver to complete");
	handle.join().unwrap();

	// To make sure the records per second gets printed out
	thread::sleep(Duration::from_secs(2));
	println!("Done");
}
