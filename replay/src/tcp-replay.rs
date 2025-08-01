use clap::Parser;
use crossbeam::channel::{bounded, Receiver, Sender};
use data::{record::RecordBatchesDeserializer, record_bytes::RecordBatchBytes};
use serde::{Deserialize, Serialize};
use std::{
	fs::File,
	io::prelude::*,
	net::TcpStream,
	sync::atomic::{AtomicU64, Ordering::SeqCst},
	thread,
};
use utils::{u64_from_bytes, Duration, Instant};

static GENERATED: AtomicU64 = AtomicU64::new(0);
static BEHIND: AtomicU64 = AtomicU64::new(0);

/// String format options:
/// - `TrueRate`
/// - `NoWait`
/// - `RecordsPerSecond:[amount]`
#[derive(
	Copy, Clone, Default, Debug, PartialEq, Eq, Serialize, Deserialize,
)]
enum DataRate {
	#[default]
	TrueRate,
	RecordsPerSecond(u64),
	NoWait,
}

impl std::str::FromStr for DataRate {
	type Err = &'static str;
	fn from_str(s: &str) -> Result<Self, Self::Err> {
		let sp: Vec<_> = s.split(':').collect();
		match &sp[..] {
			[variant] if *variant == "TrueRate" => Ok(DataRate::TrueRate),
			[variant] if *variant == "NoWait" => Ok(DataRate::NoWait),
			[variant, amt] if *variant == "RecordsPerSecond" => {
				let amt: u64 = match amt.parse() {
					Ok(x) => x,
					Err(_) => {
						return Err("Cant parse amt for RecordsPerSecond")
					}
				};
				Ok(DataRate::RecordsPerSecond(amt))
			}
			_ => Err("Error: UnknownFormat"),
		}
	}
}

impl std::fmt::Display for DataRate {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			DataRate::TrueRate => write!(f, "TrueRate"),
			DataRate::NoWait => write!(f, "NoWait"),
			DataRate::RecordsPerSecond(x) => {
				write!(f, "RecordsPerSecond:{}", x)
			}
		}
	}
}

fn counters() {
	for i in 0.. {
		let g = GENERATED.swap(0, SeqCst);
		let b = BEHIND.swap(0, SeqCst);
		println!("{i} Batches Generated: {g}, Behind: {b}");
		thread::sleep(Duration::from_secs(1));
	}
}

struct ReplayInfo {
	loop_cnt: u64,
	arrival_us: u64,
	header: Vec<u8>,
	bytes: Vec<u8>,
	record_count: u64,
}

fn replay(
	mut deserializer: RecordBatchesDeserializer,
	duration: Duration,
	fishstore: bool,
	tx: Sender<ReplayInfo>,
) {
	println!("Beginning replay");
	let mut loop_cnt = 0;
	let now = Instant::now();
	while now.elapsed() < duration {
		while let Some(bytes) = deserializer.next_batch_bytes() {
			let batch = RecordBatchBytes::new(bytes);
			let record_count = batch.record_count();
			let arrival_us = batch.arrival_us();
			let (header, bytes) = batch.prep_for_tcp(fishstore);
			let r = ReplayInfo {
				loop_cnt,
				arrival_us,
				header,
				bytes,
				record_count,
			};
			tx.send(r).unwrap();
		}
		deserializer.reset();
		loop_cnt += 1;
	}
}

fn tcp_send(
	mut stream: TcpStream,
	rx: Receiver<ReplayInfo>,
	data_rate: DataRate,
) {
	println!("Initing TCP send loop");

	let mut now = Instant::now();
	let mut start_time = u64::MAX;
	let mut this_loop = 0;

	let mut sec_start = Instant::now();
	let mut records_this_sec = 0;
	println!("Data rate: {}", data_rate);

	'recv_loop: while let Ok(r) = rx.recv() {
		let ReplayInfo {
			loop_cnt,
			arrival_us,
			header,
			bytes,
			record_count,
		} = r;
		if start_time == u64::MAX {
			start_time = arrival_us;
		}
		if loop_cnt > this_loop {
			now = Instant::now();
			this_loop = loop_cnt;
		}

		let d: u64 = arrival_us - start_time;

		if (now.elapsed().as_micros() as u64) > d {
			BEHIND.fetch_add(1, SeqCst);
		}

		match data_rate {
			DataRate::TrueRate => {
				while (now.elapsed().as_micros() as u64) < d {}
			}
			DataRate::NoWait => {}
			DataRate::RecordsPerSecond(x) => {
				let elapsed = sec_start.elapsed();
				if elapsed >= Duration::from_secs(1) {
					sec_start = Instant::now();
					records_this_sec = 0;
				}

				// sec_start.elapsed MUST be < 1sec. If we've
				// sent more than the specified amount of data,
				// go back to top and drop this batch.
				// Otherwise, send immediately (fall through).
				if records_this_sec >= x {
					continue 'recv_loop;
				}

				records_this_sec += record_count;
			}
		}

		//if !closed_loop {
		//	while (now.elapsed().as_micros() as u64) < d {}
		//}

		stream.write_all(&header).unwrap();
		stream.write_all(&bytes).unwrap();
		GENERATED.fetch_add(1, SeqCst);
	}
}

#[derive(clap::ValueEnum, Debug, Clone, Copy)]
enum StorageTarget {
	Mach,
	Fishstore,
	Influx,
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
	#[arg(short, long)]
	file_path: String,

	#[arg(short, long)]
	storage_target: StorageTarget, // mach, fishstore, influx

	#[arg(short, long)]
	duration_s: u64,

	#[arg(short, long, default_value_t=DataRate::TrueRate)]
	data_rate: DataRate,
}

fn main() {
	let args = Args::parse();
	let file_path = args.file_path.clone();
	let storage_target = args.storage_target.clone();
	let data_rate = args.data_rate;
	println!("Data rate: {}", data_rate);
	let duration = Duration::from_secs(args.duration_s);

	println!("Loading file");
	let mut file = File::open(file_path).unwrap();
	let mut data = Vec::new();
	let _ = file.read_to_end(&mut data).unwrap();
	let deserializer = RecordBatchesDeserializer::new(data);

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

	let (tx, rx) = bounded(128);

	let tcp_sender_handle =
		thread::spawn(move || tcp_send(stream, rx, data_rate));

	let replay_handle = match storage_target {
		StorageTarget::Mach => {
			thread::spawn(move || replay(deserializer, duration, false, tx))
		}
		StorageTarget::Fishstore => {
			thread::spawn(move || replay(deserializer, duration, true, tx))
		}
		StorageTarget::Influx => {
			thread::spawn(move || replay(deserializer, duration, false, tx))
		}
	};
	thread::spawn(counters);

	replay_handle.join().unwrap();
	tcp_sender_handle.join().unwrap();
}
