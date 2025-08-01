mod packet_filter;
mod syscall_bpf_lib;
mod tcp_dump;
mod valkey_load;

use clap::Parser;
use crossbeam::channel::{unbounded, Receiver, Sender};
use data::{
	kv_op::{KVLog, KVOp},
	packet::PacketCapture,
	record::{Record, RecordBatch, RecordBatchSerializer},
	syscall::SyscallEvent,
};
use std::{
	fs::File,
	io::Write,
	path::PathBuf,
	sync::atomic::{AtomicBool, AtomicU64, Ordering::SeqCst},
	thread,
};
use utils::{micros_since_epoch, Duration, Instant};

static DONE: AtomicBool = AtomicBool::new(false);
static TCP_EVENT_COUNT: AtomicU64 = AtomicU64::new(0);
static VALKEY_EVENT_COUNT: AtomicU64 = AtomicU64::new(0);
static SYSCALL_EVENT_COUNT: AtomicU64 = AtomicU64::new(0);

fn counter() {
	let mut count = 0;
	loop {
		if DONE.load(SeqCst) {
			break;
		}

		count += 1;
		let tcp = TCP_EVENT_COUNT.swap(0, SeqCst);
		let valkey = VALKEY_EVENT_COUNT.swap(0, SeqCst);
		let syscall_event = SYSCALL_EVENT_COUNT.swap(0, SeqCst);

		let latency_write = valkey_load::MAX_LATENCY_WRITE.swap(0, SeqCst);
		let latency_read = valkey_load::MAX_LATENCY_READ.swap(0, SeqCst);
		let latency_syscall = syscall_bpf_lib::SYSCALL_LATENCY.swap(0, SeqCst);

		let total = tcp + valkey + syscall_event;
		println!("Count: {}, Valkey: {}, Packets: {}, Syscalls: {}, Total: {}, lat_w: {}, lat_r: {}, lat_syscall: {}", count, valkey, tcp, syscall_event, total, latency_write, latency_read, latency_syscall);
		thread::sleep(Duration::from_secs(1));
	}
}

pub(crate) enum Event {
	TcPacket(Vec<Record>),
	Valkey(Vec<Record>),
	Syscall(Vec<Record>),
}

impl Event {
	pub fn to_record_batch(self, arrival_us: u64) -> RecordBatch {
		match self {
			Event::TcPacket(data) => {
				TCP_EVENT_COUNT.fetch_add(data.len() as u64, SeqCst);
				RecordBatch { arrival_us, data }
			}
			Event::Valkey(data) => {
				VALKEY_EVENT_COUNT.fetch_add(data.len() as u64, SeqCst);
				RecordBatch { arrival_us, data }
			}
			Event::Syscall(data) => {
				SYSCALL_EVENT_COUNT.fetch_add(data.len() as u64, SeqCst);
				RecordBatch { arrival_us, data }
			}
		}
	}
}

pub(crate) fn tc_slice_to_records(events: &[tcp_dump::TcEvent]) -> Vec<Record> {
	let mut records = Vec::new();
	for item in events.iter() {
		records.push(tc_event_to_record(item));
	}
	records
}

pub(crate) fn syscall_slice_to_records(
	events: &[syscall_bpf_lib::SyscallEvent],
) -> Vec<Record> {
	let mut records = Vec::new();
	for item in events.iter() {
		records.push(syscall_event_to_record(item));
	}
	records
}

pub(crate) fn valkey_slice_to_records(
	events: &[valkey_load::ValkeyQueryEvent],
) -> Vec<Record> {
	let mut records = Vec::new();
	for item in events.iter() {
		records.push(valkey_event_to_record(item));
	}
	records
}

fn tc_event_to_record(event: &tcp_dump::TcEvent) -> Record {
	Record::from_packet_capture(PacketCapture {
		dst_port: event.dst_port as u64,
		src_port: event.src_port as u64,
		dst_addr: event.dst_addr as u64,
		src_addr: event.src_addr as u64,
		tot_len: event.tot_len as u64,
		timestamp: event.timestamp as u64,
	})
}

fn syscall_event_to_record(event: &syscall_bpf_lib::SyscallEvent) -> Record {
	Record::from_syscall_event(SyscallEvent {
		pid: event.pid,
		tid: event.tid,
		syscall_number: event.syscall_number,
		start_time: event.timestamp_ns,
		duration_us: event.duration_us,
	})
}

fn valkey_event_to_record(event: &valkey_load::ValkeyQueryEvent) -> Record {
	Record::from_kvlog(KVLog {
		tid: event.tid,
		op: if event.write { KVOp::Write } else { KVOp::Read },
		timestamp: event.timestamp,
		duration_us: event.latency_us,
	})
}

fn event_handler(
	out_path: PathBuf,
	rx: Receiver<Event>,
	collect_syscalls: bool,
	collect_packets: bool,
	duration_secs: Option<u64>,
	sink: Sender<RecordBatch>,
) {
	let mut batches = RecordBatchSerializer::new();
	let mut count = 0;

	println!("Warming up for 15 seconds, no collection will be done");
	let warmup_start = Instant::now();
	while let Ok(_event) = rx.recv_timeout(Duration::from_secs(5)) {
		if warmup_start.elapsed() > Duration::from_secs(15) {
			break;
		}
		if DONE.load(SeqCst) {
			return;
		}
	}

	if DONE.load(SeqCst) {
		return;
	}

	println!("Warm up done, starting collection");
	let start = Instant::now();
	while let Ok(event) = rx.recv_timeout(Duration::from_secs(5)) {
		match duration_secs {
			Some(x) => {
				if start.elapsed().as_secs() as u64 > x {
					DONE.store(true, SeqCst);
					break;
				}
			}
			None => {}
		}

		match &event {
			Event::TcPacket(_) => {
				if !collect_packets {
					continue;
				}
			}
			Event::Syscall(_) => {
				if !collect_syscalls {
					continue;
				}
			}
			_ => {}
		}

		count += 1;
		let arrival_us = micros_since_epoch();
		let batch = event.to_record_batch(arrival_us);
		batches.push_batch(&batch);
		let _ = sink.send(batch);
		if DONE.load(SeqCst) {
			break;
		}
	}
	println!("Done. Received {} batches", count);

	println!("Writing to file {:?}", &out_path);
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

fn bump_memlock_rlimit() {
	let rlimit = libc::rlimit {
		rlim_cur: 128 << 20,
		rlim_max: 128 << 20,
	};
	if unsafe { libc::setrlimit(libc::RLIMIT_MEMLOCK, &rlimit) } != 0 {
		panic!("Failed to increase rlimit");
	}
}

#[derive(Clone, Debug, Parser)]
struct Args {
	/// destroy all hooks on clsact
	#[arg(long)]
	tcp_dump_detach: bool,

	/// Output path to write recorded data
	#[arg(short, long)]
	output_path: Option<String>,

	/// Set to collect syscalls
	#[arg(short, long)]
	syscalls: bool,

	/// Set to collect packets
	#[arg(short, long)]
	packets: bool,

	/// How long to run for, ctrl+c if not set
	#[arg(short, long)]
	duration_seconds: Option<u64>,
}

fn main() {
	bump_memlock_rlimit();
	let _ = ctrlc::set_handler(move || {
		DONE.store(true, SeqCst);
	});

	let args = Args::parse();

	let output_path = args.output_path.clone();
	let syscalls = args.syscalls;
	let packets = args.packets;
	let duration_seconds = args.duration_seconds;

	if args.tcp_dump_detach {
		tcp_dump::tcp_dump_detach();
		return;
	}

	let output_path = match output_path {
		None => {
			println!("Should set output path!");
			return ();
		}
		Some(x) => x,
	};

	let valkey_instances = 17;
	let valkey_threads_per_instance = 8;

	thread::spawn(counter);

	let (tx, rx) = unbounded();
	let (sink_tx, sink_rx) = unbounded();

	thread::spawn(move || while let Ok(_) = sink_rx.recv() {});
	let event_handler_handle = thread::spawn(move || {
		event_handler(
			output_path.into(),
			rx,
			syscalls,
			packets,
			duration_seconds,
			sink_tx,
		)
	});

	let valkey_workload = {
		let tx = tx.clone();
		valkey_load::run_valkey_workload(
			valkey_instances,
			valkey_threads_per_instance,
			tx,
		)
	};

	let packet_filter_state = packet_filter::init_packet_filter();

	let tcp_dump_workload = {
		let tx = tx.clone();
		tcp_dump::tcp_dump_workload(tx)
	};

	let syscall_workload = {
		let tx = tx.clone();
		syscall_bpf_lib::syscall_workload(tx)
	};

	loop {
		thread::sleep(Duration::from_secs(1));
		if DONE.load(SeqCst) {
			break;
		}
	}

	valkey_workload.halt();
	tcp_dump_workload.halt();
	syscall_workload.halt();
	packet_filter_state.halt();

	println!("Check for stray and unattached stuff using");
	println!("sudo bpftool prog");
	println!("sudo bpftool map");
	println!("ps -a");

	println!("Waiting for event handler to exit");
	let _ = event_handler_handle.join();
	println!("Waiting for valkey to exit");
	valkey_workload.wait();
	println!("Waiting for tcp dump to exit");
	tcp_dump_workload.wait();
	println!("Waiting for syscall to exit");
	syscall_workload.wait();
	println!("Waiting for packet filter to exit");
	packet_filter_state.wait();
	println!("Exiting");
}
