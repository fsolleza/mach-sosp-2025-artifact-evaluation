use clap::Parser;
use crossbeam::channel::{bounded, Receiver, Sender};
use data::record_bytes::RecordBatchBytes;
use std::{
	fs::{remove_file, File},
	io::prelude::*,
	net::{TcpListener, TcpStream},
	sync::atomic::{AtomicU64, Ordering::SeqCst},
	thread,
};
use utils::{u64_from_bytes, Duration, Instant};

static RECEIVED: AtomicU64 = AtomicU64::new(0);
static DROPPED: AtomicU64 = AtomicU64::new(0);

fn counters() {
	loop {
		let r = RECEIVED.swap(0, SeqCst);
		let d = DROPPED.swap(0, SeqCst);
		println!("Records received: {r}, dropped: {d}");
		thread::sleep(Duration::from_secs(1));
	}
}

fn handler(mut stream: TcpStream, tx: Sender<Vec<u8>>) {
	// signal to client that this connection am ready

	stream.write_all(&1234u64.to_be_bytes()).unwrap();

	loop {
		// Read header
		let mut counts = [0u8; 40];
		if stream.read_exact(&mut counts[..]).is_err() {
			println!("Failed to read from stream, breaking");
			break;
		}

		// let kv_log = u64_from_bytes(&counts[..8]);
		// let syscall = u64_from_bytes(&counts[8..16]);
		// let pagecache = u64_from_bytes(&counts[16..24]);
		// let packet = u64_from_bytes(&counts[24..32]);
		let msg_sz = u64_from_bytes(&counts[32..40]);

		let mut v = vec![0u8; msg_sz as usize];
		stream.read_exact(&mut v[..]).unwrap();

		let l = {
			let batch_bytes = RecordBatchBytes::new(v.as_slice());
			batch_bytes.data_len()
		};

		RECEIVED.fetch_add(l, SeqCst);
		if tx.try_send(v).is_err() {
			DROPPED.fetch_add(l, SeqCst);
		}
	}
}

fn listener(addr: &str, tx: Sender<Vec<u8>>) {
	let listener = TcpListener::bind(addr).unwrap();
	for stream in listener.incoming() {
		let tx = tx.clone();
		thread::spawn(move || handler(stream.unwrap(), tx));
	}
}

fn ingest(mut file: File, rx: Receiver<Vec<u8>>) {
	let mut last_flush = Instant::now();
	while let Ok(item) = rx.recv() {
		file.write_all(&item[..]).unwrap();
		if last_flush.elapsed() > Duration::from_secs(1) {
			file.sync_all().unwrap();
			last_flush = Instant::now();
		}
	}
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
	#[arg(short, long)]
	file_path: String,

	#[arg(short, long)]
	addr: String,
}

fn main() {
	let args = Args::parse();
	let addr = args.addr.clone();
	let filepath = args.file_path.clone();

	let _ = remove_file(&filepath);
	let file = File::create(&filepath).unwrap();

	let (tx, rx) = bounded(1024 * 4);

	println!("Launched listener thread");
	thread::spawn(move || listener(addr.as_str(), tx));

	println!("Launched ingest thread");
	thread::spawn(move || ingest(file, rx));
	let c = thread::spawn(counters);

	println!("Blocking forever on counter thread");
	let _ = c.join();
}
