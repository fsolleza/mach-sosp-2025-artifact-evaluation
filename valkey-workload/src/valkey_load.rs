use crate::{valkey_slice_to_records, Event};
use crossbeam::channel::Sender;
use rand::seq::SliceRandom;
use rand::*;
use redis::Commands;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::SeqCst};
use std::thread::{self, JoinHandle};
use utils::{micros_since_epoch, Duration, Instant};
use uuid::Uuid;

static HALT: AtomicBool = AtomicBool::new(false);
pub(crate) static MAX_LATENCY_READ: AtomicU64 = AtomicU64::new(0);
pub(crate) static MAX_LATENCY_WRITE: AtomicU64 = AtomicU64::new(0);

pub struct ValkeyQueryEvent {
	pub tid: u64,
	//pub port: u64,
	pub write: bool,
	pub timestamp: u64,
	pub latency_us: u64,
}

fn new_key() -> String {
	Uuid::new_v4().into()
}

fn new_value() -> Vec<u8> {
	let mut data = vec![0u8; 1024];
	let mut rng = thread_rng();
	for i in data.iter_mut() {
		*i = rng.gen();
	}
	data
}

fn load(port: u64, tx: Sender<Event>) {
	let tid = gettid::gettid();

	let addr = format!("redis://127.0.0.1:{}/", port);
	let client = redis::Client::open(addr.as_str()).unwrap();
	let mut con = client.get_connection().unwrap();
	let keys: Vec<_> = (0..10000).map(|_| new_key()).collect();
	let value = new_value();

	// populate
	for key in keys.iter() {
		con.set::<&[u8], &[u8], ()>(key.as_bytes(), value.as_slice())
			.unwrap();
	}

	let mut rng = thread_rng();
	//let mut counter = 0;
	let mut batch = Vec::new();
	loop {
		if HALT.load(SeqCst) {
			break;
		}
		let key = keys.choose(&mut rng).unwrap();
		let write: bool = rng.gen::<f64>() < 0.1f64;

		let now = Instant::now();
		let timestamp = micros_since_epoch();
		let latency_us = if write {
			let _ =
				con.set::<&[u8], &[u8], ()>(key.as_bytes(), value.as_slice());
			let latency_us = now.elapsed().as_micros() as u64;
			MAX_LATENCY_WRITE.fetch_max(latency_us, SeqCst);
			latency_us
		} else {
			let _ = con.get::<&[u8], Vec<u8>>(key.as_bytes()); //.unwrap();
			let latency_us = now.elapsed().as_micros() as u64;
			MAX_LATENCY_READ.fetch_max(latency_us, SeqCst);
			latency_us
		};

		let event = ValkeyQueryEvent {
			tid,
			//port,
			timestamp,
			write,
			latency_us,
		};
		batch.push(event);
		if batch.len() >= 1024 {
			let records = valkey_slice_to_records(batch.as_slice());
			let _ = tx.send(Event::Valkey(records));
			batch.clear();
		}
	}
}

fn init_valkey(port: &str) -> Child {
	Command::new("taskset")
		.arg("-c")
		.arg("0-20")
		.arg("/home/fsolleza/Downloads/valkey-test/valkey/src/valkey-server")
		.arg("/home/fsolleza/Downloads/valkey-test/valkey/valkey.conf")
		.arg("--port")
		.arg(port)
		.stdout(Stdio::piped())
		.spawn()
		.unwrap()
}

pub struct ValkeyWorkload {
	valkey_procs: Vec<Child>,
	load_handles: Vec<JoinHandle<()>>,
}

impl ValkeyWorkload {
	pub fn halt(&self) {
		HALT.store(true, SeqCst);
	}

	pub fn wait(mut self) {
		for h in self.load_handles.drain(..) {
			let _ = h.join();
		}
		for c in self.valkey_procs.iter_mut() {
			let _ = c.kill();
		}
	}
}

pub fn run_valkey_workload(
	instances: u64,
	threads_per_instance: u64,
	tx: Sender<Event>,
) -> ValkeyWorkload {
	let base_port: u64 = 6370;

	let mut valkey_procs = Vec::new();
	for i in 0..instances {
		let port = format!("{}", base_port + i);
		valkey_procs.push(init_valkey(&port));
	}

	thread::sleep(Duration::from_secs(5));
	let mut load_handles = Vec::new();
	for i in 0..instances {
		let port = base_port + i;
		for _ in 0..threads_per_instance {
			let tx = tx.clone();
			load_handles.push(thread::spawn(move || load(port, tx)));
		}
	}

	ValkeyWorkload {
		valkey_procs,
		load_handles,
	}
}
