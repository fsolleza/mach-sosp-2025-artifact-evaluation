use crate::SourceKind;
use crate::{DONE, DROPPED};
use crossbeam::channel::Sender;
use data::{kv_op::*, record::*};
use rand::prelude::*;
use rocksdb::{DBWithThreadMode, MultiThreaded, WriteOptions};
use std::{
	path::PathBuf,
	sync::{
		atomic::{AtomicBool, AtomicU64, Ordering::SeqCst},
		Arc,
	},
	thread,
};
use utils::{micros_since_epoch, Duration, Instant};

type DB = Arc<DBWithThreadMode<MultiThreaded>>;

pub static ROCKSDB_STARTED: AtomicBool = AtomicBool::new(false);
pub static COUNT: AtomicU64 = AtomicU64::new(0);
//pub static DROPPED: AtomicU64 = AtomicU64::new(0);

fn random_idx_bounds(l: usize, rng: &mut ThreadRng) -> (usize, usize) {
	let s = 128;
	let start: usize = rng.gen_range(0..l - s);
	(start, start + s)
}

fn do_read(db: &DB, key: u64) -> Option<Vec<u8>> {
	let res = db.get_pinned(key.to_be_bytes()).unwrap()?;
	Some((*res).into())
}

fn do_write(db: &DB, key: u64, slice: &[u8]) {
	let mut opt = WriteOptions::default();
	opt.set_sync(false);
	db.put_opt(key.to_be_bytes(), slice, &opt).unwrap();
}

fn do_work(
	db: DB,
	read_ratio: f64,
	min_key: u64,
	max_key: u64,
	_max_read_key: u64,
	tid: u64,
	data: Arc<[u8]>,
	out: Sender<(SourceKind, Vec<Record>)>,
) {
	let mut rng = thread_rng();

	let mut records = Vec::new();

	//let mut new_id = max_key;

	// let mut iter = max_key;
	// let mut counter = 0;
	// let start = Instant::now();
	loop {
		// counter += 1;
		if DONE.load(SeqCst) {
			break;
		}

		let l = records.len() as u64;
		if l == 1024 {
			COUNT.fetch_add(l, SeqCst);
			if out.try_send((SourceKind::Application, records)).is_err() {
				DROPPED.fetch_add(l as usize, SeqCst);
			}
			records = Vec::new();
		}

		let read = rng.gen::<f64>() < read_ratio;
		let key = if read {
			rng.gen_range(min_key..128 * 1024)
		} else {
			rng.gen_range(min_key..max_key)
		};

		let now = Instant::now();
		if read {
			if let Some(_sz) = do_read(&db, key) {
				let duration_us = now.elapsed().as_micros() as u64;
				let timestamp = micros_since_epoch();

				records.push(Record::from_kvlog(KVLog {
					op: KVOp::Read,
					tid,
					timestamp,
					duration_us,
				}));
				continue;
			}
		}

		let bounds = random_idx_bounds(data.len(), &mut rng);
		let slice = &data[bounds.0..bounds.1];
		do_write(&db, key, slice);
		let duration_us = now.elapsed().as_micros() as u64;
		let timestamp = micros_since_epoch();

		records.push(Record::from_kvlog(KVLog {
			op: KVOp::Write,
			tid,
			timestamp,
			duration_us,
		}));
	}

	let l = records.len() as u64;
	if l > 0 {
		if out.try_send((SourceKind::Application, records)).is_err() {
			DROPPED.fetch_add(l as usize, SeqCst);
		}
	}
}

fn setup_db(path: PathBuf) -> DB {
	let _ = std::fs::remove_dir_all(&path);
	//let mut opts = Options::default();
	//opts.set_write_buffer_size(1024 * 1024);
	//opts.create_if_missing(true);
	//let db = DBWithThreadMode::<MultiThreaded>::open(&opts, path).unwrap();

	let db = DBWithThreadMode::<MultiThreaded>::open_default(path).unwrap();
	//let mut opt = WriteOptions::default();
	//opt.set_sync(false);
	//opt.disable_wal(false);
	Arc::new(db)
}

fn load_db(db: DB, min_key: u64, max_key: u64, data: &[u8]) {
	let mut rng = thread_rng();
	for k in min_key..max_key {
		let bounds = random_idx_bounds(data.len(), &mut rng);
		let slice = &data[bounds.0..bounds.1];
		do_write(&db, k, slice);
	}
}

fn random_data(sz: usize) -> Arc<[u8]> {
	let mut data: Vec<u8> = vec![0u8; sz];
	let mut rng = thread_rng();
	for d in &mut data {
		*d = rng.gen();
	}
	data.into()
}

pub fn run_workload(
	n_instances: u64,
	threads_per_instance: u64,
	read_ratio: f64,
	key_low: u64,
	key_high: u64,
	key_max_read: u64,
	db_path: PathBuf,
	out: Sender<(SourceKind, Vec<Record>)>,
) {
	let _ = std::fs::remove_dir_all(&db_path);
	let data = random_data(4096);

	let mut instances = Vec::new();
	for i in 0..n_instances {
		let db_path = db_path.clone();
		let path = db_path.join(format!("{}", i));
		let db = setup_db(path);
		println!("Loading DB {}", i);
		load_db(db.clone(), key_low, key_high, &data);
		println!("Done loading db: {}", i);
		instances.push(db);
	}

	let mut handles = Vec::new();
	let mut core = 0;
	for db in instances {
		//let this_db = db.clone();
		let out = out.clone();
		let data = data.clone();
		for _ in 0..threads_per_instance {
			let db = db.clone();
			let out = out.clone();
			let data = data.clone();
			let this_core = core;
			core += 1;
			let h = thread::spawn(move || {
				let _ = core_affinity::set_for_current(core_affinity::CoreId {
					id: this_core,
				});
				let tid = gettid::gettid();
				do_work(
					db.clone(),
					read_ratio,
					key_low,
					key_high,
					key_max_read,
					tid,
					data,
					out.clone(),
				);
			});
			handles.push(h);
		}
	}

	thread::sleep(Duration::from_secs(1));
	ROCKSDB_STARTED.store(true, SeqCst);
	while !DONE.load(SeqCst) {
		thread::sleep(Duration::from_secs(1));
	}
}
