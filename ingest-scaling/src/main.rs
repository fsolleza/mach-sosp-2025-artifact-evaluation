use clap::{Parser, ValueEnum};
use heed::{types::*, Database, EnvFlags, EnvOpenOptions, PutFlags};
use rand::Rng;
use rocksdb::{
	ColumnFamilyDescriptor, DBWithThreadMode, Env, Options, SingleThreaded,
	WriteBatch, WriteOptions,
};
use std::fs::{create_dir_all, remove_dir_all};
use std::sync::{
	atomic::{AtomicUsize, Ordering::SeqCst},
	Arc,
};
use std::thread;
use utils::{Duration, Instant};

const ROCKSDB_PARALLELISM: i32 = 72;
const BATCH_SIZE: u64 = 1024;
const SYNC_INTERVAL: Duration = Duration::from_secs(1);
const TARGET_TIME: Duration = Duration::from_secs(60);

fn generate_random_data(size: usize) -> Vec<u8> {
	let mut v = Vec::new();
	let mut rng = rand::thread_rng();
	for _ in 0..size {
		v.push(rng.gen::<u8>());
	}
	v
}

//fn mach_worker_load(mut mach: mach_lib::Partitions, data: &[u8], total_counter: Arc<AtomicUsize>) {
//	let mut count = 0;
//
//	let now = Instant::now();
//	let last_sync = now.elapsed();
//	while now.elapsed() < TARGET_TIME {
//		mach.push(0, 0, data);
//		count += 1;
//		if count % BATCH_SIZE == 0 {
//			mach.sync(0, 0);
//		}
//	}
//	mach.sync(0, 0);
//
//	let elapsed = now.elapsed();
//	println!("Now elapsed: {:?}, count: {count}", elapsed);
//	// let size = count * random_data.len() as u64;
//	// let bytes_tp = size as f64 / elapsed.as_secs_f64();
//	// let count_tp = count as f64 / elapsed.as_secs_f64();
//	// println!("count tp: {count_tp}, bytes rate: {bytes_tp}");
//
//	total_counter.fetch_add(count as usize, SeqCst);
//}
//
//fn mach_load(args: Args) {
//	let threads = 2;
//
//	let data_size = args.data_size;
//	let storage_dir = args.storage_dir.clone();
//	let _ = remove_dir_all(&storage_dir);
//	create_dir_all(&storage_dir).unwrap();
//
//	let mut instances = Vec::new();
//	for i in 0..threads {
//		let mut path = PathBuf::from(&storage_dir);
//		let path = path.join(&format!("thread-instance-{}", i));
//		println!("Mach path: {:?}", path);
//		let mut mach = mach_lib::Partitions::new(path.into());
//		mach.define_source(0, 0);
//		instances.push(mach);
//	}
//
//	let random_data = Arc::new(generate_random_data(data_size as usize));
//	let total_count = Arc::new(AtomicUsize::new(0));
//
//	let mut handles = Vec::new();
//	for p in instances {
//		let random_data = random_data.clone();
//		let total_count = total_count.clone();
//		handles.push(thread::spawn(move || {
//			mach_worker_load(p, &random_data, total_count);
//		}));
//	}
//
//	for h in handles {
//		h.join();
//	}
//
//
//	let count = total_count.load(SeqCst);
//	let size = count * random_data.len();
//	let bytes_tp = size as f64 / 20.0;
//	let count_tp = count as f64 / 20.0;
//	println!("count tp: {count_tp}, bytes rate: {bytes_tp}");
//
//}

fn mach_load(args: Args) {
	let storage_dir = args.storage_dir.clone();
	let data_size = args.data_size;

	println!("Dir: {}", storage_dir);
	let _ = remove_dir_all(&storage_dir);
	create_dir_all(&storage_dir).unwrap();

	let mut mach = mach_lib::Partitions::new(storage_dir.into());
	mach.define_source(0, 0);

	let random_data = generate_random_data(data_size as usize);
	let mut count = 0u64;

	let now = Instant::now();
	//let last_sync = now.elapsed();
	while now.elapsed() < TARGET_TIME {
		mach.push(0, 0, random_data.as_slice());
		count += 1;
		if count % BATCH_SIZE == 0 {
			mach.sync(0, 0);
		}
	}
	mach.sync(0, 0);

	let elapsed = now.elapsed();
	let size = count * random_data.len() as u64;
	let bytes_tp = size as f64 / elapsed.as_secs_f64();
	let count_tp = count as f64 / elapsed.as_secs_f64();
	println!("count tp: {count_tp}, bytes rate: {bytes_tp}");
}

fn rocksdb_worker(
	db: Arc<DBWithThreadMode<SingleThreaded>>,
	random_data: &[u8],
	total_count: Arc<AtomicUsize>,
	tid: u64,
) {
	let cf = db.cf_handle(&format!("cf{}", tid)).unwrap();

	let mut batch = WriteBatch::default();
	let mut write_options = WriteOptions::default();
	write_options.disable_wal(true);
	let mut count = 0;
	let mut key = 0u64;
	let now = Instant::now();
	let mut last_sync = now;

	while now.elapsed() < TARGET_TIME {
		let mut key_bytes = [0u8; 16];
		key_bytes[..8].copy_from_slice(&key.to_be_bytes());
		key_bytes[8..16].copy_from_slice(&tid.to_be_bytes());

		batch.put_cf(&cf, &key_bytes, &random_data[..]);
		count += 1;
		key += 1;

		if count % BATCH_SIZE == 0 {
			db.write_opt(batch, &write_options).unwrap();
			batch = WriteBatch::default();
		}

		if last_sync.elapsed() > SYNC_INTERVAL {
			db.flush().unwrap();
			last_sync = Instant::now();
		}
	}

	// Flush the last batch
	if !batch.is_empty() {
		db.write_opt(batch, &write_options).unwrap();
	}
	db.flush().unwrap();

	println!("Elapsed: {:?}, count: {count}", now.elapsed());
	total_count.fetch_add(count as usize, SeqCst);
}

fn rocksdb_load(args: Args) {
	let threads = args.threads;
	println!("Rocksdb ingest scaling with {} threads", threads);
	let path = args.storage_dir.clone();
	let data_size = args.data_size;
	let _ = remove_dir_all(&path);
	create_dir_all(&path).unwrap();

	let mut opts = Options::default();
	let env = Env::new().unwrap();

	opts.set_env(&env);
	opts.increase_parallelism(ROCKSDB_PARALLELISM);
	opts.set_max_background_jobs(16);
	opts.create_if_missing(true);
	opts.set_write_buffer_size(2 * 1024 * 1024 * 1024);
	opts.create_missing_column_families(true);
	opts.create_if_missing(true);

	let random_data = Arc::new(generate_random_data(data_size as usize));
	let total_count = Arc::new(AtomicUsize::new(0));

	let mut cfs = Vec::new();
	for i in 0..threads {
		let cf_opts = Options::default();
		let cf = ColumnFamilyDescriptor::new(format!("cf{}", i), cf_opts);
		cfs.push(cf);
	}
	let db = Arc::new(
		DBWithThreadMode::<SingleThreaded>::open_cf_descriptors(
			&opts, &path, cfs,
		)
		.unwrap(),
	);

	let mut handles = Vec::new();
	for i in 0..threads {
		let total_count = total_count.clone();
		let random_data = random_data.clone();
		let db = db.clone();
		let h = thread::spawn(move || {
			rocksdb_worker(db.clone(), &*random_data, total_count.clone(), i);
		});
		handles.push(h);
	}

	for h in handles {
		let _ = h.join();
	}

	let count = total_count.load(SeqCst);
	let size = count * random_data.len();
	let bytes_tp = size as f64 / TARGET_TIME.as_secs_f64();
	let count_tp = count as f64 / TARGET_TIME.as_secs_f64();
	println!("count tp: {count_tp}, bytes rate: {bytes_tp}");
}

fn lmdb_load(args: Args) {
	let storage_dir = args.storage_dir.clone();
	let _ = remove_dir_all(&storage_dir);
	create_dir_all(&storage_dir).unwrap();
	let data_size = args.data_size;

	let env = unsafe {
		EnvOpenOptions::new()
			.map_size(256 * 1024 * 1024 * 1024)
			.flags(EnvFlags::NO_SYNC)
			.open(storage_dir)
			.unwrap()
	};
	let mut wtxn = env.write_txn().unwrap();
	let db: Database<Bytes, Bytes> =
		env.create_database(&mut wtxn, None).unwrap();

	let mut key = 0u64;
	let mut count = 0;
	let random_data = generate_random_data(data_size as usize);

	let now = Instant::now();
	while now.elapsed() < TARGET_TIME {
		key += 1;
		count += 1;

		db.put_with_flags(
			&mut wtxn,
			PutFlags::APPEND,
			&key.to_be_bytes(),
			random_data.as_slice(),
		)
		.unwrap();

		if count % BATCH_SIZE == 0 {
			wtxn.commit().unwrap();
			wtxn = env.write_txn().unwrap();
			//db = env.create_database(&mut wtxn, None).unwrap();
		}
	}

	if count % BATCH_SIZE != 0 {
		wtxn.commit().unwrap();
	}

	let elapsed = now.elapsed();
	let size = count * random_data.len() as u64;
	let bytes_tp = size as f64 / elapsed.as_secs_f64();
	let count_tp = count as f64 / elapsed.as_secs_f64();
	println!("count tp: {count_tp}, bytes rate: {bytes_tp}");
}

#[derive(Copy, Clone, Debug, ValueEnum, Eq, PartialEq)]
#[allow(non_camel_case_types)]
enum Db {
	rocksdb,
	lmdb,
	mach,
}

#[derive(Parser, Debug, Clone)]
#[command(version, about, long_about = None)]
struct Args {
	/// What storage is being benchmarked (lmdb, rocksdb, tsl)
	#[arg(short, long)]
	db: Db,

	/// Where mach/rocksdb/lsm tree will write to
	#[arg(short, long)]
	storage_dir: String,

	/// size of data written
	#[arg(short, long, default_value_t = 1)]
	data_size: u64,

	#[arg(short, long, default_value_t = 1)]
	threads: u64,
}

fn main() {
	let args = Args::parse();
	println!("Writing for {:?}", TARGET_TIME);
	match args.db {
		Db::rocksdb => rocksdb_load(args),
		Db::lmdb => lmdb_load(args),
		Db::mach => mach_load(args),
	}
}
