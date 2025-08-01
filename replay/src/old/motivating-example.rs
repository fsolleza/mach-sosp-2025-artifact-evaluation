use num::cast::AsPrimitive;
use memmap2::{Mmap, Advice};
use std::{
	fs::File,
	thread,
	sync::atomic::{AtomicU64, AtomicBool, Ordering::SeqCst},
	io::Write,
};
use crossbeam::channel::{Sender, Receiver, bounded};
use utils::{Instant, Duration};
use clap::Parser;
use data::{
	kv_op::KVOp,
	record::{Record, RecordBatch},
};
use replay::{ init_replayer, start_replayer, START, WRITTEN, DONE, GENERATED, DROPPED};

fn counters() {
	while !START.load(SeqCst) {
		thread::sleep(Duration::from_secs(1));
	}
	let mut count = 0;
	loop {
		let written = WRITTEN.swap(0, SeqCst);
		let generated = GENERATED.swap(0, SeqCst);
		let dropped = DROPPED.swap(0, SeqCst);
		println!("{}, Generated: {}, Written: {}, Dropped: {}", count, generated, written, dropped);
		thread::sleep(Duration::from_secs(1));
		count += 1;
		if DONE.load(SeqCst) {
			break;
		}
	}
}

fn group_by_second<T>(data: &[T], ts_fn: fn(&T) -> u64) -> Vec<(u64, &[T])> {
	if data.len() == 0 {
		return Vec::new();
	}

	let mut windows = Vec::new();

	let mut curr_sec = ts_fn(&data[0]);
	let mut start_idx = 0;

	for (curr_idx, item) in data.iter().enumerate() {
		let item_sec = ts_fn(item);
		if item_sec > curr_sec {
			windows.push((curr_sec, &data[start_idx..curr_idx]));
			curr_sec = item_sec;
			start_idx = curr_idx;
		}
	}

	windows
}

fn merge<'a, L, Lv, R, Rv>(
	l_data: &'a [(u64, L)],
	r_data: &'a [(u64, R)],
	l_func: fn(&'a L) -> &'a Lv,
	r_func: fn(&'a R) -> &'a Rv,
) -> Vec<(u64, (Option<&'a Lv>, Option<&'a Rv>))> {
	let mut merged = Vec::new();
	let mut l_idx = 0;
	let mut r_idx = 0;
	loop {
		if l_idx < l_data.len() && r_idx < r_data.len() {
			let curr_ts = l_data[l_idx].0.min(r_data[r_idx].0);
			let l_ts = l_data[l_idx].0;
			let r_ts = r_data[r_idx].0;

			let l_value: Option<&Lv> = if l_ts == curr_ts {
				let idx = l_idx;
				l_idx += 1;
				Some(l_func(&l_data[idx].1))
			} else {
				None
			};

			let r_value: Option<&Rv> = if r_ts == curr_ts {
				let idx = r_idx;
				r_idx += 1;
				Some(r_func(&r_data[idx].1))
			} else {
				None
			};

			merged.push((curr_ts, (l_value, r_value)));
		} else if r_idx < r_data.len() {
			let ts = r_data[r_idx].0;
			let value: Option<&Rv> = Some(r_func(&r_data[r_idx].1));
			merged.push((ts, (None, value)));
			r_idx += 1;
		} else if l_idx < l_data.len() {
			let ts = l_data[l_idx].0;
			let value: Option<&Lv> = Some(l_func(&l_data[l_idx].1));
			merged.push((ts, (value, None)));
			l_idx += 1;
		} else {
			break;
		}
	}
	merged
}

fn window_stats<T>(
	windows: &[(u64, &[T])],
	func: fn(&T) -> u64,
) -> Vec<(u64, (u64, u64, u64))> {
	let mut stats = Vec::new();
	let mut qtile_buffer = Vec::new();
	for (ts, window) in windows {
		let count = window.len() as u64;
		let mut sum = 0u64;
		qtile_buffer.clear();
		for item in window.iter() {
			let v = func(item);
			sum += v;
			qtile_buffer.push(v);
		}
		qtile_buffer.sort();
		let idx = (qtile_buffer.len() as f64 * 0.9999).floor() as usize;
		let qtile = qtile_buffer[idx];
		stats.push((*ts, (count, sum, qtile)));
	}
	stats
}

fn proxy_receiver(rx: Receiver<RecordBatch>, tx: Sender<Vec<RecordBatch>>, rate_limit: Option<u64>) {
	let mut now = Instant::now();
	let mut count = 0;
	let second = Duration::from_secs(1);
	let mut batches = Vec::new();
	while let Ok(item) = rx.recv() {
		let l = item.data.len() as u64;
		batches.push(item);
		WRITTEN.fetch_add(l, SeqCst);
		count += l;
		if let Some(rate) = rate_limit {
			if count >= rate && now.elapsed() < second {
				while now.elapsed() < second {}
				now = Instant::now();
				count = 0;
			}
		}
	}
	tx.send(batches).unwrap();
}

struct AnalysisData {
	kv_logs: Vec<(u64, u64)>,
	system_calls: Vec<(u64, u64)>,
	page_cache_events: Vec<(u64, u8)>,
}

fn parse_batches(batches: Vec<RecordBatch>) -> AnalysisData {
	let mut kv_logs: Vec<(u64, u64)> = Vec::new();
	let mut system_calls: Vec<(u64, u64)> = Vec::new();
	let mut page_cache_events: Vec<(u64, u8)> = Vec::new();

	for batch in batches {
		let arrival_us = batch.arrival_us;
		for record in batch.data {
			// Handle KV logs by collecting reads
			if let Some(kv_log) = record.as_kvlog() {
				let dur = kv_log.duration_us;
				match kv_log.op {
					KVOp::Read => {
						kv_logs.push((arrival_us, dur));
					}
					_ => {}
				}
			}

			// Handle system calls by collecting reads
			if let Some(syscall) = record.as_syscall_event() {
				let dur = syscall.duration_us;
				system_calls.push((arrival_us, dur));
			}

			// Handle page count
			if let Some(page_cache_event) = record.as_page_cache_event() {
				page_cache_events.push((arrival_us, 1));
			}
		}
	}
	AnalysisData { kv_logs, system_calls, page_cache_events, }
}

fn analyze_p3(data: AnalysisData, out_file: &str) {
	let AnalysisData { kv_logs, system_calls, page_cache_events } = data;

	println!("Windowing");
	fn sec_fn1<T>(item: &(u64, T)) -> u64 {
		item.0 / 1_000_000
	}
	let kv_log_windows = group_by_second(&kv_logs, sec_fn1);
	println!("KV log window length: {}", kv_log_windows[10].1.len());
	let syscall_windows = group_by_second(&system_calls, sec_fn1);
	println!("Syscall window length: {}", syscall_windows[10].1.len());
	let page_cache_windows = group_by_second(&page_cache_events, sec_fn1);

	println!("Window Stats");
	fn window_val_fn<T: AsPrimitive<u64>>(item: &(u64, T)) -> u64 {
		item.1.as_()
	}

	let kv_log_stats = window_stats(&kv_log_windows, window_val_fn);
	println!("Kv log stat {:?}", kv_log_stats[10]);
	let syscall_stats = window_stats(&syscall_windows, window_val_fn);
	println!("syscall stat {:?}", syscall_stats[10]);
	let page_cache_stats = window_stats(&page_cache_windows, window_val_fn);

	println!("Merging");
	let l_data = kv_log_stats.as_slice();
	let r_data = syscall_stats.as_slice();

	fn ident_fn<T>(data: &T) -> &T {
		data
	}
	let merged = merge(l_data, r_data, ident_fn, ident_fn);


	let merged = merge(&merged, &page_cache_stats, ident_fn, ident_fn);

	let mut results = String::new();
	results.push_str("timestamp,count,request_tail_latency,system_call_tail_latency,page_cache_event_count\n");
	for (ts, (v, page)) in merged {
		let mut count = 0;
		let (kv, sys) = match v {
			Some((kv, sys)) => (kv, sys),
			None => (&None, &None),
		};

		let kv = match kv {
			Some(x) => {
				count += x.0;
				format!("{}", x.2)
			},
			None => String::from(""),
		};
		let sys = match sys {
			Some(x) => {
				count += x.0;
				format!("{}", x.2)
			},
			None => String::from(""),
		};
		let page = match page {
			Some(x) => {
				count += x.0;
				format!("{}", x.0)
			},
			None => String::from(""),
		};
		let count  = format!("{}", count);
		let row = format!("{},{},{},{},{}\n", ts, count, kv, sys, page);
		results.push_str(&row);
	}

	println!("Writing to output file: {}", out_file);
	let mut f = File::create(out_file).unwrap();
	f.write_all(results.as_bytes()).unwrap();
}

fn analyze_p2(data: AnalysisData, out_file: &str) {
	let AnalysisData { kv_logs, system_calls, page_cache_events } = data;

	println!("Windowing");
	fn sec_fn1<T>(item: &(u64, T)) -> u64 {
		item.0 / 1_000_000
	}
	let kv_log_windows = group_by_second(&kv_logs, sec_fn1);
	let syscall_windows = group_by_second(&system_calls, sec_fn1);

	println!("Window Stats");
	fn window_val_fn<T: AsPrimitive<u64>>(item: &(u64, T)) -> u64 {
		item.1.as_()
	}

	let kv_log_stats = window_stats(&kv_log_windows, window_val_fn);
	let syscall_stats = window_stats(&syscall_windows, window_val_fn);

	println!("Merging");
	let l_data = kv_log_stats.as_slice();
	let r_data = syscall_stats.as_slice();

	fn merge_fn(data: &(u64, u64, u64)) -> &(u64, u64, u64) {
		data
	}
	let merged = merge(l_data, r_data, merge_fn, merge_fn);

	let mut results = String::new();
	results.push_str("timestamp,count,request_tail_latency,system_call_tail_latency\n");
	for (ts, (kv, sys)) in merged {
		let mut count = 0;
		let kv = match kv {
			Some(x) => {
				count += x.0;
				format!("{}", x.2)
			},
			None => String::from(""),
		};
		let sys = match sys {
			Some(x) => {
				count += x.0;
				format!("{}", x.2)
			},
			None => String::from(""),
		};
		let count = format!("{}", count);
		let row = format!("{},{},{},{}\n", ts, count, kv, sys);
		results.push_str(&row);
	}

	println!("Writing to output file: {}", out_file);
	let mut f = File::create(out_file).unwrap();
	f.write_all(results.as_bytes()).unwrap();
}

fn analyze_p1(data: AnalysisData, out_file: &str) {
	let AnalysisData { kv_logs, system_calls, page_cache_events } = data;

	println!("Windowing");
	fn sec_fn1<T>(item: &(u64, T)) -> u64 {
		item.0 / 1_000_000
	}
	let kv_log_windows = group_by_second(&kv_logs, sec_fn1);
	println!("kv_log_window len: {}", kv_log_windows[10].1.len());

	println!("Window Stats");
	fn window_val_fn<T: AsPrimitive<u64>>(item: &(u64, T)) -> u64 {
		item.1.as_()
	}

	let kv_log_stats = window_stats(&kv_log_windows, window_val_fn);
	println!("kv_log_window len: {:?}", &kv_log_stats[10]);

	let mut results = String::new();
	results.push_str("timestamp,count,request_tail_latency\n");
	for (ts, kv) in kv_log_stats {
		let row = format!("{},{},{}\n",ts,kv.0, kv.2);
		results.push_str(&row);
	}

	println!("Writing to output file: {}", out_file);
	let mut f = File::create(out_file).unwrap();
	f.write_all(results.as_bytes()).unwrap();
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
	#[arg(short, long)]
	file_path: String,

	/// Rate limit in records per second
	#[arg(short, long)]
	rate_limit: Option<u64>,

	/// Where to output analysis results files
	#[arg(short, long)]
	phase: u64,

	/// Where to output analysis results files
	#[arg(short, long)]
	output: String,
}

fn main() {

	let args = Args::parse();
	let file_path = args.file_path.clone();
	let rate_limit = args.rate_limit.clone();
	let output = args.output.clone();
	let phase = match args.phase {
		1..4 => args.phase,
		0 => panic!("Wrong phase"),
		4.. => panic!("Wrong phase"),
	};

	let rx = init_replayer(&file_path, 1024);

	let (atx, arx) = bounded(1);
	let h = thread::spawn(move || {
		proxy_receiver(rx, atx, rate_limit);
	});

	thread::spawn(counters);
	start_replayer();

	let data = arx.recv().unwrap();
	let data = parse_batches(data);

	match phase {
		3 => analyze_p3(data, &output),
		2 => analyze_p2(data, &output),
		1 => analyze_p1(data, &output),
		_ => unreachable!(),
	}
}
