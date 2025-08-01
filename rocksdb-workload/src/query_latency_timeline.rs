use clap::Parser;
use data::{
	kv_op::KVOp,
	record::{Record, RecordBatch},
};
use std::collections::HashMap;
use std::fs::*;
use std::io::prelude::*;
use num::cast::AsPrimitive;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
	#[arg(short, long)]
	file_path: String,
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

fn main() {
	let args = Args::parse();

	println!("Reading file");
	let mut file = File::open(&args.file_path).unwrap();
	let mut bytes = Vec::new();
	file.read_to_end(&mut bytes).unwrap();

	let mut offset = 0;
	let mut kv_logs: Vec<(u64, u64)> = Vec::new();
	let mut system_calls: Vec<(u64, u64)> = Vec::new();
	let mut page_cache_events: Vec<(u64, u8)> = Vec::new();

	println!("Parsing file");
	while offset < bytes.len() {
		let (batch, sz) = RecordBatch::from_bytes(&bytes[offset..]);
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
				if syscall.syscall_number == 17 {
					system_calls.push((arrival_us, dur));
				}
			}

			// Handle page count
			if let Some(page_cache_event) = record.as_page_cache_event() {
				page_cache_events.push((arrival_us, 1));
			}
		}
		offset += sz;
	}


	println!("Windowing");
	fn sec_fn1<T>(item: &(u64, T)) -> u64 {
		item.0 / 1_000_000
	}
	let kv_log_windows = group_by_second(&kv_logs, sec_fn1);
	let syscall_windows = group_by_second(&system_calls, sec_fn1);
	let page_cache_windows = group_by_second(&page_cache_events, sec_fn1);


	println!("Window Stats");
	fn window_val_fn<T: AsPrimitive<u64>>(item: &(u64, T)) -> u64 {
		item.1.as_()
	}

	let kv_log_stats = window_stats(&kv_log_windows, window_val_fn);
	let syscall_stats = window_stats(&syscall_windows, window_val_fn);
	let page_cache_stats = window_stats(&page_cache_windows, window_val_fn);

	println!("Merging");
	let l_data = kv_log_stats.as_slice();
	let r_data = syscall_stats.as_slice();

	fn merge_fn(data: &(u64, u64, u64)) -> &u64 {
		&data.2
	}
	let merged = merge(l_data, r_data, merge_fn, merge_fn);

	fn ident_fn<T>(data: &T) -> &T {
		data
	}
	fn merge_fn2(data: &(u64, u64, u64)) -> &u64 {
		&data.0
	}

	let merged = merge(&merged, &page_cache_stats, ident_fn, merge_fn2);

	for (ts, (v, page)) in merged {
		let (kv, sys) = match v {
			Some((kv, sys)) => (kv, sys),
			None => (&None, &None),
		};

		let kv = match kv {
			Some(x) => format!("{}", x),
			None => String::from(""),
		};
		let sys = match sys {
			Some(x) => format!("{}", x),
			None => String::from(""),
		};
		let page = match page {
			Some(x) => format!("{}", x),
			None => String::from(""),
		};
		println!("{},{},{},{},", ts, kv, sys, page);
	}
}
