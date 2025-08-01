use clap::Parser;
use data::{
	kv_op::KVOp,
	record::{Record, RecordBatchesDeserializer, RecordKind},
	record_bytes::{RecordBatchBytes, RecordBytes},
};
use std::{fs::File, io::prelude::*};
use utils::HashMap;
use utils::Instant;

fn calc_stats(durations: &mut [u64]) {
	durations.sort();
	let d = &durations;
	let min = d[0];
	let max = d.last().unwrap();
	let cnt = d.len() as u64;
	let sum: u64 = d.iter().sum();
	let avg = sum / cnt;

	let p10 = (d.len() as f64 * 0.10).floor() as usize;
	let p50 = (d.len() as f64 * 0.50).floor() as usize;
	let p90 = (d.len() as f64 * 0.90).floor() as usize;
	let p95 = (d.len() as f64 * 0.95).floor() as usize;
	let p99 = (d.len() as f64 * 0.99).floor() as usize;
	let p9999 = (d.len() as f64 * 0.9999).floor() as usize;

	println!("Min: {} Max: {} Cnt: {} Average: {} p10: {} p50: {} p90: {} p95: {} p99: {} p9999: {}",
			 min, max, cnt, avg,
			 d[p10],
			 d[p50],
			 d[p90],
			 d[p95],
			 d[p99],
			 d[p9999],
			 );
}

fn kvlog_dur_stats(mut deserializer: RecordBatchesDeserializer) {
	println!("Collecting stats");
	let mut write_durations = Vec::new();
	let mut read_durations = Vec::new();
	let mut total_count = 0;
	while let Some(bytes) = deserializer.next_batch_bytes() {
		let batch = RecordBatchBytes::new(bytes);
		let mut iterator = batch.iterator();
		while let Some(record_bytes) = iterator.next_record_bytes() {
			total_count += 1;
			let (record, sz) = Record::from_bytes(record_bytes.bytes);
			assert_eq!(sz, record_bytes.bytes.len());
			assert_eq!(record.source_id(), record.source_information().as_id());
			if let Some(x) = record_bytes.as_kv_log() {
				match x.op {
					KVOp::Read => read_durations.push(x.duration_us),
					KVOp::Write => write_durations.push(x.duration_us),
				}
			}
		}
	}
	println!("Done");
	println!("KV Reads");
	println!("Total count: {total_count}");
	calc_stats(&mut read_durations[..]);

	println!("KV Writes");
	calc_stats(&mut write_durations[..]);
}

fn syscall_dur_stats(mut deserializer: RecordBatchesDeserializer) {
	println!("Collecting stats");
	let mut total_records = 0;
	let mut durations = HashMap::new();
	while let Some(bytes) = deserializer.next_batch_bytes() {
		let batch = RecordBatchBytes::new(bytes);
		let mut iterator = batch.iterator();
		while let Some(record_bytes) = iterator.next_record_bytes() {
			total_records += 1;
			let sysnum = record_bytes.syscall_event_syscall_number();
			let dur = record_bytes.syscall_event_duration_us();
			match (sysnum, dur) {
				(Some(s), Some(d)) => {
					let v = durations.entry(s).or_insert_with(Vec::new);
					v.push(d);
				}
				_ => {}
			};
		}
	}
	println!("Done");

	println!("Total records: {total_records}");
	for (s, mut d) in durations.drain() {
		d.sort();
		let min = d[0];
		let max = d.last().unwrap();
		let cnt = d.len() as u64;
		let sum: u64 = d.iter().sum();
		let avg = sum / cnt;

		let p10 = (d.len() as f64 * 0.10).floor() as usize;
		let p50 = (d.len() as f64 * 0.50).floor() as usize;
		let p90 = (d.len() as f64 * 0.90).floor() as usize;
		let p95 = (d.len() as f64 * 0.95).floor() as usize;
		let p99 = (d.len() as f64 * 0.99).floor() as usize;
		let p9999 = (d.len() as f64 * 0.9999).floor() as usize;

		println!("Syscall number: {} Cnt: {} Min: {} Max: {} Cnt: {} Average: {} p10: {} p50: {} p90: {} p95: {} p99: {} p9999: {}",
				 s, cnt, min, max, cnt, avg,
				 d[p10],
				 d[p50],
				 d[p90],
				 d[p95],
				 d[p99],
				 d[p9999],
		);
	}
}

fn page_cache_count(mut deserializer: RecordBatchesDeserializer) {
	println!("Collecting stats");
	let mut total_records = 0;
	let mut page_cache = 0;
	while let Some(bytes) = deserializer.next_batch_bytes() {
		let batch = RecordBatchBytes::new(bytes);
		let mut iterator = batch.iterator();
		while let Some(record_bytes) = iterator.next_record_bytes() {
			total_records += 1;
			if record_bytes.is_page_cache_event() {
				page_cache += 1;
			}
		}
	}
	println!("Done");
	println!("Total records: {total_records}, Page cache count: {page_cache}");
}

fn mangled_packet_count(mut deserializer: RecordBatchesDeserializer) {
	println!("Collecting stats");
	let mut total_records = 0;
	let mut mangled_packet_count = 0;
	while let Some(bytes) = deserializer.next_batch_bytes() {
		let batch = RecordBatchBytes::new(bytes);
		let mut iterator = batch.iterator();
		while let Some(record_bytes) = iterator.next_record_bytes() {
			total_records += 1;
			if let Some(x) = record_bytes.as_packet_capture() {
				if x.dst_port == 5555 {
					mangled_packet_count += 1;
				}
			}
		}
	}
	println!("Done");
	println!("Total records: {total_records}, manged packet count: {mangled_packet_count}");
}

fn data_summary(mut deserializer: RecordBatchesDeserializer) {
	let mut kv_events = 0;
	let mut syscall_events = 0;
	let mut page_cache_events = 0;
	let mut tcp_events = 0;

	let mut start_time = u64::MAX;
	let now = Instant::now();
	while let Some(bytes) = deserializer.next_batch_bytes() {
		let batch = RecordBatchBytes::new(bytes);
		let arrival_us = batch.arrival_us();
		if start_time == u64::MAX {
			start_time = arrival_us;
		}
		let mut iterator = batch.iterator();

		while let Some(record_bytes) = iterator.next_record_bytes() {
			match record_bytes.kind() {
				RecordKind::KVLog => kv_events += 1,
				RecordKind::SyscallEvent => syscall_events += 1,
				RecordKind::PageCacheEvent => page_cache_events += 1,
				RecordKind::PacketCapture => tcp_events += 1,
			}
		}

		let d: u64 = arrival_us - start_time;
		while (now.elapsed().as_micros() as u64) < d {}
	}
	let d = now.elapsed();

	let kv_events_per_sec = kv_events as f64 / d.as_secs_f64();
	let syscall_events_per_sec = syscall_events as f64 / d.as_secs_f64();
	let page_cache_events_per_sec = page_cache_events as f64 / d.as_secs_f64();
	let tcp_events_per_sec = tcp_events as f64 / d.as_secs_f64();
	println!("Kv {kv_events_per_sec} Syscall {syscall_events_per_sec} Page cache {page_cache_events_per_sec} Tcp {tcp_events_per_sec}");
}

fn data_to_csv(out_path: &str, mut deserializer: RecordBatchesDeserializer) {
	let mut f = File::create(out_path).unwrap();
	f.write_all(b"source,ts,desc,val\n").unwrap();
	while let Some(bytes) = deserializer.next_batch_bytes() {
		let mut buf = String::new();
		let batch = RecordBatchBytes::new(bytes);
		let arrival_us = batch.arrival_us();
		let mut iterator = batch.iterator();
		while let Some(record_bytes) = iterator.next_record_bytes() {
			let s = record_to_row(arrival_us, record_bytes);
			buf.push_str(&s);
		}
		f.write_all(buf.as_bytes()).unwrap();
	}
	f.sync_all().unwrap();
}

fn record_to_row(arrival_us: u64, record: RecordBytes) -> String {
	match record.kind() {
		RecordKind::KVLog => kv_log_to_row(arrival_us, record),
		RecordKind::SyscallEvent => sys_event_to_row(arrival_us, record),
		RecordKind::PacketCapture => packet_to_row(arrival_us, record),
		_ => panic!("Unhandled Record Kind"),
	}
}

fn kv_log_to_row(arrival_us: u64, record: RecordBytes) -> String {
	format!(
		"kv,{},op_dur,{}\n",
		arrival_us,
		record.kv_log_duration_us().unwrap()
	)
}

fn sys_event_to_row(arrival_us: u64, record: RecordBytes) -> String {
	let num = record.syscall_event_syscall_number().unwrap();
	format!(
		"sys{},{},op_dur,{}\n",
		num,
		arrival_us,
		record.syscall_event_duration_us().unwrap()
	)
}

fn packet_to_row(arrival_us: u64, record: RecordBytes) -> String {
	format!(
		"packet,{},dst_port,{}\n",
		arrival_us,
		record.as_packet_capture().unwrap().dst_port
	)
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
	#[arg(short, long)]
	file_path: String,

	#[arg(short, long)]
	option: u64,

	#[arg(short, long)]
	out_path: Option<String>,
}

fn main() {
	let args = Args::parse();
	let file_path = args.file_path.clone();
	let option = args.option;
	let out_path = args.out_path.clone();

	println!("Loading file");
	let mut file = File::open(file_path).unwrap();
	let mut data = Vec::new();
	let _ = file.read_to_end(&mut data).unwrap();
	let deserializer = RecordBatchesDeserializer::new(data);

	match option {
		0 => kvlog_dur_stats(deserializer),
		1 => syscall_dur_stats(deserializer),
		2 => page_cache_count(deserializer),
		3 => data_summary(deserializer),
		4 => {
			let out = out_path.as_ref().unwrap();
			data_to_csv(out, deserializer);
		}
		5 => mangled_packet_count(deserializer),
		_ => unimplemented!(),
	}
}
