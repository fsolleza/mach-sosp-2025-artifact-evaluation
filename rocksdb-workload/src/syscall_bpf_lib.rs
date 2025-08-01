// SPDX-License-Identifier: (LGPL-2.1 OR BSD-2-Clause)

use crate::DONE;
use crossbeam::channel::{unbounded, Receiver, Sender};
use data::{record::Record, syscall::SyscallEvent};
use std::{
	collections::HashMap, process, sync::atomic::Ordering::SeqCst, thread,
};
use utils::{Duration, Instant};

use crate::SourceKind;
use libbpf_rs::skel::OpenSkel;
use libbpf_rs::skel::Skel;
use libbpf_rs::skel::SkelBuilder;
use libbpf_rs::PerfBufferBuilder;

mod syscall_latency {
	include!(concat!(env!("OUT_DIR"), "/syscall_latency.skel.rs"));
}
use syscall_latency::*;

#[repr(C)]
#[derive(Copy, Clone)]
struct BpfSyscallEvent {
	kind: u32,
	pid: u32,
	tid: u32,
	syscall_number: u64,
	timestamp_ns: u64,
}

#[repr(C)]
#[derive(Copy, Clone)]
struct BpfSyscallEventBuffer {
	length: u32,
	buffer: [BpfSyscallEvent; 256],
}

fn lost_event_handler(cpu: i32, count: u64) {
	eprintln!("Lost {count} events on CPU {cpu}");
}

fn event_receiver(
	event_rx: Receiver<BpfSyscallEventBuffer>,
	record_batch_tx: Sender<(SourceKind, Vec<Record>)>,
) {
	println!("Event receiver running");

	let mut sys_entry = HashMap::new();
	let mut records = Vec::new();
	let mut _unmatched_enters = 0;
	let mut _unmatched_exits = 0;
	let mut print_unmatched_timer = Instant::now();
	while let Ok(buffer) = event_rx.recv() {
		if DONE.load(SeqCst) {
			break;
		}

		for event in &buffer.buffer[..buffer.length as usize] {
			// If it's a syscall entry do this
			if event.kind == 0 {
				let pid = event.pid;
				let tid = event.tid;
				let sys = event.syscall_number;
				let ts = event.timestamp_ns;
				if sys_entry.insert((pid, tid, sys), ts).is_some() {
					_unmatched_enters += 1;
				};
			}
			// Syscall exit, lookup the corresponding entry in the map and
			// calculate the duration. It is possible for a syscall exit to
			// arrive before entry. For now, we ignore those
			else if event.kind == 1 {
				let pid = event.pid;
				let tid = event.tid;
				let sys = event.syscall_number;
				let mut unmatched = true;
				if let Some(start) = sys_entry.remove(&(pid, tid, sys)) {
					let end = event.timestamp_ns;
					if end >= start {
						unmatched = false;
						let dur = (end - start) / 1_000;
						let e = SyscallEvent {
							pid,
							tid,
							syscall_number: sys,
							start_time: start,
							duration_us: dur,
						};
						let r = Record::from_syscall_event(e);
						records.push(r);
						let l = records.len();
						if l >= 1024 {
							let to_send = (SourceKind::Syscall, records);
							if record_batch_tx.try_send(to_send).is_err() {}
							records = Vec::new();
						}
					}
				}
				_unmatched_exits += unmatched as u64;
			} else {
				panic!("Unhandled event.kind");
			}
		}

		if print_unmatched_timer.elapsed() > Duration::from_secs(1) {
			//println!(
			//	"Unmatched enters: {} exits: {}",
			//	unmatched_enters, unmatched_exits
			//);
			_unmatched_enters = 0;
			_unmatched_exits = 0;
			print_unmatched_timer = Instant::now();
		}
	}
}

fn attach(tx: Sender<(SourceKind, Vec<Record>)>) {
	let this_thread_id = gettid::gettid();

	let skel_builder = SyscallLatencySkelBuilder::default();
	let mut open_skel = skel_builder.open().unwrap();
	open_skel.rodata().my_pid = process::id();
	open_skel.rodata().my_tid = this_thread_id as u32;

	let (event_tx, event_rx) = unbounded();
	thread::spawn(move || {
		event_receiver(event_rx, tx);
	});

	let mut skel = open_skel.load().unwrap();
	skel.attach().unwrap();

	let perf = PerfBufferBuilder::new(skel.maps_mut().perf_buffer())
		.sample_cb(|_cpu: i32, bytes: &[u8]| {
			let bytes_ptr = bytes.as_ptr();
			let ptr = bytes_ptr as *const BpfSyscallEventBuffer;
			let event_buffer = unsafe { *ptr };
			let _ = event_tx.send(event_buffer);
		})
		.lost_cb(lost_event_handler)
		.build()
		.unwrap();
	loop {
		if DONE.load(SeqCst) {
			break;
		}
		perf.poll(Duration::from_secs(10)).unwrap();
	}
}

pub fn run_workload(tx: Sender<(SourceKind, Vec<Record>)>) {
	attach(tx);
}
