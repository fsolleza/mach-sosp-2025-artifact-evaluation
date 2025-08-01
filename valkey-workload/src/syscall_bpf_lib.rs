// SPDX-License-Identifier: (LGPL-2.1 OR BSD-2-Clause)

use crossbeam::channel::{unbounded, Receiver, Sender};
//use data::{record::Record, syscall::SyscallEvent};
use std::{
	collections::{BTreeSet, HashMap},
	process,
	thread::{self, JoinHandle},
};
use utils::{Duration, Instant};

use libbpf_rs::skel::OpenSkel;
use libbpf_rs::skel::Skel;
use libbpf_rs::skel::SkelBuilder;
use libbpf_rs::PerfBufferBuilder;

mod syscall_latency {
	include!(concat!(env!("OUT_DIR"), "/syscall_latency.skel.rs"));
}
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::SeqCst};
use syscall_latency::*;

use crate::{syscall_slice_to_records, Event};

pub static SYSCALL_LATENCY: AtomicU64 = AtomicU64::new(0);
static HALT: AtomicBool = AtomicBool::new(false);

#[repr(C)]
#[derive(Copy, Clone)]
struct BpfSyscallEvent {
	kind: u32,
	pid: u32,
	tid: u32,
	syscall_number: u64,
	timestamp_ns: u64,
}

pub struct SyscallEvent {
	pub pid: u32,
	pub tid: u32,
	pub syscall_number: u64,
	pub timestamp_ns: u64,
	pub duration_us: u64,
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
	record_batch_tx: Sender<crate::Event>,
) {
	println!("Event receiver running");

	//while !WORKLOAD_START.load(SeqCst) {
	//	thread::sleep(Duration::from_secs(1));
	//}

	let mut sys_entry: HashMap<(u32, u32, u64), BTreeSet<u64>> = HashMap::new();
	let mut records = Vec::new();
	let mut unmatched_enters = 0;
	let mut unmatched_exits = 0;
	let mut print_unmatched_timer = Instant::now();
	while let Ok(buffer) = event_rx.recv() {
		if HALT.load(SeqCst) {
			break;
		}

		for event in &buffer.buffer[..buffer.length as usize] {
			// If it's a syscall entry do this
			if event.kind == 0 {
				let pid = event.pid;
				let tid = event.tid;
				let sys = event.syscall_number;
				let ts = event.timestamp_ns;
				let k = (pid, tid, sys);
				let _ =
					sys_entry.entry(k).or_insert_with(BTreeSet::new).insert(ts);
			}
			// Syscall exit, lookup the corresponding entry in the map and
			// calculate the duration. It is possible for a syscall exit to
			// arrive before entry. For now, we ignore those
			else if event.kind == 1 {
				let pid = event.pid;
				let tid = event.tid;
				let sys = event.syscall_number;
				let end = event.timestamp_ns;

				let k = (pid, tid, sys);
				let starts = sys_entry.entry(k).or_insert_with(BTreeSet::new);

				let mut last_start = u64::MAX;
				for start in starts.iter() {
					if *start < end {
						last_start = *start;
					} else {
						break;
					}
				}

				if last_start == u64::MAX {
					unmatched_exits += 1;
				} else {
					let _ = starts.remove(&last_start);
					let dur = (end - last_start) / 1_000;
					SYSCALL_LATENCY.fetch_max(dur, SeqCst);
					let e = SyscallEvent {
						pid,
						tid,
						syscall_number: sys,
						timestamp_ns: last_start,
						duration_us: dur,
					};
					records.push(e);
					let l = records.len();
					if l >= 1024 {
						let to_send =
							Event::Syscall(syscall_slice_to_records(&records));
						if record_batch_tx.try_send(to_send).is_err() {}
						records.clear();
					}
				}
			} else {
				panic!("Unhandled event.kind");
			}
		}

		if print_unmatched_timer.elapsed() > Duration::from_secs(1) {
			//println!(
			//	"Unmatched enters: {} exits: {}",
			//	unmatched_enters, unmatched_exits
			//);
			unmatched_enters = 0;
			unmatched_exits = 0;
			print_unmatched_timer = Instant::now();
		}
	}
}

fn attach(tx: Sender<BpfSyscallEventBuffer>) {
	let this_thread_id = gettid::gettid();

	let skel_builder = SyscallLatencySkelBuilder::default();
	let mut open_skel = skel_builder.open().unwrap();
	open_skel.rodata().my_pid = process::id();
	open_skel.rodata().my_tid = this_thread_id as u32;

	let mut skel = open_skel.load().unwrap();
	skel.attach().unwrap();

	let perf = PerfBufferBuilder::new(skel.maps_mut().perf_buffer())
		.sample_cb(|_cpu: i32, bytes: &[u8]| {
			let bytes_ptr = bytes.as_ptr();
			let ptr = bytes_ptr as *const BpfSyscallEventBuffer;
			let event_buffer = unsafe { *ptr };
			let _ = tx.send(event_buffer);
		})
		.lost_cb(lost_event_handler)
		.build()
		.unwrap();
	loop {
		if perf.poll(Duration::from_secs(10)).is_err() {
			break;
		}
		if HALT.load(SeqCst) {
			break;
		}
	}
}

pub struct SyscallWorkload {
	event_receiver_handle: JoinHandle<()>,
	attach_handle: JoinHandle<()>,
}

impl SyscallWorkload {
	pub fn halt(&self) {
		HALT.store(true, SeqCst);
	}

	pub fn wait(self) {
		let _ = self.event_receiver_handle.join();
		let _ = self.attach_handle.join();
	}
}

pub fn syscall_workload(tx: Sender<crate::Event>) -> SyscallWorkload {
	let (inner_tx, inner_rx) = unbounded();

	let event_receiver_handle = thread::spawn(move || {
		event_receiver(inner_rx, tx);
	});

	let attach_handle = thread::spawn(move || attach(inner_tx));
	SyscallWorkload {
		event_receiver_handle,
		attach_handle,
	}
}
