use crate::SourceKind;
use crossbeam::channel::{unbounded, Receiver, Sender};
use data::{page_cache::PageCacheEvent, record::Record};
use libbpf_rs::skel::OpenSkel;
use libbpf_rs::skel::Skel;
use libbpf_rs::skel::SkelBuilder;
use libbpf_rs::PerfBufferBuilder;
use std::process;
use std::thread;
use utils::Duration;

mod page_cache {
	include!(concat!(env!("OUT_DIR"), "/page_cache.skel.rs"));
}
use page_cache::*;

#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct PageCacheBpfEvent {
	pid: u32,
	tid: u32,
	timestamp_nanos: u64,
	pfn: i64,
	i_ino: i64,
	index: i64,
	s_dev: i32,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct PageCacheEventBuffer {
	length: u32,
	buffer: [PageCacheBpfEvent; 256],
}

fn lost_event_handler(cpu: i32, count: u64) {
	eprintln!("Lost {count} events on CPU {cpu}");
}

fn event_receiver(
	event_rx: Receiver<PageCacheEventBuffer>,
	record_batch_tx: Sender<(SourceKind, Vec<Record>)>,
) {
	println!("Event receiver running");
	let mut records = Vec::new();
	while let Ok(buffer) = event_rx.recv() {
		for event in &buffer.buffer[..buffer.length as usize] {
			let p = Record::from_page_cache_event(PageCacheEvent {
				pid: event.pid,
				tid: event.tid,
				timestamp_us: event.timestamp_nanos / 1_000,
				pfn: event.pfn,
				i_ino: event.i_ino,
				index: event.index,
				s_dev: event.s_dev,
			});
			records.push(p);
		}
		if records.len() >= 1024 {
			let _ = record_batch_tx.try_send((SourceKind::PageCache, records));
			records = Vec::new();
		}
	}
}

fn attach(tx: Sender<(SourceKind, Vec<Record>)>) {
	println!("Attaching page cache");

	let (event_tx, event_rx) = unbounded();
	thread::spawn(move || {
		event_receiver(event_rx, tx);
	});

	let skel_builder = PageCacheSkelBuilder::default();
	let mut open_skel = skel_builder.open().unwrap();
	open_skel.rodata().my_pid = process::id();

	let mut skel = open_skel.load().unwrap();
	skel.attach().unwrap();
	let perf = PerfBufferBuilder::new(skel.maps_mut().perf_buffer())
		.sample_cb(|_cpu: i32, bytes: &[u8]| {
			let bytes_ptr = bytes.as_ptr();
			let ptr = bytes_ptr as *const PageCacheEventBuffer;
			let event_buffer = unsafe { *ptr };
			event_tx.send(event_buffer).unwrap();
		})
		.lost_cb(lost_event_handler)
		.build()
		.unwrap();
	loop {
		perf.poll(Duration::from_secs(1)).unwrap();
	}
}

pub fn run_workload(tx: Sender<(SourceKind, Vec<Record>)>) {
	attach(tx);
}
