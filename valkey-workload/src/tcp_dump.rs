use libbpf_rs::PerfBufferBuilder;
use std::sync::atomic::{AtomicBool, Ordering::SeqCst};

use std::os::fd::AsFd;
use std::thread::{self, JoinHandle};
use utils::Duration;

use crossbeam::channel::{bounded, Receiver, Sender};
use libbpf_rs::skel::OpenSkel;
use libbpf_rs::skel::SkelBuilder;
use libbpf_rs::TcHookBuilder;
use libbpf_rs::TC_EGRESS;
use libbpf_rs::TC_INGRESS;

mod tc {
	include!(concat!(env!("OUT_DIR"), "/tc.skel.rs"));
}
use tc::*;

use crate::{tc_slice_to_records, Event};

static HALT: AtomicBool = AtomicBool::new(false);

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct TcEvent {
	pub timestamp: u64,
	pub dst_port: u16,
	pub src_port: u16,
	pub dst_addr: u32,
	pub src_addr: u32,
	pub tot_len: u16,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct TcEventBuf {
	pub len: u32,
	pub buffer: [TcEvent; 256],
}

fn lost_event_handler(cpu: i32, count: u64) {
	eprintln!("Lost {count} events on CPU {cpu}");
}

fn detach(progs: &TcProgs) {
	let indexes: Vec<i32> = nix::net::if_::if_nameindex()
		.unwrap()
		.iter()
		.map(|x| x.index() as i32)
		.collect();

	for ifidx in indexes {
		let mut tc_builder = TcHookBuilder::new(progs.handle_tc().as_fd());
		tc_builder
			.ifindex(ifidx)
			.replace(true)
			.handle(1)
			.priority(1);

		let mut egress = tc_builder.hook(TC_EGRESS);
		let mut ingress = tc_builder.hook(TC_INGRESS);
		if let Err(e) = ingress.destroy() {
			println!("failed to destroy all {e}");
		}
		if let Err(e) = egress.destroy() {
			println!("failed to destroy all {e}");
		}
	}
}

fn attach(progs: &TcProgs) {
	let indexes: Vec<i32> = nix::net::if_::if_nameindex()
		.unwrap()
		.iter()
		.map(|x| x.index() as i32)
		.collect();

	for ifidx in &indexes {
		let ifidx = *ifidx;

		let mut tc_builder = TcHookBuilder::new(progs.handle_tc().as_fd());
		tc_builder
			.ifindex(ifidx)
			.replace(true)
			.handle(1)
			.priority(1);

		let mut egress = tc_builder.hook(TC_EGRESS);
		let mut ingress = tc_builder.hook(TC_INGRESS);

		if let Err(e) = ingress.create() {
			println!("failed to create ingress hook {e}");
		}

		if let Err(e) = egress.create() {
			println!("failed to create egress hook {e}");
		}

		if let Err(e) = egress.attach() {
			println!("failed to attach egress hook {e}");
		}

		if let Err(e) = ingress.attach() {
			println!("failed to attach ingress hook {e}");
		}
	}
}

fn event_receiver(rx: Receiver<TcEventBuf>, tx: Sender<Event>) {
	let mut events = Vec::new();
	while let Ok(event_buf) = rx.recv() {
		if HALT.load(SeqCst) {
			break;
		}
		events.extend_from_slice(&event_buf.buffer[..event_buf.len as usize]);
		if events.len() > 1024 {
			let records =
				Event::TcPacket(tc_slice_to_records(events.as_slice()));
			let _ = tx.send(records);
			events.clear();
		}
	}
}

pub fn tcp_dump_detach() {
	let builder = TcSkelBuilder::default();
	let open = builder.open().unwrap();
	let skel = open.load().unwrap();

	let progs = skel.progs();
	detach(&progs);
}

pub struct TcpDumpWorkload {
	handle: JoinHandle<()>,
	receiver_handle: JoinHandle<()>,
}

impl TcpDumpWorkload {
	pub fn halt(&self) {
		HALT.store(true, SeqCst);
	}

	pub fn wait(self) {
		let _ = self.handle.join();
		let _ = self.receiver_handle.join();
		println!("Detaching tc hooks");
		tcp_dump_detach();
	}
}

pub fn tcp_dump_workload(tx: Sender<Event>) -> TcpDumpWorkload {
	let (inner_tx, inner_rx) = bounded(4096);

	let receiver_handle = thread::spawn(move || {
		event_receiver(inner_rx, tx);
	});

	let handle = thread::spawn(move || {
		let builder = TcSkelBuilder::default();
		let open = builder.open().unwrap();
		let mut skel = open.load().unwrap();

		let progs = skel.progs();
		attach(&progs);

		let perf = PerfBufferBuilder::new(skel.maps_mut().perf_buffer())
			.sample_cb(|_cpu: i32, bytes: &[u8]| {
				let bytes_ptr = bytes.as_ptr();
				let ptr = bytes_ptr as *const TcEventBuf;
				let event_buffer = unsafe { *ptr };
				let _ = inner_tx.send(event_buffer);
			})
			.lost_cb(lost_event_handler)
			.build()
			.unwrap();

		loop {
			if perf.poll(Duration::from_secs(1)).is_err() {
				break;
			}
			if HALT.load(SeqCst) {
				break;
			}
		}
	});
	TcpDumpWorkload {
		handle,
		receiver_handle,
	}
}
