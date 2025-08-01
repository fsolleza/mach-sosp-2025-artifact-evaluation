use std::os::fd::{AsFd, AsRawFd, RawFd};
use std::{
	sync::atomic::{AtomicBool, Ordering::SeqCst},
	thread::{self, JoinHandle},
};
use utils::Duration;

use libbpf_rs::skel::OpenSkel;
use libbpf_rs::skel::SkelBuilder;

mod packet_filter {
	include!(concat!(env!("OUT_DIR"), "/packet_filter.skel.rs"));
}
use packet_filter::*;

static DONE: AtomicBool = AtomicBool::new(false);

//fn detach(prog: Program, ifidx: i32) {
//	let prog_fd = prog.as_fd().as_raw_fd();
//	let attach_opts = {
//		let mut attach_opts = libbpf_sys::bpf_xdp_attach_opts::default();
//
//		// required param when validating opts in kernel, 8 for this
//		// particular opt: 4 for prog_fd, and 4 for padding (see definition)
//		attach_opts.sz = 8;
//		attach_opts.old_prog_fd = prog_fd;
//		attach_opts
//	};
//
//	libbpf_sys::bpf_xdp_detach(ifindex, libbpf_sys::XDP_FLAGS_SKB_MODE, &attach_opts as *const _);
//}

//fn attach(prog: Program, ifidx: i32) {
//
//	// For some reason, I can't make attach_xdp (uses
//	// libbpf_sys::bpf_program_attach__xdp) to work on this machine. Use
//	// bpf_xdp_attach instead to specify SKB_MODE. Since there's no link, we
//	// have to detach this manually
//	// https://github.com/libbpf/libbpf-bootstrap/issues/44
//	// https://github.com/libbpf/libbpf/issues/587
//
//	let prog_fd = prog.as_fd().as_raw_fd();
//	unsafe {
//		libbpf_sys::bpf_xdp_attach(ifidx, prog_fd, libbpf_sys::XDP_FLAGS_SKB_MODE, std::ptr::null());
//	}
//}

struct AttachedProgram {
	fd: RawFd,
	ifidx: i32,
}

impl Drop for AttachedProgram {
	fn drop(&mut self) {
		unsafe {
			let attach_opts = {
				let mut attach_opts =
					libbpf_sys::bpf_xdp_attach_opts::default();

				// required param when validating opts in kernel, 8 for this
				// particular opt: 4 for prog_fd, and 4 for padding (see definition)
				attach_opts.sz = 8;
				attach_opts.old_prog_fd = self.fd;
				attach_opts
			};

			libbpf_sys::bpf_xdp_detach(
				self.ifidx,
				libbpf_sys::XDP_FLAGS_SKB_MODE,
				&attach_opts as *const _,
			);
		}
	}
}

fn attach_to_all_ifaces() -> Vec<AttachedProgram> {
	let skel_builder = PacketFilterSkelBuilder::default();
	let open_skel = skel_builder.open().unwrap();
	let skel = open_skel.load().unwrap();
	let progs = skel.progs();
	let prog = progs.xdp_packet_filter();
	let prog_fd = prog.as_fd().as_raw_fd();

	//let mut links = Vec::new();
	let indexes: Vec<i32> = nix::net::if_::if_nameindex()
		.unwrap()
		.iter()
		.map(|x| x.index() as i32)
		.collect();

	let mut attached_programs = Vec::new();
	for ifidx in indexes.iter().copied() {
		println!("Linking to {}", ifidx);

		// For some reason, I can't make attach_xdp (uses
		// libbpf_sys::bpf_program_attach__xdp) to work on this machine. Use
		// bpf_xdp_attach instead to specify SKB_MODE. Since there's no link, we
		// have to detach this manually
		// https://github.com/libbpf/libbpf-bootstrap/issues/44
		// https://github.com/libbpf/libbpf/issues/587

		unsafe {
			libbpf_sys::bpf_xdp_attach(
				ifidx,
				prog_fd,
				libbpf_sys::XDP_FLAGS_SKB_MODE,
				std::ptr::null(),
			);
		}
		attached_programs.push(AttachedProgram { fd: prog_fd, ifidx });
	}
	attached_programs
}

pub struct PacketFilterState {
	handle: JoinHandle<()>,
}

impl PacketFilterState {
	pub fn halt(&self) {
		DONE.store(true, SeqCst);
	}

	pub fn wait(self) {
		let _ = self.handle.join();
	}
}

pub fn init_packet_filter() -> PacketFilterState {
	let handle = thread::spawn(move || {
		let _attached_progs = attach_to_all_ifaces();

		// Loop keeps links alive untile DONE
		loop {
			thread::sleep(Duration::from_secs(1));
			if DONE.load(SeqCst) {
				break;
			}
		}

		// Progs dropped and detached here.
	});

	PacketFilterState { handle }
}
