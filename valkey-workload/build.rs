use libbpf_cargo::SkeletonBuilder;
use std::env;
use std::path::PathBuf;

const SYSCALL_LATENCY_BPF: &str = "src/bpf/syscall_latency.bpf.c";
const TC_BPF: &str = "src/bpf/tc.bpf.c";
const PACKET_FILTER_BPF: &str = "src/bpf/packet_filter.bpf.c";

fn main() {
	let out = PathBuf::from(
		env::var_os("OUT_DIR").expect("OUT_DIR must be set in build script"),
	);

	{
		let mut out = out.clone();
		out.push("tc.skel.rs");
		SkeletonBuilder::new()
			.source(TC_BPF)
			.build_and_generate(&out)
			.unwrap();
	}

	{
		let mut out = out.clone();
		out.push("syscall_latency.skel.rs");
		SkeletonBuilder::new()
			.source(SYSCALL_LATENCY_BPF)
			.build_and_generate(&out)
			.unwrap();
	}

	{
		let mut out = out.clone();
		out.push("packet_filter.skel.rs");
		SkeletonBuilder::new()
			.source(PACKET_FILTER_BPF)
			.build_and_generate(&out)
			.unwrap();
	}

	println!("cargo:rerun-if-changed={TC_BPF}");
	println!("cargo:rerun-if-changed={SYSCALL_LATENCY_BPF}");
	println!("cargo:rerun-if-changed={PACKET_FILTER_BPF}");
}
