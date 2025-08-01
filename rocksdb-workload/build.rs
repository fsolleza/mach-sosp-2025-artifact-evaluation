use libbpf_cargo::SkeletonBuilder;
use std::env;
use std::path::PathBuf;

const SYSCALL_LATENCY_BPF: &str = "src/bpf/syscall_latency.bpf.c";
const PAGE_CACHE_BPF: &str = "src/bpf/page_cache.bpf.c";

fn main() {
	let out = PathBuf::from(
		env::var_os("OUT_DIR").expect("OUT_DIR must be set in build script"),
	);

	let mut syscall_out = out.clone();
	syscall_out.push("syscall_latency.skel.rs");

	let mut page_cache_out = out.clone();
	page_cache_out.push("page_cache.skel.rs");

	SkeletonBuilder::new()
		.source(SYSCALL_LATENCY_BPF)
		.build_and_generate(&syscall_out)
		.unwrap();

	SkeletonBuilder::new()
		.source(PAGE_CACHE_BPF)
		.build_and_generate(&page_cache_out)
		.unwrap();

	println!("cargo:rerun-if-changed={SYSCALL_LATENCY_BPF}");
	println!("cargo:rerun-if-changed={PAGE_CACHE_BPF}");
}
