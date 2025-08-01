const K: usize = 1024;
const M: usize = K * 1024;
const G: usize = M * 1024;

pub const CHUNK_SZ: usize = 64 * K;
pub const BLOCK_SZ: usize = 64 * M;
pub const FILE_SZ: usize = G;
pub const PARTITIONS: usize = 1;

pub const BLOCKS_PER_FILE: usize = FILE_SZ / BLOCK_SZ;

//// The size of the in memory buffer in bytes
//const MB: usize = 1024 * 1024;
//const BLOCK_BYTES_MULTIPLIER: usize = 1;
//pub const BLOCK_BYTES: usize = BLOCK_BYTES_MULTIPLIER * MB;
//pub static BLOCKS_PER_FILE: usize = 1024 / BLOCK_BYTES_MULTIPLIER; // 1G File
//pub static FILE_SIZE: usize = BLOCK_BYTES * BLOCKS_PER_FILE;
//pub const PARTITIONS: usize = 3;
//
//pub fn print_mach_parameters() {
//	const TOTAL_IN_MEM: usize = PARTITIONS * BLOCK_BYTES * 2;
//	println!("Mach parameters:");
//	println!("\tBlock size (Mb): {}", BLOCK_BYTES / MB);
//	println!("\tFile size (Mb): {}", FILE_SIZE / MB);
//	println!("\tBlocks per file: {}", BLOCKS_PER_FILE);
//	println!("\tPartitions: {}", PARTITIONS);
//	println!("\tTotal in-memory block size (Mb): {}", TOTAL_IN_MEM / MB);
//}
