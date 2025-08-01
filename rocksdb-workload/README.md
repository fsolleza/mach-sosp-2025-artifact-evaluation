
## Building

Need to build zlib first:

'''
cd ../zlib
checkout 09155ea
./configure
make
'''

To compile, need to specify the flag. Note that a full path is required (as
opposed to relative)

'''
RUSTFLAGS="-L/home/fsolleza/Repositories/mach-v2/workload-generators/zlib -lz" cargo build
'''

This is encoded in `.cargo/config.toml'

## Running

Need to run with sudo if running with BPF

'''
cargo build --release && sudo ./target/release/rocksdb-workload
'''

