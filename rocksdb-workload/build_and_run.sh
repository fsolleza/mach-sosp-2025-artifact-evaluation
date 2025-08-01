RUSTFLAGS="-L/home/fsolleza/Repositories/mach-v2/osdi-2025/zlib -lz" cargo build --release

OUTPUT_DIR="/nvme/data/tmp/can_delete"

DUR=120

echo "Doing P3"
sudo ../target/release/rocksdb-workload -s -p \
	--output-path $OUTPUT_DIR/rocksdb-p3-data-small \
	--rocksdb-path "/nvme/data/tmp/can_delete/rocksdb-app" \
	--duration-seconds $DUR

echo "Doing P2"
sudo ../target/release/rocksdb-workload -s \
	--output-path $OUTPUT_DIR/rocksdb-p2-data-small \
	--rocksdb-path "/nvme/data/tmp/can_delete/rocksdb-app" \
	--duration-seconds $DUR

echo "Doing P1"
sudo ../target/release/rocksdb-workload \
	--output-path $OUTPUT_DIR/rocksdb-p1-data-small \
	--rocksdb-path "/nvme/data/tmp/can_delete/rocksdb-app" \
	--duration-seconds $DUR

DUR=600

echo "Doing P3"
sudo ../target/release/rocksdb-workload -s -p \
	--output-path $OUTPUT_DIR/rocksdb-p3-data \
	--rocksdb-path "/nvme/data/tmp/can_delete/rocksdb-app" \
	--duration-seconds $DUR

echo "Doing P2"
sudo ../target/release/rocksdb-workload -s \
	--output-path $OUTPUT_DIR/rocksdb-p2-data \
	--rocksdb-path "/nvme/data/tmp/can_delete/rocksdb-app" \
	--duration-seconds $DUR

echo "Doing P1"
sudo ../target/release/rocksdb-workload \
	--output-path $OUTPUT_DIR/rocksdb-p1-data \
	--rocksdb-path "/nvme/data/tmp/can_delete/rocksdb-app" \
	--duration-seconds $DUR
