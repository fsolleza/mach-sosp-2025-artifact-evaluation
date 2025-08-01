#!/usr/bin/env bash

set -e

FISHSTORE_BIN=/home/fsolleza/Repositories/mach-v2/osdi-2025/third-party/FishStore/build/Release/hft
OUTPUT_DIR=/nvme/data/tmp/can_delete/rocksdb-workload
ROCKSDB_DIR=$OUTPUT_DIR/rocksdb-app
OUTPUT_FILE=$OUTPUT_DIR/raw-to-file
DUR=120

sudo rm -rf $OUTPUT_DIR
mkdir -p $OUTPUT_DIR

echo "running fishstore application"
# taskset -c 0-40 $FISHSTORE_BIN &> $OUTPUT_DIR/fishstore-log &
# FISHSTORE_PID=$!
# echo "fishstore pid: " $FISHSTORE_PID
# 
# echo "sleeping for 10 seconds"
# sleep 10

echo "building workload "
cargo build --release &> $OUTPUT_DIR/cargo-build-log

echo "running workload "
sudo ../target/release/rocksdb-workload -s -p \
	--output-path $OUTPUT_FILE \
	--rocksdb-path $ROCKSDB_DIR \
	--duration-seconds $DUR \
	--alternative-recording tcp

# echo killing $FISHSTORE_PID
# kill -9 $FISHTORE_PID
sudo rm -rf $OUTPUT_DIR
