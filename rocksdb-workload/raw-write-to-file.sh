#!/usr/bin/env bash

set -e

cargo build --release

OUTPUT_DIR="/nvme/data/tmp/can_delete"
DUR=120

sudo ../target/release/rocksdb-workload -s -p \
	--output-path $OUTPUT_DIR/rocksdb-raw-to-file \
	--rocksdb-path "/nvme/data/tmp/can_delete/rocksdb-app" \
	--duration-seconds $DUR \
	--alternative-recording raw-to-file \
 	--flush-interval-mb 64

#sudo ../target/release/rocksdb-workload -s -p \
#	--output-path $OUTPUT_DIR/rocksdb-raw-to-file \
#	--rocksdb-path "/nvme/data/tmp/can_delete/rocksdb-app" \
#	--duration-seconds $DUR \
#	--alternative-recording noop

