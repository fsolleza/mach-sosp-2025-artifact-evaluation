#! /usr/bin/env bash

set -e

cargo build --release

OUTPUT_DIR="/nvme/data/tmp/can_delete"
DUR=120

echo "Doing P3"
sudo ../target/release/valkey-load -s -p \
	--output-path $OUTPUT_DIR/valkey-p3-data-small \
	--duration-seconds $DUR

# echo "Doing P2"
# sudo ../target/release/valkey-load -s \
# 	--output-path $OUTPUT_DIR/valkey-p2-data-small \
# 	--duration-seconds $DUR
# 
# echo "Doing P1"
# sudo ../target/release/valkey-load \
# 	--output-path $OUTPUT_DIR/valkey-p1-data-small \
# 	--duration-seconds $DUR
# 
# DUR=600
# 
# echo "Doing P3"
# sudo ../target/release/valkey-load -s -p \
# 	--output-path $OUTPUT_DIR/valkey-p3-data \
# 	--duration-seconds $DUR
# 
# echo "Doing P2"
# sudo ../target/release/valkey-load -s \
# 	--output-path $OUTPUT_DIR/valkey-p2-data \
# 	--duration-seconds $DUR
# 
# echo "Doing P1"
# sudo ../target/release/valkey-load \
# 	--output-path $OUTPUT_DIR/valkey-p1-data \
# 	--duration-seconds $DUR
