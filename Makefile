MAKEFLAGS += --no-print-directory

# Don't change this
TCP_SERVER_ADDR:="127.0.0.1:7878"
TCP_SERVER_PORT:="7878"

# Path to data files
RECORDING_DIR:=[ABSOLUTE PATH TO DATA]

# Path to the root directory of this repo.
REPO_PATH:=[ABSOLUTE PATH TO THIS REPO]

# Path to output directory
OUTPUT_DIR:=$(REPO_PATH)/evaluation-logs

# Where to save storage engine data. This should live in fast storage
TMP_DATA_DIR:=[ABSOLUTE PATH TO FAST STORAGE]

# Fishstore will store its data here
FISHSTORE_DIR:=$(TMP_DATA_DIR)/fishstore_data

# Mach will store its data here
MACH_DIR:=$(TMP_DATA_DIR)/mach_store

# The file app will store its data here
FILE_APP_DIR:=$(TMP_DATA_DIR)/file_store

# InfluxDB will store its data here. Make sure to update the influx.conf file
# with this path.
INFLUX_DIR:=$(TMP_DATA_DIR)/influxdb2

# Path to the influxd binary. Make sure to update the influx.conf file with this
# path.
INFLUX_BIN:=[ABSOLUTE PATH TO INFLUXD BINARY]

# CPU pinning. We use CPU pinning to isolate replay processes from other
# ingest processes. You can change these as well.
REPLAY_TASKSET:=taskset -c 50-55
INFLUXD_TASKSET:=taskset -c 0-40

# Ramdisk RocksDB configuration for probe effects evaluation. You will need to
# setup your ramdisk. See README for details.
RAMDISK_MOUNT:=[ABSOLUTE PATH TO RAMDISK MOUNT]
RAMDISK_DIR:=$(RAMDISK_MOUNT)/data

##############################
# Clean all data directories #
##############################
clean-all:
	sudo rm -rf $(RAMDISK_DIR)
	rm -rf $(TMP_DATA_DIR)

###################
# Replay commands #
###################

rocksdb-p1:
	$(REPLAY_TASKSET) ./target/release/tcp-replay \
		--storage-target mach \
		--duration-s $(REPLAY_DURATION) \
		--file-path $(RECORDING_DIR)/rocksdb-p1-small

rocksdb-p2:
	$(REPLAY_TASKSET) ./target/release/tcp-replay \
		--storage-target mach \
		--duration-s $(REPLAY_DURATION) \
		--file-path $(RECORDING_DIR)/rocksdb-p2-small

rocksdb-p3:
	$(REPLAY_TASKSET) ./target/release/tcp-replay \
		--storage-target mach \
		--duration-s $(REPLAY_DURATION) \
		--file-path $(RECORDING_DIR)/rocksdb-p3-small

valkey-p1:
	$(REPLAY_TASKSET) ./target/release/tcp-replay \
		--storage-target mach \
		--duration-s $(REPLAY_DURATION) \
		--file-path $(RECORDING_DIR)/valkey-p1-small

valkey-p2:
	$(REPLAY_TASKSET) ./target/release/tcp-replay \
		--storage-target mach \
		--duration-s $(REPLAY_DURATION) \
		--file-path $(RECORDING_DIR)/valkey-p2-small

valkey-p3:
	$(REPLAY_TASKSET) ./target/release/tcp-replay \
		--storage-target mach \
		--duration-s $(REPLAY_DURATION) \
		--file-path $(RECORDING_DIR)/valkey-p3-small \

rocksdb-fishstore-p1:
	$(REPLAY_TASKSET) ./target/release/tcp-replay \
		--storage-target fishstore \
		--duration-s $(REPLAY_DURATION) \
		--file-path $(RECORDING_DIR)/rocksdb-p1-small

rocksdb-fishstore-p2:
	$(REPLAY_TASKSET) ./target/release/tcp-replay \
		--storage-target fishstore \
		--duration-s $(REPLAY_DURATION) \
		--file-path $(RECORDING_DIR)/rocksdb-p2-small

rocksdb-fishstore-p3:
	$(REPLAY_TASKSET) ./target/release/tcp-replay \
		--storage-target fishstore \
		--duration-s $(REPLAY_DURATION) \
		--file-path $(RECORDING_DIR)/rocksdb-p3-small

valkey-fishstore-p1:
	$(REPLAY_TASKSET) ./target/release/tcp-replay \
		--storage-target fishstore \
		--duration-s $(REPLAY_DURATION) \
		--file-path $(RECORDING_DIR)/valkey-p1-small

valkey-fishstore-p2:
	$(REPLAY_TASKSET) ./target/release/tcp-replay \
		--storage-target fishstore \
		--duration-s $(REPLAY_DURATION) \
		--file-path $(RECORDING_DIR)/valkey-p2-small

valkey-fishstore-p3:
	./target/release/tcp-replay \
		--storage-target fishstore \
		--duration-s $(REPLAY_DURATION) \
		--file-path $(RECORDING_DIR)/valkey-p3-small

##########################
# Command to run InfluxD #
##########################

influxd:
	rm -rf $(INFLUX_DIR)
	mkdir -p $(INFLUX_DIR)
	$(INFLUXD_TASKSET) $(INFLUX_BIN) --config influx.conf
	rm -rf $(INFLUX_DIR)

##############################
# Command to build FishStore #
##############################

build-fishstore:
	cd third-party/FishStore && \
		mkdir -p build/Release && \
		cd build/Release && \
		cmake -DCMAKE_BUILD_TYPE=Release ../.. && \
		cmake --build . --target hft psf_lib

######################################
# Commands for End to end evaluation #
######################################

e2e-influx-app-dropped:
	rm -f $(OUTPUT_DIR)/$@-$(QUERY)
	$(INFLUXD_TASKSET) target/release/influx-app \
	 	--addr $(TCP_SERVER_ADDR) \
		--runtime-threads 16 \
		--influx-connections 16 \
		--query $(QUERY) \
		| tee $(OUTPUT_DIR)/$@-$(QUERY)

e2e-influx-app:
	rm -f $(OUTPUT_DIR)/$@-$(QUERY)
	$(INFLUXD_TASKSET) target/release/influx-app \
	 	--addr $(TCP_SERVER_ADDR) \
		--runtime-threads 16 \
		--influx-connections 16 \
		--complete-data \
		--query $(QUERY) \
		| tee $(OUTPUT_DIR)/$@-$(QUERY)

e2e-fishstore-app:
	rm -rf $(FISHSTORE_DIR)
	mkdir -p $(FISHSTORE_DIR)
	rm -f $(OUTPUT_DIR)/$@-$(QUERY)
	(cd third-party/FishStore/build/Release && \
		taskset -c 0-15 ./hft 8 $(FISHSTORE_DIR) \
		$(TCP_SERVER_PORT) $(QUERY) 1) \
		| tee $(OUTPUT_DIR)/$@-$(QUERY)
	rm -rf $(FISHSTORE_DIR)

e2e-mach-app:
	rm -rf $(MACH_DIR)
	mkdir -p $(MACH_DIR)
	rm -f $(OUTPUT_DIR)/$@-$(QUERY)
	taskset -c 0-7 target/release/mach-app \
		--addr $(TCP_SERVER_ADDR) \
		--mach-path $(MACH_DIR) \
		--query $(QUERY) | tee $(OUTPUT_DIR)/$@-$(QUERY)

e2e-build: build-fishstore
	cargo build --release \
		-p influx-app \
		-p mach-app \
		-p replay

#########################################
# Commands for Probe Effects Evaluation #
#########################################

pe-build: build-fishstore
	RUSTFLAGS="-L/home/fsolleza/Repositories/sosp-2025/mach-v2/third-party/zlib -lz" \
		cargo build --release \
			-p file-writer-app \
			-p influx-app \
			-p mach-app \
			-p rocksdb-workload

pe-influx-app:
	target/release/influx-app \
		--addr $(TCP_SERVER_ADDR) \
		--runtime-threads 16 \
		--influx-connections 16

pe-mach-app:
	rm -rf $(MACH_DIR)
	mkdir -p $(MACH_DIR)
	target/release/mach-app \
		--addr $(TCP_SERVER_ADDR) \
		--mach-path $(MACH_DIR) \
		--query rocksdb-p3-probe-effect

pe-file-writer-app:
	rm -rf $(FILE_APP_DIR)
	mkdir -p $(FILE_APP_DIR)
	target/release/file-writer-app \
		--addr $(TCP_SERVER_ADDR) \
		--file-path $(FILE_APP_DIR)/workload_file

pe-fishstore-app-no-index:
	rm -rf $(FISHSTORE_DIR);
	mkdir -p $(FISHSTORE_DIR);
	cd third-party/FishStore/build/Release && \
		./hft 8 $(FISHSTORE_DIR) $(TCP_SERVER_PORT) no-index 0

pe-fishstore-app:
	rm -rf $(FISHSTORE_DIR);
	mkdir -p $(FISHSTORE_DIR);
	cd third-party/FishStore/build/Release && \
		./hft 8 $(FISHSTORE_DIR) $(TCP_SERVER_PORT) rocksdb-p3 0


ROCKSDB_PE_DIR:=$(RAMDISK_DIR)/rocksdb-app

pe-rocksdb:
	sudo rm -rf $(ROCKSDB_PE_DIR)
	sudo target/release/rocksdb-workload \
		--rocksdb-path $(ROCKSDB_PE_DIR) \
		--duration-seconds 120 \
		--no-drop-caches \
		--syscalls \
		--page-cache \
		--alternative-recording tcp \
		| tee $(OUTPUT_DIR)/pe-$(OUT_FILE)

pe-rocksdb-fishstore:
	sudo rm -rf $(ROCKSDB_PE_DIR)
	sudo target/release/rocksdb-workload \
		--rocksdb-path $(ROCKSDB_PE_DIR) \
		--duration-seconds 120 \
		--no-drop-caches \
		--syscalls \
		--page-cache \
		--alternative-recording tcp \
		--fishstore \
		| tee $(OUTPUT_DIR)/pe-$(OUT_FILE)

pe-rocksdb-noop:
	sudo rm -rf $(ROCKSDB_PE_DIR)
	sudo target/release/rocksdb-workload \
		--rocksdb-path $(ROCKSDB_PE_DIR) \
		--duration-seconds 120 \
		--no-drop-caches \
		--syscalls \
		--page-cache \
		--alternative-recording noop \
		| tee $(OUTPUT_DIR)/pe-noop

##########################################
# Commands for Ingest Scaling Evaluation #
##########################################

INGEST_SCALING_DIR:=$(TMP_DATA_DIR)/ingest-scaling

is-build: build-fishstore
	cargo build --release -p ingest-scaling

is-lmdb:
	rm -f $(OUTPUT_DIR)/is-lmdb-$(SIZE)
	touch $(OUTPUT_DIR)/is-lmdb-$(SIZE)
	for number in 1 2 3 4 5; do \
		echo RUN $$number; \
		rm -rf $(INGEST_SCALING_DIR); \
		mkdir -p $(INGEST_SCALING_DIR); \
		./target/release/ingest-scaling \
			--db lmdb \
			--storage-dir $(INGEST_SCALING_DIR) \
			--data-size $(SIZE) \
			| tee -a  $(OUTPUT_DIR)/is-lmdb-$(SIZE); \
	done
	rm -rf $(INGEST_SCALING_DIR)

is-rocksdb:
	rm -f $(OUTPUT_DIR)/is-rocksdb$(THREADS)-$(SIZE)
	touch $(OUTPUT_DIR)/is-rocksdb$(THREADS)-$(SIZE)
	for number in 1 2 3 4 5; do \
		echo RUN $$number; \
		rm -rf $(INGEST_SCALING_DIR); \
		mkdir -p $(INGEST_SCALING_DIR); \
		./target/release/ingest-scaling \
			--db rocksdb \
			--storage-dir $(INGEST_SCALING_DIR) \
			--data-size $(SIZE) \
			--threads $(THREADS) \
			| tee -a  $(OUTPUT_DIR)/is-rocksdb$(THREADS)-$(SIZE); \
	done
	rm -rf $(INGEST_SCALING_DIR)

is-mach:
	rm -f $(OUTPUT_DIR)/is-mach-$(SIZE)
	touch $(OUTPUT_DIR)/is-mach-$(SIZE)
	for number in 1 2 3 4 5; do \
		echo RUN $$number; \
		rm -rf $(INGEST_SCALING_DIR); \
		mkdir -p $(INGEST_SCALING_DIR); \
		./target/release/ingest-scaling \
			--db mach \
			--storage-dir $(INGEST_SCALING_DIR) \
			--data-size $(SIZE) \
			| tee -a  $(OUTPUT_DIR)/is-mach-$(SIZE); \
	done
	rm -rf $(INGEST_SCALING_DIR)

is-fishstore:
	rm -f $(OUTPUT_DIR)/is-fishstore$(THREADS)-$(SIZE)
	touch $(OUTPUT_DIR)/is-fishstore$(THREADS)-$(SIZE)
	cd third-party/FishStore/build/Release; \
	for number in 1 2 3 4 5; do \
		echo RUN $$number; \
		rm -rf $(INGEST_SCALING_DIR); \
		mkdir -p $(INGEST_SCALING_DIR); \
		./hft ingest_scaling $(INGEST_SCALING_DIR) $(SIZE) $(THREADS) \
			| tee -a  $(OUTPUT_DIR)/is-fishstore$(THREADS)-$(SIZE);\
	done
	rm -rf $(INGEST_SCALING_DIR)


####################################
# Commands for Ablation Evaluation #
####################################

ab-build: e2e-build

ab-mach-app:
	rm -rf $(MACH_DIR)
	mkdir -p $(MACH_DIR)
	taskset -c 0-7 target/release/mach-app \
		--addr $(TCP_SERVER_ADDR) \
		--mach-path $(MACH_DIR) \
		--query $(QUERY) \
		--ablation-lookback $(LOOKBACK) \
		| tee $(OUTPUT_DIR)/ab-mach-$(QUERY)-$(LOOKBACK)
	rm -rf $(MACH_DIR)

###################################
# Commands for Generating Figures #
###################################

figures:
	cd figures; \
	python3 parse-output.py; \
	python3 rocksdb-end-to-end-query.py; \
	python3 valkey-end-to-end-query.py; \
	python3 probe-effect.py; \
	python3 ingest-scaling.py; \
	python3 ablation.py;



.PHONY: mach-app influx-app figures
