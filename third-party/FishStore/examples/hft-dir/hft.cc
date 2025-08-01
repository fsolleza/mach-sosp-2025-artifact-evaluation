#undef _NULL_DISK

#include <cstring>
#include <iostream>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <cstdio>
#include <experimental/filesystem>
#include <fstream>
#include <cstdint>
#include <thread>
#include <atomic>
#include <rigtorp/MPMCQueue.h>
#include <filesystem>
#include <random>

#include "core/fishstore.h"
#include "adapters/common_utils.h"

#include "helpers.h"
#include "parser_adapter.h"
#include "tcp_server.h"
#include "scan_contexts.h"


using namespace fishstore;

typedef environment::QueueIoHandler handler_t;
typedef device::FileSystemDisk<handler_t, 1073741824L> disk_t;
typedef ParserAdapter adapter_t;

using store_t = core::FishStore<disk_t, adapter_t>;
using ingest_scaling_store_t = core::FishStore<disk_t, IngestScalingParserAdapter>;
using random_bytes_engine = std::independent_bits_engine<
    std::default_random_engine, CHAR_BIT, unsigned char>;

std::atomic<uint64_t> CURRENT_TIME(0);

void counter(bool &done) {
	uint64_t total_received = 0;
	uint64_t total_dropped = 0;
	bool printed_zero = false;
	while (!done) {
		uint64_t received = TCP_RECEIVED.exchange(0);
		uint64_t dropped = TCP_DROPPED.exchange(0);
		uint64_t written = received - dropped;
		total_received += received;
		total_dropped += dropped;
		uint64_t total_written = total_received - total_dropped;

		if (received > 0) {
			printed_zero = false;
			printf("Received: %ld, Written: %ld, Dropped: %ld, total received %ld dropped %ld\n", received, written, dropped, total_received, total_dropped);
		} else if (!printed_zero) {
			printed_zero = true;
			printf("Ingest paused...\n");
		}

		sleep(10);
	}
}

void ingest(store_t &store, data_queue_t &data_queue) {

	auto callback = [](IAsyncContext * ctxt, Status result) {
		assert(false);
	};
	store.StartSession();
	uint64_t serial_num = 0;
	for (;;) {
		serial_num += 1;
		DataBuffer data;
		data_queue.wait_dequeue(data);

		const char *buf = (const char *)data.buffer;
		uint32_t x = store.BatchInsert(
			buf,
			data.length,
			serial_num
		);
		if (serial_num % 4096 == 0) {
			store.Refresh();
			store.CompletePending(true);
		}

		DataBuffer_drop(data);
	}
	store.StopSession();
}


void timed_ingest(store_t &store, data_queue_t &data_queue, std::atomic<uint64_t> &total_count) {
	auto callback = [](IAsyncContext * ctxt, Status result) {
		assert(false);
	};
	store.StartSession();
	double seconds = 240;
	uint64_t serial_num = 0;
	bool first = true;
	auto begin = std::chrono::high_resolution_clock::now();
	uint64_t count = 0;
	for (;;) {
		serial_num += 1;
		DataBuffer data;
		data_queue.wait_dequeue(data);
		if (first) {
			first = false;
			begin = std::chrono::high_resolution_clock::now();
		}

		const char *buf = (const char *)data.buffer;
		uint32_t x = store.BatchInsert(
			buf,
			data.length,
			serial_num
		);
		store.CompletePending(true);

		count += DataBuffer_item_count(&data);
		DataBuffer_drop(data);

		auto end = std::chrono::high_resolution_clock::now();
		double d = std::chrono::duration<double>(end - begin).count();
		if (d > seconds) {
			break;
		}
	}
	auto end = std::chrono::high_resolution_clock::now();
	double d = std::chrono::duration<double>(end - begin).count();
	total_count.fetch_add(count);
	printf("Thread ending in %f, count %ld\n", d, count);
	store.StopSession();
}


std::atomic<uint64_t> START_ADDR(0);
void get_start_address_in(store_t &store, uint64_t seconds) {
	std::this_thread::sleep_for(std::chrono::seconds(seconds));
	uint64_t a = store.Size();
	START_ADDR.exchange(a);
}

std::atomic<uint64_t> END_ADDR(0);
void get_end_address_in(store_t &store, uint64_t seconds) {
	std::this_thread::sleep_for(std::chrono::seconds(seconds));
	uint64_t a = store.Size();
	END_ADDR.exchange(a);
}

void rocksdb_q1(
	store_t &store,
	uint32_t predicate_id,
	uint64_t start_addr,
	uint64_t end_addr,
	uint64_t ts_min,
	uint64_t ts_max
)
{
	auto begin = std::chrono::high_resolution_clock::now();
	store.StartSession();
	auto callback = [](IAsyncContext * ctxt, Status result) {
		assert(result == Status::Ok);
	};

	RocksDBQ1ScanContext ctx { predicate_id, 0, ts_min, ts_max};
	auto status = store.Scan(ctx, callback, 1, start_addr, end_addr);

	printf("Got status: %d\n", (int)status);
	store.CompletePending(true);
	printf("Done with complete pending\n");
	store.StopSession();
	auto end = std::chrono::high_resolution_clock::now();
	double d = std::chrono::duration<double>(end - begin).count();
	printf("Query Latency (Application Max Latency and Tail Latency): %.6f secs \n", d);
}

void rocksdb_q2(
	store_t &store,
	uint32_t predicate_id,
	uint64_t start_addr,
	uint64_t end_addr,
	uint64_t ts_min,
	uint64_t ts_max
)
{
	auto begin = std::chrono::high_resolution_clock::now();
	store.StartSession();
	auto callback = [](IAsyncContext * ctxt, Status result) {
		assert(result == Status::Ok);
	};

	RocksDBQ2ScanContext ctx { predicate_id, 0, ts_min, ts_max};
	auto status = store.Scan(ctx, callback, 1, start_addr, end_addr);

	printf("Got status: %d\n", (int)status);
	store.CompletePending(true);
	printf("Done with complete pending\n");
	store.StopSession();
	auto end = std::chrono::high_resolution_clock::now();
	double d = std::chrono::duration<double>(end - begin).count();
	printf("Query Latency (pread64 Max Latency and Tail Latency): %.6f secs \n", d);
}

void rocksdb_q3(
	store_t &store,
	uint32_t predicate_id,
	uint64_t start_addr,
	uint64_t end_addr,
	uint64_t ts_min,
	uint64_t ts_max
)
{
	auto begin = std::chrono::high_resolution_clock::now();
	store.StartSession();
	auto callback = [](IAsyncContext * ctxt, Status result) {
		assert(result == Status::Ok);
	};

	RocksDBQ3ScanContext ctx { predicate_id, 0, ts_min, ts_max};
	auto status = store.Scan(ctx, callback, 1, start_addr, end_addr);

	printf("Got status: %d\n", (int)status);
	store.CompletePending(true);
	printf("Done with complete pending\n");
	store.StopSession();
	auto end = std::chrono::high_resolution_clock::now();
	double d = std::chrono::duration<double>(end - begin).count();
	printf("Query Latency (Page Cache Event Count): %.6f secs \n", d);
}

void valkey_q1(
	store_t &store,
	uint32_t predicate_id,
	uint64_t start_addr,
	uint64_t end_addr,
	uint64_t ts_min,
	uint64_t ts_max
)
{
	store.StartSession();
	auto callback = [](IAsyncContext * ctxt, Status result) {
		assert(result == Status::Ok);
	};

	uint64_t tile = 0;
	std::vector<uint64_t> durations;
	ValkeyQ1ScanContext ctx { predicate_id, 0, ts_min, ts_max, &tile, durations};
	auto begin = std::chrono::high_resolution_clock::now();
	auto status = store.Scan(ctx, callback, 1, start_addr, end_addr);
	store.CompletePending(true);
	auto end = std::chrono::high_resolution_clock::now();
	double d = std::chrono::duration<double>(end - begin).count();

	printf("Query Latency (Page Cache Event Count): %.6f secs \n", d);

	printf("Done with complete pending\n");
	printf("Tile: %ld Number of u64 in memory: %ld\n", tile, durations.size());
	printf("Query Latency (Slow Requests): %.6f secs \n", d);

	// We already have all the data we need to perform the "scan" since we
	// pushed all the data in the durations vec

	store.StopSession();
}

void valkey_q2(
	store_t &store,
	uint32_t predicate_id_1,
	uint32_t predicate_id_2,
	uint64_t start_addr,
	uint64_t end_addr,
	uint64_t ts_min,
	uint64_t ts_max
)
{
	store.StartSession();
	auto callback = [](IAsyncContext * ctxt, Status result) {
		assert(result == Status::Ok);
	};

	double dur1, dur2;
	// We reuse the scan context in valkey q1
	{
		uint64_t tile = 0;
		std::vector<uint64_t> durations;
		ValkeyQ1ScanContext ctx { predicate_id_1, 0, ts_min, ts_max, &tile, durations};

		auto begin = std::chrono::high_resolution_clock::now();
		auto status = store.Scan(ctx, callback, 1, start_addr, end_addr);
		store.CompletePending(true);
		auto end = std::chrono::high_resolution_clock::now();
		double d = std::chrono::duration<double>(end - begin).count();
		dur1 = d;
		printf("Got status: %d\n", (int)status);
		printf("KV scan Tile: %ld Number of u64 in memory: %ld\n", tile, durations.size());
	}

	// We already have all the data we need to perform the "scan" since we
	// pushed all the data in the durations vec
	{
		uint64_t tile = 0;
		std::vector<uint64_t> durations;
		ValkeyQ2ScanContext ctx { predicate_id_2, 0, ts_min, ts_max, &tile, durations};

		auto begin = std::chrono::high_resolution_clock::now();

		auto status = store.Scan(ctx, callback, 1, start_addr, end_addr);
		store.CompletePending(true);
		auto end = std::chrono::high_resolution_clock::now();
		double d = std::chrono::duration<double>(end - begin).count();
		printf("Got status: %d\n", (int)status);
		dur2 = d;
	}

	printf("Query Latency (Slow Requests): %.6f secs \n", dur1);
	printf("Query Latency (Slow sendto Execution): %.6f secs \n", dur2);

	store.StopSession();
}

void valkey_q3(
	store_t &store,
	uint32_t predicate_id_1,
	uint32_t predicate_id_2,
	uint64_t start_addr,
	uint64_t end_addr,
	uint64_t ts_min,
	uint64_t ts_max
)
{
	store.StartSession();
	auto callback = [](IAsyncContext * ctxt, Status result) {
		assert(result == Status::Ok);
	};

	// We reuse the scan context in valkey q1
	double dur1, dur2;
	uint64_t out_max = 0;
	uint64_t out_ts = 0;
	{
		ValkeyQ3MaxScanContext ctx { predicate_id_1, 0, ts_min, ts_max, &out_max, &out_ts};

		auto begin = std::chrono::high_resolution_clock::now();
		auto status = store.Scan(ctx, callback, 1, start_addr, end_addr);
		store.CompletePending(true);
		auto end = std::chrono::high_resolution_clock::now();
		double d = std::chrono::duration<double>(end - begin).count();
		dur1 = d;
		printf("Got status: %d\n", (int)status);
	}

	{
		uint64_t packet_ts_min = out_ts - 5000000;
		uint64_t packet_ts_max = out_ts + 5000000;
		uint64_t count = 0;
		ValkeyQ3PacketScanContext ctx { predicate_id_2, 0, packet_ts_min, packet_ts_max, &count };

		auto begin = std::chrono::high_resolution_clock::now();
		auto status = store.Scan(ctx, callback, 1, start_addr, end_addr);
		store.CompletePending(true);
		auto end = std::chrono::high_resolution_clock::now();
		double d = std::chrono::duration<double>(end - begin).count();
		dur2 = d;

		printf("Got status: %d\n", (int)status);
	}

	printf("Query Latency (Maximum Latency Request): %.6f secs \n", dur1);
	printf("Query Latency (TCP Packet Dump): %.6f secs \n", dur2);
	store.StopSession();
}

void microbenchmark(
	store_t &store,
	uint32_t predicate_id,
	uint64_t ts_min,
	uint64_t ts_max
)
{
	STOP_INGEST.exchange(1);

	store.StartSession();
	auto callback = [](IAsyncContext * ctxt, Status result) {
		assert(result == Status::Ok);
	};

	uint64_t count = 0;
	MicrobenchmarkScanContext ctx { predicate_id, 0, ts_min, ts_max, &count };
	auto status = store.Scan(ctx, callback, 1);

	printf("Got status: %d\n", (int)status);
	store.CompletePending(true);
	printf("Done with complete pending\n");
	printf("Result: %ld\n", count);
	store.StopSession();
}

enum RunMode {
	NoIndex = 0,
	RunModeRocksDBP1 = 1,
	RunModeRocksDBP2 = 2,
	RunModeRocksDBP3 = 3,
	RunModeValkeyP1 = 4,
	RunModeValkeyP2 = 5,
	RunModeValkeyP3 = 6,
	RunModeExactMicrobench20 = 7,
	RunModeExactMicrobench60 = 8,
	RunModeExactMicrobench120 = 9,
	RunModeExactMicrobench300 = 10,
	RunModeIndexIngest1 = 11,
	RunModeIndexIngest4 = 12,
	RunModeIndexIngest10 = 13,
	RunModeIndexIngest20 = 14,
	RunModeExactMicrobench600 = 15,
};

RunMode RunMode_from_str(char* v) {
	if (strcmp(v, "no-index") == 0) {
		return NoIndex;
	}
	if (strcmp(v, "rocksdb-p1") == 0) {
		return RunModeRocksDBP1;
	}
	if (strcmp(v, "rocksdb-p2") == 0) {
		return RunModeRocksDBP2;
	}
	if (strcmp(v, "rocksdb-p3") == 0) {
		return RunModeRocksDBP3;
	}
	if (strcmp(v, "valkey-p1") == 0) {
		return RunModeValkeyP1;
	}
	if (strcmp(v, "valkey-p2") == 0) {
		return RunModeValkeyP2;
	}
	if (strcmp(v, "valkey-p3") == 0) {
		return RunModeValkeyP3;
	}
	if (strcmp(v, "exact-microbenchmark-20") == 0) {
		return RunModeExactMicrobench20;
	}
	if (strcmp(v, "exact-microbenchmark-60") == 0) {
		return RunModeExactMicrobench60;
	}
	if (strcmp(v, "exact-microbenchmark-120") == 0) {
		return RunModeExactMicrobench120;
	}
	if (strcmp(v, "exact-microbenchmark-300") == 0) {
		return RunModeExactMicrobench300;
	}
	if (strcmp(v, "exact-microbenchmark-600") == 0) {
		return RunModeExactMicrobench600;
	}
	if (strcmp(v, "index-ingest-1") == 0) {
		return RunModeIndexIngest1;
	}
	if (strcmp(v, "index-ingest-4") == 0) {
		return RunModeIndexIngest4;
	}
	if (strcmp(v, "index-ingest-10") == 0) {
		return RunModeIndexIngest10;
	}
	if (strcmp(v, "index-ingest-20") == 0) {
		return RunModeIndexIngest20;
	}
	printf("Invalid run mode %s\n", v);
	exit(1);
}



void ingest_scaling_loop(ingest_scaling_store_t &store, uint8_t* random_data, uint64_t data_size, uint64_t batch_size, double duration_s, std::atomic<uint64_t> &total_count) {

	printf("Running ingest thread for data size %ld for %f duration\n", data_size, duration_s);
	store.StartSession();
	uint8_t *buffer = new uint8_t[data_size + 8];
	uint64_t offset = 0;
	U64Bytes data_size_bytes = u64_to_be_bytes((uint64_t)data_size);
	memcpy(buffer, data_size_bytes.arr, 8);
	offset += 8;
	memcpy(buffer + offset, random_data, data_size);
	offset += data_size;

	uint64_t count = 0;

	auto last_sync = std::chrono::high_resolution_clock::now();
	auto start = std::chrono::high_resolution_clock::now();
	for (;;) {
		// offset += data_size;
		// count += 1;

		// if (count % batch_size == 0) {
		// 	uint32_t x = store.BatchInsert((const char*)buffer, offset, count);
		// 	offset = 8;
		// 	auto end = std::chrono::high_resolution_clock::now();
		// 	store.Refresh();
		// 	store.CompletePending(true);
		// 	double d = std::chrono::duration<double>(end - start).count();
		// 	if (d > duration_s) {
		// 		break;
		// 	}
		// }

		uint32_t x = store.BatchInsert((const char*)buffer, offset, count);
		count += 1;
		auto end = std::chrono::high_resolution_clock::now();
		double d = std::chrono::duration<double>(end - start).count();
		if (d > duration_s) {
			break;
		}

		if (count % batch_size == 0) {
			//uint32_t x = store.BatchInsert((const char*)buffer, offset, count);
			//offset = 8;
			store.Refresh();
			store.CompletePending(true);
		}
	}
	store.Refresh();
	store.CompletePending(true);
	store.StopSession();

	auto end = std::chrono::high_resolution_clock::now();
	double d = std::chrono::duration<double>(end - start).count();

	printf("Count: %ld in %f seconds\n", count, d);
	total_count.fetch_add(count);

	delete buffer;
}


void ingest_scaling(int data_size, const char* path, int ingest_threads) {
	size_t hash_table_size = 1LL << 24;
	size_t in_mem_buffer_size = 1 * (1LL << 30);
	double duration_s = 60;
	// int ingest_threads = 3;

	std::filesystem::remove_all(path);
	std::filesystem::create_directories(path);
	ingest_scaling_store_t store {hash_table_size, in_mem_buffer_size, path};

	store.StartSession();
	uint64_t lib_id = store.LoadPSFLibrary("./libpsf_lib.so");

	std::vector<ParserAction> parser_actions;
	uint32_t p1 = store.MakeInlinePSF({ "field" }, lib_id, "psf7");
	parser_actions.push_back({ REGISTER_INLINE_PSF, p1 });
	uint64_t safe_register_address, safe_unregister_address;
	safe_unregister_address = store.ApplyParserShift(
		parser_actions,
		[&safe_register_address](uint64_t safe_address) {
			safe_register_address = safe_address;
		}
	);
	printf("Safe %ld, Unsafe %ld\n", safe_register_address, safe_unregister_address);
	store.CompleteAction(true);
	store.StopSession();

	uint8_t *random_data = new uint8_t[data_size];
	for (int i = 0; i < data_size; i++) {
		random_data[i] = rand();
	}

	std::atomic<uint64_t> total_count(0);
	std::vector<std::thread> thread_handles;
	printf("Launching %d ingest threads\n", ingest_threads);
	for (int i = 0; i < ingest_threads ; ++i) {
		thread_handles.push_back(
			std::thread([&] {
				ingest_scaling_loop(store, random_data, data_size, 1024, duration_s, total_count);
			})
		);
	}

	for (int i = 0; i < ingest_threads; ++i) {
		thread_handles[i].join();
	}

	uint64_t count = total_count.load();
	uint64_t size = count * data_size;

	double rps = static_cast<double>(count) / duration_s;
	double bps = static_cast<double>(size) / duration_s;

	printf("Result rps: %f bps: %f\n", rps, bps);

	printf("Exiting\n");
	delete random_data;
}

int main(int argc, char* argv[]) {
	setbuf(stdout, NULL);
	if (strcmp(argv[1], "ingest_scaling") == 0) {
		std::string path = argv[2];
		int data_size = atoi(argv[3]);
		int ingest_threads = atoi(argv[4]);
		ingest_scaling(data_size, path.c_str(), ingest_threads);
		return 0;
	}

	if (argc != 6) {
		printf("Usage: ./hft <ingest threads> <path> <port> <run mode> <with query (0, 1)>\n");
		exit(1);
	}

	std::string path = argv[2];
	int ingest_threads = atoi(argv[1]);
	in_port_t port = htons(atoi(argv[3]));
	RunMode mode = RunMode_from_str(argv[4]);
	bool with_query = atoi(argv[5]) == 1;

	size_t hash_table_size = 1LL << 24;
	size_t in_mem_buffer_size = 1 * (1LL << 30);

	printf("Initializing Fishstore\n");

	// FishStore constructor:
	// FishStore(size_t hash_table_size, size_t in_mem_buffer_size, const std::string& store_dst);
	std::filesystem::remove_all(argv[2]);
	std::filesystem::create_directories(argv[2]);
	store_t store {hash_table_size, in_mem_buffer_size, argv[2]};

	printf("Starting indexing");
	store.StartSession();

	uint64_t lib_id = store.LoadPSFLibrary("./libpsf_lib.so");

	std::vector<ParserAction> parser_actions;

	uint32_t p1, p2, p3;
	switch (mode) {
		case RunModeRocksDBP1:
			p1 = store.MakeInlinePSF({ "KvLogOp", "SyscallNumber", "RecordKind" }, lib_id, "psf1");
			parser_actions.push_back({ REGISTER_INLINE_PSF, p1 });
			break;
		case RunModeRocksDBP2:
			p1 = store.MakeInlinePSF({ "KvLogOp", "SyscallNumber", "RecordKind" }, lib_id, "psf1");
			p2 = store.MakeInlinePSF({ "KvLogOp", "SyscallNumber", "RecordKind" }, lib_id, "psf2");
			parser_actions.push_back({ REGISTER_INLINE_PSF, p1 });
			parser_actions.push_back({ REGISTER_INLINE_PSF, p2 });
			break;
		case RunModeRocksDBP3:
			p1 = store.MakeInlinePSF({ "KvLogOp", "SyscallNumber", "RecordKind" }, lib_id, "psf1");
			p2 = store.MakeInlinePSF({ "KvLogOp", "SyscallNumber", "RecordKind" }, lib_id, "psf2");
			p3 = store.MakeInlinePSF({ "KvLogOp", "SyscallNumber", "RecordKind" }, lib_id, "psf3");
			parser_actions.push_back({ REGISTER_INLINE_PSF, p1 });
			parser_actions.push_back({ REGISTER_INLINE_PSF, p2 });
			parser_actions.push_back({ REGISTER_INLINE_PSF, p3 });
			break;
		case RunModeValkeyP1:
			p1 = store.MakeInlinePSF({ "KvLogOp", "SyscallNumber", "RecordKind" }, lib_id, "psf1");
			parser_actions.push_back({ REGISTER_INLINE_PSF, p1 });
			break;
		case RunModeValkeyP2:
			p1 = store.MakeInlinePSF({ "KvLogOp", "SyscallNumber", "RecordKind" }, lib_id, "psf1");
			// This one indexes syscall 44
			p2 = store.MakeInlinePSF({ "KvLogOp", "SyscallNumber", "RecordKind" }, lib_id, "psf4");
			parser_actions.push_back({ REGISTER_INLINE_PSF, p1 });
			parser_actions.push_back({ REGISTER_INLINE_PSF, p2 });
			break;
		case RunModeValkeyP3:
			p1 = store.MakeInlinePSF({ "KvLogOp", "SyscallNumber", "RecordKind" }, lib_id, "psf1");
			p2 = store.MakeInlinePSF({ "KvLogOp", "SyscallNumber", "RecordKind" }, lib_id, "psf4");
			p3 = store.MakeInlinePSF({ "KvLogOp", "SyscallNumber", "RecordKind" }, lib_id, "psf5");
			parser_actions.push_back({ REGISTER_INLINE_PSF, p1 });
			parser_actions.push_back({ REGISTER_INLINE_PSF, p2 });
			parser_actions.push_back({ REGISTER_INLINE_PSF, p3 });
			break;
		case RunModeExactMicrobench20:
		case RunModeExactMicrobench60:
		case RunModeExactMicrobench120:
		case RunModeExactMicrobench300:
		case RunModeExactMicrobench600:
			p1 = store.MakeInlinePSF({ "KvLogOp", "KvLogDuration" }, lib_id, "psf6");
			parser_actions.push_back({ REGISTER_INLINE_PSF, p1 });
			break;

		case RunModeIndexIngest1:
			p1 = store.MakeInlinePSF(
				{ "KvLogDuration", "SyscallNumber", "RecordKind", "SyscallDuration" },
				lib_id, "IndexIngest1"
			);
			parser_actions.push_back({ REGISTER_INLINE_PSF, p1 });
			break;

		case RunModeIndexIngest4:
			p1 = store.MakeInlinePSF(
				{ "KvLogDuration", "SyscallNumber", "RecordKind", "SyscallDuration" },
				lib_id, "IndexIngest4"
			);
			parser_actions.push_back({ REGISTER_INLINE_PSF, p1 });
			break;

		case RunModeIndexIngest10:
			p1 = store.MakeInlinePSF(
				{ "KvLogDuration", "SyscallNumber", "RecordKind", "SyscallDuration" },
				lib_id, "IndexIngest10"
			);
			parser_actions.push_back({ REGISTER_INLINE_PSF, p1 });
			break;

		case RunModeIndexIngest20:
			p1 = store.MakeInlinePSF(
				{ "KvLogDuration", "SyscallNumber", "RecordKind", "SyscallDuration" },
				lib_id, "IndexIngest20"
			);
			parser_actions.push_back({ REGISTER_INLINE_PSF, p1 });
			break;

		case NoIndex:
			break;
		default:
			printf("Unhandled mode\n");
			exit(1);
	}

	if (mode != NoIndex)
	{
		uint64_t safe_register_address, safe_unregister_address;
		safe_unregister_address = store.ApplyParserShift(
			 parser_actions,
			 [&safe_register_address](uint64_t safe_address)
			 {
				safe_register_address = safe_address;
			 }
		);
		printf("Safe %ld, Unsafe %ld\n", safe_register_address, safe_unregister_address);
	}
	store.CompleteAction(true);
	store.StopSession();

	printf("Setup done... beginning writing\n");
	bool done = false;
	std::thread c([&] { counter(done); });

	data_queue_t data_queue(8 * 1024);
	std::thread tcp_server_thread([&] {
				      tcp_server(port, data_queue);
				      });

	std::vector<std::thread> thread_handles;
	printf("Starting up %d ingest threads\n", ingest_threads);

	std::atomic<uint64_t> total_count{0};
	switch (mode) {
		case RunModeIndexIngest1:
		case RunModeIndexIngest4:
		case RunModeIndexIngest10:
		case RunModeIndexIngest20:
			for (int t = 0; t < ingest_threads; ++t) {
				thread_handles.push_back(
					std::thread([&] { timed_ingest(store, data_queue, total_count); })
				);
			}
			break;
		default:
			for (int t = 0; t < ingest_threads; ++t) {
				thread_handles.push_back(
					std::thread([&] { ingest(store, data_queue); })
				);
			}
			break;
	}

	std::vector<double> durations;

	if (!with_query) {
		for (int t = 0; t < ingest_threads; ++t) {
			thread_handles[t].join();
		}
		done = true;
		uint64_t t = total_count.load();
		printf("Total count: %ld\n", t);
		return 0;
	}

	if (mode == RunModeExactMicrobench20) { // || mode == RunModeMicrobench120) {

		// Lookback of 20 seconds
		// We sleep for at least 20 + 120 seconds. We add some slack so
		// we sleep for 20 + 120 + 15. Our query will add 5 seconds of
		// lookback. Scan should complete when data is 10 seconds from
		// the end.
		printf("Exact indexing lookback of 20 seconds\n");
		printf("Sleeping for %d seconds\n", 20 + 120 + 5);
		std::this_thread::sleep_for(std::chrono::seconds(20 + 120 + 5));

		uint64_t us_since_epoch =
			std::chrono::system_clock::now().time_since_epoch() /
			std::chrono::microseconds(1);
		uint64_t ts_max = us_since_epoch - (20 + 5) * 1000000;
		uint64_t ts_min = ts_max - 120 * 1000000;
		printf("Max ts: %ld\n", ts_max);
		printf("min ts: %ld\n", ts_min);

		auto begin = std::chrono::high_resolution_clock::now();
		microbenchmark(store, p1, ts_min, ts_max);
		auto end = std::chrono::high_resolution_clock::now();
		double d = std::chrono::duration<double>(end - begin).count();
		printf("Scan done in %.6f secs\n", d);
		return 0;
	}

	if (mode == RunModeExactMicrobench60) { 

		// Lookback of 60s. See Lookback of 20 for explanation
		printf("Exact indexing lookback of 60 seconds\n");
		printf("Sleeping for %d seconds\n", 60 + 120 + 5);
		std::this_thread::sleep_for(std::chrono::seconds(60 + 120 + 5));

		uint64_t us_since_epoch =
			std::chrono::system_clock::now().time_since_epoch() /
			std::chrono::microseconds(1);
		uint64_t ts_max = us_since_epoch - (60 + 5) * 1000000;
		uint64_t ts_min = ts_max - 120 * 1000000;
		printf("Max ts: %ld\n", ts_max);
		printf("min ts: %ld\n", ts_min);

		auto begin = std::chrono::high_resolution_clock::now();
		microbenchmark(store, p1, ts_min, ts_max);
		auto end = std::chrono::high_resolution_clock::now();
		double d = std::chrono::duration<double>(end - begin).count();
		printf("Scan done in %.6f secs\n", d);
		return 0;
	}

	if (mode == RunModeExactMicrobench120) { 

		// Lookback of 120s
		printf("Exact indexing lookback of 120 seconds\n");
		printf("Sleeping for %d seconds\n", 120 + 120 + 5);
		std::this_thread::sleep_for(std::chrono::seconds(120 + 120 + 5));

		uint64_t us_since_epoch =
			std::chrono::system_clock::now().time_since_epoch() /
			std::chrono::microseconds(1);
		uint64_t ts_max = us_since_epoch - (120 + 5) * 1000000;
		uint64_t ts_min = ts_max - 120 * 1000000;
		printf("Max ts: %ld\n", ts_max);
		printf("min ts: %ld\n", ts_min);

		auto begin = std::chrono::high_resolution_clock::now();
		microbenchmark(store, p1, ts_min, ts_max);
		auto end = std::chrono::high_resolution_clock::now();
		double d = std::chrono::duration<double>(end - begin).count();
		printf("Scan done in %.6f secs\n", d);
		return 0;
	}

	if (mode == RunModeExactMicrobench300) { 

		// Lookback for 300 seconds
		printf("Exact indexing lookback of 300 seconds\n");
		printf("Sleeping for %d seconds\n", 300 + 120 + 15);
		std::this_thread::sleep_for(std::chrono::seconds(300 + 120 + 15));

		uint64_t us_since_epoch =
			std::chrono::system_clock::now().time_since_epoch() /
			std::chrono::microseconds(1);
		uint64_t ts_max = us_since_epoch - (300 + 5) * 1000000;
		uint64_t ts_min = ts_max - 120 * 1000000;
		printf("Max ts: %ld\n", ts_max);
		printf("min ts: %ld\n", ts_min);

		auto begin = std::chrono::high_resolution_clock::now();
		microbenchmark(store, p1, ts_min, ts_max);
		auto end = std::chrono::high_resolution_clock::now();
		double d = std::chrono::duration<double>(end - begin).count();
		printf("Scan done in %.6f secs\n", d);
		return 0;
	}

	if (mode == RunModeExactMicrobench600) { 

		// Lookback for 600 seconds
		printf("Exact indexing lookback of 600 seconds\n");
		printf("Sleeping for %d seconds\n", 600 + 120 + 15);
		std::this_thread::sleep_for(std::chrono::seconds(600 + 120 + 15));

		uint64_t us_since_epoch =
			std::chrono::system_clock::now().time_since_epoch() /
			std::chrono::microseconds(1);
		uint64_t ts_max = us_since_epoch - (600 + 5) * 1000000;
		uint64_t ts_min = ts_max - 120 * 1000000;
		printf("Max ts: %ld\n", ts_max);
		printf("min ts: %ld\n", ts_min);

		auto begin = std::chrono::high_resolution_clock::now();
		microbenchmark(store, p1, ts_min, ts_max);
		auto end = std::chrono::high_resolution_clock::now();
		double d = std::chrono::duration<double>(end - begin).count();
		printf("Scan done in %.6f secs\n", d);
		return 0;
	}



	for (int i = 0; i < 5; ++i) {
		// We have no way of stopping a scan as it executes so we need
		// jump a few hoops to calculate an address to stop the scan at.
		// Look back is 80 seconds so 120 - 80 = 40	
		std::thread t1([&] { get_start_address_in(store, 40); } );
		std::thread t2([&] { get_end_address_in(store, 100); } );

		// Collect data for 120 seconds
		std::this_thread::sleep_for(std::chrono::seconds(120));

		// We need to stop ingest because Fishstore seems to not read
		// disk data when there are concurrent writers
		STOP_INGEST.exchange(1);

		uint64_t us_since_epoch =
			std::chrono::system_clock::now().time_since_epoch() /
			std::chrono::microseconds(1);
		uint64_t ts_min = us_since_epoch - 80 * 1000000;
		uint64_t ts_max = us_since_epoch - 20 * 1000000;

		std::this_thread::sleep_for(std::chrono::seconds(10));
		printf("Querying\n");

		uint64_t start_addr = START_ADDR.load();
		uint64_t end_addr = END_ADDR.load();
		t1.join();
		t2.join();
		if (start_addr == 0) {
			printf("Start addr was not set\n");
			exit(1);
		}

		switch (mode) {
			case (RunModeRocksDBP1):
				rocksdb_q1(store, p1, start_addr, end_addr, ts_min, ts_max);
				break;
			case (RunModeRocksDBP2):
				rocksdb_q2(store, p2, start_addr, end_addr, ts_min, ts_max);
				break;
			case (RunModeRocksDBP3):
				rocksdb_q3(store, p3, start_addr, end_addr, ts_min, ts_max);
				break;
			case (RunModeValkeyP1):
				valkey_q1(store, p1, start_addr, end_addr, ts_min, ts_max);
				break;
			case (RunModeValkeyP2):
				valkey_q2(store, p1, p2, start_addr, end_addr, ts_min, ts_max);
				break;
			case (RunModeValkeyP3):
				valkey_q3(store, p1, p3, start_addr, end_addr, ts_min, ts_max);
				break;
			default:
				printf("Unhandled run mode\n");
				exit(1);
				break;
		}

		STOP_INGEST.exchange(0);
	}

	done = true;
	return 0;
}
