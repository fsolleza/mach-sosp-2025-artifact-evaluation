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

#include "core/fishstore.h"
#include "adapters/common_utils.h"

#include "helpers.h"
#include "parser_adapter.h"
#include "tcp_server.h"
#include "contexts.h"

using namespace fishstore;

typedef environment::QueueIoHandler handler_t;
typedef device::FileSystemDisk<handler_t, 1073741824L> disk_t;
typedef ParserAdapter adapter_t;
using store_t = core::FishStore<disk_t, adapter_t>;

std::atomic<uint64_t> CURRENT_TIME(0);

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

void rocksdb_q2(
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


	RocksDBQ2ScanContext ctx { predicate_id, 0, ts_min, ts_max};
	auto status = store.Scan(ctx, callback, 1, start_addr, end_addr);

	printf("Got status: %d\n", (int)status);
	store.CompletePending(true);
	printf("Done with complete pending\n");
	store.StopSession();
}

//void rocksdb_q3(
//	store_t &store,
//	uint32_t predicate_id,
//	uint64_t start_addr,
//	uint64_t end_addr,
//	uint64_t ts_min,
//	uint64_t ts_max
//)
//{
//	store.StartSession();
//	auto callback = [](IAsyncContext * ctxt, Status result) {
//		assert(result == Status::Ok);
//	};
//
//
//	RocksDBQ3ScanContext ctx { predicate_id, 0, ts_min, ts_max};
//	auto status = store.Scan(ctx, callback, 1, start_addr, end_addr);
//
//	printf("Got status: %d\n", (int)status);
//	store.CompletePending(true);
//	printf("Done with complete pending\n");
//	store.StopSession();
//}


int main(int argc, char* argv[]) {

	std::string path = argv[2];
	int ingest_threads = atoi(argv[1]);
	in_port_t port = htons(atoi(argv[3]));

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

	//auto p1 = store.MakeInlinePSF({ "KvLogDuration", "RecordKind", "SyscallNumber", "SyscallDuration", "KvLogOp" }, lib_id, "register_fields_psf");
	auto p2 = store.MakeInlinePSF({ "KvLogOp", "SyscallNumber", "RecordKind" }, lib_id, "psf2");
	parser_actions.push_back({ REGISTER_INLINE_PSF, p2 });

	//auto p3 = store.MakeInlinePSF({ "RecordKind" }, lib_id, "psf3");
	//parser_actions.push_back({ REGISTER_INLINE_PSF, p3 });

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

	printf("Setup done... beginning writing\n");
	bool done = false;
	std::thread c([&] { counter(done); });

	data_queue_t data_queue(4096);
	std::thread tcp_server_thread([&] {
		tcp_server(port, data_queue);
	});

	std::vector<std::thread> thread_handles;
	printf("Starting up ingest threads\n");
	for (int t = 0; t < ingest_threads; ++t) {
		thread_handles.push_back(
			std::thread([&] { ingest(store, data_queue); })
		);
	}

	std::vector<double> durations;

	for (int i = 0; i < 3; ++i) {
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
		auto begin = std::chrono::high_resolution_clock::now();
		printf("Q1 starting at %ld - %ld\n", start_addr, end_addr);

		rocksdb_q2(store, p2, start_addr, end_addr, ts_min, ts_max);

		//switch (mode) {
		//	// case (RunModeRocksDBP1):
		//	// 	rocksdb_q1(store, p1, start_addr, end_addr, ts_min, ts_max);
		//	// 	break;
		//	// case (RunModeRocksDBP2):
		//	// 	rocksdb_q2(store, p2, start_addr, end_addr, ts_min, ts_max);
		//	// 	break;
		//	case (RunModeRocksDBP3):
		//		rocksdb_q3(store, p3, start_addr, end_addr, ts_min, ts_max);
		//		break;
		//	default:
		//		printf("Unhandled run mode\n");
		//		exit(1);
		//		break;
		//}
		auto end = std::chrono::high_resolution_clock::now();
		double d = std::chrono::duration<double>(end - begin).count();
		printf("Scan done in %.6f secs\n", d);
		durations.push_back(d);

		STOP_INGEST.exchange(0);
	}

	printf("Durations: ");
	for (int i = 0; i < durations.size(); ++i) {
		printf("%f, ", durations[i]);
	}

	done = true;
	return 0;
}
