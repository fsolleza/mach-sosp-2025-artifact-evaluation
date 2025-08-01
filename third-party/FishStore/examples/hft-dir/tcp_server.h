#pragma once

#include <errno.h>
#include <string.h>
#include <rigtorp/MPMCQueue.h>

#include "blockingconcurrentqueue.h"
#include "core/fishstore.h"
#include "adapters/common_utils.h"

#include "helpers.h"

using namespace fishstore;

typedef environment::QueueIoHandler handler_t;
typedef device::FileSystemDisk<handler_t, 1073741824L> disk_t;
typedef ParserAdapter adapter_t;
using store_t = core::FishStore<disk_t, adapter_t>;

typedef moodycamel::BlockingConcurrentQueue<DataBuffer> data_queue_t;

std::atomic<uint64_t> STOP_INGEST(0);

std::atomic<uint64_t> KVLOG(0);
std::atomic<uint64_t> SYSCALL(0);
std::atomic<uint64_t> PAGECACHE(0);
std::atomic<uint64_t> PACKETCAP(0);

std::atomic<uint64_t> DROPPED_KVLOG(0);
std::atomic<uint64_t> DROPPED_SYSCALL(0);
std::atomic<uint64_t> DROPPED_PAGECACHE(0);
std::atomic<uint64_t> DROPPED_PACKETCAP(0);

std::atomic<uint64_t> TCP_RECEIVED(0);
std::atomic<uint64_t> TCP_DROPPED(0);

// void counter(bool &done) {
// 	uint64_t total_received = 0;
// 	uint64_t total_dropped = 0;
// 	bool printed_zero = false;
// 	while (!done) {
// 		uint64_t received = TCP_RECEIVED.exchange(0);
// 		uint64_t dropped = TCP_DROPPED.exchange(0);
// 		uint64_t written = received - dropped;
// 		total_received += received;
// 		total_dropped += dropped;
// 		uint64_t total_written = total_received - total_dropped;
// 
// 		if (received > 0) {
// 			printed_zero = false;
// 			printf("Received: %ld, Written: %ld, Dropped: %ld, Total Received: %ld Written: %ld Dropped %ld\n", received, written, dropped, total_received, total_written, total_dropped);
// 		} else if (!printed_zero) {
// 			printed_zero = true;
// 			printf("Ingest paused...\n");
// 		}
// 
// 		sleep(10);
// 	}
// }

// void ingest(store_t &store, data_queue_t &data_queue) {
// 
// 	auto callback = [](IAsyncContext * ctxt, Status result) {
// 		assert(false);
// 	};
// 	store.StartSession();
// 	uint64_t serial_num = 0;
// 	for (;;) {
// 		serial_num += 1;
// 		DataBuffer data;
// 		data_queue.wait_dequeue(data);
// 
// 		const char *buf = (const char *)data.buffer;
// 		uint32_t x = store.BatchInsert(
// 			buf,
// 			data.length,
// 			serial_num
// 		);
// 		if (serial_num % 256 == 0) {
// 			store.Refresh();
// 			store.CompletePending(true);
// 		}
// 
// 		DataBuffer_drop(data);
// 	}
// 	store.StopSession();
// }


// This assumes buffer is at least x bytes long,
// and that the socket is blocking.
int read_bytes(int socket, uint8_t *buffer, uint64_t x)
{
    int bytes_read = 0;
    int result;
    while (bytes_read < x)
    {
        result = read(socket, buffer + bytes_read, x - bytes_read);
        if (result < 1 )
        {
		return -1;
        }

        bytes_read += result;
    }
    return bytes_read;
}

void handle_conn(int conn_fd, data_queue_t &data_queue) {
	printf("Handling conn %d\n", conn_fd);

	U64Bytes start_signal = u64_to_be_bytes(1234);
	if (write(conn_fd, start_signal.arr, 8) != 8) {
		printf("Failed to send start signal");
		return;
	}
	printf("Sent start signal, beginning to receive data\n");

	for (;;) {

		int r = 0;

		// we assume that sizeof(length) will return 8 here.
		uint8_t header[40] = { 0 };
		r = read_bytes(conn_fd, header, 40);
		if (r < 0) {
			printf("Failed to read from conn %d\n", conn_fd);
			break;
		}

		uint64_t kvlog = u64_from_be_bytes(header);
		uint64_t syscall = u64_from_be_bytes(header + 8);
		uint64_t pagecache = u64_from_be_bytes(header + 16);
		uint64_t packet_cap = u64_from_be_bytes(header + 24);
		uint64_t sz = u64_from_be_bytes(header + 32);

		DataBuffer data = DataBuffer_new(sz);
		r = read_bytes(conn_fd, data.buffer, sz);
		if (r < 0) {
			printf("Failed to read from conn %d\n", conn_fd);
			break;
		}

		if (r != sz) {
			printf("Didn't read expected amount of data from conn %d\n", conn_fd);
			break;
		}

		if (STOP_INGEST.load() == 0) {
			uint64_t item_count = DataBuffer_item_count(&data);

			KVLOG.fetch_add(kvlog);
			SYSCALL.fetch_add(syscall);
			PAGECACHE.fetch_add(pagecache);
			PACKETCAP.fetch_add(packet_cap);

			TCP_RECEIVED.fetch_add(item_count);
			if (!data_queue.try_enqueue(data)) {
				DROPPED_KVLOG.fetch_add(kvlog);
				DROPPED_SYSCALL.fetch_add(syscall);
				DROPPED_PAGECACHE.fetch_add(pagecache);
				DROPPED_PACKETCAP.fetch_add(packet_cap);
				TCP_DROPPED.fetch_add(item_count);
				DataBuffer_drop(data);
			}
		}
	}

	printf("Done handling conn %d\n", conn_fd);
}

void tcp_server(in_port_t port, data_queue_t &data_queue) {
	// creating socket
	int sockfd = socket(AF_INET, SOCK_STREAM, 0);
	if (sockfd == -1) {
		printf("Socket creation failed\n");
		exit(0);
	}

	// specifying address
	sockaddr_in server_addr;
	bzero(&server_addr, sizeof(server_addr));

	server_addr.sin_family = AF_INET;
	server_addr.sin_port = port;
	server_addr.sin_addr.s_addr = INADDR_ANY;

	// binding to IP and port
	int bind_result = bind(
		sockfd,
		(const sockaddr*)&server_addr,
		sizeof(server_addr)
	);

	if (bind_result != 0) {
		printf("socket bind failed\n");
		printf("%s\n", strerror(errno));
        	exit(0);
	}

	// listening
	int listen_result = listen(sockfd, 128);
	if (listen_result != 0) {
		printf("failed to listen\n");
		exit(0);
	}

	printf("Waiting for a connection\n");
	sockaddr_in accept_addr;
	int accept_addr_len = sizeof(accept_addr);

	int conn_fd = accept(
		sockfd,
		(struct sockaddr *)&accept_addr,
		(socklen_t*)&accept_addr_len
	);

	if (conn_fd == -1) {
		printf("failed to accept\n");
		exit(0);
	}

	printf("Got a connection\n");
	handle_conn(conn_fd, data_queue);
}
