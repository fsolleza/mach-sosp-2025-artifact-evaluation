// SPDX-License-Identifier: GPL-2.0
// Copyright (c) 2019 Facebook
#include "vmlinux.h"
#include <bpf/bpf_core_read.h>
#include <bpf/bpf_helpers.h>
//#include <linux/sched.h>
char LICENSE[] SEC("license") = "Dual BSD/GPL";

const volatile uint32_t my_pid = 0;

// from: /sys/kernel/debug/tracing/events/filemap/mm_filemap_delete_from_page_cache/format
//        field:unsigned short common_type;       offset:0;       size:2; signed:0;
//        field:unsigned char common_flags;       offset:2;       size:1; signed:0;
//        field:unsigned char common_preempt_count;       offset:3;       size:1; signed:0;
//        field:int common_pid;   offset:4;       size:4; signed:1;
//
//        field:unsigned long pfn;        offset:8;       size:8; signed:0;
//        field:unsigned long i_ino;      offset:16;      size:8; signed:0;
//        field:unsigned long index;      offset:24;      size:8; signed:0;
//        field:dev_t s_dev;      offset:32;      size:4; signed:0;

struct mm_filemap_delete_from_page_cache_ctx {
	uint64_t pad;
	int64_t pfn;
	int64_t i_ino;
	int64_t index;
	int32_t s_dev;
};

struct page_cache_event {
	uint32_t pid;
	uint32_t tid;
	uint64_t timestamp;
	int64_t pfn;
	int64_t i_ino;
	int64_t index;
	int32_t s_dev;
};

struct delete_from_page_cache_event_buffer {
	uint32_t length;
	struct page_cache_event buffer[256];
};

// PERF_EVENT_ARRAY to communicate with userspace
struct {
	__uint(type, BPF_MAP_TYPE_PERF_EVENT_ARRAY);
	__uint(key_size, sizeof(int));
	__uint(value_size, sizeof(int));
} perf_buffer SEC(".maps");

struct {
	__uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
	__type(key, __u32);
	__type(value, struct delete_from_page_cache_event_buffer);
	__uint(max_entries, 1);
} file_map_delete_buffers SEC(".maps");

SEC("tp/filemap/mm_filemap_add_to_page_cache")
int handle_delete_from_page_cache(struct mm_filemap_delete_from_page_cache_ctx * ctx) {
	struct task_struct* task = (struct task_struct*)bpf_get_current_task();
	uint32_t pid = 0;
	uint32_t tid = 0;
	bpf_probe_read(&pid, sizeof(pid), &task->tgid);
	bpf_probe_read(&tid, sizeof(pid), &task->pid);

	if (pid != my_pid) {
		return 0;
	}

	struct page_cache_event e = {0};
	e.pid = pid;
	e.tid = tid;
	e.timestamp = bpf_ktime_get_ns();
	bpf_probe_read(&e.pfn, sizeof(uint64_t), &ctx->pfn);
	bpf_probe_read(&e.i_ino, sizeof(uint64_t), &ctx->i_ino);
	bpf_probe_read(&e.index, sizeof(uint64_t), &ctx->index);
	bpf_probe_read(&e.s_dev, sizeof(uint32_t), &ctx->s_dev);

	int zero = 0;
	struct delete_from_page_cache_event_buffer *buffer =
		bpf_map_lookup_elem(&file_map_delete_buffers, &zero);
	if (!buffer) {
		return 0;
	}

	if (buffer->length < 256) {
		buffer->buffer[buffer->length] = e;
		buffer->length += 1;
	}

	if (buffer->length == 256) {
		bpf_perf_event_output(ctx, &perf_buffer, BPF_F_CURRENT_CPU, buffer, sizeof(*buffer));
		buffer->length = 0;
	}
}
