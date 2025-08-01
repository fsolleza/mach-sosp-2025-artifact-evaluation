// SPDX-License-Identifier: GPL-2.0
// Copyright (c) 2019 Facebook
#include "vmlinux.h"
#include <bpf/bpf_core_read.h>
#include <bpf/bpf_helpers.h>
//#include <linux/sched.h>
char LICENSE[] SEC("license") = "Dual BSD/GPL";

const volatile uint32_t my_pid = 0;
const volatile uint32_t my_tid = 0;

// from: /sys/kernel/debug/tracing/events/raw_syscalls/sys_enter/format
struct sys_enter_ctx {
	uint64_t pad;
	int64_t syscall_number;
	uint32_t args[6];
};

struct sys_exit_ctx {
	uint64_t pad;
	int64_t syscall_number;
	uint64_t ret;
};

struct syscall_event {
	uint32_t kind;
	uint32_t pid;
	uint32_t tid;
	uint64_t syscall_number;
	uint64_t timestamp;
};

struct syscall_event_buffer {
	uint32_t length;
	struct syscall_event buffer[256];
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
	__type(value, struct syscall_event_buffer);
	__uint(max_entries, 1);
} syscall_enter_buffers SEC(".maps");

SEC("tp/raw_syscalls/sys_enter")
int handle_sys_enter(struct sys_enter_ctx *ctx) {
    struct task_struct* task = (struct task_struct*)bpf_get_current_task();
    uint32_t pid = 0;
    uint32_t tid = 0;
    bpf_probe_read(&pid, sizeof(pid), &task->tgid);
    bpf_probe_read(&tid, sizeof(tid), &task->pid);

	// We want to watch this pid, filter out things that are not my pid
	if (pid != my_pid) {
		return 0;
	}

	// We want to filter out my thread which is polling this thread
	if (tid == my_tid) {
		return 0;
	}

    int zero = 0;
    uint64_t time = bpf_ktime_get_ns();
    int syscall_number = ctx->syscall_number;

    struct syscall_event e = {0};
	e.kind = 0;
    e.pid = pid;
    e.tid = tid;
    e.syscall_number = syscall_number;
    e.timestamp = time;

    struct syscall_event_buffer *buffer = bpf_map_lookup_elem(&syscall_enter_buffers, &zero);
    if (!buffer) {
        bpf_printk("ERROR GETTING BUFFER");
        return 0;
    }

    if (buffer->length < 256) {
        buffer->buffer[buffer->length] = e;
        buffer->length += 1;
    }

    if (buffer->length == 256) {
        bpf_perf_event_output((void *)ctx, &perf_buffer, BPF_F_CURRENT_CPU, buffer, sizeof(*buffer));
        buffer->length = 0;
    }

    return 0;
}

SEC("tp/raw_syscalls/sys_exit")
int handle_sys_exit(struct sys_exit_ctx *ctx) {
    struct task_struct* task = (struct task_struct*)bpf_get_current_task();
    uint32_t pid = 0;
    uint32_t tid = 0;
    bpf_probe_read(&pid, sizeof(pid), &task->tgid);
    bpf_probe_read(&tid, sizeof(tid), &task->pid);

	// We want to watch this pid, filter out things that are not my pid
	if (pid != my_pid) {
		return 0;
	}

	// We want to filter out my thread which is polling this thread
	if (tid == my_tid) {
		return 0;
	}

    int zero = 0;
    uint64_t time = bpf_ktime_get_ns();
    int syscall_number = ctx->syscall_number;

    struct syscall_event e = {0};
	e.kind = 1;
    e.pid = pid;
    e.tid = tid;
    e.syscall_number = syscall_number;
    e.timestamp = time;

    struct syscall_event_buffer *buffer = bpf_map_lookup_elem(&syscall_enter_buffers, &zero);
    if (!buffer) {
        bpf_printk("ERROR GETTING BUFFER");
        return 0;
    }

    if (buffer->length < 256) {
        buffer->buffer[buffer->length] = e;
        buffer->length += 1;
    }

    if (buffer->length == 256) {
        bpf_perf_event_output((void *)ctx, &perf_buffer, BPF_F_CURRENT_CPU, buffer, sizeof(*buffer));
        buffer->length = 0;
    }

    return 0;
}
