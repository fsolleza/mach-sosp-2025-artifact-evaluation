// SPDX-License-Identifier: GPL-2.0 OR BSD-3-Clause
#include "vmlinux.h"
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_core_read.h>
#include <bpf/bpf_endian.h>

char LICENSE[] SEC("license") = "Dual BSD/GPL";

#define ETH_P_IP    0x0800

// Must line up with common::sk_buf.rs
struct skbuf_event {
	uint64_t timestamp;
	uint16_t dst_port;
	uint16_t src_port;
	uint32_t daddr;
	uint32_t saddr;
	uint16_t tot_len;
};

// Must line up with common::sk_buf.rs
struct skbuf_event_buffer {
	uint32_t length;
	struct skbuf_event buffer[256];
};

struct {
	__uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
	__type(key, __u32);
	__type(value, struct skbuf_event_buffer);
	__uint(max_entries, 1);
} buffers SEC(".maps");

struct {
	__uint(type, BPF_MAP_TYPE_PERF_EVENT_ARRAY);
	__uint(key_size, sizeof(int));
	__uint(value_size, sizeof(int));
} perf_buffer SEC(".maps");

SEC("tc")
int handle_tc(struct __sk_buff *skb) {

	struct task_struct* task = (struct task_struct*)bpf_get_current_task();

	void *data_end = (void*)(long)skb->data_end;
	struct ethhdr *eth = (struct ethhdr*)(void*)(long)skb->data;

	__be16 dst_port = 0;
	__be16 src_port = 0;
	__be32 saddr = 0;
	__be32 daddr = 0;
	__u8 proto = 0;
	__be16 tot_len = 0;
	void *trans_data;
	struct iphdr *iph;

	if (eth + 1 > data_end) {
		return 0;
	}

	if (eth->h_proto == bpf_htons(ETH_P_IP)) { // ipv4
		iph = (struct iphdr *)((void*)eth + sizeof(*eth));
		if ((void*)(iph + 1) > data_end) {
			return 0;
		}

		saddr = iph->saddr;
		daddr = iph->daddr;
		tot_len = iph->tot_len;
		proto = iph->protocol;
		trans_data = (void*)iph + (iph->ihl * 4);
	} else {
		return 0;
	}
	if (proto == IPPROTO_TCP)  {
		struct tcphdr *tcph = (struct tcphdr *)trans_data;
		if ((void*)(trans_data + sizeof(*tcph)) > data_end) {
			return 0;
		}

		dst_port = tcph->dest;
		src_port = tcph->source;
	} else {
		return 0;
	}

	if (!iph) {
		return 0;
	}

	struct skbuf_event e = {0};

	e.src_port = bpf_ntohs(src_port);
	e.dst_port = bpf_ntohs(dst_port);
	e.saddr = bpf_ntohl(saddr);
	e.daddr = bpf_ntohl(daddr);
	e.tot_len = bpf_ntohs(tot_len);
	e.timestamp = bpf_ktime_get_ns();

	//bpf_perf_event_output((void*)skb, &perf_buffer, BPF_F_CURRENT_CPU, &e, sizeof(struct skbuf_event));

	int zero = 0;
	struct skbuf_event_buffer *buffer = bpf_map_lookup_elem(&buffers, &zero);
	if (!buffer) {
		bpf_printk("ERROR GETTING BUFFER");
		return 0;
	}

	if (buffer->length < 256) {
		buffer->buffer[buffer->length] = e;
		buffer->length += 1;
	}

	if (buffer->length == 256) {
		bpf_perf_event_output((void*)skb, &perf_buffer, BPF_F_CURRENT_CPU, buffer, sizeof(*buffer));
		buffer->length = 0;
	}
	return 0;
}

