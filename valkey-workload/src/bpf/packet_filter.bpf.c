// SPDX-License-Identifier: GPL-2.0
#include "vmlinux.h"
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_core_read.h>
#include <bpf/bpf_endian.h>

char _license[] SEC("license") = "GPL";


#define ETH_P_IP 0x0800

//static u16 PORTS[16] = [
//	6370, 6371, 6372, 6373,
//	6374, 6375, 6376, 6377,
//	6378, 6379, 6380, 6381,
//	6382, 6383, 6384, 6385,
//];

struct drop_ts_range {
	u64 start;
	u64 end;
};

struct {
	__uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
	__type(key, __u32);
	__type(value, struct drop_ts_range);
	__uint(max_entries, 1);
} timestamps SEC(".maps");

SEC("xdp")
int xdp_packet_filter(struct xdp_md *ctx)
{


	// Pointers to packet data
    void *data = (void *)(long)ctx->data;
    void *data_end = (void *)(long)ctx->data_end;

    // Parse Ethernet header
    struct ethhdr *eth = data;

	// Ensure Ethernet header is within bounds
    if ((void *)(eth + 1) > data_end)
        return XDP_PASS;

    // Only handle IPv4 packets
    if (bpf_ntohs(eth->h_proto) != ETH_P_IP)
        return XDP_PASS;

    // Cast to IP header
    struct iphdr *ip = (struct iphdr *)(eth + 1);

    // Ensure IP header is within bounds
    if ((void *)(ip + 1) > data_end)
        return XDP_PASS;

    // Check if the protocol is TCP
    if (ip->protocol != IPPROTO_TCP)
        return XDP_PASS;

    // Calculate IP header length
    int ip_hdr_len = ip->ihl * 4;
    if (ip_hdr_len < sizeof(struct iphdr)) {
        return XDP_PASS;
    }

    // Ensure IP header is within packet bounds
    if ((void *)ip + ip_hdr_len > data_end) {
        return XDP_PASS;
    }

    // Parse TCP header
    struct tcphdr *tcph = (struct tcphdr *)((unsigned char *)ip + ip_hdr_len);

    // Ensure TCP header is within packet bounds
    if ((void *)(tcph + 1) > data_end) {
        return XDP_PASS;
    }

	u16 dst_port = bpf_ntohs(tcph->dest);
	//u16 src_port = tcph->source;
	//bool in_port = false;

	//if (6370 <= dst_port && dst_port < 6370 + 16) {
	//	in_port = true;
	//	//// modify
	//	//if (drop) {
	//	//	tcph->dest = bpf_ntohs(5555);
	//	//}
	//}

	bool in_port = 6370 <= dst_port && dst_port < 6370 + 16;
	if (!in_port) {
		return XDP_PASS;
	}
	u64 should_drop = (u64)bpf_get_prandom_u32() % 100000000;
	if (should_drop < 10) {
		tcph->dest = bpf_htons(5555);
	}
	return XDP_PASS;

	// now let's see if we should drop

	//u64 timestamp_ns = bpf_ktime_get_ns();
	//u32 zero = 0;
	//struct drop_ts_range *drop_ts = bpf_map_lookup_elem(&timestamps, &zero);
	//if (!drop_ts) {
	//	return XDP_PASS;
	//}

	//bool drop = false;
	//u64 nanos = 1000000000;

	//// On first ts or one second after indicated timestamp, choose a new
	//// timestamp. The next drop timestamp is 10 - 20 seconds from now
	//if ((drop_ts->start == 0) || (timestamp_ns > drop_ts->end)) {
	//	u64 next_drop_s = (10 + (u64)bpf_get_prandom_u32() % 10) * nanos;
	//	u64 next_drop_e = next_drop_s + 100000000; // 100ms
	//	drop_ts->start = timestamp_ns + next_drop_s;
	//	drop_ts->end = timestamp_ns + next_drop_e;
	//	bpf_printk("Resetting drop: %llu, %llu", drop_ts->start, drop_ts->end);
	//}

	//if (timestamp_ns > drop_ts->start && timestamp_ns < drop_ts->end) {
	//	u64 should_drop = (u64)bpf_get_prandom_u32() % 10000;
	//	if (should_drop < 10) {
	//		return XDP_DROP;
	//	}
	//}
	//return XDP_PASS;
}

