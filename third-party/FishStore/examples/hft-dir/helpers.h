#pragma once

#include <atomic>
#include <cassert>
#include <cstdint>
#include "adapters/common_utils.h"

using namespace fishstore;

// Sizes are from data/src. 8 bytes were add in by the tcp-replayer to account
// for timestamp

#define KVLOG_BYTES 32;
#define SYSCALL_EVENT_BYTES 32;
#define PAGE_CACHE_EVENT_BYTES 44;
#define PACKET_CAPTURE_BYTES 48;

enum FieldKind {
	FieldKindNone = 0,
	FieldKindInt = 1,
	FieldKindLong = 2,
	FieldKindFloat = 3,
	FieldKindDouble = 4,
	FieldKindBool = 5,
	FieldKindStringRef = 6,
	FieldKindString = 7,
};

struct FieldValue {
	int32_t int_;
	int64_t long_;
	float float_;
	double double_;
	bool bool_;
	adapter::StringRef string_ref_;
	std::string string_;
	FieldKind kind;
};

adapter::NullableInt FieldValue_as_int(FieldValue *f);
adapter::NullableLong FieldValue_as_long(FieldValue *f);
adapter::NullableBool FieldValue_as_bool(FieldValue *f);
adapter::NullableFloat FieldValue_as_float(FieldValue *f);
adapter::NullableDouble FieldValue_as_double(FieldValue *f);
adapter::NullableString FieldValue_as_string(FieldValue *f);
adapter::NullableStringRef FieldValue_as_string_ref(FieldValue *f);

typedef struct U64Bytes {
	uint8_t arr[8];
} U64Bytes;

uint64_t u64_from_be_bytes(uint8_t *arr);
U64Bytes u64_to_be_bytes(uint64_t num);

enum FieldName {
	FieldKvLogDuration = 0,
	FieldRecordKind = 1,
	FieldSyscallNumber = 2,
	FieldSyscallDuration = 3,
	FieldKvLogOp = 4,
	UnknownField = -1,
};

FieldName FieldName_from_string(std::string &x);

enum RecordKind {
	KvLog = 0,
	SyscallEvent = 1,
	PageCacheEvent = 2,
	PacketCapture = 3,
	None = 999,
};

uint64_t RecordKind_size(RecordKind r);
RecordKind RecordKind_from_value(uint64_t x);

typedef struct {
	uint8_t * buffer;
	size_t length;
} DataBuffer;

DataBuffer DataBuffer_new(size_t length) {
	DataBuffer data;
	data.buffer = (uint8_t*)malloc(length);
	data.length = length;
	return data;
}

void DataBuffer_drop(DataBuffer data);
uint64_t DataBuffer_arrival_us(DataBuffer *data);
uint64_t DataBuffer_item_count(DataBuffer *data);

typedef struct {
	uint8_t * buffer;
} RecordBuffer;

RecordKind RecordBuffer_kind(RecordBuffer *r);
uint64_t RecordBuffer_size(RecordBuffer *r);

// Implementations

adapter::NullableInt FieldValue_as_int(FieldValue *f) {
	switch (f->kind) {
		case FieldKindInt: {
			int32_t t = f->int_;
			return { true, t };
		}
		default: {
			int32_t t = -1;
			return { false, t };
		}
	};
}

adapter::NullableLong FieldValue_as_long(FieldValue *f) {
	switch (f->kind) {
		case FieldKindLong: {
			int64_t t = f->long_;
			return { true, t };
		}
		default: {
			int64_t t = -1;
			return { false, t };
		}
	};
}

adapter::NullableBool FieldValue_as_bool(FieldValue *f) {
	switch (f->kind) {
		case FieldKindBool: {
			bool t = f->bool_;
			return { true, t };
		}
		default: {
			bool t = false;
			return { false, t };
		}
	};
}

adapter::NullableFloat FieldValue_as_float(FieldValue *f) {
	switch (f->kind) {
		case FieldKindFloat: {
			float t = f->float_;
			return { true, t };
		}
		default: {
			float t = 0.0;
			return { false, t };
		}
	};
}

adapter::NullableDouble FieldValue_as_double(FieldValue *f) {
	switch (f->kind) {
		case FieldKindDouble: {
			double t = f->double_;
			return { true, t };
		}
		default: {
			double t = 0.0;
			return { false, t };
		}
	};
}

adapter::NullableString FieldValue_as_string(FieldValue *f) {
	switch (f->kind) {
		case FieldKindString: {
			std::string t = f->string_;
			return { true, t };
		}
		default: {
			std::string_view temp{};
			return { false, std::string(temp) };
		}
	};
}

adapter::NullableStringRef FieldValue_as_string_ref(FieldValue *f) {
	switch (f->kind) {
		case FieldKindStringRef: {
			adapter::StringRef t = f->string_ref_;
			return { true, t };
		}
		default: {
			std::string_view temp{};
			adapter::StringRef str_ref { temp.data(), temp.length() };
			return { false, str_ref };
		}
	};
}

uint64_t u64_from_be_bytes(uint8_t *arr) {
	return
		((uint64_t)(((uint8_t *)(arr))[7]) <<  0)+
		((uint64_t)(((uint8_t *)(arr))[6]) <<  8)+
		((uint64_t)(((uint8_t *)(arr))[5]) << 16)+
		((uint64_t)(((uint8_t *)(arr))[4]) << 24)+
		((uint64_t)(((uint8_t *)(arr))[3]) << 32)+
		((uint64_t)(((uint8_t *)(arr))[2]) << 40)+
		((uint64_t)(((uint8_t *)(arr))[1]) << 48)+
		((uint64_t)(((uint8_t *)(arr))[0]) << 56);
}

U64Bytes u64_to_be_bytes(uint64_t num) {
	U64Bytes bytes;
	bytes.arr[7] = (num >> 0) & 0xff;
	bytes.arr[6] = (num >> 8) & 0xff;
	bytes.arr[5] = (num >> (8 * 2)) & 0xff;
	bytes.arr[4] = (num >> (8 * 3)) & 0xff;
	bytes.arr[3] = (num >> (8 * 4)) & 0xff;
	bytes.arr[2] = (num >> (8 * 5)) & 0xff;
	bytes.arr[1] = (num >> (8 * 6)) & 0xff;
	bytes.arr[0] = (num >> (8 * 7)) & 0xff;
	return bytes;
}

FieldName FieldName_from_string(std::string &x) {
	if (x == "KvLogDuration") {
		return FieldKvLogDuration;
	}

	if (x == "RecordKind") {
		return FieldRecordKind;
	}

	if (x == "SyscallNumber") {
		return FieldSyscallNumber;
	}

	if (x == "SyscallDuration") {
		return FieldSyscallDuration;
	}

	if (x == "KvLogOp") {
		return FieldKvLogOp;
	}

	printf("unknown field\n");
	exit(1);
}

uint64_t RecordKind_size(RecordKind r) {
	switch(r) {
		case KvLog: {
			return KVLOG_BYTES;
		}
		case SyscallEvent: {
			return SYSCALL_EVENT_BYTES;
		}
		case PageCacheEvent: {
			return PAGE_CACHE_EVENT_BYTES;
		}
		case PacketCapture: {
			return PACKET_CAPTURE_BYTES;
		}
	}
	return 0;
}

RecordKind RecordKind_from_value(uint64_t x) {
	if (x > 3) {
		printf("unexpected RecordKind value: %ld\n", x);
		exit(1);
	}

	switch(x) {
		case 0:
			return KvLog;
		case 1: {
			return SyscallEvent;
		}
		case 2: {
			return PageCacheEvent;
		}
		case 3: {
			return PacketCapture;
		}
		default: {
			printf("ERROR: unhhandled kind form value\n");
			exit(1);
		}

	};

	// Unreachable, to silence warnings
	return KvLog;
}

void DataBuffer_drop(DataBuffer data) {
	free(data.buffer);
}

uint64_t DataBuffer_arrival_us(DataBuffer *data) {
	return u64_from_be_bytes(data->buffer);
}

uint64_t DataBuffer_item_count(DataBuffer *data) {
	return u64_from_be_bytes(data->buffer + 8);
}



// Layout of a record buffer is as described in data/src except that we added 8
// bytes in the beginning to include arrival timestamp
// [..8] are the arrival timestamp
// [8..16] are the id
// [16..24] are the kind

FieldValue RecordBuffer_arrival_timestamp(RecordBuffer *r) {
	uint64_t ts = u64_from_be_bytes(r->buffer);
	FieldValue v = { 0 };
	v.long_ = (int64_t)ts;
	v.kind = FieldKindLong;
	return v;
}

RecordKind RecordBuffer_kind(RecordBuffer *r) {
	uint64_t record_type = u64_from_be_bytes(r->buffer + 16);
	return RecordKind_from_value(record_type);
}

FieldValue RecordBuffer_kv_log_op(RecordBuffer *r)  {
	FieldValue v = { 0 };
	v.kind = FieldKindNone;
	RecordKind k = RecordBuffer_kind(r);
	if (k == KvLog) {
		uint64_t d = u64_from_be_bytes(r->buffer + 32);
		v.long_ = (int64_t) d;
		v.kind = FieldKindLong;
	}
	return v;
}

FieldValue RecordBuffer_kv_log_duration(RecordBuffer *r)  {
	FieldValue v = { 0 };
	v.kind = FieldKindNone;
	RecordKind k = RecordBuffer_kind(r);
	if (k == KvLog) {
		uint64_t d = u64_from_be_bytes(r->buffer + 48);
		v.long_ = (int64_t) d;
		v.kind = FieldKindLong;
	}
	return v;
}

FieldValue RecordBuffer_syscall_number(RecordBuffer *r)  {
	FieldValue v = { 0 };
	v.kind = FieldKindNone;
	RecordKind k = RecordBuffer_kind(r);
	if (k == SyscallEvent) {
		uint64_t d = u64_from_be_bytes(r->buffer + 32);
		if (d > 456) {
			printf("unexpected syscall number value: %ld\n", d);
			exit(1);
		}
		v.long_ = (int64_t) d;
		v.kind = FieldKindLong;
	}
	return v;
}

FieldValue RecordBuffer_syscall_duration(RecordBuffer *r)  {
	FieldValue v = { 0 };
	v.kind = FieldKindNone;
	RecordKind k = RecordBuffer_kind(r);
	if (k == SyscallEvent) {
		uint64_t d = u64_from_be_bytes(r->buffer + 48);
		v.long_ = (int64_t) d;
		v.kind = FieldKindLong;
	}
	return v;
}


uint64_t RecordBuffer_size(RecordBuffer *r) {
	RecordKind k = RecordBuffer_kind(r);
	uint64_t sz = RecordKind_size(k);

	// Add 24 to account for arrival timestamp, id, and kind
	return sz + 24;
}

FieldValue RecordBuffer_record_kind(RecordBuffer *r)  {
	FieldValue v = { 0 };
	v.kind = FieldKindNone;
	RecordKind k = RecordBuffer_kind(r);
	if (k == KvLog) {
		v.int_ = 0;
		v.kind = FieldKindInt;
	} else if (k == SyscallEvent) {
		v.int_ = 1;
		v.kind = FieldKindInt;
	} else if (k == PageCacheEvent) {
		v.int_ = 2;
		v.kind = FieldKindInt;
	} else if (k == PacketCapture) {
		v.int_ = 3;
		v.kind = FieldKindInt;
	} else {
		printf("Unexpected value found\n");
		exit(1);
	}
	return v;
}

FieldValue RecordBuffer_get_field(RecordBuffer *r, FieldName id) {
	FieldValue v;
	v.kind = FieldKindNone;

	switch(id) {
		case FieldKvLogDuration: {
			v = RecordBuffer_kv_log_duration(r);
			break;
		}
		case FieldRecordKind: {
			v = RecordBuffer_record_kind(r);
			break;
		}
		case FieldSyscallDuration: {
			v = RecordBuffer_syscall_duration(r);
			break;
		}
		case FieldSyscallNumber: {
			v = RecordBuffer_syscall_number(r);
			break;
		}
		case FieldKvLogOp: {
			v = RecordBuffer_kv_log_op(r);
			break;
		}
		default: {
				 printf("Unhandled field\n");
				 exit(1);
		}
	};
	return v;
}
