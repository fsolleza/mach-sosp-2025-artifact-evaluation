#include <cstdio>
#include <iostream>
#include <vector>
#include <cassert>
#include "core/psf.h"
#include "parser_adapter.h"
#include "helpers.h"

// NOTE:
// NullableInt from fishstore core is DIFFERENT from NullableInt in the
// parser_api. (WTF.)
// Good thing we did it correctly the first time but here's the difference:
// NullableInt in fishstore's core/psf.h is defined as { is_null, value } while the one
// in the parser/parser_api.h is defined as { has_value, value }. See the teeny tiny
// fucking difference??

typedef ParserAdapter adapter_t;
using namespace fishstore::core;

extern "C" fishstore::core::NullableInt psf1(std::vector<adapter_t::field_t>& fields) {
	auto record_kind = fields[2].GetAsInt();
	if (record_kind.HasValue()) {
		int32_t v = record_kind.Value();
		if (v > 3) {
			printf("Got invalid record kind\n");
			exit(1);
		}
		if (v != 0) {
			return NullableInt { true, 0 };
		}
	} else {
		printf("Got invalid record kind\n");
		exit(1);
	}

	auto dur = fields[0].GetAsLong();
	if (dur.HasValue()) {
		int64_t v = dur.Value();
		// Project only if it's a read!
		if (v > 1) {
			printf("Got invalid op\n");
			exit(1);
		}
		if (v == 0) {
			return NullableInt { false, 0 };
		}
	}
	return NullableInt { true, 0 };
}

extern "C" fishstore::core::NullableInt psf2(std::vector<adapter_t::field_t>& fields) {
	auto record_kind = fields[2].GetAsInt();
	if (record_kind.HasValue()) {
		int32_t v = record_kind.Value();
		if (v > 3) {
			printf("Got invalid record kind\n");
			exit(1);
		}
		if (v != 1) {
			return NullableInt { true, 0 };
		}
	} else {
		printf("Got invalid record kind\n");
		exit(1);
	}

	auto sys_num = fields[1].GetAsLong();
	if (sys_num.HasValue()) {
		int64_t v = sys_num.Value();
		if (v > 500) {
			printf("Got invalid record syscall\n");
			exit(1);
		}
		if (v == 17) {
			return NullableInt { false, 0 };
		}
	}

	return NullableInt { true, 0 };
}

extern "C" fishstore::core::NullableInt psf3(std::vector<adapter_t::field_t>& fields) {
	auto record_kind = fields[2].GetAsInt();
	if (record_kind.HasValue()) {
		int32_t v = record_kind.Value();
		if (v > 3) {
			printf("Got invalid record kind\n");
			exit(1);
		}
		if (v == 2) {
			return NullableInt { false, 0 };
		}
	} else {
		printf("Got invalid record kind\n");
		exit(1);
	}
	return NullableInt { true, 0 };
}

extern "C" fishstore::core::NullableInt psf4(std::vector<adapter_t::field_t>& fields) {
	auto record_kind = fields[2].GetAsInt();
	if (record_kind.HasValue()) {
		int32_t v = record_kind.Value();
		if (v > 3) {
			printf("Got invalid record kind\n");
			exit(1);
		}
		if (v != 1) {
			return NullableInt { true, 0 };
		}
	} else {
		printf("Got invalid record kind\n");
		exit(1);
	}

	auto sys_num = fields[1].GetAsLong();
	if (sys_num.HasValue()) {
		int64_t v = sys_num.Value();
		if (v > 500) {
			printf("Got invalid record syscall\n");
			exit(1);
		}
		if (v == 44) {
			return NullableInt { false, 0 };
		}
	}
	return NullableInt { true, 0 };
}

extern "C" fishstore::core::NullableInt psf5(std::vector<adapter_t::field_t>& fields) {
	auto record_kind = fields[2].GetAsInt();
	if (record_kind.HasValue()) {
		int32_t v = record_kind.Value();
		if (v > 3) {
			printf("Got invalid record kind\n");
			exit(1);
		}
		if (v == 3) {
			return NullableInt { false, 0 };
		}
	} else {
		printf("Got invalid record kind\n");
		exit(1);
	}
	return NullableInt { true, 0 };
}


extern "C" fishstore::core::NullableInt psf6(std::vector<adapter_t::field_t>& fields) {
	auto f_kvop = fields[0].GetAsLong();
	auto f_dur = fields[1].GetAsLong();

	int64_t duration = 0;
	int64_t kvop = -1;
	if (f_dur.HasValue()) {
		duration = f_dur.Value();
	}

	if (f_kvop.HasValue()) {
		kvop = f_kvop.Value();
	}

	if (kvop == 0 && duration > 80) {
		return NullableInt { false, 0 };
	}
	return NullableInt { true, 0 };
}

extern "C" fishstore::core::NullableInt psf7(std::vector<adapter_t::field_t>& fields) {
	return NullableInt { true, 0 };
}


extern "C" fishstore::core::NullableInt IndexIngest1(std::vector<adapter_t::field_t>& fields) {
	auto record_kind = fields[2].GetAsInt();
	if (record_kind.HasValue()) {
		int32_t v = record_kind.Value();
		if (v > 3) {
			printf("Got invalid record kind\n");
			exit(1);
		}
		if (v != 0) {
			return NullableInt { true, 0 };
		}
	} else {
		printf("Got invalid record kind\n");
		exit(1);
	}
	auto dur = fields[0].GetAsLong();
	if (dur.HasValue()) {
		int64_t v = dur.Value();
		if (v < 100) {
			return NullableInt { false, 0 };
		} else {
			return NullableInt { false, 1 };
		}
	}
	return NullableInt { true, 0 };
}

extern "C" fishstore::core::NullableInt IndexIngest4(std::vector<adapter_t::field_t>& fields) {
	auto record_kind = fields[2].GetAsInt();
	if (record_kind.HasValue()) {
		int32_t v = record_kind.Value();
		if (v > 3) {
			printf("Got invalid record kind\n");
			exit(1);
		}
		if (v != 0) {
			return NullableInt { true, 0 };
		}
	} else {
		printf("Got invalid record kind\n");
		exit(1);
	}
	auto dur = fields[0].GetAsLong();
	if (dur.HasValue()) {
		int64_t v = dur.Value();
		int32_t r = (int32_t) v / 25;
		if (v < 100) {
			return NullableInt { false, r };
		} else {
			return NullableInt { false, 100 };
		}
	}
	return NullableInt { true, 0 };
}

extern "C" fishstore::core::NullableInt IndexIngest10(std::vector<adapter_t::field_t>& fields) {
	auto record_kind = fields[2].GetAsInt();
	if (record_kind.HasValue()) {
		int32_t v = record_kind.Value();
		if (v > 3) {
			printf("Got invalid record kind\n");
			exit(1);
		}
		if (v != 0) {
			return NullableInt { true, 0 };
		}
	} else {
		printf("Got invalid record kind\n");
		exit(1);
	}
	auto dur = fields[0].GetAsLong();
	if (dur.HasValue()) {
		int64_t v = dur.Value();
		int32_t r = (int32_t) v / 10;
		if (v < 100) {
			return NullableInt { false, r };
		} else {
			return NullableInt { false, 100 };
		}
	}
	return NullableInt { true, 0 };
}

extern "C" fishstore::core::NullableInt IndexIngest20(std::vector<adapter_t::field_t>& fields) {
	auto record_kind = fields[2].GetAsInt();
	if (record_kind.HasValue()) {
		int32_t v = record_kind.Value();
		if (v > 3) {
			printf("Got invalid record kind\n");
			exit(1);
		}
		if (v != 0) {
			return NullableInt { true, 0 };
		}
	} else {
		printf("Got invalid record kind\n");
		exit(1);
	}
	auto dur = fields[0].GetAsLong();
	if (dur.HasValue()) {
		int64_t v = dur.Value();
		int32_t r = (int32_t) v / 5;
		if (v < 100) {
			return NullableInt { false, r };
		} else {
			return NullableInt { false, 100 };
		}
	}
	return NullableInt { true, 0 };
}
