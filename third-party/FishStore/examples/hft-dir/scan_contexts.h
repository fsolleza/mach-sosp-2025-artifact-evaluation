class RocksDBQ1ScanContext : public IAsyncContext {
	public:
		RocksDBQ1ScanContext(uint32_t psf_id, int32_t value, uint64_t ts_min, uint64_t ts_max)
			: psf_id_(psf_id), value_(value), ts_min(ts_min), ts_max(ts_max), cnt(0), max(0) {}

		inline void Touch(const char* payload, uint32_t payload_size) {
			RecordBuffer r;
			r.buffer = (uint8_t*)payload;
			
			// Check if in timestamp
			FieldValue arr = RecordBuffer_arrival_timestamp(&r);
			adapter::NullableLong arr2 = FieldValue_as_long(&arr);
			if (arr2.HasValue()) {
				uint64_t arr3 = arr2.Value();
				if (arr3 < ts_min || arr3 > ts_max) {
					return;
				}
			} else {
				printf("ERROR: should have a value!");
				exit(1);
			}

			// And then evaluate the value
			FieldValue v = RecordBuffer_kv_log_duration(&r);
			adapter::NullableLong n = FieldValue_as_long(&v);
			if (n.HasValue()) {
				uint64_t d = (uint64_t)n.Value();
				if (d > max) {
					max = d;
				}
				++cnt;
				durations.push_back(d);
			} else {
				printf("ERROR: should have a value!");
				exit(1);
			}
		}

		inline void Finalize() {
			printf("Finalizing %u %ld\n", cnt, durations.size());

			// Calculate percentile
			printf("Sorting\n");
			std::sort(durations.begin(), durations.end());
			printf("Calculating index\n");
			double index_dbl = static_cast<double>(cnt) * .9999;
			uint64_t idx = static_cast<uint64_t>(std::floor(index_dbl));
			printf("Index: %ld\n", idx);
			uint64_t tile = durations[idx];

			// Print result
			printf("Q1 count %u, max %ld, 99.99: %ld \n", cnt, max, tile);
		}

		inline core::KeyHash get_hash() const {
			return core::KeyHash{ core::Utility::GetHashCode(psf_id_, value_) };
		}

		inline bool check(const core::KeyPointer* kpt) {
			return kpt->mode == 1 && kpt->inline_psf_id == psf_id_ && kpt->value == value_;
		}

	protected:
		Status DeepCopy_Internal(IAsyncContext*& context_copy) {
			return IAsyncContext::DeepCopy_Internal(*this, context_copy);
		}

	private:
		uint32_t psf_id_;
		int32_t value_;
		uint32_t cnt;
		uint64_t max;
		uint64_t ts_min;
		uint64_t ts_max;
		std::vector<uint64_t> durations;
};

class RocksDBQ2ScanContext : public IAsyncContext {
	public:
		RocksDBQ2ScanContext(uint32_t psf_id, int32_t value, uint64_t ts_min, uint64_t ts_max) :
			psf_id_(psf_id),
			value_(value),
			ts_min(ts_min),
			ts_max(ts_max),
			cnt(0),
			max(0),
			touched(0)
		{}

		inline void Touch(const char* payload, uint32_t payload_size) {
			++touched;
			RecordBuffer r;
			r.buffer = (uint8_t*)payload;
			
			// Check if in timestamp
			FieldValue arr = RecordBuffer_arrival_timestamp(&r);
			adapter::NullableLong arr2 = FieldValue_as_long(&arr);
			if (arr2.HasValue()) {
				uint64_t arr3 = arr2.Value();
				if (arr3 < ts_min || arr3 > ts_max) {
					return;
				}
			} else {
				printf("ERROR: should have a value!");
				exit(1);
			}

			// And then evaluate the value
			FieldValue v = RecordBuffer_syscall_duration(&r);
			adapter::NullableLong n = FieldValue_as_long(&v);
			if (n.HasValue()) {
				uint64_t d = (uint64_t)n.Value();
				if (d > max) {
					max = d;
				}
				++cnt;
				durations.push_back(d);
			} else {
				printf("ERROR: should have a value!");
				exit(1);
			}
		}

		inline void Finalize() {
			printf("Finalizing touched %ld, count %u %ld\n", touched, cnt, durations.size());

			// Calculate percentile
			printf("Sorting\n");
			std::sort(durations.begin(), durations.end());
			printf("Calculating index\n");
			double index_dbl = static_cast<double>(cnt) * .9999;
			uint64_t idx = static_cast<uint64_t>(std::floor(index_dbl));
			printf("Index: %ld\n", idx);
			uint64_t tile = durations[idx];

			// Print result
			printf("Q2 count %u, max %ld, 99.99: %ld \n", cnt, max, tile);
		}

		inline core::KeyHash get_hash() const {
			return core::KeyHash{ core::Utility::GetHashCode(psf_id_, value_) };
		}

		inline bool check(const core::KeyPointer* kpt) {
			return kpt->mode == 1 && kpt->inline_psf_id == psf_id_ && kpt->value == value_;
		}

	protected:
		Status DeepCopy_Internal(IAsyncContext*& context_copy) {
			return IAsyncContext::DeepCopy_Internal(*this, context_copy);
		}

	private:
		uint64_t touched;
		uint32_t psf_id_;
		int32_t value_;
		uint32_t cnt;
		uint64_t max;
		uint64_t ts_min;
		uint64_t ts_max;
		std::vector<uint64_t> durations;
};

class RocksDBQ3ScanContext : public IAsyncContext {
	public:
		RocksDBQ3ScanContext(uint32_t psf_id, int32_t value, uint64_t ts_min, uint64_t ts_max) :
			psf_id_(psf_id),
			value_(value),
			ts_min(ts_min),
			ts_max(ts_max),
			cnt(0),
			touched(0)
		{}

		inline void Touch(const char* payload, uint32_t payload_size) {
			++touched;
			RecordBuffer r;
			r.buffer = (uint8_t*)payload;
			
			// Check if in timestamp
			FieldValue arr = RecordBuffer_arrival_timestamp(&r);
			adapter::NullableLong arr2 = FieldValue_as_long(&arr);
			if (arr2.HasValue()) {
				uint64_t arr3 = arr2.Value();
				if (arr3 < ts_min || arr3 > ts_max) {
					return;
				}
			} else {
				printf("ERROR: should have a value!");
				exit(1);
			}

			// And then evaluate the value
			FieldValue v = RecordBuffer_record_kind(&r);
			adapter::NullableInt n = FieldValue_as_int(&v);
			if (n.HasValue()) {
				int64_t d = n.Value();
				if (d == 2) {
					++cnt;
				} else {
					printf("ERROR: P3 index captured");
					exit(1);
				}
			} else {
				printf("ERROR: should have a value!");
				exit(1);
			}
		}

		inline void Finalize() {
			printf("Finalizing touched %ld, count %u\n", touched, cnt);
			// Print result
			printf("Q3 count %u\n", cnt);
		}

		inline core::KeyHash get_hash() const {
			return core::KeyHash{ core::Utility::GetHashCode(psf_id_, value_) };
		}

		inline bool check(const core::KeyPointer* kpt) {
			return kpt->mode == 1 && kpt->inline_psf_id == psf_id_ && kpt->value == value_;
		}

	protected:
		Status DeepCopy_Internal(IAsyncContext*& context_copy) {
			return IAsyncContext::DeepCopy_Internal(*this, context_copy);
		}

	private:
		uint64_t touched;
		uint32_t psf_id_;
		int32_t value_;
		uint32_t cnt;
		uint64_t ts_min;
		uint64_t ts_max;
};

class ValkeyQ1ScanContext : public IAsyncContext {
	public:
		ValkeyQ1ScanContext(
			uint32_t psf_id,
			int32_t value,
			uint64_t ts_min,
			uint64_t ts_max,
			uint64_t *out_tile,
			std::vector<uint64_t> &durations
		):
			psf_id_(psf_id),
			value_(value),
			ts_min(ts_min),
			ts_max(ts_max),
			cnt(0),
			max(0),
			out_tile(out_tile),
			durations(durations)
		{}

		inline void Touch(const char* payload, uint32_t payload_size) {
			RecordBuffer r;
			r.buffer = (uint8_t*)payload;
			
			// Check if in timestamp
			FieldValue arr = RecordBuffer_arrival_timestamp(&r);
			adapter::NullableLong arr2 = FieldValue_as_long(&arr);
			if (arr2.HasValue()) {
				uint64_t arr3 = arr2.Value();
				if (arr3 < ts_min || arr3 > ts_max) {
					return;
				}
			} else {
				printf("ERROR: should have a value!");
				exit(1);
			}

			// And then evaluate the value
			FieldValue v = RecordBuffer_kv_log_duration(&r);
			adapter::NullableLong n = FieldValue_as_long(&v);
			if (n.HasValue()) {
				uint64_t d = (uint64_t)n.Value();
				if (d > max) {
					max = d;
				}
				++cnt;
				durations.push_back(d);
			} else {
				printf("ERROR: should have a value!");
				exit(1);
			}
		}

		inline void Finalize() {
			printf("Finalizing %u %ld\n", cnt, durations.size());

			// Calculate percentile
			printf("Sorting\n");
			std::sort(durations.begin(), durations.end());
			printf("Calculating index\n");
			double index_dbl = static_cast<double>(cnt) * .9999;
			uint64_t idx = static_cast<uint64_t>(std::floor(index_dbl));
			printf("Index: %ld\n", idx);
			*out_tile = durations[idx];

			// Print result
			printf("Q1 count %u, max %ld, 99.99: %ld \n", cnt, max,
			       *out_tile);
		}

		inline core::KeyHash get_hash() const {
			return core::KeyHash{ core::Utility::GetHashCode(psf_id_, value_) };
		}

		inline bool check(const core::KeyPointer* kpt) {
			return kpt->mode == 1 && kpt->inline_psf_id == psf_id_ && kpt->value == value_;
		}

	protected:
		Status DeepCopy_Internal(IAsyncContext*& context_copy) {
			return IAsyncContext::DeepCopy_Internal(*this, context_copy);
		}

	private:
		uint32_t psf_id_;
		int32_t value_;
		uint32_t cnt;
		uint64_t max;
		uint64_t ts_min;
		uint64_t ts_max;

		uint64_t *out_tile;
		std::vector<uint64_t> &durations;
};


class ValkeyQ2ScanContext : public IAsyncContext {
	public:
		ValkeyQ2ScanContext(
			uint32_t psf_id,
			int32_t value,
			uint64_t ts_min,
			uint64_t ts_max,
			uint64_t *out_tile,
			std::vector<uint64_t> &durations
		):
			psf_id_(psf_id),
			value_(value),
			ts_min(ts_min),
			ts_max(ts_max),
			cnt(0),
			max(0),
			out_tile(out_tile),
			durations(durations)
		{}

		inline void Touch(const char* payload, uint32_t payload_size) {
			RecordBuffer r;
			r.buffer = (uint8_t*)payload;
			
			// Check if in timestamp
			FieldValue arr = RecordBuffer_arrival_timestamp(&r);
			adapter::NullableLong arr2 = FieldValue_as_long(&arr);
			if (arr2.HasValue()) {
				uint64_t arr3 = arr2.Value();
				if (arr3 < ts_min || arr3 > ts_max) {
					return;
				}
			} else {
				printf("ERROR: should have a value!");
				exit(1);
			}

			// And then evaluate the value
			FieldValue v = RecordBuffer_syscall_duration(&r);
			adapter::NullableLong n = FieldValue_as_long(&v);
			if (n.HasValue()) {
				uint64_t d = (uint64_t)n.Value();
				if (d > max) {
					max = d;
				}
				++cnt;
				durations.push_back(d);
			} else {
				printf("ERROR: should have a value!");
				exit(1);
			}
		}

		inline void Finalize() {
			printf("Finalizing %u %ld\n", cnt, durations.size());

			// Calculate percentile
			printf("Sorting\n");
			std::sort(durations.begin(), durations.end());
			printf("Calculating index\n");
			double index_dbl = static_cast<double>(cnt) * .9999;
			uint64_t idx = static_cast<uint64_t>(std::floor(index_dbl));
			printf("Index: %ld\n", idx);
			*out_tile = durations[idx];

			// Print result
			printf("Q1 count %u, max %ld, 99.99: %ld \n", cnt, max,
			       *out_tile);
		}

		inline core::KeyHash get_hash() const {
			return core::KeyHash{ core::Utility::GetHashCode(psf_id_, value_) };
		}

		inline bool check(const core::KeyPointer* kpt) {
			return kpt->mode == 1 && kpt->inline_psf_id == psf_id_ && kpt->value == value_;
		}

	protected:
		Status DeepCopy_Internal(IAsyncContext*& context_copy) {
			return IAsyncContext::DeepCopy_Internal(*this, context_copy);
		}

	private:
		uint32_t psf_id_;
		int32_t value_;
		uint32_t cnt;
		uint64_t max;
		uint64_t ts_min;
		uint64_t ts_max;

		uint64_t *out_tile;
		std::vector<uint64_t> &durations;
};

class ValkeyQ3MaxScanContext : public IAsyncContext {
	public:
		ValkeyQ3MaxScanContext(
			uint32_t psf_id,
			int32_t value,
			uint64_t ts_min,
			uint64_t ts_max,
			uint64_t *out_max,
			uint64_t *out_ts
		):
			psf_id_(psf_id),
			value_(value),
			ts_min(ts_min),
			ts_max(ts_max),
			cnt(0),
			max(0),
			out_max(out_max),
			out_ts(out_ts)
		{}

		inline void Touch(const char* payload, uint32_t payload_size) {
			RecordBuffer r;
			r.buffer = (uint8_t*)payload;
			
			// Check if in timestamp
			FieldValue arr = RecordBuffer_arrival_timestamp(&r);
			adapter::NullableLong arr2 = FieldValue_as_long(&arr);
			uint64_t curr_ts = 0;
			if (arr2.HasValue()) {
				uint64_t arr3 = arr2.Value();
				if (arr3 < ts_min || arr3 > ts_max) {
					return;
				}
				curr_ts = arr3;
			} else {
				printf("ERROR: should have a value!");
				exit(1);
			}

			// And then evaluate the value
			FieldValue v = RecordBuffer_kv_log_duration(&r);
			adapter::NullableLong n = FieldValue_as_long(&v);
			if (n.HasValue()) {
				uint64_t d = (uint64_t)n.Value();
				if (d > *out_max) {
					*out_max = d;
					*out_ts = curr_ts;
				}
				++cnt;
			} else {
				printf("ERROR: should have a value!");
				exit(1);
			}
		}

		inline void Finalize() {
			// Print result
			printf("Finalizing Valkey 3 max count %u, max %ld\n", cnt, *out_max);
		}

		inline core::KeyHash get_hash() const {
			return core::KeyHash{ core::Utility::GetHashCode(psf_id_, value_) };
		}

		inline bool check(const core::KeyPointer* kpt) {
			return kpt->mode == 1 && kpt->inline_psf_id == psf_id_ && kpt->value == value_;
		}

	protected:
		Status DeepCopy_Internal(IAsyncContext*& context_copy) {
			return IAsyncContext::DeepCopy_Internal(*this, context_copy);
		}

	private:
		uint32_t psf_id_;
		int32_t value_;
		uint32_t cnt;
		uint64_t max;
		uint64_t ts_min;
		uint64_t ts_max;

		uint64_t *out_max;
		uint64_t *out_ts;
};

class ValkeyQ3PacketScanContext : public IAsyncContext {
	public:
		ValkeyQ3PacketScanContext(
			uint32_t psf_id,
			int32_t value,
			uint64_t ts_min,
			uint64_t ts_max,
			uint64_t *out_count
		):
			psf_id_(psf_id),
			value_(value),
			ts_min(ts_min),
			ts_max(ts_max),
			out_count(out_count)
		{}

		inline void Touch(const char* payload, uint32_t payload_size) {
			RecordBuffer r;
			r.buffer = (uint8_t*)payload;
			
			// Check if in timestamp
			FieldValue arr = RecordBuffer_arrival_timestamp(&r);
			adapter::NullableLong arr2 = FieldValue_as_long(&arr);
			uint64_t curr_ts = 0;
			if (arr2.HasValue()) {
				uint64_t arr3 = arr2.Value();
			if (arr3 < ts_min || arr3 > ts_max) {
					return;
				}
				curr_ts = arr3;
			} else {
				printf("ERROR: should have a value!");
				exit(1);
			}

			// And then evaluate the value
			FieldValue v = RecordBuffer_record_kind(&r);
			adapter::NullableInt n = FieldValue_as_int(&v);
			if (n.HasValue()) {
				int32_t d = (uint64_t)n.Value();
				if (d != 3)
				{
					printf("Unexpected record kind\n");
					exit(1);
				}
				*out_count += 1;
			} else {
				printf("ERROR: should have a value!");
				exit(1);
			}
		}

		inline void Finalize() {
			// Print result
			printf("Finalizing Valkey 3 max count %ld\n", *out_count);
		}

		inline core::KeyHash get_hash() const {
			return core::KeyHash{ core::Utility::GetHashCode(psf_id_, value_) };
		}

		inline bool check(const core::KeyPointer* kpt) {
			return kpt->mode == 1 && kpt->inline_psf_id == psf_id_ && kpt->value == value_;
		}

	protected:
		Status DeepCopy_Internal(IAsyncContext*& context_copy) {
			return IAsyncContext::DeepCopy_Internal(*this, context_copy);
		}

	private:
		uint32_t psf_id_;
		int32_t value_;
		uint64_t ts_min;
		uint64_t ts_max;
		uint64_t *out_count;
};

class MicrobenchmarkScanContext : public IAsyncContext {
	public:
		MicrobenchmarkScanContext(
			uint32_t psf_id,
			int32_t value,
			uint64_t ts_min,
			uint64_t ts_max,
			uint64_t *out_count
		):
			psf_id_(psf_id),
			value_(value),
			ts_min(ts_min),
			ts_max(ts_max),
			out_count(out_count)
		{}

		inline void Touch(const char* payload, uint32_t payload_size) {
			RecordBuffer r;
			r.buffer = (uint8_t*)payload;
			
			// Check if in timestamp
			FieldValue arr = RecordBuffer_arrival_timestamp(&r);
			adapter::NullableLong arr2 = FieldValue_as_long(&arr);
			uint64_t curr_ts = 0;
			if (arr2.HasValue()) {
				uint64_t arr3 = arr2.Value();
				if (arr3 < ts_min || arr3 > ts_max) {
					return;
				}
				curr_ts = arr3;
			} else {
				printf("ERROR: should have a value!");
				exit(1);
			}

			// Check that all records are of the right kind
			FieldValue v = RecordBuffer_record_kind(&r);
			adapter::NullableInt n = FieldValue_as_int(&v);
			if (n.HasValue()) {
				int32_t d = (uint64_t)n.Value();
				if (d != 0)
				{
					printf("Unexpected record kind\n");
					exit(1);
				}
			} else {
				printf("ERROR: should have a value!");
				exit(1);
			}

			// check that all values are over the expected index
			// field value (80)
			FieldValue dur_fv = RecordBuffer_kv_log_duration(&r);
			adapter::NullableLong dur = FieldValue_as_long(&dur_fv);
			if (dur.HasValue()) {
				int64_t d = dur.Value();
				if (d <= 80) {
					printf("Unexpected kv log duration\n");
					exit(1);
				}
			} else {
				printf("ERROR: should have a value!");
				exit(1);
			}

			*out_count += 1;
		}

		inline void Finalize() {
			// Print result
			printf("Finalizing Microbench 3 max count %ld\n", *out_count);
		}

		inline core::KeyHash get_hash() const {
			return core::KeyHash{ core::Utility::GetHashCode(psf_id_, value_) };
		}

		inline bool check(const core::KeyPointer* kpt) {
			return kpt->mode == 1 && kpt->inline_psf_id == psf_id_ && kpt->value == value_;
		}

	protected:
		Status DeepCopy_Internal(IAsyncContext*& context_copy) {
			return IAsyncContext::DeepCopy_Internal(*this, context_copy);
		}

	private:
		uint32_t psf_id_;
		int32_t value_;
		uint64_t ts_min;
		uint64_t ts_max;
		uint64_t *out_count;
};



