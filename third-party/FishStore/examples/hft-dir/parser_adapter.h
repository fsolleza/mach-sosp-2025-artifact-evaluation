#pragma once

#include "helpers.h"
#include "adapters/common_utils.h"

// NOTE:
// NullableInt from fishstore core is DIFFERENT from NullableInt in the
// parser_api. (WTF.)
// Good thing we did it correctly the first time but here's the difference:
// NullableInt in fishstore's core/psf.h is defined as { is_null, value } while the one
// in the parser/parser_api.h is defined as { has_value, value }. See the teeny tiny
// fucking difference??

class ParserField
{
	public:
		ParserField(int64_t field_id, RecordBuffer record_, FieldName field_):
      field_id(field_id),
			record(std::move(record_)),
			field(field_)
		{ }

		// THIS FIELD SHOULD BE A COUNTER GENERATED IN THE FIELD PARSING LOOP IN THE
		// PARSER RECORD STRUCT. THIS IS BECAUSE THE MAP CREATED IN MAKE INLINE PSF
		// INSERTS FIELDS BASED ON THIS ORDER.
		// IN BATCH INSERT IT POPULATES A VARIABLE FIELD MAP WITH THIS FIELD ID,
		// THEN LOOKS UP THE VALUE IN THE MAP POPULATED BY MAKE INLINE PSF. IF THE
		// TWO DON'T MATCH, IT FAILS __SILENTLY__.
		// THE WAY THE CODE IS SETUP ALSO MEANS THAT THE EASIEST WAY TO RESOLVE THIS
		// IS HAVE ONE PSF AT A TIME
		inline int64_t FieldId() const {
			return field_id;
		}

		inline adapter::NullableInt GetAsInt() const {
			RecordBuffer * rec = (RecordBuffer *)&record;
			FieldValue v = RecordBuffer_get_field(rec, field);
			return FieldValue_as_int(&v);
		}

		inline adapter::NullableLong GetAsLong() const {
			RecordBuffer * rec = (RecordBuffer *)&record;
			FieldValue v = RecordBuffer_get_field(rec, field);
			return FieldValue_as_long(&v);
		}

		inline adapter::NullableBool GetAsBool() const {
			RecordBuffer * rec = (RecordBuffer *)&record;
			FieldValue v = RecordBuffer_get_field(rec, field);
			return FieldValue_as_bool(&v);
		}

		inline adapter::NullableFloat GetAsFloat() const {
			RecordBuffer * rec = (RecordBuffer *)&record;
			FieldValue v = RecordBuffer_get_field(rec, field);
			return FieldValue_as_float(&v);
		}

		inline adapter::NullableDouble GetAsDouble() const {
			RecordBuffer * rec = (RecordBuffer *)&record;
			FieldValue v = RecordBuffer_get_field(rec, field);
			return FieldValue_as_double(&v);
		}

		inline adapter::NullableString GetAsString() const {
			RecordBuffer * rec = (RecordBuffer *)&record;
			FieldValue v = RecordBuffer_get_field(rec, field);
			return FieldValue_as_string(&v);
		}

		inline adapter::NullableStringRef GetAsStringRef() const {
			RecordBuffer * rec = (RecordBuffer *)&record;
			FieldValue v = RecordBuffer_get_field(rec, field);
			return FieldValue_as_string_ref(&v);
		}

		int64_t field_id;
		FieldName field;
		RecordBuffer record;
};

class ParserRecord
{
	public:
		ParserRecord() = default;

		ParserRecord(RecordBuffer record_, std::vector<FieldName> &field_names):
			record(std::move(record_))
		{
      int64_t id = 0;
			fields.reserve(field_names.size());
			for (auto &item: field_names) {
				ParserField field = ParserField(id, record, item);
				fields.push_back(field);
        ++id;
			}
		}

		inline const std::vector<ParserField> &GetFields() const {
			return fields;
		}

		inline adapter::StringRef GetRawText() const {
			RecordBuffer *rec = (RecordBuffer *)&record;
			size_t len = (size_t)RecordBuffer_size(rec);
			const char *buf = (const char *)record.buffer;
			return adapter::StringRef(buf, len);
		}

	private:
		RecordBuffer record;
		std::vector<ParserField> fields;
};

class Parser
{
	public:
		Parser(std::vector<std::string> field_names_)
			: field_names(std::move(field_names_))
		{
			for (auto &item: field_names) {
				FieldName id = FieldName_from_string(item);
				field_ids.push_back(id);
			}
		}

		inline void Load(const char *buffer, size_t length)
		{
			data.buffer = (uint8_t *)buffer;
			data.length = length;
			offset = 16;
		}

		inline bool HasNext()
		{
			return offset < data.length;
		}

		inline const ParserRecord &NextRecord()
		{
			assert(offset < data.length);

			// Assemble record pointer
			RecordBuffer r;
			r.buffer = data.buffer + offset;
			record = ParserRecord(r, field_ids);

			// Adjust offset to the next record
			offset += RecordBuffer_size(&r);

			return record;
		}

	private:
		// keep these around for memory safety reasons
		DataBuffer data;
		size_t offset;
		std::vector<std::string> field_names;
		std::vector<FieldName> field_ids;
		ParserRecord record;
};

class ParserAdapter {
	public:
		typedef Parser parser_t;
		typedef ParserField field_t;
		typedef ParserRecord record_t;

		inline static parser_t *NewParser(const std::vector<std::string> &fields) {
			return new parser_t{fields};
		}

		inline static void Load(parser_t *const parser, const char *payload, size_t length, size_t offset = 0) {
			assert(offset <= length);
			parser->Load(payload + offset, length - offset);
		}

		inline static bool HasNext(parser_t *const parser) {
			return parser->HasNext();
		}

		inline static const record_t &NextRecord(parser_t *const parser) {
			return parser->NextRecord();
		}
};

class IngestScalingParserField
{
	public:
		IngestScalingParserField() = default;
		IngestScalingParserField(const char *buffer, uint64_t offset, uint64_t size):
			buffer(buffer), offset(offset), size(size)
		{ }

		// For this workload, we are not indexing so we just return
		// zeros across the board

		inline int64_t FieldId() const {
			return 0;
		}

		inline adapter::NullableInt GetAsInt() const {
			return {false, 0};
		}

		inline adapter::NullableLong GetAsLong() const {
			return {false, 0};
		}

		inline adapter::NullableBool GetAsBool() const {
			return {false, false};
		}

		inline adapter::NullableFloat GetAsFloat() const {
			return {false, 0.0};
		}

		inline adapter::NullableDouble GetAsDouble() const {
			return {false, 0.0};
		}

		inline adapter::NullableString GetAsString() const {
			std::string_view temp{};
			return { false, std::string(temp) };
		}

		inline adapter::NullableStringRef GetAsStringRef() const {
			std::string_view temp{};
			adapter::StringRef str_ref { temp.data(), temp.length() };
			return { false, str_ref };
		}

	private:
		const char *buffer;
		uint64_t offset;
		uint64_t size;
};

class IngestScalingParserRecord
{
	public:
		IngestScalingParserRecord() = default;
		IngestScalingParserRecord(const char *buffer, uint64_t offset, uint64_t size):
			buffer(buffer), offset(offset), size(size)
		{
			IngestScalingParserField field = IngestScalingParserField(buffer, offset, size);
			fields.push_back(field);
		}

		inline const std::vector<IngestScalingParserField> &GetFields() const {
			return fields;
		}

		inline adapter::StringRef GetRawText() const {
			return adapter::StringRef(buffer + offset, size);
		}

	private:
		const char *buffer;
		uint64_t offset;
		uint64_t size;
		std::vector<IngestScalingParserField> fields;
};

class IngestScalingParser
{
	public:
		IngestScalingParser(std::vector<std::string> field_names):
			field_names(std::move(field_names))
		{ }

		inline void Load(const char *b, size_t l)
		{
			buffer = b;
			length = l;
			offset = 8;
			item_size = u64_from_be_bytes((uint8_t*)b);
		}

		inline bool HasNext()
		{
			return offset < length;
		}

		inline const IngestScalingParserRecord &NextRecord()
		{
			// Assemble record pointer
			record = IngestScalingParserRecord(buffer, offset, item_size);

			// Adjust offset to the next record
			offset += item_size;

			return record;
		}

	private:
		std::vector<std::string> field_names;
		const char * buffer;
		size_t length;
		size_t offset;
		uint64_t item_size;
		IngestScalingParserRecord record;
};

class IngestScalingParserAdapter
{
	public:
		typedef IngestScalingParser parser_t;
		typedef IngestScalingParserField field_t;
		typedef IngestScalingParserRecord record_t;

		inline static parser_t *NewParser(const std::vector<std::string> &fields) {
			return new parser_t{fields};
		}

		inline static void Load(parser_t *const parser, const char *payload, size_t length, size_t offset = 0) {
			assert(offset <= length);
			parser->Load(payload + offset, length - offset);
		}

		inline static bool HasNext(parser_t *const parser) {
			return parser->HasNext();
		}

		inline static const record_t &NextRecord(parser_t *const parser) {
			return parser->NextRecord();
		}
};
