/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include <algorithm>
#include <optional>
#include <span>
#include <vector>

#include <boost/test/unit_test.hpp>
#include <seastar/core/simple-stream.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/util/memory-data-source.hh>

#include "dht/i_partitioner.hh"
#include "mutation/mutation.hh"
#include "replica/logstor/ondisk.hh"
#include "replica/logstor/record_value.hh"
#include "replica/logstor/segment_io.hh"
#include "replica/logstor/single_row_partition.hh"
#include "schema/schema_builder.hh"

// This header gives the records for the logstor unit tests: a key-value schema, its mutations and
// records, and readers of the chunks that the records end up in.
namespace tests::logstor {

inline schema_ptr make_kv_schema() {
    return schema_builder(1, "ks", "cf")
            .with_column("pk", utf8_type, column_kind::partition_key)
            .with_column("v", utf8_type)
            .build();
}

inline mutation make_kv_mutation(schema_ptr schema, const partition_key& key, sstring value, api::timestamp_type ts = api::min_timestamp) {
    auto dk = dht::decorate_key(*schema, key);
    mutation m(schema, dk);
    auto& row = m.partition().clustered_row(*schema, clustering_key::make_empty());
    row.apply(row_marker(ts));
    const auto& v_def = *schema->get_column_definition("v");
    row.cells().apply(v_def, atomic_cell::make_live(*v_def.type, ts, serialized(value)));
    return m;
}

inline mutation make_kv_mutation(schema_ptr schema, sstring pk, sstring value, api::timestamp_type ts = api::min_timestamp) {
    auto key = partition_key::from_single_value(*schema, serialized(pk));
    return make_kv_mutation(std::move(schema), key, std::move(value), ts);
}

inline replica::logstor::log_record make_log_record(schema_ptr schema, sstring pk, sstring value, api::timestamp_type ts = api::min_timestamp) {
    auto m = make_kv_mutation(schema, std::move(pk), std::move(value), ts);
    return replica::logstor::log_record {
        .header = {
            .key = m.decorated_key(),
            .timestamp = ts,
            .table = schema->id(),
        },
        .value = replica::logstor::encode_record_value(m),
    };
}

// The mutation that a record value decodes to, for the tests that compare mutations: the decoder
// gives a single_row_partition.
inline mutation decode_to_mutation(const replica::logstor::record_value& v, schema_ptr s, const replica::logstor::record_header& h) {
    auto partition = replica::logstor::decode_record_value(v, *s, h);
    return replica::logstor::to_mutation(std::move(s), h.key, std::move(partition));
}

// The mutation that a whole record holds.
inline mutation to_mutation(const replica::logstor::log_record& r, schema_ptr s) {
    return decode_to_mutation(r.value, std::move(s), r.header);
}

inline temporary_buffer<char> copy_chunk(bytes_view chunk) {
    return temporary_buffer<char>(reinterpret_cast<const char*>(chunk.data()), chunk.size());
}

inline replica::logstor::log_record read_record_at_location(const temporary_buffer<char>& segment, replica::logstor::record_location loc) {
    BOOST_REQUIRE_EQUAL(loc.offset + loc.size <= segment.size(), true);

    temporary_buffer<char> buf(loc.size);
    std::copy_n(segment.get() + loc.offset, loc.size, buf.get_write());
    auto frame = simple_memory_input_stream(buf.begin(), buf.size());
    return replica::logstor::ondisk::read_record_frame(frame);
}

inline replica::logstor::ondisk::chunk_header_common read_chunk_header_common(const temporary_buffer<char>& buf) {
    auto in = seastar::simple_memory_input_stream(buf.get(), replica::logstor::ondisk::chunk_header_common_size);
    return ser::deserialize(in, std::type_identity<replica::logstor::ondisk::chunk_header_common>{});
}

// Returns the bytes of the chunk header at the start of `buf` that follow its common part.
inline std::span<const char> chunk_header_rest(const temporary_buffer<char>& buf, replica::logstor::segment_kind kind) {
    namespace ondisk = replica::logstor::ondisk;
    return std::span(buf.get() + ondisk::chunk_header_common_size, ondisk::chunk_header_size(kind) - ondisk::chunk_header_common_size);
}

// Returns the chunk header at the start of `buf`, or nullopt if it is not valid.
inline std::optional<replica::logstor::ondisk::chunk_header> read_chunk_header(const temporary_buffer<char>& buf) {
    const auto common = read_chunk_header_common(buf);
    return replica::logstor::ondisk::parse_chunk_header(common, chunk_header_rest(buf, common.kind));
}

// A record that a scan of a segment found, and its location.
struct scanned_record {
    replica::logstor::record_location location;
    replica::logstor::log_record record;
};

inline std::vector<scanned_record> scan_buffer_records(const temporary_buffer<char>& buf, replica::logstor::log_segment_id segment_id) {
    temporary_buffer<char> copy(buf.size());
    std::copy_n(buf.get(), buf.size(), copy.get_write());
    auto in = seastar::util::as_input_stream(std::move(copy));
    std::vector<scanned_record> records;

    replica::logstor::segment_scanner scanner(in, segment_id, buf.size());
    while (auto* chunk = scanner.next_chunk().get()) {
        for (const auto& record : chunk->records()) {
            records.push_back(scanned_record{.location = record.location, .record = record.to_log_record()});
        }
    }
    in.close().get();

    return records;
}

} // namespace tests::logstor
