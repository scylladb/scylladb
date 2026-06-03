/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#include <boost/test/unit_test.hpp>
#include <algorithm>
#include <functional>
#include <vector>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/util/memory-data-source.hh>
#include <seastar/util/defer.hh>

#include "replica/logstor/ondisk.hh"
#include "replica/logstor/write_buffer.hh"
#include <seastar/testing/thread_test_case.hh>

#include "idl/logstor.dist.hh"
#include "idl/logstor.dist.impl.hh"
#include "replica/logstor/segment_io.hh"
#include "schema/schema_builder.hh"
#include <seastar/core/simple-stream.hh>
#include "test/lib/mutation_assertions.hh"

using namespace replica::logstor;

namespace {

schema_ptr make_kv_schema() {
    return schema_builder(1, "ks", "cf")
            .with_column("pk", utf8_type, column_kind::partition_key)
            .with_column("v", utf8_type)
            .build();
}

mutation make_kv_mutation(schema_ptr schema, sstring pk, sstring value, api::timestamp_type ts = api::min_timestamp) {
    auto key = partition_key::from_single_value(*schema, serialized(pk));
    auto dk = dht::decorate_key(*schema, key);
    mutation m(schema, dk);
    auto& row = m.partition().clustered_row(*schema, clustering_key::make_empty());
    row.apply(row_marker(ts));
    const auto& v_def = *schema->get_column_definition("v");
    row.cells().apply(v_def, atomic_cell::make_live(*v_def.type, ts, serialized(value)));
    return m;
}

log_record make_log_record(schema_ptr schema, sstring pk, sstring value, api::timestamp_type ts = api::min_timestamp) {
    auto m = make_kv_mutation(schema, std::move(pk), std::move(value), ts);
    return log_record {
        .header = {
            .key = primary_index_key{m.decorated_key()},
            .timestamp = ts,
            .table = schema->id(),
        },
        .mut = canonical_mutation(m)
    };
}

temporary_buffer<char> make_serialized_buffer_copy(const raw_write_buffer& wb) {
    temporary_buffer<char> buf(wb.serialized_size());
    std::copy_n(wb.data(), wb.serialized_size(), buf.get_write());
    return buf;
}

temporary_buffer<char> concat_serialized_buffers(std::initializer_list<const temporary_buffer<char>*> bufs) {
    size_t total_size = 0;
    for (const auto* buf : bufs) {
        total_size += buf->size();
    }

    temporary_buffer<char> out(total_size);
    size_t offset = 0;
    for (const auto* buf : bufs) {
        std::copy_n(buf->get(), buf->size(), out.get_write() + offset);
        offset += buf->size();
    }
    return out;
}

log_record read_record_at_location(const temporary_buffer<char>& segment, log_location loc) {
    BOOST_REQUIRE_EQUAL(loc.offset + loc.size <= segment.size(), true);

    temporary_buffer<char> buf(loc.size);
    std::copy_n(segment.get() + loc.offset, loc.size, buf.get_write());
    return deserialize_log_record(simple_memory_input_stream(buf.begin(), buf.size()));
}

void flip_byte(temporary_buffer<char>& buf, size_t offset) {
    buf.get_write()[offset] ^= char(0x1);
}

std::optional<segment_header> read_segment_header_from_bytes(const temporary_buffer<char>& buf) {
    temporary_buffer<char> copy(buf.size());
    std::copy_n(buf.get(), buf.size(), copy.get_write());
    auto in = seastar::util::as_input_stream(std::move(copy));
    auto header = read_segment_header(in).get();
    in.close().get();
    return header;
}

}

// Checks that sealing a full raw write buffer writes the expected header fields.
SEASTAR_THREAD_TEST_CASE(test_logstor_write_buffer_record_and_header_serialization) {
    auto schema = make_kv_schema();
    auto expected = make_log_record(schema, "pk0", "v0", api::timestamp_type(7));

    raw_write_buffer wb(32 * 1024, segment_kind::full);
    auto writer = log_record_writer(expected);
    auto expected_data_size = size_t(ondisk::record_header_size) + writer.size();
    expected_data_size = ((expected_data_size + ondisk::record_alignment - 1) / ondisk::record_alignment) * ondisk::record_alignment;
    wb.append(std::move(writer));
    wb.seal(segment_sequence{17}, schema->id(), ondisk::block_alignment);

    BOOST_REQUIRE_EQUAL(wb.serialized_size() % ondisk::block_alignment, 0u);

    seastar::simple_memory_input_stream in(wb.data(), wb.serialized_size());
    auto bh = ser::deserialize(in, std::type_identity<ondisk::buffer_header>{});
    BOOST_REQUIRE(raw_write_buffer::validate_header(bh));
    BOOST_REQUIRE(bh.kind == segment_kind::full);
    BOOST_REQUIRE_EQUAL(bh.segment_seq.value, 17u);
    BOOST_REQUIRE_EQUAL(bh.data_size, expected_data_size);

    auto sh = ser::deserialize(in, std::type_identity<ondisk::segment_header>{});
    BOOST_REQUIRE_EQUAL(sh.table, schema->id());
    BOOST_REQUIRE_EQUAL(sh.first_token, expected.header.key.dk.token());
    BOOST_REQUIRE_EQUAL(sh.last_token, expected.header.key.dk.token());
}

// Checks that a raw write buffer can hold and seal a record whose serialized size is exactly max_record_size().
SEASTAR_THREAD_TEST_CASE(test_logstor_write_buffer_accepts_record_at_max_record_size) {
    auto schema = make_kv_schema();

    raw_write_buffer wb(ondisk::block_alignment, segment_kind::mixed);
    auto max_size = wb.max_record_size();

    sstring value;
    auto record = make_log_record(schema, "pk", "", api::timestamp_type(27));
    log_record_writer writer(record);

    while (writer.size() < max_size) {
        value += "x";
        record = make_log_record(schema, "pk", value, api::timestamp_type(27));
        writer = log_record_writer(record);
    }

    BOOST_REQUIRE_EQUAL(writer.size(), max_size);
    BOOST_REQUIRE(wb.can_fit(writer));

    wb.append(writer);
    wb.seal(segment_sequence{29}, std::nullopt, ondisk::block_alignment);

    BOOST_REQUIRE_EQUAL(wb.serialized_size(), ondisk::block_alignment);
}

// Checks that scan_segment() returns mixed-buffer log locations that can be used to read back the expected records.
SEASTAR_THREAD_TEST_CASE(test_logstor_segment_scan_mixed_buffers_report_readable_log_locations) {
    auto schema = make_kv_schema();

    raw_write_buffer wb0(64 * 1024, segment_kind::mixed);
    raw_write_buffer wb1(64 * 1024, segment_kind::mixed);

    auto expected0 = make_kv_mutation(schema, "pk0", "value0", api::timestamp_type(11));
    auto expected1 = make_kv_mutation(schema, "pk1", "value1-longer", api::timestamp_type(12));
    auto expected2 = make_kv_mutation(schema, "pk2", "v2", api::timestamp_type(13));
    auto expected3 = make_kv_mutation(schema, "pk3", "value-three-is-even-longer-than-before", api::timestamp_type(14));

    wb0.append(log_record_writer(make_log_record(schema, "pk0", "value0", api::timestamp_type(11))));
    wb0.append(log_record_writer(make_log_record(schema, "pk1", "value1-longer", api::timestamp_type(12))));
    wb1.append(log_record_writer(make_log_record(schema, "pk2", "v2", api::timestamp_type(13))));
    wb1.append(log_record_writer(make_log_record(schema, "pk3", "value-three-is-even-longer-than-before", api::timestamp_type(14))));

    wb0.seal(segment_sequence{23}, std::nullopt, ondisk::block_alignment);
    wb1.seal(segment_sequence{23}, std::nullopt, ondisk::block_alignment);

    auto serialized0 = make_serialized_buffer_copy(wb0);
    auto serialized1 = make_serialized_buffer_copy(wb1);
    auto segment = concat_serialized_buffers({&serialized0, &serialized1});
    const auto segment_size = segment.size();
    const auto* segment_data = segment.get();
    auto in = seastar::util::as_input_stream(std::move(segment));

    std::vector<segment_header> seen_segment_headers;
    std::vector<log_record_header> seen_record_headers;
    std::vector<log_location> seen_locations;

    scan_segment(in, log_segment_id{3}, segment_size,
        [&seen_segment_headers] (const segment_header& sh) {
            seen_segment_headers.push_back(sh);
            return make_ready_future<>();
        },
        [&seen_record_headers, &seen_locations] (log_location loc, const log_record_header& rh) {
            seen_record_headers.push_back(rh);
            seen_locations.push_back(loc);
            return want_data::yes;
        },
        [] (log_location, log_record) {
            return make_ready_future<>();
         }).get();
    in.close().get();

    temporary_buffer<char> segment_copy(segment_size);
    std::copy_n(segment_data, segment_size, segment_copy.get_write());

    BOOST_REQUIRE_EQUAL(seen_segment_headers.size(), 2u);
    for (const auto& sh : seen_segment_headers) {
        BOOST_REQUIRE(sh.kind == segment_kind::mixed);
        BOOST_REQUIRE_EQUAL(sh.segment_seq.value, 23u);
    }

    BOOST_REQUIRE_EQUAL(seen_record_headers.size(), 4u);
    BOOST_REQUIRE_EQUAL(seen_record_headers[0].timestamp, api::timestamp_type(11));
    BOOST_REQUIRE_EQUAL(seen_record_headers[1].timestamp, api::timestamp_type(12));
    BOOST_REQUIRE_EQUAL(seen_record_headers[2].timestamp, api::timestamp_type(13));
    BOOST_REQUIRE_EQUAL(seen_record_headers[3].timestamp, api::timestamp_type(14));
    BOOST_REQUIRE_EQUAL(seen_record_headers[0].table, schema->id());
    BOOST_REQUIRE_EQUAL(seen_record_headers[1].table, schema->id());
    BOOST_REQUIRE_EQUAL(seen_record_headers[2].table, schema->id());
    BOOST_REQUIRE_EQUAL(seen_record_headers[3].table, schema->id());

    BOOST_REQUIRE_EQUAL(seen_locations.size(), 4u);
    assert_that(read_record_at_location(segment_copy, seen_locations[0]).mut.to_mutation(schema)).is_equal_to(expected0);
    assert_that(read_record_at_location(segment_copy, seen_locations[1]).mut.to_mutation(schema)).is_equal_to(expected1);
    assert_that(read_record_at_location(segment_copy, seen_locations[2]).mut.to_mutation(schema)).is_equal_to(expected2);
    assert_that(read_record_at_location(segment_copy, seen_locations[3]).mut.to_mutation(schema)).is_equal_to(expected3);

    auto maybe_header = read_segment_header_from_bytes(segment_copy);
    BOOST_REQUIRE(maybe_header);
    BOOST_REQUIRE(maybe_header->kind == segment_kind::mixed);
    BOOST_REQUIRE_EQUAL(maybe_header->segment_seq.value, 23u);
    BOOST_REQUIRE(std::holds_alternative<segment_header::mixed>(maybe_header->v));
}

// Checks that scan_segment() only delivers records whose headers were accepted with want_data::yes.
SEASTAR_THREAD_TEST_CASE(test_logstor_segment_scan_returns_only_selected_records) {
    auto schema = make_kv_schema();

    raw_write_buffer wb0(64 * 1024, segment_kind::mixed);
    raw_write_buffer wb1(64 * 1024, segment_kind::mixed);

    auto expected1 = make_kv_mutation(schema, "pk1", "value1", api::timestamp_type(72));
    auto expected3 = make_kv_mutation(schema, "pk3", "value3", api::timestamp_type(74));

    wb0.append(log_record_writer(make_log_record(schema, "pk0", "value0", api::timestamp_type(71))));
    wb0.append(log_record_writer(make_log_record(schema, "pk1", "value1", api::timestamp_type(72))));
    wb1.append(log_record_writer(make_log_record(schema, "pk2", "value2", api::timestamp_type(73))));
    wb1.append(log_record_writer(make_log_record(schema, "pk3", "value3", api::timestamp_type(74))));

    wb0.seal(segment_sequence{81}, std::nullopt, ondisk::block_alignment);
    wb1.seal(segment_sequence{81}, std::nullopt, ondisk::block_alignment);

    auto serialized0 = make_serialized_buffer_copy(wb0);
    auto serialized1 = make_serialized_buffer_copy(wb1);
    auto segment = concat_serialized_buffers({&serialized0, &serialized1});
    const auto segment_size = segment.size();
    auto in = seastar::util::as_input_stream(std::move(segment));

    std::vector<api::timestamp_type> seen_header_timestamps;
    std::vector<log_record> selected_records;

    scan_segment(in, log_segment_id{4}, segment_size,
        [] (const segment_header&) {
            return make_ready_future<>();
        },
        [&seen_header_timestamps] (log_location, const log_record_header& rh) {
            seen_header_timestamps.push_back(rh.timestamp);
            return (rh.timestamp == api::timestamp_type(72) || rh.timestamp == api::timestamp_type(74))
                    ? want_data::yes : want_data::no;
        },
        [&selected_records] (log_location, log_record rec) {
            selected_records.push_back(std::move(rec));
            return make_ready_future<>();
        }).get();
    in.close().get();

    BOOST_REQUIRE_EQUAL(seen_header_timestamps.size(), 4u);
    BOOST_REQUIRE_EQUAL(seen_header_timestamps[0], api::timestamp_type(71));
    BOOST_REQUIRE_EQUAL(seen_header_timestamps[1], api::timestamp_type(72));
    BOOST_REQUIRE_EQUAL(seen_header_timestamps[2], api::timestamp_type(73));
    BOOST_REQUIRE_EQUAL(seen_header_timestamps[3], api::timestamp_type(74));

    BOOST_REQUIRE_EQUAL(selected_records.size(), 2u);
    BOOST_REQUIRE_EQUAL(selected_records[0].header.timestamp, api::timestamp_type(72));
    BOOST_REQUIRE_EQUAL(selected_records[1].header.timestamp, api::timestamp_type(74));
    assert_that(selected_records[0].mut.to_mutation(schema)).is_equal_to(expected1);
    assert_that(selected_records[1].mut.to_mutation(schema)).is_equal_to(expected3);
}

// Checks that scan_segment() reads all records from a full buffer with varying serialized sizes.
SEASTAR_THREAD_TEST_CASE(test_logstor_segment_scan_reads_full_buffer_records_with_varying_lengths) {
    auto schema = make_kv_schema();

    raw_write_buffer wb(64 * 1024, segment_kind::full);
    auto expected0 = make_kv_mutation(schema, "pk-full-0", "x", api::timestamp_type(31));
    auto expected1 = make_kv_mutation(schema, "pk-full-1-with-longer-key", "medium-value", api::timestamp_type(32));
    auto expected2 = make_kv_mutation(schema, "pk-full-2", "value-with-a-significantly-longer-payload-to-exercise-varying-record-sizes", api::timestamp_type(33));
    wb.append(log_record_writer(make_log_record(schema, "pk-full-0", "x", api::timestamp_type(31))));
    wb.append(log_record_writer(make_log_record(schema, "pk-full-1-with-longer-key", "medium-value", api::timestamp_type(32))));
    wb.append(log_record_writer(make_log_record(schema, "pk-full-2", "value-with-a-significantly-longer-payload-to-exercise-varying-record-sizes", api::timestamp_type(33))));
    wb.seal(segment_sequence{41}, schema->id(), ondisk::block_alignment);

    auto serialized = make_serialized_buffer_copy(wb);
    auto maybe_header = read_segment_header_from_bytes(serialized);
    auto in = seastar::util::as_input_stream(std::move(serialized));

    std::vector<segment_header> seen_segment_headers;
    std::vector<log_record> seen_records;

    scan_segment(in, log_segment_id{7}, wb.serialized_size(),
        [&seen_segment_headers] (const segment_header& sh) {
            seen_segment_headers.push_back(sh);
            return make_ready_future<>();
        },
        [] (log_location, const log_record_header&) {
            return want_data::yes;
        },
        [&seen_records] (log_location, log_record rec) {
            seen_records.push_back(std::move(rec));
            return make_ready_future<>();
        }).get();
    in.close().get();

    BOOST_REQUIRE_EQUAL(seen_segment_headers.size(), 1u);
    BOOST_REQUIRE(seen_segment_headers.front().kind == segment_kind::full);
    BOOST_REQUIRE_EQUAL(seen_segment_headers.front().segment_seq.value, 41u);
    BOOST_REQUIRE(std::holds_alternative<segment_header::full>(seen_segment_headers.front().v));
    BOOST_REQUIRE_EQUAL(seen_records.size(), 3u);
    BOOST_REQUIRE_EQUAL(seen_records[0].header.table, schema->id());
    BOOST_REQUIRE_EQUAL(seen_records[1].header.table, schema->id());
    BOOST_REQUIRE_EQUAL(seen_records[2].header.table, schema->id());
    BOOST_REQUIRE_EQUAL(seen_records[0].header.timestamp, api::timestamp_type(31));
    BOOST_REQUIRE_EQUAL(seen_records[1].header.timestamp, api::timestamp_type(32));
    BOOST_REQUIRE_EQUAL(seen_records[2].header.timestamp, api::timestamp_type(33));
    assert_that(seen_records[0].mut.to_mutation(schema)).is_equal_to(expected0);
    assert_that(seen_records[1].mut.to_mutation(schema)).is_equal_to(expected1);
    assert_that(seen_records[2].mut.to_mutation(schema)).is_equal_to(expected2);

    BOOST_REQUIRE(maybe_header);
    BOOST_REQUIRE(maybe_header->kind == segment_kind::full);
    BOOST_REQUIRE_EQUAL(maybe_header->segment_seq.value, 41u);
    BOOST_REQUIRE(std::holds_alternative<segment_header::full>(maybe_header->v));
    auto& full = std::get<segment_header::full>(maybe_header->v);
    auto expected_first_token = std::min({
        seen_records[0].header.key.dk.token(),
        seen_records[1].header.key.dk.token(),
        seen_records[2].header.key.dk.token(),
    });
    auto expected_last_token = std::max({
        seen_records[0].header.key.dk.token(),
        seen_records[1].header.key.dk.token(),
        seen_records[2].header.key.dk.token(),
    });
    BOOST_REQUIRE_EQUAL(full.table, schema->id());
    BOOST_REQUIRE_EQUAL(full.first_token, expected_first_token);
    BOOST_REQUIRE_EQUAL(full.last_token, expected_last_token);
}

// Checks that scan_segment() stops before a later mixed buffer whose sequence number is lower.
SEASTAR_THREAD_TEST_CASE(test_logstor_segment_scan_stops_on_mixed_buffer_lower_sequence_number) {
    auto schema = make_kv_schema();

    raw_write_buffer wb0(64 * 1024, segment_kind::mixed);
    raw_write_buffer wb1(64 * 1024, segment_kind::mixed);

    auto expected0 = make_kv_mutation(schema, "pk0", "value0", api::timestamp_type(51));
    auto expected1 = make_kv_mutation(schema, "pk1", "value1", api::timestamp_type(52));
    wb0.append(log_record_writer(make_log_record(schema, "pk0", "value0", api::timestamp_type(51))));
    wb0.append(log_record_writer(make_log_record(schema, "pk1", "value1", api::timestamp_type(52))));
    wb1.append(log_record_writer(make_log_record(schema, "pk2", "value2", api::timestamp_type(53))));
    wb1.append(log_record_writer(make_log_record(schema, "pk3", "value3", api::timestamp_type(54))));

    wb0.seal(segment_sequence{61}, std::nullopt, ondisk::block_alignment);
    wb1.seal(segment_sequence{60}, std::nullopt, ondisk::block_alignment);

    auto serialized0 = make_serialized_buffer_copy(wb0);
    auto serialized1 = make_serialized_buffer_copy(wb1);
    auto segment = concat_serialized_buffers({&serialized0, &serialized1});
    const auto segment_size = segment.size();
    auto in = seastar::util::as_input_stream(std::move(segment));

    std::vector<segment_header> seen_segment_headers;
    std::vector<log_record_header> seen_record_headers;
    std::vector<canonical_mutation> seen_mutations;

    scan_segment(in, log_segment_id{5}, segment_size,
        [&seen_segment_headers] (const segment_header& sh) {
            seen_segment_headers.push_back(sh);
            return make_ready_future<>();
        },
        [&seen_record_headers] (log_location, const log_record_header& rh) {
            seen_record_headers.push_back(rh);
            return want_data::yes;
        },
        [&seen_mutations] (log_location, log_record rec) {
            seen_mutations.push_back(std::move(rec.mut));
            return make_ready_future<>();
        }).get();
    in.close().get();

    BOOST_REQUIRE_EQUAL(seen_segment_headers.size(), 1u);
    BOOST_REQUIRE(seen_segment_headers.front().kind == segment_kind::mixed);
    BOOST_REQUIRE_EQUAL(seen_segment_headers.front().segment_seq.value, 61u);

    BOOST_REQUIRE_EQUAL(seen_record_headers.size(), 2u);
    BOOST_REQUIRE_EQUAL(seen_record_headers[0].timestamp, api::timestamp_type(51));
    BOOST_REQUIRE_EQUAL(seen_record_headers[1].timestamp, api::timestamp_type(52));

    BOOST_REQUIRE_EQUAL(seen_mutations.size(), 2u);
    assert_that(seen_mutations[0].to_mutation(schema)).is_equal_to(expected0);
    assert_that(seen_mutations[1].to_mutation(schema)).is_equal_to(expected1);
}

// Checks that scan_segment() stops after a later mixed buffer with a corrupted header crc.
SEASTAR_THREAD_TEST_CASE(test_logstor_segment_scan_stops_on_corrupted_later_mixed_buffer_header) {
    auto schema = make_kv_schema();

    raw_write_buffer wb0(64 * 1024, segment_kind::mixed);
    raw_write_buffer wb1(64 * 1024, segment_kind::mixed);

    auto expected0 = make_kv_mutation(schema, "pk0", "value0", api::timestamp_type(91));
    auto expected1 = make_kv_mutation(schema, "pk1", "value1", api::timestamp_type(92));

    wb0.append(log_record_writer(make_log_record(schema, "pk0", "value0", api::timestamp_type(91))));
    wb0.append(log_record_writer(make_log_record(schema, "pk1", "value1", api::timestamp_type(92))));
    wb1.append(log_record_writer(make_log_record(schema, "pk2", "value2", api::timestamp_type(93))));
    wb1.append(log_record_writer(make_log_record(schema, "pk3", "value3", api::timestamp_type(94))));

    wb0.seal(segment_sequence{101}, std::nullopt, ondisk::block_alignment);
    wb1.seal(segment_sequence{101}, std::nullopt, ondisk::block_alignment);

    auto serialized0 = make_serialized_buffer_copy(wb0);
    auto serialized1 = make_serialized_buffer_copy(wb1);
    flip_byte(serialized1, ondisk::buffer_header_size - sizeof(uint32_t));

    auto segment = concat_serialized_buffers({&serialized0, &serialized1});
    const auto segment_size = segment.size();
    auto in = seastar::util::as_input_stream(std::move(segment));

    std::vector<segment_header> seen_segment_headers;
    std::vector<log_record_header> seen_record_headers;
    std::vector<canonical_mutation> seen_mutations;

    scan_segment(in, log_segment_id{6}, segment_size,
        [&seen_segment_headers] (const segment_header& sh) {
            seen_segment_headers.push_back(sh);
            return make_ready_future<>();
        },
        [&seen_record_headers] (log_location, const log_record_header& rh) {
            seen_record_headers.push_back(rh);
            return want_data::yes;
        },
        [&seen_mutations] (log_location, log_record rec) {
            seen_mutations.push_back(std::move(rec.mut));
            return make_ready_future<>();
        }).get();
    in.close().get();

    BOOST_REQUIRE_EQUAL(seen_segment_headers.size(), 1u);
    BOOST_REQUIRE_EQUAL(seen_record_headers.size(), 2u);
    BOOST_REQUIRE_EQUAL(seen_mutations.size(), 2u);
    BOOST_REQUIRE_EQUAL(seen_record_headers[0].timestamp, api::timestamp_type(91));
    BOOST_REQUIRE_EQUAL(seen_record_headers[1].timestamp, api::timestamp_type(92));
    assert_that(seen_mutations[0].to_mutation(schema)).is_equal_to(expected0);
    assert_that(seen_mutations[1].to_mutation(schema)).is_equal_to(expected1);
}
