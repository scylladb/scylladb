/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#include <boost/test/unit_test.hpp>
#include <algorithm>
#include <chrono>
#include <functional>
#include <stdexcept>
#include <vector>
#include <fmt/format.h>
#include <seastar/core/semaphore.hh>
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

temporary_buffer<char> make_serialized_buffer_copy(const write_buffer& wb) {
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

temporary_buffer<char> slice_buffer(const temporary_buffer<char>& buf, size_t offset, size_t size) {
    temporary_buffer<char> out(size);
    std::copy_n(buf.get() + offset, size, out.get_write());
    return out;
}

ondisk::buffer_header read_buffer_header(const temporary_buffer<char>& buf) {
    seastar::simple_memory_input_stream in(buf.get(), buf.size());
    return ser::deserialize(in, std::type_identity<ondisk::buffer_header>{});
}

struct rewritten_stream_result {
    temporary_buffer<char> data;
    size_t write_count{0};
};

rewritten_stream_result rewrite_streamed_segment(log_segment_id segment_id, segment_sequence seq_num, std::span<temporary_buffer<char>> chunks) {
    std::vector<char> written;
    size_t write_count = 0;

    streamed_segment_rewriter rewriter(segment_id, seq_num, [&written, &write_count] (bytes_view data) {
        auto* ptr = reinterpret_cast<const char*>(data.data());
        written.insert(written.end(), ptr, ptr + data.size());
        ++write_count;
        return make_ready_future<>();
    });

    rewriter.put(chunks).get();
    rewriter.close().get();

    temporary_buffer<char> out(written.size());
    std::copy(written.begin(), written.end(), out.get_write());
    return rewritten_stream_result{.data = std::move(out), .write_count = write_count};
}

struct scanned_record {
    log_location location;
    log_record record;
};

std::vector<scanned_record> scan_buffer_records(const temporary_buffer<char>& buf, log_segment_id segment_id) {
    temporary_buffer<char> copy(buf.size());
    std::copy_n(buf.get(), buf.size(), copy.get_write());
    auto in = seastar::util::as_input_stream(std::move(copy));
    std::vector<scanned_record> records;

    scan_segment(in, segment_id, buf.size(),
        [] (const segment_header&) {
            return make_ready_future<>();
        },
        [] (log_location, const log_record_header&) {
            return want_data::yes;
        },
        [&records] (log_location loc, log_record rec) {
            records.push_back(scanned_record{.location = loc, .record = std::move(rec)});
            return make_ready_future<>();
        }).get();
    in.close().get();

    return records;
}

void assert_log_record_matches(schema_ptr schema, const log_record& actual, const log_record& expected) {
    BOOST_REQUIRE_EQUAL(actual.header.timestamp, expected.header.timestamp);
    BOOST_REQUIRE_EQUAL(actual.header.table, expected.header.table);
    assert_that(actual.mut.to_mutation(schema)).is_equal_to(expected.mut.to_mutation(schema));
}

db::timeout_clock::time_point test_timeout() {
    return db::timeout_clock::now() + std::chrono::minutes(1);
}

buffered_writer_config make_buffered_writer_config(size_t buffer_size, size_t ring_size, size_t max_queued_write_bytes = 0) {
    return buffered_writer_config{
        .buffer_size = buffer_size,
        .ring_size = ring_size,
        .flush_sg = seastar::default_scheduling_group(),
        .max_queued_write_bytes = max_queued_write_bytes,
    };
}

sstring make_single_buffer_value(schema_ptr schema, size_t buffer_size) {
    sstring value;
    while (true) {
        auto next_value = value;
        next_value += sstring("x");
        auto next_record = make_log_record(schema, "pk0000", next_value, api::timestamp_type(17));
        auto next_writer = log_record_writer(next_record);
        raw_write_buffer wb(buffer_size, segment_kind::mixed);

        BOOST_REQUIRE(wb.can_fit(next_writer));
        wb.append(next_writer);
        if (!wb.can_fit(next_writer)) {
            return next_value;
        }

        value = std::move(next_value);
    }
}

log_record make_buffered_writer_record(schema_ptr schema, size_t idx, const sstring& value, api::timestamp_type ts) {
    return make_log_record(schema, sstring(fmt::format("pk{:04}", idx)), value, ts);
}

log_location wait_for_persisted(future<log_location_with_holder>& fut) {
    auto [loc, op] = fut.get();
    return loc;
}

struct test_flush_controller {
    struct flushed_buffer {
        temporary_buffer<char> data;
        log_location base_location;
        size_t record_count;
    };

    bool pause_flushes{false};
    std::optional<size_t> fail_flush_index;
    seastar::semaphore flush_started{0};
    seastar::semaphore flush_release{0};
    std::vector<flushed_buffer> flushed_buffers;
    size_t started_count{0};
    uint32_t next_segment_id{100};
    uint64_t next_sequence{1};

    future<> operator()(write_buffer& wb) {
        const auto flush_idx = started_count++;
        flush_started.signal(1);

        if (pause_flushes) {
            auto units = co_await get_units(flush_release, 1);
        }

        if (fail_flush_index && *fail_flush_index == flush_idx) {
            throw std::runtime_error("injected flush failure");
        }

        wb.seal(segment_sequence{next_sequence++}, std::nullopt, ondisk::block_alignment);
        auto base_location = log_location{
            .segment = log_segment_id{next_segment_id++},
            .offset = 0,
            .size = static_cast<uint32_t>(wb.serialized_size()),
        };
        flushed_buffers.push_back(flushed_buffer{
            .data = make_serialized_buffer_copy(wb),
            .base_location = base_location,
            .record_count = wb.record_count(),
        });
        co_await wb.complete_writes(base_location);
    }

    void wait_for_flush_starts(size_t target_count) {
        while (started_count < target_count) {
            flush_started.wait().get();
        }
    }

    void release_one_flush() {
        flush_release.signal(1);
    }

    const temporary_buffer<char>& buffer_for_segment(log_segment_id segment_id) const {
        for (const auto& flushed : flushed_buffers) {
            if (flushed.base_location.segment == segment_id) {
                return flushed.data;
            }
        }
        throw std::runtime_error(fmt::format("Missing flushed segment {}", segment_id));
    }

    std::vector<scanned_record> all_records() const {
        std::vector<scanned_record> records;
        for (const auto& flushed : flushed_buffers) {
            auto buffer_records = scan_buffer_records(flushed.data, flushed.base_location.segment);
            records.insert(records.end(), std::make_move_iterator(buffer_records.begin()), std::make_move_iterator(buffer_records.end()));
        }
        return records;
    }
};

void assert_records_in_order(schema_ptr schema, const std::vector<scanned_record>& actual, const std::vector<log_record>& expected) {
    BOOST_REQUIRE_EQUAL(actual.size(), expected.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        assert_log_record_matches(schema, actual[i].record, expected[i]);
    }
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

// Checks that the rewriter updates the initial full-buffer header sequence number.
SEASTAR_THREAD_TEST_CASE(test_logstor_streamed_segment_rewriter_rewrites_initial_full_buffer_header) {
    auto schema = make_kv_schema();

    raw_write_buffer wb(64 * 1024, segment_kind::full);
    auto expected0 = make_kv_mutation(schema, "pk0", "value0", api::timestamp_type(201));
    auto expected1 = make_kv_mutation(schema, "pk1-longer-key", "value1", api::timestamp_type(202));
    auto expected2 = make_kv_mutation(schema, "pk2", "value2-with-a-longer-payload", api::timestamp_type(203));
    wb.append(log_record_writer(make_log_record(schema, "pk0", "value0", api::timestamp_type(201))));
    wb.append(log_record_writer(make_log_record(schema, "pk1-longer-key", "value1", api::timestamp_type(202))));
    wb.append(log_record_writer(make_log_record(schema, "pk2", "value2-with-a-longer-payload", api::timestamp_type(203))));
    wb.seal(segment_sequence{211}, schema->id(), ondisk::block_alignment);

    auto serialized = make_serialized_buffer_copy(wb);
    auto rewritten = rewrite_streamed_segment(log_segment_id{33}, segment_sequence{221}, std::span(&serialized, 1));
    auto bh = read_buffer_header(rewritten.data);

    auto rewritten_size = rewritten.data.size();
    auto in = seastar::util::as_input_stream(rewritten.data.share());
    std::vector<segment_header> seen_segment_headers;
    std::vector<log_record> seen_records;

    scan_segment(in, log_segment_id{33}, rewritten_size,
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

    BOOST_REQUIRE_EQUAL(rewritten.write_count, 1u);
    BOOST_REQUIRE(ondisk::validate_header(bh));
    BOOST_REQUIRE_EQUAL(bh.segment_seq.value, 221u);
    BOOST_REQUIRE_EQUAL(seen_segment_headers.size(), 1u);
    BOOST_REQUIRE(seen_segment_headers.front().kind == segment_kind::full);
    BOOST_REQUIRE_EQUAL(seen_segment_headers.front().segment_seq.value, 221u);
    BOOST_REQUIRE(std::holds_alternative<segment_header::full>(seen_segment_headers.front().v));
    BOOST_REQUIRE_EQUAL(seen_records.size(), 3u);
    BOOST_REQUIRE_EQUAL(seen_records[0].header.timestamp, api::timestamp_type(201));
    BOOST_REQUIRE_EQUAL(seen_records[1].header.timestamp, api::timestamp_type(202));
    BOOST_REQUIRE_EQUAL(seen_records[2].header.timestamp, api::timestamp_type(203));
    BOOST_REQUIRE_EQUAL(seen_records[0].header.table, schema->id());
    BOOST_REQUIRE_EQUAL(seen_records[1].header.table, schema->id());
    BOOST_REQUIRE_EQUAL(seen_records[2].header.table, schema->id());
    assert_that(seen_records[0].mut.to_mutation(schema)).is_equal_to(expected0);
    assert_that(seen_records[1].mut.to_mutation(schema)).is_equal_to(expected1);
    assert_that(seen_records[2].mut.to_mutation(schema)).is_equal_to(expected2);
}

// Checks that the rewriter can wait for a fragmented initial header before rewriting it.
SEASTAR_THREAD_TEST_CASE(test_logstor_streamed_segment_rewriter_handles_fragmented_initial_header) {
    auto schema = make_kv_schema();

    raw_write_buffer wb(64 * 1024, segment_kind::mixed);
    wb.append(log_record_writer(make_log_record(schema, "pk0", "value0", api::timestamp_type(231))));
    wb.seal(segment_sequence{241}, std::nullopt, ondisk::block_alignment);

    auto serialized = make_serialized_buffer_copy(wb);
    auto split = ondisk::buffer_header_size - 1;
    std::vector<temporary_buffer<char>> chunks;
    chunks.push_back(slice_buffer(serialized, 0, split));
    chunks.push_back(slice_buffer(serialized, split, serialized.size() - split));

    auto rewritten = rewrite_streamed_segment(log_segment_id{35}, segment_sequence{251}, chunks);
    auto bh = read_buffer_header(rewritten.data);

    BOOST_REQUIRE_EQUAL(rewritten.write_count, 1u);
    BOOST_REQUIRE_EQUAL(bh.segment_seq.value, 251u);
}

// Checks that the rewriter rejects a stream whose initial buffer header is corrupted.
SEASTAR_THREAD_TEST_CASE(test_logstor_streamed_segment_rewriter_rejects_invalid_initial_header) {
    auto schema = make_kv_schema();

    raw_write_buffer wb(64 * 1024, segment_kind::mixed);
    wb.append(log_record_writer(make_log_record(schema, "pk0", "value0", api::timestamp_type(291))));
    wb.seal(segment_sequence{301}, std::nullopt, ondisk::block_alignment);

    auto serialized = make_serialized_buffer_copy(wb);
    flip_byte(serialized, 0);

    BOOST_REQUIRE_THROW(rewrite_streamed_segment(log_segment_id{39}, segment_sequence{311}, std::span(&serialized, 1)), std::runtime_error);
}

// Checks that the rewriter rejects streams that end before the initial header is complete.
SEASTAR_THREAD_TEST_CASE(test_logstor_streamed_segment_rewriter_rejects_truncated_initial_header) {
    auto schema = make_kv_schema();

    raw_write_buffer wb(64 * 1024, segment_kind::mixed);
    wb.append(log_record_writer(make_log_record(schema, "pk0", "value0", api::timestamp_type(321))));
    wb.seal(segment_sequence{331}, std::nullopt, ondisk::block_alignment);

    auto serialized = make_serialized_buffer_copy(wb);
    auto truncated = slice_buffer(serialized, 0, ondisk::buffer_header_size - 1);

    BOOST_REQUIRE_THROW(rewrite_streamed_segment(log_segment_id{41}, segment_sequence{341}, std::span(&truncated, 1)), std::runtime_error);
}

// Checks that buffered_writer forwards records to the flush callback and resolves returned locations.
SEASTAR_THREAD_TEST_CASE(test_logstor_buffered_writer_basic_flushes_records) {
    auto schema = make_kv_schema();
    constexpr size_t buffer_size = 4 * 1024;

    test_flush_controller flush_ctl;
    buffered_writer writer(make_buffered_writer_config(buffer_size, 3), [&flush_ctl] (write_buffer& wb) {
        return flush_ctl(wb);
    });

    writer.start().get();
    auto stop = defer([&writer] {
        writer.stop().get();
    });

    std::vector<log_record> expected;
    std::vector<future<log_location_with_holder>> persisted;
    for (size_t i = 0; i < 3; ++i) {
        auto record = make_buffered_writer_record(schema, i, sstring(fmt::format("value{}", i)), api::timestamp_type(100 + i));
        expected.push_back(record);
        auto accepted = writer.write_to_buffer(log_record_writer(record), test_timeout()).get();
        persisted.push_back(std::move(accepted.persisted));
    }

    std::vector<log_location> locations;
    for (auto& fut : persisted) {
        locations.push_back(wait_for_persisted(fut));
    }

    const auto actual = flush_ctl.all_records();
    assert_records_in_order(schema, actual, expected);

    BOOST_REQUIRE_EQUAL(locations.size(), expected.size());
    for (size_t i = 0; i < locations.size(); ++i) {
        auto read_back = read_record_at_location(flush_ctl.buffer_for_segment(locations[i].segment), locations[i]);
        assert_log_record_matches(schema, read_back, expected[i]);
    }
}

// Checks that a paused tail flush can fill the ring, queue later writes, and then drain them without losing order.
SEASTAR_THREAD_TEST_CASE(test_logstor_buffered_writer_paused_flush_fills_ring_then_drains) {
    auto schema = make_kv_schema();
    constexpr size_t buffer_size = 4 * 1024;
    const auto large_value = make_single_buffer_value(schema, buffer_size);

    test_flush_controller flush_ctl{.pause_flushes = true};
    buffered_writer writer(make_buffered_writer_config(buffer_size, 2), [&flush_ctl] (write_buffer& wb) {
        return flush_ctl(wb);
    });

    writer.start().get();
    auto stop = defer([&writer] {
        writer.stop().get();
    });

    std::vector<log_record> expected;
    for (size_t i = 0; i < 4; ++i) {
        expected.push_back(make_buffered_writer_record(schema, i, large_value, api::timestamp_type(200 + i)));
    }

    auto accepted0 = writer.write_to_buffer(log_record_writer(expected[0]), test_timeout()).get();
    auto persisted0 = std::move(accepted0.persisted);
    flush_ctl.wait_for_flush_starts(1);
    BOOST_REQUIRE(!persisted0.available());

    auto accepted1 = writer.write_to_buffer(log_record_writer(expected[1]), test_timeout()).get();
    auto persisted1 = std::move(accepted1.persisted);

    auto queued2 = writer.write_to_buffer(log_record_writer(expected[2]), test_timeout());
    auto queued3 = writer.write_to_buffer(log_record_writer(expected[3]), test_timeout());
    BOOST_REQUIRE(!queued2.available());
    BOOST_REQUIRE(!queued3.available());
    BOOST_REQUIRE_EQUAL(writer.queued_write_count(), 2u);

    flush_ctl.release_one_flush();
    wait_for_persisted(persisted0);
    flush_ctl.wait_for_flush_starts(2);

    auto accepted2 = queued2.get();
    auto persisted2 = std::move(accepted2.persisted);
    BOOST_REQUIRE(!queued3.available());
    BOOST_REQUIRE_EQUAL(writer.queued_write_count(), 1u);

    flush_ctl.release_one_flush();
    wait_for_persisted(persisted1);
    flush_ctl.wait_for_flush_starts(3);

    auto accepted3 = queued3.get();
    auto persisted3 = std::move(accepted3.persisted);
    BOOST_REQUIRE_EQUAL(writer.queued_write_count(), 0u);

    flush_ctl.release_one_flush();
    wait_for_persisted(persisted2);
    flush_ctl.wait_for_flush_starts(4);

    flush_ctl.release_one_flush();
    wait_for_persisted(persisted3);

    assert_records_in_order(schema, flush_ctl.all_records(), expected);
}

// Checks that queued writes are accepted and persisted in FIFO order once capacity becomes available.
SEASTAR_THREAD_TEST_CASE(test_logstor_buffered_writer_queued_writes_preserve_fifo_order) {
    auto schema = make_kv_schema();
    constexpr size_t buffer_size = 4 * 1024;
    const auto large_value = make_single_buffer_value(schema, buffer_size);

    test_flush_controller flush_ctl{.pause_flushes = true};
    buffered_writer writer(make_buffered_writer_config(buffer_size, 2), [&flush_ctl] (write_buffer& wb) {
        return flush_ctl(wb);
    });

    writer.start().get();
    auto stop = defer([&writer] {
        writer.stop().get();
    });

    std::vector<log_record> expected;
    for (size_t i = 0; i < 5; ++i) {
        expected.push_back(make_buffered_writer_record(schema, i, large_value, api::timestamp_type(300 + i)));
    }

    auto accepted0 = writer.write_to_buffer(log_record_writer(expected[0]), test_timeout()).get();
    auto persisted0 = std::move(accepted0.persisted);
    flush_ctl.wait_for_flush_starts(1);

    auto accepted1 = writer.write_to_buffer(log_record_writer(expected[1]), test_timeout()).get();
    auto persisted1 = std::move(accepted1.persisted);

    auto queued2 = writer.write_to_buffer(log_record_writer(expected[2]), test_timeout());
    auto queued3 = writer.write_to_buffer(log_record_writer(expected[3]), test_timeout());
    auto queued4 = writer.write_to_buffer(log_record_writer(expected[4]), test_timeout());

    BOOST_REQUIRE(!queued2.available());
    BOOST_REQUIRE(!queued3.available());
    BOOST_REQUIRE(!queued4.available());
    BOOST_REQUIRE_EQUAL(writer.queued_write_count(), 3u);

    std::vector<size_t> accepted_order;

    flush_ctl.release_one_flush();
    wait_for_persisted(persisted0);
    flush_ctl.wait_for_flush_starts(2);
    accepted_order.push_back(2);
    auto accepted2 = queued2.get();
    auto persisted2 = std::move(accepted2.persisted);
    BOOST_REQUIRE(!queued3.available());
    BOOST_REQUIRE(!queued4.available());

    flush_ctl.release_one_flush();
    wait_for_persisted(persisted1);
    flush_ctl.wait_for_flush_starts(3);
    accepted_order.push_back(3);
    auto accepted3 = queued3.get();
    auto persisted3 = std::move(accepted3.persisted);
    BOOST_REQUIRE(!queued4.available());

    flush_ctl.release_one_flush();
    wait_for_persisted(persisted2);
    flush_ctl.wait_for_flush_starts(4);
    accepted_order.push_back(4);
    auto accepted4 = queued4.get();
    auto persisted4 = std::move(accepted4.persisted);

    flush_ctl.release_one_flush();
    wait_for_persisted(persisted3);
    flush_ctl.wait_for_flush_starts(5);

    flush_ctl.release_one_flush();
    wait_for_persisted(persisted4);

    const std::vector<size_t> expected_acceptance_order{2, 3, 4};
    BOOST_REQUIRE_EQUAL_COLLECTIONS(accepted_order.begin(), accepted_order.end(), expected_acceptance_order.begin(), expected_acceptance_order.end());
    assert_records_in_order(schema, flush_ctl.all_records(), expected);
}

// Checks that queued writes are rejected once max_queued_write_bytes would be exceeded.
SEASTAR_THREAD_TEST_CASE(test_logstor_buffered_writer_rejects_when_max_queued_write_bytes_exceeded) {
    auto schema = make_kv_schema();
    constexpr size_t buffer_size = 4 * 1024;
    const auto large_value = make_single_buffer_value(schema, buffer_size);

    auto record0 = make_buffered_writer_record(schema, 0, large_value, api::timestamp_type(400));
    const auto queued_write_budget = log_record_writer(record0).size() * 2;

    test_flush_controller flush_ctl{.pause_flushes = true};
    buffered_writer writer(make_buffered_writer_config(buffer_size, 2, queued_write_budget), [&flush_ctl] (write_buffer& wb) {
        return flush_ctl(wb);
    });

    writer.start().get();
    auto stop = defer([&writer] {
        writer.stop().get();
    });

    std::vector<log_record> expected{record0};
    for (size_t i = 1; i < 4; ++i) {
        expected.push_back(make_buffered_writer_record(schema, i, large_value, api::timestamp_type(400 + i)));
    }

    auto accepted0 = writer.write_to_buffer(log_record_writer(expected[0]), test_timeout()).get();
    auto persisted0 = std::move(accepted0.persisted);
    flush_ctl.wait_for_flush_starts(1);

    auto accepted1 = writer.write_to_buffer(log_record_writer(expected[1]), test_timeout()).get();
    auto persisted1 = std::move(accepted1.persisted);

    auto queued2 = writer.write_to_buffer(log_record_writer(expected[2]), test_timeout());
    auto queued3 = writer.write_to_buffer(log_record_writer(expected[3]), test_timeout());
    BOOST_REQUIRE(!queued2.available());
    BOOST_REQUIRE(!queued3.available());
    BOOST_REQUIRE_EQUAL(writer.queued_write_count(), 2u);

    auto rejected = make_buffered_writer_record(schema, 4, large_value, api::timestamp_type(404));
    BOOST_REQUIRE_THROW(writer.write_to_buffer(log_record_writer(rejected), test_timeout()).get(), replica::rate_limit_exception);

    flush_ctl.release_one_flush();
    wait_for_persisted(persisted0);
    flush_ctl.wait_for_flush_starts(2);
    auto accepted2 = queued2.get();
    auto persisted2 = std::move(accepted2.persisted);

    flush_ctl.release_one_flush();
    wait_for_persisted(persisted1);
    flush_ctl.wait_for_flush_starts(3);
    auto accepted3 = queued3.get();
    auto persisted3 = std::move(accepted3.persisted);

    flush_ctl.release_one_flush();
    wait_for_persisted(persisted2);
    flush_ctl.wait_for_flush_starts(4);

    flush_ctl.release_one_flush();
    wait_for_persisted(persisted3);

    assert_records_in_order(schema, flush_ctl.all_records(), expected);
}

// Checks that write_to_buffer() stays pending while a request is queued and only persistence remains pending once it is accepted.
SEASTAR_THREAD_TEST_CASE(test_logstor_buffered_writer_acceptance_stays_blocked_while_write_is_queued) {
    auto schema = make_kv_schema();
    constexpr size_t buffer_size = 4 * 1024;
    const auto large_value = make_single_buffer_value(schema, buffer_size);

    test_flush_controller flush_ctl{.pause_flushes = true};
    buffered_writer writer(make_buffered_writer_config(buffer_size, 2), [&flush_ctl] (write_buffer& wb) {
        return flush_ctl(wb);
    });

    writer.start().get();
    auto stop = defer([&writer] {
        writer.stop().get();
    });

    auto record0 = make_buffered_writer_record(schema, 0, large_value, api::timestamp_type(500));
    auto record1 = make_buffered_writer_record(schema, 1, large_value, api::timestamp_type(501));
    auto record2 = make_buffered_writer_record(schema, 2, large_value, api::timestamp_type(502));

    auto accepted0 = writer.write_to_buffer(log_record_writer(record0), test_timeout()).get();
    auto persisted0 = std::move(accepted0.persisted);
    flush_ctl.wait_for_flush_starts(1);

    auto accepted1 = writer.write_to_buffer(log_record_writer(record1), test_timeout()).get();
    auto persisted1 = std::move(accepted1.persisted);

    auto queued2 = writer.write_to_buffer(log_record_writer(record2), test_timeout());
    BOOST_REQUIRE(!queued2.available());
    BOOST_REQUIRE_EQUAL(writer.queued_write_count(), 1u);

    flush_ctl.release_one_flush();
    wait_for_persisted(persisted0);
    flush_ctl.wait_for_flush_starts(2);

    auto accepted2 = queued2.get();
    BOOST_REQUIRE(!accepted2.persisted.available());

    flush_ctl.release_one_flush();
    wait_for_persisted(persisted1);
    flush_ctl.wait_for_flush_starts(3);
    BOOST_REQUIRE(!accepted2.persisted.available());

    flush_ctl.release_one_flush();
    wait_for_persisted(accepted2.persisted);

    const auto actual = flush_ctl.all_records();
    const std::vector<log_record> expected{record0, record1, record2};
    assert_records_in_order(schema, actual, expected);
}

// Checks that stop() waits for a blocked in-flight flush instead of dropping it.
SEASTAR_THREAD_TEST_CASE(test_logstor_buffered_writer_stop_drains_blocked_in_flight_write) {
    auto schema = make_kv_schema();
    constexpr size_t buffer_size = 4 * 1024;
    const auto record = make_buffered_writer_record(schema, 0, "value", api::timestamp_type(600));

    test_flush_controller flush_ctl{.pause_flushes = true};
    buffered_writer writer(make_buffered_writer_config(buffer_size, 2), [&flush_ctl] (write_buffer& wb) {
        return flush_ctl(wb);
    });

    writer.start().get();

    auto accepted = writer.write_to_buffer(log_record_writer(record), test_timeout()).get();
    auto persisted = std::move(accepted.persisted);
    flush_ctl.wait_for_flush_starts(1);

    auto stop_fut = writer.stop();
    BOOST_REQUIRE(!stop_fut.available());

    flush_ctl.release_one_flush();
    wait_for_persisted(persisted);
    stop_fut.get();

    const auto actual = flush_ctl.all_records();
    BOOST_REQUIRE_EQUAL(actual.size(), 1u);
    assert_log_record_matches(schema, actual.front().record, record);
}

// Checks that a flush failure is propagated to every write that was already accepted into that buffer.
SEASTAR_THREAD_TEST_CASE(test_logstor_buffered_writer_flush_failure_fails_all_writes_in_buffer) {
    auto schema = make_kv_schema();
    constexpr size_t buffer_size = 4 * 1024;

    auto record0 = make_buffered_writer_record(schema, 0, "value0", api::timestamp_type(700));
    auto record1 = make_buffered_writer_record(schema, 1, "value1", api::timestamp_type(701));
    raw_write_buffer scratch(buffer_size, segment_kind::mixed);
    BOOST_REQUIRE(scratch.can_fit(log_record_writer(record0)));
    scratch.append(log_record_writer(record0));
    BOOST_REQUIRE(scratch.can_fit(log_record_writer(record1)));

    test_flush_controller flush_ctl{.fail_flush_index = 0};
    buffered_writer writer(make_buffered_writer_config(buffer_size, 3), [&flush_ctl] (write_buffer& wb) {
        return flush_ctl(wb);
    });

    writer.start().get();
    auto stop = defer([&writer] {
        writer.stop().get();
    });

    auto accepted0 = writer.write_to_buffer(log_record_writer(record0), test_timeout()).get();
    auto accepted1 = writer.write_to_buffer(log_record_writer(record1), test_timeout()).get();

    BOOST_REQUIRE_THROW(accepted0.persisted.get(), std::runtime_error);
    BOOST_REQUIRE_THROW(accepted1.persisted.get(), std::runtime_error);
    BOOST_REQUIRE(flush_ctl.flushed_buffers.empty());
}
