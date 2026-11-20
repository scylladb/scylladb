/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// combined_tests has the main function.
#undef SEASTAR_TESTING_MAIN

#include <boost/test/unit_test.hpp>
#include <algorithm>
#include <chrono>
#include <optional>
#include <stdexcept>
#include <vector>
#include <fmt/format.h>
#include <seastar/core/abort_source.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/simple-stream.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/thread.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/memory-data-source.hh>

#include "replica/logstor/ondisk.hh"
#include "replica/logstor/segment_io.hh"
#include "replica/logstor/write_buffer.hh"
#include "schema/schema_builder.hh"
#include "test/lib/logstor_record_utils.hh"
#include "test/lib/mutation_assertions.hh"

using namespace replica::logstor;
using namespace tests::logstor;

namespace {

void assert_log_record_matches(schema_ptr schema, const log_record& actual, const log_record& expected) {
    BOOST_REQUIRE_EQUAL(actual.header.timestamp, expected.header.timestamp);
    BOOST_REQUIRE_EQUAL(actual.header.table, expected.header.table);
    assert_that(to_mutation(actual, schema)).is_equal_to(to_mutation(expected, schema));
}

db::timeout_clock::time_point test_timeout() {
    return db::timeout_clock::now() + std::chrono::minutes(1);
}

write_buffer_ring_config make_write_buffer_ring_config(size_t buffer_size, size_t ring_size, std::chrono::milliseconds sync_period = std::chrono::milliseconds(0)) {
    return write_buffer_ring_config{
        .buffer_size = buffer_size,
        .ring_size = ring_size,
        .flush_sg = seastar::default_scheduling_group(),
        .sync_period = sync_period,
    };
}

sstring make_single_buffer_value(schema_ptr schema, size_t buffer_size) {
    sstring value;
    while (true) {
        auto next_value = value;
        next_value += sstring("x");
        auto next_record = make_log_record(schema, "pk0000", next_value, api::timestamp_type(17));
        auto next_writer = log_record_writer(next_record);
        chunk_builder wb(buffer_size, segment_kind::mixed);

        BOOST_REQUIRE(wb.can_fit(next_writer));
        wb.append(next_writer);
        if (!wb.can_fit(next_writer)) {
            return next_value;
        }

        value = std::move(next_value);
    }
}

log_record make_write_buffer_ring_record(schema_ptr schema, size_t idx, const sstring& value, api::timestamp_type ts) {
    return make_log_record(schema, sstring(fmt::format("pk{:04}", idx)), value, ts);
}

record_location wait_for_persisted(future<record_location_with_holder>& fut) {
    auto [loc, op] = fut.get();
    return loc;
}

struct test_flush_controller {
    struct flushed_buffer {
        temporary_buffer<char> data;
        segment_position base_position;
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
            co_await flush_release.wait(1);
        }

        if (fail_flush_index && *fail_flush_index == flush_idx) {
            throw std::runtime_error("injected flush failure");
        }

        auto base_position = segment_position{
            .segment = log_segment_id{next_segment_id++},
            .offset = 0,
        };
        flushed_buffers.push_back(flushed_buffer{
            .data = copy_chunk(wb.seal(segment_sequence{next_sequence++}, std::nullopt)),
            .base_position = base_position,
            .record_count = wb.record_count(),
        });
        co_await wb.complete_writes(base_position);
    }

    void wait_for_flush_starts(size_t target_count) {
        while (started_count < target_count) {
            flush_started.wait().get();
        }
    }

    void release_one_flush() {
        flush_release.signal(1);
    }

    void release_all_flushes() {
        pause_flushes = false;
        flush_release.signal(started_count);
    }

    const temporary_buffer<char>& buffer_for_segment(log_segment_id segment_id) const {
        for (const auto& flushed : flushed_buffers) {
            if (flushed.base_position.segment == segment_id) {
                return flushed.data;
            }
        }
        throw std::runtime_error(fmt::format("Missing flushed segment {}", segment_id));
    }

    std::vector<scanned_record> all_records() const {
        std::vector<scanned_record> records;
        for (const auto& flushed : flushed_buffers) {
            auto buffer_records = scan_buffer_records(flushed.data, flushed.base_position.segment);
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

write_buffer_pool::config make_test_write_buffer_pool_config(size_t capacity, size_t max_cached, size_t preallocate = 0) {
    return write_buffer_pool::config{
        .capacity = capacity,
        .buffer_size = 4 * 1024,
        .kind = segment_kind::full,
        .preallocate = preallocate,
        .max_cached = max_cached,
    };
}

// A pooled buffer has to be closed before it can go back to the pool, which happens when the
// handle is destroyed - here, when this returns.
void close_and_return(owned_write_buffer buf) {
    buf->close().get();
}

} // anonymous namespace

BOOST_AUTO_TEST_SUITE(logstor_write_buffer_test)

// Checks that sealing a full chunk builder writes the expected header fields.
SEASTAR_THREAD_TEST_CASE(test_logstor_chunk_builder_record_and_header_serialization) {
    auto schema = make_kv_schema();
    auto expected = make_log_record(schema, "pk0", "v0", api::timestamp_type(7));

    chunk_builder wb(32 * 1024, segment_kind::full);
    auto writer = log_record_writer(expected);
    auto expected_records_size = size_t(ondisk::record_frame_header_size) + writer.record_size();
    expected_records_size = ((expected_records_size + ondisk::record_alignment - 1) / ondisk::record_alignment) * ondisk::record_alignment;
    wb.append(std::move(writer));

    const auto chunk = wb.seal(segment_sequence{17}, schema->id());
    BOOST_REQUIRE_EQUAL(chunk.size() % ondisk::block_alignment, 0u);

    seastar::simple_memory_input_stream in(reinterpret_cast<const char*>(chunk.data()), chunk.size());
    auto common = ser::deserialize(in, std::type_identity<ondisk::chunk_header_common>{});
    auto ch = ondisk::parse_chunk_header(common, std::span(reinterpret_cast<const char*>(chunk.data()) + ondisk::chunk_header_common_size,
            ondisk::chunk_header_full_part_size));
    BOOST_REQUIRE(ch);
    BOOST_REQUIRE(ch->kind() == segment_kind::full);
    BOOST_REQUIRE_EQUAL(ch->segment.segment_seq.value, 17u);
    BOOST_REQUIRE_EQUAL(ch->records_size, expected_records_size);

    const auto& full = std::get<segment_info::full>(ch->segment.v);
    BOOST_REQUIRE_EQUAL(full.table, schema->id());
    BOOST_REQUIRE_EQUAL(full.first_token, expected.header.key.token());
    BOOST_REQUIRE_EQUAL(full.last_token, expected.header.key.token());
}

// Checks that a chunk builder can hold and seal a record whose serialized size is exactly max_record_size().
SEASTAR_THREAD_TEST_CASE(test_logstor_chunk_builder_accepts_record_at_max_record_size) {
    auto schema = make_kv_schema();

    chunk_builder wb(ondisk::block_alignment, segment_kind::mixed);
    auto max_size = wb.max_record_size();

    sstring value;
    auto record = make_log_record(schema, "pk", "", api::timestamp_type(27));
    log_record_writer writer(record);

    while (writer.record_size() < max_size) {
        value += "x";
        record = make_log_record(schema, "pk", value, api::timestamp_type(27));
        writer = log_record_writer(record);
    }

    BOOST_REQUIRE_EQUAL(writer.record_size(), max_size);
    BOOST_REQUIRE(wb.can_fit(writer));

    wb.append(writer);

    BOOST_REQUIRE_EQUAL(wb.seal(segment_sequence{29}, std::nullopt).size(), ondisk::block_alignment);
}

// Checks that the size finish() returns is the size of the chunk seal() returns, and that the
// chunk carries the sequence number seal() got.
SEASTAR_THREAD_TEST_CASE(test_logstor_chunk_builder_finish_returns_the_final_chunk_size) {
    auto schema = make_kv_schema();

    chunk_builder wb(4 * ondisk::block_alignment, segment_kind::mixed);
    wb.append(log_record_writer(make_log_record(schema, "pk0", "value0", api::timestamp_type(1))));
    const auto size = wb.finish();
    BOOST_REQUIRE_EQUAL(size % ondisk::block_alignment, 0u);
    BOOST_REQUIRE_EQUAL(wb.finish(), size);

    auto chunk = copy_chunk(wb.seal(segment_sequence{3}, std::nullopt));
    BOOST_REQUIRE_EQUAL(chunk.size(), size);
    BOOST_REQUIRE_EQUAL(read_chunk_header(chunk).value().segment.segment_seq.value, 3u);
    BOOST_REQUIRE_THROW(wb.seal(segment_sequence{3}, std::nullopt), std::runtime_error);
}

// Checks that a buffer holding records that were never flushed can still be closed and reused
// after aborting its writes. A pending write is resolved only by the flush and holds the buffer's
// write gate until then, so close() alone never completes. The compaction buffer pool relies on
// abort_writes() to reclaim a buffer left behind by a compaction that failed before flushing.
SEASTAR_THREAD_TEST_CASE(test_logstor_write_buffer_abort_writes_reclaims_unflushed_buffer) {
    auto schema = make_kv_schema();

    write_buffer wb(32 * 1024, segment_kind::full);
    auto written = wb.write(log_record_writer(make_log_record(schema, "pk0", "v0", api::timestamp_type(1))));
    BOOST_REQUIRE(!written.available());

    auto closed = wb.close();
    seastar::thread::yield();
    BOOST_REQUIRE(!closed.available());

    wb.abort_writes(std::make_exception_ptr(std::runtime_error("buffer was not flushed"))).get();
    BOOST_REQUIRE_THROW(written.get(), std::runtime_error);
    closed.get();
    BOOST_REQUIRE(wb.is_closed());

    wb.reset();
    BOOST_REQUIRE(!wb.is_closed());
    BOOST_REQUIRE(!wb.has_data());
}

// Checks that a pool builds its buffers at the point of use and keeps only max_cached of them once
// they are returned, so that a pool whose capacity is rarely reached does not hold the memory for it.
SEASTAR_THREAD_TEST_CASE(test_logstor_write_buffer_pool_builds_buffers_on_demand) {
    abort_source as;
    write_buffer_pool pool(make_test_write_buffer_pool_config(4, 1));
    auto stop_pool = seastar::defer([&pool] noexcept { pool.stop().get(); });

    BOOST_REQUIRE_EQUAL(pool.allocated_buffer_count(), 0u);

    auto b0 = pool.allocate(as).get();
    auto b1 = pool.allocate(as).get();
    BOOST_REQUIRE_EQUAL(pool.used_buffer_count(), 2u);
    BOOST_REQUIRE_EQUAL(pool.allocated_buffer_count(), 2u);
    BOOST_REQUIRE_EQUAL(pool.get_stats().buffers_created, 2u);
    BOOST_REQUIRE_EQUAL(b0->buffer_size(), 4u * 1024);

    close_and_return(std::move(b0));
    close_and_return(std::move(b1));
    BOOST_REQUIRE_EQUAL(pool.used_buffer_count(), 0u);
    BOOST_REQUIRE_EQUAL(pool.allocated_buffer_count(), 1u);

    // The kept buffer is handed out again, the one that was let go is built anew.
    auto b2 = pool.allocate(as).get();
    BOOST_REQUIRE_EQUAL(pool.get_stats().buffers_created, 2u);
    auto b3 = pool.allocate(as).get();
    BOOST_REQUIRE_EQUAL(pool.get_stats().buffers_created, 3u);

    close_and_return(std::move(b2));
    close_and_return(std::move(b3));
}

// Checks that a pool configured to build its buffers up front never builds one at the point of use,
// which is what the compaction buffer pool relies on.
SEASTAR_THREAD_TEST_CASE(test_logstor_write_buffer_pool_preallocates_its_buffers) {
    abort_source as;
    write_buffer_pool pool(make_test_write_buffer_pool_config(2, 2, 2));
    auto stop_pool = seastar::defer([&pool] noexcept { pool.stop().get(); });

    BOOST_REQUIRE_EQUAL(pool.allocated_buffer_count(), 2u);
    BOOST_REQUIRE_EQUAL(pool.get_stats().buffers_created, 2u);

    auto bufs = pool.allocate_many(2, as).get();
    BOOST_REQUIRE_EQUAL(bufs.size(), 2u);
    BOOST_REQUIRE(bufs[0].get() != bufs[1].get());
    BOOST_REQUIRE_EQUAL(pool.used_buffer_count(), 2u);
    BOOST_REQUIRE_EQUAL(pool.get_stats().buffers_created, 2u);

    for (auto& buf : bufs) {
        close_and_return(std::move(buf));
    }
    BOOST_REQUIRE_EQUAL(pool.used_buffer_count(), 0u);
    BOOST_REQUIRE_EQUAL(pool.allocated_buffer_count(), 2u);
}

// Checks that the capacity bounds how many buffers are out at once, and that raising it lets a
// waiting allocation through.
SEASTAR_THREAD_TEST_CASE(test_logstor_write_buffer_pool_capacity_bounds_allocations) {
    abort_source as;
    write_buffer_pool pool(make_test_write_buffer_pool_config(1, 1));
    auto stop_pool = seastar::defer([&pool] noexcept { pool.stop().get(); });

    auto b0 = pool.allocate(as).get();
    BOOST_REQUIRE_EQUAL(pool.used_buffer_count(), 1u);

    auto waiting = pool.allocate(as);
    seastar::thread::yield();
    BOOST_REQUIRE(!waiting.available());
    BOOST_REQUIRE_EQUAL(pool.get_stats().allocation_waits, 1u);

    pool.set_capacity(2);
    auto b1 = waiting.get();
    BOOST_REQUIRE_EQUAL(pool.used_buffer_count(), 2u);

    close_and_return(std::move(b0));
    close_and_return(std::move(b1));
}

// Checks that lowering the capacity while buffers are out takes effect as they come back: the first
// return covers the capacity that was taken away and its buffer is freed rather than kept.
SEASTAR_THREAD_TEST_CASE(test_logstor_write_buffer_pool_capacity_shrinks_while_buffers_are_out) {
    abort_source as;
    write_buffer_pool pool(make_test_write_buffer_pool_config(2, 2));
    auto stop_pool = seastar::defer([&pool] noexcept { pool.stop().get(); });

    auto b0 = pool.allocate(as).get();
    auto b1 = pool.allocate(as).get();

    pool.set_capacity(1);
    BOOST_REQUIRE_EQUAL(pool.capacity(), 1u);

    close_and_return(std::move(b0));
    BOOST_REQUIRE_EQUAL(pool.allocated_buffer_count(), 1u);

    auto waiting = pool.allocate(as);
    seastar::thread::yield();
    BOOST_REQUIRE(!waiting.available());

    close_and_return(std::move(b1));
    auto b2 = waiting.get();
    BOOST_REQUIRE_EQUAL(pool.used_buffer_count(), 1u);
    BOOST_REQUIRE_EQUAL(pool.allocated_buffer_count(), 1u);

    close_and_return(std::move(b2));
}

// Checks that write_buffer_ring forwards records to the flush callback and resolves returned locations.
SEASTAR_THREAD_TEST_CASE(test_logstor_write_buffer_ring_basic_flushes_records) {
    auto schema = make_kv_schema();
    constexpr size_t buffer_size = 4 * 1024;

    test_flush_controller flush_ctl;
    write_buffer_ring ring(make_write_buffer_ring_config(buffer_size, 3), [&flush_ctl] (write_buffer& wb) {
        return flush_ctl(wb);
    });

    ring.start().get();
    auto stop = defer([&ring] noexcept {
        ring.stop().get();
    });

    std::vector<log_record> expected;
    std::vector<future<record_location_with_holder>> persisted;
    for (size_t i = 0; i < 3; ++i) {
        auto record = make_write_buffer_ring_record(schema, i, sstring(fmt::format("value{}", i)), api::timestamp_type(100 + i));
        expected.push_back(record);
        auto accepted = ring.write_to_buffer(log_record_writer(record), test_timeout()).get();
        persisted.push_back(std::move(accepted.persisted));
    }

    std::vector<record_location> locations;
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

// Checks that flush() seals and writes a partially filled head buffer even when the sync timer has not expired.
SEASTAR_THREAD_TEST_CASE(test_logstor_write_buffer_ring_flushes_partial_buffer_with_large_sync_period) {
    auto schema = make_kv_schema();
    constexpr size_t buffer_size = 4 * 1024;
    constexpr auto sync_period = std::chrono::hours(1);

    test_flush_controller flush_ctl;
    write_buffer_ring ring(make_write_buffer_ring_config(buffer_size, 3, sync_period), [&flush_ctl] (write_buffer& wb) {
        return flush_ctl(wb);
    });

    ring.start().get();
    auto stop = defer([&ring] noexcept {
        ring.stop().get();
    });

    std::vector<log_record> expected;
    std::vector<future<record_location_with_holder>> persisted;
    for (size_t i = 0; i < 3; ++i) {
        auto record = make_write_buffer_ring_record(schema, i, sstring(fmt::format("value{}", i)), api::timestamp_type(150 + i));
        expected.push_back(record);
        auto accepted = ring.write_to_buffer(log_record_writer(record), test_timeout()).get();
        persisted.push_back(std::move(accepted.persisted));
    }

    BOOST_REQUIRE(flush_ctl.flushed_buffers.empty());

    auto flush = ring.flush();
    flush_ctl.wait_for_flush_starts(1);

    std::vector<record_location> locations;
    for (auto& fut : persisted) {
        locations.push_back(wait_for_persisted(fut));
    }

    flush.get();

    BOOST_REQUIRE_EQUAL(flush_ctl.flushed_buffers.size(), 1u);
    BOOST_REQUIRE_EQUAL(flush_ctl.flushed_buffers.front().record_count, expected.size());

    const auto actual = flush_ctl.all_records();
    assert_records_in_order(schema, actual, expected);

    BOOST_REQUIRE_EQUAL(locations.size(), expected.size());
    for (size_t i = 0; i < locations.size(); ++i) {
        auto read_back = read_record_at_location(flush_ctl.buffer_for_segment(locations[i].segment), locations[i]);
        assert_log_record_matches(schema, read_back, expected[i]);
    }
}

// Checks that a paused tail flush can fill the ring, queue later writes, and then drain them without losing order.
SEASTAR_THREAD_TEST_CASE(test_logstor_write_buffer_ring_paused_flush_fills_ring_then_drains) {
    auto schema = make_kv_schema();
    constexpr size_t buffer_size = 4 * 1024;
    const auto large_value = make_single_buffer_value(schema, buffer_size);

    test_flush_controller flush_ctl{.pause_flushes = true};
    write_buffer_ring ring(make_write_buffer_ring_config(buffer_size, 2), [&flush_ctl] (write_buffer& wb) {
        return flush_ctl(wb);
    });

    ring.start().get();
    auto stop = defer([&ring] noexcept {
        ring.stop().get();
    });

    std::vector<log_record> expected;
    for (size_t i = 0; i < 4; ++i) {
        expected.push_back(make_write_buffer_ring_record(schema, i, large_value, api::timestamp_type(200 + i)));
    }

    auto accepted0 = ring.write_to_buffer(log_record_writer(expected[0]), test_timeout()).get();
    auto persisted0 = std::move(accepted0.persisted);
    flush_ctl.wait_for_flush_starts(1);
    BOOST_REQUIRE(!persisted0.available());

    auto accepted1 = ring.write_to_buffer(log_record_writer(expected[1]), test_timeout()).get();
    auto persisted1 = std::move(accepted1.persisted);

    auto queued2 = ring.write_to_buffer(log_record_writer(expected[2]), test_timeout());
    auto queued3 = ring.write_to_buffer(log_record_writer(expected[3]), test_timeout());
    BOOST_REQUIRE(!queued2.available());
    BOOST_REQUIRE(!queued3.available());
    BOOST_REQUIRE_EQUAL(ring.queued_write_count(), 2u);

    flush_ctl.release_one_flush();
    wait_for_persisted(persisted0);
    flush_ctl.wait_for_flush_starts(2);

    auto accepted2 = queued2.get();
    auto persisted2 = std::move(accepted2.persisted);
    BOOST_REQUIRE(!queued3.available());
    BOOST_REQUIRE_EQUAL(ring.queued_write_count(), 1u);

    flush_ctl.release_one_flush();
    wait_for_persisted(persisted1);
    flush_ctl.wait_for_flush_starts(3);

    auto accepted3 = queued3.get();
    auto persisted3 = std::move(accepted3.persisted);
    BOOST_REQUIRE_EQUAL(ring.queued_write_count(), 0u);

    flush_ctl.release_one_flush();
    wait_for_persisted(persisted2);
    flush_ctl.wait_for_flush_starts(4);

    flush_ctl.release_one_flush();
    wait_for_persisted(persisted3);

    assert_records_in_order(schema, flush_ctl.all_records(), expected);
}

// A record that finds no room in the ring is queued, and it has to keep the write target it came
// with: the holders of that target are what keep its compaction group from being reported empty to
// a tablet split and from finishing its stop() while the write is still to come.
SEASTAR_THREAD_TEST_CASE(test_logstor_write_buffer_ring_queued_write_keeps_its_write_target) {
    auto schema = make_kv_schema();
    constexpr size_t buffer_size = 4 * 1024;
    const auto large_value = make_single_buffer_value(schema, buffer_size);

    // Outlive the ring, so that the holders of the queued write are released before the gates
    // they were taken from are destroyed.
    seastar::gate non_empty_gate;
    seastar::gate alive_gate;

    test_flush_controller flush_ctl{.pause_flushes = true};
    write_buffer_ring ring(make_write_buffer_ring_config(buffer_size, 2), [&flush_ctl] (write_buffer& wb) {
        return flush_ctl(wb);
    });

    ring.start().get();
    auto stop = defer([&ring] noexcept {
        ring.stop().get();
    });

    std::vector<log_record> expected;
    for (size_t i = 0; i < 3; ++i) {
        expected.push_back(make_write_buffer_ring_record(schema, i, large_value, api::timestamp_type(300 + i)));
    }

    // Fill the ring: the first record is being flushed and the second holds the only other buffer.
    auto accepted0 = ring.write_to_buffer(log_record_writer(expected[0]), test_timeout()).get();
    auto persisted0 = std::move(accepted0.persisted);
    flush_ctl.wait_for_flush_starts(1);
    auto accepted1 = ring.write_to_buffer(log_record_writer(expected[1]), test_timeout()).get();
    auto persisted1 = std::move(accepted1.persisted);

    // The third record has nowhere to go, so it is queued rather than appended.
    auto queued = ring.write_to_buffer(log_record_writer(expected[2]), test_timeout(), write_target{
            .cg = nullptr,
            .non_empty_holder = non_empty_gate.hold(),
            .alive_holder = alive_gate.hold(),
    });
    BOOST_REQUIRE(!queued.available());
    BOOST_REQUIRE_EQUAL(ring.queued_write_count(), 1u);

    // The write is in flight, so its group is still held by both of the target's holders. Checked
    // rather than required, so that a failure here still drains the ring below: a queued write
    // left behind would hang the stop() of this test rather than fail it.
    BOOST_CHECK_EQUAL(non_empty_gate.get_count(), 1u);
    BOOST_CHECK_EQUAL(alive_gate.get_count(), 1u);

    // Let the queued record through and drain everything, which releases the holders.
    flush_ctl.release_one_flush();
    wait_for_persisted(persisted0);
    flush_ctl.wait_for_flush_starts(2);

    auto accepted2 = queued.get();
    auto persisted2 = std::move(accepted2.persisted);
    BOOST_REQUIRE_EQUAL(ring.queued_write_count(), 0u);

    flush_ctl.release_one_flush();
    wait_for_persisted(persisted1);
    flush_ctl.wait_for_flush_starts(3);

    flush_ctl.release_one_flush();
    wait_for_persisted(persisted2);

    assert_records_in_order(schema, flush_ctl.all_records(), expected);
}

// Checks that queued writes are accepted and persisted in FIFO order once capacity becomes available.
SEASTAR_THREAD_TEST_CASE(test_logstor_write_buffer_ring_queued_writes_preserve_fifo_order) {
    auto schema = make_kv_schema();
    constexpr size_t buffer_size = 4 * 1024;
    const auto large_value = make_single_buffer_value(schema, buffer_size);

    test_flush_controller flush_ctl{.pause_flushes = true};
    write_buffer_ring ring(make_write_buffer_ring_config(buffer_size, 2), [&flush_ctl] (write_buffer& wb) {
        return flush_ctl(wb);
    });

    ring.start().get();
    auto stop = defer([&ring] noexcept {
        ring.stop().get();
    });

    std::vector<log_record> expected;
    for (size_t i = 0; i < 5; ++i) {
        expected.push_back(make_write_buffer_ring_record(schema, i, large_value, api::timestamp_type(300 + i)));
    }

    auto accepted0 = ring.write_to_buffer(log_record_writer(expected[0]), test_timeout()).get();
    auto persisted0 = std::move(accepted0.persisted);
    flush_ctl.wait_for_flush_starts(1);

    auto accepted1 = ring.write_to_buffer(log_record_writer(expected[1]), test_timeout()).get();
    auto persisted1 = std::move(accepted1.persisted);

    auto queued2 = ring.write_to_buffer(log_record_writer(expected[2]), test_timeout());
    auto queued3 = ring.write_to_buffer(log_record_writer(expected[3]), test_timeout());
    auto queued4 = ring.write_to_buffer(log_record_writer(expected[4]), test_timeout());

    BOOST_REQUIRE(!queued2.available());
    BOOST_REQUIRE(!queued3.available());
    BOOST_REQUIRE(!queued4.available());
    BOOST_REQUIRE_EQUAL(ring.queued_write_count(), 3u);
    const auto record_size = log_record_writer(expected[2]).record_size();
    BOOST_REQUIRE_EQUAL(ring.queued_write_bytes(), 3 * record_size);
    BOOST_REQUIRE_EQUAL(ring.total_queued_writes(), 3u);

    // FIFO acceptance is proven by the !available() checks after each drain step below:
    // each step frees exactly one buffer, and only the oldest queued write may be
    // accepted into it while every later one stays pending.

    flush_ctl.release_one_flush();
    wait_for_persisted(persisted0);
    flush_ctl.wait_for_flush_starts(2);
    auto accepted2 = queued2.get();
    auto persisted2 = std::move(accepted2.persisted);
    BOOST_REQUIRE(!queued3.available());
    BOOST_REQUIRE(!queued4.available());

    flush_ctl.release_one_flush();
    wait_for_persisted(persisted1);
    flush_ctl.wait_for_flush_starts(3);
    auto accepted3 = queued3.get();
    auto persisted3 = std::move(accepted3.persisted);
    BOOST_REQUIRE(!queued4.available());

    flush_ctl.release_one_flush();
    wait_for_persisted(persisted2);
    flush_ctl.wait_for_flush_starts(4);
    auto accepted4 = queued4.get();
    auto persisted4 = std::move(accepted4.persisted);

    flush_ctl.release_one_flush();
    wait_for_persisted(persisted3);
    flush_ctl.wait_for_flush_starts(5);

    flush_ctl.release_one_flush();
    wait_for_persisted(persisted4);

    BOOST_REQUIRE_EQUAL(ring.queued_write_bytes(), 0u);
    BOOST_REQUIRE_EQUAL(ring.total_queued_writes(), 3u);
    assert_records_in_order(schema, flush_ctl.all_records(), expected);
}

// Checks that a queued write that times out fails with timed_out_error, leaves the queue, and leaves
// no failed future that nothing waits for.
SEASTAR_THREAD_TEST_CASE(test_logstor_write_buffer_ring_queued_write_times_out) {
    auto schema = make_kv_schema();
    constexpr size_t buffer_size = 4 * 1024;
    const auto large_value = make_single_buffer_value(schema, buffer_size);

    test_flush_controller flush_ctl{.pause_flushes = true};
    write_buffer_ring ring(make_write_buffer_ring_config(buffer_size, 2), [&flush_ctl] (write_buffer& wb) {
        return flush_ctl(wb);
    });

    ring.start().get();
    auto stop = defer([&ring, &flush_ctl] noexcept {
        flush_ctl.release_all_flushes();
        ring.stop().get();
    });

    auto accepted0 = ring.write_to_buffer(log_record_writer(make_write_buffer_ring_record(schema, 0, large_value, api::timestamp_type(500))), test_timeout()).get();
    auto persisted0 = std::move(accepted0.persisted);
    flush_ctl.wait_for_flush_starts(1);

    auto accepted1 = ring.write_to_buffer(log_record_writer(make_write_buffer_ring_record(schema, 1, large_value, api::timestamp_type(501))), test_timeout()).get();
    auto persisted1 = std::move(accepted1.persisted);

    const auto abandoned_before = seastar::engine().abandoned_failed_futures();
    auto queued = ring.write_to_buffer(log_record_writer(make_write_buffer_ring_record(schema, 2, large_value, api::timestamp_type(502))),
            db::timeout_clock::now() + std::chrono::milliseconds(10));
    BOOST_REQUIRE_EQUAL(ring.queued_write_count(), 1u);
    BOOST_REQUIRE_THROW(queued.get(), seastar::timed_out_error);
    BOOST_REQUIRE_EQUAL(ring.queued_write_count(), 0u);
    BOOST_REQUIRE_EQUAL(ring.queued_write_bytes(), 0u);
    BOOST_REQUIRE_EQUAL(ring.total_queued_writes(), 1u);
    BOOST_REQUIRE_EQUAL(seastar::engine().abandoned_failed_futures(), abandoned_before);

    flush_ctl.release_all_flushes();
    wait_for_persisted(persisted0);
    wait_for_persisted(persisted1);
}

// Checks that write_to_buffer() stays pending while a request is queued and only persistence remains pending once it is accepted.
SEASTAR_THREAD_TEST_CASE(test_logstor_write_buffer_ring_acceptance_stays_blocked_while_write_is_queued) {
    auto schema = make_kv_schema();
    constexpr size_t buffer_size = 4 * 1024;
    const auto large_value = make_single_buffer_value(schema, buffer_size);

    test_flush_controller flush_ctl{.pause_flushes = true};
    write_buffer_ring ring(make_write_buffer_ring_config(buffer_size, 2), [&flush_ctl] (write_buffer& wb) {
        return flush_ctl(wb);
    });

    ring.start().get();
    auto stop = defer([&ring] noexcept {
        ring.stop().get();
    });

    auto record0 = make_write_buffer_ring_record(schema, 0, large_value, api::timestamp_type(500));
    auto record1 = make_write_buffer_ring_record(schema, 1, large_value, api::timestamp_type(501));
    auto record2 = make_write_buffer_ring_record(schema, 2, large_value, api::timestamp_type(502));

    auto accepted0 = ring.write_to_buffer(log_record_writer(record0), test_timeout()).get();
    auto persisted0 = std::move(accepted0.persisted);
    flush_ctl.wait_for_flush_starts(1);

    auto accepted1 = ring.write_to_buffer(log_record_writer(record1), test_timeout()).get();
    auto persisted1 = std::move(accepted1.persisted);

    auto queued2 = ring.write_to_buffer(log_record_writer(record2), test_timeout());
    BOOST_REQUIRE(!queued2.available());
    BOOST_REQUIRE_EQUAL(ring.queued_write_count(), 1u);

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
SEASTAR_THREAD_TEST_CASE(test_logstor_write_buffer_ring_stop_drains_blocked_in_flight_write) {
    auto schema = make_kv_schema();
    constexpr size_t buffer_size = 4 * 1024;
    const auto record = make_write_buffer_ring_record(schema, 0, "value", api::timestamp_type(600));

    test_flush_controller flush_ctl{.pause_flushes = true};
    write_buffer_ring ring(make_write_buffer_ring_config(buffer_size, 2), [&flush_ctl] (write_buffer& wb) {
        return flush_ctl(wb);
    });

    ring.start().get();

    auto accepted = ring.write_to_buffer(log_record_writer(record), test_timeout()).get();
    auto persisted = std::move(accepted.persisted);
    flush_ctl.wait_for_flush_starts(1);

    auto stop_fut = ring.stop();
    BOOST_REQUIRE(!stop_fut.available());

    flush_ctl.release_one_flush();
    wait_for_persisted(persisted);
    stop_fut.get();

    const auto actual = flush_ctl.all_records();
    BOOST_REQUIRE_EQUAL(actual.size(), 1u);
    assert_log_record_matches(schema, actual.front().record, record);
}

// Checks that a flush failure is propagated to every write that was already accepted into that buffer.
SEASTAR_THREAD_TEST_CASE(test_logstor_write_buffer_ring_flush_failure_fails_all_writes_in_buffer) {
    auto schema = make_kv_schema();
    constexpr size_t buffer_size = 4 * 1024;

    auto record0 = make_write_buffer_ring_record(schema, 0, "value0", api::timestamp_type(700));
    auto record1 = make_write_buffer_ring_record(schema, 1, "value1", api::timestamp_type(701));
    chunk_builder scratch(buffer_size, segment_kind::mixed);
    BOOST_REQUIRE(scratch.can_fit(log_record_writer(record0)));
    scratch.append(log_record_writer(record0));
    BOOST_REQUIRE(scratch.can_fit(log_record_writer(record1)));

    test_flush_controller flush_ctl{.fail_flush_index = 0};
    // long sync period so both writes land in the same buffer.
    write_buffer_ring ring(make_write_buffer_ring_config(buffer_size, 3, std::chrono::hours(1)), [&flush_ctl] (write_buffer& wb) {
        return flush_ctl(wb);
    });

    ring.start().get();
    auto stop = defer([&ring] noexcept {
        ring.stop().get();
    });

    auto accepted0 = ring.write_to_buffer(log_record_writer(record0), test_timeout()).get();
    auto accepted1 = ring.write_to_buffer(log_record_writer(record1), test_timeout()).get();

    auto flush = ring.flush();

    BOOST_REQUIRE_THROW(accepted0.persisted.get(), std::runtime_error);
    BOOST_REQUIRE_THROW(accepted1.persisted.get(), std::runtime_error);

    flush.get();

    BOOST_REQUIRE_EQUAL(flush_ctl.started_count, 1u);
    BOOST_REQUIRE(flush_ctl.flushed_buffers.empty());
}

BOOST_AUTO_TEST_SUITE_END()
