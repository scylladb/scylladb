/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#include <boost/test/unit_test.hpp>
#include <algorithm>
#include <chrono>
#include <array>
#include <cmath>
#include <deque>
#include <functional>
#include <stdexcept>
#include <map>
#include <span>
#include <vector>
#include <fmt/format.h>
#include <seastar/core/semaphore.hh>
#include <seastar/core/format.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/util/memory-data-source.hh>
#include <seastar/util/defer.hh>

#include "replica/logstor/index.hh"
#include "replica/logstor/logstor.hh"
#include "replica/logstor/ondisk.hh"
#include "replica/logstor/write_buffer.hh"
#include <seastar/testing/thread_test_case.hh>

#include "replica/logstor/segment_io.hh"
#include "replica/database.hh"
#include "dht/i_partitioner.hh"
#include "schema/schema_builder.hh"
#include <seastar/core/simple-stream.hh>
#include "sstables/key.hh"
#include "test/lib/mutation_assertions.hh"
#include "test/lib/mutation_reader_assertions.hh"
#include "test/lib/reader_concurrency_semaphore.hh"
#include "test/lib/tmpdir.hh"
#include "utils/disk-error-handler.hh"
#include "utils/error_injection.hh"
#include "utils/exceptions.hh"

using namespace replica::logstor;

namespace {

schema_ptr make_kv_schema() {
    return schema_builder(1, "ks", "cf")
            .with_column("pk", utf8_type, column_kind::partition_key)
            .with_column("v", utf8_type)
            .build();
}

primary_index_key make_index_key(const schema& s, const dht::decorated_key& dk) {
    return primary_index_key{s, dk};
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
            .key = make_index_key(*schema, m.decorated_key()),
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

buffered_writer_config make_buffered_writer_config(size_t buffer_size, size_t ring_size, size_t max_queued_write_bytes = 0, std::chrono::milliseconds sync_period = std::chrono::milliseconds(0)) {
    return buffered_writer_config{
        .buffer_size = buffer_size,
        .ring_size = ring_size,
        .flush_sg = seastar::default_scheduling_group(),
        .max_queued_write_bytes = max_queued_write_bytes,
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
            co_await flush_release.wait(1);
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

struct shared_logstor_cache {
    ::cache_tracker shared_tracker;
    replica::logstor::cache_tracker logstor_tracker;

    shared_logstor_cache()
        : shared_tracker(utils::updateable_value<double>(1.0), ::cache_tracker::register_metrics::no)
        , logstor_tracker(shared_tracker) {
    }
};

logstor_config make_test_logstor_config(const std::filesystem::path& base_dir) {
    constexpr size_t segment_size = 128 * 1024;
    constexpr size_t file_size = 32 * segment_size;
    return logstor_config{
        .segment_manager_cfg = {
            .base_dir = base_dir,
            .segment_size = segment_size,
            .file_size = file_size,
            .disk_size = 4 * file_size,
            .compaction_enabled = true,
            .max_segments_per_compaction = 8,
            .compaction_sg = seastar::current_scheduling_group(),
            .compaction_static_shares = utils::updateable_value<float>(0.0f),
            .compaction_max_shares = utils::updateable_value<float>(2000.0f),
            .separator_sg = seastar::current_scheduling_group(),
            .split_compaction_sg = seastar::current_scheduling_group(),
        },
        .flush_sg = seastar::current_scheduling_group(),
    };
}

class test_compaction_group_handle final : public logstor_group {
    ::table_id _table_id;
    std::unique_ptr<primary_index> _owned_index;
    primary_index& _index;
    compaction_manager& _cm;
public:
    test_compaction_group_handle(schema_ptr schema, logstor& ls)
        : _table_id(schema->id())
        , _owned_index(ls.make_primary_index(schema, false))
        , _index(*_owned_index)
        , _cm(ls.get_compaction_manager()) {
        _cm.add(*this);
    }

    // A group of a table that already has an index: the index is per table, and all the groups of
    // a table share it. This is what the groups of a split have.
    test_compaction_group_handle(schema_ptr schema, logstor& ls, primary_index& index)
        : _table_id(schema->id())
        , _index(index)
        , _cm(ls.get_compaction_manager()) {
        _cm.add(*this);
    }

    ~test_compaction_group_handle() override {
        _cm.remove(*this).get();
    }

    ::table_id table_id() const noexcept override {
        return _table_id;
    }

    primary_index& logstor_index() noexcept override {
        return _index;
    }

    const primary_index& logstor_index() const noexcept override {
        return _index;
    }

protected:
    compaction_manager& logstor_compaction_manager() noexcept override {
        return _cm;
    }
};

std::set<log_segment_id> snapshot_segment_ids(const utils::chunked_vector<segment_snapshot>& snapshot) {
    std::set<log_segment_id> ids;
    for (const auto& seg : snapshot) {
        ids.insert(seg.segment_id);
    }
    return ids;
}

// Writes all the mutations and flushes the group's separator once, so that they all end up in a
// single segment of the group.
void write_and_flush_segment(logstor& ls, test_compaction_group_handle& cg, std::span<const mutation> ms) {
    for (const auto& m : ms) {
        ls.write(m, write_target(&cg, {}), db::no_timeout).get();
    }
    ls.flush_to_separator().get();
    cg.flush_separator().get();
}

void write_and_flush_segment(logstor& ls, test_compaction_group_handle& cg, const mutation& m) {
    write_and_flush_segment(ls, cg, std::span(&m, 1));
}

// Which side of a split a key falls on is decided by its token, so a split test needs its keys in
// token order. Returns `count` distinct keys, sorted by token.
std::vector<sstring> make_token_ordered_keys(schema_ptr schema, size_t count) {
    auto token_of = [&] (const sstring& pk) {
        return dht::decorate_key(*schema, partition_key::from_single_value(*schema, serialized(pk))).token();
    };
    std::vector<sstring> keys;
    for (size_t i = 0; i < count; ++i) {
        keys.push_back(seastar::format("pk{}", i));
    }
    std::ranges::sort(keys, std::less<>{}, token_of);
    return keys;
}

// Scans every segment of the snapshot and counts the records it holds by their timestamp, which is
// what the tests give each record version to tell them apart.
std::map<api::timestamp_type, size_t> count_records_by_timestamp(logstor& ls, utils::chunked_vector<segment_snapshot>& snapshot) {
    std::map<api::timestamp_type, size_t> counts;
    const auto segment_size = ls.get_segment_manager().get_segment_size();
    for (auto& snap : snapshot) {
        auto in = snap.source(seastar::file_input_stream_options{
            .buffer_size = std::min<size_t>(segment_size, 128 * 1024),
            .read_ahead = 1,
        }).get();
        scan_segment(in, snap.segment_id, segment_size,
            [] (const segment_header&) { return make_ready_future<>(); },
            [&counts] (log_location, const log_record_header& rh) {
                counts[rh.timestamp]++;
                return want_data::no;
            },
            [] (log_location, log_record) { return make_ready_future<>(); }
        ).get();
        in.close().get();
    }
    return counts;
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

size_t record_size_with_value(schema_ptr schema, const sstring& pk, size_t value_size) {
    return log_record_writer(make_log_record(schema, pk, sstring(value_size, 'x'))).size();
}

// Builds a mutation whose log record serializes to exactly `record_size` bytes, by sizing its value
// to whatever is left over. The serialized size grows by a byte per value byte, apart from the
// value's length prefix, so this converges in a step or two.
mutation make_kv_mutation_of_record_size(schema_ptr schema, const sstring& pk, size_t record_size) {
    const auto target = static_cast<ssize_t>(record_size);
    auto size_with_value = [&] (ssize_t value_size) {
        return static_cast<ssize_t>(record_size_with_value(schema, pk, std::max<ssize_t>(value_size, 0)));
    };

    ssize_t value_size = target - size_with_value(0);
    for (int i = 0; i < 4 && size_with_value(value_size) != target; ++i) {
        value_size += target - size_with_value(value_size);
    }

    BOOST_REQUIRE_EQUAL(size_with_value(value_size), target);
    return make_kv_mutation(schema, pk, sstring(value_size, 'x'));
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
    BOOST_REQUIRE_EQUAL(sh.first_token, expected.header.key.token());
    BOOST_REQUIRE_EQUAL(sh.last_token, expected.header.key.token());
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

// A record written to a mixed segment is rewritten by the separator into a full segment of its
// compaction group, so it has to fit a buffer of either kind, which do not have the same room for
// records. The write path used to bound a record by the mixed buffer alone, so a record that only
// fits that one was accepted and could then never be separated, spinning in write_to_separator.
SEASTAR_THREAD_TEST_CASE(test_logstor_largest_accepted_record_can_be_separated) {
    auto schema = make_kv_schema();
    tmpdir dir;

    shared_logstor_cache cache;
    logstor ls(make_test_logstor_config(dir.path()), cache.shared_tracker);
    ls.do_recovery_for_test().get();
    ls.start().get();
    auto stop_store = seastar::defer([&ls] noexcept { ls.stop().get(); });

    test_compaction_group_handle cg(schema, ls);

    const auto segment_size = ls.get_segment_manager().get_segment_size();
    const auto max_size = raw_write_buffer::max_record_size_any_kind(segment_size);

    auto expected = make_kv_mutation_of_record_size(schema, "pk0", max_size);
    auto key = expected.decorated_key();

    ls.write(expected, write_target(&cg, {}), db::no_timeout).get();
    ls.flush_to_separator().get();
    cg.flush_separator().get();

    BOOST_REQUIRE_EQUAL(cg.logstor_segments().segment_count(), 1u);

    auto actual = ls.read(schema, cg.logstor_index(), key, schema->full_slice()).get();
    BOOST_REQUIRE(actual);
    assert_that(*actual).is_equal_to(expected);
}

// Checks that a record that does not fit a segment of either kind is rejected by the write path,
// rather than accepted into a mixed segment that the separator would then not be able to split.
SEASTAR_THREAD_TEST_CASE(test_logstor_rejects_record_that_does_not_fit_a_segment_of_either_kind) {
    auto schema = make_kv_schema();
    tmpdir dir;

    shared_logstor_cache cache;
    logstor ls(make_test_logstor_config(dir.path()), cache.shared_tracker);
    ls.do_recovery_for_test().get();
    ls.start().get();
    auto stop_store = seastar::defer([&ls] noexcept { ls.stop().get(); });

    test_compaction_group_handle cg(schema, ls);

    const auto segment_size = ls.get_segment_manager().get_segment_size();
    const auto max_size = raw_write_buffer::max_record_size_any_kind(segment_size);

    auto too_big = make_kv_mutation_of_record_size(schema, "pk0", max_size + 1);
    BOOST_REQUIRE_THROW(ls.write(too_big, write_target(&cg, {}), db::no_timeout).get(), std::runtime_error);

    BOOST_REQUIRE(!cg.separator_has_data());
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
    BOOST_REQUIRE_EQUAL(b0->get_buffer_size(), 4u * 1024);

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

// Checks that a compaction that fails after rewriting records into its buffer returns the buffer to
// the pool, so that later compactions - and shutdown, which waits for the pool to drain - are not
// blocked by it.
SEASTAR_THREAD_TEST_CASE(test_logstor_failed_compaction_returns_its_buffer_to_the_pool) {
    if constexpr (!std::is_same_v<utils::error_injection_type, utils::error_injection<true>>) {
        return;
    }

    auto schema = make_kv_schema();
    tmpdir dir;

    shared_logstor_cache cache;
    logstor ls(make_test_logstor_config(dir.path()), cache.shared_tracker);
    ls.do_recovery_for_test().get();
    ls.start().get();
    auto stop_store = seastar::defer([&ls] noexcept { ls.stop().get(); });

    test_compaction_group_handle cg(schema, ls);
    auto setup_guard = std::make_optional(ls.get_compaction_manager().disable_compaction(cg).get());

    auto pk0_v0 = make_kv_mutation(schema, "pk0", "v0", api::timestamp_type(1));
    auto pk1_v0 = make_kv_mutation(schema, "pk1", "v1", api::timestamp_type(2));
    auto pk0_v1 = make_kv_mutation(schema, "pk0", "v0-new", api::timestamp_type(3));

    write_and_flush_segment(ls, cg, pk0_v0);
    write_and_flush_segment(ls, cg, pk1_v0);
    write_and_flush_segment(ls, cg, pk0_v1);

    BOOST_REQUIRE_EQUAL(cg.logstor_segments().segment_count(), 3u);

    const auto pk0 = make_index_key(*schema, pk0_v1.decorated_key());
    const auto pk1 = make_index_key(*schema, pk1_v0.decorated_key());

    auto pk0_before = cg.logstor_index().get(pk0);
    auto pk1_before = cg.logstor_index().get(pk1);
    BOOST_REQUIRE(pk0_before);
    BOOST_REQUIRE(pk1_before);

    // The first compaction fails right after rewriting a record, leaving records in its buffer that
    // will never be flushed.
    utils::get_local_injector().enable("logstor_compaction_fail_after_rewrite", true /* one shot */);

    setup_guard.reset();
    ls.get_compaction_manager().submit(cg);
    setup_guard = ls.get_compaction_manager().disable_compaction(cg).get();

    // Nothing was flushed, so the group and the index are unchanged.
    BOOST_REQUIRE_EQUAL(cg.logstor_segments().segment_count(), 3u);
    BOOST_REQUIRE(cg.logstor_index().get(pk0)->location == pk0_before->location);
    BOOST_REQUIRE(cg.logstor_index().get(pk1)->location == pk1_before->location);

    // The buffer of the failed compaction is back in the pool, so this one runs to completion.
    setup_guard.reset();
    ls.get_compaction_manager().submit(cg);
    auto compaction_guard = ls.get_compaction_manager().disable_compaction(cg).get();

    BOOST_REQUIRE_EQUAL(cg.logstor_segments().segment_count(), 1u);

    auto pk0_after = cg.logstor_index().get(pk0);
    auto pk1_after = cg.logstor_index().get(pk1);
    BOOST_REQUIRE(pk0_after);
    BOOST_REQUIRE(pk1_after);
    BOOST_REQUIRE(pk0_after->location != pk0_before->location);
    BOOST_REQUIRE(pk1_after->location != pk1_before->location);

    assert_that(*ls.read(schema, cg.logstor_index(), pk0_v1.decorated_key(), schema->full_slice()).get()).is_equal_to(pk0_v1);
    assert_that(*ls.read(schema, cg.logstor_index(), pk1_v0.decorated_key(), schema->full_slice()).get()).is_equal_to(pk1_v0);
}

// Checks that a compaction whose index updates fail returns its buffer to the pool. This failure
// surfaces only after the output segment was written, so it lands after flush() handed the pending
// updates to when_all_succeed() - which moves them out of the vector. That is the path where a
// teardown still walking that vector would operate on moved-from futures.
SEASTAR_THREAD_TEST_CASE(test_logstor_compaction_failing_index_update_returns_its_buffer_to_the_pool) {
    if constexpr (!std::is_same_v<utils::error_injection_type, utils::error_injection<true>>) {
        return;
    }

    auto schema = make_kv_schema();
    tmpdir dir;

    shared_logstor_cache cache;
    logstor ls(make_test_logstor_config(dir.path()), cache.shared_tracker);
    ls.do_recovery_for_test().get();
    ls.start().get();
    auto stop_store = seastar::defer([&ls] noexcept { ls.stop().get(); });

    test_compaction_group_handle cg(schema, ls);
    auto setup_guard = std::make_optional(ls.get_compaction_manager().disable_compaction(cg).get());

    auto pk0_v0 = make_kv_mutation(schema, "pk0", "v0", api::timestamp_type(1));
    auto pk1_v0 = make_kv_mutation(schema, "pk1", "v1", api::timestamp_type(2));
    auto pk0_v1 = make_kv_mutation(schema, "pk0", "v0-new", api::timestamp_type(3));

    write_and_flush_segment(ls, cg, pk0_v0);
    write_and_flush_segment(ls, cg, pk1_v0);
    write_and_flush_segment(ls, cg, pk0_v1);

    utils::get_local_injector().enable("logstor_compaction_fail_index_update", true /* one shot */);

    setup_guard.reset();
    ls.get_compaction_manager().submit(cg);
    setup_guard = ls.get_compaction_manager().disable_compaction(cg).get();

    // The records are readable whichever location the index kept for them.
    assert_that(*ls.read(schema, cg.logstor_index(), pk0_v1.decorated_key(), schema->full_slice()).get()).is_equal_to(pk0_v1);
    assert_that(*ls.read(schema, cg.logstor_index(), pk1_v0.decorated_key(), schema->full_slice()).get()).is_equal_to(pk1_v0);

    // The buffer of the failed compaction is back in the pool, so compaction still runs.
    setup_guard.reset();
    ls.get_compaction_manager().submit(cg);
    auto compaction_guard = ls.get_compaction_manager().disable_compaction(cg).get();

    assert_that(*ls.read(schema, cg.logstor_index(), pk0_v1.decorated_key(), schema->full_slice()).get()).is_equal_to(pk0_v1);
    assert_that(*ls.read(schema, cg.logstor_index(), pk1_v0.decorated_key(), schema->full_slice()).get()).is_equal_to(pk1_v0);
}

// Checks that primary_index accounting callbacks track live bytes across
// inserts, overwrites, relocations, erases, range erases, and clear().
SEASTAR_THREAD_TEST_CASE(test_logstor_primary_index_space_accounting) {
    auto schema = make_kv_schema();
    struct accounting_subscriber : space_accounting_subscriber {
        ssize_t live_bytes = 0;
        size_t add_calls = 0;
        size_t free_calls = 0;
        std::vector<log_location> added_locations;
        std::vector<log_location> freed_locations;

        bool is_live(log_location loc) const {
            return std::count(added_locations.begin(), added_locations.end(), loc)
                 > std::count(freed_locations.begin(), freed_locations.end(), loc);
        }

        size_t live_location_count() const {
            return std::count_if(added_locations.begin(), added_locations.end(), [&] (log_location loc) {
                return is_live(loc);
            });
        }

        void on_add_record(log_location loc) noexcept override {
            live_bytes += loc.size;
            ++add_calls;
            added_locations.push_back(loc);
        }

        void on_free_record(log_location loc) noexcept override {
            live_bytes -= loc.size;
            ++free_calls;
            BOOST_REQUIRE(is_live(loc));
            freed_locations.push_back(loc);
        }
    } accounting;

    primary_index index(schema, accounting, nullptr);

    const auto pk0 = make_index_key(*schema, make_kv_mutation(schema, "pk0", "v0").decorated_key());
    const auto pk1 = make_index_key(*schema, make_kv_mutation(schema, "pk1", "v1").decorated_key());
    const auto pk2 = make_index_key(*schema, make_kv_mutation(schema, "pk2", "v2").decorated_key());

    const log_location loc0{.segment = log_segment_id{1}, .offset = 0, .size = 11};
    const log_location loc0_old{.segment = log_segment_id{1}, .offset = 16, .size = 7};
    const log_location loc1{.segment = log_segment_id{2}, .offset = 0, .size = 17};
    const log_location loc2{.segment = log_segment_id{3}, .offset = 0, .size = 13};
    const log_location loc3{.segment = log_segment_id{4}, .offset = 0, .size = 19};
    const log_location loc4{.segment = log_segment_id{5}, .offset = 0, .size = 23};
    const log_location loc5{.segment = log_segment_id{6}, .offset = 0, .size = 29};

    // insert(pk0, loc0): new entry, succeeds, no previous entry to free  →  {pk0: loc0}
    auto outcome0 = index.insert(pk0, index_entry{.location = loc0, .timestamp = api::timestamp_type(10)});
    BOOST_REQUIRE(outcome0.inserted());
    BOOST_REQUIRE(!outcome0.previous_entry);
    BOOST_REQUIRE_EQUAL(accounting.live_bytes, ssize_t(loc0.size));
    BOOST_REQUIRE_EQUAL(accounting.add_calls, 1u);
    BOOST_REQUIRE_EQUAL(accounting.free_calls, 0u);
    BOOST_REQUIRE_EQUAL(accounting.live_location_count(), 1);
    BOOST_REQUIRE(accounting.is_live(loc0));

    // insert(pk0, loc0_old): older timestamp, rejected, no accounting change  →  {pk0: loc0}
    auto outcome_old = index.insert(pk0, index_entry{.location = loc0_old, .timestamp = api::timestamp_type(9)});
    BOOST_REQUIRE(outcome_old.result == primary_index::insert_result::superseded);
    BOOST_REQUIRE(outcome_old.previous_entry);
    BOOST_REQUIRE(outcome_old.previous_entry->location == loc0);
    BOOST_REQUIRE_EQUAL(outcome_old.previous_entry->timestamp, api::timestamp_type(10));
    BOOST_REQUIRE_EQUAL(accounting.live_bytes, ssize_t(loc0.size));
    BOOST_REQUIRE_EQUAL(accounting.add_calls, 1u);
    BOOST_REQUIRE_EQUAL(accounting.free_calls, 0u);
    BOOST_REQUIRE_EQUAL(accounting.live_location_count(), 1);
    BOOST_REQUIRE(accounting.is_live(loc0));

    // insert(pk0, loc1): newer timestamp, replaces loc0, old location freed via accounting  →  {pk0: loc1}
    auto outcome1 = index.insert(pk0, index_entry{.location = loc1, .timestamp = api::timestamp_type(11)});
    BOOST_REQUIRE(outcome1.inserted());
    BOOST_REQUIRE(outcome1.previous_entry);
    BOOST_REQUIRE(outcome1.previous_entry->location == loc0);
    BOOST_REQUIRE_EQUAL(outcome1.previous_entry->timestamp, api::timestamp_type(10));
    BOOST_REQUIRE_EQUAL(accounting.live_bytes, ssize_t(loc1.size));
    BOOST_REQUIRE_EQUAL(accounting.add_calls, 2u);
    BOOST_REQUIRE_EQUAL(accounting.free_calls, 1u);
    BOOST_REQUIRE_EQUAL(accounting.live_location_count(), 1);
    BOOST_REQUIRE(accounting.is_live(loc1));
    BOOST_REQUIRE(!accounting.is_live(loc0));
    BOOST_REQUIRE(accounting.added_locations.back() == loc1);
    BOOST_REQUIRE(accounting.freed_locations.back() == loc0);

    // insert(pk1, loc2): new key, succeeds, adds to live bytes  →  {pk0: loc1, pk1: loc2}
    auto outcome2 = index.insert(pk1, index_entry{.location = loc2, .timestamp = api::timestamp_type(7)});
    BOOST_REQUIRE(outcome2.inserted());
    BOOST_REQUIRE(!outcome2.previous_entry);
    BOOST_REQUIRE_EQUAL(accounting.live_bytes, ssize_t(loc1.size + loc2.size));
    BOOST_REQUIRE_EQUAL(accounting.add_calls, 3u);
    BOOST_REQUIRE_EQUAL(accounting.free_calls, 1u);
    BOOST_REQUIRE_EQUAL(accounting.live_location_count(), 2);
    BOOST_REQUIRE(accounting.is_live(loc1));
    BOOST_REQUIRE(accounting.is_live(loc2));

    // erase(pk1, loc1): location mismatch, returns false, no accounting change  →  {pk0: loc1, pk1: loc2}
    BOOST_REQUIRE(!index.erase(pk1, loc1));
    BOOST_REQUIRE_EQUAL(accounting.live_bytes, ssize_t(loc1.size + loc2.size));
    BOOST_REQUIRE_EQUAL(accounting.add_calls, 3u);
    BOOST_REQUIRE_EQUAL(accounting.free_calls, 1u);
    BOOST_REQUIRE_EQUAL(accounting.live_location_count(), 2);

    // update_record_location(pk0, loc1 -> loc3): old location matches, succeeds, frees loc1 adds loc3  →  {pk0: loc3, pk1: loc2}
    BOOST_REQUIRE(index.update_record_location(pk0, loc1, loc3));
    BOOST_REQUIRE_EQUAL(accounting.live_bytes, ssize_t(loc2.size + loc3.size));
    BOOST_REQUIRE_EQUAL(accounting.add_calls, 4u);
    BOOST_REQUIRE_EQUAL(accounting.free_calls, 2u);
    BOOST_REQUIRE_EQUAL(accounting.live_location_count(), 2);
    BOOST_REQUIRE(accounting.is_live(loc2));
    BOOST_REQUIRE(accounting.is_live(loc3));
    BOOST_REQUIRE(!accounting.is_live(loc1));
    BOOST_REQUIRE(accounting.added_locations.back() == loc3);
    BOOST_REQUIRE(accounting.freed_locations.back() == loc1);

    // update_record_location(pk0, loc1 -> loc4): old location no longer current, fails, no accounting change  →  {pk0: loc3, pk1: loc2}
    BOOST_REQUIRE(!index.update_record_location(pk0, loc1, loc4));
    BOOST_REQUIRE_EQUAL(accounting.live_bytes, ssize_t(loc2.size + loc3.size));
    BOOST_REQUIRE_EQUAL(accounting.add_calls, 4u);
    BOOST_REQUIRE_EQUAL(accounting.free_calls, 2u);
    BOOST_REQUIRE_EQUAL(accounting.live_location_count(), 2);

    // erase(pk1, loc2): location matches, succeeds, frees loc2  →  {pk0: loc3}
    BOOST_REQUIRE(index.erase(pk1, loc2));
    BOOST_REQUIRE_EQUAL(accounting.live_bytes, ssize_t(loc3.size));
    BOOST_REQUIRE_EQUAL(accounting.add_calls, 4u);
    BOOST_REQUIRE_EQUAL(accounting.free_calls, 3u);
    BOOST_REQUIRE_EQUAL(accounting.live_location_count(), 1);
    BOOST_REQUIRE(accounting.is_live(loc3));
    BOOST_REQUIRE(!accounting.is_live(loc2));
    BOOST_REQUIRE(accounting.freed_locations.back() == loc2);

    // insert(pk1, loc4): new entry for pk1 (previously erased), succeeds  →  {pk0: loc3, pk1: loc4}
    auto outcome4 = index.insert(pk1, index_entry{.location = loc4, .timestamp = api::timestamp_type(12)});
    BOOST_REQUIRE(outcome4.inserted());
    BOOST_REQUIRE(!outcome4.previous_entry);
    // insert(pk2, loc5): new key, succeeds  →  {pk0: loc3, pk1: loc4, pk2: loc5}
    auto outcome5 = index.insert(pk2, index_entry{.location = loc5, .timestamp = api::timestamp_type(13)});
    BOOST_REQUIRE(outcome5.inserted());
    BOOST_REQUIRE(!outcome5.previous_entry);
    BOOST_REQUIRE_EQUAL(accounting.live_bytes, ssize_t(loc3.size + loc4.size + loc5.size));
    BOOST_REQUIRE_EQUAL(accounting.add_calls, 6u);
    BOOST_REQUIRE_EQUAL(accounting.free_calls, 3u);
    BOOST_REQUIRE_EQUAL(accounting.live_location_count(), 3);

    // range erase pk1: removes pk1 entry (loc4), frees via accounting  →  {pk0: loc3, pk2: loc5}
    dht::token_range pk1_range(
            std::optional(interval_bound(pk1.token(), true)),
            std::optional(interval_bound(pk1.token(), true)));
    index.erase(pk1_range).get();
    BOOST_REQUIRE_EQUAL(accounting.live_bytes, ssize_t(loc3.size + loc5.size));
    BOOST_REQUIRE_EQUAL(accounting.add_calls, 6u);
    BOOST_REQUIRE_EQUAL(accounting.free_calls, 4u);
    BOOST_REQUIRE_EQUAL(accounting.live_location_count(), 2);
    BOOST_REQUIRE(!accounting.is_live(loc4));
    BOOST_REQUIRE(accounting.is_live(loc3));
    BOOST_REQUIRE(accounting.is_live(loc5));
    BOOST_REQUIRE(accounting.freed_locations.back() == loc4);

    // clear(): removes all remaining entries (pk0->loc3, pk2->loc5), all freed  →  {}
    index.clear().get();
    BOOST_REQUIRE(index.empty());
    BOOST_REQUIRE_EQUAL(accounting.live_bytes, 0);
    BOOST_REQUIRE_EQUAL(accounting.add_calls, 6u);
    BOOST_REQUIRE_EQUAL(accounting.free_calls, 6u);
    BOOST_REQUIRE_EQUAL(accounting.live_location_count(), 0);
    BOOST_REQUIRE_EQUAL(accounting.freed_locations.size(), 6u);
}

// Checks that range erase and clear() account for freed log locations correctly.
SEASTAR_THREAD_TEST_CASE(test_logstor_primary_index_range_erase_and_clear_space_accounting) {
    auto schema = make_kv_schema();
    struct accounting_subscriber : space_accounting_subscriber {
        ssize_t live_bytes = 0;
        size_t add_calls = 0;
        size_t free_calls = 0;
        std::vector<log_location> freed_locations;

        void on_add_record(log_location loc) noexcept override {
            live_bytes += loc.size;
            ++add_calls;
        }

        void on_free_record(log_location loc) noexcept override {
            live_bytes -= loc.size;
            ++free_calls;
            freed_locations.push_back(loc);
        }
    } accounting;

    primary_index index(schema, accounting, nullptr);

    struct entry {
        primary_index_key key;
        log_location loc;
    };

    std::vector<entry> entries = {
        { make_index_key(*schema, make_kv_mutation(schema, "pk0", "v0").decorated_key()), {.segment = log_segment_id{11}, .offset = 0, .size = 5} },
        { make_index_key(*schema, make_kv_mutation(schema, "pk1", "v1").decorated_key()), {.segment = log_segment_id{12}, .offset = 0, .size = 7} },
        { make_index_key(*schema, make_kv_mutation(schema, "pk2", "v2").decorated_key()), {.segment = log_segment_id{13}, .offset = 0, .size = 11} },
        { make_index_key(*schema, make_kv_mutation(schema, "pk3", "v3").decorated_key()), {.segment = log_segment_id{14}, .offset = 0, .size = 13} },
        { make_index_key(*schema, make_kv_mutation(schema, "pk4", "v4").decorated_key()), {.segment = log_segment_id{15}, .offset = 0, .size = 17} },
    };

    std::sort(entries.begin(), entries.end(), [&] (const entry& a, const entry& b) {
        return a.key.token() < b.key.token();
    });

    auto insert = [&] (const entry& e, api::timestamp_type ts) {
        auto outcome = index.insert(e.key, index_entry{.location = e.loc, .timestamp = ts});
        BOOST_REQUIRE(outcome.inserted());
        BOOST_REQUIRE(!outcome.previous_entry);
    };

    insert(entries[0], api::timestamp_type(10));
    insert(entries[1], api::timestamp_type(11));
    insert(entries[2], api::timestamp_type(12));
    insert(entries[3], api::timestamp_type(13));
    insert(entries[4], api::timestamp_type(14));

    BOOST_REQUIRE_EQUAL(accounting.add_calls, 5u);
    BOOST_REQUIRE_EQUAL(accounting.free_calls, 0u);
    BOOST_REQUIRE_EQUAL(accounting.live_bytes, ssize_t(entries[0].loc.size + entries[1].loc.size + entries[2].loc.size + entries[3].loc.size + entries[4].loc.size));

    dht::token_range range(
            std::optional(interval_bound(entries[1].key.token(), true)),
            std::optional(interval_bound(entries[3].key.token(), true)));
    index.erase(range).get();

    BOOST_REQUIRE_EQUAL(accounting.add_calls, 5u);
    BOOST_REQUIRE_EQUAL(accounting.free_calls, 3u);
    BOOST_REQUIRE_EQUAL(accounting.live_bytes, ssize_t(entries[0].loc.size + entries[4].loc.size));
    BOOST_REQUIRE(index.get(entries[0].key).has_value());
    BOOST_REQUIRE(!index.get(entries[1].key).has_value());
    BOOST_REQUIRE(!index.get(entries[2].key).has_value());
    BOOST_REQUIRE(!index.get(entries[3].key).has_value());
    BOOST_REQUIRE(index.get(entries[4].key).has_value());
    BOOST_REQUIRE_EQUAL(std::count(accounting.freed_locations.begin(), accounting.freed_locations.end(), entries[1].loc), 1);
    BOOST_REQUIRE_EQUAL(std::count(accounting.freed_locations.begin(), accounting.freed_locations.end(), entries[2].loc), 1);
    BOOST_REQUIRE_EQUAL(std::count(accounting.freed_locations.begin(), accounting.freed_locations.end(), entries[3].loc), 1);

    index.clear().get();

    BOOST_REQUIRE(index.empty());
    BOOST_REQUIRE_EQUAL(accounting.add_calls, 5u);
    BOOST_REQUIRE_EQUAL(accounting.free_calls, 5u);
    BOOST_REQUIRE_EQUAL(accounting.live_bytes, 0);
    BOOST_REQUIRE_EQUAL(accounting.freed_locations.size(), 5u);
    BOOST_REQUIRE_EQUAL(std::count(accounting.freed_locations.begin(), accounting.freed_locations.end(), entries[0].loc), 1);
    BOOST_REQUIRE_EQUAL(std::count(accounting.freed_locations.begin(), accounting.freed_locations.end(), entries[4].loc), 1);
}

// Checks that primary_index caps the number of distinct keys sharing a token at
// max_keys_per_token, and that freeing a slot lets another key in.
SEASTAR_THREAD_TEST_CASE(test_logstor_primary_index_bounds_keys_per_token) {
    struct noop_accounting : space_accounting_subscriber {
        void on_add_record(log_location) noexcept override {}
        void on_free_record(log_location) noexcept override {}
    } accounting;

    auto schema = make_kv_schema();
    primary_index index(schema, accounting, nullptr);

    const auto colliding_token = dht::token::from_int64(0x1234567890abcdef);
    // Keys that share a token but differ in their key hash.
    auto make_colliding_key = [&] (uint8_t suffix) {
        key_hash hash{};
        hash.back() = suffix;
        return primary_index_key(colliding_token, hash);
    };

    auto entry_at = [] (uint32_t segment) {
        return index_entry{
            .location = log_location{.segment = log_segment_id{segment}, .offset = 0, .size = 8},
            .timestamp = api::timestamp_type(1),
        };
    };

    std::vector<primary_index_key> keys;
    for (size_t i = 0; i < primary_index::max_keys_per_token + 1; ++i) {
        keys.push_back(make_colliding_key(uint8_t(i)));
    }

    for (size_t i = 0; i < primary_index::max_keys_per_token; ++i) {
        BOOST_REQUIRE(index.insert(keys[i], entry_at(i)).inserted());
    }
    BOOST_REQUIRE_EQUAL(index.get_key_count(), primary_index::max_keys_per_token);

    // One key too many for this token: rejected, and the index is left untouched.
    const auto& extra_key = keys.back();
    auto overflow = index.insert(extra_key, entry_at(100));
    BOOST_REQUIRE(overflow.result == primary_index::insert_result::token_overflow);
    BOOST_REQUIRE(!overflow.previous_entry);
    BOOST_REQUIRE(!index.get(extra_key));
    BOOST_REQUIRE_EQUAL(index.get_key_count(), primary_index::max_keys_per_token);
    for (size_t i = 0; i < primary_index::max_keys_per_token; ++i) {
        auto entry = index.get(keys[i]);
        BOOST_REQUIRE(entry);
        BOOST_REQUIRE(entry->location == entry_at(i).location);
    }

    // The bound counts distinct keys, so overwriting a resident key still works.
    auto overwrite = index.insert(keys[0], index_entry{.location = entry_at(200).location, .timestamp = api::timestamp_type(2)});
    BOOST_REQUIRE(overwrite.inserted());
    BOOST_REQUIRE(overwrite.previous_entry);
    BOOST_REQUIRE_EQUAL(index.get_key_count(), primary_index::max_keys_per_token);

    // A key on another token is unaffected by the full bucket.
    auto other_token_key = primary_index_key(dht::token::from_int64(1), key_hash{});
    BOOST_REQUIRE(index.insert(other_token_key, entry_at(300)).inserted());
    BOOST_REQUIRE_EQUAL(index.get_key_count(), primary_index::max_keys_per_token + 1);

    // Freeing a slot admits the previously rejected key.
    BOOST_REQUIRE(index.erase(keys[1], entry_at(1).location));
    BOOST_REQUIRE(index.insert(extra_key, entry_at(100)).inserted());
    BOOST_REQUIRE(index.get(extra_key));
    BOOST_REQUIRE_EQUAL(index.get_key_count(), primary_index::max_keys_per_token + 1);

    index.clear().get();
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
        seen_records[0].header.key.token(),
        seen_records[1].header.key.token(),
        seen_records[2].header.key.token(),
    });
    auto expected_last_token = std::max({
        seen_records[0].header.key.token(),
        seen_records[1].header.key.token(),
        seen_records[2].header.key.token(),
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
    auto stop = defer([&writer] noexcept {
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

// Checks that flush() seals and writes a partially filled head buffer even when the sync timer has not expired.
SEASTAR_THREAD_TEST_CASE(test_logstor_buffered_writer_flushes_partial_buffer_with_large_sync_period) {
    auto schema = make_kv_schema();
    constexpr size_t buffer_size = 4 * 1024;
    constexpr auto sync_period = std::chrono::hours(1);

    test_flush_controller flush_ctl;
    buffered_writer writer(make_buffered_writer_config(buffer_size, 3, 0, sync_period), [&flush_ctl] (write_buffer& wb) {
        return flush_ctl(wb);
    });

    writer.start().get();
    auto stop = defer([&writer] noexcept {
        writer.stop().get();
    });

    std::vector<log_record> expected;
    std::vector<future<log_location_with_holder>> persisted;
    for (size_t i = 0; i < 3; ++i) {
        auto record = make_buffered_writer_record(schema, i, sstring(fmt::format("value{}", i)), api::timestamp_type(150 + i));
        expected.push_back(record);
        auto accepted = writer.write_to_buffer(log_record_writer(record), test_timeout()).get();
        persisted.push_back(std::move(accepted.persisted));
    }

    BOOST_REQUIRE(flush_ctl.flushed_buffers.empty());

    auto flush = writer.flush();
    flush_ctl.wait_for_flush_starts(1);

    std::vector<log_location> locations;
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
SEASTAR_THREAD_TEST_CASE(test_logstor_buffered_writer_paused_flush_fills_ring_then_drains) {
    auto schema = make_kv_schema();
    constexpr size_t buffer_size = 4 * 1024;
    const auto large_value = make_single_buffer_value(schema, buffer_size);

    test_flush_controller flush_ctl{.pause_flushes = true};
    buffered_writer writer(make_buffered_writer_config(buffer_size, 2), [&flush_ctl] (write_buffer& wb) {
        return flush_ctl(wb);
    });

    writer.start().get();
    auto stop = defer([&writer] noexcept {
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
    auto stop = defer([&writer] noexcept {
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
    auto stop = defer([&writer] noexcept {
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
    auto stop = defer([&writer] noexcept {
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
    // long sync period so both writes land in the same buffer.
    buffered_writer writer(make_buffered_writer_config(buffer_size, 3, 0, std::chrono::hours(1)), [&flush_ctl] (write_buffer& wb) {
        return flush_ctl(wb);
    });

    writer.start().get();
    auto stop = defer([&writer] noexcept {
        writer.stop().get();
    });

    auto accepted0 = writer.write_to_buffer(log_record_writer(record0), test_timeout()).get();
    auto accepted1 = writer.write_to_buffer(log_record_writer(record1), test_timeout()).get();

    auto flush = writer.flush();

    BOOST_REQUIRE_THROW(accepted0.persisted.get(), std::runtime_error);
    BOOST_REQUIRE_THROW(accepted1.persisted.get(), std::runtime_error);

    flush.get();

    BOOST_REQUIRE_EQUAL(flush_ctl.started_count, 1u);
    BOOST_REQUIRE(flush_ctl.flushed_buffers.empty());
}

// A device error reported by the file layer on a segment write must fire the logstor disk error
// signal, on top of failing the write. The signal is what makes the node isolate itself instead of
// staying in the ring with a store it can no longer write to.
SEASTAR_THREAD_TEST_CASE(test_logstor_segment_write_io_error_signals_disk_error) {
    if constexpr (!std::is_same_v<utils::error_injection_type, utils::error_injection<true>>) {
        return;
    }

    auto schema = make_kv_schema();
    tmpdir dir;

    shared_logstor_cache cache;
    logstor ls(make_test_logstor_config(dir.path()), cache.shared_tracker);
    ls.do_recovery_for_test().get();
    ls.start().get();
    auto stop_store = seastar::defer([&ls] noexcept { ls.stop().get(); });

    test_compaction_group_handle cg(schema, ls);

    unsigned signalled = 0;
    boost::signals2::scoped_connection conn = logstor_error.connect([&signalled] { ++signalled; });

    utils::get_local_injector().enable("logstor_segment_write_io_error", true /* one shot */);
    auto m = make_kv_mutation(schema, "pk0", "io-error-value");
    BOOST_REQUIRE_THROW(ls.write(m, write_target(&cg, {}), db::no_timeout).get(), storage_io_error);
    BOOST_REQUIRE_EQUAL(signalled, 1u);

    // The failed write only retired its segment, so a write to a new segment still succeeds and
    // does not signal again.
    write_and_flush_segment(ls, cg, make_kv_mutation(schema, "pk1", "value-after-io-error"));
    BOOST_REQUIRE_EQUAL(signalled, 1u);
}

SEASTAR_THREAD_TEST_CASE(test_logstor_write_and_separator_flush) {
    auto schema = make_kv_schema();
    tmpdir dir;

    shared_logstor_cache cache;
    logstor ls(make_test_logstor_config(dir.path()), cache.shared_tracker);
    ls.do_recovery_for_test().get();
    ls.start().get();
    auto stop_store = seastar::defer([&ls] noexcept { ls.stop().get(); });

    test_compaction_group_handle cg(schema, ls);

    auto expected = make_kv_mutation(schema, "pk0", "separator-value");
    auto key = expected.decorated_key();

    ls.write(expected, write_target(&cg, {}), db::no_timeout).get();
    ls.flush_to_separator().get();

    BOOST_REQUIRE(cg.separator_has_data());
    BOOST_REQUIRE_EQUAL(cg.separator_held_segment_count(), 1u);
    BOOST_REQUIRE_EQUAL(cg.logstor_segments().segment_count(), 0u);

    auto entry_before_flush = cg.logstor_index().get(make_index_key(*schema, key));
    BOOST_REQUIRE(entry_before_flush);

    cg.flush_separator().get();

    BOOST_REQUIRE(!cg.separator_has_data());
    // The flush released everything the buffer was holding, which is also what gives it back to the
    // pool: a group with nothing to separate holds no buffer.
    BOOST_REQUIRE_EQUAL(cg.separator_held_segment_count(), 0u);
    BOOST_REQUIRE_EQUAL(cg.logstor_segments().segment_count(), 1u);

    auto snapshot = ls.get_segment_manager().make_snapshot(cg).get();
    BOOST_REQUIRE_EQUAL(snapshot.size(), 1u);

    auto entry_after_flush = cg.logstor_index().get(make_index_key(*schema, key));
    BOOST_REQUIRE(entry_after_flush);
    BOOST_REQUIRE(entry_after_flush->location.segment != entry_before_flush->location.segment);
    BOOST_REQUIRE_EQUAL(entry_after_flush->location.segment.value, snapshot.front().segment_id.value);

    auto actual = ls.read(schema, cg.logstor_index(), key, schema->full_slice()).get();
    BOOST_REQUIRE(actual);
    assert_that(*actual).is_equal_to(expected);
}

// Checks that a group discarded while the separator still holds unflushed records for it gives up
// what it was holding: its records are left where they are, and the segments they are in are left
// allocated. The index still points at them there, so freeing one would let it be reallocated over
// live records - free_segment() aborts on the live data it finds rather than allow that, so the
// reference has to be released as a failed flush rather than as a completed one.
SEASTAR_THREAD_TEST_CASE(test_logstor_discarded_group_does_not_free_its_unflushed_source_segments) {
    auto schema = make_kv_schema();
    tmpdir dir;

    shared_logstor_cache cache;
    logstor ls(make_test_logstor_config(dir.path()), cache.shared_tracker);
    ls.do_recovery_for_test().get();
    ls.start().get();
    auto stop_store = seastar::defer([&ls] noexcept { ls.stop().get(); });

    auto& sm = ls.get_segment_manager();
    test_compaction_group_handle cg(schema, ls);
    // Writes for this group fill the shared active segment, so that the segment holding the record
    // below is switched away from and drops the reference it holds itself. Its own records are
    // separated out, which gives back the reference its separator buffer takes.
    test_compaction_group_handle filler(schema, ls);

    auto expected = make_kv_mutation(schema, "pk0", "separator-value");
    auto key = expected.decorated_key();
    ls.write(expected, write_target(&cg, {}), db::no_timeout).get();
    ls.flush_to_separator().get();
    BOOST_REQUIRE_EQUAL(cg.separator_held_segment_count(), 1u);

    auto entry_before_discard = cg.logstor_index().get(make_index_key(*schema, key));
    BOOST_REQUIRE(entry_before_discard);

    // A value that takes most of a segment, so that the second record does not fit next to the first
    // and the active segment has to be switched.
    const auto filler_value = sstring(sm.get_segment_size() * 5 / 8, 'x');
    write_and_flush_segment(ls, filler, make_kv_mutation(schema, "fill0", filler_value));
    write_and_flush_segment(ls, filler, make_kv_mutation(schema, "fill1", filler_value));

    // The separator buffer of the group under test now holds the last reference to the segment its
    // record is in, and nothing has flushed it.
    BOOST_REQUIRE(cg.separator_has_data());
    BOOST_REQUIRE_EQUAL(cg.separator_held_segment_count(), 1u);

    // Discard the group the way a compaction group stopped while the separator still has records for
    // it does. Removing it rather than destroying it keeps its index readable below.
    ls.get_compaction_manager().remove(cg).get();

    // Nothing of the buffer is left held, which is also what gives it back to the pool.
    BOOST_REQUIRE(!cg.separator_has_data());
    BOOST_REQUIRE_EQUAL(cg.separator_held_segment_count(), 0u);

    // The records were never written out, so the index still points at them where they were, and the
    // segment that holds them was not freed and reused underneath it.
    auto entry_after_discard = cg.logstor_index().get(make_index_key(*schema, key));
    BOOST_REQUIRE(entry_after_discard);
    BOOST_REQUIRE(entry_after_discard->location == entry_before_discard->location);

    auto actual = ls.read(schema, cg.logstor_index(), key, schema->full_slice()).get();
    BOOST_REQUIRE(actual);
    assert_that(*actual).is_equal_to(expected);
}

// Checks that a group whose separator was closed takes no separator writes at all. Nothing can flush
// what such a group buffers, so a record handed to it afterwards - which is what a write that was
// already in flight when the group was closed amounts to - has to be refused rather than buffered.
// The write itself still succeeds and the record stays readable where it is.
SEASTAR_THREAD_TEST_CASE(test_logstor_closed_group_takes_no_separator_writes) {
    auto schema = make_kv_schema();
    tmpdir dir;

    shared_logstor_cache cache;
    logstor ls(make_test_logstor_config(dir.path()), cache.shared_tracker);
    ls.do_recovery_for_test().get();
    ls.start().get();
    auto stop_store = seastar::defer([&ls] noexcept { ls.stop().get(); });

    test_compaction_group_handle cg(schema, ls);

    // Close the group's separator the way stopping its compaction group does, and then write to it
    // anyway.
    ls.get_compaction_manager().remove(cg).get();

    auto expected = make_kv_mutation(schema, "pk0", "separator-value");
    auto key = expected.decorated_key();
    ls.write(expected, write_target(&cg, {}), db::no_timeout).get();
    ls.flush_to_separator().get();

    // The separator refused the record, so the group holds nothing: no buffer of its own, no reference
    // to the segment the record is in, and no segment of its own that it was written into.
    BOOST_REQUIRE(!cg.separator_has_data());
    BOOST_REQUIRE_EQUAL(cg.separator_held_segment_count(), 0u);
    BOOST_REQUIRE_EQUAL(cg.logstor_segments().segment_count(), 0u);

    // The write itself succeeded and the record is readable where it was written.
    BOOST_REQUIRE(cg.logstor_index().get(make_index_key(*schema, key)));

    auto actual = ls.read(schema, cg.logstor_index(), key, schema->full_slice()).get();
    BOOST_REQUIRE(actual);
    assert_that(*actual).is_equal_to(expected);
}

// Checks that free-segment watermarks are correctly derived from disk size and target fraction,
// with floor/cap enforcement ensuring no configuration prevents compaction from starting or stopping.
SEASTAR_THREAD_TEST_CASE(test_logstor_free_segment_watermarks) {
    // Properties that have to hold whatever the disk size and whatever the live-updatable trigger
    // threshold happens to be set to.
    auto check = [] (uint64_t segment_count, double fraction) {
        const auto w = make_free_segment_watermarks(segment_count, fraction);
        BOOST_TEST_CONTEXT("segment_count=" << segment_count << " fraction=" << fraction) {
            BOOST_REQUIRE_LE(w.low, w.high);
            // `high` has to stay reachable, or automatic compaction could never stop.
            BOOST_REQUIRE_LE(w.high, segment_count);
            // And there has to be a band for the hysteresis to work in, unless the trigger is
            // disabled or the target already covers the whole disk.
            BOOST_REQUIRE(w.low < w.high || w.low == 0 || w.high == segment_count);
        }
        return w;
    };
    for (uint64_t segment_count : {1u, 8u, 32u, 128u, 1024u, 100000u}) {
        for (double fraction : {0.0, 0.001, 0.05, 0.2, 0.9, 1.0, 1.5, -0.05, std::nan(""), std::numeric_limits<double>::infinity()}) {
            check(segment_count, fraction);
        }
    }

    // On a disk large enough for the configured fraction to clear the floor, that is what the
    // target is.
    const auto large = check(100000, 0.05);
    BOOST_REQUIRE_EQUAL(large.low, 5000u);
    const auto smaller = check(10000, 0.05);
    BOOST_REQUIRE_EQUAL(smaller.low, 500u);

    // An explicitly configured fraction is never clamped down by the floor.
    BOOST_REQUIRE_EQUAL(check(100000, 0.20).low, 20000u);

    // Where the fraction rounds down to a segment or two the absolute floor takes over, so that the
    // target still covers what compaction holds while it works.
    const auto floored = check(1024, 0.001);
    BOOST_REQUIRE_GT(floored.low, static_cast<uint64_t>(std::ceil(1024 * 0.001)));
    BOOST_REQUIRE_GE(floored.low, 2 * min_segments_per_compaction);

    // On a disk too small for even that, the floor is itself capped, so it cannot claim an
    // unreasonable share of the disk.
    const auto tiny = check(32, 0.05);
    BOOST_REQUIRE_LE(tiny.low, 32u / 4);

    // Zero disables the trigger, and so does anything out of range: the threshold is live-updatable
    // with no range check at the config layer, and a negative value must not underflow into a target
    // that swallows the disk.
    for (double fraction : {0.0, -0.05, -1.0, std::nan(""), -std::numeric_limits<double>::infinity()}) {
        BOOST_TEST_CONTEXT("fraction=" << fraction) {
            const auto disabled = make_free_segment_watermarks(100000, fraction);
            BOOST_REQUIRE_EQUAL(disabled.low, 0u);
            BOOST_REQUIRE_EQUAL(disabled.high, 0u);
        }
    }

    // A fraction at or above 1 asks for the whole disk, which is as far as it can go.
    const auto over_one = check(100000, 1.5);
    BOOST_REQUIRE_EQUAL(over_one.low, 100000u);
    BOOST_REQUIRE_EQUAL(over_one.high, 100000u);
}

// Checks that compaction limits enforce that concurrent jobs don't exceed the free-segment target,
// preventing deadlock, with batch size shrinking first and parallelism second as target shrinks.
SEASTAR_THREAD_TEST_CASE(test_logstor_compaction_limits) {
    constexpr size_t max_batch_cap = 32;

    // A compaction job frees its inputs only when it is done, so what all the jobs in flight can
    // hold at once has to fit in the free-segment target. This is the relation the two limits exist
    // to maintain, and it has to survive any combination of disk size, threshold and configured cap.
    for (uint64_t segment_count : {32u, 128u, 256u, 1024u, 4096u, 100000u}) {
        for (double fraction : {0.0, 0.01, 0.05, 0.2, 1.0}) {
            for (size_t cap : {1u, 8u, 32u, 1024u}) {
                const auto watermarks = make_free_segment_watermarks(segment_count, fraction);
                const auto limits = make_compaction_limits(watermarks, cap);
                BOOST_TEST_CONTEXT("segment_count=" << segment_count << " fraction=" << fraction << " cap=" << cap) {
                    BOOST_REQUIRE_GE(limits.auto_parallelism, 1u);
                    BOOST_REQUIRE_LE(limits.auto_parallelism, max_auto_compaction_parallelism);
                    // A smaller batch could not reclaim anything, so it would never be worth running.
                    BOOST_REQUIRE_GE(limits.batch_cap, min_segments_per_compaction);
                    BOOST_REQUIRE_LE(limits.batch_cap, std::max(cap, min_segments_per_compaction));
                    // Except on a disk whose target is below a single batch, where the target has
                    // already been capped by the disk size and there is nothing left to give.
                    if (watermarks.low >= min_segments_per_compaction) {
                        BOOST_REQUIRE_LE(limits.auto_parallelism * limits.batch_cap, watermarks.low);
                    }
                }
            }
        }
    }

    // A disk large enough for the target to cover every job in flight gets the configured batch
    // bound and the full parallelism.
    const auto large = make_compaction_limits(make_free_segment_watermarks(100000, 0.05), max_batch_cap);
    BOOST_REQUIRE_EQUAL(large.auto_parallelism, max_auto_compaction_parallelism);
    BOOST_REQUIRE_EQUAL(large.batch_cap, max_batch_cap);

    // A disabled trigger leaves no target to protect and no automatic compaction to protect it
    // from, but compaction submitted explicitly still needs a batch it can reclaim with.
    const auto no_trigger = make_compaction_limits(make_free_segment_watermarks(100000, 0.0), max_batch_cap);
    BOOST_REQUIRE_EQUAL(no_trigger.auto_parallelism, 1u);
    BOOST_REQUIRE_EQUAL(no_trigger.batch_cap, min_segments_per_compaction);

    // A configured bound below the smallest useful batch would leave every batch unable to reclaim,
    // so it is raised rather than honoured.
    const auto tiny_bound = make_compaction_limits(make_free_segment_watermarks(100000, 0.05), 1);
    BOOST_REQUIRE_EQUAL(tiny_bound.batch_cap, min_segments_per_compaction);
}

// Verifies that compaction shares pressure scales with free space, stays monotone and within [0, 1],
// and places the target at the optimal control point regardless of disk size or degenerate watermarks.
SEASTAR_THREAD_TEST_CASE(test_logstor_compaction_shares_pressure) {
    for (uint64_t segment_count : {128u, 4000u, 100000u}) {
        for (double fraction : {0.05, 0.2}) {
            const auto watermarks = make_free_segment_watermarks(segment_count, fraction);
            BOOST_TEST_CONTEXT("segment_count=" << segment_count << " fraction=" << fraction) {
                // At and above the watermark where automatic compaction stops there is no space
                // demand at all, so the controller asks for no more than its floor.
                BOOST_REQUIRE_EQUAL(compaction_shares_pressure(segment_count, watermarks), 0.0f);
                BOOST_REQUIRE_EQUAL(compaction_shares_pressure(watermarks.high, watermarks), 0.0f);
                BOOST_REQUIRE_EQUAL(compaction_shares_pressure(0, watermarks), 1.0f);

                // The free-segment target is the intended steady-state operating point, and the
                // relative hysteresis puts it a third of the way up the ramp on any disk size -
                // which is where the shares controller puts its middle control point. Only up to
                // the rounding of the two watermarks to whole segments, hence the tolerance.
                BOOST_REQUIRE_CLOSE(compaction_shares_pressure(watermarks.low, watermarks),
                        compaction_shares_pressure_at_target, 10.0);
            }
        }
    }

    // A disabled trigger has no free-segment target, hence no space-driven demand for shares, even
    // with the disk fully consumed.
    BOOST_REQUIRE_EQUAL(compaction_shares_pressure(0, make_free_segment_watermarks(4000, 0.0)), 0.0f);

    // A one-segment target degenerates to a zero saturation point; pressure must stay in range and
    // still reach 1 rather than dividing by zero.
    const free_segment_watermarks minimal{.low = 1, .high = 2};
    BOOST_REQUIRE_EQUAL(compaction_shares_pressure(2, minimal), 0.0f);
    BOOST_REQUIRE_GT(compaction_shares_pressure(1, minimal), 0.0f);
    BOOST_REQUIRE_LT(compaction_shares_pressure(1, minimal), 1.0f);
    BOOST_REQUIRE_EQUAL(compaction_shares_pressure(0, minimal), 1.0f);
}

SEASTAR_THREAD_TEST_CASE(test_logstor_compaction_candidate_score_ranks_by_efficiency) {
    constexpr uint64_t segment_size = 128 * 1024;

    // Rewriting 8 segments into 6 reclaims two segments, but copies 5.28 segments worth of live
    // data to do it - a marginal write amplification of 2.6. Rewriting the emptiest 3 into 2
    // reclaims only one segment, for 1.98 segments copied. The efficiency rule prefers the latter.
    const compaction_candidate_score big{.n_in = 8, .n_out = 6, .live_bytes = 528 * segment_size / 100};
    const compaction_candidate_score cheap{.n_in = 3, .n_out = 2, .live_bytes = 198 * segment_size / 100};
    BOOST_REQUIRE_EQUAL(big.reclaimed(), 2u);
    BOOST_REQUIRE_EQUAL(cheap.reclaimed(), 1u);
    BOOST_REQUIRE_LT(big.efficiency(segment_size), cheap.efficiency(segment_size));
    BOOST_REQUIRE(big < cheap);

    // Efficiency is the reciprocal of the batch's marginal write amplification.
    BOOST_REQUIRE_CLOSE(cheap.efficiency(segment_size), 1.0 / 1.98, 0.1);

    // Equal efficiency is broken by reclaiming more per job.
    const compaction_candidate_score two_in{.n_in = 2, .n_out = 1, .live_bytes = segment_size};
    const compaction_candidate_score four_in{.n_in = 4, .n_out = 2, .live_bytes = 2 * segment_size};
    BOOST_REQUIRE_EQUAL(two_in.efficiency(segment_size), four_in.efficiency(segment_size));
    BOOST_REQUIRE(two_in < four_in);

    // A batch of fully dead segments copies nothing and outranks any batch that copies.
    const compaction_candidate_score all_dead{.n_in = 2, .n_out = 0, .live_bytes = 0};
    BOOST_REQUIRE(cheap < all_dead);
    BOOST_REQUIRE(four_in < all_dead);
}

// Checks that compaction candidate selection chooses segments in ascending utilization order,
// respects the batch cap, and the returned score accurately describes the selected segments.
SEASTAR_THREAD_TEST_CASE(test_logstor_select_compaction_batch) {
    constexpr uint64_t segment_size = 128 * 1024;
    constexpr size_t record_size = 1024;
    constexpr size_t records_per_segment = segment_size / record_size;
    // About a tenth live, so that a few of them rewrite into a single segment.
    constexpr size_t sparse_records = records_per_segment / 10;
    // A batch of n segments reclaims only when its mean utilization is below 1 - 1/n, so at this
    // utilization no batch that select_compaction_batch() would consider has a net gain.
    constexpr size_t dense_records = records_per_segment * 98 / 100;
    constexpr size_t sparse_count = 3;

    // Descriptors are intrusively linked into the sets, so they must keep their addresses, and every
    // set must be destroyed before them.
    std::deque<segment_descriptor> descs;
    segment_set segments;
    auto add_segment = [&] (segment_set& set, size_t live_records) {
        auto& desc = descs.emplace_back();
        desc.reset(segment_size);
        desc.on_write(live_records * record_size, live_records);
        set.add_segment(desc);
    };

    // Added interleaved, so that the batch below can only be right if selection went through the
    // free-space histogram rather than the order the segments were added in.
    for (size_t i = 0; i < sparse_count; ++i) {
        add_segment(segments, dense_records);
        add_segment(segments, sparse_records);
    }

    // What the caller relies on for any batch: the segments come least utilized first, and the score
    // describes exactly the segments returned. The same score is what ranks this group against the
    // others in find_top_compaction_candidates(), so a score describing a different batch would rank
    // the group by work it is not about to do.
    auto check_batch = [&] (const compaction_batch& batch) {
        BOOST_REQUIRE(!batch.segments.empty());
        BOOST_REQUIRE_EQUAL(batch.score.n_in, batch.segments.size());
        BOOST_REQUIRE_GT(batch.score.reclaimed(), 0u);
        uint64_t live_bytes = 0;
        size_t prev_net_data_size = 0;
        for (const auto* desc : batch.segments) {
            BOOST_REQUIRE_GE(desc->net_data_size(segment_size), prev_net_data_size);
            prev_net_data_size = desc->net_data_size(segment_size);
            live_bytes += desc->net_data_size(segment_size);
        }
        BOOST_REQUIRE_EQUAL(batch.score.live_bytes, live_bytes);
    };

    auto batch = select_compaction_batch(segments, segment_size, min_segments_per_compaction);
    BOOST_REQUIRE(batch);
    check_batch(*batch);

    // Only the sparse segments are worth taking: they rewrite into a single segment, while extending
    // into an almost fully live one would cost far more than the segment it reclaims.
    BOOST_REQUIRE_EQUAL(batch->segments.size(), sparse_count);
    for (const auto* desc : batch->segments) {
        BOOST_REQUIRE_EQUAL(desc->net_data_size(segment_size), sparse_records * record_size);
    }

    // The cap bounds the candidate set, not only the prefix chosen out of it.
    auto capped = select_compaction_batch(segments, segment_size, 2);
    BOOST_REQUIRE(capped);
    check_batch(*capped);
    BOOST_REQUIRE_LE(capped->segments.size(), 2u);

    // A group whose segments are all nearly full has no batch with a net gain, which is the answer
    // for the whole group and not only for the prefix that happened to be scored.
    segment_set dense;
    for (size_t i = 0; i < min_segments_per_compaction; ++i) {
        add_segment(dense, dense_records);
    }
    BOOST_REQUIRE(!select_compaction_batch(dense, segment_size, min_segments_per_compaction));

    // An empty group has nothing to compact.
    segment_set empty;
    BOOST_REQUIRE(!select_compaction_batch(empty, segment_size, min_segments_per_compaction));
}

SEASTAR_THREAD_TEST_CASE(test_logstor_group_compaction_rewrites_live_records) {
    auto schema = make_kv_schema();
    tmpdir dir;

    shared_logstor_cache cache;
    logstor ls(make_test_logstor_config(dir.path()), cache.shared_tracker);
    ls.do_recovery_for_test().get();
    ls.start().get();
    auto stop_store = seastar::defer([&ls] noexcept { ls.stop().get(); });

    test_compaction_group_handle cg(schema, ls);
    auto setup_guard = std::make_optional(ls.get_compaction_manager().disable_compaction(cg).get());

    auto pk0_v0 = make_kv_mutation(schema, "pk0", "v0", api::timestamp_type(1));
    auto pk1_v0 = make_kv_mutation(schema, "pk1", "v1", api::timestamp_type(2));
    auto pk2_v0 = make_kv_mutation(schema, "pk2", "v2", api::timestamp_type(3));
    auto pk0_v1 = make_kv_mutation(schema, "pk0", "v0-new", api::timestamp_type(4));
    auto pk1_v1 = make_kv_mutation(schema, "pk1", "v1-new", api::timestamp_type(5));

    write_and_flush_segment(ls, cg, pk0_v0);
    write_and_flush_segment(ls, cg, pk1_v0);
    write_and_flush_segment(ls, cg, pk2_v0);

    const auto pk0 = make_index_key(*schema, pk0_v1.decorated_key());
    const auto pk1 = make_index_key(*schema, pk1_v1.decorated_key());
    const auto pk2 = make_index_key(*schema, pk2_v0.decorated_key());

    auto stale_pk0_location = cg.logstor_index().get(pk0);
    auto stale_pk1_location = cg.logstor_index().get(pk1);

    BOOST_REQUIRE(stale_pk0_location);
    BOOST_REQUIRE(stale_pk1_location);

    write_and_flush_segment(ls, cg, pk0_v1);
    write_and_flush_segment(ls, cg, pk1_v1);

    BOOST_REQUIRE_EQUAL(cg.logstor_segments().segment_count(), 5u);

    auto live_pk0_before = cg.logstor_index().get(pk0);
    auto live_pk1_before = cg.logstor_index().get(pk1);
    auto live_pk2_before = cg.logstor_index().get(pk2);

    BOOST_REQUIRE(live_pk0_before);
    BOOST_REQUIRE(live_pk1_before);
    BOOST_REQUIRE(live_pk2_before);

    auto old_snapshot = ls.get_segment_manager().make_snapshot(cg).get();
    BOOST_REQUIRE_EQUAL(old_snapshot.size(), 5u);
    const auto old_segment_ids = snapshot_segment_ids(old_snapshot);

    setup_guard.reset();
    ls.get_compaction_manager().submit(cg);
    auto compaction_guard = ls.get_compaction_manager().disable_compaction(cg).get();

    auto new_snapshot = ls.get_segment_manager().make_snapshot(cg).get();
    BOOST_REQUIRE_EQUAL(new_snapshot.size(), 1u);
    const auto new_segment_ids = snapshot_segment_ids(new_snapshot);

    auto live_pk0_after = cg.logstor_index().get(pk0);
    auto live_pk1_after = cg.logstor_index().get(pk1);
    auto live_pk2_after = cg.logstor_index().get(pk2);

    BOOST_REQUIRE(live_pk0_after);
    BOOST_REQUIRE(live_pk1_after);
    BOOST_REQUIRE(live_pk2_after);

    BOOST_REQUIRE(live_pk0_after->location != live_pk0_before->location);
    BOOST_REQUIRE(live_pk1_after->location != live_pk1_before->location);
    BOOST_REQUIRE(live_pk2_after->location != live_pk2_before->location);

    BOOST_REQUIRE(new_segment_ids.contains(live_pk0_after->location.segment));
    BOOST_REQUIRE(new_segment_ids.contains(live_pk1_after->location.segment));
    BOOST_REQUIRE(new_segment_ids.contains(live_pk2_after->location.segment));

    for (const auto old_segment_id : old_segment_ids) {
        BOOST_REQUIRE(!new_segment_ids.contains(old_segment_id));
    }

    BOOST_REQUIRE(!new_segment_ids.contains(stale_pk0_location->location.segment));
    BOOST_REQUIRE(!new_segment_ids.contains(stale_pk1_location->location.segment));

    auto actual_pk0 = ls.read(schema, cg.logstor_index(), pk0_v1.decorated_key(), schema->full_slice()).get();
    auto actual_pk1 = ls.read(schema, cg.logstor_index(), pk1_v1.decorated_key(), schema->full_slice()).get();
    auto actual_pk2 = ls.read(schema, cg.logstor_index(), pk2_v0.decorated_key(), schema->full_slice()).get();

    BOOST_REQUIRE(actual_pk0);
    BOOST_REQUIRE(actual_pk1);
    BOOST_REQUIRE(actual_pk2);

    assert_that(*actual_pk0).is_equal_to(pk0_v1);
    assert_that(*actual_pk1).is_equal_to(pk1_v1);
    assert_that(*actual_pk2).is_equal_to(pk2_v0);

    // Scan all segments after compaction and verify they contain exactly the live records.
    // Live records (pk0_v1 ts=4, pk1_v1 ts=5, pk2_v0 ts=3) must appear exactly once;
    // overwritten records (pk0_v0 ts=1, pk1_v0 ts=2) must not appear at all.
    auto record_counts = count_records_by_timestamp(ls, new_snapshot);

    // Each live record appears exactly once.
    BOOST_REQUIRE_EQUAL(record_counts[api::timestamp_type(3)], 1u); // pk2_v0 - untouched
    BOOST_REQUIRE_EQUAL(record_counts[api::timestamp_type(4)], 1u); // pk0_v1 - latest version
    BOOST_REQUIRE_EQUAL(record_counts[api::timestamp_type(5)], 1u); // pk1_v1 - latest version

    // Overwritten records do not appear.
    BOOST_REQUIRE_EQUAL(record_counts.count(api::timestamp_type(1)), 0u); // pk0_v0 - stale
    BOOST_REQUIRE_EQUAL(record_counts.count(api::timestamp_type(2)), 0u); // pk1_v0 - stale
}

SEASTAR_THREAD_TEST_CASE(test_logstor_disabled_group_does_not_compact_on_submit) {
    auto schema = make_kv_schema();
    tmpdir dir;

    shared_logstor_cache cache;
    logstor ls(make_test_logstor_config(dir.path()), cache.shared_tracker);
    ls.do_recovery_for_test().get();
    ls.start().get();
    auto stop_store = seastar::defer([&ls] noexcept { ls.stop().get(); });

    test_compaction_group_handle cg(schema, ls);
    auto compaction_guard = ls.get_compaction_manager().disable_compaction(cg).get();

    auto pk0_v0 = make_kv_mutation(schema, "pk0", "v0", api::timestamp_type(1));
    auto pk1_v0 = make_kv_mutation(schema, "pk1", "v1", api::timestamp_type(2));
    auto pk2_v0 = make_kv_mutation(schema, "pk2", "v2", api::timestamp_type(3));
    auto pk0_v1 = make_kv_mutation(schema, "pk0", "v0-new", api::timestamp_type(4));
    auto pk1_v1 = make_kv_mutation(schema, "pk1", "v1-new", api::timestamp_type(5));

    write_and_flush_segment(ls, cg, pk0_v0);
    write_and_flush_segment(ls, cg, pk1_v0);
    write_and_flush_segment(ls, cg, pk2_v0);
    write_and_flush_segment(ls, cg, pk0_v1);
    write_and_flush_segment(ls, cg, pk1_v1);

    BOOST_REQUIRE_EQUAL(cg.logstor_segments().segment_count(), 5u);

    const auto pk0 = make_index_key(*schema, pk0_v1.decorated_key());
    const auto pk1 = make_index_key(*schema, pk1_v1.decorated_key());
    const auto pk2 = make_index_key(*schema, pk2_v0.decorated_key());

    auto live_pk0_before = cg.logstor_index().get(pk0);
    auto live_pk1_before = cg.logstor_index().get(pk1);
    auto live_pk2_before = cg.logstor_index().get(pk2);

    BOOST_REQUIRE(live_pk0_before);
    BOOST_REQUIRE(live_pk1_before);
    BOOST_REQUIRE(live_pk2_before);

    auto snapshot_before = ls.get_segment_manager().make_snapshot(cg).get();
    const auto segment_ids_before = snapshot_segment_ids(snapshot_before);

    ls.get_compaction_manager().submit(cg);

    auto snapshot_after = ls.get_segment_manager().make_snapshot(cg).get();
    const auto segment_ids_after = snapshot_segment_ids(snapshot_after);

    BOOST_REQUIRE_EQUAL(snapshot_after.size(), snapshot_before.size());
    BOOST_REQUIRE(segment_ids_after == segment_ids_before);

    auto live_pk0_after = cg.logstor_index().get(pk0);
    auto live_pk1_after = cg.logstor_index().get(pk1);
    auto live_pk2_after = cg.logstor_index().get(pk2);

    BOOST_REQUIRE(live_pk0_after);
    BOOST_REQUIRE(live_pk1_after);
    BOOST_REQUIRE(live_pk2_after);

    BOOST_REQUIRE(live_pk0_after->location == live_pk0_before->location);
    BOOST_REQUIRE(live_pk1_after->location == live_pk1_before->location);
    BOOST_REQUIRE(live_pk2_after->location == live_pk2_before->location);

    auto actual_pk0 = ls.read(schema, cg.logstor_index(), pk0_v1.decorated_key(), schema->full_slice()).get();
    auto actual_pk1 = ls.read(schema, cg.logstor_index(), pk1_v1.decorated_key(), schema->full_slice()).get();
    auto actual_pk2 = ls.read(schema, cg.logstor_index(), pk2_v0.decorated_key(), schema->full_slice()).get();

    BOOST_REQUIRE(actual_pk0);
    BOOST_REQUIRE(actual_pk1);
    BOOST_REQUIRE(actual_pk2);

    assert_that(*actual_pk0).is_equal_to(pk0_v1);
    assert_that(*actual_pk1).is_equal_to(pk1_v1);
    assert_that(*actual_pk2).is_equal_to(pk2_v0);
}

// Split compaction hands every segment of a group to the group its records belong to after the
// split: a segment whose records are all on one side is moved as it is, and one that straddles the
// split is rewritten into a segment per side, skipping the records that are no longer live.
SEASTAR_THREAD_TEST_CASE(test_logstor_split_compaction_splits_segments_between_target_groups) {
    auto schema = make_kv_schema();
    tmpdir dir;

    shared_logstor_cache cache;
    logstor ls(make_test_logstor_config(dir.path()), cache.shared_tracker);
    ls.do_recovery_for_test().get();
    ls.start().get();
    auto stop_store = seastar::defer([&ls] noexcept { ls.stop().get(); });

    // The src and target groups in a split share the logstor index.
    test_compaction_group_handle src(schema, ls);
    test_compaction_group_handle left(schema, ls, src.logstor_index());
    test_compaction_group_handle right(schema, ls, src.logstor_index());
    auto& index = src.logstor_index();
    auto setup_guard = std::make_optional(ls.get_compaction_manager().disable_compaction(src).get());

    // Keys in token order, split in the middle: keys[0..2] go to the left group, keys[3..5] to the
    // right one.
    const auto keys = make_token_ordered_keys(schema, 6);
    const auto boundary = dht::decorate_key(*schema, partition_key::from_single_value(*schema, serialized(keys[2]))).token();
    auto classify = [boundary] (dht::token t) -> mutation_writer::token_group_id {
        return t <= boundary ? 0 : 1;
    };

    auto k0 = make_kv_mutation(schema, keys[0], "v0", api::timestamp_type(1));
    auto k1 = make_kv_mutation(schema, keys[1], "v1", api::timestamp_type(2));
    auto k2_v0 = make_kv_mutation(schema, keys[2], "v2", api::timestamp_type(3));
    auto k3 = make_kv_mutation(schema, keys[3], "v3", api::timestamp_type(4));
    auto k4 = make_kv_mutation(schema, keys[4], "v4", api::timestamp_type(5));
    auto k5 = make_kv_mutation(schema, keys[5], "v5", api::timestamp_type(6));
    auto k2_v1 = make_kv_mutation(schema, keys[2], "v2-new", api::timestamp_type(7));

    // One segment on each side of the split, one that straddles it, and one that overwrites a
    // record of the straddling segment, so that the split also has a dead record to skip.
    write_and_flush_segment(ls, src, k0);
    const std::array right_only = {k4, k5};
    write_and_flush_segment(ls, src, right_only);
    const std::array straddling = {k1, k2_v0, k3};
    write_and_flush_segment(ls, src, straddling);
    write_and_flush_segment(ls, src, k2_v1);

    BOOST_REQUIRE_EQUAL(src.logstor_segments().segment_count(), 4u);

    std::vector<primary_index_key> index_keys;
    for (const auto& m : {k0, k1, k2_v1, k3, k4, k5}) {
        index_keys.push_back(make_index_key(*schema, m.decorated_key()));
    }

    std::vector<log_location> locations_before;
    for (const auto& key : index_keys) {
        auto entry = index.get(key);
        BOOST_REQUIRE(entry);
        locations_before.push_back(entry->location);
    }

    const auto single_sided_segments = std::set<log_segment_id>{
        locations_before[0].segment, // k0
        locations_before[2].segment, // k2_v1
        locations_before[4].segment, // k4, k5
    };
    const auto straddling_segment = locations_before[1].segment; // k1, k2_v0, k3
    BOOST_REQUIRE(!single_sided_segments.contains(straddling_segment));
    BOOST_REQUIRE(locations_before[4].segment == locations_before[5].segment);
    BOOST_REQUIRE(locations_before[3].segment == straddling_segment);

    setup_guard.reset();
    ls.get_compaction_manager().submit_split_compaction(src, classify,
            [&] (log_segment_id, dht::token first_token, dht::token last_token) -> logstor_group& {
                // Split compaction only asks about segments it has decided are on a single side.
                BOOST_REQUIRE_EQUAL(classify(first_token), classify(last_token));
                return classify(first_token) == 0 ? static_cast<logstor_group&>(left) : right;
            }).get();

    // Every segment of the group being split ends up in one of the target groups.
    BOOST_REQUIRE_EQUAL(src.logstor_segments().segment_count(), 0u);

    auto left_snapshot = ls.get_segment_manager().make_snapshot(left).get();
    auto right_snapshot = ls.get_segment_manager().make_snapshot(right).get();
    const auto left_ids = snapshot_segment_ids(left_snapshot);
    const auto right_ids = snapshot_segment_ids(right_snapshot);

    std::vector<log_location> locations_after;
    for (const auto& key : index_keys) {
        auto entry = index.get(key);
        BOOST_REQUIRE(entry);
        locations_after.push_back(entry->location);
    }

    // The single-sided segments were moved as they are: the records they hold did not move, and
    // their segments are now owned by the group of their side.
    BOOST_REQUIRE(locations_after[0] == locations_before[0]);
    BOOST_REQUIRE(locations_after[2] == locations_before[2]);
    BOOST_REQUIRE(locations_after[4] == locations_before[4]);
    BOOST_REQUIRE(locations_after[5] == locations_before[5]);
    BOOST_REQUIRE(left_ids.contains(locations_after[0].segment));
    BOOST_REQUIRE(left_ids.contains(locations_after[2].segment));
    BOOST_REQUIRE(right_ids.contains(locations_after[4].segment));

    // The straddling segment was rewritten into a new segment per side and freed.
    BOOST_REQUIRE(locations_after[1] != locations_before[1]);
    BOOST_REQUIRE(locations_after[3] != locations_before[3]);
    BOOST_REQUIRE(left_ids.contains(locations_after[1].segment));
    BOOST_REQUIRE(right_ids.contains(locations_after[3].segment));
    BOOST_REQUIRE(!left_ids.contains(straddling_segment));
    BOOST_REQUIRE(!right_ids.contains(straddling_segment));

    // All the records are readable, from whichever group they ended up in.
    for (const auto& expected : {k0, k1, k2_v1, k3, k4, k5}) {
        auto actual = ls.read(schema, index, expected.decorated_key(), schema->full_slice()).get();
        BOOST_REQUIRE(actual);
        assert_that(*actual).is_equal_to(expected);
    }

    // Each live record is held exactly once, by the group of its side, and the record the overwrite
    // of k2 made dead was not rewritten.
    auto left_records = count_records_by_timestamp(ls, left_snapshot);
    auto right_records = count_records_by_timestamp(ls, right_snapshot);

    const auto expected_left = std::map<api::timestamp_type, size_t>{
        {api::timestamp_type(1), 1}, // k0
        {api::timestamp_type(2), 1}, // k1
        {api::timestamp_type(7), 1}, // k2_v1
    };
    const auto expected_right = std::map<api::timestamp_type, size_t>{
        {api::timestamp_type(4), 1}, // k3
        {api::timestamp_type(5), 1}, // k4
        {api::timestamp_type(6), 1}, // k5
    };
    BOOST_REQUIRE(left_records == expected_left);
    BOOST_REQUIRE(right_records == expected_right);
}

// Everything a reader test needs: a started logstor holding a single compaction group, and a
// semaphore to take read permits from. Tests fill the group with populate() and read it back
// through make_reader().
struct reader_test_env {
    schema_ptr schema{make_kv_schema()};
    tmpdir dir;
    shared_logstor_cache cache;
    logstor ls{make_test_logstor_config(dir.path()), cache.shared_tracker};
    std::optional<test_compaction_group_handle> cg;
    tests::reader_concurrency_semaphore_wrapper semaphore;

    reader_test_env() {
        ls.do_recovery_for_test().get();
        ls.start().get();
        cg.emplace(schema, ls);
    }

    ~reader_test_env() {
        cg.reset();
        ls.stop().get();
    }

    void populate(std::span<const mutation> ms) {
        write_and_flush_segment(ls, *cg, ms);
    }

    mutation_reader make_reader(const dht::partition_range& pr,
            streamed_mutation::forwarding fwd = streamed_mutation::forwarding::no,
            mutation_reader::forwarding fwd_mr = mutation_reader::forwarding::no) {
        return ls.make_reader(schema, cg->logstor_index(), semaphore.make_permit(), pr,
                schema->full_slice(), nullptr, fwd, fwd_mr);
    }
};

// Distinct key-value mutations in token order, which is also the order a reader produces them in.
std::vector<mutation> make_token_ordered_mutations(schema_ptr schema, size_t count) {
    std::vector<mutation> ms;
    for (const auto& key : make_token_ordered_keys(schema, count)) {
        ms.push_back(make_kv_mutation(schema, key, "value-" + key, api::timestamp_type(1)));
    }
    return ms;
}

dht::partition_range singular_range(const mutation& m) {
    return dht::partition_range::make_singular(m.decorated_key());
}

// The range between two mutations, excluding both.
dht::partition_range range_between(const mutation& lower, const mutation& upper) {
    using bound = dht::partition_range::bound;
    return dht::partition_range(bound(dht::ring_position(lower.decorated_key()), false),
            bound(dht::ring_position(upper.decorated_key()), false));
}

// Both readers fill their buffer by draining a reader made for the partition they are on, so that
// reader has to be bounded by the buffer size the caller asked for. Without that, a caller that
// shrank its buffer is served the partition reader's own, much larger, default.
SEASTAR_THREAD_TEST_CASE(test_logstor_readers_honor_max_buffer_size) {
    reader_test_env env;
    auto ms = make_token_ordered_mutations(env.schema, 2);
    env.populate(ms);

    // Fills the reader with the smallest buffer bound there is and tells whether it stopped short
    // of the end of the partition it was on. An unbounded partition reader hands over the whole
    // partition, its end included, in one go.
    auto stops_inside_the_partition = [] (mutation_reader rd) {
        rd.set_max_buffer_size(1);
        rd.fill_buffer().get();
        bool saw_partition_end = false;
        while (!rd.is_buffer_empty()) {
            saw_partition_end |= rd.pop_mutation_fragment().is_end_of_partition();
        }
        rd.close().get();
        return !saw_partition_end;
    };

    BOOST_REQUIRE(stops_inside_the_partition(env.make_reader(singular_range(ms[0]))));
    BOOST_REQUIRE(stops_inside_the_partition(env.make_reader(query::full_partition_range)));
}

// Checks that a reader over a singular partition range honors the mutation_reader
// contract: next_partition() called before the partition was entered must not skip
// it, a caller that may fast-forward the reader is served the following ranges, and
// a forwarding reader produces a partition in clustering sub-streams.
SEASTAR_THREAD_TEST_CASE(test_logstor_single_key_reader_partition_navigation) {
    reader_test_env env;
    auto ms = make_token_ordered_mutations(env.schema, 2);
    env.populate(ms);

    const auto& first = ms[0];
    const auto& second = ms[1];

    assert_that(env.make_reader(singular_range(first)))
            .produces(first)
            .produces_end_of_stream();

    // next_partition() before the partition was entered must not skip it.
    assert_that(env.make_reader(singular_range(first)))
            .next_partition()
            .produces_partition_start(first.decorated_key())
            .next_partition()
            .produces_end_of_stream();

    // A reader that may be fast-forwarded has to serve the ranges it is moved to, so a
    // single-partition reader must not be used for it: make_reader() hands out the range
    // reader instead, which serves a singular range too.
    assert_that(env.make_reader(singular_range(first), streamed_mutation::forwarding::no, mutation_reader::forwarding::yes))
            .produces(first)
            .produces_end_of_stream()
            .fast_forward_to(singular_range(second))
            .produces(second)
            .produces_end_of_stream();

    // In forwarding mode the partition is produced in clustering sub-streams.
    assert_that(env.make_reader(singular_range(first), streamed_mutation::forwarding::yes))
            .produces_partition_start(first.decorated_key())
            .produces_end_of_stream()
            .fast_forward_to(position_range::all_clustered_rows())
            .produces_row_with_key(clustering_key::make_empty())
            .produces_end_of_stream();
}

// Checks that next_partition() does not skip a partition when the reader's buffer ran out right
// at the end of the previous one, and that it does skip what is left of a partition the reader
// is in the middle of.
SEASTAR_THREAD_TEST_CASE(test_logstor_range_reader_next_partition_at_buffer_boundary) {
    reader_test_env env;
    auto ms = make_token_ordered_mutations(env.schema, 4);
    env.populate(ms);

    // A buffer this small holds a single fragment, so the reader runs out of buffer at every
    // fragment boundary, the ones inside a partition included.
    auto at_partition_end = env.make_reader(query::full_partition_range);
    at_partition_end.set_max_buffer_size(1);
    auto at_partition_end_assertions = assert_that(std::move(at_partition_end));
    for (const auto& m : ms) {
        at_partition_end_assertions.produces_partition_start(m.decorated_key())
                .produces_row_with_key(clustering_key::make_empty())
                .produces_partition_end()
                .next_partition();
    }
    at_partition_end_assertions.produces_end_of_stream();

    // Called with the partition entered but not consumed, next_partition() drops the rest of it
    // and moves on to the one that follows.
    auto mid_partition = env.make_reader(query::full_partition_range);
    mid_partition.set_max_buffer_size(1);
    auto mid_partition_assertions = assert_that(std::move(mid_partition));
    for (const auto& m : ms) {
        mid_partition_assertions.produces_partition_start(m.decorated_key())
                .next_partition();
    }
    mid_partition_assertions.produces_end_of_stream();
}

// Checks that the range reader serves the bounds of the partition range it was given. It scans
// the index by token, with both ends of the token range inclusive, so keys that fall outside the
// range have to be filtered out by key once their records were read.
SEASTAR_THREAD_TEST_CASE(test_logstor_range_reader_honors_partition_range_bounds) {
    using bound = dht::partition_range::bound;

    reader_test_env env;
    auto ms = make_token_ordered_mutations(env.schema, 5);
    env.populate(ms);

    auto pos = [] (const mutation& m) { return dht::ring_position(m.decorated_key()); };

    assert_that(env.make_reader(query::full_partition_range))
            .produces(ms)
            .produces_end_of_stream();

    // [1, 3]
    assert_that(env.make_reader(dht::partition_range(bound(pos(ms[1]), true), bound(pos(ms[3]), true))))
            .produces(ms[1])
            .produces(ms[2])
            .produces(ms[3])
            .produces_end_of_stream();

    // (1, 3)
    assert_that(env.make_reader(dht::partition_range(bound(pos(ms[1]), false), bound(pos(ms[3]), false))))
            .produces(ms[2])
            .produces_end_of_stream();

    // (-inf, 1]
    assert_that(env.make_reader(dht::partition_range(std::nullopt, bound(pos(ms[1]), true))))
            .produces(ms[0])
            .produces(ms[1])
            .produces_end_of_stream();

    // [3, +inf)
    assert_that(env.make_reader(dht::partition_range(bound(pos(ms[3]), true), std::nullopt)))
            .produces(ms[3])
            .produces(ms[4])
            .produces_end_of_stream();

    // Bounds given by token only, without a key.
    assert_that(env.make_reader(dht::partition_range(
                    bound(dht::ring_position::starting_at(ms[2].token()), true),
                    bound(dht::ring_position::ending_at(ms[3].token()), true))))
            .produces(ms[2])
            .produces(ms[3])
            .produces_end_of_stream();
}

// Checks that a reader that may be fast-forwarded serves every range it is moved to, including
// ranges that hold nothing and ranges it is moved to before the current partition was consumed.
SEASTAR_THREAD_TEST_CASE(test_logstor_range_reader_fast_forward_to) {
    reader_test_env env;
    auto ms = make_token_ordered_mutations(env.schema, 5);
    env.populate(ms);

    auto make_forwardable_reader = [&] {
        return env.make_reader(singular_range(ms[0]), streamed_mutation::forwarding::no,
                mutation_reader::forwarding::yes);
    };

    assert_that(make_forwardable_reader())
            .produces(ms[0])
            .produces_end_of_stream()
            .fast_forward_to(singular_range(ms[2]))
            .produces(ms[2])
            .produces_end_of_stream()
            .fast_forward_to(singular_range(ms[4]))
            .produces(ms[4])
            .produces_end_of_stream();

    // A range with nothing in it is served as an empty stream, and does not end the reader:
    // it still serves the range it is moved to next.
    assert_that(make_forwardable_reader())
            .produces(ms[0])
            .produces_end_of_stream()
            .fast_forward_to(range_between(ms[1], ms[2]))
            .produces_end_of_stream()
            .fast_forward_to(singular_range(ms[3]))
            .produces(ms[3])
            .produces_end_of_stream();

    // Forwarding out of a partition that was entered but not consumed.
    auto mid_partition = make_forwardable_reader();
    mid_partition.set_max_buffer_size(1);
    assert_that(std::move(mid_partition))
            .produces_partition_start(ms[0].decorated_key())
            .fast_forward_to(singular_range(ms[1]))
            .produces(ms[1])
            .produces_end_of_stream();
}

// The index scan hands the reader one batch of entries at a time, so a scan of more partitions
// than a batch holds has to carry on across batches without dropping, repeating or reordering a
// partition.
SEASTAR_THREAD_TEST_CASE(test_logstor_range_reader_scans_across_index_batches) {
    reader_test_env env;
    // Comfortably more than the reader's read-ahead, so the scan takes several batches.
    auto ms = make_token_ordered_mutations(env.schema, 25);
    env.populate(ms);

    assert_that(env.make_reader(query::full_partition_range))
            .produces(ms)
            .produces_end_of_stream();

    assert_that(env.make_reader(query::full_partition_range)).has_monotonic_positions();
}

// Checks that a range reader in forwarding mode produces every partition in clustering
// sub-streams, and that next_partition() is what moves it to the next partition.
SEASTAR_THREAD_TEST_CASE(test_logstor_range_reader_streamed_mutation_forwarding) {
    reader_test_env env;
    auto ms = make_token_ordered_mutations(env.schema, 3);
    env.populate(ms);

    auto assertions = assert_that(env.make_reader(query::full_partition_range, streamed_mutation::forwarding::yes));
    for (const auto& m : ms) {
        assertions.produces_partition_start(m.decorated_key())
                .produces_end_of_stream()
                .fast_forward_to(position_range::all_clustered_rows())
                .produces_row_with_key(clustering_key::make_empty())
                .produces_end_of_stream()
                .next_partition();
    }
    assertions.produces_end_of_stream();
}

// Checks that a range holding no key ends the stream right away, whether the key it asks for was
// never written or the range simply falls between two keys that were.
SEASTAR_THREAD_TEST_CASE(test_logstor_reader_produces_nothing_for_empty_ranges) {
    reader_test_env env;
    auto ms = make_token_ordered_mutations(env.schema, 3);
    // Everything but the middle mutation, so that its key is the one missing from a populated index.
    const std::array written = {ms[0], ms[2]};
    env.populate(written);

    assert_that(env.make_reader(singular_range(ms[1])))
            .produces_end_of_stream();

    assert_that(env.make_reader(singular_range(ms[1]), streamed_mutation::forwarding::no, mutation_reader::forwarding::yes))
            .produces_end_of_stream();

    // The keys that were written bound this range without being in it, so the reader has to read
    // their records and then filter both of them out.
    assert_that(env.make_reader(range_between(ms[0], ms[2])))
            .produces_end_of_stream();
}
