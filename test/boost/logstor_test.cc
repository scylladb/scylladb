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

#include "replica/logstor/index.hh"
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

    primary_index index(schema, accounting);

    const auto pk0 = primary_index_key{make_kv_mutation(schema, "pk0", "v0").decorated_key()};
    const auto pk1 = primary_index_key{make_kv_mutation(schema, "pk1", "v1").decorated_key()};
    const auto pk2 = primary_index_key{make_kv_mutation(schema, "pk2", "v2").decorated_key()};

    const log_location loc0{.segment = log_segment_id{1}, .offset = 0, .size = 11};
    const log_location loc0_old{.segment = log_segment_id{1}, .offset = 16, .size = 7};
    const log_location loc1{.segment = log_segment_id{2}, .offset = 0, .size = 17};
    const log_location loc2{.segment = log_segment_id{3}, .offset = 0, .size = 13};
    const log_location loc3{.segment = log_segment_id{4}, .offset = 0, .size = 19};
    const log_location loc4{.segment = log_segment_id{5}, .offset = 0, .size = 23};
    const log_location loc5{.segment = log_segment_id{6}, .offset = 0, .size = 29};

    // insert(pk0, loc0): new entry, succeeds, no previous entry to free  →  {pk0: loc0}
    auto [inserted0, prev0] = index.insert(pk0, index_entry{.location = loc0, .timestamp = api::timestamp_type(10)});
    BOOST_REQUIRE(inserted0);
    BOOST_REQUIRE(!prev0);
    BOOST_REQUIRE_EQUAL(accounting.live_bytes, ssize_t(loc0.size));
    BOOST_REQUIRE_EQUAL(accounting.add_calls, 1u);
    BOOST_REQUIRE_EQUAL(accounting.free_calls, 0u);
    BOOST_REQUIRE_EQUAL(accounting.live_location_count(), 1);
    BOOST_REQUIRE(accounting.is_live(loc0));

    // insert(pk0, loc0_old): older timestamp, rejected, no accounting change  →  {pk0: loc0}
    auto [inserted_old, prev_old] = index.insert(pk0, index_entry{.location = loc0_old, .timestamp = api::timestamp_type(9)});
    BOOST_REQUIRE(!inserted_old);
    BOOST_REQUIRE(prev_old);
    BOOST_REQUIRE(prev_old->location == loc0);
    BOOST_REQUIRE_EQUAL(prev_old->timestamp, api::timestamp_type(10));
    BOOST_REQUIRE_EQUAL(accounting.live_bytes, ssize_t(loc0.size));
    BOOST_REQUIRE_EQUAL(accounting.add_calls, 1u);
    BOOST_REQUIRE_EQUAL(accounting.free_calls, 0u);
    BOOST_REQUIRE_EQUAL(accounting.live_location_count(), 1);
    BOOST_REQUIRE(accounting.is_live(loc0));

    // insert(pk0, loc1): newer timestamp, replaces loc0, old location freed via accounting  →  {pk0: loc1}
    auto [inserted1, prev1] = index.insert(pk0, index_entry{.location = loc1, .timestamp = api::timestamp_type(11)});
    BOOST_REQUIRE(inserted1);
    BOOST_REQUIRE(prev1);
    BOOST_REQUIRE(prev1->location == loc0);
    BOOST_REQUIRE_EQUAL(prev1->timestamp, api::timestamp_type(10));
    BOOST_REQUIRE_EQUAL(accounting.live_bytes, ssize_t(loc1.size));
    BOOST_REQUIRE_EQUAL(accounting.add_calls, 2u);
    BOOST_REQUIRE_EQUAL(accounting.free_calls, 1u);
    BOOST_REQUIRE_EQUAL(accounting.live_location_count(), 1);
    BOOST_REQUIRE(accounting.is_live(loc1));
    BOOST_REQUIRE(!accounting.is_live(loc0));
    BOOST_REQUIRE(accounting.added_locations.back() == loc1);
    BOOST_REQUIRE(accounting.freed_locations.back() == loc0);

    // insert(pk1, loc2): new key, succeeds, adds to live bytes  →  {pk0: loc1, pk1: loc2}
    auto [inserted2, prev2] = index.insert(pk1, index_entry{.location = loc2, .timestamp = api::timestamp_type(7)});
    BOOST_REQUIRE(inserted2);
    BOOST_REQUIRE(!prev2);
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
    auto [inserted4, prev4] = index.insert(pk1, index_entry{.location = loc4, .timestamp = api::timestamp_type(12)});
    BOOST_REQUIRE(inserted4);
    BOOST_REQUIRE(!prev4);
    // insert(pk2, loc5): new key, succeeds  →  {pk0: loc3, pk1: loc4, pk2: loc5}
    auto [inserted5, prev5] = index.insert(pk2, index_entry{.location = loc5, .timestamp = api::timestamp_type(13)});
    BOOST_REQUIRE(inserted5);
    BOOST_REQUIRE(!prev5);
    BOOST_REQUIRE_EQUAL(accounting.live_bytes, ssize_t(loc3.size + loc4.size + loc5.size));
    BOOST_REQUIRE_EQUAL(accounting.add_calls, 6u);
    BOOST_REQUIRE_EQUAL(accounting.free_calls, 3u);
    BOOST_REQUIRE_EQUAL(accounting.live_location_count(), 3);

    // range erase pk1: removes pk1 entry (loc4), frees via accounting  →  {pk0: loc3, pk2: loc5}
    index.erase(dht::partition_range::make_singular(pk1.dk)).get();
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

    primary_index index(schema, accounting);

    struct entry {
        primary_index_key key;
        log_location loc;
    };

    std::vector<entry> entries = {
        { primary_index_key{make_kv_mutation(schema, "pk0", "v0").decorated_key()}, {.segment = log_segment_id{11}, .offset = 0, .size = 5} },
        { primary_index_key{make_kv_mutation(schema, "pk1", "v1").decorated_key()}, {.segment = log_segment_id{12}, .offset = 0, .size = 7} },
        { primary_index_key{make_kv_mutation(schema, "pk2", "v2").decorated_key()}, {.segment = log_segment_id{13}, .offset = 0, .size = 11} },
        { primary_index_key{make_kv_mutation(schema, "pk3", "v3").decorated_key()}, {.segment = log_segment_id{14}, .offset = 0, .size = 13} },
        { primary_index_key{make_kv_mutation(schema, "pk4", "v4").decorated_key()}, {.segment = log_segment_id{15}, .offset = 0, .size = 17} },
    };

    std::sort(entries.begin(), entries.end(), [&] (const entry& a, const entry& b) {
        return dht::decorated_key::less_comparator(schema)(a.key.dk, b.key.dk);
    });

    auto insert = [&] (const entry& e, api::timestamp_type ts) {
        auto [inserted, prev] = index.insert(e.key, index_entry{.location = e.loc, .timestamp = ts});
        BOOST_REQUIRE(inserted);
        BOOST_REQUIRE(!prev);
    };

    insert(entries[0], api::timestamp_type(10));
    insert(entries[1], api::timestamp_type(11));
    insert(entries[2], api::timestamp_type(12));
    insert(entries[3], api::timestamp_type(13));
    insert(entries[4], api::timestamp_type(14));

    BOOST_REQUIRE_EQUAL(accounting.add_calls, 5u);
    BOOST_REQUIRE_EQUAL(accounting.free_calls, 0u);
    BOOST_REQUIRE_EQUAL(accounting.live_bytes, ssize_t(entries[0].loc.size + entries[1].loc.size + entries[2].loc.size + entries[3].loc.size + entries[4].loc.size));

    index.erase(dht::partition_range(
            dht::partition_range::bound(entries[1].key.dk, true),
            dht::partition_range::bound(entries[3].key.dk, true))).get();

    BOOST_REQUIRE_EQUAL(accounting.add_calls, 5u);
    BOOST_REQUIRE_EQUAL(accounting.free_calls, 3u);
    BOOST_REQUIRE_EQUAL(accounting.live_bytes, ssize_t(entries[0].loc.size + entries[4].loc.size));
    BOOST_REQUIRE(index.find(entries[0].key.dk) != index.end());
    BOOST_REQUIRE(index.find(entries[1].key.dk) == index.end());
    BOOST_REQUIRE(index.find(entries[2].key.dk) == index.end());
    BOOST_REQUIRE(index.find(entries[3].key.dk) == index.end());
    BOOST_REQUIRE(index.find(entries[4].key.dk) != index.end());
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
