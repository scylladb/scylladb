/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <boost/test/unit_test.hpp>

#include <algorithm>
#include <array>
#include <optional>
#include <random>
#include <span>
#include <tuple>
#include <vector>

#include <seastar/core/seastar.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>

#include "replica/logstor/index.hh"
#include "replica/logstor/logstor.hh"
#include "replica/logstor/segment_manager.hh"
#include "schema/schema_builder.hh"
#include "test/lib/logstor_test_utils.hh"
#include "test/lib/mutation_assertions.hh"
#include "test/lib/tmpdir.hh"

using namespace replica::logstor;
using namespace tests::logstor;

// Each test writes with one store, stops it, and recovers what it wrote with the next store over the
// same directory, through a catalog of its own groups. The next store runs the recovery of a node:
// do_recovery() before start().

namespace {

schema_ptr make_kv_schema(const sstring& cf) {
    return schema_builder(1, "ks", cf)
            .with_column("pk", utf8_type, column_kind::partition_key)
            .with_column("v", utf8_type)
            .build();
}

mutation make_kv_mutation(schema_ptr schema, const sstring& pk, const sstring& value, api::timestamp_type ts) {
    auto key = partition_key::from_single_value(*schema, serialized(pk));
    mutation m(schema, dht::decorate_key(*schema, key));
    auto& row = m.partition().clustered_row(*schema, clustering_key::make_empty());
    row.apply(row_marker(ts));
    const auto& v_def = *schema->get_column_definition("v");
    row.cells().apply(v_def, atomic_cell::make_live(*v_def.type, ts, serialized(value)));
    return m;
}

dht::token token_of(const schema& s, const sstring& pk) {
    return dht::decorate_key(s, partition_key::from_single_value(s, serialized(pk))).token();
}

// Returns `count` distinct keys, sorted by token, so that a test can put a group boundary between
// two of them.
std::vector<sstring> make_token_ordered_keys(schema_ptr schema, size_t count) {
    std::vector<sstring> keys;
    for (size_t i = 0; i < count; ++i) {
        keys.push_back(seastar::format("pk{}", i));
    }
    std::ranges::sort(keys, std::less<>{}, [&] (const sstring& pk) { return token_of(*schema, pk); });
    return keys;
}

// The tokens up to the boundary, and the tokens above it.
dht::token_range left_of(dht::token boundary) {
    return dht::token_range::make_ending_with({boundary, true});
}

dht::token_range right_of(dht::token boundary) {
    return dht::token_range::make_starting_with({boundary, false});
}

// A logstor over the directory of a test, with the cache it needs.
class test_store {
    shared_logstor_cache _cache;
    logstor _ls;
public:
    explicit test_store(const tmpdir& dir, logstor_params params = {})
        : _ls(make_test_logstor_config(dir.path(), params), _cache.shared_tracker) {
    }

    logstor& ls() noexcept {
        return _ls;
    }
};

void write(logstor& ls, test_logstor_group& group, const mutation& m) {
    ls.write(m, write_target(&group, {}), db::no_timeout).get();
}

// Writes the mutations and flushes the group's separator once, so that they end up in one full
// segment of the group. The mixed segment they were written to keeps a copy of them.
void write_to_full_segment(logstor& ls, test_logstor_group& group, std::span<const mutation> ms) {
    for (const auto& m : ms) {
        write(ls, group, m);
    }
    ls.flush_to_separator().get();
    group.flush_separator().get();
}

void write_to_full_segment(logstor& ls, test_logstor_group& group, const mutation& m) {
    write_to_full_segment(ls, group, std::span(&m, 1));
}

// Writes the mutation to the mixed segment only. Its copy waits in the group's separator buffer, and
// a group that is destroyed before the separator is flushed discards it, so the next recovery finds
// the record in the mixed segment only.
void write_to_mixed_segment(logstor& ls, test_logstor_group& group, const mutation& m) {
    write(ls, group, m);
    ls.flush_to_separator().get();
}

void require_value(logstor& ls, const primary_index& index, const mutation& expected) {
    auto actual = read_mutation(ls, expected.schema(), index, expected.decorated_key(), expected.schema()->full_slice()).get();
    BOOST_REQUIRE(actual);
    assert_that(*actual).is_equal_to(expected);
}

void require_no_value(logstor& ls, const primary_index& index, const mutation& m) {
    BOOST_REQUIRE(!read_mutation(ls, m.schema(), index, m.decorated_key(), m.schema()->full_slice()).get());
}

bool on_disk(const sstring& path) {
    return seastar::file_exists(path).get();
}

} // anonymous namespace

SEASTAR_THREAD_TEST_CASE(test_logstor_recovery_of_an_empty_directory) {
    tmpdir dir;
    test_store store(dir);
    auto& ls = store.ls();
    auto stop_store = seastar::defer([&ls] noexcept { ls.stop().get(); });

    test_recovery_catalog catalog;
    const auto summary = ls.do_recovery(catalog).get();
    ls.start().get();

    BOOST_REQUIRE_EQUAL(summary.found_segments, 0u);
    BOOST_REQUIRE_EQUAL(summary.used_segments, 0u);
    BOOST_REQUIRE_EQUAL(summary.free_segments, 0u);
    BOOST_REQUIRE_EQUAL(summary.dropped_records, 0u);
    BOOST_REQUIRE_EQUAL(summary.separated_segments, 0u);
    BOOST_REQUIRE_EQUAL(summary.next_segment_seq.value, 1u);
}

// The newest record of each key comes back, for two tables: an overwrite with a newer timestamp, and
// an overwrite with an equal timestamp in the same segment, which the offset orders.
SEASTAR_THREAD_TEST_CASE(test_logstor_recovery_restores_the_newest_records) {
    auto schema_a = make_kv_schema("a");
    auto schema_b = make_kv_schema("b");
    const auto a0_v0 = make_kv_mutation(schema_a, "pk0", "v0", 1);
    const auto a0_v1 = make_kv_mutation(schema_a, "pk0", "v1", 2);
    const auto a1_v0 = make_kv_mutation(schema_a, "pk1", "v0", 3);
    const auto a1_v1 = make_kv_mutation(schema_a, "pk1", "v1", 3);
    const auto b0 = make_kv_mutation(schema_b, "pk0", "v0", 1);
    tmpdir dir;

    {
        test_store store(dir);
        auto& ls = store.ls();
        test_recovery_catalog no_tables;
        ls.do_recovery(no_tables).get();
        ls.start().get();
        auto stop_store = seastar::defer([&ls] noexcept { ls.stop().get(); });
        test_logstor_group group_a(schema_a, ls);
        test_logstor_group group_b(schema_b, ls);
        auto compaction_a = ls.get_compaction_manager().disable_compaction(group_a).get();
        auto compaction_b = ls.get_compaction_manager().disable_compaction(group_b).get();

        write_to_full_segment(ls, group_a, a0_v0);
        write(ls, group_a, a0_v1);
        write(ls, group_a, a1_v0);
        write(ls, group_a, a1_v1);
        write(ls, group_b, b0);
        ls.flush_to_separator().get();
        group_a.flush_separator().get();
        group_b.flush_separator().get();
    }

    test_store store(dir);
    auto& ls = store.ls();
    auto stop_store = seastar::defer([&ls] noexcept { ls.stop().get(); });
    test_logstor_group group_a(schema_a, ls);
    test_logstor_group group_b(schema_b, ls);
    test_recovery_catalog catalog;
    catalog.add(group_a);
    catalog.add(group_b);

    const auto summary = ls.do_recovery(catalog).get();
    ls.start().get();

    // The first full segment of group_a holds only a0_v0, which a0_v1 replaced, and the mixed
    // segment holds only copies that the full segments replaced.
    BOOST_REQUIRE_EQUAL(summary.used_segments, 2u);
    BOOST_REQUIRE_EQUAL(summary.free_segments + summary.used_segments, summary.found_segments);
    BOOST_REQUIRE_EQUAL(summary.dropped_records, 0u);
    BOOST_REQUIRE_EQUAL(summary.separated_segments, 0u);
    BOOST_REQUIRE_EQUAL(group_a.logstor_segments().segment_count(), 1u);
    BOOST_REQUIRE_EQUAL(group_b.logstor_segments().segment_count(), 1u);
    BOOST_REQUIRE_EQUAL(group_a.logstor_index().get_key_count(), 2u);
    BOOST_REQUIRE_EQUAL(group_b.logstor_index().get_key_count(), 1u);
    require_value(ls, group_a.logstor_index(), a0_v1);
    require_value(ls, group_a.logstor_index(), a1_v1);
    require_value(ls, group_b.logstor_index(), b0);
}

// A full segment goes back whole to the group that owns its range. The records of a mixed segment
// go through the separator to the group of each record.
SEASTAR_THREAD_TEST_CASE(test_logstor_recovery_places_segments_by_their_kind) {
    auto schema = make_kv_schema("cf");
    const auto keys = make_token_ordered_keys(schema, 4);
    const auto boundary = token_of(*schema, keys[1]);
    const auto full_left = make_kv_mutation(schema, keys[0], "v", 1);
    const auto mixed_left = make_kv_mutation(schema, keys[1], "v", 1);
    const auto full_right = make_kv_mutation(schema, keys[2], "v", 1);
    const auto mixed_right = make_kv_mutation(schema, keys[3], "v", 1);
    tmpdir dir;

    {
        test_store store(dir);
        auto& ls = store.ls();
        test_recovery_catalog no_tables;
        ls.do_recovery(no_tables).get();
        ls.start().get();
        auto stop_store = seastar::defer([&ls] noexcept { ls.stop().get(); });
        test_logstor_group left(schema, ls);
        test_logstor_group right(schema, ls, left.logstor_index());
        auto compaction_left = ls.get_compaction_manager().disable_compaction(left).get();
        auto compaction_right = ls.get_compaction_manager().disable_compaction(right).get();

        write_to_full_segment(ls, left, full_left);
        write_to_full_segment(ls, right, full_right);
        write_to_mixed_segment(ls, left, mixed_left);
        write_to_mixed_segment(ls, right, mixed_right);
    }

    test_store store(dir);
    auto& ls = store.ls();
    auto stop_store = seastar::defer([&ls] noexcept { ls.stop().get(); });
    test_logstor_group left(schema, ls);
    test_logstor_group right(schema, ls, left.logstor_index());
    test_recovery_catalog catalog;
    catalog.add(left, left_of(boundary));
    catalog.add(right, right_of(boundary));

    const auto summary = ls.do_recovery(catalog).get();
    ls.start().get();

    BOOST_REQUIRE_EQUAL(summary.used_segments, 3u);
    BOOST_REQUIRE_EQUAL(summary.separated_segments, 0u);
    BOOST_REQUIRE_EQUAL(left.logstor_segments().segment_count(), 1u);
    BOOST_REQUIRE_EQUAL(right.logstor_segments().segment_count(), 1u);

    left.flush_separator().get();
    right.flush_separator().get();
    BOOST_REQUIRE_EQUAL(left.logstor_segments().segment_count(), 2u);
    BOOST_REQUIRE_EQUAL(right.logstor_segments().segment_count(), 2u);
    for (const auto& m : {full_left, mixed_left, full_right, mixed_right}) {
        require_value(ls, left.logstor_index(), m);
    }
}

// The records of a table that the catalog does not have are left out, and are not counted as
// dropped: a table that is gone takes its records with it.
SEASTAR_THREAD_TEST_CASE(test_logstor_recovery_leaves_out_a_table_that_is_gone) {
    auto schema_a = make_kv_schema("a");
    auto schema_b = make_kv_schema("b");
    const auto a0 = make_kv_mutation(schema_a, "pk0", "v0", 1);
    const auto b0 = make_kv_mutation(schema_b, "pk0", "v0", 1);
    tmpdir dir;

    {
        test_store store(dir);
        auto& ls = store.ls();
        test_recovery_catalog no_tables;
        ls.do_recovery(no_tables).get();
        ls.start().get();
        auto stop_store = seastar::defer([&ls] noexcept { ls.stop().get(); });
        test_logstor_group group_a(schema_a, ls);
        test_logstor_group group_b(schema_b, ls);
        write_to_full_segment(ls, group_a, a0);
        write_to_full_segment(ls, group_b, b0);
    }

    test_store store(dir);
    auto& ls = store.ls();
    auto stop_store = seastar::defer([&ls] noexcept { ls.stop().get(); });
    test_logstor_group group_a(schema_a, ls);
    test_recovery_catalog catalog;
    catalog.add(group_a);

    const auto summary = ls.do_recovery(catalog).get();
    ls.start().get();

    BOOST_REQUIRE_EQUAL(summary.used_segments, 1u);
    BOOST_REQUIRE_EQUAL(summary.free_segments + summary.used_segments, summary.found_segments);
    BOOST_REQUIRE_EQUAL(summary.dropped_records, 0u);
    BOOST_REQUIRE_EQUAL(group_a.logstor_index().get_key_count(), 1u);
    require_value(ls, group_a.logstor_index(), a0);
}

// The records of a tablet that has no group on this shard are dropped and counted, and their
// segments are free.
SEASTAR_THREAD_TEST_CASE(test_logstor_recovery_drops_the_records_of_a_tablet_that_is_not_here) {
    auto schema = make_kv_schema("cf");
    const auto keys = make_token_ordered_keys(schema, 2);
    const auto boundary = token_of(*schema, keys[0]);
    const auto left_record = make_kv_mutation(schema, keys[0], "v", 1);
    const auto right_record = make_kv_mutation(schema, keys[1], "v", 1);
    tmpdir dir;

    {
        test_store store(dir);
        auto& ls = store.ls();
        test_recovery_catalog no_tables;
        ls.do_recovery(no_tables).get();
        ls.start().get();
        auto stop_store = seastar::defer([&ls] noexcept { ls.stop().get(); });
        test_logstor_group left(schema, ls);
        test_logstor_group right(schema, ls, left.logstor_index());
        write_to_full_segment(ls, left, left_record);
        write_to_full_segment(ls, right, right_record);
    }

    test_store store(dir);
    auto& ls = store.ls();
    auto stop_store = seastar::defer([&ls] noexcept { ls.stop().get(); });
    test_logstor_group left(schema, ls);
    test_recovery_catalog catalog;
    catalog.add(left, left_of(boundary));

    const auto summary = ls.do_recovery(catalog).get();
    ls.start().get();

    // Both copies of right_record: the one in the mixed segment it was written to, and the one the
    // separator made in a full segment of the right group.
    BOOST_REQUIRE_EQUAL(summary.dropped_records, 2u);
    BOOST_REQUIRE_EQUAL(summary.used_segments, 1u);
    BOOST_REQUIRE_EQUAL(summary.free_segments + summary.used_segments, summary.found_segments);
    BOOST_REQUIRE_EQUAL(left.logstor_segments().segment_count(), 1u);
    require_value(ls, left.logstor_index(), left_record);
    require_no_value(ls, left.logstor_index(), right_record);
}

// A full segment whose range fits no single group of the shard is separated: each record goes to the
// group of its token.
SEASTAR_THREAD_TEST_CASE(test_logstor_recovery_separates_a_full_segment_that_fits_no_group) {
    auto schema = make_kv_schema("cf");
    const auto keys = make_token_ordered_keys(schema, 2);
    const auto boundary = token_of(*schema, keys[0]);
    const std::array records = {
        make_kv_mutation(schema, keys[0], "v", 1),
        make_kv_mutation(schema, keys[1], "v", 1),
    };
    tmpdir dir;

    {
        test_store store(dir);
        auto& ls = store.ls();
        test_recovery_catalog no_tables;
        ls.do_recovery(no_tables).get();
        ls.start().get();
        auto stop_store = seastar::defer([&ls] noexcept { ls.stop().get(); });
        test_logstor_group group(schema, ls);
        write_to_full_segment(ls, group, records);
    }

    test_store store(dir);
    auto& ls = store.ls();
    auto stop_store = seastar::defer([&ls] noexcept { ls.stop().get(); });
    test_logstor_group left(schema, ls);
    test_logstor_group right(schema, ls, left.logstor_index());
    test_recovery_catalog catalog;
    catalog.add(left, left_of(boundary));
    catalog.add(right, right_of(boundary));

    const auto summary = ls.do_recovery(catalog).get();
    ls.start().get();

    BOOST_REQUIRE_EQUAL(summary.separated_segments, 1u);
    BOOST_REQUIRE_EQUAL(summary.used_segments, 1u);
    BOOST_REQUIRE_EQUAL(summary.dropped_records, 0u);

    left.flush_separator().get();
    right.flush_separator().get();
    BOOST_REQUIRE_EQUAL(left.logstor_segments().segment_count(), 1u);
    BOOST_REQUIRE_EQUAL(right.logstor_segments().segment_count(), 1u);
    for (const auto& m : records) {
        require_value(ls, left.logstor_index(), m);
    }
}

// The records of the full segments that fit no group are more than the two separator buffers of a
// group hold, so the separator must flush buffers. A flush takes a segment from the pool, which only
// start() fills, so recovery leaves the separation to start().
SEASTAR_THREAD_TEST_CASE(test_logstor_recovery_separates_many_full_segments_that_fit_no_group) {
    auto schema = make_kv_schema("cf");
    constexpr size_t segment_count = 8;
    constexpr size_t keys_per_side = 3;
    const auto keys = make_token_ordered_keys(schema, 2 * segment_count * keys_per_side);
    const auto boundary = token_of(*schema, keys[segment_count * keys_per_side - 1]);
    const sstring value(18 * 1024, 'x');
    // Each segment holds keys on both sides of the boundary.
    std::vector<std::vector<mutation>> segments(segment_count);
    for (size_t s = 0; s < segment_count; ++s) {
        for (size_t i = 0; i < keys_per_side; ++i) {
            segments[s].push_back(make_kv_mutation(schema, keys[s * keys_per_side + i], value, 1));
            segments[s].push_back(make_kv_mutation(schema, keys[(segment_count + s) * keys_per_side + i], value, 1));
        }
    }
    tmpdir dir;

    {
        test_store store(dir);
        auto& ls = store.ls();
        test_recovery_catalog no_tables;
        ls.do_recovery(no_tables).get();
        ls.start().get();
        auto stop_store = seastar::defer([&ls] noexcept { ls.stop().get(); });
        test_logstor_group group(schema, ls);
        auto compaction_guard = ls.get_compaction_manager().disable_compaction(group).get();
        for (const auto& records : segments) {
            write_to_full_segment(ls, group, records);
        }
        BOOST_REQUIRE_EQUAL(group.logstor_segments().segment_count(), segment_count);
    }

    test_store store(dir);
    auto& ls = store.ls();
    auto stop_store = seastar::defer([&ls] noexcept { ls.stop().get(); });
    test_logstor_group left(schema, ls);
    test_logstor_group right(schema, ls, left.logstor_index());
    test_recovery_catalog catalog;
    catalog.add(left, left_of(boundary));
    catalog.add(right, right_of(boundary));

    const auto summary = ls.do_recovery(catalog).get();
    BOOST_REQUIRE_EQUAL(summary.used_segments, segment_count);
    BOOST_REQUIRE_EQUAL(summary.separated_segments, segment_count);
    BOOST_REQUIRE_EQUAL(summary.dropped_records, 0u);
    BOOST_REQUIRE_EQUAL(left.logstor_segments().segment_count(), 0u);
    BOOST_REQUIRE_EQUAL(right.logstor_segments().segment_count(), 0u);

    ls.start().get();
    left.flush_separator().get();
    right.flush_separator().get();
    BOOST_REQUIRE_GE(left.logstor_segments().segment_count(), 2u);
    BOOST_REQUIRE_GE(right.logstor_segments().segment_count(), 2u);
    for (const auto& records : segments) {
        for (const auto& m : records) {
            require_value(ls, left.logstor_index(), m);
        }
    }
}

// The segment sequence goes on from the highest one on disk, so a record written after a restart is
// newer than every record before it: of two records with the same timestamp, the later one wins
// after the next restart too.
SEASTAR_THREAD_TEST_CASE(test_logstor_recovery_continues_the_segment_sequence) {
    auto schema = make_kv_schema("cf");
    const auto before_restart = make_kv_mutation(schema, "pk0", "v0", 1);
    const auto after_restart = make_kv_mutation(schema, "pk0", "v1", 1);
    tmpdir dir;

    {
        test_store store(dir);
        auto& ls = store.ls();
        test_recovery_catalog no_tables;
        ls.do_recovery(no_tables).get();
        ls.start().get();
        auto stop_store = seastar::defer([&ls] noexcept { ls.stop().get(); });
        test_logstor_group group(schema, ls);
        write_to_full_segment(ls, group, before_restart);
    }

    recovery_summary first;
    {
        test_store store(dir);
        auto& ls = store.ls();
        auto stop_store = seastar::defer([&ls] noexcept { ls.stop().get(); });
        test_logstor_group group(schema, ls);
        test_recovery_catalog catalog;
        catalog.add(group);
        first = ls.do_recovery(catalog).get();
        ls.start().get();
        require_value(ls, group.logstor_index(), before_restart);
        write_to_full_segment(ls, group, after_restart);
    }

    test_store store(dir);
    auto& ls = store.ls();
    auto stop_store = seastar::defer([&ls] noexcept { ls.stop().get(); });
    test_logstor_group group(schema, ls);
    test_recovery_catalog catalog;
    catalog.add(group);
    const auto second = ls.do_recovery(catalog).get();
    ls.start().get();

    BOOST_REQUIRE_GT(second.next_segment_seq.value, first.next_segment_seq.value);
    require_value(ls, group.logstor_index(), after_restart);
}

// With a smaller configuration, recovery removes the files above the configured count that hold no
// used segment, from the end, and keeps the others. A segment above the configured count never goes
// on the free list.
SEASTAR_THREAD_TEST_CASE(test_logstor_recovery_with_a_smaller_configuration) {
    constexpr size_t segment_size = 128 * 1024;
    // The pool keeps segments back for compaction, so the store needs room for the test's segments
    // beyond those.
    constexpr size_t segments_per_file = 16;
    const logstor_params larger{
        .segment_size = segment_size,
        .file_size = segments_per_file * segment_size,
        .disk_size = 4 * segments_per_file * segment_size,
        .compaction_enabled = false,
    };
    auto smaller = larger;
    smaller.disk_size = 2 * segments_per_file * segment_size;
    auto schema = make_kv_schema("cf");

    // Writes `count` versions of one key, each to a full segment of its own, and returns the last
    // one. The full segments are allocated in order, so only the last one holds a live record.
    auto write_versions = [&] (const tmpdir& dir, size_t count) {
        test_store store(dir, larger);
        auto& ls = store.ls();
        test_recovery_catalog no_tables;
        ls.do_recovery(no_tables).get();
        ls.start().get();
        auto stop_store = seastar::defer([&ls] noexcept { ls.stop().get(); });
        test_logstor_group group(schema, ls);
        std::optional<mutation> last;
        for (size_t i = 0; i < count; ++i) {
            last = make_kv_mutation(schema, "pk0", seastar::format("v{}", i), api::timestamp_type(i + 1));
            write_to_full_segment(ls, group, *last);
        }
        return *last;
    };

    auto recover_smaller = [&] (const tmpdir& dir, const mutation& last) {
        test_store store(dir, smaller);
        auto& ls = store.ls();
        auto stop_store = seastar::defer([&ls] noexcept { ls.stop().get(); });
        test_logstor_group group(schema, ls);
        test_recovery_catalog catalog;
        catalog.add(group);
        const auto summary = ls.do_recovery(catalog).get();
        ls.start().get();
        require_value(ls, group.logstor_index(), last);
        const auto& sm = ls.get_segment_manager();
        return std::tuple(summary, sm.get_usage().disk_usage,
                on_disk(sm.get_segment_file_path(log_segment_id(2 * segments_per_file))),
                on_disk(sm.get_segment_file_path(log_segment_id(3 * segments_per_file))));
    };

    {
        // Files 2 and 3 hold no used segment.
        tmpdir dir;
        const auto last = write_versions(dir, 2);
        const auto [summary, disk_usage, file2_on_disk, file3_on_disk] = recover_smaller(dir, last);
        BOOST_REQUIRE_EQUAL(summary.found_segments, 4 * segments_per_file);
        BOOST_REQUIRE_EQUAL(summary.used_segments, 1u);
        BOOST_REQUIRE_EQUAL(summary.free_segments, 2 * segments_per_file - 1);
        BOOST_REQUIRE_EQUAL(disk_usage, smaller.disk_size);
        BOOST_REQUIRE(!file2_on_disk);
        BOOST_REQUIRE(!file3_on_disk);
    }
    {
        // The full segment with the live record is in file 3, so no file is removed. All the segments
        // below the configured count are free, and the unused ones above it are neither used nor free.
        tmpdir dir;
        const auto last = write_versions(dir, 3 * segments_per_file);
        const auto [summary, disk_usage, file2_on_disk, file3_on_disk] = recover_smaller(dir, last);
        BOOST_REQUIRE_EQUAL(summary.found_segments, 4 * segments_per_file);
        BOOST_REQUIRE_EQUAL(summary.used_segments, 1u);
        BOOST_REQUIRE_EQUAL(summary.free_segments, 2 * segments_per_file);
        BOOST_REQUIRE_EQUAL(disk_usage, larger.disk_size);
        BOOST_REQUIRE(file2_on_disk);
        BOOST_REQUIRE(file3_on_disk);
    }
}

// The tests below do not restart the store. They check that recovery would rebuild the live index
// exactly after each of the operations that shadow, move or erase records, through
// recovery_divergence() in test/lib/logstor_test_utils.hh: recovery's own rebuild, run over a
// quiescent store and compared with the live index.

// After compaction, the records it rewrote exist twice on disk: in the freed source segments,
// which nothing has overwritten yet, and in its output. The copies have to be the ones recovery
// picks.
SEASTAR_THREAD_TEST_CASE(test_logstor_recovery_reproduces_index_after_compaction) {
    auto schema = make_kv_schema("cf");
    tmpdir dir;

    test_store store(dir);
    auto& ls = store.ls();
    test_recovery_catalog no_tables;
    ls.do_recovery(no_tables).get();
    ls.start().get();
    auto stop_store = seastar::defer([&ls] noexcept { ls.stop().get(); });

    test_logstor_group cg(schema, ls);
    auto setup_guard = std::make_optional(ls.get_compaction_manager().disable_compaction(cg).get());

    write_to_full_segment(ls, cg, make_kv_mutation(schema, "pk0", "v0", api::timestamp_type(1)));
    write_to_full_segment(ls, cg, make_kv_mutation(schema, "pk1", "v1", api::timestamp_type(2)));
    write_to_full_segment(ls, cg, make_kv_mutation(schema, "pk2", "v2", api::timestamp_type(3)));
    write_to_full_segment(ls, cg, make_kv_mutation(schema, "pk0", "v0-new", api::timestamp_type(4)));
    write_to_full_segment(ls, cg, make_kv_mutation(schema, "pk1", "v1-new", api::timestamp_type(5)));
    BOOST_REQUIRE_EQUAL(cg.logstor_segments().segment_count(), 5u);

    setup_guard.reset();
    ls.get_compaction_manager().submit(cg);
    auto compaction_guard = ls.get_compaction_manager().disable_compaction(cg).get();
    BOOST_REQUIRE_EQUAL(cg.logstor_segments().segment_count(), 1u);

    BOOST_REQUIRE_EQUAL(recovery_divergence(ls, schema->id(), cg.logstor_index()), std::string());
}

// Split compaction moves single-sided segments as they are and rewrites the straddling ones, so
// after it the index points into segments of both target groups and the freed straddling segment
// still holds its records.
SEASTAR_THREAD_TEST_CASE(test_logstor_recovery_reproduces_index_after_split_compaction) {
    auto schema = make_kv_schema("cf");
    tmpdir dir;

    test_store store(dir);
    auto& ls = store.ls();
    test_recovery_catalog no_tables;
    ls.do_recovery(no_tables).get();
    ls.start().get();
    auto stop_store = seastar::defer([&ls] noexcept { ls.stop().get(); });

    test_logstor_group src(schema, ls);
    test_logstor_group left(schema, ls, src.logstor_index());
    test_logstor_group right(schema, ls, src.logstor_index());
    auto& index = src.logstor_index();
    auto setup_guard = std::make_optional(ls.get_compaction_manager().disable_compaction(src).get());

    const auto keys = make_token_ordered_keys(schema, 6);
    const auto boundary = dht::decorate_key(*schema, partition_key::from_single_value(*schema, serialized(keys[2]))).token();
    auto classify = [boundary] (dht::token t) -> mutation_writer::token_group_id {
        return t <= boundary ? 0 : 1;
    };

    write_to_full_segment(ls, src, make_kv_mutation(schema, keys[0], "v0", api::timestamp_type(1)));
    const std::array right_only = {
        make_kv_mutation(schema, keys[4], "v4", api::timestamp_type(5)),
        make_kv_mutation(schema, keys[5], "v5", api::timestamp_type(6)),
    };
    write_to_full_segment(ls, src, right_only);
    const std::array straddling = {
        make_kv_mutation(schema, keys[1], "v1", api::timestamp_type(2)),
        make_kv_mutation(schema, keys[2], "v2", api::timestamp_type(3)),
        make_kv_mutation(schema, keys[3], "v3", api::timestamp_type(4)),
    };
    write_to_full_segment(ls, src, straddling);
    write_to_full_segment(ls, src, make_kv_mutation(schema, keys[2], "v2-new", api::timestamp_type(7)));
    BOOST_REQUIRE_EQUAL(src.logstor_segments().segment_count(), 4u);

    setup_guard.reset();
    ls.get_compaction_manager().submit_split_compaction(src, classify,
            [&] (log_segment_id, dht::token first_token, dht::token last_token) -> logstor_group& {
                return classify(first_token) == 0 ? static_cast<logstor_group&>(left) : right;
            }).get();
    BOOST_REQUIRE_EQUAL(src.logstor_segments().segment_count(), 0u);

    BOOST_REQUIRE_EQUAL(recovery_divergence(ls, schema->id(), index), std::string());
}

// A fixed pseudo-random mix of writes and overwrites, with timestamps that do not follow the write
// order so that writes lose to earlier ones and ties happen, interleaved with separator flushes and
// compactions.
SEASTAR_THREAD_TEST_CASE(test_logstor_recovery_reproduces_index_under_mixed_workload) {
    auto schema = make_kv_schema("cf");
    tmpdir dir;

    test_store store(dir);
    auto& ls = store.ls();
    test_recovery_catalog no_tables;
    ls.do_recovery(no_tables).get();
    ls.start().get();
    auto stop_store = seastar::defer([&ls] noexcept { ls.stop().get(); });

    test_logstor_group cg(schema, ls);

    std::mt19937 rng(20260930);
    constexpr size_t key_count = 48;
    constexpr size_t write_count = 400;
    const sstring filler(6 * 1024, 'x');

    for (size_t i = 0; i < write_count; ++i) {
        const auto key = rng() % key_count;
        const auto ts = api::timestamp_type(1 + rng() % 8);
        ls.write(make_kv_mutation(schema, seastar::format("pk{}", key), seastar::format("v{}-{}", i, filler), ts), write_target(&cg, {}), db::no_timeout).get();
        if (i % 40 == 39) {
            ls.flush_to_separator().get();
            cg.flush_separator().get();
        }
        if (i % 160 == 159) {
            ls.get_compaction_manager().submit(cg);
            ls.get_compaction_manager().disable_compaction(cg).get();
        }
    }

    ls.flush_to_separator().get();
    cg.flush_separator().get();
    auto compaction_guard = ls.get_compaction_manager().disable_compaction(cg).get();

    BOOST_REQUIRE_EQUAL(recovery_divergence(ls, schema->id(), cg.logstor_index()), std::string());
}
