/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <ranges>

#include <boost/test/unit_test.hpp>
#include <fmt/ranges.h>
#include "test/lib/scylla_test_case.hh"

#include "mutation/position_range_utils.hh"
#include "schema/schema_builder.hh"

#include <seastar/core/smp.hh>

static schema_ptr make_schema() {
    return schema_builder(this_smp_shard_count(), "ks", "cf")
            .with_column("pk", int32_type, column_kind::partition_key)
            .with_column("a", int32_type, column_kind::clustering_key)
            .with_column("b", int32_type, column_kind::clustering_key)
            .with_column("c", int32_type, column_kind::clustering_key)
            .with_column("v", int32_type)
            .build();
}

static clustering_key_prefix ckp(const schema& s, std::vector<int32_t> values) {
    std::vector<bytes> exploded;
    for (auto v : values) {
        exploded.push_back(int32_type->decompose(v));
    }
    return clustering_key_prefix::from_exploded(s, exploded);
}

// The range of a single row.
static position_range row(const schema& s, std::vector<int32_t> key) {
    auto k = ckp(s, std::move(key));
    return position_range(position_in_partition::before_key(k), position_in_partition::after_key(s, k));
}

// The range of `a = 0 AND b > 1`: ((0, 1), (0)], which lies after all rows with the prefix (0, 1).
static position_range after_0_1(const schema& s) {
    return position_range::from_range(query::clustering_range::make({ckp(s, {0, 1}), false}, {ckp(s, {0}), true}));
}

// [start, end] of clustering keys or prefixes.
static position_range closed(const schema& s, std::vector<int32_t> start, std::vector<int32_t> end) {
    return position_range::from_range(query::clustering_range::make({ckp(s, std::move(start)), true}, {ckp(s, std::move(end)), true}));
}

static void check_equal(const schema& s, const position_range_vector& actual, const std::vector<position_range>& expected) {
    position_in_partition::equal_compare eq(s);
    BOOST_TEST_INFO(fmt::format("actual: {}, expected: {}", actual, expected));
    BOOST_REQUIRE_EQUAL(actual.size(), expected.size());
    for (size_t i = 0; i < actual.size(); ++i) {
        BOOST_TEST_INFO(fmt::format("actual: {}, expected: {}", actual, expected));
        BOOST_REQUIRE(eq(actual[i].start(), expected[i].start()));
        BOOST_REQUIRE(eq(actual[i].end(), expected[i].end()));
    }
}

static position_range_vector vec(std::vector<position_range> ranges) {
    position_range_vector result;
    for (auto& r : ranges) {
        result.push_back(std::move(r));
    }
    return result;
}

// interval<clustering_key_prefix> orders ((0, 1), (0)] before [(0, 1, 0), (0, 1, 0)], because (0, 1) is a prefix
// of (0, 1, 0). By position, the row comes first.
SEASTAR_THREAD_TEST_CASE(test_sort_orders_prefix_bounds_by_position) {
    auto s = make_schema();
    auto ranges = vec({after_0_1(*s), row(*s, {0, 1, 0})});
    sort_position_ranges(*s, ranges);
    check_equal(*s, ranges, {row(*s, {0, 1, 0}), after_0_1(*s)});
    ranges = deoverlap_position_ranges(*s, std::move(ranges)).get();
    check_equal(*s, ranges, {row(*s, {0, 1, 0}), after_0_1(*s)});

    auto clustering_ranges = ranges | std::views::transform([&] (const position_range& r) {
        return *position_range_to_clustering_range(r, *s);
    }) | std::ranges::to<std::vector>();
    BOOST_REQUIRE_EQUAL(clustering_ranges.size(), 2);
    BOOST_REQUIRE(clustering_ranges[0].is_singular());
    BOOST_REQUIRE(clustering_key_prefix::equality(*s)(clustering_ranges[0].start()->value(), ckp(*s, {0, 1, 0})));
    BOOST_REQUIRE(!clustering_ranges[1].start()->is_inclusive());
    BOOST_REQUIRE(clustering_key_prefix::equality(*s)(clustering_ranges[1].start()->value(), ckp(*s, {0, 1})));
    BOOST_REQUIRE(clustering_ranges[1].end()->is_inclusive());
    BOOST_REQUIRE(clustering_key_prefix::equality(*s)(clustering_ranges[1].end()->value(), ckp(*s, {0})));
}

// [(1, 1, 1), (1, 1)] covers the rows (1, 1, x) with x >= 1, so a key prefix order (where (1, 1) < (1, 1, 1)) sees
// an empty range here and merging it with other ranges loses it (scylladb#22817).
SEASTAR_THREAD_TEST_CASE(test_deoverlap_keeps_range_ending_in_prefix) {
    auto s = make_schema();
    auto r = closed(*s, {1, 1, 1}, {1, 1});
    auto after = position_range::from_range(query::clustering_range::make_starting_with({ckp(*s, {9, 9, 9}), true}));
    auto before = position_range::from_range(query::clustering_range::make_ending_with({ckp(*s, {1, 1, 1}), true}));
    auto ranges = vec({r, after, before});
    sort_position_ranges(*s, ranges);
    ranges = deoverlap_position_ranges(*s, std::move(ranges)).get();
    // before and r overlap at (1, 1, 1), so they are merged.
    check_equal(*s, ranges, {position_range(before.start(), r.end()), after});
}

SEASTAR_THREAD_TEST_CASE(test_deoverlap_merges_overlapping_and_adjacent_ranges) {
    auto s = make_schema();
    // Overlapping.
    check_equal(*s, deoverlap_position_ranges(*s, vec({closed(*s, {1}, {3}), closed(*s, {2}, {5})})).get(),
            {closed(*s, {1}, {5})});
    // Contained.
    check_equal(*s, deoverlap_position_ranges(*s, vec({closed(*s, {1}, {5}), closed(*s, {2}, {3})})).get(),
            {closed(*s, {1}, {5})});
    // Adjacent: [before 1, before 2) and [before 2, after 2).
    auto k1 = ckp(*s, {1});
    auto k2 = ckp(*s, {2});
    auto first = position_range(position_in_partition::before_key(k1), position_in_partition::before_key(k2));
    auto second = position_range(position_in_partition::before_key(k2), position_in_partition::after_key(*s, k2));
    check_equal(*s, deoverlap_position_ranges(*s, vec({first, second})).get(),
            {position_range(first.start(), second.end())});
    // Disjoint ranges stay apart.
    check_equal(*s, deoverlap_position_ranges(*s, vec({row(*s, {0, 0, 1}), row(*s, {0, 0, 2})})).get(),
            {row(*s, {0, 0, 1}), row(*s, {0, 0, 2})});
}

SEASTAR_THREAD_TEST_CASE(test_deoverlap_drops_empty_ranges) {
    auto s = make_schema();
    auto k = ckp(*s, {1});
    auto empty = position_range(position_in_partition::before_key(k), position_in_partition::before_key(k));
    check_equal(*s, deoverlap_position_ranges(*s, vec({empty})).get(), {});
    check_equal(*s, deoverlap_position_ranges(*s, vec({empty, row(*s, {2, 0, 0})})).get(), {row(*s, {2, 0, 0})});
    check_equal(*s, deoverlap_position_ranges(*s, vec({})).get(), {});
}

SEASTAR_THREAD_TEST_CASE(test_merge) {
    auto s = make_schema();
    // A range tombstone list and a row list, as in a view update's read-before-write.
    auto tombstones = vec({closed(*s, {0, 0}, {0, 0}), after_0_1(*s)});
    auto rows = vec({row(*s, {0, 0, 5}), row(*s, {0, 1, 0}), row(*s, {0, 2, 0}), row(*s, {1, 0, 0})});
    // (0, 0, 5) is inside [(0, 0), (0, 0)], (0, 2, 0) inside ((0, 1), (0)].
    check_equal(*s, merge_position_ranges(*s, std::move(tombstones), std::move(rows)).get(),
            {closed(*s, {0, 0}, {0, 0}), row(*s, {0, 1, 0}), after_0_1(*s), row(*s, {1, 0, 0})});

    check_equal(*s, merge_position_ranges(*s, vec({}), vec({row(*s, {1, 0, 0})})).get(), {row(*s, {1, 0, 0})});
    check_equal(*s, merge_position_ranges(*s, vec({row(*s, {1, 0, 0})}), vec({})).get(), {row(*s, {1, 0, 0})});
    check_equal(*s, merge_position_ranges(*s, vec({}), vec({})).get(), {});
}

SEASTAR_THREAD_TEST_CASE(test_intersection) {
    auto s = make_schema();
    auto all = position_range::all_clustered_rows();

    auto r = position_range_intersection(*s, after_0_1(*s), all);
    BOOST_REQUIRE(r);
    check_equal(*s, vec({*r}), {after_0_1(*s)});

    // The rows with the prefix (0, 1) are before ((0, 1), (0)].
    BOOST_REQUIRE(!position_range_intersection(*s, after_0_1(*s), closed(*s, {0, 1}, {0, 1})));
    BOOST_REQUIRE(!position_range_intersection(*s, after_0_1(*s), row(*s, {0, 1, 7})));

    // ...but the rows with the prefix (0, 2) are in it.
    r = position_range_intersection(*s, after_0_1(*s), closed(*s, {0, 2}, {1}));
    BOOST_REQUIRE(r);
    check_equal(*s, vec({*r}), {closed(*s, {0, 2}, {0})});

    // An inclusive prefix end includes the rows extending it.
    r = position_range_intersection(*s, closed(*s, {0}, {0, 1}), row(*s, {0, 1, 3}));
    BOOST_REQUIRE(r);
    check_equal(*s, vec({*r}), {row(*s, {0, 1, 3})});
}
