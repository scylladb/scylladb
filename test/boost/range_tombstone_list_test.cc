/*
 * Copyright (C) 2016-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#define BOOST_TEST_MODULE core

#include <boost/test/unit_test.hpp>
#include <random>
#include <iostream>
#include "keys.hh"
#include "schema_builder.hh"
#include "range_tombstone_list.hh"
#include "test/boost/range_tombstone_list_assertions.hh"
#include "test/lib/log.hh"

#include <seastar/util/defer.hh>

static thread_local schema_ptr s = schema_builder("ks", "cf")
        .with_column("pk", int32_type, column_kind::partition_key)
        .with_column("ck1", int32_type, column_kind::clustering_key)
        .with_column("ck2", int32_type, column_kind::clustering_key)
        .with_column("v", int32_type, column_kind::regular_column)
        .build();

static auto gc_now = gc_clock::now();

static clustering_key_prefix key(std::vector<int32_t> components) {
    std::vector<bytes> exploded;
    std::transform(components.begin(), components.end(), std::back_inserter(exploded), [](auto&& c) {
        return int32_type->decompose(c);
    });
    return clustering_key_prefix::from_clustering_prefix(*s, exploded_clustering_prefix(std::move(exploded)));
}

static void assert_rt(const range_tombstone& expected, const range_tombstone& actual) {
    if (!expected.equal(*s, actual)) {
        BOOST_FAIL(format("Expected {} but got {}", expected, actual));
    }
}

static range_tombstone rt(int32_t start, int32_t end, api::timestamp_type timestamp) {
    return range_tombstone(key({start}), key({end}), {timestamp, gc_now});
}


static range_tombstone rtie(int32_t start, int32_t end, api::timestamp_type timestamp) {
    return range_tombstone(key({start}), bound_kind::incl_start, key({end}), bound_kind::excl_end, {timestamp, gc_now});
}

static range_tombstone rtei(int32_t start, int32_t end, api::timestamp_type timestamp) {
    return range_tombstone(key({start}), bound_kind::excl_start, key({end}), bound_kind::incl_end, {timestamp, gc_now});
}

static range_tombstone rtee(int32_t start, int32_t end, api::timestamp_type timestamp) {
    return range_tombstone(key({start}), bound_kind::excl_start, key({end}), bound_kind::excl_end, {timestamp, gc_now});
}

static range_tombstone at_least(int32_t start, api::timestamp_type timestamp) {
    return range_tombstone(bound_view(key({start}), bound_kind::incl_start), bound_view::top(), {timestamp, gc_now});
}

static range_tombstone at_most(int32_t end, api::timestamp_type timestamp) {
    return range_tombstone(bound_view::bottom(), bound_view(key({end}), bound_kind::incl_end), {timestamp, gc_now});
}

static range_tombstone less_than(int32_t end, api::timestamp_type timestamp) {
    return range_tombstone(bound_view::bottom(), bound_view(key({end}), bound_kind::excl_end), {timestamp, gc_now});
}

static range_tombstone greater_than(int32_t start, api::timestamp_type timestamp) {
    return range_tombstone(bound_view(key({start}), bound_kind::excl_start), bound_view::top(), {timestamp, gc_now});
}

BOOST_AUTO_TEST_CASE(test_sorted_addition) {
    range_tombstone_list l(*s);

    auto rt1 = rt(1, 5, 3);
    auto rt2 = rt(7, 10, 2);
    auto rt3 = rt(10, 13, 1);

    l.apply(*s, rt1);
    l.apply(*s, rt2);
    l.apply(*s, rt3);

    auto it = l.begin();
    assert_rt(rt1, *it++);
    assert_rt(rt2, *it++);
    assert_rt(rtei(10, 13, 1), *it++);
    BOOST_REQUIRE(it == l.end());
}

BOOST_AUTO_TEST_CASE(test_non_sorted_addition) {
    range_tombstone_list l(*s);

    auto rt1 = rt(1, 5, 3);
    auto rt2 = rt(7, 10, 2);
    auto rt3 = rt(10, 13, 1);

    l.apply(*s, rt2);
    l.apply(*s, rt1);
    l.apply(*s, rt3);

    auto it = l.begin();
    assert_rt(rt1, *it++);
    assert_rt(rt2, *it++);
    assert_rt(rtei(10, 13, 1), *it++);
    BOOST_REQUIRE(it == l.end());
}

BOOST_AUTO_TEST_CASE(test_adjacent_ranges_are_merged) {
    range_tombstone_list l(*s);

    l.apply(*s, rtie(1, 5, 1));
    l.apply(*s, rt(5, 7, 1));
    l.apply(*s, rtei(7, 8, 1));

    l.apply(*s, rt(18, 20, 1));
    l.apply(*s, rtee(15, 18, 1));
    l.apply(*s, rt(12, 15, 1));

    auto it = l.begin();
    BOOST_REQUIRE(it != l.end());
    assert_rt(rt(1, 8, 1), *it++);
    BOOST_REQUIRE(it != l.end());
    assert_rt(rt(12, 20, 1), *it++);
    BOOST_REQUIRE(it == l.end());
}

BOOST_AUTO_TEST_CASE(test_adjacent_ranges_with_differing_timestamps_are_not_merged) {
    range_tombstone_list l(*s);

    auto rt1 = rtie(1, 5, 1);
    auto rt2 = rt(5, 7, 2);
    auto rt3 = rtei(7, 8, 1);

    l.apply(*s, rt1);
    l.apply(*s, rt2);
    l.apply(*s, rt3);

    auto it = l.begin();
    BOOST_REQUIRE(it != l.end());
    assert_rt(rt1, *it++);
    BOOST_REQUIRE(it != l.end());
    assert_rt(rt2, *it++);
    BOOST_REQUIRE(it != l.end());
    assert_rt(rt3, *it++);
    BOOST_REQUIRE(it == l.end());
}

static bool no_overlap(const range_tombstone_list& l) {
    bound_view::tri_compare cmp(*s);
    std::optional<range_tombstone> prev;
    for (const range_tombstone& r : l) {
        if (prev) {
            if (cmp(prev->end_bound(), r.start_bound()) >= 0) {
                return false;
            }
        }
        prev = r;
    }
    return true;
}

BOOST_AUTO_TEST_CASE(test_overlap_around_same_key) {
    range_tombstone_list l(*s);

    auto rt1 = rt(1, 5, 1);
    auto rt2 = rt(5, 7, 2);

    l.apply(*s, rt1);
    l.apply(*s, rt2);
    BOOST_REQUIRE(no_overlap(l));
}

BOOST_AUTO_TEST_CASE(test_overlapping_addition) {
    range_tombstone_list l(*s);

    l.apply(*s, rt(4, 10, 3));
    l.apply(*s, rt(1, 7, 2));
    l.apply(*s, rt(8, 13, 4));
    l.apply(*s, rt(0, 15, 1));

    auto it = l.begin();
    assert_rt(rtie(0, 1, 1), *it++);
    assert_rt(rtie(1, 4, 2), *it++);
    assert_rt(rtie(4, 8, 3), *it++);
    assert_rt(rt(8, 13, 4), *it++);
    assert_rt(rtei(13, 15, 1), *it++);
    BOOST_REQUIRE(it == l.end());
}

BOOST_AUTO_TEST_CASE(test_simple_overlap) {
    range_tombstone_list l1(*s);

    l1.apply(*s, rt(0, 10, 3));
    l1.apply(*s, rt(3, 7, 5));

    auto it = l1.begin();
    assert_rt(rtie(0, 3, 3), *it++);
    assert_rt(rt(3, 7, 5), *it++);
    assert_rt(rtei(7, 10, 3), *it++);
    BOOST_REQUIRE(it == l1.end());

    range_tombstone_list l2(*s);

    l2.apply(*s, rt(0, 10, 3));
    l2.apply(*s, rt(3, 7, 2));

    it = l2.begin();
    assert_rt(rt(0, 10, 3), *it++);
    BOOST_REQUIRE(it == l2.end());
}

BOOST_AUTO_TEST_CASE(test_overlapping_previous_end_equals_start) {
    range_tombstone_list l(*s);

    l.apply(*s, rt(11, 12, 2));
    l.apply(*s, rt(1, 4, 2));
    l.apply(*s, rt(4, 10, 5));

    BOOST_REQUIRE(2 == l.search_tombstone_covering(*s, key({3})).timestamp);
    BOOST_REQUIRE(5 == l.search_tombstone_covering(*s, key({4})).timestamp);
    BOOST_REQUIRE(5 == l.search_tombstone_covering(*s, key({8})).timestamp);
    BOOST_REQUIRE(3 == l.size());
}

BOOST_AUTO_TEST_CASE(test_search) {
    range_tombstone_list l(*s);

    l.apply(*s, rt(0, 4, 5));
    l.apply(*s, rt(4, 6, 2));
    l.apply(*s, rt(9, 12, 1));
    l.apply(*s, rt(14, 15, 3));
    l.apply(*s, rt(15, 17, 6));

    BOOST_REQUIRE(tombstone() == l.search_tombstone_covering(*s, key({-1})));

    BOOST_REQUIRE(5 == l.search_tombstone_covering(*s, key({0})).timestamp);
    BOOST_REQUIRE(5 == l.search_tombstone_covering(*s, key({3})).timestamp);
    BOOST_REQUIRE(5 == l.search_tombstone_covering(*s, key({4})).timestamp);

    BOOST_REQUIRE(tombstone() == l.search_tombstone_covering(*s, key({18})));

    BOOST_REQUIRE(3 == l.search_tombstone_covering(*s, key({14})).timestamp);

    BOOST_REQUIRE(6 == l.search_tombstone_covering(*s, key({15})).timestamp);

    BOOST_REQUIRE(tombstone() == l.search_tombstone_covering(*s, key({18})));

    range_tombstone_list l2(*s);
    l2.apply(*s, rt(1, 2, 5));
    BOOST_REQUIRE(5 == l2.search_tombstone_covering(*s, key({2})).timestamp);

    range_tombstone_list l3(*s);
    l3.apply(*s, rtie(1, 2, 5));
    BOOST_REQUIRE(tombstone() == l3.search_tombstone_covering(*s, key({2})));
}

BOOST_AUTO_TEST_CASE(test_search_prefix) {
    range_tombstone_list l(*s);

    l.apply(*s, range_tombstone(key({1}), bound_kind::incl_start, key({1, 2}), bound_kind::incl_end, {8, gc_now}));
    l.apply(*s, range_tombstone(key({1, 2}), bound_kind::excl_start, key({1, 3}), bound_kind::incl_end, {12, gc_now}));

    BOOST_REQUIRE(8 == l.search_tombstone_covering(*s, key({1})).timestamp);

    range_tombstone_list l1(*s);

    l1.apply(*s, range_tombstone(key({1}), bound_kind::excl_start, key({2, 2}), bound_kind::incl_end, {8, gc_now}));
    l1.apply(*s, range_tombstone(key({1, 2}), bound_kind::excl_start, key({1, 3}), bound_kind::incl_end, {12, gc_now}));

    BOOST_REQUIRE(tombstone() == l1.search_tombstone_covering(*s, key({1})));

    range_tombstone_list l2(*s);

    l2.apply(*s, rt(1, 1, 8));

    BOOST_REQUIRE(8 == l2.search_tombstone_covering(*s, key({1, 2})).timestamp);

    range_tombstone_list l3(*s);

    l3.apply(*s, range_tombstone(key({1}), bound_kind::incl_start, key({1, 2}), bound_kind::incl_end, {8, gc_now}));
    l3.apply(*s, range_tombstone(key({1, 2}), bound_kind::excl_start, key({1, 3}), bound_kind::incl_end, {10, gc_now}));
    l3.apply(*s, range_tombstone(key({1, 3}), bound_kind::excl_start, key({1}), bound_kind::incl_end, {12, gc_now}));
    BOOST_REQUIRE(8 == l3.search_tombstone_covering(*s, key({1})).timestamp);

    range_tombstone_list l4(*s);

    l4.apply(*s, range_tombstone(key({1, 2}), bound_kind::incl_start, key({1, 3}), bound_kind::incl_end, {12, gc_now}));
    BOOST_REQUIRE(tombstone() == l4.search_tombstone_covering(*s, key({1})));
}

BOOST_AUTO_TEST_CASE(test_add_prefixes) {
    range_tombstone_list l(*s);

    auto tomb = tombstone{8, gc_now};

    l.apply(*s, range_tombstone(key({1}), bound_kind::excl_start, key({2, 2}), bound_kind::incl_end, tomb));
    l.apply(*s, range_tombstone(key({1}), bound_kind::incl_start, key({1}), bound_kind::incl_end, tomb));

    auto it = l.begin();
    assert_rt(range_tombstone(key({1}), bound_kind::incl_start, key({2, 2}), bound_kind::incl_end, tomb), *it++);
    BOOST_REQUIRE(it == l.end());

    range_tombstone_list l2(*s);

    l2.apply(*s, range_tombstone(key({1}), bound_kind::incl_start, key({1}), bound_kind::incl_end, tomb));
    l2.apply(*s, range_tombstone(key({1}), bound_kind::excl_start, key({2, 2}), bound_kind::incl_end, tomb));

    it = l2.begin();
    assert_rt(range_tombstone(key({1}), bound_kind::incl_start, key({2, 2}), bound_kind::incl_end, tomb), *it++);
    BOOST_REQUIRE(it == l2.end());
}

BOOST_AUTO_TEST_CASE(test_add_different_prefixes) {
    range_tombstone_list l(*s);
    auto rt1 = range_tombstone(key({4}), key({4}), {7, gc_now});
    l.apply(*s, rt1);
    auto rt2 = range_tombstone(key({4, 1}), key({4, 2}), {7, gc_now});
    l.apply(*s, rt2);

    auto it = l.begin();
    assert_rt(rt1, *it++);
    BOOST_REQUIRE(it == l.end());

    auto rt3 = range_tombstone(key({4, 1}), key({4, 2}), {8, gc_now});
    l.apply(*s, rt3);

    it = l.begin();
    assert_rt(range_tombstone(key({4}), bound_kind::incl_start, key({4, 1}), bound_kind::excl_end, {7, gc_now}), *it++);
    assert_rt(range_tombstone(key({4, 1}), bound_kind::incl_start, key({4, 2}), bound_kind::incl_end, {8, gc_now}), *it++);
    assert_rt(range_tombstone(key({4, 2}), bound_kind::excl_start, key({4}), bound_kind::incl_end, {7, gc_now}), *it++);
    BOOST_REQUIRE(it == l.end());
}

BOOST_AUTO_TEST_CASE(test_add_same) {
    range_tombstone_list l(*s);

    l.apply(*s, rt(4, 4, 5));
    l.apply(*s, rt(4, 4, 6));
    l.apply(*s, rt(4, 4, 4));

    auto it = l.begin();
    assert_rt(rt(4, 4, 6), *it++);
    BOOST_REQUIRE(it == l.end());
}

BOOST_AUTO_TEST_CASE(test_single_range_is_preserved) {
    range_tombstone_list l(*s);

    l.apply(*s, rt(1, 2, 10));
    l.apply(*s, rt(7, 13, 8));
    l.apply(*s, rt(13, 13, 20));
    l.apply(*s, rt(13, 18, 12));

    auto it = l.begin();
    assert_rt(rt(1, 2, 10), *it++);
    assert_rt(rtie(7, 13, 8), *it++);
    assert_rt(rt(13, 13, 20), *it++);
    assert_rt(rtei(13, 18, 12),  *it++);
    BOOST_REQUIRE(it == l.end());
}

BOOST_AUTO_TEST_CASE(test_single_range_is_replaced) {
    range_tombstone_list l(*s);

    l.apply(*s, rt(7, 13, 8));
    l.apply(*s, rt(13, 13, 20));
    l.apply(*s, rt(13, 18, 32));

    auto it = l.begin();
    assert_rt(rtie(7, 13, 8), *it++);
    assert_rt(rt(13, 18, 32),  *it++);
    BOOST_REQUIRE(it == l.end());
}

static bool assert_valid(range_tombstone_list& l) {
    bound_view::compare less(*s);
    auto it = l.begin();
    if (it == l.end()) {
        return true;
    }
    auto prev = *it;
    if (less(prev.end_bound(), prev.start_bound())) {
        std::cout << "Invalid empty slice " << prev << std::endl;
        return false;
    }
    while(++it != l.end()) {
        auto cur = *it;
        if (less(cur.end_bound(), cur.start_bound())) {
            std::cout << "Invalid empty slice " << cur << std::endl;
            return false;
        }
        if (less(cur.start_bound(), prev.end_bound())) {
            std::cout << "Ranges not in order or overlapping " << prev << " " << cur << std::endl;
            return false;
        }
    }
    return true;
}

static std::random_device rd;
static std::mt19937 gen(rd());
static std::uniform_int_distribution<int32_t> dist(0, 50);

static std::vector<range_tombstone> make_random() {
    std::vector<range_tombstone> rts;
    bound_view::compare less(*s);
    position_in_partition::tri_compare cmp(*s);

    auto make_random_ckey = [] {
        if (dist(gen) % 2 == 0) {
            return key({dist(gen)});
        } else {
            return key({dist(gen), dist(gen)});
        }
    };

    int32_t size = dist(gen) + 7;
    for (int32_t i = 0; i < size; ++i) {
        clustering_key_prefix start_key = make_random_ckey();
        clustering_key_prefix end_key = make_random_ckey();

        bool start_incl = dist(gen) > 25;
        bool end_incl = dist(gen) > 25;

        auto x = cmp(start_key, end_key);
        if (x == 0) {
            start_incl = end_incl = true;
        } else if (x > 0) {
            std::swap(start_key, end_key);
        }

        bound_view start_b = bound_view(start_key, start_incl ? bound_kind::incl_start : bound_kind::excl_start);
        bound_view end_b = bound_view(end_key, end_incl ? bound_kind::incl_end : bound_kind::excl_end);
        if (less(end_b, start_b)) {
            /*
             * Despite the start key is less (or equal) than the end one there's still
             * a chance to get the invalid range like this
             *
             *  { k0 ; excl } ... { k0.k1 ; anything }
             *
             * The guy is fixable by making the start bound inclusive.
             */
            start_b = bound_view(start_key, bound_kind::incl_start);
            if (less(end_b, start_b)) {
                std::cout << "Unfixable slice " << start_b << " ... " << end_b << std::endl;
                std::abort();
            }
        }

        rts.emplace_back(std::move(start_b), std::move(end_b), tombstone(dist(gen), gc_now));
    }

    return rts;
}

BOOST_AUTO_TEST_CASE(test_random_list_is_not_overlapped) {
    for (uint32_t i = 0; i < 2000; ++i) {
        auto input = make_random();
        range_tombstone_list l(*s);
        for (auto&& rt : input) {
            l.apply(*s, rt);
        }
        if (!assert_valid(l)) {
            std::cout << "For input:" << std::endl;
            for (auto&& rt : input) {
                std::cout << rt << std::endl;
            }
            std::cout << "Produced:" << std::endl;
            for (auto&& rt : l) {
                std::cout << rt << std::endl;
            }
            BOOST_REQUIRE(false);
        }
    }
}

BOOST_AUTO_TEST_CASE(test_non_sorted_addition_with_one_range_with_empty_end) {
    range_tombstone_list l(*s);

    auto rt1 = rt(1, 5, 3);
    auto rt2 = rt(7, 10, 2);
    auto rt3 = at_least(11, 1);

    l.apply(*s, rt2);
    l.apply(*s, rt3);
    l.apply(*s, rt1);

    auto it = l.begin();
    assert_rt(rt1, *it++);
    assert_rt(rt2, *it++);
    assert_rt(rt3, *it++);
    BOOST_REQUIRE(it == l.end());
}

BOOST_AUTO_TEST_CASE(test_add_range_with_empty_end_which_include_existing_range) {
    range_tombstone_list l(*s);

    l.apply(*s, rt(4, 10, 3));
    l.apply(*s, at_least(3, 4));

    auto it = l.begin();
    assert_rt(at_least(3, 4), *it++);
    BOOST_REQUIRE(it == l.end());
}

BOOST_AUTO_TEST_CASE(test_add_range_with_empty_start_and_end) {
    range_tombstone_list l(*s);

    l.apply(*s, rt(4, 10, 3));
    l.apply(*s, at_most(12, 4));

    auto it = l.begin();
    assert_rt(at_most(12, 4), *it++);
    BOOST_REQUIRE(it == l.end());
}

BOOST_AUTO_TEST_CASE(test_add_range_with_empty_end_to_range_with_empty_start_and_end) {
    range_tombstone_list l(*s);

    l.apply(*s, range_tombstone(bound_view::bottom(), bound_view::top(), tombstone(2, gc_now)));
    l.apply(*s, at_least(12, 4));

    auto it = l.begin();
    assert_rt(less_than(12, 2), *it++);
    assert_rt(at_least(12, 4), *it++);
    BOOST_REQUIRE(it == l.end());
}

BOOST_AUTO_TEST_CASE(test_add_range_with_empty_end_which_include_existing_range_with_empty_end) {
    range_tombstone_list l(*s);

    l.apply(*s, at_least(5, 3));
    l.apply(*s, at_least(3, 4));

    auto it = l.begin();
    assert_rt(at_least(3, 4), *it++);
    BOOST_REQUIRE(it == l.end());
}

BOOST_AUTO_TEST_CASE(test_add_included_range_to_range_with_empty_end) {
    range_tombstone_list l(*s);

    l.apply(*s, at_least(3, 3));
    l.apply(*s, rt(4, 10, 4));

    auto it = l.begin();
    assert_rt(rtie(3, 4, 3), *it++);
    assert_rt(rt(4, 10, 4), *it++);
    assert_rt(greater_than(10, 3), *it++);
    BOOST_REQUIRE(it == l.end());
}

BOOST_AUTO_TEST_CASE(test_add_included_range_with_empty_end_to_range_with_empty_end) {
    range_tombstone_list l(*s);

    l.apply(*s, at_least(3, 3));
    l.apply(*s, at_least(5, 4));

    auto it = l.begin();
    assert_rt(rtie(3, 5, 3), *it++);
    assert_rt(at_least(5, 4), *it++);
    BOOST_REQUIRE(it == l.end());
}

BOOST_AUTO_TEST_CASE(test_add_range_with_empty_end_which_overlaps_existing_range) {
    range_tombstone_list l(*s);

    l.apply(*s, rt(4, 10, 3));
    l.apply(*s, at_least(6, 4));

    auto it = l.begin();
    assert_rt(rtie(4, 6, 3), *it++);
    assert_rt(at_least(6, 4), *it++);
    BOOST_REQUIRE(it == l.end());
}

BOOST_AUTO_TEST_CASE(test_add_overlapping_range_to_range_with_empty_end) {
    range_tombstone_list l(*s);

    l.apply(*s, at_least(3, 3));
    l.apply(*s, rt(1, 10, 4));

    auto it = l.begin();
    assert_rt(rt(1, 10, 4), *it++);
    assert_rt(greater_than(10, 3), *it++);
    BOOST_REQUIRE(it == l.end());
}

// Reproduces https://github.com/scylladb/scylla/issues/3083
BOOST_AUTO_TEST_CASE(test_coalescing_with_end_bound_inclusiveness_change_with_prefix_bound) {
    range_tombstone_list l(*s);

    auto rt1 = rtie(4, 8, 4);
    auto rt2 = range_tombstone(key({8, 1}), bound_kind::incl_start, key({10}), bound_kind::excl_end, {1, gc_now});

    l.apply(*s, rt1);
    l.apply(*s, rt2);

    l.apply(*s, rt(1, 5, 4));

    auto it = l.begin();
    assert_rt(rtie(1, 8, 4), *it++);
    assert_rt(rt2, *it++);
    BOOST_REQUIRE(it == l.end());
}

BOOST_AUTO_TEST_CASE(test_search_with_empty_start) {
    range_tombstone_list l(*s);

    l.apply(*s, at_most(4, 5));
    l.apply(*s, rt(4, 6, 2));
    l.apply(*s, rt(9, 12, 1));
    l.apply(*s, rt(14, 15, 3));
    l.apply(*s, rt(15, 17, 6));

    BOOST_REQUIRE(5 == l.search_tombstone_covering(*s, key({-1})).timestamp);
    BOOST_REQUIRE(5 == l.search_tombstone_covering(*s, key({0})).timestamp);
    BOOST_REQUIRE(5 == l.search_tombstone_covering(*s, key({3})).timestamp);
    BOOST_REQUIRE(5 == l.search_tombstone_covering(*s, key({4})).timestamp);

    BOOST_REQUIRE(2 == l.search_tombstone_covering(*s, key({5})).timestamp);

    BOOST_REQUIRE(tombstone() == l.search_tombstone_covering(*s, key({7})));

    BOOST_REQUIRE(3 == l.search_tombstone_covering(*s, key({14})).timestamp);

    BOOST_REQUIRE(6 == l.search_tombstone_covering(*s, key({15})).timestamp);

    BOOST_REQUIRE(tombstone() == l.search_tombstone_covering(*s, key({18})));
}

BOOST_AUTO_TEST_CASE(test_search_with_empty_end) {
    range_tombstone_list l(*s);

    l.apply(*s, rt(0, 4, 5));
    l.apply(*s, rt(4, 6, 2));
    l.apply(*s, rt(9, 12, 1));
    l.apply(*s, rt(14, 15, 3));
    l.apply(*s, at_least(15, 6));

    BOOST_REQUIRE(tombstone() == l.search_tombstone_covering(*s, key({-1})));

    BOOST_REQUIRE(5 == l.search_tombstone_covering(*s, key({0})).timestamp);
    BOOST_REQUIRE(5 == l.search_tombstone_covering(*s, key({3})).timestamp);
    BOOST_REQUIRE(5 == l.search_tombstone_covering(*s, key({4})).timestamp);

    BOOST_REQUIRE(2 == l.search_tombstone_covering(*s, key({5})).timestamp);

    BOOST_REQUIRE(tombstone() == l.search_tombstone_covering(*s, key({7})));

    BOOST_REQUIRE(3 == l.search_tombstone_covering(*s, key({14})).timestamp);

    BOOST_REQUIRE(6 == l.search_tombstone_covering(*s, key({15})).timestamp);
    BOOST_REQUIRE(6 == l.search_tombstone_covering(*s, key({1000})).timestamp);
}

BOOST_AUTO_TEST_CASE(test_difference_with_self) {
    range_tombstone_list l(*s);
    l.apply(*s, rt(1, 1, 7));
    l.apply(*s, rt(3, 3, 8));

    BOOST_REQUIRE(l.difference(*s, l).empty());
}

BOOST_AUTO_TEST_CASE(test_difference_with_bigger_tombstone) {
    range_tombstone_list l1(*s);
    l1.apply(*s, rt(1, 2, 3));
    l1.apply(*s, rt(5, 7, 3));
    l1.apply(*s, rt(8, 11, 3));
    l1.apply(*s, rt(12, 14, 3));

    range_tombstone_list l2(*s);
    l2.apply(*s, rt(3, 4, 2));
    l2.apply(*s, rt(6, 9, 2));
    l2.apply(*s, rt(10, 13, 2));

    auto diff = l1.difference(*s, l2);
    auto it = diff.begin();
    assert_rt(rt(1, 2, 3), *it++);
    assert_rt(rt(5, 7, 3), *it++);
    assert_rt(rt(8, 11, 3), *it++);
    assert_rt(rt(12, 14, 3), *it++);
    BOOST_REQUIRE(it == diff.end());
}

BOOST_AUTO_TEST_CASE(test_difference_with_smaller_tombstone) {
    range_tombstone_list l1(*s);
    l1.apply(*s, rt(1, 2, 1));
    l1.apply(*s, rt(5, 7, 1));
    l1.apply(*s, rt(8, 11, 1));
    l1.apply(*s, rt(12, 14, 1));

    range_tombstone_list l2(*s);
    l2.apply(*s, rt(3, 4, 2));
    l2.apply(*s, rt(6, 9, 2));
    l2.apply(*s, rt(10, 13, 2));

    auto diff = l1.difference(*s, l2);
    auto it = diff.begin();
    assert_rt(rt(1, 2, 1), *it++);
    assert_rt(rtie(5, 6, 1), *it++);
    assert_rt(rtee(9, 10, 1), *it++);
    assert_rt(rtei(13, 14, 1), *it++);
    BOOST_REQUIRE(it == diff.end());
}

BOOST_AUTO_TEST_CASE(test_exception_safety) {
    int pos = 1;
    auto next_pos = [&] { return pos++; };

    auto ts0 = 0;
    auto ts1 = 1;

    range_tombstone_list original(*s);
    range_tombstone_list to_apply(*s);

    {
        auto p1 = next_pos();
        auto p2 = next_pos();
        original.apply(*s, rtie(p1, p2, ts0));
    }
    {
        auto p1 = next_pos();
        auto p2 = next_pos();
        to_apply.apply(*s, rtie(p1, p2, ts0));
    }
    {
        auto p1 = next_pos();
        auto p2 = next_pos();
        to_apply.apply(*s, rtie(p1, p2, ts0));
    }
    {
        auto p1 = next_pos();
        auto p2 = next_pos();
        original.apply(*s, rtie(p1, p2, ts0));
    }

    {
        auto p1 = next_pos();
        auto p2 = next_pos();
        auto p3 = next_pos();
        auto p4 = next_pos();
        auto p5 = next_pos();
        auto p6 = next_pos();
        to_apply.apply(*s, rtie(p1, p3, ts1));
        to_apply.apply(*s, rtie(p4, p6, ts1));
        original.apply(*s, rtie(p2, p5, ts0));
    }

    {
        auto p1 = next_pos();
        auto p2 = next_pos();
        auto p3 = next_pos();
        auto p4 = next_pos();
        auto p5 = next_pos();
        auto p6 = next_pos();
        to_apply.apply(*s, rtie(p1, p3, ts0));
        to_apply.apply(*s, rtie(p4, p6, ts0));
        original.apply(*s, rtie(p2, p5, ts1));
    }

    {
        auto p1 = next_pos();
        auto p2 = next_pos();
        auto p3 = next_pos();
        auto p4 = next_pos();
        auto p5 = next_pos();
        auto p6 = next_pos();
        to_apply.apply(*s, rtie(p1, p3, ts0));
        to_apply.apply(*s, rtie(p4, p6, ts1));
        original.apply(*s, rtie(p2, p5, ts1));
    }

    {
        auto p1 = next_pos();
        auto p2 = next_pos();
        auto p3 = next_pos();
        auto p4 = next_pos();
        auto p5 = next_pos();
        auto p6 = next_pos();
        to_apply.apply(*s, rtie(p1, p3, ts1));
        to_apply.apply(*s, rtie(p4, p6, ts0));
        original.apply(*s, rtie(p2, p5, ts1));
    }

    {
        auto p1 = next_pos();
        auto p2 = next_pos();
        auto p3 = next_pos();
        auto p4 = next_pos();
        auto p5 = next_pos();
        auto p6 = next_pos();
        to_apply.apply(*s, rtie(p1, p3, ts1));
        to_apply.apply(*s, rtie(p4, p6, ts1));
        original.apply(*s, rtie(p2, p5, ts1));
    }

    {
        auto p1 = next_pos();
        auto p2 = next_pos();
        auto p3 = next_pos();
        auto p4 = next_pos();
        to_apply.apply(*s, rtie(p1, p4, ts1));
        original.apply(*s, rtie(p2, p3, ts0));
    }

    {
        auto p1 = next_pos();
        auto p2 = next_pos();
        auto p3 = next_pos();
        auto p4 = next_pos();
        to_apply.apply(*s, rtie(p1, p4, ts0));
        original.apply(*s, rtie(p2, p3, ts1));
    }

    {
        auto p1 = next_pos();
        auto p2 = next_pos();
        auto p3 = next_pos();
        auto p4 = next_pos();
        to_apply.apply(*s, rtie(p2, p3, ts0));
        original.apply(*s, rtie(p1, p4, ts1));
    }

    {
        auto p1 = next_pos();
        auto p2 = next_pos();
        auto p3 = next_pos();
        auto p4 = next_pos();
        to_apply.apply(*s, rtie(p2, p3, ts1));
        original.apply(*s, rtie(p1, p4, ts0));
    }

    auto expected = original;
    expected.apply(*s, to_apply);

    // test apply_monotonically()
    memory::with_allocation_failures([&] () {
        auto list = original;
        auto d = defer([&] {
            memory::scoped_critical_alloc_section dfg;
            assert_that(*s, list).has_no_less_information_than(original);
        });
        list.apply_monotonically(*s, to_apply);
        assert_that(*s, list).is_equal_to(expected);
    });

    // test apply_reversibly()
    memory::with_allocation_failures([&] () {
        auto list = original;
        auto d = defer([&] () {
            memory::scoped_critical_alloc_section dfg;
            assert_that(*s, list).is_equal_to_either(original, expected);
        });
        list.apply_reversibly(*s, to_apply).cancel();
        assert_that(*s, list).is_equal_to(expected);
    });
}

BOOST_AUTO_TEST_CASE(test_accumulator) {
    auto ts1 = 1;
    auto ts2 = 2;

    testlog.info("Forward");
    auto acc = range_tombstone_accumulator(*s, false);
    acc.apply(rtie(0, 4, ts1));
    BOOST_REQUIRE_EQUAL(acc.tombstone_for_row(key({ 0 })), tombstone(ts1, gc_now));
    acc.apply(rtie(1, 2, ts2));
    BOOST_REQUIRE_EQUAL(acc.tombstone_for_row(key({ 1 })), tombstone(ts2, gc_now));
    BOOST_REQUIRE_EQUAL(acc.tombstone_for_row(key({ 2 })), tombstone(ts1, gc_now));
    BOOST_REQUIRE_EQUAL(acc.tombstone_for_row(key({ 3 })), tombstone(ts1, gc_now));
    BOOST_REQUIRE_EQUAL(acc.tombstone_for_row(key({ 4 })), tombstone());
    BOOST_REQUIRE_EQUAL(acc.tombstone_for_row(key({ 5 })), tombstone());
    acc.apply(rtie(6, 8, ts2));
    BOOST_REQUIRE_EQUAL(acc.tombstone_for_row(key({ 9 })), tombstone());
    acc.apply(rtie(10, 12, ts1));
    BOOST_REQUIRE_EQUAL(acc.tombstone_for_row(key({ 10 })), tombstone(ts1, gc_now));
    acc.apply(rtie(11, 14, ts2));
    BOOST_REQUIRE_EQUAL(acc.tombstone_for_row(key({ 11 })), tombstone(ts2, gc_now));
    BOOST_REQUIRE_EQUAL(acc.tombstone_for_row(key({ 12 })), tombstone(ts2, gc_now));
    BOOST_REQUIRE_EQUAL(acc.tombstone_for_row(key({ 13 })), tombstone(ts2, gc_now));
    BOOST_REQUIRE_EQUAL(acc.tombstone_for_row(key({ 14 })), tombstone());
    BOOST_REQUIRE_EQUAL(acc.tombstone_for_row(key({ 15 })), tombstone());

    testlog.info("Reversed");
    acc = range_tombstone_accumulator(*s, true);

    BOOST_REQUIRE_EQUAL(acc.tombstone_for_row(key({ 15 })), tombstone());
    BOOST_REQUIRE_EQUAL(acc.tombstone_for_row(key({ 14 })), tombstone());
    acc.apply(rtie(11, 14, ts2));
    BOOST_REQUIRE_EQUAL(acc.tombstone_for_row(key({ 13 })), tombstone(ts2, gc_now));
    BOOST_REQUIRE_EQUAL(acc.tombstone_for_row(key({ 12 })), tombstone(ts2, gc_now));
    acc.apply(rtie(10, 12, ts1));
    BOOST_REQUIRE_EQUAL(acc.tombstone_for_row(key({ 11 })), tombstone(ts2, gc_now));
    BOOST_REQUIRE_EQUAL(acc.tombstone_for_row(key({ 10 })), tombstone(ts1, gc_now));
    BOOST_REQUIRE_EQUAL(acc.tombstone_for_row(key({ 9 })), tombstone());
    acc.apply(rtie(6, 8, ts2));
    BOOST_REQUIRE_EQUAL(acc.tombstone_for_row(key({ 5 })), tombstone());
    BOOST_REQUIRE_EQUAL(acc.tombstone_for_row(key({ 4 })), tombstone());
    acc.apply(rtie(0, 4, ts1));
    BOOST_REQUIRE_EQUAL(acc.tombstone_for_row(key({ 3 })), tombstone(ts1, gc_now));
    BOOST_REQUIRE_EQUAL(acc.tombstone_for_row(key({ 2 })), tombstone(ts1, gc_now));
    acc.apply(rtie(1, 2, ts2));
    BOOST_REQUIRE_EQUAL(acc.tombstone_for_row(key({ 1 })), tombstone(ts2, gc_now));
    BOOST_REQUIRE_EQUAL(acc.tombstone_for_row(key({ 0 })), tombstone(ts1, gc_now));
}
