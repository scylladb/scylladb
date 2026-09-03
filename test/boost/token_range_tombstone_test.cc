/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <boost/test/unit_test.hpp>

#include <algorithm>
#include <random>

#include "mutation/token_range_tombstone.hh"
#include "mutation/mutation.hh"
#include "schema/schema_builder.hh"
#include "test/lib/scylla_test_case.hh"
#include "test/lib/log.hh"

static gc_clock::time_point gc_now = gc_clock::now();

static dht::token tok(int64_t v) {
    return dht::token(v);
}

static tombstone tomb(api::timestamp_type ts) {
    return tombstone(ts, gc_now);
}

static token_range_tombstone trt(int64_t start, int64_t end, api::timestamp_type ts) {
    return token_range_tombstone(tok(start), tok(end), tomb(ts));
}

// Checks the list's invariants and that it agrees with a brute force model of
// the same set of tombstones.
static void verify(const token_range_tombstone_list& list, const std::vector<token_range_tombstone>& applied,
        int64_t lo, int64_t hi) {
    const token_range_tombstone* prev = nullptr;
    for (auto&& rt : list) {
        BOOST_REQUIRE_MESSAGE(!rt.empty(), fmt::format("empty entry in {}", list));
        if (prev) {
            BOOST_REQUIRE_MESSAGE(prev->end_inclusive() <= rt.start_exclusive(), fmt::format("overlapping entries in {}", list));
            BOOST_REQUIRE_MESSAGE(prev->end_inclusive() != rt.start_exclusive() || prev->tomb() != rt.tomb(),
                    fmt::format("uncoalesced entries in {}", list));
        }
        prev = &rt;
    }
    for (int64_t t = lo; t <= hi; ++t) {
        tombstone expected;
        for (auto&& rt : applied) {
            if (rt.contains(tok(t))) {
                expected.apply(rt.tomb());
            }
        }
        BOOST_REQUIRE_MESSAGE(list.search(tok(t)) == expected,
                fmt::format("at token {}: got {}, expected {}, list {}", t, list.search(tok(t)), expected, list));
    }
}

BOOST_AUTO_TEST_CASE(test_empty_list) {
    token_range_tombstone_list l;
    BOOST_REQUIRE(l.empty());
    BOOST_REQUIRE_EQUAL(l.size(), 0u);
    BOOST_REQUIRE(!l.search(tok(0)));
    BOOST_REQUIRE(!l.max_tombstone());
}

BOOST_AUTO_TEST_CASE(test_range_is_open_on_the_left_and_closed_on_the_right) {
    auto rt = trt(10, 20, 1);
    BOOST_REQUIRE(!rt.contains(tok(10)));
    BOOST_REQUIRE(rt.contains(tok(11)));
    BOOST_REQUIRE(rt.contains(tok(20)));
    BOOST_REQUIRE(!rt.contains(tok(21)));

    token_range_tombstone_list l;
    l.apply(rt);
    BOOST_REQUIRE(!l.search(tok(10)));
    BOOST_REQUIRE(l.search(tok(11)));
    BOOST_REQUIRE(l.search(tok(20)));
    BOOST_REQUIRE(!l.search(tok(21)));
}

BOOST_AUTO_TEST_CASE(test_full_ring_covers_every_token) {
    auto rt = token_range_tombstone::full_ring(tomb(1));
    BOOST_REQUIRE(!rt.empty());
    BOOST_REQUIRE(rt.contains(dht::token::first()));
    BOOST_REQUIRE(rt.contains(dht::token::last()));
    BOOST_REQUIRE(rt.contains(tok(0)));

    token_range_tombstone_list l;
    l.apply(rt);
    BOOST_REQUIRE_EQUAL(l.size(), 1u);
    BOOST_REQUIRE_EQUAL(l.search(dht::token::first()), tomb(1));
    BOOST_REQUIRE_EQUAL(l.search(dht::token::last()), tomb(1));
}

BOOST_AUTO_TEST_CASE(test_empty_tombstones_are_ignored) {
    token_range_tombstone_list l;
    l.apply(trt(10, 20, api::missing_timestamp));   // no deletion
    l.apply(token_range_tombstone(tok(30), tok(30), tomb(1)));  // no tokens
    l.apply(token_range_tombstone(tok(50), tok(40), tomb(1)));  // reversed
    BOOST_REQUIRE(l.empty());
}

BOOST_AUTO_TEST_CASE(test_disjoint_ranges_are_kept_apart) {
    token_range_tombstone_list l;
    l.apply(trt(30, 40, 1));
    l.apply(trt(10, 20, 1));
    BOOST_REQUIRE_EQUAL(l.size(), 2u);
    BOOST_REQUIRE_EQUAL(l.begin()->start_exclusive(), tok(10));
    verify(l, {trt(10, 20, 1), trt(30, 40, 1)}, 5, 45);
}

BOOST_AUTO_TEST_CASE(test_adjacent_ranges_with_equal_tombstone_are_coalesced) {
    token_range_tombstone_list l;
    l.apply(trt(10, 20, 1));
    l.apply(trt(20, 30, 1));
    BOOST_REQUIRE_EQUAL(l.size(), 1u);
    BOOST_REQUIRE_EQUAL(l.begin()->start_exclusive(), tok(10));
    BOOST_REQUIRE_EQUAL(l.begin()->end_inclusive(), tok(30));

    // Filling a hole between two equal tombstones coalesces all three.
    token_range_tombstone_list l2;
    l2.apply(trt(10, 20, 1));
    l2.apply(trt(30, 40, 1));
    l2.apply(trt(20, 30, 1));
    BOOST_REQUIRE_EQUAL(l2.size(), 1u);
    BOOST_REQUIRE_EQUAL(l2.begin()->start_exclusive(), tok(10));
    BOOST_REQUIRE_EQUAL(l2.begin()->end_inclusive(), tok(40));
}

BOOST_AUTO_TEST_CASE(test_adjacent_ranges_with_different_tombstones_are_kept_apart) {
    token_range_tombstone_list l;
    l.apply(trt(10, 20, 1));
    l.apply(trt(20, 30, 2));
    BOOST_REQUIRE_EQUAL(l.size(), 2u);
    verify(l, {trt(10, 20, 1), trt(20, 30, 2)}, 5, 35);
}

BOOST_AUTO_TEST_CASE(test_newer_tombstone_overwrites_older) {
    token_range_tombstone_list l;
    l.apply(trt(10, 40, 1));
    l.apply(trt(20, 30, 2));
    verify(l, {trt(10, 40, 1), trt(20, 30, 2)}, 5, 45);
    BOOST_REQUIRE_EQUAL(l.size(), 3u);
    BOOST_REQUIRE_EQUAL(l.search(tok(15)), tomb(1));
    BOOST_REQUIRE_EQUAL(l.search(tok(25)), tomb(2));
    BOOST_REQUIRE_EQUAL(l.search(tok(35)), tomb(1));
}

BOOST_AUTO_TEST_CASE(test_older_tombstone_does_not_overwrite_newer) {
    token_range_tombstone_list l;
    l.apply(trt(20, 30, 2));
    l.apply(trt(10, 40, 1));
    verify(l, {trt(10, 40, 1), trt(20, 30, 2)}, 5, 45);
    BOOST_REQUIRE_EQUAL(l.size(), 3u);
}

BOOST_AUTO_TEST_CASE(test_covering_tombstone_absorbs_older_ones) {
    token_range_tombstone_list l;
    l.apply(trt(10, 20, 1));
    l.apply(trt(30, 40, 1));
    l.apply(trt(50, 60, 2));
    l.apply(trt(0, 100, 3));
    BOOST_REQUIRE_EQUAL(l.size(), 1u);
    BOOST_REQUIRE_EQUAL(l.begin()->start_exclusive(), tok(0));
    BOOST_REQUIRE_EQUAL(l.begin()->end_inclusive(), tok(100));
    BOOST_REQUIRE_EQUAL(l.begin()->tomb(), tomb(3));
}

BOOST_AUTO_TEST_CASE(test_apply_is_idempotent) {
    token_range_tombstone_list l;
    l.apply(trt(10, 40, 1));
    l.apply(trt(20, 30, 2));
    auto expected = l;
    l.apply(trt(10, 40, 1));
    l.apply(trt(20, 30, 2));
    BOOST_REQUIRE(l.equal(expected));
}

BOOST_AUTO_TEST_CASE(test_apply_is_commutative_and_associative) {
    std::vector<token_range_tombstone> rts = {
        trt(10, 40, 1), trt(20, 30, 2), trt(35, 55, 2), trt(0, 15, 3), trt(50, 70, 1), trt(70, 80, 1),
    };
    std::mt19937 rnd(42);
    token_range_tombstone_list reference;
    for (auto&& rt : rts) {
        reference.apply(rt);
    }
    verify(reference, rts, -5, 85);
    for (int i = 0; i < 100; ++i) {
        auto shuffled = rts;
        std::shuffle(shuffled.begin(), shuffled.end(), rnd);
        // Apply in a random order, in random groupings, to also exercise
        // list-into-list merges.
        token_range_tombstone_list l;
        size_t k = 0;
        while (k < shuffled.size()) {
            size_t n = 1 + (rnd() % 3);
            token_range_tombstone_list group;
            for (size_t m = 0; m < n && k < shuffled.size(); ++m, ++k) {
                group.apply(shuffled[k]);
            }
            l.apply(group);
        }
        BOOST_REQUIRE_MESSAGE(l.equal(reference), fmt::format("{} != {}", l, reference));
    }
}

BOOST_AUTO_TEST_CASE(test_random_applies_agree_with_brute_force) {
    std::mt19937 rnd(7);
    for (int i = 0; i < 200; ++i) {
        token_range_tombstone_list l;
        std::vector<token_range_tombstone> applied;
        for (int j = 0; j < 20; ++j) {
            int64_t a = rnd() % 40;
            int64_t b = rnd() % 40;
            auto rt = trt(std::min(a, b), std::max(a, b), 1 + (rnd() % 4));
            applied.push_back(rt);
            l.apply(rt);
            verify(l, applied, -2, 42);
        }
    }
}

BOOST_AUTO_TEST_CASE(test_slice) {
    token_range_tombstone_list l;
    l.apply(trt(10, 20, 1));
    l.apply(trt(30, 40, 2));

    auto all = l.slice(dht::token::minimum(), dht::token::maximum());
    BOOST_REQUIRE(all.equal(l));

    auto s = l.slice(tok(15), tok(35));
    BOOST_REQUIRE_EQUAL(s.size(), 2u);
    BOOST_REQUIRE_EQUAL(s.begin()->start_exclusive(), tok(15));
    BOOST_REQUIRE_EQUAL(s.begin()->end_inclusive(), tok(20));
    BOOST_REQUIRE_EQUAL(std::next(s.begin())->start_exclusive(), tok(30));
    BOOST_REQUIRE_EQUAL(std::next(s.begin())->end_inclusive(), tok(35));

    // A slice which touches an entry only at its exclusive start is empty.
    BOOST_REQUIRE(l.slice(tok(20), tok(30)).empty());
    BOOST_REQUIRE(l.slice(tok(0), tok(5)).empty());
}

BOOST_AUTO_TEST_CASE(test_purge) {
    auto old = gc_clock::time_point(gc_clock::duration(1000));
    auto recent = gc_clock::time_point(gc_clock::duration(2000));
    token_range_tombstone_list l;
    l.apply(token_range_tombstone(tok(10), tok(20), tombstone(1, old)));
    l.apply(token_range_tombstone(tok(30), tok(40), tombstone(2, recent)));
    l.purge(gc_clock::time_point(gc_clock::duration(1500)));
    BOOST_REQUIRE_EQUAL(l.size(), 1u);
    BOOST_REQUIRE_EQUAL(l.begin()->start_exclusive(), tok(30));
}

BOOST_AUTO_TEST_CASE(test_max_tombstone) {
    token_range_tombstone_list l;
    l.apply(trt(10, 20, 1));
    l.apply(trt(30, 40, 5));
    l.apply(trt(50, 60, 3));
    BOOST_REQUIRE_EQUAL(l.max_tombstone(), tomb(5));
}

SEASTAR_TEST_CASE(test_apply_to_mutation) {
    return seastar::async([] {
        auto s = schema_builder(1, "ks", "cf")
                .with_column("pk", int32_type, column_kind::partition_key)
                .with_column("v", int32_type, column_kind::regular_column)
                .build();

        auto make = [&] (int32_t pk, api::timestamp_type ts) {
            auto key = partition_key::from_single_value(*s, int32_type->decompose(pk));
            mutation m(s, key);
            m.set_clustered_cell(clustering_key::make_empty(), "v", data_value(pk), ts);
            return m;
        };

        auto m = make(1, 100);
        auto t = m.token();

        // A tombstone which does not cover the mutation's token leaves it alone.
        {
            auto m2 = m;
            m2.apply(token_range_tombstone(t, dht::token::maximum(), tomb(200)));
            BOOST_REQUIRE(m2 == m);
        }

        // A tombstone which covers the token acts exactly like a partition
        // tombstone with the same timestamp.
        {
            auto expected = m;
            expected.partition().apply(tomb(200));

            auto m2 = m;
            m2.apply(token_range_tombstone::full_ring(tomb(200)));
            BOOST_REQUIRE(m2 == expected);
            BOOST_REQUIRE_EQUAL(m2.live_row_count(), 0u);

            // Going through the list gives the same result.
            auto m3 = m;
            token_range_tombstone_list l;
            l.apply(token_range_tombstone::full_ring(tomb(200)));
            l.apply_to(m3);
            BOOST_REQUIRE(m3 == expected);
        }

        // An older tombstone does not delete newer data.
        {
            auto m2 = m;
            m2.apply(token_range_tombstone::full_ring(tomb(50)));
            BOOST_REQUIRE_EQUAL(m2.live_row_count(), 1u);
        }

        // Applying a token range tombstone commutes with merging mutations:
        // it does not matter whether the deletion is applied to the parts or
        // to the whole.
        {
            auto m1 = make(1, 100);
            auto m2 = make(1, 300);
            auto trt = token_range_tombstone::full_ring(tomb(200));

            auto whole = m1;
            whole.apply(m2);
            whole.apply(trt);

            auto parts = m1;
            parts.apply(trt);
            auto m2_deleted = m2;
            m2_deleted.apply(trt);
            parts.apply(m2_deleted);

            BOOST_REQUIRE(whole == parts);
            // The newer write survives the deletion, the older one does not.
            BOOST_REQUIRE_EQUAL(whole.live_row_count(), 1u);
        }
    });
}
