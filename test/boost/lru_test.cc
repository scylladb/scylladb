/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#define BOOST_TEST_MODULE lru

#include <boost/test/unit_test.hpp>
#include <vector>
#include <algorithm>
#include <memory>

#include "utils/count_min_sketch.hh"
#include "utils/lru.hh"

// A concrete evictable for testing.
struct test_evictable : public evictable {
    int id;
    bool was_evicted = false;

    explicit test_evictable(int id) : id(id) {}

    void on_evicted() noexcept override {
        was_evicted = true;
    }

    ~test_evictable() {
        // Ensure unlinked before destruction.
    }
};

// ---------------------------------------------------------------------------
// Count-Min Sketch Tests
// ---------------------------------------------------------------------------

BOOST_AUTO_TEST_CASE(test_count_min_sketch_basic) {
    utils::count_min_sketch sketch(10); // width = 1024

    // An unseen key should have estimate 0.
    BOOST_REQUIRE_EQUAL(sketch.estimate(42), 0);

    sketch.increment(42);
    BOOST_REQUIRE_EQUAL(sketch.estimate(42), 1);

    sketch.increment(42);
    sketch.increment(42);
    BOOST_REQUIRE_EQUAL(sketch.estimate(42), 3);

    // A different key should be independent.
    BOOST_REQUIRE_EQUAL(sketch.estimate(100), 0);
    sketch.increment(100);
    BOOST_REQUIRE_EQUAL(sketch.estimate(100), 1);
    BOOST_REQUIRE_EQUAL(sketch.estimate(42), 3);
}

BOOST_AUTO_TEST_CASE(test_count_min_sketch_max_counter) {
    utils::count_min_sketch sketch(10);

    for (int i = 0; i < 20; ++i) {
        sketch.increment(1);
    }
    // 4-bit counter caps at 15.
    BOOST_REQUIRE_EQUAL(sketch.estimate(1), 15);
}

BOOST_AUTO_TEST_CASE(test_count_min_sketch_reset) {
    utils::count_min_sketch sketch(10);

    sketch.increment(1);
    sketch.increment(1);
    sketch.increment(1);
    sketch.increment(1); // freq = 4
    BOOST_REQUIRE_EQUAL(sketch.estimate(1), 4);

    sketch.reset(); // halve → 2
    BOOST_REQUIRE_EQUAL(sketch.estimate(1), 2);

    sketch.reset(); // halve → 1
    BOOST_REQUIRE_EQUAL(sketch.estimate(1), 1);

    sketch.reset(); // halve → 0
    BOOST_REQUIRE_EQUAL(sketch.estimate(1), 0);
}

// ---------------------------------------------------------------------------
// W-TinyLFU LRU Tests
// ---------------------------------------------------------------------------

BOOST_AUTO_TEST_CASE(test_lru_add_and_evict) {
    lru l;
    test_evictable e1(1), e2(2), e3(3);

    l.add(e1);
    l.add(e2);
    l.add(e3);

    BOOST_REQUIRE(e1.is_linked());
    BOOST_REQUIRE(e2.is_linked());
    BOOST_REQUIRE(e3.is_linked());

    // Evict removes at least one entry.
    auto r = l.evict();
    BOOST_REQUIRE(r == seastar::memory::reclaiming_result::reclaimed_something);

    // At least one entry should have been evicted.
    int evicted_count = (e1.was_evicted ? 1 : 0) + (e2.was_evicted ? 1 : 0) + (e3.was_evicted ? 1 : 0);
    BOOST_REQUIRE_GE(evicted_count, 1);

    // Clean up remaining linked entries.
    if (e1.is_linked()) l.remove(e1);
    if (e2.is_linked()) l.remove(e2);
    if (e3.is_linked()) l.remove(e3);
}

BOOST_AUTO_TEST_CASE(test_lru_evict_empty) {
    lru l;
    auto r = l.evict();
    BOOST_REQUIRE(r == seastar::memory::reclaiming_result::reclaimed_nothing);
}

BOOST_AUTO_TEST_CASE(test_lru_touch_keeps_entry_alive) {
    lru l;

    // Create entries with different access patterns.
    test_evictable hot(1), cold1(2), cold2(3);

    l.add(hot);
    l.add(cold1);
    l.add(cold2);

    // Touch 'hot' many times to build frequency.
    for (int i = 0; i < 10; ++i) {
        l.touch(hot);
    }

    // Evict all - the hot entry may survive longer than cold entries.
    l.evict();
    l.evict();

    // Hot entry should still be linked (survived eviction of cold entries).
    BOOST_REQUIRE(hot.is_linked());

    // Clean up.
    l.remove(hot);
    // cold entries may or may not still be linked, clean up if needed.
    if (cold1.is_linked()) l.remove(cold1);
    if (cold2.is_linked()) l.remove(cold2);
}

BOOST_AUTO_TEST_CASE(test_lru_evict_all) {
    lru l;
    test_evictable e1(1), e2(2), e3(3);

    l.add(e1);
    l.add(e2);
    l.add(e3);

    l.evict_all();

    BOOST_REQUIRE(!e1.is_linked());
    BOOST_REQUIRE(!e2.is_linked());
    BOOST_REQUIRE(!e3.is_linked());
    BOOST_REQUIRE(e1.was_evicted);
    BOOST_REQUIRE(e2.was_evicted);
    BOOST_REQUIRE(e3.was_evicted);
}

BOOST_AUTO_TEST_CASE(test_lru_remove) {
    lru l;
    test_evictable e1(1), e2(2), e3(3);

    l.add(e1);
    l.add(e2);
    l.add(e3);

    l.remove(e2);
    BOOST_REQUIRE(!e2.is_linked());
    BOOST_REQUIRE(!e2.was_evicted); // remove does not call on_evicted

    l.evict_all();
    BOOST_REQUIRE(e1.was_evicted);
    BOOST_REQUIRE(e3.was_evicted);
}

BOOST_AUTO_TEST_CASE(test_lru_add_before) {
    lru l;
    test_evictable e1(1), e2(2), e3(3);

    l.add(e1);
    l.add(e2);

    // Insert e3 before e2 so e3 is evicted before e2.
    l.add_before(e2, e3);

    BOOST_REQUIRE(e1.is_linked());
    BOOST_REQUIRE(e2.is_linked());
    BOOST_REQUIRE(e3.is_linked());

    // Clean up.
    l.evict_all();
}

BOOST_AUTO_TEST_CASE(test_lru_frequency_based_eviction) {
    lru l;

    // Create entries with different access patterns.
    // Use a fixed-size array to avoid move construction issues.
    static constexpr int N = 20;
    std::unique_ptr<test_evictable> entries[N];
    for (int i = 0; i < N; ++i) {
        entries[i] = std::make_unique<test_evictable>(i);
    }

    for (int i = 0; i < N; ++i) {
        l.add(*entries[i]);
    }

    // Touch entries 15-19 many times (they should be "hot").
    for (int round = 0; round < 10; ++round) {
        for (int i = 15; i < N; ++i) {
            l.touch(*entries[i]);
        }
    }

    // Evict half the entries.
    for (int i = 0; i < 10; ++i) {
        l.evict();
    }

    // Hot entries (15-19) should still be linked.
    for (int i = 15; i < N; ++i) {
        BOOST_REQUIRE_MESSAGE(entries[i]->is_linked(),
            "Hot entry " << i << " should survive eviction");
    }

    // Clean up remaining entries.
    for (int i = 0; i < N; ++i) {
        if (entries[i]->is_linked()) {
            l.remove(*entries[i]);
        }
    }
}

BOOST_AUTO_TEST_CASE(test_lru_touch_promotes_from_probation) {
    lru l;

    // Create entries.
    static constexpr int N = 10;
    std::unique_ptr<test_evictable> entries[N];
    for (int i = 0; i < N; ++i) {
        entries[i] = std::make_unique<test_evictable>(i);
    }
    for (int i = 0; i < N; ++i) {
        l.add(*entries[i]);
    }

    // Evict and re-add some to force entries into probation via the eviction logic.
    // The eviction drains excess from window to probation.
    // After enough evictions, remaining entries should be in probation or protected.

    // Touch entries 0-4 multiple times to build frequency.
    for (int round = 0; round < 5; ++round) {
        for (int i = 0; i < 5; ++i) {
            l.touch(*entries[i]);
        }
    }

    // Evict 5 entries - cold entries (5-9) should be evicted.
    for (int i = 0; i < 5; ++i) {
        l.evict();
    }

    // Entries 0-4 (frequently touched) should survive.
    for (int i = 0; i < 5; ++i) {
        BOOST_REQUIRE_MESSAGE(entries[i]->is_linked(),
            "Frequently touched entry " << i << " should survive eviction");
    }

    // Clean up.
    for (int i = 0; i < N; ++i) {
        if (entries[i]->is_linked()) {
            l.remove(*entries[i]);
        }
    }
}
