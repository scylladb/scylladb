/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <seastar/testing/thread_test_case.hh>
#include <seastar/testing/test_case.hh>

#include "utils/lru.hh"

#include <vector>

using namespace seastar;

// Test entry representing a row cache data entry.
class test_data_entry final : public evictable {
public:
    int id;
    bool evicted = false;

    explicit test_data_entry(int id) : id(id) {}

    void on_evicted() noexcept override {
        evicted = true;
    }
};

// Test entry representing an index cache entry (formerly index_evictable).
// Now just a plain evictable — no separate list, no special treatment.
class test_index_entry final : public evictable {
public:
    int id;
    bool evicted = false;

    explicit test_index_entry(int id) : id(id) {}

    void on_evicted() noexcept override {
        evicted = true;
    }
};

// Basic: entries are evicted in LRU order.
SEASTAR_THREAD_TEST_CASE(test_lru_basic_eviction_order) {
    lru l;
    test_data_entry e1(1), e2(2), e3(3);

    l.add(e1);
    l.add(e2);
    l.add(e3);

    // e1 is oldest, should be evicted first
    l.evict();
    BOOST_REQUIRE(e1.evicted);
    BOOST_REQUIRE(!e2.evicted);
    BOOST_REQUIRE(!e3.evicted);

    l.evict();
    BOOST_REQUIRE(e2.evicted);
    BOOST_REQUIRE(!e3.evicted);

    l.evict();
    BOOST_REQUIRE(e3.evicted);
}

// Touch moves entry to back of LRU.
SEASTAR_THREAD_TEST_CASE(test_lru_touch_moves_to_back) {
    lru l;
    test_data_entry e1(1), e2(2), e3(3);

    l.add(e1);
    l.add(e2);
    l.add(e3);

    // Touch e1 — should now be last to evict
    l.touch(e1);

    l.evict();
    BOOST_REQUIRE(!e1.evicted);
    BOOST_REQUIRE(e2.evicted);

    l.evict();
    BOOST_REQUIRE(!e1.evicted);
    BOOST_REQUIRE(e3.evicted);

    l.evict();
    BOOST_REQUIRE(e1.evicted);
}

// Index entries and data entries share the same LRU and evict in recency order.
SEASTAR_THREAD_TEST_CASE(test_unified_lru_index_and_data_interleaved) {
    lru l;
    test_data_entry d1(1), d2(2);
    test_index_entry i1(10), i2(20);

    // Interleave insertions: d1, i1, d2, i2
    l.add(d1);
    l.add(i1);
    l.add(d2);
    l.add(i2);

    // Eviction should follow insertion order (LRU)
    l.evict();
    BOOST_REQUIRE(d1.evicted);
    BOOST_REQUIRE(!i1.evicted);

    l.evict();
    BOOST_REQUIRE(i1.evicted);
    BOOST_REQUIRE(!d2.evicted);

    l.evict();
    BOOST_REQUIRE(d2.evicted);
    BOOST_REQUIRE(!i2.evicted);

    l.evict();
    BOOST_REQUIRE(i2.evicted);
}

// Hot index entries survive while cold data entries are evicted.
// This proves that without the 20% cap, frequently-touched index pages
// naturally stay cached.
SEASTAR_THREAD_TEST_CASE(test_hot_index_survives_over_cold_data) {
    lru l;

    // Simulate: 3 cold data entries + 1 hot index entry
    test_data_entry cold1(1), cold2(2), cold3(3);
    test_index_entry hot_index(100);

    l.add(cold1);
    l.add(hot_index);
    l.add(cold2);
    l.add(cold3);

    // Touch the hot index entry to make it most-recently-used
    l.touch(hot_index);

    // Evict 3 entries — all cold data should go, hot index survives
    l.evict();
    l.evict();
    l.evict();

    BOOST_REQUIRE(cold1.evicted);
    BOOST_REQUIRE(cold2.evicted);
    BOOST_REQUIRE(cold3.evicted);
    BOOST_REQUIRE(!hot_index.evicted);

    // Clean up
    l.remove(hot_index);
}

// Cold index entries are evicted before hot data entries.
// This proves scan protection: one-shot index reads don't displace hot rows.
SEASTAR_THREAD_TEST_CASE(test_cold_index_evicted_before_hot_data) {
    lru l;

    test_data_entry hot_data(1);
    test_index_entry cold_idx1(10), cold_idx2(20), cold_idx3(30);

    // Insert hot data first, then a bunch of cold index entries (simulating a scan)
    l.add(hot_data);
    l.add(cold_idx1);
    l.add(cold_idx2);
    l.add(cold_idx3);

    // Touch hot data — simulates repeated reads
    l.touch(hot_data);

    // Evict — cold index entries should go first
    l.evict();
    BOOST_REQUIRE(cold_idx1.evicted);
    BOOST_REQUIRE(!hot_data.evicted);

    l.evict();
    BOOST_REQUIRE(cold_idx2.evicted);
    BOOST_REQUIRE(!hot_data.evicted);

    l.evict();
    BOOST_REQUIRE(cold_idx3.evicted);
    BOOST_REQUIRE(!hot_data.evicted);

    // Only now does the hot data get evicted
    l.evict();
    BOOST_REQUIRE(hot_data.evicted);
}

// When index working set exceeds 20% but is genuinely hot, it should all survive.
// With the old hard cap, this would have forced eviction of useful index pages.
SEASTAR_THREAD_TEST_CASE(test_index_exceeds_old_cap_survives_when_hot) {
    lru l;

    // Simulate 8 data entries and 4 index entries.
    // Index = 33% of entries — exceeds the old 20% cap.
    // All index entries are hot (touched after insertion).
    std::vector<test_data_entry> data;
    std::vector<test_index_entry> index;
    data.reserve(8);
    index.reserve(4);

    for (int i = 0; i < 8; ++i) {
        data.emplace_back(i);
    }
    for (int i = 0; i < 4; ++i) {
        index.emplace_back(100 + i);
    }

    // Insert all
    for (auto& d : data) l.add(d);
    for (auto& idx : index) l.add(idx);

    // Touch all index entries (they are hot)
    for (auto& idx : index) l.touch(idx);

    // Evict 8 entries — should evict all data (cold), keep all index (hot)
    for (int i = 0; i < 8; ++i) {
        l.evict();
    }

    for (auto& d : data) {
        BOOST_REQUIRE_MESSAGE(d.evicted, "cold data entry " << d.id << " should be evicted");
    }
    for (auto& idx : index) {
        BOOST_REQUIRE_MESSAGE(!idx.evicted, "hot index entry " << idx.id << " should survive");
    }

    // Clean up
    for (auto& idx : index) l.remove(idx);
}

// Regression test: when index is cold, it's evicted quickly even without the hard cap.
// This matches the old behavior where cold index pages were evicted when data pressure appeared.
SEASTAR_THREAD_TEST_CASE(test_cold_index_evicted_under_pressure) {
    lru l;

    // Insert 4 cold index entries, then 4 hot data entries
    std::vector<test_index_entry> cold_index;
    std::vector<test_data_entry> hot_data;
    cold_index.reserve(4);
    hot_data.reserve(4);

    for (int i = 0; i < 4; ++i) cold_index.emplace_back(i);
    for (int i = 0; i < 4; ++i) hot_data.emplace_back(100 + i);

    for (auto& idx : cold_index) l.add(idx);
    for (auto& d : hot_data) l.add(d);

    // Touch data entries to keep them hot
    for (auto& d : hot_data) l.touch(d);

    // Evict 4 — all cold index should go
    for (int i = 0; i < 4; ++i) l.evict();

    for (auto& idx : cold_index) {
        BOOST_REQUIRE_MESSAGE(idx.evicted, "cold index " << idx.id << " should be evicted");
    }
    for (auto& d : hot_data) {
        BOOST_REQUIRE_MESSAGE(!d.evicted, "hot data " << d.id << " should survive");
    }

    // Clean up
    for (auto& d : hot_data) l.remove(d);
}

// add_before preserves eviction ordering.
SEASTAR_THREAD_TEST_CASE(test_add_before_ordering) {
    lru l;
    test_data_entry e1(1), e2(2), e3(3);

    l.add(e1);
    l.add(e3);

    // Insert e2 before e3 — eviction order should be e1, e2, e3
    l.add_before(e3, e2);

    l.evict();
    BOOST_REQUIRE(e1.evicted);
    BOOST_REQUIRE(!e2.evicted);
    BOOST_REQUIRE(!e3.evicted);

    l.evict();
    BOOST_REQUIRE(e2.evicted);
    BOOST_REQUIRE(!e3.evicted);

    l.evict();
    BOOST_REQUIRE(e3.evicted);
}

// evict_all empties the LRU completely.
SEASTAR_THREAD_TEST_CASE(test_evict_all) {
    lru l;
    test_data_entry e1(1), e2(2);
    test_index_entry i1(10);

    l.add(e1);
    l.add(i1);
    l.add(e2);

    l.evict_all();

    BOOST_REQUIRE(e1.evicted);
    BOOST_REQUIRE(i1.evicted);
    BOOST_REQUIRE(e2.evicted);
}

// Evicting from empty LRU returns reclaimed_nothing.
SEASTAR_THREAD_TEST_CASE(test_evict_empty) {
    lru l;
    auto result = l.evict();
    BOOST_REQUIRE(result == memory::reclaiming_result::reclaimed_nothing);
}

// Remove prevents eviction of an entry.
SEASTAR_THREAD_TEST_CASE(test_remove_prevents_eviction) {
    lru l;
    test_data_entry e1(1), e2(2), e3(3);

    l.add(e1);
    l.add(e2);
    l.add(e3);

    // Remove e1 from LRU — it won't be evicted
    l.remove(e1);

    l.evict();
    BOOST_REQUIRE(!e1.evicted);
    BOOST_REQUIRE(e2.evicted); // e2 is now the oldest

    l.evict();
    BOOST_REQUIRE(e3.evicted);

    // LRU should be empty now
    auto result = l.evict();
    BOOST_REQUIRE(result == memory::reclaiming_result::reclaimed_nothing);
}
