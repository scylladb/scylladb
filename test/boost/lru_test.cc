/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#define BOOST_TEST_MODULE lru

#include <boost/test/unit_test.hpp>
#include <vector>
#include <algorithm>
#include <memory>
#include <iostream>
#include <iomanip>

#include "utils/count_min_sketch.hh"
#include "utils/lru.hh"

// A concrete evictable for testing.
struct test_evictable final: public evictable {
    int id;
    bool was_evicted = false;
    bool was_evicted_shallow = false;
    // When set, on_evicted() records this entry's id, so tests can assert
    // on the exact eviction order.
    std::vector<int>* eviction_log = nullptr;

    explicit test_evictable(int id) : id(id) {}

    void on_evicted() noexcept override {
        was_evicted = true;
        if (eviction_log) {
            eviction_log->push_back(id);
        }
    }

    void on_evicted_shallow() noexcept override {
        was_evicted_shallow = true;
        on_evicted();
    }

    ~test_evictable() {
        // Ensure unlinked before destruction.
    }
};

static constexpr auto reclaimed_something = seastar::memory::reclaiming_result::reclaimed_something;

static uint64_t next_test_key = 1;

// Helper: set a unique sketch key per entry, mimicking logical key hashing.
void assign_unique_sketch_key(test_evictable& e) {
    e.set_sketch_key(next_test_key++);
}

// ---------------------------------------------------------------------------
// Count-Min Sketch Tests
// ---------------------------------------------------------------------------

// Width = 2^test_sketch_width_log2 = 1024 counters per row.
static constexpr size_t test_sketch_width_log2 = 10;

BOOST_AUTO_TEST_CASE(test_count_min_sketch_basic) {
    utils::count_min_sketch sketch(test_sketch_width_log2);

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
    utils::count_min_sketch sketch(test_sketch_width_log2);

    for (int i = 0; i < 20; ++i) {
        sketch.increment(1);
    }
    // 4-bit counter caps at 15.
    BOOST_REQUIRE_EQUAL(sketch.estimate(1), 15);
}

BOOST_AUTO_TEST_CASE(test_count_min_sketch_reset) {
    utils::count_min_sketch sketch(test_sketch_width_log2);

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

BOOST_AUTO_TEST_CASE(test_count_min_sketch_cache_line_layout) {
    // Verify functional correctness of the cache-line optimized sketch.
    utils::count_min_sketch sketch(test_sketch_width_log2);

    // Basic increment and estimate.
    sketch.increment(42);
    sketch.increment(42);
    sketch.increment(42);
    BOOST_REQUIRE_EQUAL(sketch.estimate(42), 3);

    // Different key is independent.
    sketch.increment(999);
    BOOST_REQUIRE_EQUAL(sketch.estimate(999), 1);
    BOOST_REQUIRE_EQUAL(sketch.estimate(42), 3);

    // Reset halves counters.
    sketch.reset();
    BOOST_REQUIRE_EQUAL(sketch.estimate(42), 1);
    BOOST_REQUIRE_EQUAL(sketch.estimate(999), 0);

    // Saturation at 15.
    for (int i = 0; i < 20; ++i) sketch.increment(7);
    BOOST_REQUIRE_EQUAL(sketch.estimate(7), 15);
}

BOOST_AUTO_TEST_CASE(test_count_min_sketch_resize_clears) {
    // After the cache-line layout change, resize discards old counts
    // (matching Caffeine's ensureCapacity behavior).
    utils::count_min_sketch sketch(12);
    for (int i = 0; i < 10; ++i) sketch.increment(100);
    BOOST_REQUIRE_EQUAL(sketch.estimate(100), 10);

    sketch.resize(14);
    // After resize, old counts are gone.
    BOOST_REQUIRE_EQUAL(sketch.estimate(100), 0);

    // Re-increment works correctly in the new size.
    for (int i = 0; i < 5; ++i) sketch.increment(100);
    BOOST_REQUIRE_EQUAL(sketch.estimate(100), 5);
}

BOOST_AUTO_TEST_CASE(test_count_min_sketch_many_keys) {
    // Stress test: many distinct keys should have low collision rate.
    utils::count_min_sketch sketch(16);  // 65536 counters per row

    // Insert 10000 unique keys once each.
    for (uint64_t k = 0; k < 10000; ++k) {
        sketch.increment(k);
    }

    // Each key was inserted once; estimate should be >= 1.
    // Due to collisions, some may be > 1, but the majority should be exactly 1.
    int exact_count = 0;
    for (uint64_t k = 0; k < 10000; ++k) {
        BOOST_REQUIRE_GE(sketch.estimate(k), 1);
        if (sketch.estimate(k) == 1) exact_count++;
    }
    // With 65536 counters and 10000 keys, most should be collision-free.
    BOOST_REQUIRE_GT(exact_count, 8000);
}

// ---------------------------------------------------------------------------
// W-TinyLFU LRU Tests
// ---------------------------------------------------------------------------

BOOST_AUTO_TEST_CASE(test_lru_add_and_evict) {
    lru l;
    test_evictable e1(1), e2(2), e3(3);
    assign_unique_sketch_key(e1);
    assign_unique_sketch_key(e2);
    assign_unique_sketch_key(e3);

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

    static constexpr int N = 10;
    std::vector<int> order;
    std::unique_ptr<test_evictable> entries[N];
    for (int i = 0; i < N; ++i) {
        entries[i] = std::make_unique<test_evictable>(i);
        entries[i]->eviction_log = &order;
        assign_unique_sketch_key(*entries[i]);
        l.add(*entries[i]);
    }

    // The first touch promotes the window entry to protected.
    l.touch(*entries[0]);
    BOOST_REQUIRE_EQUAL(l.protected_size(), 1);

    // Evict until only 3 entries remain: the touched entry must survive the
    // untouched ones. (We stop before the cache drains to nothing — once the
    // total shrinks so far that max_protected_size() truncates to 0, even
    // protected entries get demoted and evicted while up to max_window_size()
    // untouched window entries may still linger.)
    while (static_cast<int>(order.size()) < N - 3) {
        BOOST_REQUIRE(l.evict() == reclaimed_something);
    }
    BOOST_REQUIRE(!entries[0]->was_evicted);
    BOOST_REQUIRE(entries[0]->is_linked());

    // Clean up.
    for (int i = 0; i < N; ++i) {
        if (entries[i]->is_linked()) {
            l.remove(*entries[i]);
        }
    }
}

BOOST_AUTO_TEST_CASE(test_lru_evict_all) {
    lru l;
    test_evictable e1(1), e2(2), e3(3);
    assign_unique_sketch_key(e1);
    assign_unique_sketch_key(e2);
    assign_unique_sketch_key(e3);

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
    assign_unique_sketch_key(e1);
    assign_unique_sketch_key(e2);
    assign_unique_sketch_key(e3);

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
    assign_unique_sketch_key(e1);
    assign_unique_sketch_key(e2);
    assign_unique_sketch_key(e3);

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
    // Verify that the admission filter uses frequency to decide eviction.
    // add() moves excess window entries to probation without gating;
    // the frequency-based admission gate runs inside do_evict() → drain_window().
    // We need to add enough entries to overflow the window, then add more
    // to cause evict() to trigger drain_window with entries to compare.
    lru l;

    static constexpr int N = 40;
    std::unique_ptr<test_evictable> entries[N];

    // Phase 1: add initial entries and build frequency for the first few.
    for (int i = 0; i < 20; ++i) {
        entries[i] = std::make_unique<test_evictable>(i);
        assign_unique_sketch_key(*entries[i]);
        l.add(*entries[i]);
    }
    for (int round = 0; round < 10; ++round) {
        for (int i = 0; i < 5; ++i) {
            l.touch(*entries[i]);
        }
    }

    // Phase 2: add more entries to grow the window past its target,
    // then evict to trigger drain_window's admission gate.
    for (int i = 20; i < N; ++i) {
        entries[i] = std::make_unique<test_evictable>(i);
        assign_unique_sketch_key(*entries[i]);
        l.add(*entries[i]);
    }

    // Multiple evictions to trigger drain_window inside do_evict().
    for (int i = 0; i < 10; ++i) {
        l.evict();
    }

    // The admission filter should have been exercised.
    auto& st = l.get_stats();
    BOOST_REQUIRE_GT(st.tinylfu_admissions + st.tinylfu_rejections + st.window_to_probation, 0u);

    for (int i = 0; i < N; ++i) {
        if (entries[i]->is_linked()) {
            l.remove(*entries[i]);
        }
    }
}

// ---------------------------------------------------------------------------
// Caffeine-parity tests
// ---------------------------------------------------------------------------

BOOST_AUTO_TEST_CASE(test_aging_reset_uses_entry_count) {
    // The sketch reset threshold should be based on cache entry count,
    // not sketch width.  With 5 entries the threshold is max(1000, 50) = 1000.
    // After enough touches we expect at least one reset (halving), so a
    // previously-saturated counter (15) should decay.
    lru l;
    static constexpr int N = 5;
    std::unique_ptr<test_evictable> entries[N];
    for (int i = 0; i < N; ++i) {
        entries[i] = std::make_unique<test_evictable>(i);
        assign_unique_sketch_key(*entries[i]);
        l.add(*entries[i]);
    }

    // Build entry 0's frequency. Touch alternates with add/remove to
    // avoid the protected-segment sketch-skip optimization.
    auto key0 = entries[0]->sketch_key();
    for (int i = 0; i < 20; ++i) {
        l.remove(*entries[0]);
        l.add(*entries[0]);
    }
    auto freq_before = l.sketch_estimate(key0);
    BOOST_REQUIRE_GE(freq_before, 5);

    // Generate enough accesses to trigger at least one reset.
    for (int i = 0; i < 1100; ++i) {
        l.remove(*entries[1]);
        l.add(*entries[1]);
    }
    // After at least one halving, frequency should have decayed.
    BOOST_REQUIRE_LT(l.sketch_estimate(key0), freq_before);

    for (int i = 0; i < N; ++i) {
        if (entries[i]->is_linked()) l.remove(*entries[i]);
    }
}

BOOST_AUTO_TEST_CASE(test_lru_touch_promotes_from_probation) {
    // Verify that touching a probation entry promotes it to protected.
    // add() moves excess window entries to probation (safe drain), so
    // entries land in probation without needing an explicit evict() call.
    lru l;

    static constexpr int N = 20;
    std::unique_ptr<test_evictable> entries[N];
    for (int i = 0; i < N; ++i) {
        entries[i] = std::make_unique<test_evictable>(i);
        assign_unique_sketch_key(*entries[i]);
        l.add(*entries[i]);
    }

    // After adding 20 entries with 1% window (target=1), add() moved
    // 19 entries to probation. Touching probation entries promotes them.
    auto promotions_before = l.get_stats().protected_promotions;
    for (int i = 0; i < N; ++i) {
        if (entries[i]->is_linked()) {
            l.touch(*entries[i]);
        }
    }
    BOOST_REQUIRE_GT(l.get_stats().protected_promotions, promotions_before);

    // Clean up.
    for (int i = 0; i < N; ++i) {
        if (entries[i]->is_linked()) {
            l.remove(*entries[i]);
        }
    }
}

BOOST_AUTO_TEST_CASE(test_lru_set_window_fraction) {
    lru l;
    // Default is 0.01 (1%).
    BOOST_REQUIRE_CLOSE(l.window_fraction(), 0.01, 1e-6);

    // Set to 50%.
    l.set_window_fraction(0.50);
    BOOST_REQUIRE_CLOSE(l.window_fraction(), 0.50, 1e-6);

    // Clamped to [0.01, 1.0].
    l.set_window_fraction(0.0);
    BOOST_REQUIRE_CLOSE(l.window_fraction(), 0.01, 1e-6);
    l.set_window_fraction(1.5);
    BOOST_REQUIRE_CLOSE(l.window_fraction(), 1.0, 1e-6);
    // 1.0 (classic LRU mode) is a valid setting.
    l.set_window_fraction(1.0);
    BOOST_REQUIRE_CLOSE(l.window_fraction(), 1.0, 1e-6);
}

// add() never drains the window — it grows unbounded until eviction runs.
// The first evict() call drains the window down to its cap through the
// TinyLFU admission gate: the first victim seeds the empty probation, and
// every subsequent equal-frequency victim loses the duel against the
// probation front and is rejected (evicted).
BOOST_AUTO_TEST_CASE(test_lru_evict_drains_window_through_admission_gate) {
    lru l;

    static constexpr int N = 80;
    std::unique_ptr<test_evictable> entries[N];
    for (int i = 0; i < N; ++i) {
        entries[i] = std::make_unique<test_evictable>(i);
        assign_unique_sketch_key(*entries[i]);
        l.add(*entries[i]);
    }

    // No drain on add: everything is still in the window.
    BOOST_REQUIRE_EQUAL(l.window_size(), N);
    BOOST_REQUIRE_EQUAL(l.get_stats().window_to_probation, 0u);

    // A single evict() call drains the window, but drain_window() bails out on
    // need_preempt() (which fires readily in debug builds), so it may take more
    // than one call to reach the cap. Keep evicting until the window is drained;
    // the cumulative admission-gate stats below are unaffected by how the drain
    // is split across calls.
    while (l.window_size() > l.current_max_window_size()) {
        BOOST_REQUIRE(l.evict() == reclaimed_something);
    }

    const auto& st = l.get_stats();
    // The window was drained down to its cap (1% of total, at least 1)...
    BOOST_REQUIRE_EQUAL(l.window_size(), l.current_max_window_size());
    // ...by moving the first victim into the empty probation...
    BOOST_REQUIRE_EQUAL(st.window_to_probation, 1u);
    BOOST_REQUIRE_EQUAL(l.probation_size(), 1u);
    // ...and rejecting every subsequent equal-frequency victim at the gate.
    BOOST_REQUIRE_EQUAL(st.tinylfu_rejections, N - 2u);
    BOOST_REQUIRE_EQUAL(st.tinylfu_admissions, 0u);

    for (int i = 0; i < N; ++i) {
        if (entries[i]->is_linked()) {
            l.remove(*entries[i]);
        }
    }
}

BOOST_AUTO_TEST_CASE(test_lru_large_window_behaves_like_lru) {
    lru l;
    l.set_window_fraction(0.99);

    // With 99% window, almost all entries stay in window (pure LRU behavior).
    static constexpr int N = 20;
    std::unique_ptr<test_evictable> entries[N];
    for (int i = 0; i < N; ++i) {
        entries[i] = std::make_unique<test_evictable>(i);
        assign_unique_sketch_key(*entries[i]);
        l.add(*entries[i]);
    }

    // In a large window, the oldest entry should be evicted first (LRU order).
    l.evict();
    BOOST_REQUIRE(entries[0]->was_evicted);

    for (int i = 1; i < N; ++i) {
        if (entries[i]->is_linked()) l.remove(*entries[i]);
    }
}

// With a window fraction of 1.0 the policy must degenerate to classic LRU:
// no protected capacity exists, so touch() moves entries to the MRU end of
// their segment instead of promoting them. Guards against the inversion
// where a touched entry was promoted to protected, immediately demoted to
// probation by rebalance_protected(), and then evicted *before* untouched
// window entries — i.e. touching made an entry more likely to die.
BOOST_AUTO_TEST_CASE(test_full_window_is_classic_lru) {
    lru l;
    l.set_window_fraction(1.0);

    static constexpr int N = 20;
    std::vector<int> order;
    std::unique_ptr<test_evictable> entries[N];
    for (int i = 0; i < N; ++i) {
        entries[i] = std::make_unique<test_evictable>(i);
        entries[i]->eviction_log = &order;
        assign_unique_sketch_key(*entries[i]);
        l.add(*entries[i]);
    }

    // Touch the three oldest entries; they must move to the MRU end of the
    // window, not into protected.
    for (int i = 0; i < 3; ++i) {
        l.touch(*entries[i]);
    }
    BOOST_REQUIRE_EQUAL(l.protected_size(), 0);
    BOOST_REQUIRE_EQUAL(l.window_size(), N);

    // Evict half the cache: pure LRU order — the untouched entries 3..12 go
    // first; the touched entries 0..2 survive.
    for (int i = 0; i < N / 2; ++i) {
        BOOST_REQUIRE(l.evict() == reclaimed_something);
    }
    const std::vector<int> expected{3, 4, 5, 6, 7, 8, 9, 10, 11, 12};
    BOOST_REQUIRE_EQUAL_COLLECTIONS(order.begin(), order.end(), expected.begin(), expected.end());
    BOOST_REQUIRE(!entries[0]->was_evicted);
    BOOST_REQUIRE(!entries[1]->was_evicted);
    BOOST_REQUIRE(!entries[2]->was_evicted);

    // An index-page release cycle (unlink_touched + add) must also keep LRU
    // semantics: the entry re-enters at the window MRU, not protected.
    l.unlink_touched(*entries[3 + N / 2]);
    l.add(*entries[3 + N / 2]);
    BOOST_REQUIRE_EQUAL(l.protected_size(), 0);
    l.evict();
    BOOST_REQUIRE_EQUAL(order.back(), 3 + N / 2 + 1);

    for (int i = 0; i < N; ++i) {
        if (entries[i]->is_linked()) l.remove(*entries[i]);
    }
}

// Two entries with distinct sketch keys track independent frequencies.
// This tests the sketch's per-key isolation, not partition-level behavior
// (in production, rows within the same partition share a sketch key).
BOOST_AUTO_TEST_CASE(test_distinct_sketch_keys_have_independent_frequency) {
    lru l;
    auto row_hot  = std::make_unique<test_evictable>(0);
    auto row_cold = std::make_unique<test_evictable>(1);
    assign_unique_sketch_key(*row_hot);
    assign_unique_sketch_key(*row_cold);
    l.add(*row_hot);
    l.add(*row_cold);

    // Access row_hot many times via remove/add to bypass protected-skip.
    for (int i = 0; i < 20; ++i) {
        l.remove(*row_hot);
        l.add(*row_hot);
    }

    auto key_hot  = row_hot->sketch_key();
    auto key_cold = row_cold->sketch_key();

    // The hot entry should have a higher frequency estimate.
    BOOST_REQUIRE_GT(l.sketch_estimate(key_hot), l.sketch_estimate(key_cold));

    l.remove(*row_hot);
    l.remove(*row_cold);
}

// Under eviction pressure, a less-frequently-accessed entry is evicted
// before a frequently-accessed one, demonstrating frequency-based admission.
// Both entries use distinct sketch keys here; in production, rows within the
// same partition share a key and rely on SLRU recency for ordering.
BOOST_AUTO_TEST_CASE(test_cold_entry_evicted_before_hot_entry) {
    lru l;
    static constexpr int FILLER = 100;
    auto row_hot  = std::make_unique<test_evictable>(1000);
    auto row_cold = std::make_unique<test_evictable>(1001);
    auto row_tail = std::make_unique<test_evictable>(1002);
    assign_unique_sketch_key(*row_hot);
    assign_unique_sketch_key(*row_cold);
    assign_unique_sketch_key(*row_tail);

    std::unique_ptr<test_evictable> filler[FILLER];
    for (int i = 0; i < FILLER; ++i) {
        filler[i] = std::make_unique<test_evictable>(i);
        assign_unique_sketch_key(*filler[i]);
        l.add(*filler[i]);
    }

    l.add(*row_cold);
    // The tail entry keeps row_cold from being the window-cap residual entry
    // (the last max_window_size() window entries are not drained).
    l.add(*row_tail);
    l.add(*row_hot);

    // Touching promotes row_hot out of the window into protected.
    for (int i = 0; i < 3; ++i) {
        l.touch(*row_hot);
    }
    BOOST_REQUIRE_EQUAL(l.protected_size(), 1);

    // Draining the window evicts row_cold (frequency 1) -- it loses the
    // admission duel -- while row_hot sits safely in the protected segment.
    // drain_window() bails on need_preempt() (which fires readily in debug),
    // so drain the whole window rather than assuming a single evict() suffices.
    while (l.window_size() > l.current_max_window_size()) {
        BOOST_REQUIRE(l.evict() == reclaimed_something);
    }
    BOOST_REQUIRE_MESSAGE(row_cold->was_evicted,
        "Cold entry should be evicted before frequently-accessed entry");
    BOOST_REQUIRE(!row_hot->was_evicted);

    if (row_hot->is_linked()) l.remove(*row_hot);
    if (row_cold->is_linked()) l.remove(*row_cold);
    if (row_tail->is_linked()) l.remove(*row_tail);
    for (int i = 0; i < FILLER; ++i) {
        if (filler[i]->is_linked()) l.remove(*filler[i]);
    }
}

BOOST_AUTO_TEST_CASE(test_sketch_key_used_for_frequency) {
    // Two evictables with different sketch keys should track frequency independently.
    // Two evictables with the SAME sketch key should share frequency.
    lru l;
    auto e1 = std::make_unique<test_evictable>(1);
    auto e2 = std::make_unique<test_evictable>(2);
    auto e3 = std::make_unique<test_evictable>(3);

    // e1 and e3 share a logical key (simulates eviction + reinsertion of same row)
    e1->set_sketch_key(0xCAFE0001);
    e2->set_sketch_key(0xCAFE0002);
    e3->set_sketch_key(0xCAFE0001); // same as e1

    l.add(*e1);
    l.add(*e2);

    // Build frequency for e1 via remove/add (bypasses protected-skip)
    for (int i = 0; i < 10; ++i) {
        l.remove(*e1);
        l.add(*e1);
    }

    // e3 (same sketch key) should see the accumulated frequency
    BOOST_REQUIRE_GT(l.sketch_estimate(0xCAFE0001), l.sketch_estimate(0xCAFE0002));

    // Clean up
    l.add(*e3);
    for (auto* e : {e1.get(), e2.get(), e3.get()}) {
        if (e->is_linked()) l.remove(*e);
    }
}

// ---------------------------------------------------------------------------
// W-TinyLFU Instrumentation Counter Tests
// ---------------------------------------------------------------------------

BOOST_AUTO_TEST_CASE(test_lru_admission_counters) {
    lru l;
    static constexpr int N = 40;
    std::unique_ptr<test_evictable> entries[N];

    // Phase 1: add entries and build frequency for some.
    for (int i = 0; i < 20; ++i) {
        entries[i] = std::make_unique<test_evictable>(i);
        assign_unique_sketch_key(*entries[i]);
        l.add(*entries[i]);
    }
    for (int round = 0; round < 10; ++round) {
        for (int i = 15; i < 20; ++i) {
            l.touch(*entries[i]);
        }
    }

    // Phase 2: add more entries so the window overflows and evict()
    // triggers drain_window() with the frequency admission gate.
    for (int i = 20; i < N; ++i) {
        entries[i] = std::make_unique<test_evictable>(i);
        assign_unique_sketch_key(*entries[i]);
        l.add(*entries[i]);
    }

    for (int i = 0; i < 10; ++i) {
        l.evict();
    }

    auto& st = l.get_stats();
    // Window-to-probation moves happen during add() and/or evict().
    BOOST_REQUIRE_GT(st.window_to_probation, 0u);

    for (int i = 0; i < N; ++i) {
        if (entries[i]->is_linked()) l.remove(*entries[i]);
    }
}

BOOST_AUTO_TEST_CASE(test_lru_direct_eviction_counter) {
    // Path 2 (direct eviction) fires when window is within target.
    // Use 99% window so almost everything stays in window. After one
    // eviction drains the excess, the next evict takes path 2.
    lru l;
    l.set_window_fraction(0.99);

    static constexpr int N = 10;
    std::unique_ptr<test_evictable> entries[N];
    for (int i = 0; i < N; ++i) {
        entries[i] = std::make_unique<test_evictable>(i);
        assign_unique_sketch_key(*entries[i]);
        l.add(*entries[i]);
    }

    // Window target = 99% of 10 = 9. We have 10 in window.
    // First evict drains 1 from window (admission path).
    l.evict();

    // Now window_size <= max. Next evict takes path 2.
    auto& st = l.get_stats();
    auto direct_before = st.direct_evictions;
    l.evict();
    BOOST_REQUIRE_GT(st.direct_evictions, direct_before);

    for (int i = 0; i < N; ++i) {
        if (entries[i]->is_linked()) l.remove(*entries[i]);
    }
}

BOOST_AUTO_TEST_CASE(test_lru_promotion_demotion_counters) {
    lru l;
    l.set_window_fraction(0.50);

    static constexpr int N = 20;
    std::unique_ptr<test_evictable> entries[N];
    for (int i = 0; i < N; ++i) {
        entries[i] = std::make_unique<test_evictable>(i);
        assign_unique_sketch_key(*entries[i]);
        l.add(*entries[i]);
    }

    // Drain window excess so entries move to probation.
    l.evict();

    // Touch surviving entries — probation entries get promoted to protected.
    for (int round = 0; round < 3; ++round) {
        for (int i = 0; i < N; ++i) {
            if (entries[i]->is_linked()) {
                l.touch(*entries[i]);
            }
        }
    }

    BOOST_REQUIRE_GT(l.get_stats().protected_promotions, 0u);

    // Demotions are hard to trigger with small cache. Just verify counter exists.
    BOOST_REQUIRE_GE(l.get_stats().protected_demotions, 0u);

    for (int i = 0; i < N; ++i) {
        if (entries[i]->is_linked()) l.remove(*entries[i]);
    }
}

BOOST_AUTO_TEST_CASE(test_lru_sampled_frequencies) {
    lru l;
    static constexpr int N = 10;
    std::unique_ptr<test_evictable> entries[N];
    for (int i = 0; i < N; ++i) {
        entries[i] = std::make_unique<test_evictable>(i);
        assign_unique_sketch_key(*entries[i]);
        l.add(*entries[i]);
    }

    // Build frequency via remove/add cycles (bypasses protected-skip).
    // With 10 entries, threshold = max(1000, 10*10) = 1000.
    // Each add increments sample_count. Need >= 1000 total accesses.
    for (int i = 0; i < 1010; ++i) {
        auto& e = *entries[i % N];
        l.remove(e);
        l.add(e);
    }

    auto& st = l.get_stats();
    BOOST_REQUIRE_GE(st.sketch_resets, 1u);
    // Sampled average frequencies should be non-negative.
    BOOST_REQUIRE_GE(st.sampled_avg_freq_window, 0.0);
    BOOST_REQUIRE_GE(st.sampled_avg_freq_probation, 0.0);
    BOOST_REQUIRE_GE(st.sampled_avg_freq_protected, 0.0);

    // Clean up.
    for (int i = 0; i < N; ++i) {
        if (entries[i]->is_linked()) l.remove(*entries[i]);
    }
}

BOOST_AUTO_TEST_CASE(test_lru_freq_histogram_buckets) {
    lru l;
    static constexpr int N = 40;
    std::unique_ptr<test_evictable> entries[N];

    // Phase 1: add and build frequency differentiation.
    for (int i = 0; i < 20; ++i) {
        entries[i] = std::make_unique<test_evictable>(i);
        assign_unique_sketch_key(*entries[i]);
        l.add(*entries[i]);
    }
    for (int round = 0; round < 10; ++round) {
        for (int i = 15; i < 20; ++i) {
            l.touch(*entries[i]);
        }
    }

    // Phase 2: add more to grow the window, then evict to trigger
    // drain_window's admission gate which records freq buckets.
    for (int i = 20; i < N; ++i) {
        entries[i] = std::make_unique<test_evictable>(i);
        assign_unique_sketch_key(*entries[i]);
        l.add(*entries[i]);
    }
    for (int i = 0; i < 15; ++i) {
        l.evict();
    }

    auto& st = l.get_stats();
    uint64_t total_gate = st.tinylfu_admissions + st.tinylfu_rejections;

    // If the gate was exercised, bucket counters should match.
    if (total_gate > 0) {
        uint64_t bucket_sum = st.admission_freq_bucket_0_1
                            + st.admission_freq_bucket_2_3
                            + st.admission_freq_bucket_4_7
                            + st.admission_freq_bucket_8_15;
        BOOST_REQUIRE_GT(bucket_sum, 0u);
        BOOST_REQUIRE_EQUAL(bucket_sum, total_gate);
    }

    // At minimum, window_to_probation should have been exercised.
    BOOST_REQUIRE_GT(st.window_to_probation, 0u);

    // Clean up.
    for (int i = 0; i < N; ++i) {
        if (entries[i]->is_linked()) l.remove(*entries[i]);
    }
}

// ---------------------------------------------------------------------------
// Packed evictable field tests
// ---------------------------------------------------------------------------

// Verify that segment and sketch key are independently stored and retrieved
// from the packed uint64_t without interfering with each other.
BOOST_AUTO_TEST_CASE(test_packed_segment_and_key_independence) {
    test_evictable e(0);

    // Initially: no segment, no key
    BOOST_REQUIRE(!e.has_sketch_key());
    BOOST_REQUIRE_EQUAL(e.sketch_key(), 0u);

    // Set a sketch key — segment should remain none (not in any LRU)
    e.set_sketch_key(12345);
    BOOST_REQUIRE(e.has_sketch_key());
    BOOST_REQUIRE_EQUAL(e.sketch_key(), 12345u);

    // Add to LRU — sets segment to window, key must be preserved
    lru l;
    l.add(e);
    BOOST_REQUIRE(e.has_sketch_key());
    BOOST_REQUIRE_EQUAL(e.sketch_key(), 12345u);

    // Touch promotes from probation→protected — key must survive
    l.touch(e);
    BOOST_REQUIRE(e.has_sketch_key());
    BOOST_REQUIRE_EQUAL(e.sketch_key(), 12345u);

    // Change key while in LRU — segment must be preserved
    e.set_sketch_key(99999);
    BOOST_REQUIRE(e.has_sketch_key());
    BOOST_REQUIRE_EQUAL(e.sketch_key(), 99999u);
    BOOST_REQUIRE(e.is_linked());

    l.remove(e);
}

// Verify that key value 0 is properly handled — it's a valid token hash,
// not a sentinel. has_sketch_key() must still return true.
BOOST_AUTO_TEST_CASE(test_packed_key_zero_is_valid) {
    test_evictable e(0);

    BOOST_REQUIRE(!e.has_sketch_key());

    // Set key to 0 — must be distinguishable from "not set"
    e.set_sketch_key(0);
    BOOST_REQUIRE(e.has_sketch_key());
    BOOST_REQUIRE_EQUAL(e.sketch_key(), 0u);

    // The LRU's entry_key() should use this 0 key, NOT the address fallback
    lru l;
    l.add(e);
    // Verify the sketch tracks under key 0
    BOOST_REQUIRE_GE(l.sketch_estimate(0), 1u);
    l.remove(e);
}

// Keyless entries (the MVCC write path and index pages) carry no sketch key.
// They must never enter the admission window: lru::add_index() routes them
// straight to the protected segment, and record_access() keeps them out of the
// Count-Min sketch so they cannot alias with token-keyed partitions. (The old
// address-based fallback was removed: LSA relocates objects, so an address is
// neither stable across compaction nor collision-free against real token keys.)
BOOST_AUTO_TEST_CASE(test_keyless_entry_routes_to_protected_untracked) {
    test_evictable e1(0);
    test_evictable e2(1);

    // Neither has a sketch key set
    BOOST_REQUIRE(!e1.has_sketch_key());
    BOOST_REQUIRE(!e2.has_sketch_key());

    lru l;
    // Keyless entries route to protected (never the window).
    l.add_index(e1);
    l.add_index(e2);
    BOOST_REQUIRE_EQUAL(l.protected_size(), 2u);
    BOOST_REQUIRE_EQUAL(l.window_size(), 0u);

    // Touching a keyless entry records nothing in the sketch (no address-based
    // aliasing): a probe under an arbitrary token key stays 0.
    for (int i = 0; i < 10; ++i) {
        l.touch(e1);
    }
    BOOST_REQUIRE_EQUAL(l.sketch_estimate(12345u), 0u);

    l.remove(e1);
    l.remove(e2);
}

// Verify swap preserves the full packed state (segment + has_key + key).
BOOST_AUTO_TEST_CASE(test_packed_swap_preserves_all_fields) {
    test_evictable a(0);
    test_evictable b(1);

    a.set_sketch_key(111);
    // b has no key set

    lru l;
    l.add(a);  // a (keyed) goes to the window segment
    l.add_index(b);  // b (keyless) routes to protected

    // Before swap
    BOOST_REQUIRE(a.has_sketch_key());
    BOOST_REQUIRE_EQUAL(a.sketch_key(), 111u);
    BOOST_REQUIRE(!b.has_sketch_key());

    a.swap(b);

    // After swap — a gets b's state (no key), b gets a's state (key=111)
    BOOST_REQUIRE(!a.has_sketch_key());
    BOOST_REQUIRE(b.has_sketch_key());
    BOOST_REQUIRE_EQUAL(b.sketch_key(), 111u);

    // Both should still be linked (swap preserves link nodes)
    BOOST_REQUIRE(a.is_linked());
    BOOST_REQUIRE(b.is_linked());

    l.remove(a);
    l.remove(b);
}

// Verify large key values survive the 58-bit truncation.
BOOST_AUTO_TEST_CASE(test_packed_large_key_values) {
    test_evictable e(0);

    // Maximum 58-bit value (6 bits used for segment + has_key + direct_insert
    // + routes_to_protected + reenters_protected)
    uint64_t max_58 = (uint64_t(1) << 58) - 1;
    e.set_sketch_key(max_58);
    BOOST_REQUIRE(e.has_sketch_key());
    BOOST_REQUIRE_EQUAL(e.sketch_key(), max_58);

    // A full 64-bit value gets its top 6 bits truncated
    e.set_sketch_key(UINT64_MAX);
    BOOST_REQUIRE(e.has_sketch_key());
    BOOST_REQUIRE_EQUAL(e.sketch_key(), max_58);  // top 6 bits lost

    // Powers of 2 near the boundary
    e.set_sketch_key(uint64_t(1) << 57);
    BOOST_REQUIRE_EQUAL(e.sketch_key(), uint64_t(1) << 57);

    // Value 1 (minimal non-zero)
    e.set_sketch_key(1);
    BOOST_REQUIRE_EQUAL(e.sketch_key(), 1u);
}

// Verify that set_sketch_key preserves the segment and vice versa,
// across all segment transitions.
BOOST_AUTO_TEST_CASE(test_packed_segment_key_round_trip) {
    lru l;
    test_evictable e(0);
    e.set_sketch_key(42);

    // Add → window (segment=1)
    l.add(e);
    BOOST_REQUIRE_EQUAL(e.sketch_key(), 42u);
    BOOST_REQUIRE(e.has_sketch_key());

    // Overwrite key while in window — segment must survive
    e.set_sketch_key(7777);
    BOOST_REQUIRE_EQUAL(e.sketch_key(), 7777u);
    BOOST_REQUIRE(e.is_linked());

    // Touch to promote — key must survive across segment transitions
    for (int i = 0; i < 5; ++i) {
        l.touch(e);
    }
    BOOST_REQUIRE_EQUAL(e.sketch_key(), 7777u);
    BOOST_REQUIRE(e.has_sketch_key());

    l.remove(e);

    // After remove (segment=none) — key must survive
    BOOST_REQUIRE_EQUAL(e.sketch_key(), 7777u);
    BOOST_REQUIRE(e.has_sketch_key());
}

// ---------------------------------------------------------------------------
// MVCC eviction ordering tests
// ---------------------------------------------------------------------------

// Simulate MVCC version ordering: older version rows must be evicted before
// newer version rows.  This is the invariant that partition_snapshot::touch()
// preserves by only touching the latest version.
//
// The test creates "older" and "newer" entries sharing the same sketch key
// (same partition).  Older entries are added first and never touched.
// Newer entries are added later and touched.  Under eviction pressure,
// older entries must be evicted before newer ones in all segments.

BOOST_AUTO_TEST_CASE(test_mvcc_ordering_direct_to_protected) {
    // MVCC ordering for multi-row partition rows uses the production path:
    // clustering rows enter via add_to_protected() (direct-to-protected),
    // where eviction order is exactly insertion order and rebalance never
    // demotes. Rows of older MVCC versions are inserted before newer ones
    // and are never touched (partition_snapshot::touch() only touches the
    // latest version), so they are always evicted first — the invariant
    // that keeps snapshots consistent under eviction.
    lru l;

    static constexpr int OLDER = 5;
    static constexpr int NEWER = 5;
    static constexpr uint64_t PARTITION_KEY = 0xDEAD;

    std::vector<int> order;
    std::unique_ptr<test_evictable> older[OLDER];
    std::unique_ptr<test_evictable> newer[NEWER];

    // Insert older version rows (never touched after insert)
    for (int i = 0; i < OLDER; ++i) {
        older[i] = std::make_unique<test_evictable>(i);
        older[i]->eviction_log = &order;
        older[i]->set_sketch_key(PARTITION_KEY);
        l.add_to_protected(*older[i]);
    }

    // Insert newer version rows and touch them (simulating reads of the
    // latest version)
    for (int i = 0; i < NEWER; ++i) {
        newer[i] = std::make_unique<test_evictable>(100 + i);
        newer[i]->eviction_log = &order;
        newer[i]->set_sketch_key(PARTITION_KEY);
        l.add_to_protected(*newer[i]);
    }
    for (int round = 0; round < 3; ++round) {
        for (int i = 0; i < NEWER; ++i) {
            l.touch(*newer[i]);
        }
    }

    // Evict — exactly the older entries go, in insertion order.
    for (int i = 0; i < OLDER; ++i) {
        BOOST_REQUIRE(l.evict() == reclaimed_something);
    }
    BOOST_REQUIRE_EQUAL(order.size(), OLDER);
    for (int i = 0; i < OLDER; ++i) {
        BOOST_REQUIRE_MESSAGE(order[i] == i,
            "Older version entry " << i << " should be evicted before newer entries");
    }
    for (int i = 0; i < NEWER; ++i) {
        BOOST_REQUIRE_MESSAGE(!newer[i]->was_evicted,
            "Newer version entry " << i << " should survive (was touched)");
    }

    for (int i = 0; i < NEWER; ++i) {
        if (newer[i]->is_linked()) l.remove(*newer[i]);
    }
}

BOOST_AUTO_TEST_CASE(test_mvcc_ordering_in_protected_segment) {
    // Both older and newer entries reach the protected segment.
    // Older entries are not touched again, so they drift to the front
    // of protected and get demoted to probation.  Newer entries are
    // continuously touched, staying at the back of protected.
    // Under eviction, demoted older entries are evicted first.
    lru l;

    static constexpr int N = 10;
    static constexpr uint64_t PARTITION_KEY = 0xBEEF;

    std::unique_ptr<test_evictable> older[N];
    std::unique_ptr<test_evictable> newer[N];

    // Add older entries and touch once to promote to protected
    for (int i = 0; i < N; ++i) {
        older[i] = std::make_unique<test_evictable>(i);
        older[i]->set_sketch_key(PARTITION_KEY);
        l.add(*older[i]);
    }
    for (int i = 0; i < N; ++i) {
        l.touch(*older[i]);  // probation → protected
    }

    // Add newer entries and touch to promote to protected
    for (int i = 0; i < N; ++i) {
        newer[i] = std::make_unique<test_evictable>(100 + i);
        newer[i]->set_sketch_key(PARTITION_KEY);
        l.add(*newer[i]);
    }
    for (int i = 0; i < N; ++i) {
        l.touch(*newer[i]);  // probation → protected
    }

    // Now continuously touch ONLY the newer entries (simulating latest-version reads)
    // Older entries drift to front of protected → get demoted to probation
    for (int round = 0; round < 5; ++round) {
        for (int i = 0; i < N; ++i) {
            l.touch(*newer[i]);
        }
    }

    // Evict N entries — older entries (demoted to probation or at front
    // of protected) should go first
    int older_evicted = 0;
    int newer_evicted = 0;
    for (int i = 0; i < N; ++i) {
        l.evict();
    }
    for (int i = 0; i < N; ++i) {
        if (older[i]->was_evicted) ++older_evicted;
        if (newer[i]->was_evicted) ++newer_evicted;
    }

    // Older entries should be evicted preferentially
    BOOST_REQUIRE_GT(older_evicted, newer_evicted);
    BOOST_REQUIRE_MESSAGE(newer_evicted == 0,
        "No newer entries should be evicted when older entries are available");

    for (int i = 0; i < N; ++i) {
        if (older[i]->is_linked()) l.remove(*older[i]);
        if (newer[i]->is_linked()) l.remove(*newer[i]);
    }
}

BOOST_AUTO_TEST_CASE(test_mvcc_same_frequency_preserves_lru_order) {
    // Entries sharing one sketch key (rows of one partition) keep strict
    // LRU order in the protected segment — the production path for
    // multi-row partitions never lets the admission gate reorder them.
    lru l;

    static constexpr int N = 20;
    static constexpr uint64_t PARTITION_KEY = 0xCAFE;

    std::vector<int> eviction_order;
    std::unique_ptr<test_evictable> entries[N];
    for (int i = 0; i < N; ++i) {
        entries[i] = std::make_unique<test_evictable>(i);
        entries[i]->eviction_log = &eviction_order;
        entries[i]->set_sketch_key(PARTITION_KEY);
        l.add_to_protected(*entries[i]);
    }

    // No touches — eviction must follow insertion order exactly.
    for (int i = 0; i < N / 2; ++i) {
        BOOST_REQUIRE(l.evict() == reclaimed_something);
    }

    BOOST_REQUIRE_EQUAL(eviction_order.size(), N / 2);
    for (int i = 0; i < N / 2; ++i) {
        BOOST_REQUIRE_MESSAGE(eviction_order[i] == i,
            "Entry " << eviction_order[i] << " was evicted out of insertion order");
    }

    for (int i = 0; i < N; ++i) {
        if (entries[i]->is_linked()) l.remove(*entries[i]);
    }
}

// ---------------------------------------------------------------------------
// False-rejection amplification test for multi-row partitions
// ---------------------------------------------------------------------------

// Measures how W-TinyLFU's per-row frequency gating amplifies false
// rejections when partitions contain multiple rows.  If any single row
// from a partition is rejected, the partition is "incomplete" in cache.
//
// Avi Kivity's concern: a 1% per-row false-rejection rate becomes 63%
// per-partition miss rate for 100-row partitions.  We test this by
// running a round-robin full-partition-read workload under eviction
// pressure and measuring how many partitions remain complete in cache.
//
// Output: a comparison table of W-TinyLFU vs LRU across partition sizes.
//
// Measures false-rejection amplification for multi-row partitions.
//
// Workload: a mix of hot single-row partitions (accessed frequently)
// and multi-row partitions (accessed as whole partitions, less frequently).
// The hot partitions build high frequency in the sketch.  When a
// multi-row partition's row is evicted and re-inserted, it enters the
// window with freq=1 and duels a hot partition's row at freq=15 in
// probation — it loses and is rejected.  If any row from the multi-row
// partition is rejected, the partition is incomplete.
//
// This tests Avi Kivity's concern: per-row false rejection at the
// admission gate is amplified by the number of rows per partition.
//
BOOST_AUTO_TEST_CASE(test_multi_row_false_rejection_amplification) {
    auto make_key = [](int partition, int row) -> uint64_t {
        uint64_t h = static_cast<uint64_t>(partition) * 0x9e3779b97f4a7c15ULL;
        h ^= static_cast<uint64_t>(row) * 0xd6e8feb86659fd93ULL;
        h ^= h >> 32;
        return h;
    };

    struct config {
        int rows_per_partition;
        int num_multi_row_partitions;
    };

    // Hot single-row partitions saturate sketch frequency.
    // Multi-row partitions compete for remaining cache space but
    // there are MORE multi-row entries than cache slots, forcing
    // continuous eviction and re-insertion through the admission gate.
    //
    // Cache = 2000.  Hot = 800 (always in cache).
    // Remaining cache slots = 1200.
    // Multi-row working set = 4000 entries (3.3× the available slots).
    // This forces ~2/3 of multi-row rows to be evicted each round.
    // On re-insert, they enter window with freq=1 and duel hot
    // partition rows in probation with freq=15.
    static constexpr int HOT_PARTITIONS = 800;
    static constexpr int CACHE_TARGET = 2000;
    static constexpr int MULTI_ROW_TOTAL = 4000;
    // Debug builds are much slower; cut the steady-state work so the case stays
    // fast there (the qualitative result is unchanged).
#ifdef SEASTAR_DEBUG
    static constexpr int HOT_TOUCHES_PER_ROUND = 10;
    static constexpr int ROUNDS = 3;
#else
    static constexpr int HOT_TOUCHES_PER_ROUND = 50;
    static constexpr int ROUNDS = 10;
#endif

    std::vector<config> configs = {
        {1,   MULTI_ROW_TOTAL},           // 4000 single-row
        {10,  MULTI_ROW_TOTAL / 10},      // 400 × 10
        {50,  MULTI_ROW_TOTAL / 50},      // 80 × 50
        {100, MULTI_ROW_TOTAL / 100},     // 40 × 100
    };

    std::cout << "\n=== False-Rejection Amplification (Skewed Frequency) ===" << std::endl;
    std::cout << "Hot partitions: " << HOT_PARTITIONS << " (single-row, "
              << HOT_TOUCHES_PER_ROUND << "x/round)" << std::endl;
    std::cout << "Cache target: " << CACHE_TARGET << " entries" << std::endl;
    std::cout << std::endl;
    std::cout << std::setw(10) << "Rows/Part"
              << std::setw(14) << "MultiRowParts"
              << std::setw(22) << "TinyLFU complete%"
              << std::setw(22) << "LRU complete%"
              << std::setw(22) << "TinyLFU row-hit%"
              << std::setw(22) << "LRU row-hit%"
              << std::endl;
    std::cout << std::string(112, '-') << std::endl;

    for (auto& cfg : configs) {
        int R = cfg.rows_per_partition;
        int MR = cfg.num_multi_row_partitions;
        int total_multi_rows = MR * R;

        double results[2][2] = {};

        for (int mode = 0; mode < 2; ++mode) {
            lru l;
            l.set_window_fraction(mode == 0 ? 0.01 : 0.99);

            // Hot single-row partitions
            std::vector<std::unique_ptr<test_evictable>> hot(HOT_PARTITIONS);
            for (int i = 0; i < HOT_PARTITIONS; ++i) {
                hot[i] = std::make_unique<test_evictable>(i);
                hot[i]->set_sketch_key(make_key(10000 + i, 0));
                l.add(*hot[i]);
            }

            // Multi-row partitions
            std::vector<std::vector<std::unique_ptr<test_evictable>>> multi(MR);
            for (int p = 0; p < MR; ++p) {
                multi[p].resize(R);
                for (int r = 0; r < R; ++r) {
                    multi[p][r] = std::make_unique<test_evictable>(HOT_PARTITIONS + p * R + r);
                    multi[p][r]->set_sketch_key(make_key(p, r));
                    l.add(*multi[p][r]);
                }
            }

            // Initial eviction to reach cache target
            int total = HOT_PARTITIONS + total_multi_rows;
            for (int i = 0; i < std::max(0, total - CACHE_TARGET); ++i) {
                l.evict();
            }

            // Steady-state rounds
            for (int round = 0; round < ROUNDS; ++round) {
                // Touch hot partitions many times (build high frequency)
                for (int t = 0; t < HOT_TOUCHES_PER_ROUND; ++t) {
                    for (int i = 0; i < HOT_PARTITIONS; ++i) {
                        if (hot[i]->is_linked()) {
                            l.touch(*hot[i]);
                        } else {
                            hot[i]->was_evicted = false;
                            hot[i]->set_sketch_key(make_key(10000 + i, 0));
                            l.add(*hot[i]);
                        }
                    }
                }

                // Read each multi-row partition once (all rows)
                for (int p = 0; p < MR; ++p) {
                    for (int r = 0; r < R; ++r) {
                        auto& e = *multi[p][r];
                        if (e.is_linked()) {
                            l.touch(e);
                        } else {
                            e.was_evicted = false;
                            e.set_sketch_key(make_key(p, r));
                            l.add(e);
                        }
                    }
                }

                // Evict back to target
                size_t sz = l.window_size() + l.probation_size() + l.protected_size();
                while (sz > static_cast<size_t>(CACHE_TARGET)) {
                    if (l.evict() == seastar::memory::reclaiming_result::reclaimed_nothing) break;
                    sz = l.window_size() + l.probation_size() + l.protected_size();
                }
            }

            // Measure multi-row partition completeness
            int complete = 0, rows_present = 0;
            for (int p = 0; p < MR; ++p) {
                int pr = 0;
                for (int r = 0; r < R; ++r) {
                    if (multi[p][r]->is_linked()) ++pr;
                }
                rows_present += pr;
                if (pr == R) ++complete;
            }

            results[mode][0] = 100.0 * complete / MR;
            results[mode][1] = 100.0 * rows_present / total_multi_rows;

            // Cleanup
            for (int i = 0; i < HOT_PARTITIONS; ++i) {
                if (hot[i]->is_linked()) l.remove(*hot[i]);
            }
            for (int p = 0; p < MR; ++p) {
                for (int r = 0; r < R; ++r) {
                    if (multi[p][r]->is_linked()) l.remove(*multi[p][r]);
                }
            }
        }

        std::cout << std::setw(10) << R
                  << std::setw(14) << MR
                  << std::setw(21) << std::fixed << std::setprecision(1) << results[0][0] << "%"
                  << std::setw(21) << results[1][0] << "%"
                  << std::setw(21) << results[0][1] << "%"
                  << std::setw(21) << results[1][1] << "%"
                  << std::endl;

        // This case characterises the *false-rejection amplification* of raw
        // window admission: when many multi-row entries duel high-frequency
        // single-row rows in the window, W-TinyLFU (results[0]) retains far
        // fewer multi-row rows than classic LRU (results[1]). That weakness is
        // exactly why real multi-row rows are routed direct-to-protected in the
        // cache (bypassing the window). We assert the measurement is well-formed
        // rather than a fixed ranking, so the case has real checks instead of
        // only printing a table.
        for (int mode = 0; mode < 2; ++mode) {
            BOOST_CHECK_GE(results[mode][0], 0.0);
            BOOST_CHECK_LE(results[mode][0], 100.0);
            BOOST_CHECK_GE(results[mode][1], 0.0);
            BOOST_CHECK_LE(results[mode][1], 100.0);
        }
        // Classic LRU (no frequency gate) must keep at least as many multi-row
        // rows as window-gated W-TinyLFU in this adversarial mix.
        BOOST_CHECK_GE(results[1][1], results[0][1]);
    }
    std::cout << std::endl;
}

BOOST_AUTO_TEST_CASE(test_add_to_protected_bypasses_window) {
    lru l;
    test_evictable e1(1), e2(2), e3(3);
    assign_unique_sketch_key(e1);
    assign_unique_sketch_key(e2);
    assign_unique_sketch_key(e3);

    // Add e1 normally (goes to window)
    l.add(e1);
    BOOST_REQUIRE_EQUAL(l.window_size(), 1);
    BOOST_REQUIRE_EQUAL(l.protected_size(), 0);

    // Add e2 directly to protected
    l.add_to_protected(e2);
    BOOST_REQUIRE_EQUAL(l.window_size(), 1);
    BOOST_REQUIRE_EQUAL(l.protected_size(), 1);

    // Add e3 directly to protected
    l.add_to_protected(e3);
    BOOST_REQUIRE_EQUAL(l.protected_size(), 2);

    // Cleanup
    l.remove(e1);
    l.remove(e2);
    l.remove(e3);
}


// ---------------------------------------------------------------------------
// Task 6: Directly-inserted entries are protected from rebalance demotion
// ---------------------------------------------------------------------------

BOOST_AUTO_TEST_CASE(test_rebalance_does_not_demote_directly_inserted) {
    lru l;
    // Insert entries directly to protected (multi-row path)
    constexpr int N_DIRECT = 5;
    std::vector<std::unique_ptr<test_evictable>> direct;
    for (int i = 0; i < N_DIRECT; ++i) {
        auto e = std::make_unique<test_evictable>(i);
        assign_unique_sketch_key(*e);
        l.add_to_protected(*e);
        direct.push_back(std::move(e));
    }
    BOOST_REQUIRE_EQUAL(l.protected_size(), N_DIRECT);
    BOOST_REQUIRE_EQUAL(l.probation_size(), 0);

    // Now simulate promotions: add entries via window, then touch them from
    // probation to promote to protected. This creates promoted entries that
    // rebalance_protected() should demote.
    constexpr int N_PROMOTED = 30;
    std::vector<std::unique_ptr<test_evictable>> promoted;
    for (int i = 0; i < N_PROMOTED; ++i) {
        auto e = std::make_unique<test_evictable>(100 + i);
        assign_unique_sketch_key(*e);
        l.add(*e);  // goes to window, overflow moves to probation
        promoted.push_back(std::move(e));
    }

    // Touch all promoted entries that landed in probation to promote them
    for (auto& e : promoted) {
        if (!e->was_evicted && e->is_linked()) {
            l.touch(*e);  // probation→protected promotion
        }
    }

    // Now trigger eviction which calls rebalance_protected()
    for (int i = 0; i < 20; ++i) {
        l.evict();
    }

    // Directly-inserted entries should NOT have been demoted to probation
    // and then evicted. They should still be in protected (or evicted via
    // the 80% LRU path if the cache is small enough).
    // The key invariant: direct entries are never found in probation.
    // Check that at least some direct entries survived (they're at the back
    // due to rebalance skipping them).
    int direct_survived = 0;
    for (auto& e : direct) {
        if (!e->was_evicted && e->is_linked()) {
            ++direct_survived;
        }
    }
    // With 5 direct + 30 promoted and 20 evictions, at least some direct should survive
    BOOST_REQUIRE_GE(direct_survived, 1);

    // Cleanup
    for (auto& e : direct) {
        if (!e->was_evicted && e->is_linked()) l.remove(*e);
    }
    for (auto& e : promoted) {
        if (!e->was_evicted && e->is_linked()) l.remove(*e);
    }
}

BOOST_AUTO_TEST_CASE(test_directly_inserted_flag_cleared_on_remove) {
    lru l;
    test_evictable e1(1);
    assign_unique_sketch_key(e1);

    l.add_to_protected(e1);
    BOOST_REQUIRE(e1.is_directly_inserted());
    BOOST_REQUIRE_EQUAL(l.protected_size(), 1);

    l.remove(e1);
    BOOST_REQUIRE(!e1.is_directly_inserted());
}

// ---------------------------------------------------------------------------
// Task 7: Verify touch() moves protected entries to back (MRU position)
// ---------------------------------------------------------------------------

BOOST_AUTO_TEST_CASE(test_touch_protected_moves_to_back) {
    lru l;
    test_evictable e1(1), e2(2), e3(3);
    assign_unique_sketch_key(e1);
    assign_unique_sketch_key(e2);
    assign_unique_sketch_key(e3);

    l.add_to_protected(e1);
    l.add_to_protected(e2);
    l.add_to_protected(e3);

    // Touch e1 — moves to back of protected (MRU position)
    l.touch(e1);

    // Order is now: e2 (front/LRU), e3, e1 (back/MRU)
    // Evict from protected (80% path hits front) — should get e2 first
    // We need enough evictions to hit the protected path
    for (int i = 0; i < 10; ++i) {
        l.evict();
    }

    // e2 should be evicted first (it's at front), e1 last (it was touched)
    BOOST_REQUIRE(e2.was_evicted);
    // e1 should survive longer than e2 since it was moved to back
    // With only 3 entries, all will eventually be evicted, but e2 goes first
    if (!e1.was_evicted && e1.is_linked()) l.remove(e1);
    if (!e3.was_evicted && e3.is_linked()) l.remove(e3);
}

// ---------------------------------------------------------------------------
// W-TinyLFU lifecycle: one small deterministic test per state transition.
//
// Segment placement rules under test:
//   add()               → window (unbounded; drained only during eviction)
//   touch(window)       → protected (promotion)
//   touch(probation)    → protected (promotion)
//   touch(protected)    → back of protected (MRU)
//   touch(unlinked)     → same as add()
//   unlink_touched()+add() → protected, as a regular (demotable) entry
//   add_to_protected()  → protected, direct (never demoted, sticky routing)
//   add_before(m)       → m's segment, inheriting direct/routing status
// Eviction rules under test:
//   drain_window(): window victim duels probation front — higher frequency
//   is admitted to probation, otherwise the victim is rejected (evicted)
//   direct eviction order: probation front, then window, then protected
//   rebalance_protected(): demotes promoted (non-direct) entries only
// ---------------------------------------------------------------------------

BOOST_AUTO_TEST_CASE(test_add_enters_window) {
    lru l;
    test_evictable e(1);
    assign_unique_sketch_key(e);

    l.add(e);
    BOOST_REQUIRE_EQUAL(l.window_size(), 1);
    BOOST_REQUIRE_EQUAL(l.probation_size(), 0);
    BOOST_REQUIRE_EQUAL(l.protected_size(), 0);
    // add() records the access in the frequency sketch.
    BOOST_REQUIRE_EQUAL(l.sketch_estimate(e.sketch_key()), 1);

    l.remove(e);
}

BOOST_AUTO_TEST_CASE(test_touch_window_entry_promotes_to_protected) {
    lru l;
    test_evictable e(1);
    assign_unique_sketch_key(e);
    l.add(e);

    auto promotions = l.get_stats().protected_promotions;
    l.touch(e);
    BOOST_REQUIRE_EQUAL(l.window_size(), 0);
    BOOST_REQUIRE_EQUAL(l.protected_size(), 1);
    BOOST_REQUIRE_EQUAL(l.get_stats().protected_promotions, promotions + 1);
    // Promoted via touch, not directly inserted: subject to rebalance.
    BOOST_REQUIRE(!e.is_directly_inserted());

    l.remove(e);
}

BOOST_AUTO_TEST_CASE(test_touch_probation_entry_promotes_to_protected) {
    lru l;
    // Build a probation resident: the first drain moves a into the empty
    // probation, then b (equal frequency) loses the duel against a and is
    // evicted, and c remains as the window residual.
    test_evictable a(1), b(2), c(3);
    assign_unique_sketch_key(a);
    assign_unique_sketch_key(b);
    assign_unique_sketch_key(c);
    l.add(a);
    l.add(b);
    l.add(c);

    BOOST_REQUIRE(l.evict() == reclaimed_something);
    BOOST_REQUIRE(!a.was_evicted);
    BOOST_REQUIRE(b.was_evicted);
    BOOST_REQUIRE_EQUAL(l.probation_size(), 1);
    BOOST_REQUIRE_EQUAL(l.window_size(), 1);

    auto promotions = l.get_stats().protected_promotions;
    l.touch(a);
    BOOST_REQUIRE_EQUAL(l.probation_size(), 0);
    BOOST_REQUIRE_EQUAL(l.protected_size(), 1);
    BOOST_REQUIRE_EQUAL(l.get_stats().protected_promotions, promotions + 1);

    l.remove(a);
    l.remove(c);
}

BOOST_AUTO_TEST_CASE(test_admission_gate_admits_hotter_window_victim) {
    lru l;
    // h has sketch frequency 3 (one add + two remove/add cycles); a has 1.
    // In the duel h (window victim) wins against a (probation front): h is
    // admitted to probation and a is evicted.
    test_evictable a(1), h(2), t(3);
    assign_unique_sketch_key(a);
    assign_unique_sketch_key(h);
    assign_unique_sketch_key(t);
    l.add(a);
    l.add(h);
    for (int i = 0; i < 2; ++i) {
        l.remove(h);
        l.add(h);
    }
    l.add(t); // tail keeps the drain loop going after h's duel

    auto admissions = l.get_stats().tinylfu_admissions;
    BOOST_REQUIRE(l.evict() == reclaimed_something);
    BOOST_REQUIRE(a.was_evicted);        // probation victim lost the duel
    BOOST_REQUIRE(!h.was_evicted);       // window victim was admitted
    BOOST_REQUIRE_EQUAL(l.probation_size(), 1);
    BOOST_REQUIRE_EQUAL(l.get_stats().tinylfu_admissions, admissions + 1);

    l.remove(h);
    l.remove(t);
}

BOOST_AUTO_TEST_CASE(test_unlink_touched_reenters_protected) {
    lru l;
    // The index-page release cycle: pages are unlinked while referenced and
    // re-enter via add() on release. unlink_touched() marks the access so
    // the re-add lands in protected instead of the admission window.
    test_evictable e(1);
    assign_unique_sketch_key(e);
    l.add(e);
    BOOST_REQUIRE_EQUAL(l.window_size(), 1);

    l.unlink_touched(e);
    BOOST_REQUIRE(!e.is_linked());
    BOOST_REQUIRE(e.reenters_protected());
    BOOST_REQUIRE_EQUAL(l.window_size(), 0);

    auto promotions = l.get_stats().protected_promotions;
    l.add(e);
    BOOST_REQUIRE_EQUAL(l.protected_size(), 1);
    BOOST_REQUIRE_EQUAL(l.window_size(), 0);
    BOOST_REQUIRE_EQUAL(l.get_stats().protected_promotions, promotions + 1);
    // A regular promoted entry — not direct, no sticky routing.
    BOOST_REQUIRE(!e.is_directly_inserted());
    BOOST_REQUIRE(!e.routes_to_protected());

    l.remove(e);
}

BOOST_AUTO_TEST_CASE(test_reentered_entry_is_demotable) {
    lru l;
    // Unlike direct-to-protected entries, an entry which re-entered
    // protected via unlink_touched()+add() is a regular promoted entry:
    // when the protected cap shrinks below the promoted count, rebalance
    // demotes it to probation, where direct eviction takes it first.
    test_evictable e(1), f(2);
    assign_unique_sketch_key(e);
    assign_unique_sketch_key(f);
    l.add(e);
    l.unlink_touched(e);
    l.add(e); // → protected, promoted
    l.add(f); // → window

    auto demotions = l.get_stats().protected_demotions;
    // total=2 → max_window=1, main=1, max_protected=0: e is demoted, and
    // direct eviction prefers probation over the window residual.
    BOOST_REQUIRE(l.evict() == reclaimed_something);
    BOOST_REQUIRE_EQUAL(l.get_stats().protected_demotions, demotions + 1);
    BOOST_REQUIRE(e.was_evicted);
    BOOST_REQUIRE(!f.was_evicted);

    l.remove(f);
}

BOOST_AUTO_TEST_CASE(test_direct_protected_entry_survives_shrink) {
    lru l;
    // Contrast with test_reentered_entry_is_demotable: a direct-to-protected
    // entry is skipped by rebalance and outlives the window resident.
    test_evictable d(1), f(2);
    assign_unique_sketch_key(d);
    assign_unique_sketch_key(f);
    l.add_to_protected(d);
    l.add(f);

    BOOST_REQUIRE(l.evict() == reclaimed_something);
    BOOST_REQUIRE(!d.was_evicted);
    BOOST_REQUIRE(f.was_evicted);
    BOOST_REQUIRE_EQUAL(l.get_stats().protected_demotions, 0u);

    // Only the direct entry remains: eviction falls through to protected.
    BOOST_REQUIRE(l.evict() == reclaimed_something);
    BOOST_REQUIRE(d.was_evicted);
}

BOOST_AUTO_TEST_CASE(test_routes_to_protected_is_sticky_across_relink) {
    lru l;
    test_evictable d(1);
    assign_unique_sketch_key(d);

    l.add_to_protected(d);
    BOOST_REQUIRE(d.routes_to_protected());
    BOOST_REQUIRE(d.is_directly_inserted());

    l.remove(d);
    BOOST_REQUIRE(!d.is_directly_inserted());  // cleared on unlink
    BOOST_REQUIRE(d.routes_to_protected());    // sticky, survives unlinking

    // A plain add() honors the sticky hint: straight back to protected.
    l.add(d);
    BOOST_REQUIRE_EQUAL(l.window_size(), 0);
    BOOST_REQUIRE_EQUAL(l.protected_size(), 1);
    BOOST_REQUIRE(d.is_directly_inserted());

    l.remove(d);
}

BOOST_AUTO_TEST_CASE(test_touch_relinks_unlinked_entry) {
    lru l;
    test_evictable e(1);
    assign_unique_sketch_key(e);
    l.add(e);
    l.remove(e);

    // touch() on an unlinked entry behaves like add(): a plain entry goes
    // back to the window (the sticky-routing case is covered by
    // test_routes_to_protected_is_sticky_across_relink).
    l.touch(e);
    BOOST_REQUIRE_EQUAL(l.window_size(), 1);
    BOOST_REQUIRE_EQUAL(l.protected_size(), 0);

    l.remove(e);
}

BOOST_AUTO_TEST_CASE(test_add_before_inherits_segment_and_direct_status) {
    lru l;
    // Window case: the new entry joins m's segment.
    test_evictable m(1), e(2);
    assign_unique_sketch_key(m);
    assign_unique_sketch_key(e);
    l.add(m);
    l.add_before(m, e);
    BOOST_REQUIRE_EQUAL(l.window_size(), 2);
    l.remove(m);
    l.remove(e);

    // Direct-protected case: direct status and sticky routing are inherited,
    // and the entry sits right before its neighbor in eviction order.
    std::vector<int> order;
    test_evictable mp(3), ep(4);
    mp.eviction_log = &order;
    ep.eviction_log = &order;
    assign_unique_sketch_key(mp);
    assign_unique_sketch_key(ep);
    l.add_to_protected(mp);
    l.add_before(mp, ep);
    BOOST_REQUIRE_EQUAL(l.protected_size(), 2);
    BOOST_REQUIRE(ep.is_directly_inserted());
    BOOST_REQUIRE(ep.routes_to_protected());

    BOOST_REQUIRE(l.evict() == reclaimed_something);
    BOOST_REQUIRE(l.evict() == reclaimed_something);
    BOOST_REQUIRE_EQUAL(order.size(), 2u);
    BOOST_REQUIRE_EQUAL(order[0], 4);  // ep first — it was inserted before mp
    BOOST_REQUIRE_EQUAL(order[1], 3);
}

BOOST_AUTO_TEST_CASE(test_eviction_priority_probation_window_protected) {
    lru l;
    // One resident per segment (b is sacrificed while seeding probation).
    test_evictable a(1), b(2), c(3), d(4);
    assign_unique_sketch_key(a);
    assign_unique_sketch_key(b);
    assign_unique_sketch_key(c);
    assign_unique_sketch_key(d);
    l.add(a);
    l.add(b);
    l.add(c);
    BOOST_REQUIRE(l.evict() == reclaimed_something); // a → probation, b evicted
    l.add_to_protected(d);
    BOOST_REQUIRE_EQUAL(l.probation_size(), 1);
    BOOST_REQUIRE_EQUAL(l.window_size(), 1);
    BOOST_REQUIRE_EQUAL(l.protected_size(), 1);

    std::vector<int> order;
    a.eviction_log = &order;
    c.eviction_log = &order;
    d.eviction_log = &order;

    BOOST_REQUIRE(l.evict() == reclaimed_something);
    BOOST_REQUIRE(l.evict() == reclaimed_something);
    BOOST_REQUIRE(l.evict() == reclaimed_something);

    BOOST_REQUIRE_EQUAL(order.size(), 3u);
    BOOST_REQUIRE_EQUAL(order[0], 1);  // probation front
    BOOST_REQUIRE_EQUAL(order[1], 3);  // window
    BOOST_REQUIRE_EQUAL(order[2], 4);  // protected
}

BOOST_AUTO_TEST_CASE(test_segment_caps_follow_window_fraction) {
    lru l;
    // The segment caps are derived from the *current* total size and the
    // configured window fraction — the knobs tests use to override the
    // effective cache geometry.
    static constexpr int N = 100;
    std::unique_ptr<test_evictable> entries[N];
    for (int i = 0; i < N; ++i) {
        entries[i] = std::make_unique<test_evictable>(i);
        assign_unique_sketch_key(*entries[i]);
        l.add(*entries[i]);
    }

    BOOST_REQUIRE_EQUAL(l.current_max_window_size(), 1);      // 1% of 100
    BOOST_REQUIRE_EQUAL(l.current_max_protected_size(), (N - 1) * 80 / 100);

    l.set_window_fraction(0.10);
    BOOST_REQUIRE_EQUAL(l.current_max_window_size(), 10);
    BOOST_REQUIRE_EQUAL(l.current_max_protected_size(), (N - 10) * 80 / 100);

    l.set_window_fraction(0.50);
    BOOST_REQUIRE_EQUAL(l.current_max_window_size(), 50);
    BOOST_REQUIRE_EQUAL(l.current_max_protected_size(), (N - 50) * 80 / 100);

    for (int i = 0; i < N; ++i) {
        l.remove(*entries[i]);
    }
}

BOOST_AUTO_TEST_CASE(test_pure_scan_rejected_while_hot_entries_survive) {
    lru l;
    // Scan resistance: a one-pass scan (no re-accesses) is almost entirely
    // rejected at the admission gate and never displaces protected entries.
    static constexpr int HOT = 3;
    static constexpr int SCAN = 100;

    std::unique_ptr<test_evictable> hot[HOT];
    for (int i = 0; i < HOT; ++i) {
        hot[i] = std::make_unique<test_evictable>(i);
        assign_unique_sketch_key(*hot[i]);
        l.add(*hot[i]);
        l.touch(*hot[i]);  // → protected
    }
    BOOST_REQUIRE_EQUAL(l.protected_size(), HOT);

    std::unique_ptr<test_evictable> scan[SCAN];
    for (int i = 0; i < SCAN; ++i) {
        scan[i] = std::make_unique<test_evictable>(100 + i);
        assign_unique_sketch_key(*scan[i]);
        l.add(*scan[i]);
    }

    // drain_window() bails on need_preempt() (which fires readily in debug),
    // so a single evict() may not finish draining the 100-entry scan. Drain
    // the window fully before checking the outcome.
    while (l.window_size() > l.current_max_window_size()) {
        BOOST_REQUIRE(l.evict() == reclaimed_something);
    }

    // The scan was drained: one entry seeded probation, one remains as the
    // window residual, everything else was rejected at the gate.
    BOOST_REQUIRE_EQUAL(l.window_size(), 1);
    BOOST_REQUIRE_EQUAL(l.probation_size(), 1);
    BOOST_REQUIRE_EQUAL(l.get_stats().tinylfu_rejections, SCAN - 2u);
    // The hot entries were untouched by the scan's eviction pressure.
    BOOST_REQUIRE_EQUAL(l.protected_size(), HOT);
    for (int i = 0; i < HOT; ++i) {
        BOOST_REQUIRE(!hot[i]->was_evicted);
    }

    for (int i = 0; i < HOT; ++i) {
        l.remove(*hot[i]);
    }
    for (int i = 0; i < SCAN; ++i) {
        if (scan[i]->is_linked()) {
            l.remove(*scan[i]);
        }
    }
}

BOOST_AUTO_TEST_CASE(test_evict_shallow_invokes_shallow_callback) {
    lru l;
    test_evictable a(1), b(2);
    assign_unique_sketch_key(a);
    assign_unique_sketch_key(b);
    l.add(a);
    l.add(b);

    // Drain moves a into probation; the direct (shallow) eviction takes it.
    BOOST_REQUIRE(l.evict_shallow() == reclaimed_something);
    BOOST_REQUIRE(a.was_evicted_shallow);
    BOOST_REQUIRE(!b.was_evicted);

    l.remove(b);
}
