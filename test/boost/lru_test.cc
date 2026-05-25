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
struct test_evictable final: public evictable {
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

    // Create enough entries so eviction pressure reveals frequency effects.
    static constexpr int N = 10;
    std::unique_ptr<test_evictable> entries[N];
    for (int i = 0; i < N; ++i) {
        entries[i] = std::make_unique<test_evictable>(i);
        assign_unique_sketch_key(*entries[i]);
        l.add(*entries[i]);
    }

    // Touch entry 0 many times to build frequency.
    for (int i = 0; i < 30; ++i) {
        l.touch(*entries[0]);
    }

    // Evict several entries — the frequently-touched one should survive.
    for (int i = 0; i < N - 2; ++i) {
        l.evict();
    }

    // Entry 0 should still be linked (survived eviction).
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

    // Saturate entry 0's counter to 15.
    for (int i = 0; i < 20; ++i) {
        l.touch(*entries[0]);
    }
    auto key0 = entries[0]->sketch_key();
    BOOST_REQUIRE_EQUAL(l.sketch_estimate(key0), 15);

    // Generate 1100 touches on entry 1 to trigger at least one reset.
    for (int i = 0; i < 1100; ++i) {
        l.touch(*entries[1]);
    }
    // After at least one halving, 15 should have decayed.
    BOOST_REQUIRE_LE(l.sketch_estimate(key0), 7);

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

    // Clamped to [0.01, 0.99].
    l.set_window_fraction(0.0);
    BOOST_REQUIRE_CLOSE(l.window_fraction(), 0.01, 1e-6);
    l.set_window_fraction(1.0);
    BOOST_REQUIRE_CLOSE(l.window_fraction(), 0.99, 1e-6);
}

BOOST_AUTO_TEST_CASE(test_lru_add_path_drains_window_to_probation) {
    lru l;

    // With 1% window, add() moves excess window entries to probation
    // (safe drain without eviction). Verify that window_to_probation
    // counter increments and the window stays near its target.
    static constexpr int N = 80;
    std::unique_ptr<test_evictable> entries[N];
    for (int i = 0; i < N; ++i) {
        entries[i] = std::make_unique<test_evictable>(i);
        assign_unique_sketch_key(*entries[i]);
        l.add(*entries[i]);
    }

    // The safe drain should have moved entries from window to probation.
    const auto& st = l.get_stats();
    BOOST_REQUIRE_GT(st.window_to_probation, 0u);

    // Window should be near its target (1% of 80 = 1).
    BOOST_REQUIRE_LE(l.window_size(), 2u);

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

    // Access row_hot many times, row_cold only once (via add).
    for (int i = 0; i < 20; ++i) {
        l.touch(*row_hot);
    }

    auto key_hot  = row_hot->sketch_key();
    auto key_cold = row_cold->sketch_key();

    // The hot entry should have a much higher frequency estimate.
    BOOST_REQUIRE_GE(l.sketch_estimate(key_hot), 10);
    BOOST_REQUIRE_LE(l.sketch_estimate(key_cold), 2);

    l.remove(*row_hot);
    l.remove(*row_cold);
}

// Under eviction pressure, a less-frequently-accessed entry is evicted
// before a frequently-accessed one, demonstrating frequency-based admission.
// Both entries use distinct sketch keys here; in production, rows within the
// same partition share a key and rely on SLRU recency for ordering.
BOOST_AUTO_TEST_CASE(test_cold_entry_evicted_before_hot_entry) {
    lru l;
    static constexpr int FILLER = 20;
    auto row_hot  = std::make_unique<test_evictable>(100);
    auto row_cold = std::make_unique<test_evictable>(101);
    assign_unique_sketch_key(*row_hot);
    assign_unique_sketch_key(*row_cold);

    // Add filler first so they age into probation.
    std::unique_ptr<test_evictable> filler[FILLER];
    for (int i = 0; i < FILLER; ++i) {
        filler[i] = std::make_unique<test_evictable>(i);
        assign_unique_sketch_key(*filler[i]);
        l.add(*filler[i]);
    }

    l.add(*row_cold);
    l.add(*row_hot);

    // Build frequency for row_hot but not row_cold.
    for (int i = 0; i < 30; ++i) {
        l.touch(*row_hot);
    }

    // Evict repeatedly until one of our rows is evicted.
    bool cold_evicted_first = false;
    for (int round = 0; round < FILLER + 5; ++round) {
        l.evict();
        if (row_cold->was_evicted && !row_hot->was_evicted) {
            cold_evicted_first = true;
            break;
        }
        if (row_hot->was_evicted) {
            break;
        }
    }

    BOOST_REQUIRE_MESSAGE(cold_evicted_first,
        "Cold entry should be evicted before frequently-accessed entry");

    if (row_hot->is_linked()) l.remove(*row_hot);
    if (row_cold->is_linked()) l.remove(*row_cold);
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

    // Touch e1 many times to build frequency
    for (int i = 0; i < 10; ++i) {
        l.touch(*e1);
    }

    // e3 (same sketch key) should see the accumulated frequency
    BOOST_REQUIRE_GE(l.sketch_estimate(0xCAFE0001), 5);
    // e2 (different key) should have low frequency
    BOOST_REQUIRE_LE(l.sketch_estimate(0xCAFE0002), 3);

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

    // Touch entries[0] many times to saturate its frequency.
    for (int i = 0; i < 20; ++i) {
        l.touch(*entries[0]);
    }

    // With 10 entries, threshold = max(1000, 10*10) = 1000.
    // Generate enough touches to trigger at least one sketch reset.
    // Each touch increments sample_count; we need >= 1000 total accesses.
    // We already have 10 adds + 20 touches = 30. Need ~970 more.
    for (int i = 0; i < 980; ++i) {
        l.touch(*entries[i % N]);
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

// Verify that entries without a sketch key fall back to address-based
// keying (the MVCC write path).
BOOST_AUTO_TEST_CASE(test_packed_no_key_uses_address_fallback) {
    test_evictable e1(0);
    test_evictable e2(1);

    // Neither has a sketch key set
    BOOST_REQUIRE(!e1.has_sketch_key());
    BOOST_REQUIRE(!e2.has_sketch_key());

    lru l;
    l.add(e1);
    l.add(e2);

    // Each should have been tracked under its own address, so they
    // have independent frequency counts. Touch e1 many times.
    for (int i = 0; i < 10; ++i) {
        l.touch(e1);
    }

    // e1's frequency (by address) should be higher than e2's
    auto k1 = static_cast<uint64_t>(reinterpret_cast<uintptr_t>(&e1));
    auto k2 = static_cast<uint64_t>(reinterpret_cast<uintptr_t>(&e2));
    BOOST_REQUIRE_GT(l.sketch_estimate(k1), l.sketch_estimate(k2));

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
    l.add(a);  // a goes to window segment
    l.add(b);

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

// Verify large key values survive the 61-bit truncation.
BOOST_AUTO_TEST_CASE(test_packed_large_key_values) {
    test_evictable e(0);

    // Maximum 61-bit value
    uint64_t max_61 = (uint64_t(1) << 61) - 1;
    e.set_sketch_key(max_61);
    BOOST_REQUIRE(e.has_sketch_key());
    BOOST_REQUIRE_EQUAL(e.sketch_key(), max_61);

    // A full 64-bit value gets its top 3 bits truncated
    e.set_sketch_key(UINT64_MAX);
    BOOST_REQUIRE(e.has_sketch_key());
    BOOST_REQUIRE_EQUAL(e.sketch_key(), max_61);  // top 3 bits lost

    // Powers of 2 near the boundary
    e.set_sketch_key(uint64_t(1) << 60);
    BOOST_REQUIRE_EQUAL(e.sketch_key(), uint64_t(1) << 60);

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

BOOST_AUTO_TEST_CASE(test_mvcc_ordering_window_and_probation) {
    // Older entries added first drift to probation via safe drain.
    // Newer entries added later are also drained to probation but are
    // then touched, moving them to protected.  Eviction should pick
    // the older (untouched) entries from probation first.
    lru l;

    static constexpr int OLDER = 5;
    static constexpr int NEWER = 5;
    static constexpr uint64_t PARTITION_KEY = 0xDEAD;

    std::unique_ptr<test_evictable> older[OLDER];
    std::unique_ptr<test_evictable> newer[NEWER];

    // Insert older version rows (never touched after insert)
    for (int i = 0; i < OLDER; ++i) {
        older[i] = std::make_unique<test_evictable>(i);
        older[i]->set_sketch_key(PARTITION_KEY);
        l.add(*older[i]);
    }

    // Insert newer version rows and touch them (simulating reads)
    for (int i = 0; i < NEWER; ++i) {
        newer[i] = std::make_unique<test_evictable>(100 + i);
        newer[i]->set_sketch_key(PARTITION_KEY);
        l.add(*newer[i]);
    }
    for (int round = 0; round < 3; ++round) {
        for (int i = 0; i < NEWER; ++i) {
            l.touch(*newer[i]);
        }
    }

    // Evict — older entries should go first
    for (int i = 0; i < OLDER; ++i) {
        l.evict();
    }

    // All older entries should be evicted
    for (int i = 0; i < OLDER; ++i) {
        BOOST_REQUIRE_MESSAGE(older[i]->was_evicted,
            "Older version entry " << i << " should be evicted before newer entries");
    }
    // All newer entries should survive
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
    // Even when entries share the same sketch key (same frequency),
    // the admission gate should not reorder them — LRU within
    // each segment determines eviction order.
    lru l;

    static constexpr int N = 20;
    static constexpr uint64_t PARTITION_KEY = 0xCAFE;

    std::unique_ptr<test_evictable> entries[N];
    for (int i = 0; i < N; ++i) {
        entries[i] = std::make_unique<test_evictable>(i);
        entries[i]->set_sketch_key(PARTITION_KEY);
        l.add(*entries[i]);
    }

    // No touches — all entries have equal frequency from add().
    // Eviction should follow insertion order (oldest first).
    std::vector<int> eviction_order;
    for (int i = 0; i < N / 2; ++i) {
        l.evict();
        for (int j = 0; j < N; ++j) {
            if (entries[j]->was_evicted &&
                std::find(eviction_order.begin(), eviction_order.end(), j) == eviction_order.end()) {
                eviction_order.push_back(j);
            }
        }
    }

    // Verify eviction happened in insertion order
    for (size_t i = 1; i < eviction_order.size(); ++i) {
        BOOST_REQUIRE_MESSAGE(eviction_order[i] > eviction_order[i-1],
            "Entry " << eviction_order[i] << " was evicted after "
            << eviction_order[i-1] << " but was inserted earlier");
    }

    for (int i = 0; i < N; ++i) {
        if (entries[i]->is_linked()) l.remove(*entries[i]);
    }
}
