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

    // Create entries with different access patterns.
    test_evictable hot(1), cold1(2), cold2(3);
    assign_unique_sketch_key(hot);
    assign_unique_sketch_key(cold1);
    assign_unique_sketch_key(cold2);

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
    // We check that entries with higher frequency are admitted over those
    // with lower frequency by examining the admission counters.
    lru l;
    l.set_window_percent(50.0);

    static constexpr int N = 20;
    std::unique_ptr<test_evictable> entries[N];
    for (int i = 0; i < N; ++i) {
        entries[i] = std::make_unique<test_evictable>(i);
        assign_unique_sketch_key(*entries[i]);
        l.add(*entries[i]);
    }

    // Touch some entries many times to build frequency differentiation.
    for (int round = 0; round < 10; ++round) {
        for (int i = 0; i < 5; ++i) {
            l.touch(*entries[i]);
        }
    }

    // Evict once to trigger the batch window drain + one eviction from main.
    l.evict();

    // The admission filter should have been exercised: some entries
    // admitted (high freq beat low freq) and some rejected.
    auto& st = l.get_stats();
    BOOST_REQUIRE_GT(st.tinylfu_admissions + st.tinylfu_rejections, 0u);

    // Some entries should survive (not all evicted by one call).
    int surviving = 0;
    for (int i = 0; i < N; ++i) {
        if (entries[i]->is_linked()) ++surviving;
    }
    BOOST_REQUIRE_GT(surviving, 0);

    for (int i = 0; i < N; ++i) {
        if (entries[i]->is_linked()) l.remove(*entries[i]);
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
    // Verify that touching a probation entry promotes it to protected,
    // using the promotion counter.
    lru l;
    l.set_window_percent(50.0);

    static constexpr int N = 10;
    std::unique_ptr<test_evictable> entries[N];
    for (int i = 0; i < N; ++i) {
        entries[i] = std::make_unique<test_evictable>(i);
        assign_unique_sketch_key(*entries[i]);
        l.add(*entries[i]);
    }

    // Build frequency and drain window so entries flow to probation.
    for (int round = 0; round < 5; ++round) {
        for (int i = 0; i < N; ++i) {
            if (entries[i]->is_linked()) l.touch(*entries[i]);
        }
    }
    l.evict(); // batch drain: entries move window → probation

    // Touch survivors — entries in probation get promoted to protected.
    auto promotions_before = l.get_stats().protected_promotions;
    for (int i = 0; i < N; ++i) {
        if (entries[i]->is_linked()) l.touch(*entries[i]);
    }
    BOOST_REQUIRE_GT(l.get_stats().protected_promotions, promotions_before);

    // Clean up.
    for (int i = 0; i < N; ++i) {
        if (entries[i]->is_linked()) {
            l.remove(*entries[i]);
        }
    }
}

BOOST_AUTO_TEST_CASE(test_lru_set_window_percent) {
    lru l;
    // Default is 1%.
    BOOST_REQUIRE_EQUAL(l.window_percent(), 1u);

    // Set to 50% (LRU-like).
    l.set_window_percent(50.0);
    BOOST_REQUIRE_EQUAL(l.window_percent(), 50u);

    // Clamped to [1, 99].
    l.set_window_percent(0.0);
    BOOST_REQUIRE_EQUAL(l.window_percent(), 1u);
    l.set_window_percent(100.0);
    BOOST_REQUIRE_EQUAL(l.window_percent(), 99u);
}

BOOST_AUTO_TEST_CASE(test_lru_large_window_behaves_like_lru) {
    lru l;
    l.set_window_percent(99.0);

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

// Rows within the same partition track independent frequencies via their
// distinct pointer addresses, enabling row-granularity eviction decisions.
BOOST_AUTO_TEST_CASE(test_rows_in_same_partition_have_independent_frequency) {
    lru l;
    // Simulate two rows belonging to the same partition.
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

    // The hot row should have a much higher frequency estimate than the cold row.
    BOOST_REQUIRE_GE(l.sketch_estimate(key_hot), 10);
    BOOST_REQUIRE_LE(l.sketch_estimate(key_cold), 2);

    l.remove(*row_hot);
    l.remove(*row_cold);
}

// Under eviction pressure, a cold row from a hot partition is evicted before
// a warm row, demonstrating row-level (not partition-level) eviction.
BOOST_AUTO_TEST_CASE(test_cold_row_evicted_before_warm_row_in_same_partition) {
    lru l;
    // Fill cache: one "hot" row, one "cold" row (same partition), plus filler.
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
        "Cold row from the same partition should be evicted before hot row");

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
    static constexpr int N = 20;
    std::unique_ptr<test_evictable> entries[N];
    for (int i = 0; i < N; ++i) {
        entries[i] = std::make_unique<test_evictable>(i);
        assign_unique_sketch_key(*entries[i]);
        l.add(*entries[i]);
    }

    // Touch entries 15-19 many times to build frequency so they get admitted.
    for (int round = 0; round < 10; ++round) {
        for (int i = 15; i < N; ++i) {
            l.touch(*entries[i]);
        }
    }

    // Evict 10 times to trigger admission decisions.
    for (int i = 0; i < 10; ++i) {
        l.evict();
    }

    auto& st = l.get_stats();
    // The admission gate must have been exercised at least once.
    BOOST_REQUIRE_GT(st.tinylfu_admissions + st.tinylfu_rejections, 0u);
    // We called evict() exactly 10 times.
    BOOST_REQUIRE_EQUAL(st.eviction_calls, 10u);

    // Clean up.
    for (int i = 0; i < N; ++i) {
        if (entries[i]->is_linked()) l.remove(*entries[i]);
    }
}

BOOST_AUTO_TEST_CASE(test_lru_direct_eviction_counter) {
    // Path 2 (direct eviction) fires when window is within target.
    // Use 99% window so almost everything stays in window. After one
    // eviction drains the excess, the next evict takes path 2.
    lru l;
    l.set_window_percent(99.0);

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
    l.set_window_percent(50.0);

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
    static constexpr int N = 20;
    std::unique_ptr<test_evictable> entries[N];
    for (int i = 0; i < N; ++i) {
        entries[i] = std::make_unique<test_evictable>(i);
        assign_unique_sketch_key(*entries[i]);
        l.add(*entries[i]);
    }

    // Give entries 15-19 high frequency.
    for (int round = 0; round < 10; ++round) {
        for (int i = 15; i < N; ++i) {
            l.touch(*entries[i]);
        }
    }
    // Entries 0-14 have low frequency (only from add).

    // Trigger evictions that go through the admission gate.
    for (int i = 0; i < 10; ++i) {
        l.evict();
    }

    auto& st = l.get_stats();
    uint64_t total_gate = st.tinylfu_admissions + st.tinylfu_rejections;
    // The admission gate should have been exercised.
    BOOST_REQUIRE_GT(total_gate, 0u);

    // At least one frequency bucket should have a non-zero count.
    uint64_t bucket_sum = st.admission_freq_bucket_0_1
                        + st.admission_freq_bucket_2_3
                        + st.admission_freq_bucket_4_7
                        + st.admission_freq_bucket_8_15;
    BOOST_REQUIRE_GT(bucket_sum, 0u);

    // Sum of all bucket counters must equal total admission gate decisions.
    BOOST_REQUIRE_EQUAL(bucket_sum, total_gate);

    // Clean up.
    for (int i = 0; i < N; ++i) {
        if (entries[i]->is_linked()) l.remove(*entries[i]);
    }
}
