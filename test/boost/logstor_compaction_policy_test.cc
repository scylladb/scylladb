/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <algorithm>
#include <cmath>
#include <deque>
#include <limits>
#include <random>
#include <ranges>
#include <utility>
#include <vector>

#include <seastar/testing/thread_test_case.hh>

#include <boost/test/unit_test.hpp>

#include "replica/logstor/compaction.hh"
#include "replica/logstor/compaction_policy.hh"
#include "replica/logstor/compaction_rate_controller.hh"

using namespace replica::logstor;

// Checks that free-segment watermarks are correctly derived from disk size and target fraction,
// with floor/cap enforcement ensuring no configuration prevents compaction from starting or stopping.
SEASTAR_THREAD_TEST_CASE(test_logstor_free_segment_watermarks) {
    // Properties that have to hold whatever the disk size and whatever the live-updatable trigger
    // threshold happens to be set to.
    auto check = [] (uint64_t segment_count, double fraction) {
        const auto w = make_free_segment_watermarks(segment_count, fraction);
        BOOST_TEST_CONTEXT("segment_count=" << segment_count << " fraction=" << fraction) {
            BOOST_REQUIRE_LE(w.low, w.high);
            // `high` has to stay reachable, or automatic compaction could never stop.
            BOOST_REQUIRE_LE(w.high, segment_count);
            // And there has to be a band for the hysteresis to work in, unless the trigger is
            // disabled or the target already covers the whole disk.
            BOOST_REQUIRE(w.low < w.high || w.low == 0 || w.high == segment_count);
        }
        return w;
    };
    for (uint64_t segment_count : {1u, 8u, 32u, 128u, 1024u, 100000u}) {
        for (double fraction : {0.0, 0.001, 0.05, 0.2, 0.9, 1.0, 1.5, -0.05, std::nan(""), std::numeric_limits<double>::infinity()}) {
            check(segment_count, fraction);
        }
    }

    // On a disk large enough for the configured fraction to clear the floor, that is what the
    // target is.
    const auto large = check(100000, 0.05);
    BOOST_REQUIRE_EQUAL(large.low, 5000u);
    const auto smaller = check(10000, 0.05);
    BOOST_REQUIRE_EQUAL(smaller.low, 500u);

    // An explicitly configured fraction is never clamped down by the floor.
    BOOST_REQUIRE_EQUAL(check(100000, 0.20).low, 20000u);

    // Where the fraction rounds down to a segment or two the absolute floor takes over, so that the
    // target still covers what compaction holds while it works.
    const auto floored = check(1024, 0.001);
    BOOST_REQUIRE_GT(floored.low, static_cast<uint64_t>(std::ceil(1024 * 0.001)));
    BOOST_REQUIRE_GE(floored.low, 2 * min_segments_per_compaction);

    // On a disk too small for even that, the floor is itself capped, so it cannot claim an
    // unreasonable share of the disk.
    const auto tiny = check(32, 0.05);
    BOOST_REQUIRE_LE(tiny.low, 32u / 4);

    // Zero disables the trigger, and so does anything out of range: the threshold is live-updatable
    // with no range check at the config layer, and a negative value must not underflow into a target
    // that swallows the disk.
    for (double fraction : {0.0, -0.05, -1.0, std::nan(""), -std::numeric_limits<double>::infinity()}) {
        BOOST_TEST_CONTEXT("fraction=" << fraction) {
            const auto disabled = make_free_segment_watermarks(100000, fraction);
            BOOST_REQUIRE_EQUAL(disabled.low, 0u);
            BOOST_REQUIRE_EQUAL(disabled.high, 0u);
        }
    }

    // A fraction at or above 1 asks for the whole disk, which is as far as it can go.
    const auto over_one = check(100000, 1.5);
    BOOST_REQUIRE_EQUAL(over_one.low, 100000u);
    BOOST_REQUIRE_EQUAL(over_one.high, 100000u);
}

// Checks that compaction limits enforce that concurrent jobs don't exceed the free-segment target,
// preventing deadlock, with batch size shrinking first and parallelism second as target shrinks.
SEASTAR_THREAD_TEST_CASE(test_logstor_compaction_limits) {
    constexpr size_t max_batch_cap = 32;

    // A compaction job frees its inputs only when it is done, so what all the jobs in flight can
    // hold at once has to fit in the free-segment target. This is the relation the two limits exist
    // to maintain, and it has to survive any combination of disk size, threshold and configured cap.
    for (uint64_t segment_count : {32u, 128u, 256u, 1024u, 4096u, 100000u}) {
        for (double fraction : {0.0, 0.01, 0.05, 0.2, 1.0}) {
            for (size_t cap : {1u, 8u, 32u, 1024u}) {
                const auto watermarks = make_free_segment_watermarks(segment_count, fraction);
                const auto limits = make_compaction_limits(watermarks.low, cap);
                BOOST_TEST_CONTEXT("segment_count=" << segment_count << " fraction=" << fraction << " cap=" << cap) {
                    BOOST_REQUIRE_GE(limits.auto_parallelism, 1u);
                    BOOST_REQUIRE_LE(limits.auto_parallelism, max_auto_compaction_parallelism);
                    // A smaller batch could not reclaim anything, so it would never be worth running.
                    BOOST_REQUIRE_GE(limits.batch_cap, min_segments_per_compaction);
                    BOOST_REQUIRE_LE(limits.batch_cap, std::max(cap, min_segments_per_compaction));
                    // Except on a disk whose target is below a single batch, where the target has
                    // already been capped by the disk size and there is nothing left to give.
                    if (watermarks.low >= min_segments_per_compaction) {
                        BOOST_REQUIRE_LE(limits.auto_parallelism * limits.batch_cap, watermarks.low);
                    }
                }
            }
        }
    }

    // A disk large enough for the target to cover every job in flight gets the configured batch
    // bound and the full parallelism.
    const auto large = make_compaction_limits(make_free_segment_watermarks(100000, 0.05).low, max_batch_cap);
    BOOST_REQUIRE_EQUAL(large.auto_parallelism, max_auto_compaction_parallelism);
    BOOST_REQUIRE_EQUAL(large.batch_cap, max_batch_cap);

    // A disabled trigger leaves no target to protect and no automatic compaction to protect it
    // from, but compaction submitted explicitly still needs a batch it can reclaim with.
    const auto no_trigger = make_compaction_limits(make_free_segment_watermarks(100000, 0.0).low, max_batch_cap);
    BOOST_REQUIRE_EQUAL(no_trigger.auto_parallelism, 1u);
    BOOST_REQUIRE_EQUAL(no_trigger.batch_cap, min_segments_per_compaction);

    // A configured bound below the smallest useful batch would leave every batch unable to reclaim,
    // so it is raised rather than honoured.
    const auto tiny_bound = make_compaction_limits(make_free_segment_watermarks(100000, 0.05).low, 1);
    BOOST_REQUIRE_EQUAL(tiny_bound.batch_cap, min_segments_per_compaction);
}

// Verifies that compaction shares pressure scales with free space, stays monotone and within [0, 1],
// and places the target at the optimal control point regardless of disk size or degenerate watermarks.
SEASTAR_THREAD_TEST_CASE(test_logstor_compaction_shares_pressure) {
    for (uint64_t segment_count : {128u, 4000u, 100000u}) {
        for (double fraction : {0.05, 0.2}) {
            const auto watermarks = make_free_segment_watermarks(segment_count, fraction);
            BOOST_TEST_CONTEXT("segment_count=" << segment_count << " fraction=" << fraction) {
                // At and above the watermark where automatic compaction stops there is no space
                // demand at all, so the controller asks for no more than its floor.
                BOOST_REQUIRE_EQUAL(compaction_shares_pressure(segment_count, watermarks), 0.0f);
                BOOST_REQUIRE_EQUAL(compaction_shares_pressure(watermarks.high, watermarks), 0.0f);
                BOOST_REQUIRE_EQUAL(compaction_shares_pressure(0, watermarks), 1.0f);

                // The free-segment target is the intended steady-state operating point, and the
                // relative hysteresis puts it a third of the way up the ramp on any disk size -
                // which is where the shares controller puts its middle control point. Only up to
                // the rounding of the two watermarks to whole segments, hence the tolerance.
                BOOST_REQUIRE_CLOSE(compaction_shares_pressure(watermarks.low, watermarks),
                        compaction_shares_pressure_at_target, 10.0);
            }
        }
    }

    // A disabled trigger has no free-segment target, hence no space-driven demand for shares, even
    // with the disk fully consumed.
    BOOST_REQUIRE_EQUAL(compaction_shares_pressure(0, make_free_segment_watermarks(4000, 0.0)), 0.0f);

    // A one-segment target degenerates to a zero saturation point; pressure must stay in range and
    // still reach 1 rather than dividing by zero.
    const free_segment_watermarks minimal{.low = 1, .high = 2};
    BOOST_REQUIRE_EQUAL(compaction_shares_pressure(2, minimal), 0.0f);
    BOOST_REQUIRE_GT(compaction_shares_pressure(1, minimal), 0.0f);
    BOOST_REQUIRE_LT(compaction_shares_pressure(1, minimal), 1.0f);
    BOOST_REQUIRE_EQUAL(compaction_shares_pressure(0, minimal), 1.0f);
}

SEASTAR_THREAD_TEST_CASE(test_logstor_compaction_candidate_score_ranks_by_efficiency) {
    constexpr uint64_t segment_size = 128 * 1024;

    // Rewriting 8 segments into 6 reclaims two segments, but copies 5.28 segments worth of live
    // data to do it - a marginal write amplification of 2.6. Rewriting the emptiest 3 into 2
    // reclaims only one segment, for 1.98 segments copied. The efficiency rule prefers the latter.
    const compaction_candidate_score big{.n_in = 8, .n_out = 6, .live_bytes = 528 * segment_size / 100};
    const compaction_candidate_score cheap{.n_in = 3, .n_out = 2, .live_bytes = 198 * segment_size / 100};
    BOOST_REQUIRE_EQUAL(big.reclaimed(), 2u);
    BOOST_REQUIRE_EQUAL(cheap.reclaimed(), 1u);
    BOOST_REQUIRE_LT(big.efficiency(segment_size), cheap.efficiency(segment_size));
    BOOST_REQUIRE(big < cheap);

    // Efficiency is the reciprocal of the batch's marginal write amplification.
    BOOST_REQUIRE_CLOSE(cheap.efficiency(segment_size), 1.0 / 1.98, 0.1);

    // Equal efficiency is broken by reclaiming more per job.
    const compaction_candidate_score two_in{.n_in = 2, .n_out = 1, .live_bytes = segment_size};
    const compaction_candidate_score four_in{.n_in = 4, .n_out = 2, .live_bytes = 2 * segment_size};
    BOOST_REQUIRE_EQUAL(two_in.efficiency(segment_size), four_in.efficiency(segment_size));
    BOOST_REQUIRE(two_in < four_in);

    // A batch of fully dead segments copies nothing and outranks any batch that copies.
    const compaction_candidate_score all_dead{.n_in = 2, .n_out = 0, .live_bytes = 0};
    BOOST_REQUIRE(cheap < all_dead);
    BOOST_REQUIRE(four_in < all_dead);
}

// Checks that top_compaction_candidates keeps the best candidates by score, and returns them worst
// first. test/boost/largest_k_test.cc tests the ranking itself.
SEASTAR_THREAD_TEST_CASE(test_logstor_top_compaction_candidates) {
    struct candidate {
        int id;
        compaction_candidate_score score;
    };

    // Each candidate reclaims one segment, so a candidate that copies fewer live bytes has a better
    // score.
    auto make_candidate = [] (int id, uint64_t live_bytes) {
        return candidate{.id = id, .score = {.n_in = 2, .n_out = 1, .live_bytes = live_bytes}};
    };
    auto ids = [] (const std::vector<candidate>& candidates) {
        return candidates | std::views::transform(&candidate::id) | std::ranges::to<std::vector>();
    };

    // The best three of five are kept, in any order of arrival.
    top_compaction_candidates<candidate> top(3);
    top.push(make_candidate(3, 3000));
    top.push(make_candidate(5, 5000));
    top.push(make_candidate(1, 1000));
    top.push(make_candidate(4, 4000));
    top.push(make_candidate(2, 2000));
    BOOST_REQUIRE(ids(std::move(top).take_sorted()) == (std::vector<int>{3, 2, 1}));
}

// Checks that compaction candidate selection chooses segments in ascending utilization order,
// respects the batch cap, and the returned score accurately describes the selected segments.
SEASTAR_THREAD_TEST_CASE(test_logstor_select_compaction_batch) {
    constexpr uint64_t segment_size = 128 * 1024;
    constexpr size_t record_size = 1024;
    constexpr size_t records_per_segment = segment_size / record_size;
    // About a tenth live, so that a few of them rewrite into a single segment.
    constexpr size_t sparse_records = records_per_segment / 10;
    // A batch of n segments reclaims only when its mean utilization is below 1 - 1/n, so at this
    // utilization no batch that select_compaction_batch() would consider has a net gain.
    constexpr size_t dense_records = records_per_segment * 98 / 100;
    constexpr size_t sparse_count = 3;

    // Descriptors are intrusively linked into the sets, so they must keep their addresses, and every
    // set must be destroyed before them.
    std::deque<segment_descriptor> descs;
    segment_set segments{segment_size};
    auto add_segment = [&] (segment_set& set, size_t live_records) {
        auto& desc = descs.emplace_back();
        desc.reset(segment_size);
        desc.on_write(live_records * record_size, live_records);
        set.add_segment(desc);
    };

    // Added interleaved, so that the batch below can only be right if selection went through the
    // free-space histogram rather than the order the segments were added in.
    for (size_t i = 0; i < sparse_count; ++i) {
        add_segment(segments, dense_records);
        add_segment(segments, sparse_records);
    }

    // What the caller relies on for any batch: the segments come least utilized first, and the score
    // describes exactly the segments returned. The same score is what ranks this group against the
    // others in find_top_compaction_candidates(), so a score describing a different batch would rank
    // the group by work it is not about to do.
    auto check_batch = [&] (const compaction_batch& batch) {
        BOOST_REQUIRE(!batch.segments.empty());
        BOOST_REQUIRE_EQUAL(batch.score.n_in, batch.segments.size());
        BOOST_REQUIRE_GT(batch.score.reclaimed(), 0u);
        uint64_t live_bytes = 0;
        size_t prev_record_bytes = 0;
        for (const auto* desc : batch.segments) {
            BOOST_REQUIRE_GE(desc->record_bytes(segment_size), prev_record_bytes);
            prev_record_bytes = desc->record_bytes(segment_size);
            live_bytes += desc->record_bytes(segment_size);
        }
        BOOST_REQUIRE_EQUAL(batch.score.live_bytes, live_bytes);
    };

    auto batch = select_compaction_batch(segments, segment_size, compaction_batch_limits{.batch_cap = min_segments_per_compaction});
    BOOST_REQUIRE(batch);
    check_batch(*batch);

    // Only the sparse segments are worth taking: they rewrite into a single segment, while extending
    // into an almost fully live one would cost far more than the segment it reclaims.
    BOOST_REQUIRE_EQUAL(batch->segments.size(), sparse_count);
    for (const auto* desc : batch->segments) {
        BOOST_REQUIRE_EQUAL(desc->record_bytes(segment_size), sparse_records * record_size);
    }

    // The cap bounds the candidate set, not only the prefix chosen out of it.
    auto capped = select_compaction_batch(segments, segment_size, compaction_batch_limits{.batch_cap = 2});
    BOOST_REQUIRE(capped);
    check_batch(*capped);
    BOOST_REQUIRE_LE(capped->segments.size(), 2u);

    // A group whose segments are all nearly full has no batch with a net gain, which is the answer
    // for the whole group and not only for the prefix that happened to be scored.
    segment_set dense{segment_size};
    for (size_t i = 0; i < min_segments_per_compaction; ++i) {
        add_segment(dense, dense_records);
    }
    BOOST_REQUIRE(!select_compaction_batch(dense, segment_size, compaction_batch_limits{.batch_cap = min_segments_per_compaction}));

    // An empty group has nothing to compact.
    segment_set empty{segment_size};
    BOOST_REQUIRE(!select_compaction_batch(empty, segment_size, compaction_batch_limits{.batch_cap = min_segments_per_compaction}));
}

// A job next to a job in flight starts only if its batch reclaims nearly as well as the best batch.
// The best batch passes its own bar. A batch of dead segments passes any bar, but does not set one.
SEASTAR_THREAD_TEST_CASE(test_logstor_marginal_compaction_admission) {
    constexpr uint64_t segment_size = 128 * 1024;
    const compaction_candidate_score rich{.n_in = 8, .n_out = 4, .live_bytes = 4 * segment_size};
    const compaction_candidate_score near{.n_in = 9, .n_out = 5, .live_bytes = 5 * segment_size};
    const compaction_candidate_score marginal{.n_in = 8, .n_out = 7, .live_bytes = 7 * segment_size};
    const compaction_candidate_score all_dead{.n_in = 2, .n_out = 0, .live_bytes = 0};

    BOOST_REQUIRE(rich.efficiency_at_least(rich, compaction_marginal_admission_ratio));
    BOOST_REQUIRE(near.efficiency_at_least(rich, compaction_marginal_admission_ratio));
    BOOST_REQUIRE(!marginal.efficiency_at_least(rich, compaction_marginal_admission_ratio));
    BOOST_REQUIRE(all_dead.efficiency_at_least(rich, compaction_marginal_admission_ratio));
    BOOST_REQUIRE(!rich.efficiency_at_least(all_dead, compaction_marginal_admission_ratio));
}

// A job frees its inputs only when it ends, so the jobs in flight must fit in the space floor, and
// each batch must still be large enough to reclaim.
SEASTAR_THREAD_TEST_CASE(test_logstor_compaction_limits_fit_the_space_floor) {
    uint64_t previous_space_floor = 0;
    for (uint64_t segment_count : {32u, 128u, 1024u, 8192u, 100000u}) {
        const auto space_floor = compaction_space_floor(segment_count);
        BOOST_TEST_CONTEXT("segment_count=" << segment_count) {
            // A small part of the disk, which does not decrease as the disk grows.
            BOOST_REQUIRE_GE(space_floor, 1u);
            BOOST_REQUIRE_LE(space_floor, segment_count / 4);
            BOOST_REQUIRE_GE(space_floor, previous_space_floor);
            for (size_t cap : {1u, 8u, 32u, 1024u}) {
                const auto limits = make_compaction_limits(space_floor, cap);
                BOOST_TEST_CONTEXT("cap=" << cap) {
                    BOOST_REQUIRE_GE(limits.auto_parallelism, 1u);
                    BOOST_REQUIRE_LE(limits.auto_parallelism, max_auto_compaction_parallelism);
                    // A smaller batch cannot reclaim, so a cap below it is raised.
                    BOOST_REQUIRE_GE(limits.batch_cap, min_segments_per_compaction);
                    BOOST_REQUIRE_LE(limits.batch_cap, std::max<size_t>(cap, min_segments_per_compaction));
                    if (space_floor >= min_segments_per_compaction) {
                        BOOST_REQUIRE_LE(limits.auto_parallelism * limits.batch_cap, space_floor);
                    }
                }
            }
        }
        previous_space_floor = space_floor;
    }

    // A large disk runs at the full parallelism and the full batch cap.
    const auto large = make_compaction_limits(compaction_space_floor(100000), 32);
    BOOST_REQUIRE_EQUAL(large.auto_parallelism, max_auto_compaction_parallelism);
    BOOST_REQUIRE_EQUAL(large.batch_cap, 32u);
}

// A job runs on a full batch while there is no space pressure, on fewer candidates as the pressure
// increases, and on any candidate at the space floor.
SEASTAR_THREAD_TEST_CASE(test_logstor_effective_min_batch) {
    BOOST_REQUIRE_EQUAL(effective_min_batch(0), min_segments_per_compaction);
    BOOST_REQUIRE_EQUAL(effective_min_batch(1), 1u);
    size_t previous = min_segments_per_compaction;
    for (double pressure = 0; pressure <= 1; pressure += 0.05) {
        BOOST_REQUIRE_LE(effective_min_batch(pressure), previous);
        previous = effective_min_batch(pressure);
    }
}

// What the curve promises at each free level: the threshold is 1 at the space floor and below, the
// configured threshold at the anchor, and the minimum with the whole disk free, and it does not
// increase with the free level. The space pressure is 1 at the space floor and below, 0 at the
// anchor and above, and does not increase with the free level either.
static void check_threshold_curve(uint64_t segments, const utilization_threshold_curve& curve, double threshold, double min_threshold) {
    double previous_threshold = 1;
    double previous_pressure = 1;
    for (uint64_t available = 0; available <= segments; ++available) {
        const auto t = effective_utilization_threshold(available, curve);
        const auto pressure = compaction_space_pressure(available, curve);
        BOOST_TEST_CONTEXT("available=" << available) {
            BOOST_REQUIRE_LE(t, previous_threshold);
            BOOST_REQUIRE_GE(t, min_threshold);
            BOOST_REQUIRE_LE(pressure, previous_pressure);
            BOOST_REQUIRE_GE(pressure, 0.0);
            if (available <= curve.space_floor) {
                BOOST_REQUIRE_EQUAL(t, 1.0);
                BOOST_REQUIRE_EQUAL(pressure, 1.0);
            }
            if (available >= curve.anchor) {
                BOOST_REQUIRE_EQUAL(pressure, 0.0);
            }
        }
        previous_threshold = t;
        previous_pressure = pressure;
    }
    BOOST_REQUIRE_EQUAL(effective_utilization_threshold(curve.anchor, curve), threshold);
    BOOST_REQUIRE_EQUAL(effective_utilization_threshold(segments, curve), min_threshold);
}

SEASTAR_THREAD_TEST_CASE(test_logstor_utilization_threshold_curve) {
    const auto nan = std::numeric_limits<double>::quiet_NaN();
    // A disk too small for the default anchor and ceiling, and a normal one.
    for (auto [segments, space_floor] : {std::pair<uint64_t, uint64_t>{16, 2}, std::pair<uint64_t, uint64_t>{200, 6}}) {
        for (double threshold : {0.0, 0.1, 0.5, 0.9}) {
            // A minimum above the threshold is the threshold, and one that is not a number is 0.
            for (double min_threshold : {0.0, 0.1, threshold, 1.0, nan}) {
                const auto expected_min = std::isnan(min_threshold) ? 0.0 : std::min(min_threshold, threshold);
                // The default anchor and ceiling, and an anchor at 10% and a ceiling at 30% of the disk.
                for (auto [anchor, ceiling] : {std::pair{0.0, 0.0}, std::pair{0.1, 0.3}}) {
                    BOOST_TEST_CONTEXT("segments=" << segments << " threshold=" << threshold << " min=" << min_threshold
                            << " anchor=" << anchor << " ceiling=" << ceiling) {
                        const auto curve = make_utilization_threshold_curve(segments, space_floor, threshold, anchor, ceiling, min_threshold);
                        check_threshold_curve(segments, curve, threshold, expected_min);
                    }
                }
            }
        }
    }

    // The default ceiling is one segment above the anchor, so above the anchor only segments under
    // the minimum are compacted, which holds the free level at the anchor.
    const auto stiff = make_utilization_threshold_curve(200, 6, 0.5, 0, 0, 0.1);
    BOOST_REQUIRE_EQUAL(effective_utilization_threshold(stiff.anchor + 1, stiff), 0.1);

    // A minimum at the threshold keeps the threshold constant above the anchor.
    const auto flat = make_utilization_threshold_curve(200, 6, 0.5, 0.1, 0.3, 0.5);
    for (uint64_t available = flat.anchor; available <= 200; ++available) {
        BOOST_REQUIRE_EQUAL(effective_utilization_threshold(available, flat), 0.5);
    }
}

// A batch under a utilization threshold holds only segments under it, also where the threshold cuts
// a bucket of the histogram. A group with fewer candidates than the minimum batch has no batch,
// unless it has dead segments.
SEASTAR_THREAD_TEST_CASE(test_logstor_select_compaction_batch_threshold) {
    constexpr uint64_t segment_size = 128 * 1024;
    constexpr size_t record_size = 1024;
    constexpr size_t records_per_segment = segment_size / record_size;

    std::deque<segment_descriptor> descs;
    auto add_segment = [&] (segment_set& set, size_t live_records) {
        auto& desc = descs.emplace_back();
        desc.reset(segment_size);
        desc.on_write(live_records * record_size, live_records);
        set.add_segment(desc);
    };
    auto limits = [] (size_t min_batch, double threshold) {
        return compaction_batch_limits{
            .batch_cap = min_segments_per_compaction,
            .min_batch = min_batch,
            .max_utilization = threshold,
        };
    };

    // A threshold inside the lowest bucket applies as 1, so the thresholds stay under it.
    std::mt19937 rng(1);
    for (int i = 0; i < 200; ++i) {
        segment_set segments{segment_size};
        const auto segment_count = 1 + rng() % 24;
        for (size_t j = 0; j < segment_count; ++j) {
            add_segment(segments, rng() % (records_per_segment + 1));
        }
        const double threshold = 0.05 + 0.9 * (rng() % 1000) / 1000.0;
        BOOST_TEST_CONTEXT("i=" << i << " threshold=" << threshold) {
            auto batch = select_compaction_batch(segments, segment_size, limits(1, threshold));
            if (batch) {
                for (const auto* desc : batch->segments) {
                    BOOST_REQUIRE_LE(desc->record_bytes(segment_size), threshold * segment_size);
                }
            }
        }
    }

    // Segments just under and just over a threshold of a quarter share a bucket of the histogram. A
    // segment over the threshold does not hide the ones under it, whatever their order.
    segment_set straddling{segment_size};
    for (size_t live_records : {31, 30, 33, 34, 35}) {
        add_segment(straddling, live_records);
    }
    auto under = select_compaction_batch(straddling, segment_size, limits(2, 0.25));
    BOOST_REQUIRE(under);
    BOOST_REQUIRE_EQUAL(under->segments.size(), 2u);

    // Three segments a tenth full, and five segments six tenths full.
    segment_set mixed{segment_size};
    for (size_t i = 0; i < 3; ++i) {
        add_segment(mixed, records_per_segment / 10);
    }
    for (size_t i = 0; i < 5; ++i) {
        add_segment(mixed, records_per_segment * 6 / 10);
    }
    BOOST_REQUIRE(!select_compaction_batch(mixed, segment_size, limits(4, 0.5)));
    auto three = select_compaction_batch(mixed, segment_size, limits(3, 0.5));
    BOOST_REQUIRE(three);
    BOOST_REQUIRE_EQUAL(three->segments.size(), 3u);
    BOOST_REQUIRE(!select_compaction_batch(mixed, segment_size, limits(1, 0.05)));

    // Dead segments do not wait for the minimum batch.
    segment_set with_dead{segment_size};
    for (size_t i = 0; i < 3; ++i) {
        add_segment(with_dead, 0);
    }
    for (size_t i = 0; i < 2; ++i) {
        add_segment(with_dead, records_per_segment / 2);
    }
    auto dead = select_compaction_batch(with_dead, segment_size, limits(min_segments_per_compaction, 0.6));
    BOOST_REQUIRE(dead);
    BOOST_REQUIRE_EQUAL(dead->segments.size(), 3u);
    for (const auto* desc : dead->segments) {
        BOOST_REQUIRE_EQUAL(desc->record_bytes(segment_size), 0u);
    }
}

// The backlog counts the segments under the threshold. What compacting them would free is at most
// their number, and neither count decreases as the threshold increases.
SEASTAR_THREAD_TEST_CASE(test_logstor_reclaimable_backlog) {
    utilization_histogram utilization{};
    // Ten dead segments, four in the bucket just under a half, two just over it, one full.
    utilization[0] = 10;
    utilization[utilization_bucket_count / 2 - 1] = 4;
    utilization[utilization_bucket_count / 2] = 2;
    utilization[utilization_bucket_count - 1] = 1;

    // At the edge of a bucket the count is exact. A threshold of 0 still counts the dead segments.
    BOOST_REQUIRE_CLOSE(reclaimable_backlog(utilization, 0.0).candidate_segments, 10.0, 1e-9);
    BOOST_REQUIRE_CLOSE(reclaimable_backlog(utilization, 0.5).candidate_segments, 14.0, 1e-9);
    BOOST_REQUIRE_CLOSE(reclaimable_backlog(utilization, 1.0).candidate_segments, 17.0, 1e-9);
    BOOST_REQUIRE_GT(reclaimable_backlog(utilization, 0.0).reclaimable_segments, 9.5);

    compaction_backlog previous{};
    for (int i = 0; i <= 100; ++i) {
        const double threshold = i / 100.0;
        const auto backlog = reclaimable_backlog(utilization, threshold);
        BOOST_TEST_CONTEXT("threshold=" << threshold) {
            BOOST_REQUIRE_GE(backlog.candidate_segments, previous.candidate_segments);
            BOOST_REQUIRE_GE(backlog.reclaimable_segments, previous.reclaimable_segments);
            BOOST_REQUIRE_LE(backlog.reclaimable_segments, backlog.candidate_segments);
        }
        previous = backlog;
    }

    BOOST_REQUIRE_EQUAL(reclaimable_backlog(utilization_histogram{}, 0.5).candidate_segments, 0.0);
}

// The controller applies the curve and the minimum batch at the free level of each sample, and counts
// the backlog under the threshold it applies. So space pressure adds fuller segments to the backlog.
SEASTAR_THREAD_TEST_CASE(test_logstor_compaction_rate_controller_space_pressure) {
    const auto curve = make_utilization_threshold_curve(20000, 200, 0.5, 0, 0, 0.5);
    utilization_histogram utilization{};
    // Dead segments, and segments about 0.65 and 0.9 full.
    utilization[0] = 8;
    utilization[utilization_bucket_count * 5 / 8] = 5;
    utilization[utilization_bucket_count * 7 / 8] = 2;
    compaction_rate_controller controller;

    double previous_candidates = 0;
    for (uint64_t available : {600u, 300u, 200u}) {
        controller.tick({.available_segments = available, .curve = curve, .utilization = utilization});
        BOOST_TEST_CONTEXT("available=" << available) {
            const auto pressure = compaction_space_pressure(available, curve);
            BOOST_REQUIRE(controller.enabled());
            BOOST_REQUIRE_EQUAL(controller.space_pressure(), pressure);
            BOOST_REQUIRE_EQUAL(controller.utilization_threshold(), effective_utilization_threshold(available, curve));
            BOOST_REQUIRE_EQUAL(controller.min_batch(), effective_min_batch(pressure));
            BOOST_REQUIRE_GT(controller.backlog().candidate_segments, previous_candidates);
        }
        previous_candidates = controller.backlog().candidate_segments;
    }
}

// A threshold outside [0, 1) disables automatic compaction: no backlog and no space pressure,
// whatever the free level.
SEASTAR_THREAD_TEST_CASE(test_logstor_compaction_rate_controller_disabled) {
    utilization_histogram utilization{};
    utilization[0] = 100;
    auto sample_at = [&] (double threshold) {
        return compaction_rate_controller::sample{
            .available_segments = 0,
            .curve = make_utilization_threshold_curve(20000, 200, threshold, 0, 0, threshold),
            .utilization = utilization,
        };
    };

    for (double threshold : {1.0, -0.5, 1.5, std::numeric_limits<double>::quiet_NaN(), std::numeric_limits<double>::infinity()}) {
        compaction_rate_controller controller;
        controller.tick(sample_at(threshold));
        BOOST_TEST_CONTEXT("threshold=" << threshold) {
            BOOST_REQUIRE(!controller.enabled());
            BOOST_REQUIRE_EQUAL(controller.space_pressure(), 0.0);
            BOOST_REQUIRE_EQUAL(controller.backlog().candidate_segments, 0.0);
        }
    }

    // The edges of the range are valid: a threshold of zero compacts only dead segments.
    compaction_rate_controller controller;
    controller.tick(sample_at(0.0));
    BOOST_REQUIRE(controller.enabled());
    BOOST_REQUIRE_EQUAL(controller.space_pressure(), 1.0);

    // Disabling a running controller drops what it had.
    controller.tick(sample_at(1.0));
    BOOST_REQUIRE(!controller.enabled());
    BOOST_REQUIRE_EQUAL(controller.space_pressure(), 0.0);
}
