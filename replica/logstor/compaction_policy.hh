/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include "utils/largest_k.hh"
#include <algorithm>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <optional>
#include <vector>

namespace replica::logstor {

struct segment_descriptor;
class segment_set;

// The number of compaction jobs that may run concurrently on a shard, counted in output buffers:
// a normal compaction takes one, a split compaction takes two. It sizes the compaction buffer pool,
// which is what enforces it, and the segment pool's compaction reserve, which is what keeps an
// output segment available for every concurrent flush.
constexpr size_t max_compaction_parallelism = 8;

// The buffers a split compaction takes, and therefore the slots automatic compaction leaves free so
// that a tablet split is never queued behind it.
constexpr size_t split_compaction_buffers = 2;

// The largest number of automatic compactions that can be in flight at once. The dynamic limit is
// derived per disk by make_compaction_limits(); this is the bound the semaphore is sized for.
constexpr size_t max_auto_compaction_parallelism = std::min<size_t>(4, max_compaction_parallelism - split_compaction_buffers);

// The smallest batch worth compacting. A batch reclaims only when its mean utilization is below
// `1 - 1/n_in`, so a smaller batch could not reclaim anything from a disk that is even moderately
// packed. It also anchors the free-segment target's absolute floor.
constexpr size_t min_segments_per_compaction = 8;

// The fraction of the best reclamation efficiency that a longer batch must keep to be selected
// instead. On its own the efficiency score picks short batches, which is per-job overhead for no
// write-amplification benefit; extending within this tolerance trades about 1% of efficiency for
// half as many jobs.
constexpr double compaction_batch_extension_tolerance = 0.8;

// Watermarks, in available segments, that drive automatic compaction. It starts once the number of
// available segments drops below `low` - the free-segment target - and stops once it is back at
// `high`. Both are zero when the trigger is disabled.
struct free_segment_watermarks {
    uint64_t low;
    uint64_t high;
};

// The free-segment target is a fraction of the disk, which is the dominant write-amplification
// knob, with an absolute floor that keeps the target meaningful on small disks, where a fraction of
// the disk rounds down to a segment or two.  `target_fraction` is
// logstor_compaction_trigger_threshold; 0 disables the trigger.
free_segment_watermarks make_free_segment_watermarks(uint64_t segment_count, double target_fraction) noexcept;

// Whether the free-segment level wants automatic compaction running, given whether it is running
// now. The two watermarks are a hysteresis band: compaction starts once the free-segment target is
// breached and runs until the disk is back at the stop watermark, rather than stopping again at the
// first write that crosses back over the target. Both watermarks are zero when the trigger is
// disabled, which answers "no" at any number of available segments.
//
// This is only the space half of the decision; whether compaction may run at all is the caller's.
bool auto_compaction_wanted(bool running, uint64_t available_segments, free_segment_watermarks watermarks) noexcept;

// How much automatic compaction may take on at once, derived from the free-segment target.
// The defaults are the most conservative limits make_compaction_limits() can return, so that a
// value that has not been refreshed yet still admits compaction rather than blocking it.
struct compaction_limits {
    // Automatic compaction jobs kept in flight.
    size_t auto_parallelism = 1;
    // Input segments one job may take.
    size_t batch_cap = min_segments_per_compaction;
};

// A compaction job allocates its output segments as it goes but frees its inputs only when it is
// done, so every job in flight can hold up to `batch_cap` segments of the free-segment target at
// once. Deriving both limits from `low` is what keeps `auto_parallelism * batch_cap <= low`: without
// it, concurrent jobs can consume the whole target and then wait for a segment that only one of them
// could free. `max_batch_cap` is segment_manager_config::max_segments_per_compaction, the upper bound
// the derived cap is clamped to.
//
// The invariant cannot hold on a disk so small that the target is below one batch; there the floor
// of make_free_segment_watermarks() has already been capped by the disk size and a batch of
// `min_segments_per_compaction` is the least that can reclaim anything.
compaction_limits make_compaction_limits(free_segment_watermarks watermarks, size_t max_batch_cap) noexcept;

// The cost of one candidate compaction batch: `n_in` segments are read and rewritten into `n_out`
// segments, copying `live_bytes` of live data.
struct compaction_candidate_score {
    size_t n_in;
    size_t n_out;
    uint64_t live_bytes;

    size_t reclaimed() const noexcept {
        return n_in > n_out ? n_in - n_out : 0;
    }

    // Segments reclaimed per segment-worth of data copied, which is exactly the reciprocal of the
    // batch's marginal write amplification. Comparisons cancel the segment_size factor, so it is
    // needed only where the absolute value is wanted, i.e. for logging.
    double efficiency(uint64_t segment_size) const noexcept {
        return live_bytes == 0
                ? std::numeric_limits<double>::infinity()
                : double(reclaimed()) * double(segment_size) / double(live_bytes);
    }

    // Whether this batch's efficiency is at least `fraction` of `other`'s.
    bool efficiency_at_least(const compaction_candidate_score& other, double fraction) const noexcept {
        if (live_bytes == 0) {
            return true;
        }
        if (other.live_bytes == 0) {
            return false;
        }
        return double(reclaimed()) * double(other.live_bytes)
                >= fraction * double(other.reclaimed()) * double(live_bytes);
    }

    bool operator==(const compaction_candidate_score& other) const noexcept = default;

    // Orders batches by reclamation efficiency, so that the score is the reciprocal of the write
    // amplification the batch would incur, and ties by how much the batch reclaims.
    bool operator<(const compaction_candidate_score& other) const noexcept;
};

// The batch select_compaction_batch() chose: the segments to compact, least utilized first, and the
// score of the batch as a whole - which is also the score its group is ranked by, so that the batch
// that wins the ranking is the one that would run.
struct compaction_batch {
    std::vector<const segment_descriptor*> segments;
    compaction_candidate_score score;
};

// What bounds the batch select_compaction_batch() picks.
struct compaction_batch_limits {
    // Input segments one job may take.
    size_t batch_cap;
    // The fraction of the best efficiency that a longer batch must keep, see
    // compaction_batch_extension_tolerance.
    double extension_tolerance = compaction_batch_extension_tolerance;
};

// Picks the segments one compaction job should read, out of the ones a group owns.
//
// The set's free-space histogram is already ordered by ascending utilization, which is near-optimal
// for victim ordering, so the only decision left is where to stop: the candidate set is the
// histogram's first `batch_cap` segments, and select_compaction_prefix() picks a prefix of it.
// Returns nothing when no prefix reclaims a segment, which answers for the whole group rather than
// only for this batch, since a longer prefix would only add fuller segments.
std::optional<compaction_batch> select_compaction_batch(const segment_set& segments,
        uint64_t segment_size, const compaction_batch_limits& limits);

// Gives the score of a compaction candidate, which top_compaction_candidates ranks it by.
struct compaction_score_of {
    template <typename Candidate>
    requires std::same_as<decltype(Candidate::score), compaction_candidate_score>
    const compaction_candidate_score& operator()(const Candidate& candidate) const noexcept {
        return candidate.score;
    }
};

// Keeps the best `capacity` compaction candidates by compaction_candidate_score. `Candidate` is any
// type that has a `score` member of type compaction_candidate_score. take_sorted() returns the
// candidates in worst-to-best order, so that a caller that takes them from the back gets the best
// one first.
template <typename Candidate>
using top_compaction_candidates = utils::largest_k<Candidate, std::less<>, compaction_score_of>;

// Where the free-segment target falls on the compaction_shares_pressure() ramp. It is a property of
// the watermarks rather than a tunable: with the relative hysteresis of make_free_segment_watermarks()
// the target sits a third of the way up the ramp on any disk size, up to the rounding of the two
// watermarks to whole segments. Exported so that the shares controller can put a control point on it.
constexpr float compaction_shares_pressure_at_target = 1.0f / 3.0f;

// Space pressure driving the compaction shares controller: 0 at or above the high watermark, where
// automatic compaction stops and there is no space demand at all, compaction_shares_pressure_at_target
// at the free-segment target - the intended steady-state operating point - and 1 once half the
// target has been consumed, linear in between.
//
// The free-segment count is the integral of the mismatch between the write rate and the rate at
// which compaction reclaims, so mapping it proportionally to shares is integral control on that
// mismatch: the loop settles at whatever shares match the write rate, and the slope only decides
// where in the free-segment range it settles. That is why pressure saturates while half the target
// is still in hand instead of at the point where writes stall: a workload that needs the maximum
// shares must be able to settle somewhere that still leaves the write path room.
float compaction_shares_pressure(uint64_t available_segments, free_segment_watermarks watermarks) noexcept;

} // namespace replica::logstor
