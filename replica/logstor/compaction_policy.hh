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
// packed. It also anchors the minimum of the space floor.
constexpr size_t min_segments_per_compaction = 8;

// The fraction of the best reclamation efficiency that a longer batch must keep to be selected
// instead. On its own the efficiency score picks short batches, which is per-job overhead for no
// write-amplification benefit; extending within this tolerance trades about 1% of efficiency for
// half as many jobs.
constexpr double compaction_batch_extension_tolerance = 0.8;

// The fraction of the efficiency of the best batch that a batch must keep to start while another
// compaction job runs. The first job always starts, so the gate cannot stop compaction. The gate
// limits the write amplification of parallel jobs when the groups of a shard hold different amounts
// of garbage, see compaction_driver::run(). When the groups are similar, it refuses nothing.
//
// The bar is relative, because an absolute bar of one segment reclaimed per segment copied refuses
// every batch on a disk that is more than half full.
constexpr double compaction_marginal_admission_ratio = 0.75;

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

// The space floor: the free segments that compaction keeps for itself. The jobs in flight can hold
// this many segments, and at this free level the space pressure is 1, see
// compaction_space_pressure(). It is 1/32 of the disk, with a minimum of two batches for a small
// disk. It has no upper limit, because on a large disk it is the room that the write path has for a
// burst while compaction catches up.
uint64_t compaction_space_floor(uint64_t segment_count) noexcept;

// How much automatic compaction can take at the same time, derived from the space floor. The
// defaults are the smallest limits that make_compaction_limits() returns, so that limits that are
// not computed yet still let compaction run.
struct compaction_limits {
    // Automatic compaction jobs kept in flight.
    size_t auto_parallelism = 1;
    // Input segments one job may take.
    size_t batch_cap = min_segments_per_compaction;
};

// A job allocates its output segments while it runs and frees its inputs only when it ends, so each
// job in flight can hold up to `batch_cap` segments. Both limits come from the space floor, so that
// `auto_parallelism * batch_cap <= space_floor`. Otherwise the jobs can use all of the space floor
// and then wait for a segment that only one of them can free. `max_batch_cap` is
// segment_manager_config::max_segments_per_compaction, the upper limit of the batch cap.
//
// When the space floor is less than one batch the relation cannot hold, and the limits are one job
// of `min_segments_per_compaction` segments, the smallest batch that can reclaim.
compaction_limits make_compaction_limits(uint64_t space_floor, size_t max_batch_cap) noexcept;

// The utilization threshold as a function of the free level, in available segments: a
// piecewise-linear curve through three points.
//
//     available <= space floor          -> 1         any batch with a net gain
//     space floor < available < anchor  -> 1 .. u    space pressure increases the threshold
//     available == anchor               -> u         the configured threshold
//     anchor < available < ceiling      -> u .. m    more free space decreases it
//     available >= ceiling              -> m         the minimum threshold
//
// The slope above the anchor sets the policy. With `m = u` the threshold is constant, and the free
// level changes with the data on the disk. With the ceiling one segment above the anchor, the free
// level stays near the anchor. The part below the anchor lets a disk that is too full for `u` run:
// when the disk holds so much live data that too few segments are under `u`, compaction cannot free
// enough segments and the free level falls. Then the threshold increases, and compaction takes
// fuller segments. The minimum threshold `m` makes sure that a segment with a few long-lived records
// is compacted at any free level. This limits the space that such segments hold, at a write
// amplification of about `m / (1 - m)`.
struct utilization_threshold_curve {
    uint64_t space_floor;
    uint64_t anchor;
    // Above the anchor: the free level where the threshold reaches `min_threshold`.
    uint64_t ceiling;
    // The configured threshold, logstor_compaction_utilization_threshold. Anything outside [0, 1)
    // disables automatic compaction.
    double threshold;
    // The threshold at the ceiling and above, logstor_compaction_min_utilization_threshold, in
    // [0, threshold].
    double min_threshold;

    bool operator==(const utilization_threshold_curve&) const noexcept = default;
};

// The curve for a disk of `segment_count` segments and the space floor `space_floor`, from the
// configuration. `anchor_fraction` and `ceiling_fraction` are fractions of the disk. An anchor of 0
// is twice the space floor, and a ceiling of 0 is one segment above the anchor. The anchor is kept
// above the space floor and at most half of the disk, the ceiling above the anchor, and the minimum
// threshold in [0, threshold], whatever the configuration is.
utilization_threshold_curve make_utilization_threshold_curve(uint64_t segment_count, uint64_t space_floor,
        double threshold, double anchor_fraction, double ceiling_fraction, double min_threshold) noexcept;

// How short of segments the write path is, in [0, 1]: 0 at the anchor or above, 1 at the space
// floor or below, and linear between them.
double compaction_space_pressure(uint64_t available_segments, const utilization_threshold_curve&) noexcept;

// The fewest candidate segments that a job runs on: min_segments_per_compaction when there is no
// space pressure, and less as the pressure increases, down to 1 at the space floor. Under a
// threshold the candidates of a group arrive one at a time, and without a minimum a job had less
// than three segments on average. The decrease is gradual, because a disk that is too full for the
// threshold stays in the pressure band, and a step would make jobs of one segment normal there.
size_t effective_min_batch(double pressure) noexcept;

// The utilization threshold at this free level.
double effective_utilization_threshold(uint64_t available_segments, const utilization_threshold_curve&) noexcept;

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
    // The fewest candidate segments that a group must have for a batch, unless the batch is dead
    // segments only. A job has fixed costs: a scan, a barrier on the reads in flight, and an output
    // segment that is sealed part full. See effective_min_batch().
    size_t min_batch = 1;
    // Only a segment whose utilization is at most this is a candidate, see
    // effective_utilization_threshold(). 1 makes every segment a candidate.
    double max_utilization = 1;
    // The fraction of the best efficiency that a longer batch must keep, see
    // compaction_batch_extension_tolerance.
    double extension_tolerance = compaction_batch_extension_tolerance;
};

// Picks the segments one compaction job should read, out of the ones a group owns.
//
// The set's free-space histogram is already ordered by ascending utilization, which is near-optimal
// for victim ordering, so the only decision left is where to stop: the candidate set is the
// histogram's prefix of segments under the utilization threshold, at most `batch_cap` of them, and
// select_compaction_prefix() picks a prefix of that. With fewer candidates than `min_batch`, the
// batch is the dead candidates. Returns nothing when there is no such batch or no prefix reclaims a
// segment, which answers for the whole group rather than only for this batch, since a longer prefix
// would only add fuller segments.
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
