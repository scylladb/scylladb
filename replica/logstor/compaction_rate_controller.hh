/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include "replica/logstor/compaction_policy.hh"
#include "replica/logstor/segment_stats.hh"
#include <chrono>
#include <cstddef>
#include <cstdint>

namespace replica::logstor {

// The segments under the utilization threshold, and the net number of segments that compacting
// all of them would free, estimated from a utilization histogram. When the threshold cuts a bucket,
// the estimate takes the segments of the bucket as spread evenly over its range. So if the threshold
// cuts a bucket at 30% of its range, 30% of its segments count. The lowest bucket always counts in
// full, so that a threshold of 0 still finds the dead segments. The driver watches this estimate
// for new candidates, but the selection reads the segments.
struct compaction_backlog {
    double candidate_segments{0};
    double reclaimable_segments{0};
};
compaction_backlog reclaimable_backlog(const utilization_histogram& utilization, double threshold) noexcept;

// How often the rate controller samples the shard.
constexpr std::chrono::milliseconds compaction_rate_control_period{250};

// Decides what automatic compaction takes and how much CPU it asks for.
//
// The utilization threshold curve decides which segments to compact. The CPU shares decide how fast
// compaction runs, and nothing else throttles it. The shares demand is the space pressure: 0 at or
// above the anchor of the curve, so compaction runs at the minimum shares, and 1 at the space floor.
// When compaction does not keep up with the writes at the minimum shares, the free level decreases
// below the anchor, and the shares increase until compaction keeps up.
class compaction_rate_controller {
public:
    // One sampling period.
    struct sample {
        uint64_t available_segments;
        // The threshold as a function of the free level, from the configuration. A threshold
        // outside [0, 1) disables automatic compaction and the controller.
        utilization_threshold_curve curve;
        // The segments of the groups of the shard, by utilization, for the backlog.
        const utilization_histogram& utilization;
    };

    void tick(const sample&) noexcept;

    // Whether the configured threshold enables automatic compaction at all.
    bool enabled() const noexcept { return _enabled; }
    const compaction_backlog& backlog() const noexcept { return _backlog; }
    double space_pressure() const noexcept { return _pressure; }
    // The threshold selection applies now: the curve at the current free level.
    double utilization_threshold() const noexcept { return _threshold; }
    // The fewest candidates a job is run on now, see effective_min_batch().
    size_t min_batch() const noexcept { return effective_min_batch(_pressure); }

private:
    bool _enabled = false;
    compaction_backlog _backlog;
    double _pressure = 0;
    double _threshold = 1;
};

} // namespace replica::logstor
