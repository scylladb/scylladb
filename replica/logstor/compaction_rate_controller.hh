/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include "replica/logstor/segment_stats.hh"
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

} // namespace replica::logstor
