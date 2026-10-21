/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#include "replica/logstor/compaction_rate_controller.hh"
#include <algorithm>

namespace replica::logstor {

compaction_backlog reclaimable_backlog(const utilization_histogram& utilization, double threshold) noexcept {
    // A bucket whose upper bound is the threshold counts in full, in spite of rounding.
    constexpr double bound_tolerance = 1e-9;
    compaction_backlog backlog;
    // Bucket i holds the segments with utilization in (i/N, (i+1)/N], see utilization_bucket_of().
    const auto max_utilization = std::clamp(threshold, 0.0, 1.0);
    for (size_t i = 0; i < utilization_bucket_count; ++i) {
        const auto lower = double(i) / utilization_bucket_count;
        const auto upper = double(i + 1) / utilization_bucket_count;
        auto counted_upper = upper;
        if (i > 0 && upper > max_utilization + bound_tolerance) {
            if (lower >= max_utilization) {
                break;
            }
            counted_upper = max_utilization;
        }
        const auto fraction = (counted_upper - lower) / (upper - lower);
        backlog.candidate_segments += double(utilization[i]) * fraction;
    }
    return backlog;
}

void compaction_rate_controller::tick(const sample& s) noexcept {
    _pressure = compaction_space_pressure(s.available_segments, s.curve);
    _threshold = effective_utilization_threshold(s.available_segments, s.curve);
    _backlog = reclaimable_backlog(s.utilization, _threshold);
}

} // namespace replica::logstor
