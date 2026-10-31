/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#include "replica/logstor/compaction_rate_controller.hh"
#include <algorithm>
#include <cmath>

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
        const auto counted = double(utilization[i]) * fraction;
        const auto mean_utilization = (lower + counted_upper) / 2;
        backlog.candidate_segments += counted;
        backlog.reclaimable_segments += counted * (1 - mean_utilization);
    }
    return backlog;
}

void compaction_rate_controller::tick(const sample& s) noexcept {
    // The threshold is a live-updatable config value with no range check at the config layer, so it
    // may arrive negative, NaN or 1 and above; anything outside [0, 1) disables automatic compaction.
    _enabled = std::isfinite(s.curve.threshold) && s.curve.threshold >= 0 && s.curve.threshold < 1;
    if (!_enabled) {
        _backlog = {};
        _pressure = 0;
        _threshold = 1;
        return;
    }

    _pressure = compaction_space_pressure(s.available_segments, s.curve);
    _threshold = effective_utilization_threshold(s.available_segments, s.curve);
    _backlog = reclaimable_backlog(s.utilization, _threshold);
}

} // namespace replica::logstor
