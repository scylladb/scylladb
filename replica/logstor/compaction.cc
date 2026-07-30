/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#include "replica/logstor/compaction.hh"
#include <algorithm>
#include <cmath>

namespace replica::logstor {

free_segment_watermarks make_free_segment_watermarks(uint64_t segment_count, double target_fraction, size_t max_segments_per_compaction) noexcept {
    // target_fraction is a live-updatable config value with no range check at the config layer, so
    // it may arrive negative, NaN or above 1.0; treat anything outside (0, 1] as disabling the trigger.
    if (!std::isfinite(target_fraction) || target_fraction <= 0) {
        return {0, 0};
    }

    const auto fraction = std::min(target_fraction, 1.0);
    const auto by_fraction = static_cast<uint64_t>(std::ceil(segment_count * fraction));
    // A compaction job needs an open output segment, and the segment pool holds back
    // max_segments_per_compaction segments that normal writes cannot take, so a target below that
    // leaves the write path nothing to make progress with. The floor is itself capped, so that it
    // cannot claim an unreasonable share of a small disk.
    const auto min_target = std::min<uint64_t>(2 * max_segments_per_compaction, std::max<uint64_t>(1, segment_count / 8));
    const auto low = std::min(segment_count, std::max(by_fraction, min_target));
    // Relative hysteresis, so that the band does not grow out of proportion as the target shrinks.
    // Capped at segment_count so a high fraction can't push `high` out of reach of available_segments,
    // which would leave should_run_auto_compaction() unable to ever stop.
    const auto high = std::min(segment_count, low + std::max<uint64_t>(1, low / 4));
    return {low, high};
}

bool auto_compaction_wanted(bool running, uint64_t available_segments, free_segment_watermarks watermarks) noexcept {
    return available_segments < (running ? watermarks.high : watermarks.low);
}

} // namespace replica::logstor
