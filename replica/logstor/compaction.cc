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
    if (target_fraction == 0) {
        return {0, 0};
    }

    const auto fraction = std::min(target_fraction, 1.0);
    const auto by_fraction = static_cast<uint64_t>(std::ceil(segment_count * fraction));
    // A compaction job needs an open output segment, and the segment pool holds back
    // max_segments_per_compaction segments that normal writes cannot take, so a target below that
    // leaves the write path nothing to make progress with. The floor is itself capped, so that it
    // cannot claim an unreasonable share of a small disk.
    const auto min_target = std::min<uint64_t>(2 * max_segments_per_compaction, std::max<uint64_t>(1, segment_count / 8));
    const auto low = std::max(by_fraction, min_target);
    // Relative hysteresis, so that the band does not grow out of proportion as the target shrinks.
    return {low, low + std::max<uint64_t>(1, low / 4)};
}

bool compaction_candidate_score::operator<(const compaction_candidate_score& other) const noexcept {
    if (live_bytes == 0 || other.live_bytes == 0) {
        // A batch that copies nothing has infinite efficiency.
        if ((live_bytes == 0) != (other.live_bytes == 0)) {
            return live_bytes != 0;
        }
    } else {
        // efficiency() < other.efficiency(), with the common segment_size factor cancelled out.
        const auto lhs = reclaimed() * other.live_bytes;
        const auto rhs = other.reclaimed() * live_bytes;
        if (lhs != rhs) {
            return lhs < rhs;
        }
    }

    return reclaimed() < other.reclaimed();
}

size_t select_compaction_prefix(std::span<const compaction_candidate_score> prefix_scores, double extension_tolerance) noexcept {
    size_t best = 0;
    for (size_t i = 0; i < prefix_scores.size(); ++i) {
        if (prefix_scores[i].reclaimed() == 0) {
            continue;
        }
        // Strictly better, so that the shortest of equally efficient prefixes wins: extending is
        // only worth it when the tolerance below says so.
        if (best == 0 || prefix_scores[best - 1] < prefix_scores[i]) {
            best = i + 1;
        }
    }

    if (best == 0) {
        return 0;
    }

    // Efficiency along the prefix is a sawtooth - it jumps whenever one more segment is reclaimed
    // and decays as live bytes accumulate without reclaiming - so the longest prefix within the
    // tolerance is not necessarily the one just before the first prefix that falls outside it.
    for (size_t i = prefix_scores.size(); i > best; --i) {
        if (prefix_scores[i - 1].efficiency_at_least(prefix_scores[best - 1], extension_tolerance)) {
            return i;
        }
    }
    return best;
}

float compaction_admission_pressure(uint64_t available_segments, free_segment_watermarks watermarks) noexcept {
    if (available_segments >= watermarks.high) {
        return 0.0f;
    }
    if (available_segments <= watermarks.low) {
        return 1.0f;
    }
    return float(watermarks.high - available_segments) / float(watermarks.high - watermarks.low);
}

double compaction_max_used_fraction(float admission_pressure, size_t max_segments_per_compaction) noexcept {
    // With no space pressure, compact only segments that are nearly dead.
    static constexpr double no_pressure_bound = 0.25;
    // A compaction job writes whole output segments, so reclaiming anything requires
    // n_out < n_in, which means the batch's mean utilization must be below 1 - 1/n_in. Opening the
    // gate any further than that cannot admit a batch with a net gain.
    const auto reclaim_ceiling = 1.0 - 1.0 / double(std::max<size_t>(1, max_segments_per_compaction));
    if (reclaim_ceiling <= no_pressure_bound) {
        return reclaim_ceiling;
    }
    return no_pressure_bound + admission_pressure * (reclaim_ceiling - no_pressure_bound);
}

} // namespace replica::logstor
