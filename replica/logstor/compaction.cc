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

free_segment_watermarks make_free_segment_watermarks(uint64_t segment_count, double target_fraction) noexcept {
    // target_fraction is a live-updatable config value with no range check at the config layer, so
    // it may arrive negative, NaN or above 1.0; treat anything outside (0, 1] as disabling the trigger.
    if (!std::isfinite(target_fraction) || target_fraction <= 0) {
        return {0, 0};
    }

    const auto fraction = std::min(target_fraction, 1.0);
    const auto by_fraction = static_cast<uint64_t>(std::ceil(segment_count * fraction));
    // The target has to cover what compaction holds while it works: a job takes up to a batch of
    // segments before it gives any back (see make_compaction_limits()), and the segment pool holds
    // back segments that normal writes cannot take, so a target of less than two batches leaves the
    // write path nothing to make progress with. The floor is itself capped, so that it cannot claim
    // an unreasonable share of a small disk.
    const auto min_target = std::min<uint64_t>(2 * min_segments_per_compaction, std::max<uint64_t>(1, segment_count / 8));
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

compaction_limits make_compaction_limits(free_segment_watermarks watermarks, size_t max_batch_cap) noexcept {
    // With the trigger disabled there is no target to protect, and automatic compaction never runs;
    // the limits still have to be sane for compaction submitted explicitly, so fall back to the
    // smallest batch rather than to zero.
    const auto budget = std::max<uint64_t>(watermarks.low, min_segments_per_compaction);
    // Spend the budget on concurrency first, up to the number of jobs the buffer pool can carry,
    // then give what is left to the batch. Both directions of the clamp matter: a batch below
    // min_segments_per_compaction cannot reclaim, and one above max_batch_cap makes a single job
    // hold too much of the target.
    const auto parallelism = std::clamp<uint64_t>(budget / min_segments_per_compaction, 1, max_auto_compaction_parallelism);
    const auto batch_cap = std::clamp<uint64_t>(budget / parallelism, min_segments_per_compaction, std::max<uint64_t>(max_batch_cap, min_segments_per_compaction));

    return {
        .auto_parallelism = static_cast<size_t>(parallelism),
        .batch_cap = static_cast<size_t>(batch_cap)
    };
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

// Decides how many segments to compact, where `prefix_scores[i]` scores the batch made of the first
// i+1 candidate segments in ascending utilization order. Returns 0 if no prefix reclaims a segment.
// The most efficient prefix is extended to the longest prefix that stays within
// `extension_tolerance` of it, trading a little efficiency for fewer and larger jobs.
static size_t select_compaction_prefix(std::span<const compaction_candidate_score> prefix_scores, double extension_tolerance) noexcept {
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

std::optional<compaction_batch> select_compaction_batch(const segment_set& segments, uint64_t segment_size, size_t batch_cap) {
    std::vector<const segment_descriptor*> candidates;
    // prefix_scores[i] scores the batch made of candidates[0..i].
    std::vector<compaction_candidate_score> prefix_scores;
    candidates.reserve(batch_cap);
    prefix_scores.reserve(batch_cap);

    uint64_t accum_net_data_size = 0;
    uint64_t accum_record_count = 0;
    for (const auto& desc : segments._segments) {
        if (candidates.size() >= batch_cap) {
            break;
        }
        candidates.push_back(&desc);

        accum_net_data_size += desc.net_data_size(segment_size);
        accum_record_count += desc.record_count;

        const auto score = compaction_candidate_score{
            .n_in = candidates.size(),
            .n_out = raw_write_buffer::estimate_required_segments(accum_net_data_size, accum_record_count, segment_size, segment_kind::full),
            .live_bytes = accum_net_data_size,
        };
        prefix_scores.push_back(score);
    }

    const auto selected_count = select_compaction_prefix(prefix_scores, compaction_batch_extension_tolerance);
    if (selected_count == 0) {
        return std::nullopt;
    }

    candidates.resize(selected_count);

    return compaction_batch{
        .segments = std::move(candidates),
        .score = prefix_scores[selected_count - 1],
    };
}

float compaction_shares_pressure(uint64_t available_segments, free_segment_watermarks watermarks) noexcept {
    // Also covers the trigger being disabled, where both watermarks are zero: with no free-segment
    // target there is no space-driven demand for shares.
    if (available_segments >= watermarks.high) {
        return 0.0f;
    }
    // Reaching the maximum shares while half the target is still in hand keeps the equilibrium of a
    // saturated controller away from the point where normal writes stall.
    const auto saturation_point = watermarks.low / 2;
    if (available_segments <= saturation_point) {
        return 1.0f;
    }
    return float(watermarks.high - available_segments) / float(watermarks.high - saturation_point);
}

} // namespace replica::logstor
