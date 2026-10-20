/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#include "replica/logstor/compaction_policy.hh"
#include "replica/logstor/compaction.hh"
#include <algorithm>
#include <cmath>
#include <optional>
#include <span>

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

uint64_t compaction_space_floor(uint64_t segment_count) noexcept {
    // At least two batches, so that a job can hold one batch while the write path has the other.
    // The minimum has its own limit, so that the space floor is not most of a small disk.
    const auto minimum = std::min<uint64_t>(2 * min_segments_per_compaction, std::max<uint64_t>(1, segment_count / 8));
    return std::max<uint64_t>(segment_count / 32, minimum);
}

compaction_limits make_compaction_limits(uint64_t space_floor, size_t max_batch_cap) noexcept {
    // Below one batch the relation cannot hold. Use the smallest batch that can reclaim, and not
    // nothing.
    const auto budget = std::max<uint64_t>(space_floor, min_segments_per_compaction);
    // Spend the budget on concurrency first, up to the number of jobs the buffer pool can carry,
    // then give what is left to the batch. Both directions of the clamp matter: a batch below
    // min_segments_per_compaction cannot reclaim, and one above max_batch_cap makes a single job
    // hold too much of the space floor.
    const auto parallelism = std::clamp<uint64_t>(budget / min_segments_per_compaction, 1, max_auto_compaction_parallelism);
    const auto batch_cap = std::clamp<uint64_t>(budget / parallelism, min_segments_per_compaction, std::max<uint64_t>(max_batch_cap, min_segments_per_compaction));

    return {
        .auto_parallelism = static_cast<size_t>(parallelism),
        .batch_cap = static_cast<size_t>(batch_cap)
    };
}

utilization_threshold_curve make_utilization_threshold_curve(uint64_t segment_count, uint64_t space_floor,
        double threshold, double anchor_fraction, double ceiling_fraction, double min_threshold) noexcept {
    // A fraction of the disk, rounded to whole segments; anything that is not a usable fraction
    // means "not configured".
    auto segments_of = [segment_count] (double fraction) -> std::optional<uint64_t> {
        if (!std::isfinite(fraction) || fraction <= 0) {
            return std::nullopt;
        }
        return static_cast<uint64_t>(std::llround(std::min(fraction, 1.0) * double(segment_count)));
    };
    // The pressure band and the decrease of the threshold must have a width. A configuration
    // without one gets the smallest width, and not a division by zero.
    const auto anchor = std::max<uint64_t>(segments_of(anchor_fraction).value_or(2 * space_floor), space_floor + 1);
    const auto ceiling = std::max<uint64_t>(segments_of(ceiling_fraction).value_or(0), anchor + 1);
    const auto max_min_threshold = std::isfinite(threshold) ? std::clamp(threshold, 0.0, 1.0) : 0.0;
    return utilization_threshold_curve{
        .space_floor = space_floor,
        .anchor = anchor,
        .ceiling = ceiling,
        .threshold = threshold,
        .min_threshold = std::isfinite(min_threshold) ? std::clamp(min_threshold, 0.0, max_min_threshold) : 0.0,
    };
}

double compaction_space_pressure(uint64_t available_segments, const utilization_threshold_curve& curve) noexcept {
    if (curve.anchor <= curve.space_floor) {
        return available_segments <= curve.space_floor ? 1 : 0;
    }
    if (available_segments >= curve.anchor) {
        return 0;
    }
    if (available_segments <= curve.space_floor) {
        return 1;
    }
    return double(curve.anchor - available_segments) / double(curve.anchor - curve.space_floor);
}

double effective_utilization_threshold(uint64_t available_segments, const utilization_threshold_curve& curve) noexcept {
    if (available_segments < curve.anchor) {
        return curve.threshold + (1 - curve.threshold) * compaction_space_pressure(available_segments, curve);
    }
    if (available_segments >= curve.ceiling) {
        return curve.min_threshold;
    }
    const auto rise = double(available_segments - curve.anchor) / double(curve.ceiling - curve.anchor);
    return curve.min_threshold + (curve.threshold - curve.min_threshold) * (1 - rise);
}

size_t effective_min_batch(double pressure) noexcept {
    const auto scaled = std::llround(double(min_segments_per_compaction) * (1 - std::clamp(pressure, 0.0, 1.0)));
    return std::max<size_t>(1, static_cast<size_t>(scaled));
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

std::optional<compaction_batch> select_compaction_batch(const segment_set& segments, uint64_t segment_size,
        const compaction_batch_limits& limits) {
    std::vector<const segment_descriptor*> candidates;
    // prefix_scores[i] scores the batch made of candidates[0..i].
    std::vector<compaction_candidate_score> prefix_scores;
    candidates.reserve(limits.batch_cap);
    prefix_scores.reserve(limits.batch_cap);

    // A segment is a candidate when its utilization is at most the threshold, that is when it has
    // at least `min_free_space` free. The histogram is ordered only by bucket, so a segment over
    // the threshold stops the selection only after the buckets pass the threshold. In the bucket
    // that the threshold cuts, each segment is checked.
    const auto max_record_bytes = static_cast<uint64_t>(std::floor(std::clamp(limits.max_utilization, 0.0, 1.0) * double(segment_size)));
    auto min_free_space = segment_size - max_record_bytes;
    // The lowest bucket holds every segment with less than `min_size` free, in no order, so a
    // threshold inside it would check each nearly full segment of the group. Such a threshold
    // applies only near the space floor, where the curve is about to reach 1, so it applies as 1.
    if (min_free_space < segment_descriptor_hist_options.min_size) {
        min_free_space = 0;
    }
    const auto threshold_bucket = min_free_space > 0 ? segment_descriptor_hist_options.bucket_of(min_free_space) : 0;

    uint64_t accum_record_bytes = 0;
    uint64_t accum_record_count = 0;
    for (const auto& desc : segments.by_free_space()) {
        if (candidates.size() >= limits.batch_cap) {
            break;
        }
        if (min_free_space > 0 && desc.free_space < min_free_space) {
            if (segment_descriptor_hist_options.bucket_of(desc.free_space) < threshold_bucket) {
                break;
            }
            continue;
        }
        candidates.push_back(&desc);

        accum_record_bytes += desc.record_bytes(segment_size);
        accum_record_count += desc.record_count;

        const auto score = compaction_candidate_score{
            .n_in = candidates.size(),
            .n_out = raw_write_buffer::estimate_required_segments(accum_record_bytes, accum_record_count, segment_size, segment_kind::full),
            .live_bytes = accum_record_bytes,
        };
        prefix_scores.push_back(score);
    }

    // Too few candidates for a job. This is the answer for the whole group, because the group has
    // no other segment under the threshold. Dead segments do not wait for more candidates, because
    // a job reads and writes nothing for them.
    if (candidates.size() < limits.min_batch) {
        std::erase_if(candidates, [segment_size] (const segment_descriptor* desc) {
            return desc->record_bytes(segment_size) != 0;
        });
        if (candidates.empty()) {
            return std::nullopt;
        }
        const auto score = compaction_candidate_score{.n_in = candidates.size(), .n_out = 0, .live_bytes = 0};
        return compaction_batch{
            .segments = std::move(candidates),
            .score = score,
        };
    }

    const auto selected_count = select_compaction_prefix(prefix_scores, limits.extension_tolerance);
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
