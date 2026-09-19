/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include <algorithm>
#include <array>
#include <cstdint>
#include <seastar/core/metrics_types.hh>

namespace replica::logstor {

// The utilization of a segment is the record bytes of its live records divided by the segment
// size. The utilization is 0 when all the records of the segment are dead. When no record of the
// segment was overwritten or deleted, the utilization is a little less than 1, because the chunk
// headers and the padding of the records are not record bytes. Only a sealed segment has a
// utilization. A segment goes into a compaction group when it is sealed. After that, its live
// record bytes can only decrease.
//
// The histogram divides the utilization range into equal buckets. Bucket i holds the segments with
// a utilization in [i/N, (i+1)/N). Compaction frees space from the segments in the low buckets.
// Thus, the histogram shows how much space compaction can free, and how much data it must copy for
// it. Segments in the low buckets give back much space for a small copy. Segments in the high
// buckets hold data that is in use.
constexpr size_t utilization_bucket_count = 16;

using utilization_histogram = std::array<uint64_t, utilization_bucket_count>;

inline size_t utilization_bucket_of(uint64_t live_record_bytes, uint64_t segment_size) noexcept {
    return std::min<size_t>(live_record_bytes * utilization_bucket_count / segment_size, utilization_bucket_count - 1);
}

// The statistics of a set of sealed segments: the number of segments, the record bytes of the live
// records in them, and the distribution of the segments by utilization. Each field is a sum. Thus,
// the statistics of a shard are the sum of the statistics of the sets of its groups.
struct segment_stats {
    uint64_t segment_count{0};
    uint64_t live_record_bytes{0};
    utilization_histogram utilization{};

    void add_segment(uint64_t segment_record_bytes, size_t bucket) noexcept {
        ++segment_count;
        live_record_bytes += segment_record_bytes;
        ++utilization[bucket];
    }

    void remove_segment(uint64_t segment_record_bytes, size_t bucket) noexcept {
        --segment_count;
        live_record_bytes -= segment_record_bytes;
        --utilization[bucket];
    }

    void free_from_segment(uint64_t freed_record_bytes, size_t old_bucket, size_t new_bucket) noexcept {
        live_record_bytes -= freed_record_bytes;
        if (old_bucket != new_bucket) {
            --utilization[old_bucket];
            ++utilization[new_bucket];
        }
    }

    bool operator==(const segment_stats&) const noexcept = default;
};

// Exports the utilization histogram as a Prometheus histogram. The buckets of a Prometheus
// histogram are cumulative. The label of a bucket is the upper limit of its utilization range. A
// segment with a utilization equal to a limit goes into the next bucket, see
// utilization_bucket_of().
inline seastar::metrics::histogram to_metrics_histogram(const segment_stats& stats, uint64_t segment_size) {
    seastar::metrics::histogram res;
    res.buckets.reserve(utilization_bucket_count);

    uint64_t cumulative_count = 0;
    for (size_t i = 0; i < utilization_bucket_count; ++i) {
        cumulative_count += stats.utilization[i];
        res.buckets.push_back(seastar::metrics::histogram_bucket{
            .count = cumulative_count,
            .upper_bound = double(i + 1) / utilization_bucket_count,
        });
    }

    res.sample_count = cumulative_count;
    // The sum of the utilizations of the segments. Thus, the mean utilization is the sum divided by
    // the count, as for all histograms.
    res.sample_sum = double(stats.live_record_bytes) / segment_size;

    return res;
}

} // namespace replica::logstor
