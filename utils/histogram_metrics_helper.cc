/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "histogram_metrics_helper.hh"

seastar::metrics::histogram to_metrics_summary(const utils::summary_calculator& summary) noexcept {
    seastar::metrics::histogram res;
    res.buckets.resize(summary.quantiles().size());
    res.sample_count = summary.histogram().count();
    for (size_t i = 0; i < summary.quantiles().size(); i++) {
        res.buckets[i].count = summary.summary()[i];
        res.buckets[i].upper_bound = summary.quantiles()[i];
    }
    return res;
}

seastar::metrics::histogram estimated_histogram_to_metrics(const utils::estimated_histogram& histogram) {
    seastar::metrics::histogram res;
    res.buckets.resize(histogram.bucket_offsets.size());
    uint64_t cumulative_count = 0;
    res.sample_count = histogram._count;
    res.sample_sum = histogram._sample_sum;
    for (size_t i = 0; i < res.buckets.size(); i++) {
        auto& v = res.buckets[i];
        v.upper_bound = histogram.bucket_offsets[i];
        cumulative_count += histogram.buckets[i];
        v.count = cumulative_count;
    }
    return res;
}
