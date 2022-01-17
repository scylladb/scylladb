/*
 * Copyright (C) 2020-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <cstdint>
#include <seastar/core/metrics_types.hh>
#include "seastarx.hh"
#include "estimated_histogram.hh"


template<uint64_t Min, uint64_t Max, size_t Precision>
seastar::metrics::histogram to_metrics_histogram(const utils::approx_exponential_histogram<Min, Max, Precision>& hist) {
    seastar::metrics::histogram res;
    res.buckets.resize(hist.size() - 1);
    uint64_t cummulative_count = 0;
    res.sample_sum = 0;

    for (size_t i = 0; i < hist.NUM_BUCKETS - 1; i++) {
        auto& v = res.buckets[i];
        v.upper_bound = hist.get_bucket_lower_limit(i + 1);
        cummulative_count += hist.get(i);
        v.count = cummulative_count;
        res.sample_sum += hist.get(i) * v.upper_bound;
    }
    // The count serves as the infinite bucket
    res.sample_count = cummulative_count + hist.get(hist.NUM_BUCKETS - 1);
    res.sample_sum += hist.get(hist.NUM_BUCKETS - 1) * hist.get_bucket_lower_limit(hist.NUM_BUCKETS - 1);
    return res;
}
