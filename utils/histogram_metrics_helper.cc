/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
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
