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
#include "histogram.hh"

template<uint64_t Min, uint64_t Max, size_t Precision>
seastar::metrics::histogram to_metrics_histogram(const utils::approx_exponential_histogram<Min, Max, Precision>& hist) {
    return hist.to_metrics_histogram();
}

/*!
 * \brief get a metrics summary from timed_rate_moving_average_with_summary
 *
 * timed_rate_moving_average_with_summary contains a summary. This function
 * copy it to a metric summary.
 * A metric summary is a histogram where each bucket holds some quantile.
 */
seastar::metrics::histogram to_metrics_summary(const utils::summary_calculator& summary) noexcept;
