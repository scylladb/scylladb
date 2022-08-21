/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/core/metrics.hh>

extern seastar::metrics::label level_label;
/*!
 * \brief we use label to allow granularity when reporting metrics
 */

/*!
 * \brief Basic level are metrics that are being used by Scylla Monitoring
 *
 */
extern seastar::metrics::label_instance basic_level;
/*!
 * \brief Advanced level are metrics that are not part of the dashboards
 *  but could be helpful at specific times for debugging.
 *
 */
extern seastar::metrics::label_instance advanced_level;
/*!
 * \brief specific-level is for metrics with high cardinality.
 * The primary example is per-table metrics.
 * Metrics with high cardinality will either be disabled by default completely or,
 * will be enabled/scraped for a specific value (that is why it is called specific).
 * For example only specific tables, or a specific namespace.
 *
 */
extern seastar::metrics::label_instance specific_level;
