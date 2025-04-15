// Copyright (C) 2025-present ScyllaDB
// SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

#include <seastar/core/metrics.hh>

extern seastar::metrics::label level_label;
/*!
 * \brief we use label to allow granularity when reporting metrics
 */

/*!
 * \brief Basic level are metrics that are being used by Scylla Monitoring
 * To scrap only those metrics a user can use __level=basic
 */
extern seastar::metrics::label_instance basic_level;


/*!
 * \brief The following labels are used for specific features
 * It allows user disable a set of metrics that belong to the same
 * feature in an easy way.
 */
extern seastar::metrics::label_instance cdc_label;
extern seastar::metrics::label_instance cas_label;
extern seastar::metrics::label_instance lwt_label;
extern seastar::metrics::label_instance alternator_label;
