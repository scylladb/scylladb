/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once
#include <vector>
#include <seastar/core/metrics.hh>
#include <seastar/core/sstring.hh>

namespace raft {

// How a set of counters, e.g. server::stats, is exported as metrics. Kept
// out of server.hh so that its users do not pull in the metrics headers.
struct metrics_options {
    // Metrics are named <group_name>_<counter_name>.
    seastar::sstring group_name;
    std::vector<seastar::metrics::label_instance> labels;
    // Labels to sum the metrics over when scraped, e.g. the shard label.
    // Empty reports every series as is.
    std::vector<seastar::metrics::label> aggregate_labels;
    // Counters that are still zero are not reported; gauges always are.
    bool skip_when_empty = false;
};

} // namespace raft
