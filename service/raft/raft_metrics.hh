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
#include "raft/server.hh"

namespace service {

// How a set of raft counters or gauges is exported as metrics.
struct raft_metrics_options {
    // Metrics are named <group_name>_<metric_name>.
    seastar::sstring group_name;
    std::vector<seastar::metrics::label_instance> labels;
    // Labels to sum the metrics over when scraped, e.g. the shard label.
    // Empty reports every series as is.
    std::vector<seastar::metrics::label> aggregate_labels;
    // Counters that are still zero are not reported; gauges always are.
    bool skip_when_empty = false;
};

// Identifies the server a series belongs to where one server owns it.
extern const seastar::metrics::label raft_server_id_label;

// The counters a server accumulates. They must outlive the metric group.
void register_raft_server_stats_metrics(seastar::metrics::metric_groups& metrics,
        const raft::server_stats& stats, const raft_metrics_options& options);

// The state a server reports on demand. It must outlive the metric group.
void register_raft_server_metrics(seastar::metrics::metric_groups& metrics,
        const raft::server& server, const raft_metrics_options& options);

} // namespace service
