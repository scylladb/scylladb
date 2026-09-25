/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "service/strong_consistency/table_metrics.hh"

#include <seastar/core/metrics.hh>
#include "service/raft/raft_metrics.hh"

namespace service::strong_consistency {

namespace sm = seastar::metrics;

static const sm::label ks_label("ks");
static const sm::label cf_label("cf");
// Marks the node-aggregated per-table series, as replica/table.cc does.
static const sm::label_instance node_table_label("__per_table", "node");

// The number of log entries between two positions, zero if they crossed.
static uint64_t entries_between(raft::index_t from, raft::index_t to) {
    return to > from ? (to - from).value() : 0;
}

table_metrics::sample table_metrics::sweep() const {
    sample result;
    for (const auto* server : _servers) {
        const auto status = server->get_status();
        result.leaders += status.is_leader;
        result.in_memory_log_size += status.in_memory_log_size;
        result.log_memory_usage += status.log_memory_usage;
        result.uncommitted_entries += entries_between(status.commit_idx, status.last_idx);
        result.unapplied_entries += entries_between(status.applied_idx, status.commit_idx);
        result.log_limiter_waiters += status.log_limiter_waiters;
        result.blocked.probe += status.blocked.probe;
        result.blocked.pipeline_full += status.blocked.pipeline_full;
        result.blocked.snapshot += status.blocked.snapshot;
    }
    return result;
}

table_metrics::table_metrics(table_id table, const sstring& ks_name, const sstring& cf_name, reporting mode)
        : _table(table) {
    if (mode == reporting::none) {
        return;
    }
    raft_metrics_options options {
        .group_name = "strong_consistency_raft",
        .labels = {ks_label(ks_name), cf_label(cf_name)},
        .skip_when_empty = true,
    };
    if (mode == reporting::per_node) {
        options.labels.push_back(node_table_label);
        options.aggregate_labels.push_back(sm::shard_label);
    }
    register_raft_server_stats_metrics(_metrics, *_server_stats, options);
    register_raft_rpc_stats_metrics(_metrics, *_rpc_stats, options);
    register_table_gauges(options);
}

// The gauges of the table itself, summed over its servers by _sample.get().
void table_metrics::register_table_gauges(const raft_metrics_options& options) {
    const auto& labels = options.labels;
    const auto& aggregate = options.aggregate_labels;
    auto with_reason = [&labels] (const char* reason) {
        auto result = labels;
        result.push_back(raft_blocked_reason_label(reason));
        return result;
    };
    _metrics.add_group("strong_consistency_raft", {
        sm::make_gauge("leaders", [this] { return _sample.get().leaders; },
            sm::description("Number of tablet raft groups of the table led by this node"), labels).aggregate(aggregate),
        sm::make_gauge("in_memory_log_size", [this] { return _sample.get().in_memory_log_size; },
            sm::description("Number of entries in the in-memory part of the raft logs of the table"), labels).aggregate(aggregate),
        sm::make_gauge("log_memory_usage", [this] { return _sample.get().log_memory_usage; },
            sm::description("Bytes used by the in-memory part of the raft logs of the table"), labels).aggregate(aggregate),
        sm::make_gauge("uncommitted_entries", [this] { return _sample.get().uncommitted_entries; },
            sm::description("Number of raft log entries of the table not committed yet"), labels).aggregate(aggregate),
        sm::make_gauge("unapplied_entries", [this] { return _sample.get().unapplied_entries; },
            sm::description("Number of committed raft log entries of the table not applied yet"), labels).aggregate(aggregate),
        sm::make_gauge("log_limiter_waiters", [this] { return _sample.get().log_limiter_waiters; },
            sm::description("Number of entries currently waiting for an in-memory raft log of the table to shrink below max_log_size"), labels).aggregate(aggregate),
        sm::make_gauge("blocked_followers", [this] { return _sample.get().blocked.probe; },
            sm::description("Number of followers the tablet raft group leaders cannot send entries to, the reason label can be probe (waiting for the reply to a probe of the follower's log), pipeline_full (the maximal number of append requests is in flight) or snapshot (waiting for a snapshot transfer); followers the failure detector reports down are not counted"), with_reason("probe")).aggregate(aggregate),
        sm::make_gauge("blocked_followers", [this] { return _sample.get().blocked.pipeline_full; },
            sm::description("Number of followers the tablet raft group leaders cannot send entries to, the reason label can be probe (waiting for the reply to a probe of the follower's log), pipeline_full (the maximal number of append requests is in flight) or snapshot (waiting for a snapshot transfer); followers the failure detector reports down are not counted"), with_reason("pipeline_full")).aggregate(aggregate),
        sm::make_gauge("blocked_followers", [this] { return _sample.get().blocked.snapshot; },
            sm::description("Number of followers the tablet raft group leaders cannot send entries to, the reason label can be probe (waiting for the reply to a probe of the follower's log), pipeline_full (the maximal number of append requests is in flight) or snapshot (waiting for a snapshot transfer); followers the failure detector reports down are not counted"), with_reason("snapshot")).aggregate(aggregate),
    });
}

}
