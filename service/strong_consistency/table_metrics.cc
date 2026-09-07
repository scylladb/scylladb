/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "service/strong_consistency/table_metrics.hh"

#include <seastar/core/metrics.hh>
#include "raft/metrics_options.hh"

namespace service::strong_consistency {

namespace sm = seastar::metrics;

static const sm::label ks_label("ks");
static const sm::label cf_label("cf");
static const sm::label reason_label("reason");

// The number of log entries between two positions, zero if they crossed.
static uint64_t entries_between(raft::index_t from, raft::index_t to) {
    return to > from ? (to - from).value() : 0;
}

// Sweeps the servers at most once per lowres clock tick, which is coarse enough
// that a scrape evaluates all the gauges below against a single sweep.
const table_metrics::sample& table_metrics::take_sample() {
    const auto now = seastar::lowres_clock::now();
    if (now == _sampled_at) {
        return _sample;
    }
    _sample = {};
    for (auto* server : _servers) {
        const auto log = server->get_log_state();
        const auto blocked = server->get_blocked_followers();
        _sample.leaders += server->is_leader();
        _sample.in_memory_log_size += log.in_memory_log_size;
        _sample.log_memory_usage += log.log_memory_usage;
        _sample.uncommitted_entries += entries_between(log.commit_idx, log.last_idx);
        _sample.unapplied_entries += entries_between(log.applied_idx, log.commit_idx);
        _sample.blocked.probe += blocked.probe;
        _sample.blocked.pipeline_full += blocked.pipeline_full;
        _sample.blocked.snapshot += blocked.snapshot;
    }
    _sampled_at = now;
    return _sample;
}

table_metrics::table_metrics(table_id table, sstring ks_name, sstring cf_name, bool per_shard)
        : _table(table)
        , _ks_name(std::move(ks_name))
        , _cf_name(std::move(cf_name)) {
    const raft::metrics_options options {
        .group_name = "strong_consistency_raft",
        .labels = {ks_label(_ks_name), cf_label(_cf_name)},
        .aggregate_labels = per_shard ? std::vector<sm::label>{} : std::vector<sm::label>{sm::shard_label},
        .skip_when_empty = true,
    };
    raft::server::register_stats_metrics(_metrics, *_server_stats, options);
    raft_rpc::register_stats_metrics(_metrics, *_rpc_stats, options);

    const auto& labels = options.labels;
    const auto& aggregate = options.aggregate_labels;
    auto with_reason = [&labels] (const char* reason) {
        auto result = labels;
        result.push_back(reason_label(reason));
        return result;
    };
    _metrics.add_group("strong_consistency_raft", {
        sm::make_gauge("leaders", [this] { return take_sample().leaders; },
            sm::description("Number of tablet raft groups of the table led by this node"), labels).aggregate(aggregate),
        sm::make_gauge("in_memory_log_size", [this] { return take_sample().in_memory_log_size; },
            sm::description("Number of entries in the in-memory part of the raft logs of the table"), labels).aggregate(aggregate),
        sm::make_gauge("log_memory_usage", [this] { return take_sample().log_memory_usage; },
            sm::description("Bytes used by the in-memory part of the raft logs of the table"), labels).aggregate(aggregate),
        sm::make_gauge("uncommitted_entries", [this] { return take_sample().uncommitted_entries; },
            sm::description("Number of raft log entries of the table not committed yet"), labels).aggregate(aggregate),
        sm::make_gauge("unapplied_entries", [this] { return take_sample().unapplied_entries; },
            sm::description("Number of committed raft log entries of the table not applied yet"), labels).aggregate(aggregate),
        sm::make_gauge("blocked_followers", [this] { return take_sample().blocked.probe; },
            sm::description("Number of followers the tablet raft group leaders cannot send entries to, the reason label can be probe (waiting for the reply to a probe of the follower's log), pipeline_full (the maximal number of append requests is in flight) or snapshot (waiting for a snapshot transfer)"), with_reason("probe")).aggregate(aggregate),
        sm::make_gauge("blocked_followers", [this] { return take_sample().blocked.pipeline_full; },
            sm::description("Number of followers the tablet raft group leaders cannot send entries to, the reason label can be probe (waiting for the reply to a probe of the follower's log), pipeline_full (the maximal number of append requests is in flight) or snapshot (waiting for a snapshot transfer)"), with_reason("pipeline_full")).aggregate(aggregate),
        sm::make_gauge("blocked_followers", [this] { return take_sample().blocked.snapshot; },
            sm::description("Number of followers the tablet raft group leaders cannot send entries to, the reason label can be probe (waiting for the reply to a probe of the follower's log), pipeline_full (the maximal number of append requests is in flight) or snapshot (waiting for a snapshot transfer)"), with_reason("snapshot")).aggregate(aggregate),
    });
}

}
