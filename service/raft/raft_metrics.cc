/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "service/raft/raft_metrics.hh"

#include "utils/per_task_value.hh"

namespace service {

namespace sm = seastar::metrics;

const sm::label raft_server_id_label("id");
static const sm::label log_entry_type("log_entry_type");
static const sm::label message_type("message_type");

// @metrics options.group_name = ["raft"]
void register_raft_server_stats_metrics(sm::metric_groups& metrics,
        const raft::server_stats& s, const raft_metrics_options& options) {
    const auto& aggregate = options.aggregate_labels;
    const bool skip = options.skip_when_empty;
    auto labels = [&options] (std::initializer_list<sm::label_instance> extra = {}) {
        auto result = options.labels;
        result.insert(result.end(), extra.begin(), extra.end());
        return result;
    };
    metrics.add_group(options.group_name, {
        sm::make_total_operations("add_entries", s.add_command,
             sm::description("Number of entries added on this node, the log_entry_type label can be command, dummy or config"), labels({log_entry_type("command")})).aggregate(aggregate).set_skip_when_empty(skip),
        sm::make_total_operations("add_entries", s.add_dummy,
             sm::description("Number of entries added on this node, the log_entry_type label can be command, dummy or config"), labels({log_entry_type("dummy")})).aggregate(aggregate).set_skip_when_empty(skip),
        sm::make_total_operations("add_entries", s.add_config,
             sm::description("Number of entries added on this node, the log_entry_type label can be command, dummy or config"), labels({log_entry_type("config")})).aggregate(aggregate).set_skip_when_empty(skip),

        sm::make_total_operations("messages_received", s.append_entries_received,
             sm::description("Number of messages received, the message_type determines the type of message"), labels({message_type("append_entries")})).aggregate(aggregate).set_skip_when_empty(skip),
        sm::make_total_operations("messages_received", s.append_entries_reply_received,
             sm::description("Number of messages received, the message_type determines the type of message"), labels({message_type("append_entries_reply")})).aggregate(aggregate).set_skip_when_empty(skip),
        sm::make_total_operations("messages_received", s.request_vote_received,
             sm::description("Number of messages received, the message_type determines the type of message"), labels({message_type("request_vote")})).aggregate(aggregate).set_skip_when_empty(skip),
        sm::make_total_operations("messages_received", s.request_vote_reply_received,
             sm::description("Number of messages received, the message_type determines the type of message"), labels({message_type("request_vote_reply")})).aggregate(aggregate).set_skip_when_empty(skip),
        sm::make_total_operations("messages_received", s.timeout_now_received,
             sm::description("Number of messages received, the message_type determines the type of message"), labels({message_type("timeout_now")})).aggregate(aggregate).set_skip_when_empty(skip),
        sm::make_total_operations("messages_received", s.read_quorum_received,
             sm::description("Number of messages received, the message_type determines the type of message"), labels({message_type("read_quorum")})).aggregate(aggregate).set_skip_when_empty(skip),
        sm::make_total_operations("messages_received", s.read_quorum_reply_received,
             sm::description("Number of messages received, the message_type determines the type of message"), labels({message_type("read_quorum_reply")})).aggregate(aggregate).set_skip_when_empty(skip),

        sm::make_total_operations("messages_sent", s.append_entries_sent,
             sm::description("Number of messages sent, the message_type determines the type of message"), labels({message_type("append_entries")})).aggregate(aggregate).set_skip_when_empty(skip),
        sm::make_total_operations("messages_sent", s.append_entries_reply_sent,
             sm::description("Number of messages sent, the message_type determines the type of message"), labels({message_type("append_entries_reply")})).aggregate(aggregate).set_skip_when_empty(skip),
        sm::make_total_operations("messages_sent", s.vote_request_sent,
             sm::description("Number of messages sent, the message_type determines the type of message"), labels({message_type("request_vote")})).aggregate(aggregate).set_skip_when_empty(skip),
        sm::make_total_operations("messages_sent", s.vote_request_reply_sent,
             sm::description("Number of messages sent, the message_type determines the type of message"), labels({message_type("request_vote_reply")})).aggregate(aggregate).set_skip_when_empty(skip),
        sm::make_total_operations("messages_sent", s.install_snapshot_sent,
             sm::description("Number of messages sent, the message_type determines the type of message"), labels({message_type("install_snapshot")})).aggregate(aggregate).set_skip_when_empty(skip),
        sm::make_total_operations("messages_sent", s.snapshot_reply_sent,
             sm::description("Number of messages sent, the message_type determines the type of message"), labels({message_type("snapshot_reply")})).aggregate(aggregate).set_skip_when_empty(skip),
        sm::make_total_operations("messages_sent", s.timeout_now_sent,
             sm::description("Number of messages sent, the message_type determines the type of message"), labels({message_type("timeout_now")})).aggregate(aggregate).set_skip_when_empty(skip),
        sm::make_total_operations("messages_sent", s.read_quorum_sent,
             sm::description("Number of messages sent, the message_type determines the type of message"), labels({message_type("read_quorum")})).aggregate(aggregate).set_skip_when_empty(skip),
        sm::make_total_operations("messages_sent", s.read_quorum_reply_sent,
             sm::description("Number of messages sent, the message_type determines the type of message"), labels({message_type("read_quorum_reply")})).aggregate(aggregate).set_skip_when_empty(skip),

        sm::make_total_operations("waiter_awoken", s.waiters_awoken,
             sm::description("Number of waiters that got result back"), labels()).aggregate(aggregate).set_skip_when_empty(skip),
        sm::make_total_operations("waiter_dropped", s.waiters_dropped,
             sm::description("Number of waiters that did not get result back"), labels()).aggregate(aggregate).set_skip_when_empty(skip),
        sm::make_total_operations("polls", s.polls,
             sm::description("Number of times raft state machine polled"), labels()).aggregate(aggregate).set_skip_when_empty(skip),
        sm::make_total_operations("store_term_and_vote", s.store_term_and_vote,
             sm::description("Number of times term and vote persisted"), labels()).aggregate(aggregate).set_skip_when_empty(skip),
        sm::make_total_operations("store_snapshot", s.store_snapshot,
             sm::description("Number of snapshots persisted"), labels()).aggregate(aggregate).set_skip_when_empty(skip),
        sm::make_total_operations("sm_load_snapshot", s.sm_load_snapshot,
             sm::description("Number of times user state machine reloaded with a snapshot"), labels()).aggregate(aggregate).set_skip_when_empty(skip),
        sm::make_total_operations("truncate_persisted_log", s.truncate_persisted_log,
             sm::description("Number of times log truncated on storage"), labels()).aggregate(aggregate).set_skip_when_empty(skip),
        sm::make_total_operations("persisted_log_entries", s.persisted_log_entries,
             sm::description("Number of log entries persisted"), labels()).aggregate(aggregate).set_skip_when_empty(skip),
        sm::make_total_operations("queue_entries_for_apply", s.queue_entries_for_apply,
             sm::description("Number of log entries queued to be applied"), labels()).aggregate(aggregate).set_skip_when_empty(skip),
        sm::make_total_operations("applied_entries", s.applied_entries,
             sm::description("Number of log entries applied"), labels()).aggregate(aggregate).set_skip_when_empty(skip),
        sm::make_total_operations("snapshots_taken", s.snapshots_taken,
             sm::description("Number of times user's state machine snapshotted"), labels()).aggregate(aggregate).set_skip_when_empty(skip),
    });
}

// @metrics options.group_name = ["raft"]
void register_raft_server_metrics(sm::metric_groups& metrics,
        const raft::server& server, const raft_metrics_options& options) {
    const auto& aggregate = options.aggregate_labels;
    auto labels = [&options] (std::initializer_list<sm::label_instance> extra = {}) {
        auto result = options.labels;
        result.insert(result.end(), extra.begin(), extra.end());
        return result;
    };
    // One sweep feeds every gauge below, as table_metrics does for a whole table.
    auto sample = seastar::make_lw_shared<utils::per_task_value<std::function<raft::server_status()>>>(
            [&server] { return server.get_status(); });
    auto status = [sample] (auto&& field) {
        return [sample, field] { return field(sample->get()); };
    };
    metrics.add_group(options.group_name, {
        sm::make_gauge("in_memory_log_size", status([] (const auto& s) { return s.in_memory_log_size; }),
             sm::description("size of in-memory part of the log"), labels()).aggregate(aggregate),
        sm::make_gauge("log_memory_usage", status([] (const auto& s) { return s.log_memory_usage; }),
             sm::description("memory usage of in-memory part of the log in bytes"), labels()).aggregate(aggregate),
        sm::make_gauge("log_last_index", status([] (const auto& s) { return s.last_idx.value(); }),
             sm::description("index of the last log entry"), labels()).aggregate(aggregate),
        sm::make_gauge("log_last_term", status([] (const auto& s) { return s.last_term.value(); }),
             sm::description("term of the last log entry"), labels()).aggregate(aggregate),
        sm::make_gauge("snapshot_last_index", status([] (const auto& s) { return s.last_snapshot_idx.value(); }),
             sm::description("index of the snapshot"), labels()).aggregate(aggregate),
        sm::make_gauge("snapshot_last_term", status([] (const auto& s) { return s.last_snapshot_term.value(); }),
             sm::description("term of the snapshot"), labels()).aggregate(aggregate),
        sm::make_gauge("state", status([] (const auto& s) { return s.state; }),
             sm::description("current state: 0 - follower, 1 - candidate, 2 - leader"), labels()).aggregate(aggregate),
        sm::make_gauge("commit_index", status([] (const auto& s) { return s.commit_idx.value(); }),
             sm::description("commit index"), labels()).aggregate(aggregate),
        sm::make_gauge("apply_index", status([] (const auto& s) { return s.applied_idx.value(); }),
             sm::description("applied index"), labels()).aggregate(aggregate),
    });
}

} // namespace service
