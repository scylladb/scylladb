/*
 * Copyright 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

namespace locator {

struct tablet_id final {
    uint64_t value();
};

struct global_tablet_id final {
    ::table_id table;
    locator::tablet_id tablet;
};

struct table_load_stats final {
    uint64_t size_in_bytes;
    int64_t split_ready_seq_number;
};

struct load_stats final {
    std::unordered_map<::table_id, locator::table_load_stats> tables;
};

}

namespace service {
struct fencing_token {
    service::topology::version_t topology_version;
};

struct raft_topology_cmd {
    enum class command: uint8_t {
        barrier,
        barrier_and_drain,
        stream_ranges,
        wait_for_ip
    };
    service::raft_topology_cmd::command cmd;
};

struct raft_topology_cmd_result {
    enum class command_status: uint8_t {
        fail,
        success
    };
    service::raft_topology_cmd_result::command_status status;
};

struct raft_snapshot {
    utils::chunked_vector<canonical_mutation> mutations;
};

struct raft_snapshot_pull_params {
    std::vector<table_id> tables;
};

verb raft_topology_cmd (raft::server_id dst_id, raft::term_t term, uint64_t cmd_index, service::raft_topology_cmd) -> service::raft_topology_cmd_result;
verb [[cancellable]] raft_pull_snapshot (raft::server_id dst_id, service::raft_snapshot_pull_params) -> service::raft_snapshot;
verb [[cancellable]] tablet_stream_data (raft::server_id dst_id, locator::global_tablet_id);
verb [[cancellable]] tablet_cleanup (raft::server_id dst_id, locator::global_tablet_id);
verb [[cancellable]] table_load_stats (raft::server_id dst_id) -> locator::load_stats;
}
