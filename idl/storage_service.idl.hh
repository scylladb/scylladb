/*
 * Copyright 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "service/tablet_operation.hh"

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

struct load_stats_v1 final {
    std::unordered_map<::table_id, locator::table_load_stats> tables;
};

struct load_stats {
    std::unordered_map<::table_id, locator::table_load_stats> tables;
    std::unordered_map<locator::host_id, uint64_t> capacity;
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

struct tablet_operation_repair_result {
    gc_clock::time_point repair_time;
};

verb raft_topology_cmd (raft::server_id dst_id, raft::term_t term, uint64_t cmd_index, service::raft_topology_cmd) -> service::raft_topology_cmd_result;
verb [[cancellable]] raft_pull_snapshot (raft::server_id dst_id, service::raft_snapshot_pull_params) -> service::raft_snapshot;
verb [[cancellable]] tablet_stream_data (raft::server_id dst_id, locator::global_tablet_id);
verb [[cancellable]] tablet_cleanup (raft::server_id dst_id, locator::global_tablet_id);
verb [[cancellable]] table_load_stats_v1 (raft::server_id dst_id) -> locator::load_stats_v1;
verb [[cancellable]] table_load_stats (raft::server_id dst_id) -> locator::load_stats;
verb [[cancellable]] tablet_repair(raft::server_id dst_id, locator::global_tablet_id) -> service::tablet_operation_repair_result;
verb [[]] estimate_sstable_volume(table_id table) -> uint64_t;
verb [[]] sample_sstables(table_id table, uint64_t chunk_size, uint64_t n_chunks) -> utils::chunked_vector<temporary_buffer<char>>;

}
