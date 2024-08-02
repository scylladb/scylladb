/*
 * Copyright (C) 2022-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <boost/range/algorithm/find_if.hpp>
#include <iostream>
#include <unordered_set>
#include <unordered_map>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/shared_ptr.hh>
#include "cdc/generation_id.hh"
#include "dht/token.hh"
#include "raft/raft.hh"
#include "utils/UUID.hh"
#include "service/session.hh"
#include "mutation/canonical_mutation.hh"

namespace service {

enum class node_state: uint16_t {
    none,                // the new node joined group0 but has not bootstrapped yet (has no tokens and data to serve)
    bootstrapping,       // the node is currently in the process of streaming its part of the ring
    decommissioning,     // the node is being decommissioned and stream its data to nodes that took over
    removing,            // the node is being removed and its data is streamed to nodes that took over from still alive owners
    replacing,           // the node replaces another dead node in the cluster and it data is being streamed to it
    rebuilding,          // the node is being rebuild and is streaming data from other replicas
    normal,              // the node does not do any streaming and serves the slice of the ring that belongs to it
    left,                // the node left the cluster and group0
};

// The order of the requests is a priority
// order in which requests are executes in case
// there are multiple requests in the queue.
// The order tries to minimize the amount of cleanups.
enum class topology_request: uint16_t {
    replace,
    join,
    remove,
    leave,
    rebuild
};

enum class cleanup_status : uint16_t {
    clean,
    needed,
    running,
};

struct join_param {
    uint32_t num_tokens;
    sstring tokens_string;
};

struct rebuild_param {
    sstring source_dc;
};

struct replace_param {
    raft::server_id replaced_id;
};

using request_param = std::variant<join_param, rebuild_param, replace_param>;

enum class global_topology_request: uint16_t {
    new_cdc_generation,
    cleanup,
    keyspace_rf_change,
};

struct ring_slice {
    std::unordered_set<dht::token> tokens;
};

struct replica_state {
    node_state state;
    seastar::sstring datacenter;
    seastar::sstring rack;
    seastar::sstring release_version;
    std::optional<ring_slice> ring; // if engaged contain the set of tokens the node owns together with their state
    size_t shard_count;
    uint8_t ignore_msb;
    std::set<sstring> supported_features;
    cleanup_status cleanup;
    utils::UUID request_id; // id of the current request for the node or the last one if no current one exists
};

struct topology_features {
    // Supported features, for normal nodes
    std::unordered_map<raft::server_id, std::set<sstring>> normal_supported_features;

    // Features that are considered enabled by the cluster
    std::set<sstring> enabled_features;

    // Calculates a set of features that are supported by all normal nodes but not yet enabled.
    std::set<sstring> calculate_not_yet_enabled_features() const;
};

struct topology {
    enum class transition_state: uint16_t {
        join_group0,
        commit_cdc_generation,
        tablet_draining,
        write_both_read_old,
        write_both_read_new,
        tablet_migration,
        tablet_split_finalization,
        left_token_ring,
        rollback_to_normal,
    };

    std::optional<transition_state> tstate;

    enum class upgrade_state_type: uint16_t {
        not_upgraded,
        build_coordinator_state,
        done,
    };

    upgrade_state_type upgrade_state = upgrade_state_type::not_upgraded;

    using version_t = int64_t;
    static constexpr version_t initial_version = 1;
    version_t version = initial_version;
    version_t fence_version = initial_version;

    // Nodes that are normal members of the ring
    std::unordered_map<raft::server_id, replica_state> normal_nodes;
    // Nodes that are left
    std::unordered_set<raft::server_id> left_nodes;
    // Left nodes for which we need topology information.
    std::unordered_map<raft::server_id, replica_state> left_nodes_rs;
    // Nodes that are waiting to be joined by the topology coordinator
    std::unordered_map<raft::server_id, replica_state> new_nodes;
    // Nodes that are in the process to be added to the ring
    // Currently at most one node at a time will be here, but the code shouldn't assume it
    // because we might support parallel operations in the future.
    std::unordered_map<raft::server_id, replica_state> transition_nodes;

    // Pending topology requests
    std::unordered_map<raft::server_id, topology_request> requests;

    // Holds parameters for a request per node and valid during entire
    // operation until the node becomes normal
    std::unordered_map<raft::server_id, request_param> req_param;

    // Pending global topology request (i.e. not related to any specific node).
    std::optional<global_topology_request> global_request;

    // Pending global topology request's id, which is a new group0's state id
    std::optional<utils::UUID> global_request_id;

    // The IDs of the committed CDC generations sorted by timestamps.
    // The obsolete generations may not be in this list as they are continually deleted.
    std::vector<cdc::generation_id_v2> committed_cdc_generations;

    // This is the time UUID used to access the data of a new CDC generation introduced
    // e.g. when a new node bootstraps, needed in `commit_cdc_generation` transition state.
    // It's used as the first column of the clustering key in CDC_GENERATIONS_V3 table.
    std::optional<utils::UUID> new_cdc_generation_data_uuid;

    // The name of the KS that is being the target of the scheduled ALTER KS statement
    std::optional<sstring> new_keyspace_rf_change_ks_name;
    // The KS options to be used when executing the scheduled ALTER KS statement
    std::optional<std::unordered_map<sstring, sstring>> new_keyspace_rf_change_data;

    // The IDs of the committed yet unpublished CDC generations sorted by timestamps.
    std::vector<cdc::generation_id_v2> unpublished_cdc_generations;

    // Set of features that are considered to be enabled by the cluster.
    std::set<sstring> enabled_features;

    // Session used to create topology_guard for operations like streaming.
    session_id session;

    // When false, tablet load balancer will not try to rebalance tablets.
    bool tablet_balancing_enabled = true;

    // The set of nodes that should be considered dead during topology operations
    std::unordered_set<raft::server_id> ignored_nodes;

    // Find only nodes in non 'left' state
    const std::pair<const raft::server_id, replica_state>* find(raft::server_id id) const;
    // Return true if node exists in any state including 'left' one
    bool contains(raft::server_id id);
    // Number of nodes that are not in the 'left' state
    size_t size() const;
    // Are there any non-left nodes?
    bool is_empty() const;

    // Returns false iff we can safely start a new topology change.
    bool is_busy() const;

    // Returns the set of nodes currently excluded from synchronization-with in the topology.
    // Barrier should not wait for those nodes. Used for tablets migration only.
    std::unordered_set<raft::server_id> get_excluded_nodes() const;

    std::optional<request_param> get_request_param(raft::server_id) const;
    static raft::server_id parse_replaced_node(const std::optional<request_param>&);
    // Returns the set of nodes currently excluded from based on global topology request.
    // Used by topology coordinator code only.
    std::unordered_set<raft::server_id> get_excluded_nodes(raft::server_id id, const std::optional<topology_request>& req) const;

    // Calculates a set of features that are supported by all normal nodes but not yet enabled.
    std::set<sstring> calculate_not_yet_enabled_features() const;

    // Returns the set of zero-token normal nodes.
    std::unordered_set<raft::server_id> get_normal_zero_token_nodes() const;
};

struct raft_snapshot {
    // FIXME: handle this with rpc streaming instead as we can't guarantee size bounds.
    utils::chunked_vector<canonical_mutation> mutations;
};

struct raft_snapshot_pull_params {
    std::vector<table_id> tables;
};

// State machine that is responsible for topology change
struct topology_state_machine {
    using topology_type = topology;
    topology_type _topology;
    condition_variable event;

    future<> await_not_busy();
};

// Raft leader uses this command to drive bootstrap process on other nodes
struct raft_topology_cmd {
      enum class command: uint16_t {
          barrier,              // request to wait for the latest topology
          barrier_and_drain,    // same + drain requests which use previous versions
          stream_ranges,        // request to stream data, return when streaming is
                                // done
          wait_for_ip           // wait for a joining node IP to appear in raft_address_map
      };
      command cmd;

      raft_topology_cmd(command c) : cmd(c) {}
};

// returned as a result of raft_bootstrap_cmd
struct raft_topology_cmd_result {
    enum class command_status: uint16_t {
        fail,
        success
    };
    command_status status = command_status::fail;
};

// This class is used in RPC's signatures to hold the topology_version of the caller.
// The reason why we wrap the topology_version in this class is that we anticipate
// other versions to occur in the future, such as the schema version.
struct fencing_token {
    topology::version_t topology_version{0};
    // topology_version == 0 means the caller is not aware about
    // the fencing or doesn't use it for some reason.
    explicit operator bool() const {
        return topology_version != 0;
    }
};

struct topology_request_state {
    bool done;
    sstring error;
};

topology::transition_state transition_state_from_string(const sstring& s);
node_state node_state_from_string(const sstring& s);
topology_request topology_request_from_string(const sstring& s);
global_topology_request global_topology_request_from_string(const sstring&);
cleanup_status cleanup_status_from_string(const sstring& s);
topology::upgrade_state_type upgrade_state_from_string(const sstring&);
}

template <> struct fmt::formatter<service::cleanup_status> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(service::cleanup_status status, fmt::format_context& ctx) const -> decltype(ctx.out());
};

template <> struct fmt::formatter<service::topology::upgrade_state_type> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(service::topology::upgrade_state_type status, fmt::format_context& ctx) const -> decltype(ctx.out());
};

template <> struct fmt::formatter<service::fencing_token> : fmt::formatter<string_view> {
    auto format(const service::fencing_token& fencing_token, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "{{{}}}", fencing_token.topology_version);
    }
};

template <> struct fmt::formatter<service::topology::transition_state> : fmt::formatter<string_view> {
    auto format(service::topology::transition_state, fmt::format_context& ctx) const -> decltype(ctx.out());
};

template <> struct fmt::formatter<service::node_state> : fmt::formatter<string_view> {
    auto format(service::node_state, fmt::format_context& ctx) const -> decltype(ctx.out());
};

template <> struct fmt::formatter<service::topology_request> : fmt::formatter<string_view> {
    auto format(service::topology_request, fmt::format_context& ctx) const -> decltype(ctx.out());
};

template <> struct fmt::formatter<service::global_topology_request> : fmt::formatter<string_view> {
    auto format(service::global_topology_request, fmt::format_context& ctx) const -> decltype(ctx.out());
};

template <> struct fmt::formatter<service::raft_topology_cmd::command> : fmt::formatter<string_view> {
    auto format(service::raft_topology_cmd::command, fmt::format_context& ctx) const -> decltype(ctx.out());
};
