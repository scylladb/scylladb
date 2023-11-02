/*
 * Copyright (C) 2022-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <boost/range/algorithm/find_if.hpp>
#include "boost/range/join.hpp"
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
#include "dht/i_partitioner.hh"
#include "mutation/canonical_mutation.hh"

namespace service {

enum class node_state: uint16_t {
    none,                // the new node joined group0 but did not bootstraped yet (has no tokens and data to serve)
    bootstrapping,       // the node is currently in the process of streaming its part of the ring
    decommissioning,     // the node is being decommissioned and stream its data to nodes that took over
    removing,            // the node is being removed and its data is streamed to nodes that took over from still alive owners
    replacing,           // the node replaces another dead node in the cluster and it data is being streamed to it
    rebuilding,          // the node is being rebuild and is streaming data from other replicas
    normal,              // the node does not do any streaming and serves the slice of the ring that belongs to it
    left_token_ring,     // the node left the token ring, but not group0 yet; we wait until other nodes stop writing to it
    left                 // the node left the cluster and group0
};

enum class topology_request: uint16_t {
    join,
    leave,
    remove,
    replace,
    rebuild
};

struct join_param {
    uint32_t num_tokens;
};

struct rebuild_param {
    sstring source_dc;
};

struct removenode_param {
    std::unordered_set<raft::server_id> ignored_ids;
};

struct replace_param {
    raft::server_id replaced_id;
    std::unordered_set<raft::server_id> ignored_ids;
};

using request_param = std::variant<join_param, rebuild_param, removenode_param, replace_param>;

enum class global_topology_request: uint16_t {
    new_cdc_generation,
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
    };

    std::optional<transition_state> tstate;

    using version_t = int64_t;
    static constexpr version_t initial_version = 1;
    version_t version = initial_version;

    // Nodes that are normal members of the ring
    std::unordered_map<raft::server_id, replica_state> normal_nodes;
    // Nodes that are left
    std::unordered_set<raft::server_id> left_nodes;
    // Nodes that are waiting to be joined by the topology coordinator
    std::unordered_map<raft::server_id, replica_state> new_nodes;
    // Nodes that are in the process to be added to the ring
    // Currently only at most one node at a time will be here
    std::unordered_map<raft::server_id, replica_state> transition_nodes;

    // Pending topology requests
    std::unordered_map<raft::server_id, topology_request> requests;

    // Holds parameters for a request per node and valid during entire
    // operation untill the node becomes normal
    std::unordered_map<raft::server_id, request_param> req_param;

    // Pending global topology request (i.e. not related to any specific node).
    std::optional<global_topology_request> global_request;

    // The ID of the last introduced CDC generation.
    std::optional<cdc::generation_id_v2> current_cdc_generation_id;

    // This is the time UUID used to access the data of a new CDC generation introduced
    // e.g. when a new node bootstraps, needed in `commit_cdc_generation` transition state.
    // It's used as the first column of the clustering key in CDC_GENERATIONS_V3 table.
    std::optional<utils::UUID> new_cdc_generation_data_uuid;

    // The IDs of the commited yet unpublished CDC generations sorted by timestamps.
    std::vector<cdc::generation_id_v2> unpublished_cdc_generations;

    // Set of features that are considered to be enabled by the cluster.
    std::set<sstring> enabled_features;

    // Find only nodes in non 'left' state
    const std::pair<const raft::server_id, replica_state>* find(raft::server_id id) const;
    // Return true if node exists in any state including 'left' one
    bool contains(raft::server_id id);
    // Number of nodes that are not in the 'left' state
    size_t size() const;
    // Are there any non-left nodes?
    bool is_empty() const;

    // Calculates a set of features that are supported by all normal nodes but not yet enabled.
    std::set<sstring> calculate_not_yet_enabled_features() const;
};

struct raft_topology_snapshot {
    // Mutations for the system.topology table.
    std::vector<canonical_mutation> topology_mutations;

    // Mutations for system.cdc_generations_v3, contains all the CDC generation data.
    std::vector<canonical_mutation> cdc_generation_mutations;
};

struct raft_topology_pull_params {
};

// State machine that is responsible for topology change
struct topology_state_machine {
    using topology_type = topology;
    topology_type _topology;
    condition_variable event;
};

// Raft leader uses this command to drive bootstrap process on other nodes
struct raft_topology_cmd {
      enum class command: uint16_t {
          barrier,              // request to wait for the latest topology
          barrier_and_drain,    // same + drain requests which use previous versions
          stream_ranges,        // reqeust to stream data, return when streaming is
                                // done
          fence,                // erect the fence against requests with stale versions
          shutdown,             // a decommissioning node should shut down
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

std::ostream& operator<<(std::ostream& os, const fencing_token& fencing_token);
std::ostream& operator<<(std::ostream& os, topology::transition_state s);
topology::transition_state transition_state_from_string(const sstring& s);
std::ostream& operator<<(std::ostream& os, node_state s);
node_state node_state_from_string(const sstring& s);
std::ostream& operator<<(std::ostream& os, const topology_request& req);
topology_request topology_request_from_string(const sstring& s);
std::ostream& operator<<(std::ostream&, const global_topology_request&);
global_topology_request global_topology_request_from_string(const sstring&);
std::ostream& operator<<(std::ostream& os, const raft_topology_cmd::command& cmd);
}
