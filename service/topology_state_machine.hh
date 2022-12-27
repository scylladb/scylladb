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
#include "dht/token.hh"
#include "raft/raft.hh"
#include "utils/UUID.hh"
#include "dht/i_partitioner.hh"
#include "mutation/canonical_mutation.hh"

namespace service {

enum class node_state: uint8_t {
    none,                // the new node joined group0 but did not bootstraped yet (has no tokens and data to serve)
    bootstrapping,       // the node is currently in the process of streaming its part of the ring
    decommissioning,     // the node is being decomissioned and stream its data to nodes that took over
    removing,            // the node is being removed and its data is streamed to nodes that took over from still alive owners
    replacing,           // the node replaces another dead node in the cluster and it data is being streamed to it
    rebuilding,          // the node is being rebuild and is streaming data from other replicas
    normal,              // the node does not do any streaming and serves the slice of the ring that belongs to it
    left                 // the node left the cluster and group0
};

enum class topology_request: uint8_t {
    join,
    leave,
    remove,
    replace,
    rebuild
};

using request_param = std::variant<raft::server_id, sstring, uint32_t>;

struct ring_slice {
    enum class replication_state: uint8_t {
        write_both_read_old,
        write_both_read_new,
        owner
    };

    replication_state state;
    std::unordered_set<dht::token> tokens;
};

struct replica_state {
    node_state state;
    seastar::sstring datacenter;
    seastar::sstring rack;
    seastar::sstring release_version;
    std::optional<ring_slice> ring; // if engaged contain the set of tokens the node owns together with their state
};

struct topology {
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

    // Find only nodes in non 'left' state
    const std::pair<const raft::server_id, replica_state>* find(raft::server_id id);
    // Return true if node exists in any state including 'left' one
    bool contains(raft::server_id id);
};

struct raft_topology_snapshot {
    std::vector<canonical_mutation> mutations;
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
      enum class command: uint8_t {
          barrier,         // request to wait for the latest topology
          stream_ranges,   // reqeust to stream data, return when streaming is
                           // done
          fence_old_reads  // wait for all reads started before to complete
      };
      command cmd;
};

// returned as a result of raft_bootstrap_cmd
struct raft_topology_cmd_result {
    enum class command_status: uint8_t {
        fail,
        success
    };
    command_status status = command_status::fail;
};

std::ostream& operator<<(std::ostream& os, ring_slice::replication_state s);
ring_slice::replication_state replication_state_from_string(const sstring& s);
std::ostream& operator<<(std::ostream& os, node_state s);
node_state node_state_from_string(const sstring& s);
std::ostream& operator<<(std::ostream& os, const topology_request& req);
topology_request topology_request_from_string(const sstring& s);
std::ostream& operator<<(std::ostream& os, const raft_topology_cmd::command& cmd);
}
