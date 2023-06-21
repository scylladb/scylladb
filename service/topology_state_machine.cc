/*
 * Copyright (C) 2022-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "topology_state_machine.hh"

namespace service {

logging::logger tsmlogger("topology_state_machine");

const std::pair<const raft::server_id, replica_state>* topology::find(raft::server_id id) const {
    auto it = normal_nodes.find(id);
    if (it != normal_nodes.end()) {
        return &*it;
    }
    it = transition_nodes.find(id);
    if (it != transition_nodes.end()) {
        return &*it;
    }
    it = new_nodes.find(id);
    if (it != new_nodes.end()) {
        return &*it;
    }
    return nullptr;
}

bool topology::contains(raft::server_id id) {
    return normal_nodes.contains(id) ||
           transition_nodes.contains(id) ||
           new_nodes.contains(id) ||
           left_nodes.contains(id);
}

std::ostream& operator<<(std::ostream& os, const fencing_token& fencing_token) {
    return os << "{" << fencing_token.topology_version << "}";
}

static std::unordered_map<topology::transition_state, sstring> transition_state_to_name_map = {
    {topology::transition_state::commit_cdc_generation, "commit cdc generation"},
    {topology::transition_state::publish_cdc_generation, "publish cdc generation"},
    {topology::transition_state::write_both_read_old, "write both read old"},
    {topology::transition_state::write_both_read_new, "write both read new"},
};

std::ostream& operator<<(std::ostream& os, topology::transition_state s) {
    auto it = transition_state_to_name_map.find(s);
    if (it == transition_state_to_name_map.end()) {
        on_internal_error(tsmlogger, "cannot print transition_state");
    }
    return os << it->second;
}

topology::transition_state transition_state_from_string(const sstring& s) {
    for (auto&& e : transition_state_to_name_map) {
        if (e.second == s) {
            return e.first;
        }
    }
    on_internal_error(tsmlogger, format("cannot map name {} to transition_state", s));
}

static std::unordered_map<node_state, sstring> node_state_to_name_map = {
    {node_state::bootstrapping, "bootstrapping"},
    {node_state::decommissioning, "decommissioning"},
    {node_state::removing, "removing"},
    {node_state::normal, "normal"},
    {node_state::left_token_ring, "left_token_ring"},
    {node_state::left, "left"},
    {node_state::replacing, "replacing"},
    {node_state::rebuilding, "rebuilding"},
    {node_state::none, "none"}
};

std::ostream& operator<<(std::ostream& os, node_state s) {
    auto it = node_state_to_name_map.find(s);
    if (it == node_state_to_name_map.end()) {
        on_internal_error(tsmlogger, "cannot print node_state");
    }
    return os << it->second;
}

node_state node_state_from_string(const sstring& s) {
    for (auto&& e : node_state_to_name_map) {
        if (e.second == s) {
            return e.first;
        }
    }
    on_internal_error(tsmlogger, format("cannot map name {} to node_state", s));
}

static std::unordered_map<topology_request, sstring> topology_request_to_name_map = {
    {topology_request::join, "join"},
    {topology_request::leave, "leave"},
    {topology_request::remove, "remove"},
    {topology_request::replace, "replace"},
    {topology_request::rebuild, "rebuild"}
};

std::ostream& operator<<(std::ostream& os, const topology_request& req) {
    os << topology_request_to_name_map[req];
    return os;
}

topology_request topology_request_from_string(const sstring& s) {
    for (auto&& e : topology_request_to_name_map) {
        if (e.second == s) {
            return e.first;
        }
    }
    throw std::runtime_error(fmt::format("cannot map name {} to topology_request", s));
}

static std::unordered_map<global_topology_request, sstring> global_topology_request_to_name_map = {
    {global_topology_request::new_cdc_generation, "new_cdc_generation"},
};

std::ostream& operator<<(std::ostream& os, const global_topology_request& req) {
    auto it = global_topology_request_to_name_map.find(req);
    if (it == global_topology_request_to_name_map.end()) {
        on_internal_error(tsmlogger, format("cannot print global topology request {}", static_cast<uint8_t>(req)));
    }
    return os << it->second;
}

global_topology_request global_topology_request_from_string(const sstring& s) {
    for (auto&& e : global_topology_request_to_name_map) {
        if (e.second == s) {
            return e.first;
        }
    }

    on_internal_error(tsmlogger, format("cannot map name {} to global_topology_request", s));
}

std::ostream& operator<<(std::ostream& os, const raft_topology_cmd::command& cmd) {
    switch (cmd) {
        case raft_topology_cmd::command::barrier:
            os << "barrier";
            break;
        case raft_topology_cmd::command::barrier_and_drain:
            os << "barrier_and_drain";
            break;
        case raft_topology_cmd::command::stream_ranges:
            os << "stream_ranges";
            break;
        case raft_topology_cmd::command::fence:
            os << "fence";
            break;
        case raft_topology_cmd::command::shutdown:
            os << "shutdown";
            break;
    }
    return os;
}
}
