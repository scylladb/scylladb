/*
 * Copyright (C) 2022-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "topology_state_machine.hh"
#include "log.hh"

#include <boost/range/adaptors.hpp>

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

bool topology::is_busy() const {
    return tstate.has_value();
}

raft::server_id topology::parse_replaced_node(const std::optional<request_param>& req_param) {
    if (req_param) {
        auto *param = std::get_if<replace_param>(&*req_param);
        if (param) {
            return param->replaced_id;
        }
    }
    return {};
}

std::unordered_set<raft::server_id> topology::get_excluded_nodes(raft::server_id id,
                                                                 const std::optional<topology_request>& req) const {
    std::unordered_set<raft::server_id> exclude_nodes;
    // ignored_nodes is not per request any longer, but for now consider ignored nodes only
    // for remove and replace operations since only those operations support it on streaming level
    if ((req && (*req == topology_request::remove || *req == topology_request::replace)) ||
        (transition_nodes.contains(id) && (transition_nodes.at(id).state == node_state::removing || transition_nodes.at(id).state == node_state::replacing))) {
        exclude_nodes = ignored_nodes;
    }
    return exclude_nodes;
}

std::optional<request_param> topology::get_request_param(raft::server_id id) const {
    auto rit = req_param.find(id);
    if (rit != req_param.end()) {
        return rit->second;
    }
    return std::nullopt;
};

std::unordered_set<raft::server_id> topology::get_excluded_nodes() const {
    auto result = ignored_nodes;
    for (auto& [id, rs] : left_nodes_rs) {
        result.insert(id);
    }
    return result;
}

std::set<sstring> calculate_not_yet_enabled_features(const std::set<sstring>& enabled_features, const auto& supported_features) {
    std::set<sstring> to_enable;
    bool first = true;

    for (const auto& supported_features : supported_features) {
        if (!first && to_enable.empty()) {
            break;
        }

        if (first) {
            // This is the first node that we process.
            // Calculate the set of features that this node understands
            // but are not enabled yet.
            std::ranges::set_difference(supported_features, enabled_features, std::inserter(to_enable, to_enable.begin()));
            first = false;
        } else {
            // Erase the elements that this node doesn't support
            std::erase_if(to_enable, [&supported_features = supported_features] (const sstring& f) {
                return !supported_features.contains(f);
            });
        }
    }

    return to_enable;
}

std::set<sstring> topology_features::calculate_not_yet_enabled_features() const {
    return ::service::calculate_not_yet_enabled_features(
            enabled_features,
            normal_supported_features | boost::adaptors::map_values);
}

std::set<sstring> topology::calculate_not_yet_enabled_features() const {
    return ::service::calculate_not_yet_enabled_features(
            enabled_features,
            normal_nodes
            | boost::adaptors::map_values
            | boost::adaptors::transformed([] (const replica_state& rs) -> const std::set<sstring>& {
                return rs.supported_features;
            }));
}

std::unordered_set<raft::server_id> topology::get_normal_zero_token_nodes() const {
    std::unordered_set<raft::server_id> normal_zero_token_nodes;
    for (const auto& node: normal_nodes) {
        if (node.second.ring.value().tokens.empty()) {
            normal_zero_token_nodes.insert(node.first);
        }
    }
    return normal_zero_token_nodes;
}

size_t topology::size() const {
    return normal_nodes.size() + transition_nodes.size() + new_nodes.size();
}

bool topology::is_empty() const {
    return size() == 0;
}

static std::unordered_map<topology::transition_state, sstring> transition_state_to_name_map = {
    {topology::transition_state::join_group0, "join group0"},
    {topology::transition_state::commit_cdc_generation, "commit cdc generation"},
    {topology::transition_state::write_both_read_old, "write both read old"},
    {topology::transition_state::write_both_read_new, "write both read new"},
    {topology::transition_state::tablet_migration, "tablet migration"},
    {topology::transition_state::tablet_split_finalization, "tablet split finalization"},
    {topology::transition_state::tablet_draining, "tablet draining"},
    {topology::transition_state::left_token_ring, "left token ring"},
    {topology::transition_state::rollback_to_normal, "rollback to normal"},
};

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
    {node_state::left, "left"},
    {node_state::replacing, "replacing"},
    {node_state::rebuilding, "rebuilding"},
    {node_state::none, "none"},
};

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
    {global_topology_request::cleanup, "cleanup"},
    {global_topology_request::keyspace_rf_change, "keyspace_rf_change"},
};

global_topology_request global_topology_request_from_string(const sstring& s) {
    for (auto&& e : global_topology_request_to_name_map) {
        if (e.second == s) {
            return e.first;
        }
    }

    on_internal_error(tsmlogger, format("cannot map name {} to global_topology_request", s));
}

static std::unordered_map<cleanup_status, sstring> cleanup_status_to_name_map = {
    {cleanup_status::clean, "clean"},
    {cleanup_status::needed, "needed"},
    {cleanup_status::running, "running"},
};

cleanup_status cleanup_status_from_string(const sstring& s) {
    for (auto&& e : cleanup_status_to_name_map) {
        if (e.second == s) {
            return e.first;
        }
    }
    throw std::runtime_error(fmt::format("cannot map name {} to cleanup_status", s));
}

static const std::unordered_map<topology::upgrade_state_type, sstring> upgrade_state_to_name_map = {
    {topology::upgrade_state_type::not_upgraded, "not_upgraded"},
    {topology::upgrade_state_type::build_coordinator_state, "build_coordinator_state"},
    {topology::upgrade_state_type::done, "done"},
};

topology::upgrade_state_type upgrade_state_from_string(const sstring& s) {
    for (const auto& e : upgrade_state_to_name_map) {
        if (e.second == s) {
            return e.first;
        }
    }
    on_internal_error(tsmlogger, format("cannot map name {} to upgrade_state", s));
}

future<> topology_state_machine::await_not_busy() {
    while (_topology.is_busy()) {
        co_await event.wait();
    }
}

}

auto fmt::formatter<service::cleanup_status>::format(service::cleanup_status status,
                                                     fmt::format_context& ctx) const -> decltype(ctx.out()) {
    return fmt::format_to(ctx.out(), "{}", service::cleanup_status_to_name_map[status]);
}

auto fmt::formatter<service::topology::upgrade_state_type>::format(service::topology::upgrade_state_type state,
                                                     fmt::format_context& ctx) const -> decltype(ctx.out()) {
    return fmt::format_to(ctx.out(), "{}", service::upgrade_state_to_name_map.at(state));
}

auto fmt::formatter<service::topology::transition_state>::format(service::topology::transition_state s,
                                                                 fmt::format_context& ctx) const -> decltype(ctx.out()) {
    auto it = service::transition_state_to_name_map.find(s);
    if (it == service::transition_state_to_name_map.end()) {
        on_internal_error(service::tsmlogger, "cannot print transition_state");
    }
    return formatter<string_view>::format(std::string_view(it->second), ctx);
}

auto fmt::formatter<service::node_state>::format(service::node_state s,
                                                 fmt::format_context& ctx) const -> decltype(ctx.out()) {
    auto it = service::node_state_to_name_map.find(s);
    if (it == service::node_state_to_name_map.end()) {
        on_internal_error(service::tsmlogger, "cannot print node_state");
    }
    return formatter<string_view>::format(std::string_view(it->second), ctx);
}


auto fmt::formatter<service::topology_request>::format(service::topology_request req,
                                                       fmt::format_context& ctx) const -> decltype(ctx.out()) {
    return formatter<string_view>::format(std::string_view(service::topology_request_to_name_map[req]), ctx);
}

auto fmt::formatter<service::global_topology_request>::format(service::global_topology_request req,
                                                              fmt::format_context& ctx) const -> decltype(ctx.out()) {
    auto it = service::global_topology_request_to_name_map.find(req);
    if (it == service::global_topology_request_to_name_map.end()) {
        on_internal_error(service::tsmlogger, fmt::format("cannot print global topology request {}", static_cast<uint8_t>(req)));
    }
    return formatter<string_view>::format(std::string_view(it->second), ctx);
}


auto fmt::formatter<service::raft_topology_cmd::command>::format(service::raft_topology_cmd::command cmd,
                                                     fmt::format_context& ctx) const -> decltype(ctx.out()) {
    std::string_view name;
    using enum service::raft_topology_cmd::command;
    switch (cmd) {
        case barrier:
            name = "barrier";
            break;
        case barrier_and_drain:
            name = "barrier_and_drain";
            break;
        case stream_ranges:
            name = "stream_ranges";
            break;
        case wait_for_ip:
            name = "wait_for_ip";
            break;
    }
    return formatter<string_view>::format(name, ctx);
}
