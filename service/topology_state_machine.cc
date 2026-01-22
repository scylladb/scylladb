/*
 * Copyright (C) 2022-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#include "topology_state_machine.hh"
#include "utils/log.hh"
#include "db/system_keyspace.hh"
#include "replica/database.hh"
#include "locator/tablets.hh"
#include "topology_mutation.hh"
#include "raft/raft_group0_client.hh"
#include "raft/raft_group0.hh"

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

std::optional<request_param> topology::get_request_param(raft::server_id id) const {
    auto rit = req_param.find(id);
    if (rit != req_param.end()) {
        return rit->second;
    }
    return std::nullopt;
};

std::optional<topology_request> topology::get_request(raft::server_id id) const {
    auto it = requests.find(id);
    if (it != requests.end()) {
        return it->second;
    }
    return std::nullopt;
};

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
            normal_supported_features | std::views::values);
}

std::set<sstring> topology::calculate_not_yet_enabled_features() const {
    return ::service::calculate_not_yet_enabled_features(
            enabled_features,
            normal_nodes
            | std::views::values
            | std::views::transform([] (const replica_state& rs) -> const std::set<sstring>& {
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
    {topology::transition_state::tablet_resize_finalization, "tablet resize finalization"},
    {topology::transition_state::tablet_draining, "tablet draining"},
    {topology::transition_state::left_token_ring, "left token ring"},
    {topology::transition_state::rollback_to_normal, "rollback to normal"},
    {topology::transition_state::truncate_table, "truncate table"},
    {topology::transition_state::lock, "lock"},
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

std::optional<topology_request> try_topology_request_from_string(const sstring& s) {
    for (auto&& e : topology_request_to_name_map) {
        if (e.second == s) {
            return e.first;
        }
    }
    return std::nullopt;
}

topology_request topology_request_from_string(const sstring& s) {
    auto r = try_topology_request_from_string(s);
    if (r) {
        return *r;
    }
    throw std::runtime_error(fmt::format("cannot map name {} to topology_request", s));
}

static std::unordered_map<global_topology_request, sstring> global_topology_request_to_name_map = {
    {global_topology_request::new_cdc_generation, "new_cdc_generation"},
    {global_topology_request::cleanup, "cleanup"},
    {global_topology_request::keyspace_rf_change, "keyspace_rf_change"},
    {global_topology_request::truncate_table, "truncate_table"},
    {global_topology_request::noop_request, "noop_request"},
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

node_validation_result
validate_removing_node(replica::database& db, locator::host_id host_id) {
    auto tmptr = db.get_token_metadata_ptr();
    auto* node = tmptr->get_topology().find_node(host_id);
    if (!node) {
        return node_validation_failure{fmt::format("Node {} not found in topology", host_id)};
    }
    auto op = locator::rf_rack_topology_operation{
            locator::rf_rack_topology_operation::type::remove, host_id, node->dc(), node->rack()};
    if (!db.check_rf_rack_validity_with_topology_change(tmptr, std::move(op))) {
        return node_validation_failure{"Cannot remove the node because its removal would make some existing keyspace RF-rack-invalid"};
    }
    return node_validation_success {};
}

future<sstring> topology_state_machine::wait_for_request_completion(db::system_keyspace& sys_ks, utils::UUID id, bool require_entry) {
    tsmlogger.debug("Start waiting for topology request completion (request id {})", id);
    while (true) {
        auto c = reload_count;
        auto [done, error] = co_await sys_ks.get_topology_request_state(id, require_entry);
        if (done) {
            tsmlogger.debug("Request with id {} is completed with status: {}", id, error.empty() ? sstring("success") : error);
            co_return error;
        }
        if (c == reload_count) {
            // wait only if the state was not reloaded while we were preempted
            tsmlogger.debug("Waiting for a topology event while waiting for topology request completion (request id {})", id);
            co_await event.when();
        }
    }

    co_return sstring();
}

void topology_state_machine::generate_cancel_request_update(utils::chunked_vector<canonical_mutation>& muts,
                                                            gms::feature_service& features,
                                                            const group0_guard& guard,
                                                            raft::server_id node,
                                                            sstring reason) {
    auto* server_rs = _topology.find(node);
    if (!server_rs) {
        return;
    }

    auto request_id = server_rs->second.request_id;
    if (!request_id) {
        return;
    }

    auto i = _topology.requests.find(node);
    if (i == _topology.requests.end()) {
        // Request is not pending, and not cancelable at this stage.
        return;
    }
    auto req = i->second;

    // Request will not be considered as failed if reason is empty.
    if (reason.empty()) {
        tsmlogger.warn("Request {} canceled without specifying a reason, at: {}", request_id, seastar::current_backtrace());
        reason = "canceled";
    }

    tsmlogger.info("Will cancel {} request {} for node {}: {}", req, request_id, node, reason);

    topology_mutation_builder builder(guard.write_timestamp());
    auto& node_builder = builder.with_node(node)
            .del("topology_request");

    switch (req) {
        case topology_request::replace:
            [[fallthrough]];
        case topology_request::join:
            node_builder.set("node_state", node_state::left);
            break;
        case topology_request::leave:
            [[fallthrough]];
        case topology_request::rebuild:
            [[fallthrough]];
        case topology_request::remove:
            break;
    }

    muts.emplace_back(builder.build());

    topology_request_tracking_mutation_builder rtbuilder(request_id);
    rtbuilder.done(std::move(reason));
    muts.emplace_back(rtbuilder.build());
}

future<> topology_state_machine::abort_request(service::raft_group0& group0,
                                               abort_source& as,
                                               gms::feature_service& features,
                                               utils::UUID request_id) {
    while (true) {
        auto guard = co_await group0.client().start_operation(as, raft_timeout{});

        utils::chunked_vector<canonical_mutation> muts;

        for (auto& [node, rs] : _topology.transition_nodes) {
            if (rs.request_id == request_id) {
                throw std::runtime_error(format("request {} for node {} is already running and cannot be aborted", request_id, node));
            }
        }

        for (auto& [node, req] : _topology.requests) {
            auto *server_rs = _topology.find(node);
            if (!server_rs || server_rs->second.request_id != request_id) {
                continue;
            }

            if (req == topology_request::join || req == topology_request::replace) {
                // Only topology coordinator can abort join requests. It sends reject RPC.
                throw std::runtime_error(format("cannot abort {} request for node {}", req, node));
            }

            tsmlogger.info("aborting {} request {} for node {}", req, request_id, node);
            generate_cancel_request_update(muts, features, guard, node, "aborted on user request");
            break;
        }

        if (muts.empty()) {
            // FIXME: Handle global requests.
            throw std::runtime_error(format("Don't know how to abort {}", request_id));
        }

        topology_change change{std::move(muts)};
        group0_command cmd = group0.client().prepare_command(std::move(change), guard,
                                                              ::format("aborting topology request {}", request_id));

        try {
            co_await group0.client().add_entry(std::move(cmd), std::move(guard), as, raft_timeout{});
        } catch (group0_concurrent_modification&) {
            tsmlogger.info("aborting request {}: concurrent modification, retrying.", request_id);
            continue;
        }
        break;
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
