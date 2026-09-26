/*
 * Copyright (C) 2026-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "db/system_keyspace.hh"
#include "service/topology_utils.hh"
#include "service/topology_state_machine.hh"

#include <algorithm>

namespace service {

const std::vector<std::pair<sstring, size_t>>& auto_rf_keyspaces() {
    // FIXME: Currently an empty list, to be populated in the next patches
    static const std::vector<std::pair<sstring, size_t>> keyspaces = {{
    }};
    return keyspaces;
}

bool is_auto_rf_keyspace(std::string_view ks_name) {
    return std::ranges::any_of(auto_rf_keyspaces(), [&] (const auto& e) { return e.first == ks_name; });
}

future<bool> ongoing_rf_change(const topology& topology, db::system_keyspace& sys_ks, const group0_guard& guard, sstring ks) {
    auto ongoing_ks_rf_change = [&] (utils::UUID request_id) -> future<bool> {
        auto req_entry = co_await sys_ks.get_topology_request_entry(request_id);
        co_return std::holds_alternative<global_topology_request>(req_entry.request_type) &&
            std::get<global_topology_request>(req_entry.request_type) == global_topology_request::keyspace_rf_change &&
            req_entry.new_keyspace_rf_change_ks_name.has_value() && req_entry.new_keyspace_rf_change_ks_name.value() == ks;
    };
    if (topology.global_request_id.has_value()) {
        auto req_id = topology.global_request_id.value();
        if (co_await ongoing_ks_rf_change(req_id)) {
            co_return true;
        }
    }
    for (auto request_id : topology.paused_rf_change_requests) {
        if (co_await ongoing_ks_rf_change(request_id)) {
            co_return true;
        }
    }
    for (auto request_id : topology.global_requests_queue) {
        if (co_await ongoing_ks_rf_change(request_id)) {
            co_return true;
        }
    }
    for (auto request_id : topology.ongoing_rf_changes) {
        if (co_await ongoing_ks_rf_change(request_id)) {
            co_return true;
        }
    }
    co_return false;
}

future<bool> auto_rf_change_ongoing(const topology& topology, db::system_keyspace& sys_ks, const group0_guard& guard) {
    for (const auto& [ks_name, goal] : auto_rf_keyspaces()) {
        if (co_await ongoing_rf_change(topology, sys_ks, guard, ks_name)) {
            co_return true;
        }
    }
    co_return false;
}

}
