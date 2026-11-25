/*
 * Copyright (C) 2026-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "db/system_keyspace.hh"
#include "locator/abstract_replication_strategy.hh"
#include "replica/database.hh"
#include "service/topology_utils.hh"
#include "service/topology_state_machine.hh"

#include <algorithm>

namespace service {

const std::vector<auto_rf_keyspace>& auto_rf_keyspaces() {
    // FIXME: Currently an empty list, to be populated in the next patches
    static const std::vector<auto_rf_keyspace> keyspaces = {{
    }};
    return keyspaces;
}

bool is_auto_rf_keyspace(std::string_view ks_name) {
    return std::ranges::any_of(auto_rf_keyspaces(), [&] (const auto_rf_keyspace& ks) { return ks.name == ks_name; });
}

std::vector<sstring> auto_rf_keyspaces_using_rack(const replica::database& db, const sstring& dc, const sstring& rack) {
    std::vector<sstring> ret;
    for (const auto& [ks_name, goal] : auto_rf_keyspaces()) {
        if (!db.has_keyspace(ks_name)) {
            continue;
        }
        auto ks_md = db.find_keyspace(ks_name).metadata();
        const auto& options = ks_md->strategy_options();
        auto it = options.find(dc);
        if (!ks_md->uses_tablets() || it == options.end()) {
            continue;
        }
        auto rf_data = locator::replication_factor_data(it->second);
        if (rf_data.is_rack_based() && std::ranges::contains(rf_data.get_rack_list(), rack)) {
            ret.push_back(ks_name);
        }
    }
    return ret;
}

future<bool> ongoing_rf_change(const topology& topology, db::system_keyspace& sys_ks, const group0_guard& guard, sstring ks) {
    auto ongoing_ks_rf_change = [&] (utils::UUID request_id) -> future<bool> {
        // The row may be gone (it has a TTL); this runs on every pass of the reconciler,
        // so a missing row must not be an error.
        auto req_entry = co_await sys_ks.get_topology_request_entry_opt(request_id);
        co_return req_entry && std::holds_alternative<global_topology_request>(req_entry->request_type) &&
            std::get<global_topology_request>(req_entry->request_type) == global_topology_request::keyspace_rf_change &&
            req_entry->new_keyspace_rf_change_ks_name.has_value() && req_entry->new_keyspace_rf_change_ks_name.value() == ks;
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
