/*
 * Copyright (C) 2026-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "audit/audit_cf_storage_helper.hh"
#include "db/system_keyspace.hh"
#include "service/topology_utils.hh"
#include "service/topology_state_machine.hh"
#include "tracing/trace_keyspace_helper.hh"
#include "utils/small_vector.hh"

#include <algorithm>

namespace service {

const std::vector<std::pair<sstring, size_t>>& auto_rf_keyspaces() {
    static const std::vector<std::pair<sstring, size_t>> keyspaces = {{
        {audit::audit_cf_storage_helper::KEYSPACE_NAME, audit::audit_cf_storage_helper::RF_GOAL_PER_DC},
        {sstring(tracing::trace_keyspace_helper::KEYSPACE_NAME), tracing::trace_keyspace_helper::RF_GOAL_PER_DC},
    }};
    return keyspaces;
}

bool is_auto_rf_keyspace(std::string_view ks_name) {
    return std::ranges::any_of(auto_rf_keyspaces(), [&] (const auto& e) { return e.first == ks_name; });
}

future<bool> ongoing_rf_change_unguarded(const topology& topology, db::system_keyspace& sys_ks, sstring ks) {
    // Copy the ids before the first co_await: without a group0 guard, a topology state
    // reload can replace these containers while the lookups below are suspended.
    utils::small_vector<utils::UUID, 8> request_ids;
    if (topology.global_request_id) {
        request_ids.push_back(*topology.global_request_id);
    }
    request_ids.insert(request_ids.end(), topology.paused_rf_change_requests.begin(), topology.paused_rf_change_requests.end());
    request_ids.insert(request_ids.end(), topology.global_requests_queue.begin(), topology.global_requests_queue.end());
    request_ids.insert(request_ids.end(), topology.ongoing_rf_changes.begin(), topology.ongoing_rf_changes.end());
    for (const auto& request_id : request_ids) {
        auto req_entry = co_await sys_ks.get_topology_request_entry_opt(request_id);
        if (req_entry && std::holds_alternative<global_topology_request>(req_entry->request_type) &&
                std::get<global_topology_request>(req_entry->request_type) == global_topology_request::keyspace_rf_change &&
                req_entry->new_keyspace_rf_change_ks_name == ks) {
            co_return true;
        }
    }
    co_return false;
}

future<bool> ongoing_rf_change(const topology& topology, db::system_keyspace& sys_ks, const group0_guard&, sstring ks) {
    return ongoing_rf_change_unguarded(topology, sys_ks, std::move(ks));
}

future<bool> auto_rf_change_ongoing_unguarded(const topology& topology, db::system_keyspace& sys_ks) {
    for (const auto& [ks_name, goal] : auto_rf_keyspaces()) {
        if (co_await ongoing_rf_change_unguarded(topology, sys_ks, ks_name)) {
            co_return true;
        }
    }
    co_return false;
}

future<bool> auto_rf_change_ongoing(const topology& topology, db::system_keyspace& sys_ks, const group0_guard&) {
    return auto_rf_change_ongoing_unguarded(topology, sys_ks);
}

}
