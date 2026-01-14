/*
 * Copyright (C) 2026-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#include "db/system_keyspace.hh"
#include "service/topology_utils.hh"
#include "service/topology_state_machine.hh"

namespace service {

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
    co_return false;
}

}
