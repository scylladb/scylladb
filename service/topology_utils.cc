/*
 * Copyright (C) 2026-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "db/config.hh"
#include "db/system_keyspace.hh"
#include "gms/feature_service.hh"
#include "locator/abstract_replication_strategy.hh"
#include "replica/database.hh"
#include "service/auto_rf_keyspaces.hh"
#include "service/topology_utils.hh"
#include "service/topology_state_machine.hh"

#include <algorithm>

namespace service {

const std::vector<std::pair<sstring, size_t>>& auto_rf_keyspaces() {
    static const std::vector<std::pair<sstring, size_t>> keyspaces = {{
        {sstring(audit_keyspace_name), audit_rf_goal_per_dc},
        {sstring(system_traces_keyspace_name), system_traces_rf_goal_per_dc},
    }};
    return keyspaces;
}

bool is_auto_rf_keyspace(std::string_view ks_name) {
    return std::ranges::any_of(auto_rf_keyspaces(), [&] (const auto& e) { return e.first == ks_name; });
}

const std::unordered_set<sstring>& auto_rf_keyspace_names() {
    static const auto names = auto_rf_keyspaces() | std::views::keys | std::ranges::to<std::unordered_set<sstring>>();
    return names;
}

bool is_auto_rf_rack_eligible(const auto_rf_eligible_racks& eligible, const sstring& dc, const sstring& rack) {
    auto it = eligible.find(dc);
    return it != eligible.end() && (!it->second.has_value() || it->second->contains(rack));
}

std::optional<sstring> find_ineligible_auto_rf_rack(const auto_rf_eligible_racks& eligible, const sstring& dc, const std::vector<sstring>& rack_list) {
    if (eligible.empty() || rack_list.size() < 2) {
        return std::nullopt;
    }
    for (const auto& rack : rack_list) {
        if (!is_auto_rf_rack_eligible(eligible, dc, rack)) {
            return rack;
        }
    }
    return std::nullopt;
}

auto_rf_racks get_auto_rf_racks(const replica::database& db, const topology& topology) {
    auto_rf_racks racks;
    for (const auto& [ks_name, ks] : db.get_keyspaces()) {
        if (is_auto_rf_keyspace(ks_name)) {
            continue;
        }
        auto ks_md = ks->metadata();
        if (!ks_md->uses_tablets()) {
            continue;
        }
        for (const auto& [dc, rf] : ks_md->strategy_options()) {
            auto rf_data = locator::replication_factor_data(rf);
            if (rf_data.is_numeric()) {
                // RF 0 is how a DC is drained; it must not pull the auto-RF keyspaces into it.
                if (rf_data.count() != 0) {
                    // Every rack of the DC.
                    racks.eligible[dc] = std::nullopt;
                    auto& max_rf = racks.max_numeric_rf[dc];
                    max_rf = std::max(max_rf, rf_data.count());
                }
            } else {
                racks.rack_list_dcs.insert(dc);
                auto& entry = racks.eligible.try_emplace(dc, std::set<sstring>{}).first->second;
                if (entry) { // not already every rack of the DC
                    entry->insert(rf_data.get_rack_list().begin(), rf_data.get_rack_list().end());
                }
            }
        }
    }
    const auto& excluded = topology.excluded_tablet_nodes;
    for (const auto& [id, rs] : topology.normal_nodes) {
        if (rs.ring && !rs.ring->tokens.empty() && !excluded.contains(id)) {
            racks.token_owner_racks[rs.datacenter].insert(rs.rack);
            if (is_auto_rf_rack_eligible(racks.eligible, rs.datacenter, rs.rack)) {
                racks.allowed[rs.datacenter].insert(rs.rack);
            }
        }
    }
    const auto& cfg = db.get_config();
    racks.rack_lists_required = cfg.rf_rack_valid_keyspaces() || cfg.enforce_rack_list();
    racks.rack_lists_enabled = bool(db.features().rack_list_rf);
    return racks;
}

auto_rf_dc_mode get_auto_rf_dc_mode(const auto_rf_racks& racks, const sstring& dc) {
    const bool rack_lists = racks.rack_lists_enabled && racks.rack_lists_required;
    if (!racks.eligible.contains(dc)) {
        return rack_lists ? auto_rf_dc_mode::rack_list : auto_rf_dc_mode::keep;
    }
    if (rack_lists || (racks.rack_lists_enabled && racks.rack_list_dcs.contains(dc))) {
        return auto_rf_dc_mode::rack_list;
    }
    return auto_rf_dc_mode::numeric;
}

bool auto_rf_will_give_up_rack(const replica::database& db, const topology& topology, const sstring& dc, const sstring& rack) {
    const auto racks = get_auto_rf_racks(db, topology);
    if (is_auto_rf_rack_eligible(racks.eligible, dc, rack)) {
        return false;
    }
    for (const auto& [ks_name, goal] : auto_rf_keyspaces()) {
        if (!db.has_keyspace(ks_name)) {
            continue;
        }
        auto ks_md = db.find_keyspace(ks_name).metadata();
        if (!ks_md->uses_tablets()) {
            continue;
        }
        const auto& options = ks_md->strategy_options();
        auto it = options.find(dc);
        if (it == options.end()) {
            continue;
        }
        auto rf_data = locator::replication_factor_data(it->second);
        if (!rf_data.is_rack_based()) {
            continue;
        }
        const auto& rack_list = rf_data.get_rack_list();
        if (std::ranges::contains(rack_list, rack) && find_ineligible_auto_rf_rack(racks.eligible, dc, rack_list)) {
            return true;
        }
    }
    return false;
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
