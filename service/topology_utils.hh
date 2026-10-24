/*
 * Copyright (C) 2026-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

#include <chrono>
#include <optional>
#include <set>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace db {
class system_keyspace;
}

namespace replica {
class database;
}

namespace service {

struct topology;
class group0_guard;

// The keyspaces whose replication the topology coordinator manages (auto-RF), each
// with the number of racks per DC it aims for.
const std::vector<std::pair<seastar::sstring, size_t>>& auto_rf_keyspaces();

bool is_auto_rf_keyspace(std::string_view ks_name);

// The names of auto_rf_keyspaces().
const std::unordered_set<seastar::sstring>& auto_rf_keyspace_names();

// For each DC, the racks in which some non-auto-RF tablets keyspace places
// replicas. nullopt means every rack of the DC (a numeric RF was found).
// A DC missing from the map has no such keyspace.
using auto_rf_eligible_racks = std::unordered_map<seastar::sstring, std::optional<std::set<seastar::sstring>>>;

bool is_auto_rf_rack_eligible(const auto_rf_eligible_racks& eligible, const seastar::sstring& dc, const seastar::sstring& rack);

// The first rack of `rack_list` which no non-auto-RF tablets keyspace uses any more.
// A DC's last rack is never given up, nor any rack while no eligible keyspace exists
// (nothing then says where the operator wants the system keyspaces).
std::optional<seastar::sstring> find_ineligible_auto_rf_rack(const auto_rf_eligible_racks& eligible, const seastar::sstring& dc,
        const std::vector<seastar::sstring>& rack_list);

// Where the auto-RF keyspaces may replicate, computed once per reconciler pass.
struct auto_rf_racks {
    // The racks the auto-RF keyspaces follow; a rack outside this set is given up.
    auto_rf_eligible_racks eligible;
    // The eligible racks with a token-owning, non-excluded normal node: the racks
    // an auto-RF keyspace may be extended into. Always a subset of `eligible`.
    std::unordered_map<seastar::sstring, std::set<seastar::sstring>> allowed;
    // The racks with a token-owning, non-excluded normal node, eligible or not.
    std::unordered_map<seastar::sstring, std::set<seastar::sstring>> token_owner_racks;
    // The largest numeric RF of a non-auto-RF tablets keyspace in each DC; a DC
    // missing from the map has none.
    std::unordered_map<seastar::sstring, size_t> max_numeric_rf;
    // The DCs in which some non-auto-RF tablets keyspace uses a rack list.
    std::unordered_set<seastar::sstring> rack_list_dcs;
    // rf_rack_valid_keyspaces or enforce_rack_list is set.
    bool rack_lists_required = false;
    // The RACK_LIST_RF cluster feature is enabled.
    bool rack_lists_enabled = false;
};

// The form of an auto-RF keyspace's replication in one DC.
enum class auto_rf_dc_mode {
    // A numeric RF, at most the largest numeric RF of the other tablets keyspaces in
    // the DC. Used where they all use numeric RFs and rack lists are not required: a
    // numeric RF names no rack, so it never keeps a rack from being removed which
    // those keyspaces could leave.
    numeric,
    // A rack list following the racks of the other tablets keyspaces.
    rack_list,
    // No other tablets keyspace uses the DC and rack lists are not required: the
    // keyspace keeps its form, shrunk to a single replica.
    keep,
};

auto_rf_dc_mode get_auto_rf_dc_mode(const auto_rf_racks& racks, const seastar::sstring& dc);

auto_rf_racks get_auto_rf_racks(const replica::database& db, const topology& topology);

// Whether the reconciler's rules say that it gives `rack` of `dc` up in some auto-RF
// keyspace: the keyspace lists the rack, no non-auto-RF tablets keyspace uses it any
// more, and it is not the keyspace's last rack in the DC.
bool auto_rf_will_give_up_rack(const replica::database& db, const topology& topology, const seastar::sstring& dc, const seastar::sstring& rack);

// The longest the topology coordinator waits before retrying an auto-RF change which
// was rejected or failed.
inline constexpr std::chrono::seconds auto_rf_backoff_max{60};

// The answer to a quiesce topology request while an auto-RF change is due but held
// back by that backoff.
inline constexpr std::string_view auto_rf_backed_off_error = "auto-RF change backed off";

// Whether a keyspace_rf_change for `ks` is queued, paused, being processed or
// has migrations in flight. The guard says that the caller holds group0 and
// so sees a stable topology.
seastar::future<bool> ongoing_rf_change(const topology& topology, db::system_keyspace& sys_ks, const group0_guard& guard, seastar::sstring ks);

// Whether some auto-RF keyspace has a keyspace_rf_change queued or in flight.
seastar::future<bool> auto_rf_change_ongoing(const topology& topology, db::system_keyspace& sys_ks, const group0_guard& guard);

}
