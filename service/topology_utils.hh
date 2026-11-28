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

#include <string_view>
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

// A keyspace whose replication the topology coordinator manages (auto-RF).
struct auto_rf_keyspace {
    seastar::sstring name;
    // The number of racks per DC the keyspace aims for.
    unsigned racks_per_dc;
};

const std::vector<auto_rf_keyspace>& auto_rf_keyspaces();

bool is_auto_rf_keyspace(std::string_view ks_name);

// The auto-RF keyspaces on tablets which list `rack` of `dc`.
std::vector<seastar::sstring> auto_rf_keyspaces_using_rack(const replica::database& db, const seastar::sstring& dc, const seastar::sstring& rack);

seastar::future<bool> ongoing_rf_change(const topology& topology, db::system_keyspace& sys_ks, const group0_guard& guard, seastar::sstring ks);

// Whether some auto-RF keyspace has a keyspace_rf_change queued or in flight.
seastar::future<bool> auto_rf_change_ongoing(const topology& topology, db::system_keyspace& sys_ks, const group0_guard& guard);

}
