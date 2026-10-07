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

seastar::future<bool> ongoing_rf_change(const topology& topology, db::system_keyspace& sys_ks, const group0_guard& guard, seastar::sstring ks);

}
