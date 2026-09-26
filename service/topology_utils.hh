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
#include <utility>
#include <vector>

namespace db {
class system_keyspace;
}

namespace service {

struct topology;
class group0_guard;

// The keyspaces whose replication the topology coordinator manages (auto-RF), each
// with the number of racks per DC it aims for.
const std::vector<std::pair<seastar::sstring, size_t>>& auto_rf_keyspaces();

bool is_auto_rf_keyspace(std::string_view ks_name);

seastar::future<bool> ongoing_rf_change(const topology& topology, db::system_keyspace& sys_ks, const group0_guard& guard, seastar::sstring ks);

// Whether some auto-RF keyspace has a keyspace_rf_change queued or in flight.
seastar::future<bool> auto_rf_change_ongoing(const topology& topology, db::system_keyspace& sys_ks, const group0_guard& guard);

}
