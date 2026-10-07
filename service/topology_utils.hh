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

// Whether a keyspace_rf_change for `ks` is queued, paused, being processed or
// has migrations in flight. The guard says that the caller holds group0 and
// so sees a stable topology.
seastar::future<bool> ongoing_rf_change(const topology& topology, db::system_keyspace& sys_ks, const group0_guard& guard, seastar::sstring ks);

// The same check without the guard, for callers which deliberately let the
// coordinator make progress while they look (a request may disappear between
// the id being read and its entry being looked up; that counts as not ongoing).
seastar::future<bool> ongoing_rf_change_unguarded(const topology& topology, db::system_keyspace& sys_ks, seastar::sstring ks);

// Whether some auto-RF keyspace has a keyspace_rf_change queued or in flight.
seastar::future<bool> auto_rf_change_ongoing(const topology& topology, db::system_keyspace& sys_ks, const group0_guard& guard);
seastar::future<bool> auto_rf_change_ongoing_unguarded(const topology& topology, db::system_keyspace& sys_ks);

}
