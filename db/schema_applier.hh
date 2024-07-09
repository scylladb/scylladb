/*
 * Modified by ScyllaDB
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "db/schema_tables.hh"
#include "mutation/mutation.hh"
#include "seastar/core/future.hh"
#include "service/storage_proxy.hh"
#include "query-result-set.hh"

#include <seastar/core/distributed.hh>

namespace db {

namespace schema_tables {

// Must be called on shard 0.
future<semaphore_units<>> hold_merge_lock() noexcept;

future<> merge_schema(sharded<db::system_keyspace>& sys_ks, distributed<service::storage_proxy>& proxy, gms::feature_service& feat, std::vector<mutation> mutations, bool reload = false);

// Recalculates the local schema version.
//
// It is safe to call concurrently with recalculate_schema_version() and merge_schema() in which case it
// is guaranteed that the schema version we end up with after all calls will reflect the most recent state
// of feature_service and schema tables.
future<> recalculate_schema_version(sharded<db::system_keyspace>& sys_ks, distributed<service::storage_proxy>& proxy, gms::feature_service& feat);

}

}
