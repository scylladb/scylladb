/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

#include "seastarx.hh"

namespace replica { class database; }
namespace db { class system_keyspace; }
namespace gms { class feature_service; }

namespace service {

struct topology;

// Validates that the materialized views of a keyspace can be migrated from
// vnodes to tablets.
//
// Views are migrated as tables co-located with their base table, sharing the
// base table's tablet map, so each view must be eligible for co-location
// (its partition key must consist of exactly the base table's partition key
// columns, in the same order). Additionally:
// - the 'repair' tombstone_gc mode is not supported on co-located tables,
//   so views must not use it;
// - views must be fully built on all normal nodes. The tablet-based view
//   building bookkeeping considers a view built only if all its entries in
//   system.view_build_status_v2 are SUCCESS, and there is no handoff of
//   in-progress builds from the node-local view builder to the view
//   building coordinator.
//
// Throws std::runtime_error if any view cannot be migrated.
future<> validate_keyspace_views_for_tablets_migration(
        replica::database& db,
        db::system_keyspace& sys_ks,
        const gms::feature_service& features,
        const topology& topology,
        const sstring& ks_name);

}
