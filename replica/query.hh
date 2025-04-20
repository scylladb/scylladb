/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

/*
 * Utilities for executing queries on the local replica.
 *
 * Allows for bypassing storage proxy entirely when querying local (system) tables.
 */

#include "seastarx.hh"

#include "dht/i_partitioner_fwd.hh"
#include "db/timeout_clock.hh"
#include "schema/schema_fwd.hh"
#include "query-result.hh"

#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sharded.hh>

class reconcilable_result;


namespace query {
class partition_slice;
}

namespace replica {

class database;

/// Reads the specified range and slice of the given table, from the local replica.
///
/// There is no paging or limits applied to the result, make sure the result is
/// sufficiently small.
future<foreign_ptr<lw_shared_ptr<reconcilable_result>>> query_mutations(
        sharded<database>& db,
        schema_ptr s,
        const dht::partition_range& pr,
        const query::partition_slice& ps,
        db::timeout_clock::time_point timeout);

/// Reads the specified range and slice of the given table, from the local replica.
///
/// A variant of query_mutations() which returns query result, instead of mutations (only live data).
future<foreign_ptr<lw_shared_ptr<query::result>>> query_data(
        sharded<database>& db,
        schema_ptr s,
        const dht::partition_range& pr,
        const query::partition_slice& ps,
        db::timeout_clock::time_point timeout);

} // namespace replica
