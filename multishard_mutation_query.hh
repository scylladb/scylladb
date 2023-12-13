/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "replica/database_fwd.hh"
#include "schema/schema_fwd.hh"
#include "cache_temperature.hh"
#include "db/timeout_clock.hh"
#include "dht/i_partitioner_fwd.hh"

#include <seastar/core/distributed.hh>

#include "seastarx.hh"

class reconcilable_result;

namespace query {

class read_command;
class result;
class result_options;

} // namespace query

namespace tracing {
    class trace_state_ptr;
} // namespace tracing

/// Run the mutation query on all shards.
///
/// Under the hood it uses a multishard_combining_reader for reading the
/// range(s) from all shards.
///
/// The query uses paging. The read will stop after reaching one of the page
/// size limits. Page size is determined by the read_command (row and partition
/// limits) and by the max_size parameter (max memory size of results).
///
/// Optionally the query can be stateful. This means that after filling the
/// page, the shard readers are saved in the `querier_cache` on their home shard
/// (wrapped in a `shard_mutation_querier`). Fragments already read from
/// the shard readers, but not consumed by the results builder (due to
/// reaching the limit), are extracted from the `multishard_combining_reader`'s
/// (and the foreign readers wrapping the shard readers) buffers and pushed back
/// into the shard reader they originated from. This way only the shard readers
/// have to be cached in order to continue the query.
/// When reading the next page these querier objects are looked up from
/// their respective shard's `querier_cache`, instead of creating new shard
/// readers.
/// To enable stateful queries set the `query_uuid` field of the read command
/// to an id unique to the query. This can be easily achieved by generating a
/// random uuid with `utils::make_random_uuid()`.
/// It is advisable that the `is_first_page` flag of the read command is set on
/// the first page of the query so that a pointless lookup is avoided.
///
/// Note: params passed by reference are expected to be kept alive by the caller
/// for the duration of the query. Params passed by const reference are expected
/// to *not* change during the query, as they will possibly be accessed from
/// other shards.
///
/// \see multishard_combined_reader
/// \see querier_cache
future<std::tuple<foreign_ptr<lw_shared_ptr<reconcilable_result>>, cache_temperature>> query_mutations_on_all_shards(
        distributed<replica::database>& db,
        schema_ptr s,
        const query::read_command& cmd,
        const dht::partition_range_vector& ranges,
        tracing::trace_state_ptr trace_state,
        db::timeout_clock::time_point timeout);

/// Run the data query on all shards.
///
/// Identical to `query_mutations_on_all_shards()` except that it builds results
/// in the `query::result` format instead of in the `reconcilable_result` one.
future<std::tuple<foreign_ptr<lw_shared_ptr<query::result>>, cache_temperature>> query_data_on_all_shards(
        distributed<replica::database>& db,
        schema_ptr s,
        const query::read_command& cmd,
        const dht::partition_range_vector& ranges,
        query::result_options opts,
        tracing::trace_state_ptr trace_state,
        db::timeout_clock::time_point timeout);
