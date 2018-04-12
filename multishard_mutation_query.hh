/*
 * Copyright (C) 2018 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "database.hh"

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
/// to an id unique to the query. This can be easily achived by generating a
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
future<foreign_ptr<lw_shared_ptr<reconcilable_result>>, cache_temperature> query_mutations_on_all_shards(
        distributed<database>& db,
        schema_ptr s,
        const query::read_command& cmd,
        const dht::partition_range_vector& ranges,
        tracing::trace_state_ptr trace_state,
        uint64_t max_size,
        db::timeout_clock::time_point timeout = db::no_timeout);
