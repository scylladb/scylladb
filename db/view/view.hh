/*
 * Copyright (C) 2016 ScyllaDB
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

#include "service/storage_proxy_stats.hh"
#include "dht/i_partitioner.hh"
#include "gc_clock.hh"
#include "query-request.hh"
#include "schema.hh"
#include "mutation_fragment.hh"
#include "flat_mutation_reader.hh"
#include "stdx.hh"

#include <seastar/core/semaphore.hh>

class frozen_mutation_and_schema;

namespace db {

namespace view {

struct stats : public service::storage_proxy_stats::write_stats {
    int64_t view_updates_pushed_local = 0;
    int64_t view_updates_pushed_remote = 0;
    int64_t view_updates_failed_local = 0;
    int64_t view_updates_failed_remote = 0;

    stats(const sstring& category) : service::storage_proxy_stats::write_stats(category, false) { }
};

/**
 * Whether the view filter considers the specified partition key.
 *
 * @param base the base table schema.
 * @param view_info the view info.
 * @param key the partition key that is updated.
 * @return false if we can guarantee that inserting an update for specified key
 * won't affect the view in any way, true otherwise.
 */
bool partition_key_matches(const schema& base, const view_info& view, const dht::decorated_key& key);

/**
 * Whether the view might be affected by the provided update.
 *
 * Note that having this method return true is not an absolute guarantee that the view will be
 * updated, just that it most likely will, but a false return guarantees it won't be affected.
 *
 * @param base the base table schema.
 * @param view_info the view info.
 * @param key the partition key that is updated.
 * @param update the base table update being applied.
 * @return false if we can guarantee that inserting update for key
 * won't affect the view in any way, true otherwise.
 */
bool may_be_affected_by(const schema& base, const view_info& view, const dht::decorated_key& key, const rows_entry& update);

/**
 * Whether a given base row matches the view filter (and thus if the view should have a corresponding entry).
 *
 * Note that this differs from may_be_affected_by in that the provide row must be the current
 * state of the base row, not just some updates to it. This function also has no false positive: a base
 * row either does or doesn't match the view filter.
 *
 * Also note that this function doesn't check the partition key, as it assumes the upper layers
 * have already filtered out the views that are not affected.
 *
 * @param base the base table schema.
 * @param view_info the view info.
 * @param key the partition key that is updated.
 * @param update the current state of a particular base row.
 * @param now the current time in seconds (to decide what is live and what isn't).
 * @return whether the base row matches the view filter.
 */
bool matches_view_filter(const schema& base, const view_info& view, const partition_key& key, const clustering_row& update, gc_clock::time_point now);

bool clustering_prefix_matches(const schema& base, const partition_key& key, const clustering_key_prefix& ck);

future<std::vector<frozen_mutation_and_schema>> generate_view_updates(
        const schema_ptr& base,
        std::vector<view_ptr>&& views_to_update,
        flat_mutation_reader&& updates,
        flat_mutation_reader_opt&& existings);

query::clustering_row_ranges calculate_affected_clustering_ranges(
        const schema& base,
        const dht::decorated_key& key,
        const mutation_partition& mp,
        const std::vector<view_ptr>& views);

future<> mutate_MV(
        const dht::token& base_token,
        std::vector<frozen_mutation_and_schema> view_updates,
        db::view::stats& stats,
        db::timeout_semaphore_units pending_view_updates);

/**
 * create_virtual_column() adds a "virtual column" to a schema builder.
 * The definition of a "virtual column" is based on the given definition
 * of a regular column, except that any value types are replaced by the
 * empty type - and only the information needed to track column liveness
 * is kept: timestamp, deletion, ttl, and keys in maps.
 * In some cases we add such virtual columns for unselected columns in
 * materialized views, for reasons explained in issue #3362.
 * @param builder the schema_builder where we want to add the virtual column.
 * @param name the name of the virtual column to be created.
 * @param type of the base column for which we want to build a virtual column.
 *        When type is a multi-cell collection, so will be the virtual column.
 */
 void create_virtual_column(schema_builder& builder, const bytes& name, const data_type& type);

}

}
