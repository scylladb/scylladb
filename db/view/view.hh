/*
 * Copyright (C) 2016-present ScyllaDB
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

#include "view_stats.hh"
#include "dht/i_partitioner.hh"
#include "gc_clock.hh"
#include "query-request.hh"
#include "schema_fwd.hh"
#include "mutation_fragment.hh"
#include "flat_mutation_reader.hh"

#include <seastar/core/semaphore.hh>

class frozen_mutation_and_schema;
struct cf_stats;

namespace service {
struct allow_hints_tag;
using allow_hints = bool_class<allow_hints_tag>;
}

namespace db {

namespace view {

// Part of the view description which depends on the base schema version.
//
// This structure may change even though the view schema doesn't change, so
// it needs to live outside view_ptr.
struct base_dependent_view_info {
private:
    schema_ptr _base_schema;
    // Id of a regular base table column included in the view's PK, if any.
    // Scylla views only allow one such column, alternator can have up to two.
    std::vector<column_id> _base_non_pk_columns_in_view_pk;
    // For tracing purposes, if the view is out of sync with its base table
    // and there exists a column which is not in base, its name is stored
    // and added to debug messages.
    std::optional<bytes> _column_missing_in_base = {};
public:
    const std::vector<column_id>& base_non_pk_columns_in_view_pk() const;
    const schema_ptr& base_schema() const;

    // Indicates if the view hase pk columns which are not part of the base
    // pk, it seems that !base_non_pk_columns_in_view_pk.empty() is the same,
    // but actually there are cases where we can compute this boolean without
    // succeeding to reliably build the former.
    const bool has_base_non_pk_columns_in_view_pk;

    // If base_non_pk_columns_in_view_pk couldn't reliably be built, this base
    // info can't be used for computing view updates, only for reading the materialized
    // view.
    const bool use_only_for_reads;

    // A constructor for a base info that can facilitate reads and writes from the materialized view.
    base_dependent_view_info(schema_ptr base_schema, std::vector<column_id>&& base_non_pk_columns_in_view_pk);
    // A constructor for a base info that can facilitate only reads from the materialized view.
    base_dependent_view_info(bool has_base_non_pk_columns_in_view_pk, std::optional<bytes>&& column_missing_in_base);
};

// Immutable snapshot of view's base-schema-dependent part.
using base_info_ptr = lw_shared_ptr<const base_dependent_view_info>;

// Snapshot of the view schema and its base-schema-dependent part.
struct view_and_base {
    view_ptr view;
    base_info_ptr base;
};

/**
 * Whether the view filter considers the specified partition key.
 *
 * @param base the base table schema.
 * @param view_info the view info.
 * @param key the partition key that is updated.
 * @param time_point time of the operation (for handling liveness: TTL, tombstones, etc).
 * @return false if we can guarantee that inserting an update for specified key
 * won't affect the view in any way, true otherwise.
 */
bool partition_key_matches(const schema& base, const view_info& view, const dht::decorated_key& key, gc_clock::time_point now);

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
 * @param time_point time of the operation (for handling liveness: TTL, tombstones, etc).
 * @return false if we can guarantee that inserting update for key
 * won't affect the view in any way, true otherwise.
 */
bool may_be_affected_by(const schema& base, const view_info& view, const dht::decorated_key& key, const rows_entry& update, gc_clock::time_point now);

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

bool clustering_prefix_matches(const schema& base, const partition_key& key, const clustering_key_prefix& ck, gc_clock::time_point now);

future<std::vector<frozen_mutation_and_schema>> generate_view_updates(
        const schema_ptr& base,
        std::vector<view_and_base>&& views_to_update,
        flat_mutation_reader&& updates,
        flat_mutation_reader_opt&& existings,
        gc_clock::time_point now);

query::clustering_row_ranges calculate_affected_clustering_ranges(
        const schema& base,
        const dht::decorated_key& key,
        const mutation_partition& mp,
        const std::vector<view_and_base>& views,
        gc_clock::time_point now);

struct wait_for_all_updates_tag {};
using wait_for_all_updates = bool_class<wait_for_all_updates_tag>;
future<> mutate_MV(
        const dht::token& base_token,
        std::vector<frozen_mutation_and_schema> view_updates,
        db::view::stats& stats,
        cf_stats& cf_stats,
        tracing::trace_state_ptr tr_state,
        db::timeout_semaphore_units pending_view_updates,
        service::allow_hints allow_hints,
        wait_for_all_updates wait_for_all);

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

/**
 * Converts a collection of view schema snapshots into a collection of
 * view_and_base objects, which are snapshots of both the view schema
 * and the base-schema-dependent part of view description.
 */
std::vector<view_and_base> with_base_info_snapshot(std::vector<view_ptr>);

}

}
