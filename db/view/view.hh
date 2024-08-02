/*
 * Copyright (C) 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "gc_clock.hh"
#include "query-request.hh"
#include "schema/schema_fwd.hh"
#include "readers/mutation_reader.hh"
#include "mutation/frozen_mutation.hh"
#include "data_dictionary/data_dictionary.hh"
#include "locator/abstract_replication_strategy.hh"

class frozen_mutation_and_schema;

namespace replica {
struct cf_stats;
}

namespace db {

namespace view {

class stats;

// Part of the view description which depends on the base schema version.
//
// This structure may change even though the view schema doesn't change, so
// it needs to live outside view_ptr.
struct base_dependent_view_info {
private:
    schema_ptr _base_schema;
    // Id of a regular base table column included in the view's PK, if any.
    // Scylla views only allow one such column, alternator can have up to two.
    std::vector<column_id> _base_regular_columns_in_view_pk;
    std::vector<column_id> _base_static_columns_in_view_pk;
    // For tracing purposes, if the view is out of sync with its base table
    // and there exists a column which is not in base, its name is stored
    // and added to debug messages.
    std::optional<bytes> _column_missing_in_base = {};
public:
    const std::vector<column_id>& base_regular_columns_in_view_pk() const;
    const std::vector<column_id>& base_static_columns_in_view_pk() const;
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
    base_dependent_view_info(schema_ptr base_schema,
            std::vector<column_id>&& base_regular_columns_in_view_pk,
            std::vector<column_id>&& base_static_columns_in_view_pk);
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

// An immutable representation of a clustering or static row of the base table.
struct clustering_or_static_row {
private:
    std::optional<clustering_key_prefix> _key;
    deletable_row _row;

public:
    explicit clustering_or_static_row(clustering_row&& cr)
            : _key(std::move(cr.key()))
            , _row(std::move(cr).as_deletable_row())
    {}

    explicit clustering_or_static_row(static_row&& sr)
            : _key()
            , _row(row_tombstone(), row_marker(), std::move(sr.cells()))
    {}

    bool is_static_row() const { return !_key.has_value(); }
    bool is_clustering_row() const { return _key.has_value(); }

    const std::optional<clustering_key_prefix>& key() const { return _key; }

    row_tombstone tomb() const { return _row.deleted_at(); }
    const row_marker& marker() const { return _row.marker(); }
    const row& cells() const { return _row.cells(); }

    bool empty() const { return _row.empty(); }
    bool is_live(const schema& s, tombstone base_tombstone = tombstone(), gc_clock::time_point now = gc_clock::time_point::min()) const {
        return _row.is_live(s, column_kind(), base_tombstone, now);
    }

    ::column_kind column_kind() const {
        return _key.has_value()
                ? column_kind::regular_column : column_kind::static_column;
    }

    clustering_row as_clustering_row(const schema& s) const;
    static_row as_static_row(const schema& s) const;
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
bool partition_key_matches(data_dictionary::database, const schema& base, const view_info& view, const dht::decorated_key& key);

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
bool may_be_affected_by(data_dictionary::database, const schema& base, const view_info& view, const dht::decorated_key& key, const rows_entry& update);

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
bool matches_view_filter(data_dictionary::database, const schema& base, const view_info& view, const partition_key& key, const clustering_or_static_row& update, gc_clock::time_point now);

bool clustering_prefix_matches(data_dictionary::database, const schema& base, const partition_key& key, const clustering_key_prefix& ck);

/*
 * When a base-table update modifies a value in a materialized view's key
 * key column, Scylla needs to create a new view row. When indexing a
 * collection - Scylla needs to add multiple almost-identical rows with just
 * a different key. Scylla may also need to take additional "actions" for each
 * of those rows - namely deleting an old row or adding a row marker.
 *
 * So the following struct view_key_and_action holds one such row key and
 * one action. The action can be:
 * 1. "no_action" - Do nothing beyond adding the view row under the given
 *     key. The row's key is given, but its other columns are derived from
 *     the base table's existing row and and the update mutation..
 * 2. a row_marker - also add a CQL row marker, to allow a view row to live
 *    even if there is nothing in it besides the key.
 * 3. a (shadowable) tombstone, to remove and old view row that this one
 *    replaces.
 */
struct view_key_and_action {
    struct no_action {};
    struct shadowable_tombstone_tag {
        api::timestamp_type ts;
        shadowable_tombstone into_shadowable_tombstone(gc_clock::time_point now) const {
            return shadowable_tombstone{ts, now};
        }
    };
    using action = std::variant<no_action, row_marker, shadowable_tombstone_tag>;

    bytes _key_bytes;
    action _action = no_action{};

    view_key_and_action(bytes key_bytes)
        : _key_bytes(std::move(key_bytes))
    {}
    view_key_and_action(bytes key_bytes, action action)
        : _key_bytes(std::move(key_bytes))
        , _action(action)
    {}

};

class view_updates final {
    view_ptr _view;
    const view_info& _view_info;
    schema_ptr _base;
    base_info_ptr _base_info;
    std::unordered_map<partition_key, mutation_partition, partition_key::hashing, partition_key::equality> _updates;
    size_t _op_count = 0;
public:
    explicit view_updates(view_and_base vab)
            : _view(std::move(vab.view))
            , _view_info(*_view->view_info())
            , _base(vab.base->base_schema())
            , _base_info(vab.base)
            , _updates(8, partition_key::hashing(*_view), partition_key::equality(*_view))
    {
    }

    future<> move_to(utils::chunked_vector<frozen_mutation_and_schema>& mutations);

    void generate_update(data_dictionary::database db, const partition_key& base_key, const clustering_or_static_row& update, const std::optional<clustering_or_static_row>& existing, gc_clock::time_point now);
    bool generate_partition_tombstone_update(data_dictionary::database db, const partition_key& base_key, tombstone partition_tomb);

    size_t op_count() const;

    bool is_partition_key_permutation_of_base_partition_key() const;

    std::optional<partition_key> construct_view_partition_key_from_base(const partition_key& base_pk);

private:
    mutation_partition& partition_for(partition_key&& key);
    row_marker compute_row_marker(const clustering_or_static_row& base_row) const;
    struct view_row_entry {
        deletable_row* _row;
        view_key_and_action::action _action;
    };
    std::vector<view_row_entry> get_view_rows(const partition_key& base_key, const clustering_or_static_row& update, const std::optional<clustering_or_static_row>& existing, row_tombstone update_tomb);
    bool can_skip_view_updates(const clustering_or_static_row& update, const clustering_or_static_row& existing) const;
    void create_entry(data_dictionary::database db, const partition_key& base_key, const clustering_or_static_row& update, gc_clock::time_point now);
    void delete_old_entry(data_dictionary::database db, const partition_key& base_key, const clustering_or_static_row& existing, const clustering_or_static_row& update, gc_clock::time_point now);
    void do_delete_old_entry(const partition_key& base_key, const clustering_or_static_row& existing, const clustering_or_static_row& update, gc_clock::time_point now);
    void update_entry(data_dictionary::database db, const partition_key& base_key, const clustering_or_static_row& update, const clustering_or_static_row& existing, gc_clock::time_point now);
    void update_entry_for_computed_column(const partition_key& base_key, const clustering_or_static_row& update, const std::optional<clustering_or_static_row>& existing, gc_clock::time_point now);
};

class view_update_builder {
    data_dictionary::database _db;
    const replica::table& _base;
    schema_ptr _schema; // The base schema
    std::vector<view_updates> _view_updates;
    mutation_reader _updates;
    mutation_reader_opt _existings;
    tombstone _update_partition_tombstone;
    tombstone _update_current_tombstone;
    tombstone _existing_partition_tombstone;
    tombstone _existing_current_tombstone;
    mutation_fragment_v2_opt _update;
    mutation_fragment_v2_opt _existing;
    gc_clock::time_point _now;
    partition_key _key = partition_key::make_empty();
    bool _skip_row_updates = false;
public:

    view_update_builder(data_dictionary::database db, const replica::table& base, schema_ptr s,
        std::vector<view_updates>&& views_to_update,
        mutation_reader&& updates,
        mutation_reader_opt&& existings,
        gc_clock::time_point now)
            : _db(std::move(db))
            , _base(base)
            , _schema(std::move(s))
            , _view_updates(std::move(views_to_update))
            , _updates(std::move(updates))
            , _existings(std::move(existings))
            , _now(now) {
    }
    view_update_builder(view_update_builder&& other) noexcept = default;


    // build_some() works on batches of 100 (max_rows_for_view_updates)
    // updated rows, but can_skip_view_updates() can decide that some of
    // these rows do not effect the view, and as a result build_some() can
    // fewer than 100 rows - in extreme cases even zero (see issue #12297).
    // So we can't use an empty returned vector to signify that the view
    // update building is done - and we wrap the return value in an
    // std::optional, which is disengaged when the iteration is done.
    future<std::optional<utils::chunked_vector<frozen_mutation_and_schema>>> build_some();

    future<> close() noexcept;

private:
    void generate_update(clustering_row&& update, std::optional<clustering_row>&& existing);
    void generate_update(static_row&& update, const tombstone& update_tomb, std::optional<static_row>&& existing, const tombstone& existing_tomb);
    future<stop_iteration> on_results();

    future<stop_iteration> advance_all();
    future<stop_iteration> advance_updates();
    future<stop_iteration> advance_existings();

    future<stop_iteration> stop() const;
};

view_update_builder make_view_update_builder(
        data_dictionary::database db,
        const replica::table& base_table,
        const schema_ptr& base_schema,
        std::vector<view_and_base>&& views_to_update,
        mutation_reader&& updates,
        mutation_reader_opt&& existings,
        gc_clock::time_point now);

future<query::clustering_row_ranges> calculate_affected_clustering_ranges(
        data_dictionary::database db,
        const schema& base,
        const dht::decorated_key& key,
        const mutation_partition& mp,
        const std::vector<view_and_base>& views);

bool needs_static_row(const mutation_partition& mp, const std::vector<view_and_base>& views);

// Whether this node and shard should generate and send view updates for the given token.
// Checks that the node is one of the replicas (not a pending replicas), and is ready for reads.
bool should_generate_view_updates_on_this_shard(const schema_ptr& base, const locator::effective_replication_map_ptr& ermp, dht::token token);

size_t memory_usage_of(const frozen_mutation_and_schema& mut);

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
