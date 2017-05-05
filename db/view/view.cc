/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright (C) 2017 ScyllaDB
 *
 * Modified by ScyllaDB
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

#include <vector>
#include <functional>

#include <boost/range/algorithm/transform.hpp>
#include <boost/range/adaptors.hpp>

#include "clustering_bounds_comparator.hh"
#include "cql3/statements/select_statement.hh"
#include "cql3/util.hh"
#include "db/view/view.hh"
#include "gms/inet_address.hh"
#include "keys.hh"
#include "locator/network_topology_strategy.hh"
#include "service/storage_service.hh"
#include "view_info.hh"

static logging::logger logger("view");

view_info::view_info(const schema& schema, const raw_view_info& raw_view_info)
        : _schema(schema)
        , _raw(raw_view_info)
{ }

cql3::statements::select_statement& view_info::select_statement() const {
    if (!_select_statement) {
        std::vector<sstring_view> included;
        if (!include_all_columns()) {
            included.reserve(_schema.all_columns().size());
            boost::transform(_schema.all_columns(), std::back_inserter(included), std::mem_fn(&column_definition::name_as_text));
        }
        auto raw = cql3::util::build_select_statement(base_name(), where_clause(), std::move(included));
        raw->prepare_keyspace(_schema.ks_name());
        raw->set_bound_variables({});
        cql3::cql_stats ignored;
        auto prepared = raw->prepare(service::get_local_storage_proxy().get_db().local(), ignored, true);
        _select_statement = static_pointer_cast<cql3::statements::select_statement>(prepared->statement);
    }
    return *_select_statement;
}

const query::partition_slice& view_info::partition_slice() const {
    if (!_partition_slice) {
        _partition_slice = select_statement().make_partition_slice(cql3::query_options({ }));
    }
    return *_partition_slice;
}

const dht::partition_range_vector& view_info::partition_ranges() const {
    if (!_partition_ranges) {
        _partition_ranges = select_statement().get_restrictions()->get_partition_key_ranges(cql3::query_options({ }));
    }
    return *_partition_ranges;
}

const column_definition* view_info::view_column(const schema& base, column_id base_id) const {
    // FIXME: Map base column_ids to view_column_ids, which can be something like
    // a boost::small_vector where the position is the base column_id, and the
    // value is either empty or the view's column_id.
    return _schema.get_column_definition(base.regular_column_at(base_id).name());
}

stdx::optional<column_id> view_info::base_non_pk_column_in_view_pk(const schema& base) const {
    if (!_base_non_pk_column_in_view_pk) {
        _base_non_pk_column_in_view_pk.emplace(stdx::nullopt);
        for (auto&& view_col : boost::range::join(_schema.partition_key_columns(), _schema.clustering_key_columns())) {
            auto* base_col = base.get_column_definition(view_col.name());
            if (!base_col->is_primary_key()) {
                _base_non_pk_column_in_view_pk.emplace(base_col->id);
                break;
            }
        }
    }
    return *_base_non_pk_column_in_view_pk;
}

namespace db {

namespace view {

bool partition_key_matches(const schema& base, const view_info& view, const dht::decorated_key& key) {
    dht::ring_position rp(key);
    auto& ranges = view.partition_ranges();
    return std::any_of(ranges.begin(), ranges.end(), [&] (auto&& range) {
        return range.contains(rp, dht::ring_position_comparator(base));
    });
}

bool clustering_prefix_matches(const schema& base, const view_info& view, const partition_key& key, const clustering_key_prefix& ck) {
    bound_view::compare less(base);
    auto& ranges = view.partition_slice().row_ranges(base, key);
    return std::any_of(ranges.begin(), ranges.end(), [&] (auto&& range) {
        auto bounds = bound_view::from_range(range);
        return !less(ck, bounds.first) && !less(bounds.second, ck);
    });
}

bool may_be_affected_by(const schema& base, const view_info& view, const dht::decorated_key& key, const rows_entry& update) {
    // We can guarantee that the view won't be affected if:
    //  - the primary key is excluded by the view filter (note that this isn't true of the filter on regular columns:
    //    even if an update don't match a view condition on a regular column, that update can still invalidate a
    //    pre-existing entry) - note that the upper layers should already have checked the partition key;
    //  - the update doesn't modify any of the columns impacting the view (where "impacting" the view means that column
    //    is neither included in the view, nor used by the view filter).
    if (!clustering_prefix_matches(base, view, key.key(), update.key())) {
        return false;
    }

    // We want to check if the update modifies any of the columns that are part of the view (in which case the view is
    // affected). But iff the view includes all the base table columns, or the update has either a row deletion or a
    // row marker, we know the view is affected right away.
    if (view.include_all_columns() || update.row().deleted_at() || update.row().marker().is_live()) {
        return true;
    }

    bool affected = false;
    update.row().cells().for_each_cell_until([&] (column_id id, const atomic_cell_or_collection& cell) {
        affected = view.view_column(base, id);
        return stop_iteration(affected);
    });
    return affected;
}

static bool update_requires_read_before_write(const schema& base,
        const std::vector<view_ptr>& views,
        const dht::decorated_key& key,
        const rows_entry& update) {
    for (auto&& v : views) {
        view_info& vf = *v->view_info();
        // A view whose primary key contains only the base's primary key columns doesn't require a read-before-write.
        // However, if the view has restrictions on regular columns, then a write that doesn't match those filters
        // needs to add a tombstone (assuming a previous update matched those filter and created a view entry); for
        // now we just do a read-before-write in that case.
        if (!vf.base_non_pk_column_in_view_pk(base)
                && vf.select_statement().get_restrictions()->get_non_pk_restriction().empty()) {
            continue;
        }
        if (may_be_affected_by(base, vf, key, update)) {
            return true;
        }
    }
    return false;
}

bool matches_view_filter(const schema& base, const view_info& view, const partition_key& key, const clustering_row& update, gc_clock::time_point now) {
    return clustering_prefix_matches(base, view, key, update.key())
            && boost::algorithm::all_of(
                view.select_statement().get_restrictions()->get_non_pk_restriction() | boost::adaptors::map_values,
                [&] (auto&& r) {
                    return r->is_satisfied_by(base, key, update.key(), update.cells(), cql3::query_options({ }), now);
                });
}

class view_updates final {
    view_ptr _view;
    const view_info& _view_info;
    schema_ptr _base;
    std::unordered_map<partition_key, mutation_partition, partition_key::hashing, partition_key::equality> _updates;
public:
    explicit view_updates(view_ptr view, schema_ptr base)
            : _view(std::move(view))
            , _view_info(*_view->view_info())
            , _base(std::move(base))
            , _updates(8, partition_key::hashing(*_base), partition_key::equality(*_base)) {
    }

    void move_to(std::vector<mutation>& mutations) && {
        auto& partitioner = dht::global_partitioner();
        std::transform(_updates.begin(), _updates.end(), std::back_inserter(mutations), [&, this] (auto&& m) {
            return mutation(_view, partitioner.decorate_key(*_base, std::move(m.first)), std::move(m.second));
        });
    }

    void generate_update(const partition_key& base_key, const clustering_row& update, const stdx::optional<clustering_row>& existing, gc_clock::time_point now);
private:
    mutation_partition& partition_for(partition_key&& key) {
        auto it = _updates.find(key);
        if (it != _updates.end()) {
            return it->second;
        }
        return _updates.emplace(std::move(key), mutation_partition(_view)).first->second;
    }
    row_marker compute_row_marker(const clustering_row& base_row) const;
    deletable_row& get_view_row(const partition_key& base_key, const clustering_row& update);
    void create_entry(const partition_key& base_key, const clustering_row& update, gc_clock::time_point now);
    void delete_old_entry(const partition_key& base_key, const clustering_row& existing, gc_clock::time_point now);
    void do_delete_old_entry(const partition_key& base_key, const clustering_row& existing, gc_clock::time_point now);
    void update_entry(const partition_key& base_key, const clustering_row& update, const clustering_row& existing, gc_clock::time_point now);
    void replace_entry(const partition_key& base_key, const clustering_row& update, const clustering_row& existing, gc_clock::time_point now) {
        create_entry(base_key, update, now);
        delete_old_entry(base_key, existing, now);
    }
};

row_marker view_updates::compute_row_marker(const clustering_row& base_row) const {
    /*
     * We need to compute both the timestamp and expiration.
     *
     * For the timestamp, it makes sense to use the bigger timestamp for all view PK columns.
     *
     * This is more complex for the expiration. We want to maintain consistency between the base and the view, so the
     * entry should only exist as long as the base row exists _and_ has non-null values for all the columns that are part
     * of the view PK.
     * Which means we really have 2 cases:
     *   1) There is a column that is not in the base PK but is in the view PK. In that case, as long as that column
     *      lives, the view entry does too, but as soon as it expires (or is deleted for that matter) the entry also
     *      should expire. So the expiration for the view is the one of that column, regardless of any other expiration.
     *      To take an example of that case, if you have:
     *        CREATE TABLE t (a int, b int, c int, PRIMARY KEY (a, b))
     *        CREATE MATERIALIZED VIEW mv AS SELECT * FROM t WHERE c IS NOT NULL AND a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (c, a, b)
     *        INSERT INTO t(a, b) VALUES (0, 0) USING TTL 3;
     *        UPDATE t SET c = 0 WHERE a = 0 AND b = 0;
     *      then even after 3 seconds elapsed, the row will still exist (it just won't have a "row marker" anymore) and so
     *      the MV should still have a corresponding entry.
     *   2) The columns for the base and view PKs are exactly the same. In that case, the view entry should live
     *      as long as the base row lives. This means the view entry should only expire once *everything* in the
     *      base row has expired. So, the row TTL should be the max of any other TTL. This is particularly important
     *      in the case where the base row has a TTL, but a column *absent* from the view holds a greater TTL.
     */

    auto marker = base_row.marker();
    auto col_id = _view_info.base_non_pk_column_in_view_pk(*_base);
    if (col_id) {
        // Note: multi-cell columns can't be part of the primary key.
        auto cell = base_row.cells().cell_at(*col_id).as_atomic_cell();
        auto timestamp = std::max(marker.timestamp(), cell.timestamp());
        return cell.is_live_and_has_ttl() ? row_marker(timestamp, cell.ttl(), cell.expiry()) : row_marker(timestamp);
    }

    if (!marker.is_expiring()) {
        return marker;
    }

    auto ttl = marker.ttl();
    auto expiry = marker.expiry();
    auto maybe_update_expiry_and_ttl = [&] (atomic_cell_view&& cell) {
        // Note: Cassandra compares cell.ttl() here, but that seems very wrong.
        // See CASSANDRA-13127.
        if (cell.is_live_and_has_ttl() && cell.expiry() > expiry) {
            expiry = cell.expiry();
            ttl = cell.ttl();
        }
    };

    base_row.cells().for_each_cell([&] (column_id id, const atomic_cell_or_collection& c) {
        auto& def = _base->regular_column_at(id);
        if (def.is_atomic()) {
            maybe_update_expiry_and_ttl(c.as_atomic_cell());
        } else {
            static_pointer_cast<const collection_type_impl>(def.type)->for_each_cell(c.as_collection_mutation(), maybe_update_expiry_and_ttl);
        }
    });

    return row_marker(marker.timestamp(), ttl, expiry);
}

deletable_row& view_updates::get_view_row(const partition_key& base_key, const clustering_row& update) {
    auto get_value = boost::adaptors::transformed([&, this] (const column_definition& cdef) {
        auto* base_col = _base->get_column_definition(cdef.name());
        assert(base_col);
        switch (base_col->kind) {
        case column_kind::partition_key:
            return base_key.get_component(*_base, base_col->position());
        case column_kind::clustering_key:
            return update.key().get_component(*_base, base_col->position());
        default:
            auto& c = update.cells().cell_at(base_col->id);
            if (base_col->is_atomic()) {
                return c.as_atomic_cell().value();
            }
            return c.as_collection_mutation().data;
        }
    });
    auto& partition = partition_for(partition_key::from_range(_view->partition_key_columns() | get_value));
    auto ckey = clustering_key::from_range(_view->clustering_key_columns() | get_value);
    return partition.clustered_row(*_view, std::move(ckey));
}

static const column_definition* view_column(const schema& base, const schema& view, column_id base_id) {
    // FIXME: Map base column_ids to view_column_ids, which can be something like
    // a boost::small_vector where the position is the base column_id, and the
    // value is either empty or the view's column_id.
    return view.get_column_definition(base.regular_column_at(base_id).name());
}

static void add_cells_to_view(const schema& base, const schema& view, const row& base_cells, row& view_cells) {
    base_cells.for_each_cell([&] (column_id id, const atomic_cell_or_collection& c) {
        auto* view_col = view_column(base, view, id);
        if (view_col && !view_col->is_primary_key()) {
            view_cells.append_cell(view_col->id, c);
        }
    });
}

/**
 * Creates a view entry corresponding to the provided base row.
 * This method checks that the base row does match the view filter before applying anything.
 */
void view_updates::create_entry(const partition_key& base_key, const clustering_row& update, gc_clock::time_point now) {
    if (!matches_view_filter(*_base, _view_info, base_key, update, now)) {
        return;
    }
    deletable_row& r = get_view_row(base_key, update);
    r.apply(compute_row_marker(update));
    r.apply(update.tomb());
    add_cells_to_view(*_base, *_view, update.cells(), r.cells());
}

/**
 * Deletes the view entry corresponding to the provided base row.
 * This method checks that the base row does match the view filter before bothering.
 */
void view_updates::delete_old_entry(const partition_key& base_key, const clustering_row& existing, gc_clock::time_point now) {
    // Before deleting an old entry, make sure it was matching the view filter
    // (otherwise there is nothing to delete)
    if (matches_view_filter(*_base, _view_info, base_key, existing, now)) {
        do_delete_old_entry(base_key, existing, now);
    }
}

void view_updates::do_delete_old_entry(const partition_key& base_key, const clustering_row& existing, gc_clock::time_point now) {
    // We delete the old row using a shadowable row tombstone, making sure that
    // the tombstone deletes everything in the row (or it might still show up).
    // FIXME: If the entry is "resurrected" by a later update, we would need to
    // ensure that the timestamp for the entry then is bigger than the tombstone
    // we're just inserting, which is not currently guaranteed. See CASSANDRA-11500
    // for details.
    auto ts = existing.marker().timestamp();
    auto set_max_ts = [&ts] (atomic_cell_view&& cell) {
        ts = std::max(ts, cell.timestamp());
    };
    existing.cells().for_each_cell([&, this] (column_id id, const atomic_cell_or_collection& cell) {
        auto* def = _view_info.view_column(*_base, id);
        if (!def) {
            return;
        }
        if (def->is_atomic()) {
            set_max_ts(cell.as_atomic_cell());
        } else {
            static_pointer_cast<const collection_type_impl>(def->type)->for_each_cell(cell.as_collection_mutation(), set_max_ts);
        }
    });
    get_view_row(base_key, existing).apply(shadowable_tombstone(ts, now));
}

/**
 * Creates the updates to apply to the existing view entry given the base table row before
 * and after the update, assuming that the update hasn't changed to which view entry the
 * row corresponds (that is, we know the columns composing the view PK haven't changed).
 *
 * This method checks that the base row (before and after) matches the view filter before
 * applying anything.
 */
void view_updates::update_entry(const partition_key& base_key, const clustering_row& update, const clustering_row& existing, gc_clock::time_point now) {
    // While we know update and existing correspond to the same view entry,
    // they may not match the view filter.
    if (!matches_view_filter(*_base, _view_info, base_key, existing, now)) {
        create_entry(base_key, update, now);
        return;
    }
    if (!matches_view_filter(*_base, _view_info, base_key, update, now)) {
        do_delete_old_entry(base_key, existing, now);
        return;
    }

    deletable_row& r = get_view_row(base_key, update);
    r.apply(compute_row_marker(update));
    r.apply(update.tomb());

    auto diff = update.cells().difference(*_base, column_kind::regular_column, existing.cells());
    add_cells_to_view(*_base, *_view, diff, r.cells());
}

void view_updates::generate_update(
        const partition_key& base_key,
        const clustering_row& update,
        const stdx::optional<clustering_row>& existing,
        gc_clock::time_point now) {
    // Note that the base PK columns in update and existing are the same, since we're intrinsically dealing
    // with the same base row. So we have to check 3 things:
    //   1) that the clustering key doesn't have a null, which can happen for compact tables. If that's the case,
    //      there is no corresponding entries.
    //   2) if there is a column not part of the base PK in the view PK, whether it is changed by the update.
    //   3) whether the update actually matches the view SELECT filter

    if (!update.key().is_full(*_base)) {
        return;
    }

    auto col_id = _view_info.base_non_pk_column_in_view_pk(*_base);
    if (!col_id) {
        // The view key is necessarily the same pre and post update.
        if (existing && !existing->empty()) {
            if (update.empty()) {
                delete_old_entry(base_key, *existing, now);
            } else {
                update_entry(base_key, update, *existing, now);
            }
        }  else if (!update.empty()) {
            create_entry(base_key, update, now);
        }
        return;
    }

    auto* after = update.cells().find_cell(*col_id);
    // Note: multi-cell columns can't be part of the primary key.
    if (existing) {
        auto* before = existing->cells().find_cell(*col_id);
        if (before && before->as_atomic_cell().is_live()) {
            if (after && after->as_atomic_cell().is_live()) {
                auto cmp = compare_atomic_cell_for_merge(before->as_atomic_cell(), after->as_atomic_cell());
                if (cmp == 0) {
                    update_entry(base_key, update, *existing, now);
                } else {
                    replace_entry(base_key, update, *existing, now);
                }
            } else {
                delete_old_entry(base_key, *existing, now);
            }
            return;
        }
    }

    // No existing row or the cell wasn't live
    if (after && after->as_atomic_cell().is_live()) {
        create_entry(base_key, update, now);
    }
}

class view_update_builder {
    schema_ptr _schema; // The base schema
    std::vector<view_updates> _view_updates;
    streamed_mutation _updates;
    streamed_mutation_opt _existings;
    range_tombstone_accumulator _update_tombstone_tracker;
    range_tombstone_accumulator _existing_tombstone_tracker;
    mutation_fragment_opt _update;
    mutation_fragment_opt _existing;
    gc_clock::time_point _now;
public:

    view_update_builder(schema_ptr s,
        std::vector<view_updates>&& views_to_update,
        streamed_mutation&& updates,
        streamed_mutation_opt&& existings)
            : _schema(std::move(s))
            , _view_updates(std::move(views_to_update))
            , _updates(std::move(updates))
            , _existings(std::move(existings))
            , _update_tombstone_tracker(*_schema, false)
            , _existing_tombstone_tracker(*_schema, false)
            , _now(gc_clock::now()) {
        _update_tombstone_tracker.set_partition_tombstone(_updates.partition_tombstone());
        if (_existings) {
            _existing_tombstone_tracker.set_partition_tombstone(_existings->partition_tombstone());
        }
    }

    future<std::vector<mutation>> build();

private:
    void generate_update(clustering_row&& update, stdx::optional<clustering_row>&& existing);
    future<stop_iteration> on_results();

    future<stop_iteration> advance_all() {
        auto existings_f = _existings ? (*_existings)() : make_ready_future<optimized_optional<mutation_fragment>>();
        return when_all(_updates(), std::move(existings_f)).then([this] (auto&& fragments) mutable {
            _update = std::move(std::get<mutation_fragment_opt>(std::get<0>(fragments).get()));
            _existing = std::move(std::get<mutation_fragment_opt>(std::get<1>(fragments).get()));
            return stop_iteration::no;
        });
    }

    future<stop_iteration> advance_updates() {
        return _updates().then([this] (auto&& update) mutable {
            _update = std::move(update);
            return stop_iteration::no;
        });
    }

    future<stop_iteration> advance_existings() {
        if (!_existings) {
            return make_ready_future<stop_iteration>(stop_iteration::no);
        }
        return (*_existings)().then([this] (auto&& existing) mutable {
            _existing = std::move(existing);
            return stop_iteration::no;
        });
    }

    future<stop_iteration> stop() const {
        return make_ready_future<stop_iteration>(stop_iteration::yes);
    }
};

future<std::vector<mutation>> view_update_builder::build() {
    return advance_all().then([this] (auto&& ignored) {
        return repeat([this] {
            return this->on_results();
        });
    }).then([this] {
        std::vector<mutation> mutations;
        for (auto&& update : _view_updates) {
            std::move(update).move_to(mutations);
        }
        return mutations;
    });
}

void view_update_builder::generate_update(clustering_row&& update, stdx::optional<clustering_row>&& existing) {
    // If we have no update at all, we shouldn't get there.
    if (update.empty()) {
        throw std::logic_error("Empty materialized view updated");
    }

    auto gc_before = _now - _schema->gc_grace_seconds();

    // We allow existing to be disengaged, which we treat the same as an empty row.
    if (existing) {
        existing->marker().compact_and_expire(tombstone(), _now, always_gc, gc_before);
        existing->cells().compact_and_expire(*_schema, column_kind::regular_column, row_tombstone(), _now, always_gc, gc_before);
        update.apply(*_schema, *existing);
    }

    update.marker().compact_and_expire(tombstone(), _now, always_gc, gc_before);
    update.cells().compact_and_expire(*_schema, column_kind::regular_column, row_tombstone(), _now, always_gc, gc_before);

    for (auto&& v : _view_updates) {
        v.generate_update(_updates.key(), update, existing, _now);
    }
}

static void apply_tracked_tombstones(range_tombstone_accumulator& tracker, clustering_row& row) {
    for (auto&& rt : tracker.range_tombstones_for_row(row.key())) {
        row.apply(rt.tomb);
    }
}

future<stop_iteration> view_update_builder::on_results() {
    if (_update && _existing) {
        int cmp = position_in_partition::tri_compare(*_schema)(_update->position(), _existing->position());
        if (cmp < 0) {
            // We have an update where there was nothing before
            if (_update->is_range_tombstone()) {
                _update_tombstone_tracker.apply(std::move(_update->as_range_tombstone()));
            } else if (_update->is_clustering_row()) {
                auto& update = _update->as_mutable_clustering_row();
                apply_tracked_tombstones(_update_tombstone_tracker, update);
                auto tombstone = _existing_tombstone_tracker.current_tombstone();
                auto existing = tombstone
                              ? stdx::optional<clustering_row>(stdx::in_place, update.key(), row_tombstone(std::move(tombstone)), row_marker(), ::row())
                              : stdx::nullopt;
                generate_update(std::move(update), std::move(existing));
            }
            return advance_updates();
        }
        if (cmp > 0) {
            // We have something existing but no update (which will happen either because it's a range tombstone marker in
            // existing, or because we've fetched the existing row due to some partition/range deletion in the updates)
            if (_existing->is_range_tombstone()) {
                _existing_tombstone_tracker.apply(std::move(_existing->as_range_tombstone()));
            } else if (_existing->is_clustering_row()) {
                auto& existing = _existing->as_mutable_clustering_row();
                apply_tracked_tombstones(_existing_tombstone_tracker, existing);
                auto tombstone = _update_tombstone_tracker.current_tombstone();
                // The way we build the read command used for existing rows, we should always have a non-empty
                // tombstone, since we wouldn't have read the existing row otherwise. We don't assert that in case the
                // read method ever changes.
                if (tombstone) {
                    auto update = clustering_row(existing.key(), row_tombstone(std::move(tombstone)), row_marker(), ::row());
                    generate_update(std::move(update), { std::move(existing) });
                }
            }
            return advance_existings();
        }
        // We're updating a row that had pre-existing data
        if (_update->is_range_tombstone()) {
            assert(_existing->is_range_tombstone());
            _existing_tombstone_tracker.apply(std::move(*_existing).as_range_tombstone());
            _update_tombstone_tracker.apply(std::move(*_update).as_range_tombstone());
        } else if (_update->is_clustering_row()) {
            assert(!_existing->is_range_tombstone());
            apply_tracked_tombstones(_update_tombstone_tracker, _update->as_mutable_clustering_row());
            apply_tracked_tombstones(_existing_tombstone_tracker, _existing->as_mutable_clustering_row());
            generate_update(std::move(*_update).as_clustering_row(), { std::move(*_existing).as_clustering_row() });
        }
        return advance_all();
    }

    auto tombstone = _update_tombstone_tracker.current_tombstone();
    if (tombstone && _existing) {
        // We don't care if it's a range tombstone, as we're only looking for existing entries that get deleted
        if (_existing->is_clustering_row()) {
            auto& existing = _existing->as_clustering_row();
            auto update = clustering_row(existing.key(), row_tombstone(std::move(tombstone)), row_marker(), ::row());
            generate_update(std::move(update), { std::move(existing) });
        }
        return advance_existings();
    }

    // If we have updates and it's a range tombstone, it removes nothing pre-exisiting, so we can ignore it
    if (_update && _update->is_clustering_row()) {
        generate_update(std::move(*_update).as_clustering_row(), { });
        return advance_updates();
    }

    return stop();
}

future<std::vector<mutation>> generate_view_updates(
        const schema_ptr& base,
        std::vector<view_ptr>&& views_to_update,
        streamed_mutation&& updates,
        streamed_mutation_opt&& existings) {
    auto vs = boost::copy_range<std::vector<view_updates>>(views_to_update | boost::adaptors::transformed([&] (auto&& v) {
        return view_updates(std::move(v), base);
    }));
    auto builder = std::make_unique<view_update_builder>(base, std::move(vs), std::move(updates), std::move(existings));
    auto f = builder->build();
    return f.finally([builder = std::move(builder)] { });
}

query::clustering_row_ranges calculate_affected_clustering_ranges(const schema& base,
        const dht::decorated_key& key,
        const mutation_partition& mp,
        const std::vector<view_ptr>& views) {
    std::vector<nonwrapping_range<clustering_key_prefix_view>> row_ranges;
    std::vector<nonwrapping_range<clustering_key_prefix_view>> view_row_ranges;
    clustering_key_prefix_view::tri_compare cmp(base);
    if (mp.partition_tombstone() || !mp.row_tombstones().empty()) {
        for (auto&& v : views) {
            // FIXME: #2371
            if (v->view_info()->select_statement().get_restrictions()->has_unrestricted_clustering_columns()) {
                view_row_ranges.push_back(nonwrapping_range<clustering_key_prefix_view>::make_open_ended_both_sides());
                break;
            }
            for (auto&& r : v->view_info()->partition_slice().default_row_ranges()) {
                view_row_ranges.push_back(r.transform(std::mem_fn(&clustering_key_prefix::view)));
            }
        }
    }
    if (mp.partition_tombstone()) {
        std::swap(row_ranges, view_row_ranges);
    } else {
        // FIXME: Optimize, as most often than not clustering keys will not be restricted.
        for (auto&& rt : mp.row_tombstones()) {
            nonwrapping_range<clustering_key_prefix_view> rtr(
                    bound_view::to_range_bound<nonwrapping_range>(rt.start_bound()),
                    bound_view::to_range_bound<nonwrapping_range>(rt.end_bound()));
            for (auto&& vr : view_row_ranges) {
                auto overlap = rtr.intersection(vr, cmp);
                if (overlap) {
                    row_ranges.push_back(std::move(overlap).value());
                }
            }
        }
    }

    for (auto&& row : mp.clustered_rows()) {
        if (update_requires_read_before_write(base, views, key, row)) {
            row_ranges.emplace_back(row.key());
        }
    }

    // Note that the views could have restrictions on regular columns,
    // but even if that's the case we shouldn't apply those when we read,
    // because even if an existing row doesn't match the view filter, the
    // update can change that in which case we'll need to know the existing
    // content, in case the view includes a column that is not included in
    // this mutation.

    //FIXME: Unfortunate copy.
    return boost::copy_range<query::clustering_row_ranges>(
            nonwrapping_range<clustering_key_prefix_view>::deoverlap(std::move(row_ranges), cmp)
            | boost::adaptors::transformed([] (auto&& v) {
                return std::move(v).transform([] (auto&& ckv) { return clustering_key_prefix(ckv); });
            }));

}

// Calculate the node ("natural endpoint") to which this node should send
// a view update.
//
// A materialized view table is in the same keyspace as its base table,
// and in particular both have the same replication factor. Therefore it
// is possible, for a particular base partition and related view partition
// to "pair" between the base replicas and view replicas holding those
// partitions. The first (in ring order) base replica is paired with the
// first view replica, the second with the second, and so on. The purpose
// of this function is to find, assuming that this node is one of the base
// replicas for a given partition, the paired view replica.
//
// If the keyspace's replication strategy is a NetworkTopologyStrategy,
// we pair only nodes in the same datacenter.
// If one of the base replicas also happens to be a view replica, it is
// paired with itself (with the other nodes paired by order in the list
// after taking this node out).
//
// If the assumption that the given base token belongs to this replica
// does not hold, we return an empty optional.
static stdx::optional<gms::inet_address>
get_view_natural_endpoint(const sstring& keyspace_name,
        const dht::token& base_token, const dht::token& view_token) {
    auto &db = service::get_local_storage_service().db().local();
    auto& rs = db.find_keyspace(keyspace_name).get_replication_strategy();
    auto my_address = utils::fb_utilities::get_broadcast_address();
    auto my_datacenter = locator::i_endpoint_snitch::get_local_snitch_ptr()->get_datacenter(my_address);
    bool network_topology = dynamic_cast<const locator::network_topology_strategy*>(&rs);
    std::vector<gms::inet_address> base_endpoints, view_endpoints;
    for (auto&& base_endpoint : rs.get_natural_endpoints(base_token)) {
        if (!network_topology || locator::i_endpoint_snitch::get_local_snitch_ptr()->get_datacenter(base_endpoint) == my_datacenter) {
            base_endpoints.push_back(base_endpoint);
        }
    }

    for (auto&& view_endpoint : rs.get_natural_endpoints(view_token)) {
        // If this base replica is also one of the view replicas, we use
        // ourselves as the view replica.
        if (view_endpoint == my_address) {
            return view_endpoint;
        }
        // We have to remove any endpoint which is shared between the base
        // and the view, as it will select itself and throw off the counts
        // otherwise.
        auto it = std::find(base_endpoints.begin(), base_endpoints.end(),
            view_endpoint);
        if (it != base_endpoints.end()) {
            base_endpoints.erase(it);
        } else if (!network_topology || locator::i_endpoint_snitch::get_local_snitch_ptr()->get_datacenter(view_endpoint) == my_datacenter) {
            view_endpoints.push_back(view_endpoint);
        }
    }

    assert(base_endpoints.size() == view_endpoints.size());
    auto base_it = std::find(base_endpoints.begin(), base_endpoints.end(), my_address);
    if (base_it == base_endpoints.end()) {
        // This node is not a base replica of this key, so we return empty
        return {};
    }
    return view_endpoints[base_it - base_endpoints.begin()];
}

// Take the view mutations generated by generate_view_updates(), which pertain
// to a modification of a single base partition, and apply them to the
// appropriate paired replicas. This is done asynchronously - we do not wait
// for the writes to complete.
// FIXME: I dropped a lot of parameters the Cassandra version had,
// we may need them back: writeCommitLog, baseComplete, queryStartNanoTime.
void mutate_MV(const dht::token& base_token,
        std::vector<mutation> mutations)
{
#if 0
    Tracing.trace("Determining replicas for mutation");
    final String localDataCenter = DatabaseDescriptor.getEndpointSnitch().getDatacenter(FBUtilities.getBroadcastAddress());
    long startTime = System.nanoTime();

    try
    {
        // if we haven't joined the ring, write everything to batchlog because paired replicas may be stale
        final UUID batchUUID = UUIDGen.getTimeUUID();

        if (StorageService.instance.isStarting() || StorageService.instance.isJoining() || StorageService.instance.isMoving())
        {
            BatchlogManager.store(Batch.createLocal(batchUUID, FBUtilities.timestampMicros(),
                                                    mutations), writeCommitLog);
        }
        else
        {
            List<WriteResponseHandlerWrapper> wrappers = new ArrayList<>(mutations.size());
            List<Mutation> nonPairedMutations = new LinkedList<>();
            Token baseToken = StorageService.instance.getTokenMetadata().partitioner.getToken(dataKey);

            ConsistencyLevel consistencyLevel = ConsistencyLevel.ONE;

            //Since the base -> view replication is 1:1 we only need to store the BL locally
            final Collection<InetAddress> batchlogEndpoints = Collections.singleton(FBUtilities.getBroadcastAddress());
            BatchlogResponseHandler.BatchlogCleanup cleanup = new BatchlogResponseHandler.BatchlogCleanup(mutations.size(),
                                                                                                          () -> asyncRemoveFromBatchlog(batchlogEndpoints, batchUUID));
            // add a handler for each mutation - includes checking availability, but doesn't initiate any writes, yet
#endif
    for (auto& mut : mutations) {
        auto view_token = mut.token();
        auto keyspace_name = mut.schema()->ks_name();
        auto paired_endpoint = get_view_natural_endpoint(keyspace_name, base_token, view_token);
        auto pending_endpoints = service::get_local_storage_service().get_token_metadata().pending_endpoints_for(view_token, keyspace_name);
        if (paired_endpoint) {
            // When local node is the endpoint and there are no pending nodes we can
            // Just apply the mutation locally.
            auto my_address = utils::fb_utilities::get_broadcast_address();
            if (*paired_endpoint == my_address && pending_endpoints.empty() &&
                service::get_local_storage_service().is_joined()) {
                    // Note that we start here an asynchronous apply operation, and
                    // do not wait for it to complete.
                    // Note also that mutate_locally(mut) copies mut (in
                    // frozen from) so don't need to increase its lifetime.
                    service::get_local_storage_proxy().mutate_locally(mut).handle_exception([] (auto ep) {
                        logger.error("Error applying local view update: {}", ep);
                    });
            } else {
#if 0
                        wrappers.add(wrapViewBatchResponseHandler(mutation,
                                                                  consistencyLevel,
                                                                  consistencyLevel,
                                                                  Collections.singletonList(pairedEndpoint.get()),
                                                                  baseComplete,
                                                                  WriteType.BATCH,
                                                                  cleanup,
                                                                  queryStartNanoTime));
#endif
                // FIXME: Temporary hack: send the write directly to paired_endpoint,
                // without a batchlog, and without checking for success
                // Note we don't wait for the asynchronous operation to complete
                // FIXME: need to extend mut's lifetime???
                service::get_local_storage_proxy().send_to_endpoint(mut, *paired_endpoint, db::write_type::VIEW).handle_exception([paired_endpoint] (auto ep) {
                    logger.error("Error applying view update to {}: {}", *paired_endpoint, ep);
                });;
            }
        } else {
#if 0
                    //if there are no paired endpoints there are probably range movements going on,
                    //so we write to the local batchlog to replay later
                    if (pendingEndpoints.isEmpty())
                        logger.warn("Received base materialized view mutation for key {} that does not belong " +
                                    "to this node. There is probably a range movement happening (move or decommission)," +
                                    "but this node hasn't updated its ring metadata yet. Adding mutation to " +
                                    "local batchlog to be replayed later.",
                                    mutation.key());
                    nonPairedMutations.add(mutation);
                }
#endif
        }
    }
#if 0
            if (!wrappers.isEmpty())
            {
                // Apply to local batchlog memtable in this thread
                BatchlogManager.store(Batch.createLocal(batchUUID, FBUtilities.timestampMicros(), Lists.transform(wrappers, w -> w.mutation)),
                                      writeCommitLog);

                    // now actually perform the writes and wait for them to complete
                asyncWriteBatchedMutations(wrappers, localDataCenter, Stage.VIEW_MUTATION);
            }
#endif
#if 0
            if (!nonPairedMutations.isEmpty())
            {
                BatchlogManager.store(Batch.createLocal(batchUUID, FBUtilities.timestampMicros(), nonPairedMutations),
                                      writeCommitLog);
            }
        }
#endif
#if 0
    }
    finally
    {
        viewWriteMetrics.addNano(System.nanoTime() - startTime);
    }
#endif
}

} // namespace view
} // namespace db

