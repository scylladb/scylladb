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

#include <deque>
#include <functional>
#include <optional>
#include <unordered_set>
#include <vector>

#include <boost/range/algorithm/find_if.hpp>
#include <boost/range/algorithm/remove_if.hpp>
#include <boost/range/algorithm/transform.hpp>
#include <boost/range/adaptors.hpp>

#include <seastar/core/future-util.hh>

#include "database.hh"
#include "clustering_bounds_comparator.hh"
#include "cql3/statements/select_statement.hh"
#include "cql3/util.hh"
#include "db/view/view.hh"
#include "db/view/view_builder.hh"
#include "frozen_mutation.hh"
#include "gms/inet_address.hh"
#include "keys.hh"
#include "locator/network_topology_strategy.hh"
#include "mutation.hh"
#include "mutation_partition.hh"
#include "service/migration_manager.hh"
#include "service/storage_service.hh"
#include "view_info.hh"

using namespace std::chrono_literals;

static logging::logger vlogger("view");

view_info::view_info(const schema& schema, const raw_view_info& raw_view_info)
        : _schema(schema)
        , _raw(raw_view_info)
{ }

cql3::statements::select_statement& view_info::select_statement() const {
    if (!_select_statement) {
        shared_ptr<cql3::statements::raw::select_statement> raw;
        if (is_index()) {
            // Token column is the first clustering column
            auto token_column_it = boost::range::find_if(_schema.all_columns(), std::mem_fn(&column_definition::is_clustering_key));
            auto real_columns = _schema.all_columns() | boost::adaptors::filtered([this, token_column_it](const column_definition& cdef) {
                return std::addressof(cdef) != std::addressof(*token_column_it);
            });
            schema::columns_type columns = boost::copy_range<schema::columns_type>(std::move(real_columns));
            raw = cql3::util::build_select_statement(base_name(), where_clause(), include_all_columns(), columns);
        } else {
            raw = cql3::util::build_select_statement(base_name(), where_clause(), include_all_columns(), _schema.all_columns());
        }
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
    return view_column(base.regular_column_at(base_id));
}

const column_definition* view_info::view_column(const column_definition& base_def) const {
    return _schema.get_column_definition(base_def.name());
}

stdx::optional<column_id> view_info::base_non_pk_column_in_view_pk() const {
    return _base_non_pk_column_in_view_pk;
}

void view_info::initialize_base_dependent_fields(const schema& base) {
    for (auto&& view_col : boost::range::join(_schema.partition_key_columns(), _schema.clustering_key_columns())) {
        auto* base_col = base.get_column_definition(view_col.name());
        if (base_col && !base_col->is_primary_key()) {
            _base_non_pk_column_in_view_pk.emplace(base_col->id);
            break;
        }
    }
}

bool view_info::is_index() const {
    //TODO(sarna): result of this call can be cached instead of calling index_manager::is_index every time
    column_family& base_cf = service::get_local_storage_service().db().local().find_column_family(base_id());
    return base_cf.get_index_manager().is_index(view_ptr(_schema.shared_from_this()));
}

namespace db {

namespace view {

bool partition_key_matches(const schema& base, const view_info& view, const dht::decorated_key& key) {
    return view.select_statement().get_restrictions()->get_partition_key_restrictions()->is_satisfied_by(
            base, key.key(), clustering_key_prefix::make_empty(), row(), cql3::query_options({ }), gc_clock::now());
}

bool clustering_prefix_matches(const schema& base, const view_info& view, const partition_key& key, const clustering_key_prefix& ck) {
    return view.select_statement().get_restrictions()->get_clustering_columns_restrictions()->is_satisfied_by(
            base, key, ck, row(), cql3::query_options({ }), gc_clock::now());
}

bool may_be_affected_by(const schema& base, const view_info& view, const dht::decorated_key& key, const rows_entry& update) {
    // We can guarantee that the view won't be affected if:
    //  - the primary key is excluded by the view filter (note that this isn't true of the filter on regular columns:
    //    even if an update don't match a view condition on a regular column, that update can still invalidate a
    //    pre-existing entry) - note that the upper layers should already have checked the partition key;
    return clustering_prefix_matches(base, view, key.key(), update.key());
}

static bool update_requires_read_before_write(const schema& base,
        const std::vector<view_ptr>& views,
        const dht::decorated_key& key,
        const rows_entry& update) {
    for (auto&& v : views) {
        view_info& vf = *v->view_info();
        if (may_be_affected_by(base, vf, key, update)) {
            return true;
        }
    }
    return false;
}

static bool is_partition_key_empty(
        const schema& base,
        const schema& view_schema,
        const partition_key& base_key,
        const clustering_row& update) {
    // Empty partition keys are not supported on normal tables - they cannot
    // be inserted or queried, so enforce those rules here.
    if (view_schema.partition_key_columns().size() > 1) {
        // Composite partition keys are different: all components
        // are then allowed to be empty.
        return false;
    }
    auto* base_col = base.get_column_definition(view_schema.partition_key_columns().front().name());
    switch (base_col->kind) {
    case column_kind::partition_key:
        return base_key.get_component(base, base_col->position()).empty();
    case column_kind::clustering_key:
        return update.key().get_component(base, base_col->position()).empty();
    default:
        // No multi-cell columns in the view's partition key
        auto& c = update.cells().cell_at(base_col->id);
        return c.as_atomic_cell(*base_col).value().empty();
    }
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
            , _updates(8, partition_key::hashing(*_view), partition_key::equality(*_view)) {
    }

    void move_to(std::vector<frozen_mutation_and_schema>& mutations) && {
        auto& partitioner = dht::global_partitioner();
        std::transform(_updates.begin(), _updates.end(), std::back_inserter(mutations), [&, this] (auto&& m) {
            auto mut = mutation(_view, partitioner.decorate_key(*_view, std::move(m.first)), std::move(m.second));
            return frozen_mutation_and_schema{freeze(mut), std::move(_view)};
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
    dht::token token_for(const partition_key& base_key);
    deletable_row& get_view_row(const partition_key& base_key, const clustering_row& update);
    void create_entry(const partition_key& base_key, const clustering_row& update, gc_clock::time_point now);
    void delete_old_entry(const partition_key& base_key, const clustering_row& existing, const clustering_row& update, gc_clock::time_point now);
    void do_delete_old_entry(const partition_key& base_key, const clustering_row& existing, const clustering_row& update, gc_clock::time_point now);
    void update_entry(const partition_key& base_key, const clustering_row& update, const clustering_row& existing, gc_clock::time_point now);
    void replace_entry(const partition_key& base_key, const clustering_row& update, const clustering_row& existing, gc_clock::time_point now) {
        create_entry(base_key, update, now);
        delete_old_entry(base_key, existing, update, now);
    }
};

row_marker view_updates::compute_row_marker(const clustering_row& base_row) const {
    /*
     * We need to compute both the timestamp and expiration.
     *
     * There are 3 cases:
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
     *      This cell determines the liveness of the view row.
     *   2) The columns for the base and view PKs are exactly the same, and all base columns are selected by the view.
     *      In that case, all components (marker, deletion and cells) are the same and trivially mapped.
     *   3) The columns for the base and view PKs are exactly the same, but some base columns are not selected in the view.
     *      Use the max timestamp out of the base row marker and all the unselected columns - this ensures we can keep the
     *      view row alive. Do the same thing for the expiration, if the marker is dead or will expire, and so
     *      will all unselected columns.
     */

    auto marker = base_row.marker();
    auto col_id = _view_info.base_non_pk_column_in_view_pk();
    if (col_id) {
        auto& def = _base->regular_column_at(*col_id);
        // Note: multi-cell columns can't be part of the primary key.
        auto cell = base_row.cells().cell_at(*col_id).as_atomic_cell(def);
        return cell.is_live_and_has_ttl() ? row_marker(cell.timestamp(), cell.ttl(), cell.expiry()) : row_marker(cell.timestamp());
    }

    return marker;
}

dht::token view_updates::token_for(const partition_key& base_key) {
    return dht::global_partitioner().get_token(*_base, base_key);
}

deletable_row& view_updates::get_view_row(const partition_key& base_key, const clustering_row& update) {
    std::vector<bytes> linearized_values;
    auto get_value = boost::adaptors::transformed([&, this] (const column_definition& cdef) -> bytes_view {
        auto* base_col = _base->get_column_definition(cdef.name());
        if (!base_col) {
            if (!_view_info.is_index()) {
                throw std::logic_error(sprint("Column %s doesn't exist in base and this view is not backing a secondary index", cdef.name_as_text()));
            }
            auto& partitioner = dht::global_partitioner();
            return linearized_values.emplace_back(partitioner.token_to_bytes(token_for(base_key)));
        }
        switch (base_col->kind) {
        case column_kind::partition_key:
            return base_key.get_component(*_base, base_col->position());
        case column_kind::clustering_key:
            return update.key().get_component(*_base, base_col->position());
        default:
            auto& c = update.cells().cell_at(base_col->id);
            auto value_view = base_col->is_atomic() ? c.as_atomic_cell(cdef).value() : c.as_collection_mutation().data;
            if (value_view.is_fragmented()) {
                return linearized_values.emplace_back(value_view.linearize());
            }
            return value_view.first_fragment();
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

// Utility function for taking an existing cell, and creating a copy with an
// empty value instead of the original value, but with the original liveness
// information (expiration and deletion time) unchanged.
static atomic_cell make_empty(const atomic_cell_view& ac) {
    if (ac.is_live_and_has_ttl()) {
        return atomic_cell::make_live(*empty_type, ac.timestamp(), bytes_view{}, ac.expiry(), ac.ttl());
    } else if (ac.is_live()) {
        return atomic_cell::make_live(*empty_type, ac.timestamp(), bytes_view{});
    } else {
        return atomic_cell::make_dead(ac.timestamp(), ac.deletion_time());
    }
}

// Utility function for taking an existing collection which has both keys and
// values (i.e., either a list or map, but not a set), and creating a copy of
// this collection with all the values replaced by empty values.
// The make_empty() function above is used to ensure that liveness information
// is copied unchanged.
static collection_mutation make_empty(
        const collection_mutation_view& cm,
        const collection_type_impl& ctype) {
    collection_type_impl::mutation n;
    cm.data.with_linearized([&] (bytes_view bv) {
        auto m_view = ctype.deserialize_mutation_form(bv);
        n.tomb = m_view.tomb;
        for (auto&& c : m_view.cells) {
            n.cells.emplace_back(c.first, make_empty(c.second));
        }
    });
    return ctype.serialize_mutation_form(n);
}

// In some cases, we need to copy to a view table even columns which have not
// been SELECTed. For these columns we only need to save liveness information
// (timestamp, deletion, ttl), but not the value. We call these columns
// "virtual columns", and the reason why we need them is explained in
// issue #3362. The following function, maybe_make_virtual() takes a full
// value c (taken from the base table) for the given column col, and if that
// column is a virtual column it modifies c to remove the unwanted value.
// The function create_virtual_column(), below, creates the virtual column in
// the view schema, that maybe_make_virtual() will fill.
static void maybe_make_virtual(atomic_cell_or_collection& c, const column_definition* col) {
    if (!col->is_view_virtual()) {
        // This is a regular selected column. Leave c untouched.
        return;
    }
    if (col->type->is_atomic()) {
        // A virtual cell for an atomic value or frozen collection. Its
        // value is empty (of type empty_type).
        if (col->type != empty_type) {
            throw std::logic_error("Virtual cell has wrong type");
        }
        c = make_empty(c.as_atomic_cell(*col));
    } else {
        if (!col->type->is_collection()) {
            // TODO: when we support unfrozen UDT (#2201), we will need to
            // supported it here too.
            throw std::logic_error("Virtual cell is neither atomic nor collection");
        }
        auto ctype = static_pointer_cast<const collection_type_impl>(col->type);
        if (ctype->is_list()) {
            // A list has integers as keys, and values (the list's items).
            // We just need to build a list with the same keys (and liveness
            // information), but empty values.
            auto ltype = static_cast<const list_type_impl*>(col->type.get());
            if (ltype->get_elements_type() != empty_type) {
                throw std::logic_error("Virtual cell has wrong list type");
            }
            c = make_empty(c.as_collection_mutation(), *ctype);
        } else if (ctype->is_map()) {
            // A map has keys and values. We just need to build a map with
            // the same keys (and liveness information), but empty values.
            auto mtype = static_cast<const map_type_impl*>(col->type.get());
            if (mtype->get_values_type() != empty_type) {
                throw std::logic_error("Virtual cell has wrong map type");
            }
            c = make_empty(c.as_collection_mutation(), *ctype);
        } else if (ctype->is_set()) {
            // A set has just keys (and liveness information). We need
            // all of it as a virtual column, unfortunately, so we
            // leave c unmodified.
        } else {
            // A collection can't be anything but a list, map or set...
            throw std::logic_error("Virtual cell has unexpected collection type");
        }
    }
}

void create_virtual_column(schema_builder& builder, const bytes& name, const data_type& type) {
    if (type->is_atomic()) {
        builder.with_column(name, empty_type, column_kind::regular_column, column_view_virtual::yes);
        return;
    }
    // A multi-cell collection (a frozen collection is a single
    // cell and handled handled in the is_atomic() case above).
    // The virtual version can't be just one cell, it has to be
    // itself a collection of cells.
    auto ctype = dynamic_pointer_cast<const collection_type_impl>(type);
    if (!ctype) {
        // TODO: When #2201 is done, we also need to handle here
        // unfrozen UDTs.
        throw exceptions::invalid_request_exception(sprint("Unsupported unselected multi-cell non-collection column %s for Materialized View", name));
    }
    if (ctype->is_list()) {
        // A list has ints as keys, and values (the list's items).
        // We just need these intss, i.e., a list of empty items.
        builder.with_column(name, list_type_impl::get_instance(empty_type, true), column_kind::regular_column, column_view_virtual::yes);
    } else if (ctype->is_map()) {
        // A map has keys and values. We don't need these values,
        // and can use empty values instead.
        auto mtype = dynamic_pointer_cast<const map_type_impl>(type);
        builder.with_column(name, map_type_impl::get_instance(mtype->get_values_type(), empty_type, true), column_kind::regular_column, column_view_virtual::yes);
    } else if (ctype->is_set()) {
        // A set's cell has nothing beyond the keys, so the
        // virtual version of a set is, unfortunately, a complete
        // copy of the set.
        builder.with_column(name, type, column_kind::regular_column, column_view_virtual::yes);
    } else {
        // A collection can't be anything but a list, map or set...
        abort();
    }
}

static void add_cells_to_view(const schema& base, const schema& view, row base_cells, row& view_cells) {
    base_cells.for_each_cell([&] (column_id id, atomic_cell_or_collection& c) {
        auto* view_col = view_column(base, view, id);
        if (view_col && !view_col->is_primary_key()) {
            maybe_make_virtual(c, view_col);
            view_cells.append_cell(view_col->id, std::move(c));
        }
    });
}

/**
 * Creates a view entry corresponding to the provided base row.
 * This method checks that the base row does match the view filter before applying anything.
 */
void view_updates::create_entry(const partition_key& base_key, const clustering_row& update, gc_clock::time_point now) {
    if (is_partition_key_empty(*_base, *_view, base_key, update) || !matches_view_filter(*_base, _view_info, base_key, update, now)) {
        return;
    }
    deletable_row& r = get_view_row(base_key, update);
    auto marker = compute_row_marker(update);
    r.apply(marker);
    r.apply(update.tomb());
    add_cells_to_view(*_base, *_view, row(*_base, column_kind::regular_column, update.cells()), r.cells());
}

/**
 * Deletes the view entry corresponding to the provided base row.
 * This method checks that the base row does match the view filter before bothering.
 */
void view_updates::delete_old_entry(const partition_key& base_key, const clustering_row& existing, const clustering_row& update, gc_clock::time_point now) {
    // Before deleting an old entry, make sure it was matching the view filter
    // (otherwise there is nothing to delete)
    if (!is_partition_key_empty(*_base, *_view, base_key, existing) && matches_view_filter(*_base, _view_info, base_key, existing, now)) {
        do_delete_old_entry(base_key, existing, update, now);
    }
}

void view_updates::do_delete_old_entry(const partition_key& base_key, const clustering_row& existing, const clustering_row& update, gc_clock::time_point now) {
    auto& r = get_view_row(base_key, existing);
    auto col_id = _view_info.base_non_pk_column_in_view_pk();
    if (col_id) {
        // We delete the old row using a shadowable row tombstone, making sure that
        // the tombstone deletes everything in the row (or it might still show up).
        // Note: multi-cell columns can't be part of the primary key.
        auto& def = _base->regular_column_at(*col_id);
        auto cell = existing.cells().cell_at(*col_id).as_atomic_cell(def);
        if (cell.is_live()) {
            r.apply(shadowable_tombstone(cell.timestamp(), now));
        }
    } else {
        // "update" caused the base row to have been deleted, and !col_id
        // means view row is the same - so it needs to be deleted as well
        // using the same deletion timestamps for the individual cells.
        r.apply(update.marker());
        auto diff = update.cells().difference(*_base, column_kind::regular_column, existing.cells());
        add_cells_to_view(*_base, *_view, std::move(diff), r.cells());
    }
    r.apply(update.tomb());
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
    if (is_partition_key_empty(*_base, *_view, base_key, existing) || !matches_view_filter(*_base, _view_info, base_key, existing, now)) {
        create_entry(base_key, update, now);
        return;
    }
    if (is_partition_key_empty(*_base, *_view, base_key, update) || !matches_view_filter(*_base, _view_info, base_key, update, now)) {
        do_delete_old_entry(base_key, existing, update, now);
        return;
    }

    deletable_row& r = get_view_row(base_key, update);
    auto marker = compute_row_marker(update);
    r.apply(marker);
    r.apply(update.tomb());

    auto diff = update.cells().difference(*_base, column_kind::regular_column, existing.cells());
    add_cells_to_view(*_base, *_view, std::move(diff), r.cells());
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

    auto col_id = _view_info.base_non_pk_column_in_view_pk();
    if (!col_id) {
        // The view key is necessarily the same pre and post update.
        if (existing && existing->is_live(*_base)) {
            if (update.is_live(*_base)) {
                update_entry(base_key, update, *existing, now);
            } else {
                delete_old_entry(base_key, *existing, update, now);
            }
        } else if (update.is_live(*_base)) {
            create_entry(base_key, update, now);
        }
        return;
    }

    auto* after = update.cells().find_cell(*col_id);
    // Note: multi-cell columns can't be part of the primary key.
    auto& cdef = _base->regular_column_at(*col_id);
    if (existing) {
        auto* before = existing->cells().find_cell(*col_id);
        if (before && before->as_atomic_cell(cdef).is_live()) {
            if (after && after->as_atomic_cell(cdef).is_live()) {
                auto cmp = compare_atomic_cell_for_merge(before->as_atomic_cell(cdef), after->as_atomic_cell(cdef));
                if (cmp == 0) {
                    update_entry(base_key, update, *existing, now);
                } else {
                    replace_entry(base_key, update, *existing, now);
                }
            } else {
                delete_old_entry(base_key, *existing, update, now);
            }
            return;
        }
    }

    // No existing row or the cell wasn't live
    if (after && after->as_atomic_cell(cdef).is_live()) {
        create_entry(base_key, update, now);
    }
}

class view_update_builder {
    schema_ptr _schema; // The base schema
    std::vector<view_updates> _view_updates;
    flat_mutation_reader _updates;
    flat_mutation_reader_opt _existings;
    range_tombstone_accumulator _update_tombstone_tracker;
    range_tombstone_accumulator _existing_tombstone_tracker;
    mutation_fragment_opt _update;
    mutation_fragment_opt _existing;
    gc_clock::time_point _now;
    partition_key _key = partition_key::make_empty();
public:

    view_update_builder(schema_ptr s,
        std::vector<view_updates>&& views_to_update,
        flat_mutation_reader&& updates,
        flat_mutation_reader_opt&& existings)
            : _schema(std::move(s))
            , _view_updates(std::move(views_to_update))
            , _updates(std::move(updates))
            , _existings(std::move(existings))
            , _update_tombstone_tracker(*_schema, false)
            , _existing_tombstone_tracker(*_schema, false)
            , _now(gc_clock::now()) {
    }

    future<std::vector<frozen_mutation_and_schema>> build();

private:
    void generate_update(clustering_row&& update, stdx::optional<clustering_row>&& existing);
    future<stop_iteration> on_results();

    future<stop_iteration> advance_all() {
        auto existings_f = _existings ? (*_existings)(db::no_timeout) : make_ready_future<optimized_optional<mutation_fragment>>();
        return when_all(_updates(db::no_timeout), std::move(existings_f)).then([this] (auto&& fragments) mutable {
            _update = std::move(std::get<mutation_fragment_opt>(std::get<0>(fragments).get()));
            _existing = std::move(std::get<mutation_fragment_opt>(std::get<1>(fragments).get()));
            return stop_iteration::no;
        });
    }

    future<stop_iteration> advance_updates() {
        return _updates(db::no_timeout).then([this] (auto&& update) mutable {
            _update = std::move(update);
            return stop_iteration::no;
        });
    }

    future<stop_iteration> advance_existings() {
        if (!_existings) {
            return make_ready_future<stop_iteration>(stop_iteration::no);
        }
        return (*_existings)(db::no_timeout).then([this] (auto&& existing) mutable {
            _existing = std::move(existing);
            return stop_iteration::no;
        });
    }

    future<stop_iteration> stop() const {
        return make_ready_future<stop_iteration>(stop_iteration::yes);
    }
};

future<std::vector<frozen_mutation_and_schema>> view_update_builder::build() {
    return advance_all().then([this] (auto&& ignored) {
        assert(_update && _update->is_partition_start());
        _key = std::move(std::move(_update)->as_partition_start().key().key());
        _update_tombstone_tracker.set_partition_tombstone(_update->as_partition_start().partition_tombstone());
        if (_existing && _existing->is_partition_start()) {
            _existing_tombstone_tracker.set_partition_tombstone(_existing->as_partition_start().partition_tombstone());
        }
    }).then([this] {
        return advance_all().then([this] (auto&& ignored) {
            return repeat([this] {
                return this->on_results();
            });
        });
    }).then([this] {
        std::vector<frozen_mutation_and_schema> mutations;
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
        existing->marker().compact_and_expire(existing->tomb().tomb(), _now, always_gc, gc_before);
        existing->cells().compact_and_expire(*_schema, column_kind::regular_column, existing->tomb(), _now, always_gc, gc_before, existing->marker());
        update.apply(*_schema, *existing);
    }

    update.marker().compact_and_expire(update.tomb().tomb(), _now, always_gc, gc_before);
    update.cells().compact_and_expire(*_schema, column_kind::regular_column, update.tomb(), _now, always_gc, gc_before, update.marker());

    for (auto&& v : _view_updates) {
        v.generate_update(_key, update, existing, _now);
    }
}

static void apply_tracked_tombstones(range_tombstone_accumulator& tracker, clustering_row& row) {
    row.apply(tracker.tombstone_for_row(row.key()));
}

future<stop_iteration> view_update_builder::on_results() {
    if (_update && !_update->is_end_of_partition() && _existing && !_existing->is_end_of_partition()) {
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
            assert(_existing->is_clustering_row());
            apply_tracked_tombstones(_update_tombstone_tracker, _update->as_mutable_clustering_row());
            apply_tracked_tombstones(_existing_tombstone_tracker, _existing->as_mutable_clustering_row());
            generate_update(std::move(*_update).as_clustering_row(), { std::move(*_existing).as_clustering_row() });
        }
        return advance_all();
    }

    auto tombstone = _update_tombstone_tracker.current_tombstone();
    if (tombstone && _existing && !_existing->is_end_of_partition()) {
        // We don't care if it's a range tombstone, as we're only looking for existing entries that get deleted
        if (_existing->is_clustering_row()) {
            auto existing = clustering_row(*_schema, _existing->as_clustering_row());
            auto update = clustering_row(existing.key(), row_tombstone(std::move(tombstone)), row_marker(), ::row());
            generate_update(std::move(update), { std::move(existing) });
        }
        return advance_existings();
    }

    // If we have updates and it's a range tombstone, it removes nothing pre-exisiting, so we can ignore it
    if (_update && !_update->is_end_of_partition()) {
        if (_update->is_clustering_row()) {
            generate_update(std::move(*_update).as_clustering_row(), { });
        }
        return advance_updates();
    }

    return stop();
}

future<std::vector<frozen_mutation_and_schema>> generate_view_updates(
        const schema_ptr& base,
        std::vector<view_ptr>&& views_to_update,
        flat_mutation_reader&& updates,
        flat_mutation_reader_opt&& existings) {
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
future<> mutate_MV(
        const dht::token& base_token,
        std::vector<frozen_mutation_and_schema> view_updates,
        db::view::stats& stats,
        db::timeout_semaphore_units pending_view_updates)
{
    auto fs = std::make_unique<std::vector<future<>>>();
    fs->reserve(view_updates.size());
    auto& partitioner = dht::global_partitioner();
    for (frozen_mutation_and_schema& mut : view_updates) {
        auto view_token = partitioner.get_token(*mut.s, mut.fm.key(*mut.s));
        auto& keyspace_name = mut.s->ks_name();
        auto paired_endpoint = get_view_natural_endpoint(keyspace_name, base_token, view_token);
        auto pending_endpoints = service::get_local_storage_service().get_token_metadata().pending_endpoints_for(view_token, keyspace_name);
        auto maybe_account_failure = [&stats, units = pending_view_updates.split(mut.fm.representation().size())] (
                future<>&& f,
                gms::inet_address target,
                bool is_local,
                size_t remotes) {
            if (f.failed()) {
                stats.view_updates_failed_local += is_local;
                stats.view_updates_failed_remote += remotes;
                auto ep = f.get_exception();
                vlogger.error("Error applying view update to {}: {}", target, ep);
                return make_exception_future<>(std::move(ep));
            } else {
                return make_ready_future<>();
            }
        };
        if (paired_endpoint) {
            // When paired endpoint is the local node, we can just apply
            // the mutation locally, unless there are pending endpoints, in
            // which case we want to do an ordinary write so the view mutation
            // is sent to them as well.
            auto my_address = utils::fb_utilities::get_broadcast_address();
            bool is_endpoint_local = *paired_endpoint == my_address;
            int64_t updates_pushed_remote = !is_endpoint_local + pending_endpoints.size();

            stats.view_updates_pushed_local += is_endpoint_local;
            stats.view_updates_pushed_remote += updates_pushed_remote;

            if (is_endpoint_local && pending_endpoints.empty()) {
                // Note that we start here an asynchronous apply operation, and
                // do not wait for it to complete.
                // Note also that mutate_locally(mut) copies mut (in
                // frozen form) so don't need to increase its lifetime.
                // send_to_endpoint() below updates statistics on pending
                // writes but mutate_locally() doesn't, so we need to do that here.
                ++stats.writes;
                auto mut_ptr = std::make_unique<frozen_mutation>(std::move(mut.fm));
                fs->push_back(service::get_local_storage_proxy().mutate_locally(mut.s, *mut_ptr).then_wrapped(
                        [&stats,
                         maybe_account_failure = std::move(maybe_account_failure),
                         mut_ptr = std::move(mut_ptr)] (future<>&& f) {
                    --stats.writes;
                    return maybe_account_failure(std::move(f), utils::fb_utilities::get_broadcast_address(), true, 0);
                }));
            } else {
                vlogger.debug("Sending view update to endpoint {}, with pending endpoints = {}", *paired_endpoint, pending_endpoints);
                // Note we don't wait for the asynchronous operation to complete
                // without a batchlog, and without checking for success.
                // When the ownership of the view partition is being moved to a
                // new node (or nodes), listed in pending_enpoints, we also need
                // to send the update there. Currently, we do this from *each* of
                // the base replicas, but this is probably excessive - see
                // See https://issues.apache.org/jira/browse/CASSANDRA-14262/
                fs->push_back(service::get_local_storage_proxy().send_to_endpoint(
                        std::move(mut),
                        *paired_endpoint,
                        std::move(pending_endpoints),
                        db::write_type::VIEW, stats).then_wrapped(
                                [paired_endpoint,
                                 is_endpoint_local,
                                 updates_pushed_remote,
                                 maybe_account_failure = std::move(maybe_account_failure)] (future<>&& f) mutable {
                    return maybe_account_failure(std::move(f), std::move(*paired_endpoint), is_endpoint_local, updates_pushed_remote);
                }));
            }
        } else if (!pending_endpoints.empty()) {
            // If there is no paired endpoint, it means there's a range movement going on (decommission or move),
            // such that this base replica is gaining new token ranges. The current node is thus a pending_endpoint
            // from the POV of the coordinator that sent the request. Since we only look at natural endpoints to
            // determine base-to-view pairings, the current node won't appear in the list of base replicas. Sending
            // view updates to the view replica this base will eventually be paired with only makes a difference when
            // the base update didn't make it to the node which is currently being decommissioned or moved-from. Also,
            // if HH is enabled at the coordinator, the update will either make it there before the range movement
            // finishes, or later to this node when it becomes a natural endpoint for the token. We still ensure we
            // send to any pending view endpoints though.
            auto updates_pushed_remote = pending_endpoints.size();
            stats.view_updates_pushed_remote += updates_pushed_remote;
            auto target = pending_endpoints.back();
            pending_endpoints.pop_back();
            fs->push_back(service::get_local_storage_proxy().send_to_endpoint(
                    std::move(mut),
                    target,
                    std::move(pending_endpoints),
                    db::write_type::VIEW).then_wrapped(
                            [target,
                             updates_pushed_remote,
                             maybe_account_failure = std::move(maybe_account_failure)] (future<>&& f) {
                return maybe_account_failure(std::move(f), std::move(target), false, updates_pushed_remote);
            }));
        }
    }
    auto f = seastar::when_all_succeed(fs->begin(), fs->end());
    return f.finally([fs = std::move(fs)] { });
}

view_builder::view_builder(database& db, db::system_distributed_keyspace& sys_dist_ks, service::migration_manager& mm)
        : _db(db)
        , _sys_dist_ks(sys_dist_ks)
        , _mm(mm) {
}

future<> view_builder::start() {
    _started = seastar::async([this] {
        // Wait for schema agreement even if we're a seed node.
        while (!_mm.have_schema_agreement()) {
            if (_as.abort_requested()) {
                return;
            }
            seastar::sleep(500ms).get();
        }
        auto built = system_keyspace::load_built_views().get0();
        auto in_progress = system_keyspace::load_view_build_progress().get0();
        calculate_shard_build_step(std::move(built), std::move(in_progress)).get();
        _mm.register_listener(this);
        _current_step = _base_to_build_step.begin();
        _build_step.trigger();
    });
    return make_ready_future<>();
}

future<> view_builder::stop() {
    vlogger.info("Stopping view builder");
    _as.request_abort();
    return _started.finally([this] {
        _mm.unregister_listener(this);
        return _sem.wait().then([this] {
            _sem.broken();
            return _build_step.join();
        });
    });
}

static query::partition_slice make_partition_slice(const schema& s) {
    query::partition_slice::option_set opts;
    opts.set(query::partition_slice::option::send_partition_key);
    opts.set(query::partition_slice::option::send_clustering_key);
    opts.set(query::partition_slice::option::send_timestamp);
    opts.set(query::partition_slice::option::send_ttl);
    return query::partition_slice(
            {query::full_clustering_range},
            { },
            boost::copy_range<std::vector<column_id>>(s.regular_columns()
                    | boost::adaptors::transformed(std::mem_fn(&column_definition::id))),
            std::move(opts));
}

view_builder::build_step& view_builder::get_or_create_build_step(utils::UUID base_id) {
    auto it = _base_to_build_step.find(base_id);
    if (it == _base_to_build_step.end()) {
        auto base = _db.find_column_family(base_id).shared_from_this();
        auto p = _base_to_build_step.emplace(base_id, build_step{base, make_partition_slice(*base->schema())});
        // Iterators could have been invalidated if there was rehashing, so just reset the cursor.
        _current_step = p.first;
        it = p.first;
    }
    return it->second;
}

void view_builder::initialize_reader_at_current_token(build_step& step) {
    step.pslice = make_partition_slice(*step.base->schema());
    step.prange = dht::partition_range(dht::ring_position::starting_at(step.current_token()), dht::ring_position::max());
    step.reader = make_local_shard_sstable_reader(
            step.base->schema(),
            make_lw_shared(sstables::sstable_set(step.base->get_sstable_set())),
            step.prange,
            step.pslice,
            default_priority_class(),
            no_resource_tracking(),
            nullptr,
            streamed_mutation::forwarding::no,
            mutation_reader::forwarding::no);
}

void view_builder::load_view_status(view_builder::view_build_status status, std::unordered_set<utils::UUID>& loaded_views) {
    if (!status.next_token) {
        // No progress was made on this view, so we'll treat it as new.
        return;
    }
    vlogger.info0("Resuming to build view {}.{} at {}", status.view->ks_name(), status.view->cf_name(), *status.next_token);
    loaded_views.insert(status.view->id());
    if (status.first_token == *status.next_token) {
        // Completed, so nothing to do for this shard. Consider the view
        // as loaded and not as a new view.
        _built_views.emplace(status.view->id());
        return;
    }
    get_or_create_build_step(status.view->view_info()->base_id()).build_status.emplace_back(std::move(status));
}

void view_builder::reshard(
        std::vector<std::vector<view_builder::view_build_status>> view_build_status_per_shard,
        std::unordered_set<utils::UUID>& loaded_views) {
    // We must reshard. We aim for a simple algorithm, a step above not starting from scratch.
    // Shards build entries at different paces, so both first and last tokens will differ. We
    // want to be conservative when selecting the range that has been built. To do that, we
    // select the intersection of all the previous shard's ranges for each view.
    struct view_ptr_hash {
        std::size_t operator()(const view_ptr& v) const noexcept {
            return std::hash<utils::UUID>()(v->id());
        }
    };
    struct view_ptr_equals {
        bool operator()(const view_ptr& v1, const view_ptr& v2) const noexcept {
            return v1->id() == v2->id();
        }
    };
    std::unordered_map<view_ptr, stdx::optional<nonwrapping_range<dht::token>>, view_ptr_hash, view_ptr_equals> my_status;
    for (auto& shard_status : view_build_status_per_shard) {
        for (auto& [view, first_token, next_token] : shard_status ) {
            // We start from an open-ended range, which we'll try to restrict.
            auto& my_range = my_status.emplace(
                    std::move(view),
                    nonwrapping_range<dht::token>::make_open_ended_both_sides()).first->second;
            if (!next_token || !my_range) {
                // A previous shard made no progress, so for this view we'll start over.
                my_range = stdx::nullopt;
                continue;
            }
            if (first_token == *next_token) {
                // Completed, so don't consider this shard's progress. We know that if the view
                // is marked as in-progress, then at least one shard will have a non-full range.
                continue;
            }
            wrapping_range<dht::token> other_range(first_token, *next_token);
            if (other_range.is_wrap_around(dht::token_comparator())) {
                // The intersection of a wrapping range with a non-wrapping range may yield more
                // multiple non-contiguous ranges. To avoid the complexity of dealing with more
                // than one range, we'll just take one of the intersections.
                auto [bottom_range, top_range] = other_range.unwrap();
                if (auto bottom_int = my_range->intersection(nonwrapping_range(std::move(bottom_range)), dht::token_comparator())) {
                    my_range = std::move(bottom_int);
                } else {
                    my_range = my_range->intersection(nonwrapping_range(std::move(top_range)), dht::token_comparator());
                }
            } else {
                my_range = my_range->intersection(nonwrapping_range(std::move(other_range)), dht::token_comparator());
            }
        }
    }
    view_builder::base_to_build_step_type build_step;
    for (auto& [view, opt_range] : my_status) {
        if (!opt_range) {
            continue; // Treat it as a new table.
        }
        auto start_bound = opt_range->start() ? std::move(opt_range->start()->value()) : dht::minimum_token();
        auto end_bound = opt_range->end() ? std::move(opt_range->end()->value()) : dht::minimum_token();
        auto s = view_build_status{std::move(view), std::move(start_bound), std::move(end_bound)};
        load_view_status(std::move(s), loaded_views);
    }
}

future<> view_builder::calculate_shard_build_step(
        std::vector<system_keyspace::view_name> built,
        std::vector<system_keyspace::view_build_progress> in_progress) {
    // Shard 0 makes cleanup changes to the system tables, but none that could conflict
    // with the other shards; everyone is thus able to proceed independently.
    auto bookkeeping_ops = std::make_unique<std::vector<future<>>>();
    auto base_table_exists = [&, this] (const view_ptr& view) {
        // This is a safety check in case this node missed a create MV statement
        // but got a drop table for the base, and another node didn't get the
        // drop notification and sent us the view schema.
        try {
            _db.find_schema(view->view_info()->base_id());
            return true;
        } catch (const no_such_column_family&) {
            return false;
        }
    };
    auto maybe_fetch_view = [&, this] (system_keyspace::view_name& name) {
        try {
            auto s = _db.find_schema(name.first, name.second);
            if (s->is_view()) {
                auto view = view_ptr(std::move(s));
                if (base_table_exists(view)) {
                    return view;
                }
            }
            // The view was dropped and a table was re-created with the same name,
            // but the write to the view-related system tables didn't make it.
        } catch (const no_such_column_family&) {
            // Fall-through
        }
        if (engine().cpu_id() == 0) {
            bookkeeping_ops->push_back(_sys_dist_ks.remove_view(name.first, name.second));
            bookkeeping_ops->push_back(system_keyspace::remove_built_view(name.first, name.second));
            bookkeeping_ops->push_back(
                    system_keyspace::remove_view_build_progress_across_all_shards(
                            std::move(name.first),
                            std::move(name.second)));
        }
        return view_ptr(nullptr);
    };

    auto built_views = boost::copy_range<std::unordered_set<utils::UUID>>(built
            | boost::adaptors::transformed(maybe_fetch_view)
            | boost::adaptors::filtered([] (const view_ptr& v) { return bool(v); })
            | boost::adaptors::transformed([] (const view_ptr& v) { return v->id(); }));

    std::vector<std::vector<view_build_status>> view_build_status_per_shard;
    for (auto& [view_name, first_token, next_token_opt, cpu_id] : in_progress) {
        if (auto view = maybe_fetch_view(view_name)) {
            if (built_views.find(view->id()) != built_views.end()) {
                if (engine().cpu_id() == 0) {
                    auto f = _sys_dist_ks.finish_view_build(std::move(view_name.first), std::move(view_name.second)).then([view = std::move(view)] {
                        system_keyspace::remove_view_build_progress_across_all_shards(view->cf_name(), view->ks_name());
                    });
                    bookkeeping_ops->push_back(std::move(f));
                }
                continue;
            }
            view_build_status_per_shard.resize(std::max(view_build_status_per_shard.size(), size_t(cpu_id + 1)));
            view_build_status_per_shard[cpu_id].emplace_back(view_build_status{
                    std::move(view),
                    std::move(first_token),
                    std::move(next_token_opt)});
        }
    }

    // All shards need to arrive at the same decisions on whether or not to
    // restart a view build at some common token (reshard), and which token
    // to restart at. So we need to wait until all shards have read the view
    // build statuses before they can all proceed to make the (same) decision.
    // If we don't synchronoize here, a fast shard may make a decision, start
    // building and finish a build step - before the slowest shard even read
    // the view build information.
    container().invoke_on(0, [] (view_builder& builder) {
        if (++builder._shards_finished_read == smp::count) {
            builder._shards_finished_read_promise.set_value();
        }
        return builder._shards_finished_read_promise.get_shared_future();
    }).get();

    std::unordered_set<utils::UUID> loaded_views;
    if (view_build_status_per_shard.size() != smp::count) {
        reshard(std::move(view_build_status_per_shard), loaded_views);
    } else if (!view_build_status_per_shard.empty()) {
        for (auto& status : view_build_status_per_shard[engine().cpu_id()]) {
            load_view_status(std::move(status), loaded_views);
        }
    }

    for (auto& [_, build_step] : _base_to_build_step) {
        boost::sort(build_step.build_status, [] (view_build_status s1, view_build_status s2) {
            return *s1.next_token < *s2.next_token;
        });
        if (!build_step.build_status.empty()) {
            build_step.current_key = dht::decorated_key{*build_step.build_status.front().next_token, partition_key::make_empty()};
        }
    }

    auto all_views = _db.get_views();
    auto is_new = [&] (const view_ptr& v) {
        return base_table_exists(v) && loaded_views.find(v->id()) == loaded_views.end()
                && built_views.find(v->id()) == built_views.end();
    };
    for (auto&& view : all_views | boost::adaptors::filtered(is_new)) {
        bookkeeping_ops->push_back(add_new_view(view, get_or_create_build_step(view->view_info()->base_id())));
    }

    for (auto& [_, build_step] : _base_to_build_step) {
        initialize_reader_at_current_token(build_step);
    }

    auto f = seastar::when_all_succeed(bookkeeping_ops->begin(), bookkeeping_ops->end());
    return f.handle_exception([bookkeeping_ops = std::move(bookkeeping_ops)] (std::exception_ptr ep) {
        vlogger.error("Failed to update materialized view bookkeeping ({}), continuing anyway.", ep);
    });
}

future<> view_builder::add_new_view(view_ptr view, build_step& step) {
    vlogger.info0("Building view {}.{}, starting at token {}", view->ks_name(), view->cf_name(), step.current_token());
    step.build_status.emplace(step.build_status.begin(), view_build_status{view, step.current_token(), std::nullopt});
    auto f = engine().cpu_id() == 0 ? _sys_dist_ks.start_view_build(view->ks_name(), view->cf_name()) : make_ready_future<>();
    return when_all_succeed(
            std::move(f),
            system_keyspace::register_view_for_building(view->ks_name(), view->cf_name(), step.current_token()));
}

static future<> flush_base(lw_shared_ptr<column_family> base, abort_source& as) {
    struct empty_state { };
    return exponential_backoff_retry::do_until_value(1s, 1min, as, [base = std::move(base)] {
        return base->flush().then_wrapped([base] (future<> f) -> stdx::optional<empty_state> {
            if (f.failed()) {
                vlogger.error("Error flushing base table {}.{}: {}; retrying", base->schema()->ks_name(), base->schema()->cf_name(), f.get_exception());
                return { };
            }
            return { empty_state{} };
        });
    }).discard_result();
}

void view_builder::on_create_view(const sstring& ks_name, const sstring& view_name) {
    with_semaphore(_sem, 1, [ks_name, view_name, this] {
        auto view = view_ptr(_db.find_schema(ks_name, view_name));
        auto& step = get_or_create_build_step(view->view_info()->base_id());
        return step.base->await_pending_writes().then([this, &step] {
            return flush_base(step.base, _as);
        }).then([this, view, &step] () mutable {
            // This resets the build step to the current token. It may result in views currently
            // being built to receive duplicate updates, but it simplifies things as we don't have
            // to keep around a list of new views to build the next time the reader crosses a token
            // threshold.
            initialize_reader_at_current_token(step);
            return add_new_view(view, step).then_wrapped([this, view] (future<>&& f) {
                if (f.failed()) {
                    vlogger.error("Error setting up view for building {}.{}: {}", view->ks_name(), view->cf_name(), f.get_exception());
                }
                _build_step.trigger();
            });
        });
    }).handle_exception_type([] (no_such_column_family&) { });
}

void view_builder::on_update_view(const sstring& ks_name, const sstring& view_name, bool) {
    with_semaphore(_sem, 1, [ks_name, view_name, this] {
        auto view = view_ptr(_db.find_schema(ks_name, view_name));
        auto step_it = _base_to_build_step.find(view->view_info()->base_id());
        if (step_it == _base_to_build_step.end()) {
            return;// In case all the views for this CF have finished building already.
        }
        auto status_it = boost::find_if(step_it->second.build_status, [view] (const view_build_status& bs) {
            return bs.view->id() == view->id();
        });
        if (status_it != step_it->second.build_status.end()) {
            status_it->view = std::move(view);
        }
    }).handle_exception_type([] (no_such_column_family&) { });
}

void view_builder::on_drop_view(const sstring& ks_name, const sstring& view_name) {
    vlogger.info0("Stopping to build view {}.{}", ks_name, view_name);
    with_semaphore(_sem, 1, [ks_name, view_name, this] {
        // The view is absent from the database at this point, so find it by brute force.
        ([&, this] {
            for (auto& [_, step] : _base_to_build_step) {
                if (step.build_status.empty() || step.build_status.front().view->ks_name() != ks_name) {
                    continue;
                }
                for (auto it = step.build_status.begin(); it != step.build_status.end(); ++it) {
                    if (it->view->cf_name() == view_name) {
                        _built_views.erase(it->view->id());
                        step.build_status.erase(it);
                        return;
                    }
                }
            }
        })();
        if (engine().cpu_id() != 0) {
            // Shard 0 can't remove the entry in the build progress system table on behalf of the
            // current shard, since shard 0 may have already processed the notification, and this
            // shard may since have updated the system table if the drop happened concurrently
            // with the build.
            return system_keyspace::remove_view_build_progress(ks_name, view_name);
        }
        return when_all_succeed(
                    system_keyspace::remove_view_build_progress(ks_name, view_name),
                    system_keyspace::remove_built_view(ks_name, view_name),
                    _sys_dist_ks.remove_view(ks_name, view_name)).handle_exception([ks_name, view_name] (std::exception_ptr ep) {
            vlogger.warn("Failed to cleanup view {}.{}: {}", ks_name, view_name, ep);
        });
    });
}

future<> view_builder::do_build_step() {
    return seastar::async([this] {
        exponential_backoff_retry r(1s, 1min);
        while (!_base_to_build_step.empty() && !_as.abort_requested()) {
            auto units = get_units(_sem, 1).get0();
            try {
                execute(_current_step->second, exponential_backoff_retry(1s, 1min));
                r.reset();
            } catch (const abort_requested_exception&) {
                return;
            } catch (...) {
                auto base = _current_step->second.base->schema();
                vlogger.warn("Error executing build step for base {}.{}: {}", base->ks_name(), base->cf_name(), std::current_exception());
                r.retry(_as).get();
                initialize_reader_at_current_token(_current_step->second);
            }
            if (_current_step->second.build_status.empty()) {
                _current_step = _base_to_build_step.erase(_current_step);
            } else {
                ++_current_step;
            }
            if (_current_step == _base_to_build_step.end()) {
                _current_step = _base_to_build_step.begin();
            }
        }
    });
}

// Called in the context of a seastar::thread.
class view_builder::consumer {
public:
    struct built_views {
        build_step& step;
        std::vector<view_build_status> views;

        built_views(build_step& step)
                : step(step) {
        }

        built_views(built_views&& other)
                : step(other.step)
                , views(std::move(other.views)) {
        }

        ~built_views() {
            for (auto&& status : views) {
                // Use step.current_token(), which may have wrapped around and become < first_token.
                step.build_status.emplace_back(view_build_status{std::move(status.view), step.current_token(), step.current_token()});
            }
        }

        void release() {
            views.clear();
        }
    };

private:
    view_builder& _builder;
    build_step& _step;
    built_views _built_views;
    std::vector<view_ptr> _views_to_build;
    std::deque<mutation_fragment> _fragments;
    // The compact_for_query<> that feeds this consumer is already configured
    // to feed us up to view_builder::batchsize (128) rows and not an entire
    // partition. Still, if rows contain large blobs, saving 128 of them in
    // _fragments may be too much. So we want to track _fragment's memory
    // usage, and flush the _fragments if it has grown too large.
    // Additionally, limiting _fragment's size also solves issue #4213:
    // A single view mutation can be as large as the size of the base rows
    // used to build it, and we cannot allow its serialized size to grow
    // beyond our limit on mutation size (by default 32 MB).
    size_t _fragments_memory_usage = 0;
public:
    consumer(view_builder& builder, build_step& step)
            : _builder(builder)
            , _step(step)
            , _built_views{step} {
        if (!step.current_key.key().is_empty(*_step.reader.schema())) {
            load_views_to_build();
        }
    }

    void load_views_to_build() {
        for (auto&& vs : _step.build_status) {
            if (_step.current_token() >= vs.next_token) {
                if (partition_key_matches(*_step.reader.schema(), *vs.view->view_info(), _step.current_key)) {
                    _views_to_build.push_back(vs.view);
                }
                if (vs.next_token || _step.current_token() != vs.first_token) {
                    vs.next_token = _step.current_key.token();
                }
            } else {
                break;
            }
        }
    }

    void check_for_built_views() {
        for (auto it = _step.build_status.begin(); it != _step.build_status.end();) {
            // A view starts being built at token t1. Due to resharding, that may not necessarily be a
            // shard-owned token. We finish building the view when the next_token to build is just before
            // (or at) the first token, but the shard-owned current token is after (or at) the first token.
            // In the system tables, we set first_token = next_token to signal the completion of the build
            // process in case of a restart.
            if (it->next_token && *it->next_token <= it->first_token && _step.current_token() >= it->first_token) {
                _built_views.views.push_back(std::move(*it));
                it = _step.build_status.erase(it);
            } else {
                ++it;
            }
        }
    }

    stop_iteration consume_new_partition(const dht::decorated_key& dk) {
        _step.current_key = std::move(dk);
        check_for_built_views();
        _views_to_build.clear();
        load_views_to_build();
        return stop_iteration(_views_to_build.empty());
    }

    stop_iteration consume(tombstone) {
        return stop_iteration::no;
    }

    stop_iteration consume(static_row&&, tombstone, bool) {
        return stop_iteration::no;
    }

    stop_iteration consume(clustering_row&& cr, row_tombstone, bool) {
        if (_views_to_build.empty() || _builder._as.abort_requested()) {
            return stop_iteration::yes;
        }

        _fragments_memory_usage += cr.memory_usage(*_step.base->schema());
        _fragments.push_back(std::move(cr));
        if (_fragments_memory_usage > 1024*1024) {
            // Although we have not yet completed the batch of base rows that
            // compact_for_query<> planned for us (view_builder::batchsize),
            // we've still collected enough rows to reach sizeable memory use,
            // so let's flush these rows now.
            flush_fragments();
        }
        return stop_iteration::no;
    }

    stop_iteration consume(range_tombstone&&) {
        return stop_iteration::no;
    }

    void flush_fragments() {
        _builder._as.check();
        if (!_fragments.empty()) {
            _fragments.push_front(partition_start(_step.current_key, tombstone()));
            _step.base->populate_views(
                    _views_to_build,
                    _step.current_token(),
                    make_flat_mutation_reader_from_fragments(_step.base->schema(), std::move(_fragments))).get();
            _fragments.clear();
            _fragments_memory_usage = 0;
        }
    }

    stop_iteration consume_end_of_partition() {
        flush_fragments();
        return stop_iteration(_step.build_status.empty());
    }

    built_views consume_end_of_stream() {
        if (vlogger.is_enabled(log_level::debug)) {
            auto view_names = boost::copy_range<std::vector<sstring>>(
                    _views_to_build | boost::adaptors::transformed([](auto v) {
                        return v->cf_name();
                    }));
            vlogger.debug("Completed build step for base {}.{}, at token {}; views={}", _step.base->schema()->ks_name(),
                          _step.base->schema()->cf_name(), _step.current_token(), view_names);
        }
        if (_step.reader.is_end_of_stream() && _step.reader.is_buffer_empty()) {
            _step.current_key = {dht::minimum_token(), partition_key::make_empty()};
            for (auto&& vs : _step.build_status) {
                vs.next_token = dht::minimum_token();
            }
            _builder.initialize_reader_at_current_token(_step);
            check_for_built_views();
        }
        return std::move(_built_views);
    }
};

// Called in the context of a seastar::thread.
void view_builder::execute(build_step& step, exponential_backoff_retry r) {
    auto consumer = compact_for_query<emit_only_live_rows::yes, view_builder::consumer>(
            *step.reader.schema(),
            gc_clock::now(),
            step.pslice,
            batch_size,
            query::max_partitions,
            view_builder::consumer{*this, step});
    consumer.consume_new_partition(step.current_key); // Initialize the state in case we're resuming a partition
    auto built = step.reader.consume_in_thread(std::move(consumer), db::no_timeout);

    _as.check();

    std::vector<future<>> bookkeeping_ops;
    bookkeeping_ops.reserve(built.views.size() + step.build_status.size());
    for (auto& [view, first_token, _] : built.views) {
        bookkeeping_ops.push_back(maybe_mark_view_as_built(view, first_token));
    }
    built.release();
    for (auto& [view, _, next_token] : step.build_status) {
        if (next_token) {
            bookkeeping_ops.push_back(
                    system_keyspace::update_view_build_progress(view->ks_name(), view->cf_name(), *next_token));
        }
    }
    seastar::when_all_succeed(bookkeeping_ops.begin(), bookkeeping_ops.end()).handle_exception([] (std::exception_ptr ep) {
        vlogger.error("Failed to update materialized view bookkeeping ({}), continuing anyway.", ep);
    }).get();
}

future<> view_builder::maybe_mark_view_as_built(view_ptr view, dht::token next_token) {
    _built_views.emplace(view->id());
    vlogger.debug("Shard finished building view {}.{}", view->ks_name(), view->cf_name());
    return container().map_reduce0(
            [view_id = view->id()] (view_builder& builder) {
                return builder._built_views.count(view_id);
            },
            true,
            [] (bool result, bool shard_complete) {
                return result & shard_complete;
            }).then([this, view, next_token = std::move(next_token)] (bool built) {
        if (built) {
            return container().invoke_on_all([view_id = view->id()] (view_builder& builder) {
                if (builder._built_views.erase(view_id) == 0 || engine().cpu_id() != 0) {
                    return make_ready_future<>();
                }
                auto view = builder._db.find_schema(view_id);
                vlogger.info("Finished building view {}.{}", view->ks_name(), view->cf_name());
                return seastar::when_all_succeed(
                        system_keyspace::mark_view_as_built(view->ks_name(), view->cf_name()),
                        builder._sys_dist_ks.finish_view_build(view->ks_name(), view->cf_name())).then([view] {
                    // The view is built, so shard 0 can remove the entry in the build progress system table on
                    // behalf of all shards. It is guaranteed to have a higher timestamp than the per-shard entries.
                    return system_keyspace::remove_view_build_progress_across_all_shards(view->ks_name(), view->cf_name());
                }).then([&builder, view] {
                    auto it = builder._build_notifiers.find(std::pair(view->ks_name(), view->cf_name()));
                    if (it != builder._build_notifiers.end()) {
                        it->second.set_value();
                    }
                });
            });
        }
        return system_keyspace::update_view_build_progress(view->ks_name(), view->cf_name(), next_token);
    });
}

future<> view_builder::wait_until_built(const sstring& ks_name, const sstring& view_name) {
    return container().invoke_on(0, [ks_name, view_name] (view_builder& builder) {
        auto v = std::pair(std::move(ks_name), std::move(view_name));
        return builder._build_notifiers[std::move(v)].get_shared_future();
    });
}

update_backlog node_update_backlog::add_fetch(unsigned shard, update_backlog backlog) {
    _backlogs[shard].backlog.store(backlog, std::memory_order_relaxed);
    auto now = clock::now();
    if (now >= _last_update.load(std::memory_order_relaxed) + _interval) {
        _last_update.store(now, std::memory_order_relaxed);
        auto new_max = boost::accumulate(
                _backlogs,
                update_backlog::no_backlog(),
                [] (const update_backlog& lhs, const per_shard_backlog& rhs) {
                    return std::max(lhs, rhs.load());
                });
        _max.store(new_max, std::memory_order_relaxed);
        return new_max;
    }
    return std::max(backlog, _max.load(std::memory_order_relaxed));
}

} // namespace view
} // namespace db
