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

#include <boost/range/algorithm/transform.hpp>
#include <boost/range/adaptors.hpp>

#include "clustering_bounds_comparator.hh"
#include "cql3/statements/select_statement.hh"
#include "cql3/util.hh"
#include "db/view/view.hh"

namespace db {

namespace view {

cql3::statements::select_statement& view::select_statement() const {
    if (!_select_statement) {
        std::vector<sstring_view> included;
        if (!_schema->view_info()->include_all_columns()) {
            included.reserve(_schema->all_columns_in_select_order().size());
            boost::transform(_schema->all_columns_in_select_order(), std::back_inserter(included), std::mem_fn(&column_definition::name_as_text));
        }
        auto raw = cql3::util::build_select_statement(_schema->view_info()->base_name(), _schema->view_info()->where_clause(), std::move(included));
        raw->prepare_keyspace(_schema->ks_name());
        raw->set_bound_variables({});
        cql3::cql_stats ignored;
        auto prepared = raw->prepare(service::get_local_storage_proxy().get_db().local(), ignored, true);
        _select_statement = static_pointer_cast<cql3::statements::select_statement>(prepared->statement);
    }
    return *_select_statement;
}

const query::partition_slice& view::partition_slice() const {
    if (!_partition_slice) {
        _partition_slice = select_statement().make_partition_slice(cql3::query_options({ }));
    }
    return *_partition_slice;
}

const dht::partition_range_vector& view::partition_ranges() const {
    if (!_partition_ranges) {
        _partition_ranges = select_statement().get_restrictions()->get_partition_key_ranges(cql3::query_options({ }));
    }
    return *_partition_ranges;
}

bool view::partition_key_matches(const ::schema& base, const dht::decorated_key& key) const {
    dht::ring_position rp(key);
    auto& ranges = partition_ranges();
    return std::any_of(ranges.begin(), ranges.end(), [&] (auto&& range) {
        return range.contains(rp, dht::ring_position_comparator(base));
    });
}

bool view::clustering_prefix_matches(const ::schema& base, const partition_key& key, const clustering_key_prefix& ck) const {
    bound_view::compare less(base);
    auto& ranges = partition_slice().row_ranges(base, key);
    return std::any_of(ranges.begin(), ranges.end(), [&] (auto&& range) {
        auto bounds = bound_view::from_range(range);
        return !less(ck, bounds.first) && !less(bounds.second, ck);
    });
}

bool view::may_be_affected_by(const ::schema& base, const dht::decorated_key& key, const rows_entry& update) const {
    // We can guarantee that the view won't be affected if:
    //  - the primary key is excluded by the view filter (note that this isn't true of the filter on regular columns:
    //    even if an update don't match a view condition on a regular column, that update can still invalidate a
    //    pre-existing entry);
    //  - the update doesn't modify any of the columns impacting the view (where "impacting" the view means that column
    //    is neither included in the view, nor used by the view filter).
    if (!partition_key_matches(base, key) && !clustering_prefix_matches(base, key.key(), update.key())) {
        return false;
    }

    // We want to check if the update modifies any of the columns that are part of the view (in which case the view is
    // affected). But iff the view includes all the base table columns, or the update has either a row deletion or a
    // row marker, we know the view is affected right away.
    if (_schema->view_info()->include_all_columns() || update.row().deleted_at() || update.row().marker().is_live()) {
        return true;
    }

    bool affected = false;
    update.row().cells().for_each_cell_until([&] (column_id id, const atomic_cell_or_collection& cell) {
        affected = _schema->get_column_definition(base.column_at(column_kind::regular_column, id).name());
        return stop_iteration(affected);
    });
    return affected;
}

bool view::matches_view_filter(const ::schema& base, const partition_key& key, const clustering_row& update, gc_clock::time_point now) const {
    return clustering_prefix_matches(base, key, update.key()) &&
                boost::algorithm::all_of(
                    select_statement().get_restrictions()->get_non_pk_restriction() | boost::adaptors::map_values,
                    [&] (auto&& r) {
                        return r->is_satisfied_by(base, key, update.key(), update.cells(), cql3::query_options({ }), now);
                    });
}

} // namespace view
} // namespace db

