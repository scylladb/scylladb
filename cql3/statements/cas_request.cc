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
 * Copyright (C) 2019 ScyllaDB
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

#include "modification_statement.hh"
#include "cas_request.hh"
#include <seastar/core/sleep.hh>

namespace cql3::statements {

using namespace std::chrono;

void cas_request::add_row_update(modification_statement &stmt_arg,
        std::vector<query::clustering_range> ranges_arg,
        modification_statement::json_cache_opt json_cache_arg,
        const query_options& options_arg) {
    // TODO: reserve updates array for batches
    _updates.emplace_back(cas_row_update{
        .statement = stmt_arg,
        .ranges = std::move(ranges_arg),
        .json_cache = std::move(json_cache_arg),
        .options = options_arg});
}

std::optional<mutation> cas_request::apply_updates(api::timestamp_type ts) const {
    // We're working with a single partition, so there will be only one element
    // in the vector. A vector is used since this is a conventional format
    // to pass a mutation onward.
    std::optional<mutation> mutation_set;
    for (const cas_row_update& op: _updates) {
        update_parameters params(_schema, op.options, ts, op.statement.get_time_to_live(op.options), *_rows);

        std::vector<mutation> statement_mutations = op.statement.apply_updates(_key, op.ranges, params, op.json_cache);
        // Append all mutations (in fact only one) to the consolidated one.
        for (mutation& m : statement_mutations) {
            if (mutation_set.has_value() == false) {
                mutation_set.emplace(std::move(m));
            } else {
                mutation_set->apply(std::move(m));
            }
        }
    }

    return mutation_set;
}

lw_shared_ptr<query::read_command> cas_request::read_command() const {

    column_mask columns_to_read;
    std::vector<query::clustering_range> ranges;

    for (const cas_row_update& op : _updates) {
        if (op.statement.has_conditions() == false && op.statement.requires_read() == false) {
            // No point in pre-fetching the old row if the statement doesn't check it in a CAS and
            // doesn't use it to apply updates.
            continue;
        }
        columns_to_read.union_with(op.statement.columns_to_read());
        ranges.reserve(op.ranges.size());
        std::copy(op.ranges.begin(), op.ranges.end(), std::back_inserter(ranges));
    }
    ranges = query::clustering_range::deoverlap(std::move(ranges), clustering_key::tri_compare(*_schema));

    query::partition_slice ps(std::move(ranges), *_schema, columns_to_read, update_parameters::options);
    return make_lw_shared<query::read_command>(_schema->id(), _schema->version(), std::move(ps));
}

bool cas_request::applies_to() const {

    const partition_key& pkey = _key.front().start()->value().key().value();
    const clustering_key empty_ckey = clustering_key::make_empty();
    return std::all_of(_updates.begin(), _updates.end(), [this, &pkey, &empty_ckey] (const cas_row_update& op) {
        if (op.statement.has_conditions() == false) {
            return true;
        }
        // If primary key contains only one column, there is no clustering key, so clustering key restrictions may be empty
        // (open ended on both sides of the range). Another case when clustering key may be missing is when CAS update applies
        // only to static columns.
        const clustering_key& ckey = op.ranges.front().start() ? op.ranges.front().start()->value() : empty_ckey;
        const update_parameters::prefetch_data::row *row = _rows->find_row(pkey, ckey);
        return op.statement.applies_to(row, op.options);
    });
}

std::optional<mutation> cas_request::apply(query::result& qr,
        const query::partition_slice& slice, api::timestamp_type ts) {
    _rows = update_parameters::build_prefetch_data(_schema, qr, slice);
    if (applies_to()) {
        return apply_updates(ts);
    } else {
        return {};
    }
}

} // end of namespace "cql3::statements"
