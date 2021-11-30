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
 * Copyright (C) 2015-present ScyllaDB
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

#include "maps.hh"
#include "cql3/abstract_marker.hh"
#include "operation.hh"
#include "update_parameters.hh"
#include "exceptions/exceptions.hh"
#include "cql3/cql3_type.hh"
#include "constants.hh"
#include "types/map.hh"

namespace cql3 {
void
maps::setter::execute(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params) {
    expr::constant value = expr::evaluate(*_e, params._options);
    execute(m, row_key, params, column, value);
}

void
maps::setter::execute(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params, const column_definition& column, const expr::constant& value) {
    if (value.is_unset_value()) {
        return;
    }
    if (column.type->is_multi_cell()) {
        // Delete all cells first, then put new ones
        collection_mutation_description mut;
        mut.tomb = params.make_tombstone_just_before();
        m.set_cell(row_key, column, mut.serialize(*column.type));
    }
    do_put(m, row_key, params, value, column);
}

void
maps::setter_by_key::fill_prepare_context(prepare_context& ctx) {
    operation::fill_prepare_context(ctx);
    expr::fill_prepare_context(_k, ctx);
}

void
maps::setter_by_key::execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params) {
    using exceptions::invalid_request_exception;
    assert(column.type->is_multi_cell()); // "Attempted to set a value for a single key on a frozen map"m
    auto key = expr::evaluate(_k, params._options);
    auto value = expr::evaluate(*_e, params._options);
    if (value.is_unset_value()) {
        return;
    }
    if (key.is_unset_value()) {
        throw invalid_request_exception("Invalid unset map key");
    }
    if (key.is_null()) {
        throw invalid_request_exception("Invalid null map key");
    }
    auto ctype = static_cast<const map_type_impl*>(column.type.get());
    auto avalue = !value.is_null() ?
                     params.make_cell(*ctype->get_values_type(), value.view(), atomic_cell::collection_member::yes)
                    : params.make_dead_cell();
    collection_mutation_description update;
    update.cells.emplace_back(std::move(key.value).to_bytes(), std::move(avalue));

    m.set_cell(prefix, column, update.serialize(*ctype));
}

void
maps::putter::execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params) {
    assert(column.type->is_multi_cell()); // "Attempted to add items to a frozen map";
    expr::constant value = expr::evaluate(*_e, params._options);
    if (!value.is_unset_value()) {
        do_put(m, prefix, params, value, column);
    }
}

void
maps::do_put(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params,
        const expr::constant& map_value, const column_definition& column) {
    if (column.type->is_multi_cell()) {
        if (map_value.is_null()) {
            return;
        }

        collection_mutation_description mut;

        auto ctype = static_cast<const map_type_impl*>(column.type.get());
        for (auto&& e : expr::get_map_elements(map_value)) {
            mut.cells.emplace_back(to_bytes(e.first), params.make_cell(*ctype->get_values_type(), raw_value_view::make_value(e.second), atomic_cell::collection_member::yes));
        }

        m.set_cell(prefix, column, mut.serialize(*ctype));
    } else {
        // for frozen maps, we're overwriting the whole cell
        if (map_value.is_null()) {
            m.set_cell(prefix, column, params.make_dead_cell());
        } else {
            m.set_cell(prefix, column, params.make_cell(*column.type, map_value.view()));
        }
    }
}

void
maps::discarder_by_key::execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params) {
    assert(column.type->is_multi_cell()); // "Attempted to delete a single key in a frozen map";
    expr::constant key = expr::evaluate(*_e, params._options);
    if (key.is_null()) {
        throw exceptions::invalid_request_exception("Invalid null map key");
    }
    if (key.is_unset_value()) {
        throw exceptions::invalid_request_exception("Invalid unset map key");
    }
    collection_mutation_description mut;
    mut.cells.emplace_back(std::move(key.value).to_bytes(), params.make_dead_cell());

    m.set_cell(prefix, column, mut.serialize(*column.type));
}

}

