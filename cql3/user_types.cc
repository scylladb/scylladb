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
 * Modified by ScyllaDB
 *
 * Copyright (C) 2015-present ScyllaDB
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

#include "cql3/user_types.hh"

#include "cql3/cql3_type.hh"
#include "cql3/constants.hh"

#include <boost/range/adaptor/transformed.hpp>
#include <boost/algorithm/cxx11/any_of.hpp>

#include "types/user.hh"

namespace cql3 {
void user_types::setter::execute(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params) {
    const expr::constant value = expr::evaluate(*_e, params._options);
    execute(m, row_key, params, column, value);
}

void user_types::setter::execute(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params, const column_definition& column, const expr::constant& ut_value) {
    if (ut_value.is_unset_value()) {
        return;
    }

    auto& type = static_cast<const user_type_impl&>(*column.type);
    if (type.is_multi_cell()) {
        // Non-frozen user defined type.

        collection_mutation_description mut;

        // Setting a non-frozen (multi-cell) UDT means overwriting all cells.
        // We start by deleting all existing cells.

        // TODO? (kbraun): is this the desired behaviour?
        // This is how C* does it, but consider the following scenario:
        // create type ut (a int, b int);
        // create table cf (a int primary key, b ut);
        // insert into cf json '{"a": 1, "b":{"a":1, "b":2}}';
        // insert into cf json '{"a": 1, "b":{"a":1}}' default unset;
        // currently this would set b.b to null. However, by specifying 'default unset',
        // perhaps the user intended to leave b.a unchanged.
        mut.tomb = params.make_tombstone_just_before();

        if (!ut_value.is_null()) {
            const auto& elems = expr::get_user_type_elements(ut_value);
            // There might be fewer elements given than fields in the type
            // (e.g. when the user uses a short tuple literal), but never more.
            assert(elems.size() <= type.size());

            for (size_t i = 0; i < elems.size(); ++i) {
                if (!elems[i]) {
                    // This field was not specified or was specified as null.
                    continue;
                }

                mut.cells.emplace_back(serialize_field_index(i),
                        params.make_cell(*type.type(i), *elems[i], atomic_cell::collection_member::yes));
            }
        }

        m.set_cell(row_key, column, mut.serialize(type));
    } else {
        if (!ut_value.is_null()) {
            m.set_cell(row_key, column, params.make_cell(type, ut_value.view()));
        } else {
            m.set_cell(row_key, column, params.make_dead_cell());
        }
    }
}

void user_types::setter_by_field::execute(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params) {
    assert(column.type->is_user_type() && column.type->is_multi_cell());

    auto value = expr::evaluate(*_e, params._options);
    if (value.is_unset_value()) {
        return;
    }

    auto& type = static_cast<const user_type_impl&>(*column.type);

    collection_mutation_description mut;
    mut.cells.emplace_back(serialize_field_index(_field_idx), !value.is_null()
                ? params.make_cell(*type.type(_field_idx), value.view(), atomic_cell::collection_member::yes)
                : params.make_dead_cell());

    m.set_cell(row_key, column, mut.serialize(type));
}

void user_types::deleter_by_field::execute(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params) {
    assert(column.type->is_user_type() && column.type->is_multi_cell());

    collection_mutation_description mut;
    mut.cells.emplace_back(serialize_field_index(_field_idx), params.make_dead_cell());

    m.set_cell(row_key, column, mut.serialize(*column.type));
}

}
