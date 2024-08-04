/*
 * Modified by ScyllaDB
 *
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "utils/assert.hh"
#include "cql3/user_types.hh"

#include "cql3/expr/evaluate.hh"
#include "cql3/expr/expr-utils.hh"

#include <boost/range/adaptor/transformed.hpp>
#include <boost/algorithm/cxx11/any_of.hpp>

#include "mutation/mutation.hh"
#include "types/user.hh"

namespace cql3 {
void user_types::setter::execute(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params) {
    const cql3::raw_value value = expr::evaluate(*_e, params._options);
    execute(m, row_key, params, column, value);
}

void user_types::setter::execute(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params, const column_definition& column, const cql3::raw_value& ut_value) {
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
            const auto& elems = expr::get_user_type_elements(ut_value, type);
            // There might be fewer elements given than fields in the type
            // (e.g. when the user uses a short tuple literal), but never more.
            SCYLLA_ASSERT(elems.size() <= type.size());

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
    SCYLLA_ASSERT(column.type->is_user_type() && column.type->is_multi_cell());

    auto value = expr::evaluate(*_e, params._options);

    auto& type = static_cast<const user_type_impl&>(*column.type);

    collection_mutation_description mut;
    mut.cells.emplace_back(serialize_field_index(_field_idx), !value.is_null()
                ? params.make_cell(*type.type(_field_idx), value.view(), atomic_cell::collection_member::yes)
                : params.make_dead_cell());

    m.set_cell(row_key, column, mut.serialize(type));
}

void user_types::deleter_by_field::execute(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params) {
    SCYLLA_ASSERT(column.type->is_user_type() && column.type->is_multi_cell());

    collection_mutation_description mut;
    mut.cells.emplace_back(serialize_field_index(_field_idx), params.make_dead_cell());

    m.set_cell(row_key, column, mut.serialize(*column.type));
}

}
