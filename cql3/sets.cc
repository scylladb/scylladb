/*
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

#include "sets.hh"
#include "constants.hh"
#include "cql3_type.hh"
#include "types/map.hh"
#include "types/set.hh"

namespace cql3 {
void
sets::setter::execute(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params) {
    expr::constant value = expr::evaluate(*_e, params._options);
    execute(m, row_key, params, column, std::move(value));
}

void
sets::setter::execute(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params, const column_definition& column, const expr::constant& value) {
    if (value.is_unset_value()) {
        return;
    }
    if (column.type->is_multi_cell()) {
        // Delete all cells first, then add new ones
        collection_mutation_description mut;
        mut.tomb = params.make_tombstone_just_before();
        m.set_cell(row_key, column, mut.serialize(*column.type));
    }
    adder::do_add(m, row_key, params, value, column);
}

void
sets::adder::execute(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params) {
    const expr::constant value = expr::evaluate(*_e, params._options);
    if (value.is_unset_value()) {
        return;
    }
    assert(column.type->is_multi_cell()); // "Attempted to add items to a frozen set";
    do_add(m, row_key, params, value, column);
}

void
sets::adder::do_add(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params,
        const expr::constant& value, const column_definition& column) {
    auto& set_type = dynamic_cast<const set_type_impl&>(column.type->without_reversed());
    if (column.type->is_multi_cell()) {
        if (value.is_null()) {
            return;
        }

        utils::chunked_vector<managed_bytes> set_elements = expr::get_set_elements(value);

        if (set_elements.empty()) {
            return;
        }

        // FIXME: collection_mutation_view_description? not compatible with params.make_cell().
        collection_mutation_description mut;

        for (auto&& e : set_elements) {
            mut.cells.emplace_back(to_bytes(e), params.make_cell(*set_type.value_comparator(), bytes_view(), atomic_cell::collection_member::yes));
        }

        m.set_cell(row_key, column, mut.serialize(set_type));
    } else if (!value.is_null()) {
        // for frozen sets, we're overwriting the whole cell
        m.set_cell(row_key, column, params.make_cell(*column.type, value.view()));
    } else {
        m.set_cell(row_key, column, params.make_dead_cell());
    }
}

void
sets::discarder::execute(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params) {
    assert(column.type->is_multi_cell()); // "Attempted to remove items from a frozen set";

    expr::constant svalue = expr::evaluate(*_e, params._options);
    if (svalue.is_null_or_unset()) {
        return;
    }

    collection_mutation_description mut;
    assert(svalue.type->is_set());
    utils::chunked_vector<managed_bytes> set_elements = expr::get_set_elements(svalue);
    mut.cells.reserve(set_elements.size());
    for (auto&& e : set_elements) {
        mut.cells.push_back({to_bytes(e), params.make_dead_cell()});
    }
    m.set_cell(row_key, column, mut.serialize(*column.type));
}

void sets::element_discarder::execute(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params)
{
    assert(column.type->is_multi_cell() && "Attempted to remove items from a frozen set");
    expr::constant elt = expr::evaluate(*_e, params._options);
    if (elt.is_null()) {
        throw exceptions::invalid_request_exception("Invalid null set element");
    }
    collection_mutation_description mut;
    mut.cells.emplace_back(std::move(elt.value).to_bytes(), params.make_dead_cell());
    m.set_cell(row_key, column, mut.serialize(*column.type));
}

}
