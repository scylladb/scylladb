/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "utils/assert.hh"
#include "sets.hh"
#include "types/set.hh"
#include "cql3/expr/evaluate.hh"
#include "cql3/expr/expr-utils.hh"

namespace cql3 {
void
sets::setter::execute(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params) {
    cql3::raw_value value = expr::evaluate(*_e, params._options);
    execute(m, row_key, params, column, std::move(value));
}

void
sets::setter::execute(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params, const column_definition& column, const cql3::raw_value& value) {
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
    const cql3::raw_value value = expr::evaluate(*_e, params._options);
    SCYLLA_ASSERT(column.type->is_multi_cell()); // "Attempted to add items to a frozen set";
    do_add(m, row_key, params, value, column);
}

void
sets::adder::do_add(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params,
        const cql3::raw_value& value, const column_definition& column) {
    auto& set_type = dynamic_cast<const set_type_impl&>(column.type->without_reversed());
    if (column.type->is_multi_cell()) {
        if (value.is_null()) {
            return;
        }

        utils::chunked_vector<managed_bytes_opt> set_elements = expr::get_set_elements(value);

        if (set_elements.empty()) {
            return;
        }

        // FIXME: collection_mutation_view_description? not compatible with params.make_cell().
        collection_mutation_description mut;

        for (auto&& e : set_elements) {
            if (!e) {
                throw exceptions::invalid_request_exception("Invalid NULL value in set");
            }
            mut.cells.emplace_back(to_bytes(*e), params.make_cell(*set_type.value_comparator(), bytes_view(), atomic_cell::collection_member::yes));
        }

        m.set_cell(row_key, column, mut.serialize(set_type));
    } else if (!value.is_null()) {
        // for frozen sets, we're overwriting the whole cell
        value.view().with_value([&] (const FragmentedView auto& v) {
            set_type.validate_for_storage(v);
        });
        m.set_cell(row_key, column, params.make_cell(*column.type, value.view()));
    } else {
        m.set_cell(row_key, column, params.make_dead_cell());
    }
}

void
sets::discarder::execute(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params) {
    SCYLLA_ASSERT(column.type->is_multi_cell()); // "Attempted to remove items from a frozen set";

    cql3::raw_value svalue = expr::evaluate(*_e, params._options);
    if (svalue.is_null()) {
        return;
    }

    collection_mutation_description mut;
    utils::chunked_vector<managed_bytes_opt> set_elements = expr::get_set_elements(svalue);
    mut.cells.reserve(set_elements.size());
    for (auto&& e : set_elements) {
        if (!e) {
            throw exceptions::invalid_request_exception("Invalid NULL value in set");
        }
        mut.cells.push_back({to_bytes(*e), params.make_dead_cell()});
    }
    m.set_cell(row_key, column, mut.serialize(*column.type));
}

void sets::element_discarder::execute(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params)
{
    SCYLLA_ASSERT(column.type->is_multi_cell() && "Attempted to remove items from a frozen set");
    cql3::raw_value elt = expr::evaluate(*_e, params._options);
    if (elt.is_null()) {
        throw exceptions::invalid_request_exception("Invalid null set element");
    }
    collection_mutation_description mut;
    mut.cells.emplace_back(std::move(elt).to_bytes(), params.make_dead_cell());
    m.set_cell(row_key, column, mut.serialize(*column.type));
}

}
