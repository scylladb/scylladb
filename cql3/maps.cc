/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "utils/assert.hh"
#include "maps.hh"
#include "operation.hh"
#include "update_parameters.hh"
#include "exceptions/exceptions.hh"
#include "cql3/expr/evaluate.hh"
#include "cql3/expr/expr-utils.hh"
#include "mutation/mutation.hh"
#include "types/map.hh"

namespace cql3 {
void
maps::setter::execute(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params) {
    cql3::raw_value value = expr::evaluate(*_e, params._options);
    execute(m, row_key, params, column, value);
}

void
maps::setter::execute(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params, const column_definition& column, const cql3::raw_value& value) {
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
    SCYLLA_ASSERT(column.type->is_multi_cell()); // "Attempted to set a value for a single key on a frozen map"m
    auto key = expr::evaluate(_k, params._options);
    auto value = expr::evaluate(*_e, params._options);
    if (key.is_null()) {
        throw invalid_request_exception("Invalid null map key");
    }
    auto ctype = static_cast<const map_type_impl*>(column.type.get());
    auto avalue = !value.is_null() ?
                     params.make_cell(*ctype->get_values_type(), value.view(), atomic_cell::collection_member::yes)
                    : params.make_dead_cell();
    collection_mutation_description update;
    update.cells.emplace_back(std::move(key).to_bytes(), std::move(avalue));

    m.set_cell(prefix, column, update.serialize(*ctype));
}

void
maps::putter::execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params) {
    SCYLLA_ASSERT(column.type->is_multi_cell()); // "Attempted to add items to a frozen map";
    cql3::raw_value value = expr::evaluate(*_e, params._options);
    do_put(m, prefix, params, value, column);
}

void
maps::do_put(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params,
        const cql3::raw_value& map_value, const column_definition& column) {
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
    SCYLLA_ASSERT(column.type->is_multi_cell()); // "Attempted to delete a single key in a frozen map";
    cql3::raw_value key = expr::evaluate(*_e, params._options);
    if (key.is_null()) {
        throw exceptions::invalid_request_exception("Invalid null map key");
    }
    collection_mutation_description mut;
    mut.cells.emplace_back(std::move(key).to_bytes(), params.make_dead_cell());

    m.set_cell(prefix, column, mut.serialize(*column.type));
}

}

