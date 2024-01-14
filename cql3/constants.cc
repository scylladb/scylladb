/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "cql3/constants.hh"
#include "cql3/expr/evaluate.hh"

namespace cql3 {

void
constants::setter::execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params) {
    auto value = expr::evaluate(*_e, params._options);
    execute(m, prefix, params, column, value.view());
}

void
constants::setter::execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params, const column_definition& column, cql3::raw_value_view value) {
    if (value.is_null()) {
        m.set_cell(prefix, column, params.make_dead_cell());
    } else if (value.is_value()) {
        m.set_cell(prefix, column, params.make_cell(*column.type, value));
    }
}

void
constants::adder::execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params) {
    auto value = expr::evaluate(*_e, params._options);
    if (value.is_null()) {
        throw exceptions::invalid_request_exception("Invalid null value for counter increment");
    }
    auto increment = value.view().deserialize<int64_t>(*long_type);
    m.set_cell(prefix, column, params.make_counter_update_cell(increment));
}

void
constants::subtracter::execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params) {
    auto value = expr::evaluate(*_e, params._options);
    if (value.is_null()) {
        throw exceptions::invalid_request_exception("Invalid null value for counter increment");
    }
    auto increment = value.view().deserialize<int64_t>(*long_type);
    if (increment == std::numeric_limits<int64_t>::min()) {
        throw exceptions::invalid_request_exception(format("The negation of {:d} overflows supported counter precision (signed 8 bytes integer)", increment));
    }
    m.set_cell(prefix, column, params.make_counter_update_cell(-increment));
}

void constants::deleter::execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params) {
    if (column.type->is_multi_cell()) {
        collection_mutation_description coll_m;
        coll_m.tomb = params.make_tombstone();

        m.set_cell(prefix, column, coll_m.serialize(*column.type));
    } else {
        m.set_cell(prefix, column, params.make_dead_cell());
    }
}

expr::expression
constants::setter::prepare_new_value_for_broadcast_tables() const {
    return *_e;
}
}
