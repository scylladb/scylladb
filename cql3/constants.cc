/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "cql3/constants.hh"
#include "cql3/cql3_type.hh"
#include "cql3/statements/strongly_consistent_modification_statement.hh"

namespace cql3 {
void constants::deleter::execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params) {
    if (column.type->is_multi_cell()) {
        collection_mutation_description coll_m;
        coll_m.tomb = params.make_tombstone();

        m.set_cell(prefix, column, coll_m.serialize(*column.type));
    } else {
        m.set_cell(prefix, column, params.make_dead_cell());
    }
}

void
constants::setter::prepare_for_broadcast_tables(statements::broadcast_tables::prepared_update& query) const {
    query.new_value = *_e;
}
}
