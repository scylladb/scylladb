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
#include "service/broadcast_tables/experimental/lang.hh"

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

void constants::setter::prepare_for_broadcast_tables(service::broadcast_tables::update_query& query) const {
    // FIXME: this works only for constants and bind markers are unimplemented at this point.
    query.new_value = expr::evaluate(*_e, query_options::DEFAULT).to_bytes();
}
}
