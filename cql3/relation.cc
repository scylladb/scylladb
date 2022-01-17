/*
 */

/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "relation.hh"
#include "exceptions/unrecognized_entity_exception.hh"

namespace cql3 {

const column_definition&
relation::to_column_definition(const schema& schema, const column_identifier::raw& entity) {
    auto id = entity.prepare_column_identifier(schema);
    auto def = get_column_definition(schema, *id);
    if (!def || def->is_hidden_from_cql()) {
        throw exceptions::unrecognized_entity_exception(*id, to_string());
    }
    return *def;
}

}
