/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "expression.hh"
#include "data_dictionary/data_dictionary.hh"
#include <map>

namespace cql3 {
namespace expr {

// A comparator that orders columns by their position in the schema
// For primary key columns the `id` field is used to determine their position.
// Other columns are assumed to have position std::numeric_limits<uint32_t>::max().
// In case the position is the same they are compared by their name.
// This comparator has been used in the original restricitons code to keep
// restrictions for each column sorted by their place in the schema.
// It's not recommended to use this comparator with columns of different kind
// (partition/clustering/nonprimary) because the id field is unique
// for (kind, schema). So a partition and clustering column might
// have the same id within one schema.
struct schema_pos_column_definition_comparator {
    bool operator()(const column_definition* def1, const column_definition* def2) const;
};

// A map of single column restrictions for each column
using single_column_restrictions_map = std::map<const column_definition*, expression, schema_pos_column_definition_comparator>;


// Given a restriction from the WHERE clause prepares it and performs some validation checks.
// It will also fill the prepare context automatically, there's no need to do that later.
binary_operator validate_and_prepare_new_restriction(const binary_operator& restriction,
                                                     data_dictionary::database db,
                                                     schema_ptr schema,
                                                     prepare_context& ctx);

} // namespace expr
} // namespace cql3
