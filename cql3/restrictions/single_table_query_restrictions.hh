/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "partition_key_restrictions.hh"

namespace cql3 {
namespace restrictions {

// Contains analyzed WHERE clause restrictions for a query that
// fetches data from a single table.
struct single_table_query_restrictions {
    partition_key_restrictions partition_restrictions;

    // This will later become a separate class.
    expr::expression clustering_restrictions;
    expr::expression other_restrictions;

    static single_table_query_restrictions make(const expr::expression& prepared_where_clause,
                                                const schema_ptr& table_schema);
};
}  // namespace restrictions
}  // namespace cql3
