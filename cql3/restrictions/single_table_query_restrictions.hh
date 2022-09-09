/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

namespace cql3 {
namespace restrictions {

// Contains analyzed WHERE clause restrictions for a query that
// fetches data from a single table.
struct single_table_query_restrictions {
    // partition_key_restrictions partition_restrictions;
    // clustering_key_restrictions clustering_restrictions;
    // expr::expression other_restrictions;
    // TODO
};
}  // namespace restrictions
}  // namespace cql3
