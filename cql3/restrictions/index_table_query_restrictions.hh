/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "single_table_query_restrictions.hh"

namespace cql3 {
namespace restrictions {

// Contains analyzed WHERE clause restrictions for a query that
// uses an index to fetch data.
struct index_table_query_restrictions {
    // index table query could be represented as two single table queries
    // TODO
    single_table_query_restrictions index_table_query;
    single_table_query_restrictions base_table_query;
};
}  // namespace restrictions
}  // namespace cql3
