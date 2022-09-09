/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "cql3/expr/expression.hh"

namespace cql3 {
namespace restrictions {

// Restrictions containing only partition key columns, extracted from the WHERE clause.
class partition_key_restrictions {
    schema_ptr _table_schema;

    expr::expression _partition_restrictions;

public:
    partition_key_restrictions(expr::expression partition_restrictions, schema_ptr table_schema);

    // TODO
};
}  // namespace restrictions
}  // namespace cql3
