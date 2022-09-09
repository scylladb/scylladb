/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "partition_key_restrictions.hh"
#include "cql3/expr/expression.hh"

namespace cql3 {
namespace restrictions {
extern logging::logger rlogger;

using namespace expr;

static void assert_contains_only_partition_key_columns(const expr::expression& e) {
    const column_value* non_partition_column =
        find_in_expression<column_value>(e, [](const column_value& cval) { return !cval.col->is_partition_key(); });

    if (non_partition_column != nullptr) {
        on_internal_error(
            rlogger,
            format("partition_key_restrictions were given an expression that contains a non-parition column: {}",
                   non_partition_column->col->name_as_text()));
    }
}

partition_key_restrictions::partition_key_restrictions(expr::expression partition_restrictions, schema_ptr table_schema)
    : _table_schema(std::move(table_schema)), _partition_restrictions(std::move(partition_restrictions)) {
    if (!is_prepared(_partition_restrictions)) {
        on_internal_error(rlogger, format("partition_key_restrictions were given an unprepared expression: {}",
                                          _partition_restrictions));
    }

    assert_contains_only_partition_key_columns(_partition_restrictions);

    _single_column_partition_key_restrictions = expr::get_single_column_restrictions_map(_partition_restrictions);
}

const expr::expression& partition_key_restrictions::get_partition_key_restrictions() const {
    return _partition_restrictions;
}

const expr::single_column_restrictions_map& partition_key_restrictions::get_single_column_partition_key_restrictions()
    const {
    return _single_column_partition_key_restrictions;
}

// Check if there are no partition key restrictions.
bool partition_key_restrictions::partition_key_restrictions_is_empty() const {
    return is_empty_restriction(_partition_restrictions);
}
}  // namespace restrictions
}  // namespace cql3
