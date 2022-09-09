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
    expr::single_column_restrictions_map _single_column_partition_key_restrictions;

    // Analyzed restrictions

    // Single column restrictions for each column which restrict it to a list of values.
    // If each partition key column is restricted to a list of values
    // then we can generate a number of token values as partition range.
    expr::single_column_restrictions_map _column_eq_restrictions;

    // Restrictions which restrict the partition token to a range of values.
    expr::expression _token_range_restrictions;

    // Restrictions that will be used for filtering.
    expr::expression _filtering_restrictions;

public:
    partition_key_restrictions(expr::expression partition_restrictions, schema_ptr table_schema);

    // Access expression containing all parittion key restrictions.
    const expr::expression& get_partition_key_restrictions() const;

    // Get a map which keeps single column restrictions for each partition key column.
    const expr::single_column_restrictions_map& get_single_column_partition_key_restrictions() const;

    // Check if there are no partition key restrictions.
    bool partition_key_restrictions_is_empty() const;

    // Check that partition key restrictions contain only binary operaters with = operation.
    bool partition_key_restrictions_is_all_eq() const;

private:
    // Analyze _partition_restrictions to fill in _column_eq_restrictions, _token_range_restrictions
    // and _filtering_restrictions.
    void analyze_restrictions();

    // If each partition key column has a restriction that restricts it to a list of values
    // we can use these restrictions to generate a list of partition ranges.
    // Otherwise these restrictions have to be filtered.
    bool all_columns_have_eq_restrictions() const;
};
}  // namespace restrictions
}  // namespace cql3
