/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "single_table_query_restrictions.hh"
#include "cql3/expr/expression.hh"
#include "seastar/core/on_internal_error.hh"

namespace cql3 {
namespace restrictions {
using namespace expr;

extern logging::logger rlogger;

struct split_where_clause {
    // All restrictions that contain partition key columns and no other columns.
    // They will be used to determine partition ranges.
    expression partition;

    // All restrictions that contain partition key columns and no other columns.
    // They will be used to determine clustering ranges.
    expression clustering;

    // All other restrictions from the where clause
    expression other;
};

static split_where_clause get_split_where_clause(const expression& prepared_where_clause) {
    std::vector<expression> partition;
    std::vector<expression> clustering;
    std::vector<expression> other;

    for_each_boolean_factor(prepared_where_clause, [&](const expression& e) {
        bool contains_partition = find_in_expression<column_value>(e, [](const column_value& cval) {
                                      return cval.col->is_partition_key();
                                  }) != nullptr;
        bool contains_clustering = find_in_expression<column_value>(e, [](const column_value& cval) {
                                       return cval.col->is_clustering_key();
                                   }) != nullptr;
        bool contains_other = find_in_expression<column_value>(e, [](const column_value& cval) {
                                  return !cval.col->is_partition_key() && !cval.col->is_clustering_key();
                              }) != nullptr;

        if (contains_partition && !contains_clustering && !contains_other) {
            partition.push_back(e);
        } else if (!contains_partition && contains_clustering && !contains_other) {
            clustering.push_back(e);
        } else {
            other.push_back(e);
        }
    });

    return split_where_clause{.partition = conjunction{std::move(partition)},
                              .clustering = conjunction{std::move(clustering)},
                              .other = conjunction{std::move(other)}};
}

single_table_query_restrictions single_table_query_restrictions::make(const expr::expression& prepared_where_clause,
                                                                      const schema_ptr& table_schema) {
    if (!is_prepared(prepared_where_clause)) {
        on_internal_error(rlogger, "single_table_query_restrictions::make called with unprepared where clause");
    }

    split_where_clause split_where = get_split_where_clause(prepared_where_clause);

    return single_table_query_restrictions{
        .partition_restrictions = partition_key_restrictions(std::move(split_where.partition), table_schema),
        .clustering_restrictions = std::move(split_where.clustering),
        .other_restrictions = std::move(split_where.other),
    };
}

}  // namespace restrictions
}  // namespace cql3
