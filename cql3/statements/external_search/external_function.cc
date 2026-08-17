/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "cql3/statements/external_search/external_function.hh"

#include "cql3/expr/expr-utils.hh"
#include "cql3/selection/selection.hh"
#include "exceptions/exceptions.hh"
#include "schema/schema.hh"

namespace cql3::statements {

const column_definition* extract_scored_column(const expr::function_call& fc, std::string_view function_name) {
    const auto* col_val = expr::as_if<expr::column_value>(&fc.args[0]);
    if (!col_val) {
        throw exceptions::invalid_request_exception(
                seastar::format("First argument to {}() must be a column reference", function_name));
    }
    return col_val->col;
}

expr::expression extract_query_value(const expr::function_call& fc, std::string_view function_name) {
    const expr::expression& query_value = fc.args[1];
    if (expr::find_in_expression<expr::column_value>(query_value, [] (const expr::column_value&) {
            return true;
        })) {
        throw exceptions::invalid_request_exception(
                seastar::format("Second argument to {}() must not be a column reference", function_name));
    }
    return query_value;
}

void fetch_primary_key_columns(selection::selection& selection, const schema& schema) {
    for (const auto& cdef : schema.primary_key_columns()) {
        selection.add_column_for_post_processing(cdef);
    }
}

} // namespace cql3::statements
