/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "cql3/expr/expression.hh"

#include <optional>
#include <string_view>

class schema;
class column_definition;

namespace cql3::selection {

class selection;

}

namespace cql3::statements::external_search {

// An external function marks a query as an external search, one only the system serving it can
// answer. Every such call is written and read the same way, so that handling lives here.

/// Compares `value` and `ranking_value` if both are constants, throwing `disagreement_message` when
/// they differ; otherwise defers the check, returning `value` for execution to compare.
std::optional<expr::expression> check_query_value(const expr::expression& value, const expr::expression& ranking_value,
        std::string_view disagreement_message);

/// Reads FN(column, query_value) - the scored column and the value scored against, which must not
/// read from the row.  `function_name` spells FN in the rejections.
std::pair<const column_definition*, expr::expression> extract_call_arguments(const expr::function_call& fc,
        std::string_view function_name);

/// Adds the primary-key columns to those fetched for every row, selected or not: a score arrives
/// keyed by primary key, and that is how it is matched to its row.
void fetch_primary_key_columns(selection::selection& selection, const schema& schema);

} // namespace cql3::statements::external_search
