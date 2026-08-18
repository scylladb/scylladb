/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "cql3/expr/expression.hh"

#include <string_view>

class schema;
class column_definition;

namespace cql3::selection {

class selection;

}

namespace cql3::statements::external_search {

// An external function marks a query as an external search, one only the system serving it can
// answer. Every such call is written and read the same way, so that handling lives here.

/// Reads FN(column, query_value) - the scored column and the value scored against, which must not
/// read from the row. `function_name` spells FN in the rejections.
std::pair<const column_definition*, expr::expression> extract_call_arguments(const expr::function_call& fc,
        std::string_view function_name);

/// Whether two query values are equal: `always` for every possible binding of the markers in them,
/// `never` for none of them, `unknown` when neither of the two has been proved. `unknown` does not
/// say the values differ - two markers may well always be given one value, only that nothing here
/// proves it.
enum class equality { always, never, unknown };

/// Compares the two without evaluating either, so it can be asked at prepare, before a marker has
/// been bound. It settles the two shapes a query value is written as in practice: a literal, which
/// prepare has folded into the value itself, and a bind marker. A vector literal that a marker
/// among its elements left unfolded, a function call and a cast are left `unknown` for the caller
/// to re-ask of the bound values at execution. Two nulls are `always` equal: this is value
/// identity, not the CQL comparison that would answer null.
equality unevaluated_equality(const expr::expression& a, const expr::expression& b);

/// Adds the primary-key columns to those fetched for every row, selected or not: a score arrives
/// keyed by primary key, and that is how it is matched to its row.
void fetch_primary_key_columns(selection::selection& selection, const schema& schema);

} // namespace cql3::statements::external_search
