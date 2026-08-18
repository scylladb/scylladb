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

namespace cql3::statements {

// An external function - BM25(), ANN() - is a function of a row that this node cannot answer on its
// own: only the external search system the query is served by can.  A query may name one in several
// clauses, and what it means is the same in each of them, so the ways the clauses have to agree, and
// the reading of a call that makes agreement decidable, are the same for every external function and
// for every search.  This is what they share; what one search does with a call, and how the values
// it stands for arrive, is its own business.

/// Compares the value one external-function call scores against with the value the call the rows are
/// ranked by scores against - every call in a statement has to score against the same one - and
/// rejects them with `disagreement_message` when prepare can tell that they differ.
///
/// Returns the value to compare again once it can be evaluated, when that is all that is left to
/// tell the two apart - a bind marker in either of them, most often; std::nullopt when prepare has
/// settled the matter.
std::optional<expr::expression> check_query_value(const expr::expression& value, const expr::expression& ranking_value,
        std::string_view disagreement_message);

/// A scoring function scores one column of each row against a value the caller supplies, so its
/// two arguments are checked the same way: the first names the scored column, and the second is
/// the value to score against, which cannot be read from the row being scored.  `function_name`
/// names the function in the rejection message.
const column_definition* extract_scored_column(const expr::function_call& fc, std::string_view function_name);
expr::expression extract_query_value(const expr::function_call& fc, std::string_view function_name);

/// Asks for the primary-key columns of every fetched row.  Needed when a score is delivered in a
/// temporary slot, because the provider filling it matches each replica row against the external
/// system's response by primary key - so those columns have to be read even when the query does
/// not select them, as in `SELECT BM25(...)`.
void fetch_primary_key_columns(selection::selection& selection, const schema& schema);

} // namespace cql3::statements
