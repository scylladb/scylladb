/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "cql3/expr/expression.hh"
#include "cql3/expr/temporary_allocator.hh"
#include "cql3/selection/selector.hh"

#include <optional>
#include <string_view>

class schema;
class column_definition;

namespace cql3::selection {

class selection;

}

namespace cql3::functions {

enum class search_value;

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

/// The temporaries holding the score and the rank of one search. Each is allocated by the first
/// SELECT call asking for that value and filled per row by external_search_provider from the
/// index's response.
struct search_temporaries {
    std::optional<size_t> score;
    std::optional<size_t> rank;

    /// True when the query returns some value of the search.
    bool any() const {
        return score.has_value() || rank.has_value();
    }
};

/// The expression that replaces `call`, a call to a search function returning `value`: a read of
/// the temporary holding that value, allocated if this is the first call asking for it, or for the
/// (score, rank) pair a tuple of the two reads. Every call asking for one value reads the same
/// temporary, so BM25() and BM25_SCORE() in one query share the score's. A temporary is typed by
/// the function's declared return type, since that is what the selector reading it was typed by.
///
/// A temporary that replaces a whole call carries the call in replaced_expr, so an unaliased
/// selector still formats as the call. The two temporaries inside the tuple carry no call; see
/// name_selector_as_written().
expr::expression replace_search_call(functions::search_value value, const expr::expression& call, search_temporaries& temporaries,
        expr::temporary_allocator& allocator);

/// Names an unaliased selector after `written`, the expression the user wrote, when what it was
/// replaced with would not format as it. "SELECT BM25(c, t)" is replaced with a tuple of two
/// temporaries, which would otherwise be named "(system.temporary(0), system.temporary(1))".
void name_selector_as_written(selection::prepared_selector& selector, const expr::expression& written);

} // namespace cql3::statements::external_search
