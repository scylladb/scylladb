/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "cql3/expr/expression.hh"
#include "cql3/expr/temporary_allocator.hh"
#include "cql3/functions/scoring_fcts.hh"
#include "cql3/selection/selection.hh"
#include "cql3/statements/external_search/external_function.hh"
#include "cql3/statements/select_statement.hh"
#include "data_dictionary/data_dictionary.hh"
#include "index/secondary_index.hh"

#include <optional>
#include <vector>

namespace cql3::restrictions {
class select_restrictions;
}

namespace cql3::statements {

/// The clause a search call was written in. Only ORDER BY introduces a search.
enum class search_clause {
    ordering,
    selectors,
    restrictions,
};

/// The rejection of a call whose query value differs from the one the search it names runs with.
/// `clause` is where the call was written: in ORDER BY it collides with another call on the same
/// column, elsewhere with the search ORDER BY introduced.
sstring query_value_mismatch_message(functions::search_family family, std::string_view function_name, search_clause clause);

/// A query value that prepare could not compare with the search's own because of a bind marker.
/// Execution compares the bound values; `function_name` and `clause` describe the call it was
/// written in, for the error message.
struct deferred_query_value {
    expr::expression value;
    sstring function_name;
    search_clause clause;
};

/// One search an external index runs for the statement, identified by its family and column, and
/// the temporaries the values it reports are delivered in.
struct search_source {
    functions::search_family family;
    secondary_index::index index;
    const column_definition* column;
    /// The query vector, or the search term.
    expr::expression query_value;
    /// Allocated by the first call asking for each value. None allocated means the search is only
    /// ordered by.
    external_search::search_temporaries temporaries;
    std::vector<deferred_query_value> deferred;
    /// BM25 only: the WHERE clause's term, likewise compared by execution.
    std::optional<expr::expression> deferred_where_term;

    /// True for a vector index that reports a quantized similarity: the coordinator recomputes it
    /// and reorders the rows by it.
    bool rescores() const;

    bool is_selected() const {
        return temporaries.any();
    }
};

/// The external searches one statement runs, and the replacement of the calls that refer to them.
///
/// A call to a search function (ANN(), BM25(), ...) cannot be evaluated from its arguments: only
/// the index can. Preparation finds every such call, decides which search it refers to, and replaces
/// it with a read of the temporary that search's result is delivered in.
///
/// ORDER BY introduces the searches; a call in SELECT or WHERE must refer to one of them. The
/// clauses are handled by separate methods because they become available at different points of
/// select_statement::prepare(); resolve_ordering() comes first.
class external_search_plan {
    data_dictionary::database _db;
    schema_ptr _schema;
    prepare_context& _ctx;
    expr::temporary_allocator& _temporaries_allocator;
    std::vector<search_source> _sources;
    std::optional<expr::expression> _ordering_expr;

public:
    external_search_plan(data_dictionary::database db, schema_ptr schema, prepare_context& ctx,
            expr::temporary_allocator& temporaries_allocator)
        : _db(db)
        , _schema(std::move(schema))
        , _ctx(ctx)
        , _temporaries_allocator(temporaries_allocator) {
    }

    /// Adds the searches the ORDER BY call names. A bare ANN() or BM25() keeps the index's order;
    /// any other call is replaced like a selector and becomes the score the rows are sorted by.
    void resolve_ordering(const expr::expression& prepared_ordering);

    /// The score the rows are sorted by, when it is not the index's own order. select_statement::prepare()
    /// adds it as a hidden trailing selector for the comparator to read.
    const std::optional<expr::expression>& ordering_expr() const {
        return _ordering_expr;
    }

    /// Replaces every search call in the SELECT clause, nested occurrences included, with a read
    /// of its search's temporary.
    void replace_selectors(std::vector<selection::prepared_selector>& prepared_selectors);

    /// Checks the WHERE clause against the searches: each relation on a search function must
    /// name one of them, and each family accepts a different set of relations.
    void check_restrictions(const restrictions::select_restrictions& restrictions);

    /// True when the statement names no search, i.e. this is an ordinary query.
    bool empty() const {
        return _sources.empty();
    }

    const std::vector<search_source>& sources() const {
        return _sources;
    }

private:
    /// The search the call `fc` refers to, by family and column. In ORDER BY a call with no
    /// matching search adds one; elsewhere it is an error.
    search_source& search_of(const expr::function_call& fc, const functions::external_search_function& fun, search_clause clause);

    /// The expression that replaces `call_expr`: a read of the temporary the value is delivered in,
    /// or the similarity the coordinator computes itself for a rescoring index.
    expr::expression replacement_for(const functions::external_search_function& fun, const expr::expression& call_expr, search_source& source);

    /// Replaces every search call in `e`, nested occurrences included.
    expr::expression replace_search_calls(const expr::expression& e, search_clause clause);

    const search_source* find(functions::search_family family) const;
};

/// Orders by a score column of the result row, descending, rows without a usable score last.
select_statement::ordering_comparator_type descending_score_comparator(size_t score_column_index);

} // namespace cql3::statements
