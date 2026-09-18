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
#include "cql3/statements/select_statement.hh"
#include "data_dictionary/data_dictionary.hh"
#include "index/secondary_index.hh"

#include <optional>
#include <vector>

namespace cql3::restrictions {
class statement_restrictions;
}

namespace cql3::statements {

/// The clause a call was written in. Only ORDER BY introduces a search; a call in SELECT or WHERE
/// must refer to one the ORDER BY introduced, and each clause has its own error message when it
/// does not.
enum class search_clause {
    ordering,
    selectors,
    restrictions,
};

/// A query value written in a SELECT or WHERE call that prepare could not compare with the ORDER BY
/// one, because a bind marker is involved. Execution compares the bound values.
struct deferred_query_value {
    expr::expression value;
    /// The error to raise when the bound values turn out to differ, built here while the call the
    /// user wrote is still known.
    sstring disagreement_message;
};

/// One search an external index will be asked to run: the source of the values the calls referring
/// to it return, and the temporaries those values are delivered in.
///
/// A search is identified by its family and the column it searches, so every call on that column
/// refers to the same search, and a query using several of them makes one request. All calls
/// referring to one search must use the same query value; where a bind marker makes that
/// undecidable at prepare time, the comparison is recorded in `deferred` for execution.
struct search_source {
    functions::search_family family;
    secondary_index::index index;
    /// The column searched: the vector an ANN search is near to, the text a BM25 search matches.
    const column_definition* column;
    /// The query vector, or the search term.
    expr::expression query_value;
    /// ANN only: the index recomputes the similarity on the coordinator and reorders by it.
    bool is_rescoring_enabled = false;

    /// Temporaries the score and the rank are delivered in, each allocated on the first call asking
    /// for it. A search with neither allocated is only ordered by: no value of it is returned.
    std::optional<size_t> score_slot;
    std::optional<size_t> rank_slot;
    /// BM25 only: the temporary an excerpt of the searched text is delivered in, fetched by a second
    /// request from the text of `column`.
    std::optional<size_t> fragment_slot;

    std::vector<deferred_query_value> deferred;

    /// True when the query returns some value of this search.
    bool is_selected() const {
        return score_slot || rank_slot || fragment_slot;
    }

    /// True when the rows have to be matched to this search's answer by primary key, which a score
    /// or a rank is. An excerpt is matched by position instead.
    bool needs_primary_key() const {
        return score_slot || rank_slot;
    }
};

/// Arguments every external-search SELECT statement is built from, bundled so that
/// select_statement::prepare() need not know what family of search - or how many - it is building for.
struct external_statement_args {
    schema_ptr schema;
    uint32_t bound_terms;
    lw_shared_ptr<const raw::select_statement::parameters> parameters;
    ::shared_ptr<selection::selection> selection;
    ::shared_ptr<const restrictions::statement_restrictions> restrictions;
    ::shared_ptr<std::vector<size_t>> group_by_cell_indices;
    bool is_reversed;
    select_statement::ordering_comparator_type ordering_comparator;
    std::optional<expr::expression> limit;
    std::optional<expr::expression> per_partition_limit;
    cql_stats& stats;
    std::unique_ptr<cql3::attributes> attrs;
};

/// The external searches one statement runs, and the lowering of the calls that refer to them.
///
/// A call to an external function (ANN(), BM25(), ...) cannot be evaluated from its arguments: only
/// the index can. Preparation finds every such call in ORDER BY, SELECT and WHERE, decides which
/// search it refers to, and replaces it with a read of the temporary that search's result is
/// delivered in (or, for a rescoring vector index, with the similarity the coordinator computes).
///
/// ORDER BY is the clause that introduces a search; a call in SELECT or WHERE must refer to one the
/// ORDER BY introduced. The clauses are bound by separate methods only because they become available
/// at different points of select_statement::prepare().
class external_search_plan {
    data_dictionary::database _db;
    schema_ptr _schema;
    prepare_context& _ctx;
    expr::temporary_allocator& _temporaries_allocator;
    std::vector<search_source> _sources;
    std::optional<expr::expression> _ordering_expr;

public:
    external_search_plan(data_dictionary::database db, schema_ptr schema, prepare_context& ctx,
            expr::temporary_allocator& temporaries_allocator);

    /// Resolves the searches the ORDER BY expression refers to and lowers every call in it. Rejects
    /// an expression with no such call: the regular-ordering path skips a scoring ordering, so it
    /// would otherwise be silently ignored.
    void bind_ordering(const expr::expression& prepared_ordering);

    /// Lowers every external call in the SELECT clause, nested occurrences included. A call referring
    /// to a search the ORDER BY did not introduce is rejected.
    void bind_selectors(std::vector<selection::prepared_selector>& prepared_selectors);

    /// Validates the relations statement_restrictions held out of the filtering machinery for the
    /// search they refer to. A relation referring to no search is rejected: nothing would interpret
    /// it, and it would be silently dropped rather than applied.
    void bind_restrictions(const restrictions::statement_restrictions& restrictions);

    /// True when the statement names no search, i.e. this is an ordinary query.
    bool empty() const {
        return _sources.empty();
    }

    /// The expression the rows are sorted by when the index's own order is not the requested one: a
    /// fusion of several searches, arithmetic over a search's score, or a rescoring vector index.
    /// select_statement::prepare() adds it as a hidden trailing selector for the comparator to read.
    /// Empty when the rows are returned in the index's order.
    const std::optional<expr::expression>& ordering_expr() const {
        return _ordering_expr;
    }

    const std::vector<search_source>& sources() const {
        return _sources;
    }

    ::shared_ptr<select_statement> make_statement(external_statement_args args) const;

private:
    /// The search the call `fc` refers to. In ORDER BY a call with no matching search adds one;
    /// elsewhere it is an error. A query value that only execution can compare with the search's is
    /// recorded in the search's `deferred`.
    search_source& search_of(const expr::function_call& fc, const functions::external_search_function& fun, search_clause clause);

    /// The expression that replaces the call `call_expr`, allocating the temporary it reads on first
    /// use. Sets `unnamed` when the result does not format as the call, so that an unaliased selector
    /// holding it needs an explicit name.
    expr::expression replacement_for(const functions::external_search_function& fun, const expr::expression& call_expr, search_source& source,
            bool& unnamed);

    /// Lowers every external call in `e`, nested occurrences included.
    expr::expression lower(const expr::expression& e, search_clause clause, bool& lowered_any, bool& unnamed);
};

/// A comparator ranking result rows by the float in column `column_index`, descending, with rows
/// whose score is null or not a finite number last. Used with the hidden selector added for
/// ordering_expr(), which is a float.
select_statement::ordering_comparator_type descending_score_ordering_comparator(uint32_t column_index);

} // namespace cql3::statements
