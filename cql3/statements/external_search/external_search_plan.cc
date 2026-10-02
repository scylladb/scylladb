/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "cql3/statements/external_search/external_search_plan.hh"

#include "cql3/statements/external_search/ann_search.hh"
#include "cql3/statements/external_search/bm25_search.hh"
#include "index/vector_index.hh"

#include "cql3/expr/expr-utils.hh"
#include "cql3/functions/scoring_fcts.hh"
#include "cql3/selection/selection.hh"
#include "cql3/statements/external_search/external_function.hh"
#include "data_dictionary/data_dictionary.hh"
#include "exceptions/exceptions.hh"
#include "cql3/restrictions/statement_restrictions.hh"
#include "index/secondary_index_manager.hh"
#include "types/types.hh"
#include "utils/assert.hh"

#include <algorithm>
#include <cmath>
#include <ranges>
#include <utility>

namespace cql3::statements {

namespace {

secondary_index::index index_for(functions::search_family family, data_dictionary::database db, const schema_ptr& schema,
        const column_definition& column) {
    switch (family) {
    case functions::search_family::ann:
        return ann_search::index_for(db, schema, column);
    case functions::search_family::bm25:
        return bm25_search::index_for(db, schema, column);
    }
    std::unreachable();
}

std::string_view clause_name(search_clause clause) {
    switch (clause) {
    case search_clause::ordering:
        return "ORDER BY";
    case search_clause::selectors:
        return "SELECT";
    case search_clause::restrictions:
        return "WHERE";
    }
    std::unreachable();
}

/// The rejection of a call in SELECT or WHERE when ORDER BY has no search of its family. In WHERE
/// the call is named after its family, the relation arriving rewritten to the score function (see
/// prepare_external_search_relation_lhs()); an ANN relation is rejected before it gets here.
sstring no_search_message(const functions::external_search_function& fun, search_clause clause) {
    switch (fun.family()) {
    case functions::search_family::ann:
        return seastar::format("{}() in {} must match an ANN search in ORDER BY, with the same column and query vector; "
                "ORDER BY has no ANN search", fun.display_name(), clause_name(clause));
    case functions::search_family::bm25:
        return seastar::format("{}() in {} must match a BM25 search in ORDER BY, with the same column and search term; "
                "ORDER BY has no BM25 search", clause == search_clause::restrictions ? "BM25" : fun.display_name(), clause_name(clause));
    }
    std::unreachable();
}

/// The same, when ORDER BY searches the family, but on another column.
sstring column_mismatch_message(const functions::external_search_function& fun, search_clause clause, const column_definition& column) {
    switch (fun.family()) {
    case functions::search_family::ann:
        return seastar::format("{}() in {} must match an ANN search in ORDER BY, with the same column and query vector; "
                "ORDER BY has no ANN search on column {}", fun.display_name(), clause_name(clause), column.name_as_cql_string());
    case functions::search_family::bm25:
        return seastar::format("{}() in {} must match a BM25 search in ORDER BY, with the same column and search term; "
                "ORDER BY has no BM25 search on column {}", clause == search_clause::restrictions ? "BM25" : fun.display_name(),
                clause_name(clause), column.name_as_cql_string());
    }
    std::unreachable();
}

/// The name of the function an ORDER BY clause calls, as a user writes it: BM25_RANK for a search
/// function, a native function's name alone, a user-defined function's with its keyspace.
sstring ordering_function_name(const expr::function_call& fc) {
    if (const auto* search = functions::as_external_search_function(fc)) {
        return sstring(search->display_name());
    }
    const auto& name = std::get<shared_ptr<db::functions::function>>(fc.func)->name();
    return name == name.as_native_function() ? name.name : seastar::format("{}", name);
}

bool has_other_restrictions(const restrictions::select_restrictions& restrictions) {
    return !restrictions.partition_key_restrictions_is_empty()
            || !restrictions::is_empty_restriction(restrictions.get_clustering_columns_restrictions())
            || !restrictions::is_empty_restriction(restrictions.get_nonprimary_key_restrictions());
}

} // anonymous namespace

sstring query_value_mismatch_message(functions::search_family family, std::string_view function_name, search_clause clause) {
    // A column is searched once per query, so two ORDER BY calls on one column are one search and
    // have to agree; a call anywhere else has to agree with the search ORDER BY introduced.
    switch (family) {
    case functions::search_family::ann:
        return clause == search_clause::ordering
                ? seastar::format("{}() in ORDER BY must use the same query vector as the other ANN calls on the same column", function_name)
                : seastar::format("{}() in {} must match an ANN search in ORDER BY, with the same column and query vector; "
                        "the query vector differs", function_name, clause_name(clause));
    case functions::search_family::bm25:
        return clause == search_clause::ordering
                ? seastar::format("{}() in ORDER BY must use the same search term as the other BM25 calls on the same column", function_name)
                : seastar::format("{}() in {} must match a BM25 search in ORDER BY, with the same column and search term; "
                        "the search term differs", clause == search_clause::restrictions ? "BM25" : function_name, clause_name(clause));
    }
    std::unreachable();
}

bool search_source::rescores() const {
    return family == functions::search_family::ann && secondary_index::vector_index::is_rescoring_enabled(index.metadata().options());
}

const search_source* external_search_plan::find(functions::search_family family) const {
    auto it = std::ranges::find(_sources, family, &search_source::family);
    return it == _sources.end() ? nullptr : &*it;
}

search_source& external_search_plan::search_of(const expr::function_call& fc, const functions::external_search_function& fun,
        search_clause clause) {
    auto [column, query_value] = external_search::extract_call_arguments(fc, fun.display_name());

    auto it = std::ranges::find_if(_sources, [&] (const search_source& source) {
        return source.family == fun.family() && source.column == column;
    });
    if (it == _sources.end()) {
        if (clause == search_clause::ordering) {
            _sources.push_back(search_source{
                    .family = fun.family(),
                    .index = index_for(fun.family(), _db, _schema, *column),
                    .column = column,
                    .query_value = std::move(query_value),
            });
            return _sources.back();
        }
        // Any call of the family on the column in ORDER BY introduces the search.
        const bool on_another_column = find(fun.family()) != nullptr;
        throw exceptions::invalid_request_exception(
                on_another_column ? column_mismatch_message(fun, clause, *column) : no_search_message(fun, clause));
    }
    auto& source = *it;

    // A relation's query value is compared once the family has checked the relation's form.
    if (clause != search_clause::restrictions) {
        check_query_value(source, std::move(query_value), fun, clause);
    }
    return source;
}

void external_search_plan::check_query_value(search_source& source, expr::expression query_value,
        const functions::external_search_function& fun, search_clause clause) {
    const auto values_equal = external_search::unevaluated_equality(query_value, source.query_value);
    if (values_equal == external_search::equality::always) {
        return;
    }
    if (values_equal == external_search::equality::never) {
        throw exceptions::invalid_request_exception(query_value_mismatch_message(source.family, fun.display_name(), clause));
    }
    // A selector's value is taken out of the selector tree, so nothing else registers its bind
    // markers. The ORDER BY clause was registered whole by select_statement::prepare(), and a WHERE
    // relation with the restrictions.
    if (clause == search_clause::selectors) {
        expr::fill_prepare_context(query_value, _ctx);
    }
    source.deferred.push_back({std::move(query_value), sstring(fun.display_name()), clause});
}

expr::expression external_search_plan::replacement_for(const functions::external_search_function& fun,
        const expr::expression& call_expr, search_source& source) {
    if (!source.rescores()) {
        return external_search::search_call_replacement(fun.value(), call_expr, source.temporaries, _temporaries_allocator);
    }
    if (fun.value() != functions::search_value::score) {
        // The rows are reordered by the recomputed similarity, so the index's rank no longer
        // applies, and the rank in the new order is not known until all rows are scored, which
        // happens after the selectors. ANN() is rejected too, since its tuple contains the rank.
        throw exceptions::invalid_request_exception(seastar::format(
                "{}() is not supported with a rescoring vector index: the rank is not available after rescoring",
                fun.display_name()));
    }
    // Reads the fetched column and the query vector, so it needs no temporary.
    return ann_search::similarity_expression(source.index, source.column, source.query_value, _db, _schema);
}

expr::expression external_search_plan::replace_search_calls(const expr::expression& e, search_clause clause) {
    return expr::search_and_replace(e, [&] (const expr::expression& candidate) -> std::optional<expr::expression> {
        const auto* fc = expr::as_if<expr::function_call>(&candidate);
        if (!fc) {
            return std::nullopt;
        }
        const auto* fun = functions::as_external_search_function(*fc);
        if (!fun) {
            return std::nullopt;
        }
        auto& source = search_of(*fc, *fun, clause);
        if (clause == search_clause::ordering && source.rescores()) {
            // Without a rank in the rescored order, and with no decided meaning for the similarity
            // of a row the vector search did not return, such an index orders only by itself.
            throw exceptions::invalid_request_exception(
                    "On a rescoring vector index, ORDER BY supports only ANN() or ANN_SCORE() called directly");
        }
        return replacement_for(*fun, candidate, source);
    });
}

void external_search_plan::resolve_ordering(const expr::expression& prepared_ordering) {
    throwing_assert(_sources.empty());

    // Rejected here, or the hidden selector holding the score would make the whole statement an
    // aggregation and silently return one row.
    expr::verify_no_aggregate_functions(prepared_ordering, "ORDER BY clause");

    // ANN(), BM25(), ANN_SCORE() and BM25_SCORE() called directly mean the index's own order. The
    // rows are read in that order, so only a rescoring index needs a score to sort them by.
    if (const auto* fc = expr::as_if<expr::function_call>(&prepared_ordering)) {
        const auto* fun = functions::as_external_search_function(*fc);
        if (fun && (fun->value() == functions::search_value::score_and_rank || fun->value() == functions::search_value::score)) {
            auto& source = search_of(*fc, *fun, search_clause::ordering);
            if (source.rescores()) {
                _ordering_expr = ann_search::similarity_expression(source.index, source.column, source.query_value, _db, _schema);
            }
            return;
        }
    }

    // Any other call becomes the score the rows are sorted by, the searches being whatever its
    // arguments name. Preparation can also have folded the call to a constant, which names none.
    auto ordering = replace_search_calls(prepared_ordering, search_clause::ordering);
    if (_sources.empty()) {
        // The regular-ordering path skips a scoring ordering, so it would otherwise be silently ignored.
        throw exceptions::invalid_request_exception(
                "An ORDER BY expression must name at least one search, through ANN() or BM25()");
    }
    if (expr::type_of(ordering) != float_type) {
        throw exceptions::invalid_request_exception(seastar::format(
                "{}() cannot be used as a scoring function in ORDER BY, which sorts the rows by a float, highest first",
                ordering_function_name(expr::as<expr::function_call>(prepared_ordering))));
    }
    _ordering_expr = std::move(ordering);
}

void external_search_plan::check_restrictions(const restrictions::select_restrictions& restrictions) {
    // select_restrictions holds out the relations whose left-hand side is a call to an external
    // search function; nothing else would apply them, so each has to name a search.
    const auto& scoring = restrictions.get_scoring_function_restrictions();
    for (const auto& binop : scoring) {
        const auto& fc = expr::as<expr::function_call>(binop.lhs);
        const auto* fun = functions::as_external_search_function(fc);
        throwing_assert(fun);
        // Threshold filtering, WHERE ANN(column, query_vector) > score, is not implemented, whatever
        // the ORDER BY clause. The message names no function: the user's ANN() arrives here as
        // ANN_SCORE() (see prepare_external_search_relation_lhs()).
        if (fun->family() == functions::search_family::ann) {
            throw exceptions::invalid_request_exception("Filtering by ANN similarity in the WHERE clause is not supported");
        }
        search_of(fc, *fun, search_clause::restrictions);
    }
    if (_sources.empty()) {
        return;
    }

    auto& source = _sources.front();
    switch (source.family) {
    case functions::search_family::ann:
        // Its relations were rejected above.
        return;
    case functions::search_family::bm25:
        if (scoring.empty()) {
            throw exceptions::invalid_request_exception("Full-text search queries require a WHERE BM25() > 0 clause");
        }
        if (scoring.size() > 1) {
            throw exceptions::invalid_request_exception("Full-text search queries support only one WHERE BM25() restriction");
        }
        const auto& relation = scoring.front();
        bm25_search::validate_restriction(relation);
        const auto& fc = expr::as<expr::function_call>(relation.lhs);
        const auto& fun = *functions::as_external_search_function(fc);
        check_query_value(source, external_search::extract_call_arguments(fc, fun.display_name()).second, fun, search_clause::restrictions);
        if (has_other_restrictions(restrictions)) {
            throw exceptions::invalid_request_exception("Full-text search queries do not support additional WHERE restrictions");
        }
        return;
    }
    std::unreachable();
}

void external_search_plan::replace_selectors(std::vector<selection::prepared_selector>& prepared_selectors) {
    for (auto& ps : prepared_selectors) {
        // An unaliased selector is named after the expression the user wrote, which its replacement
        // may not format as.
        const auto written = ps.expr;
        ps.expr = replace_search_calls(ps.expr, search_clause::selectors);
        external_search::name_selector_as_written(ps, written);
    }
}

select_statement::ordering_comparator_type descending_score_comparator(size_t score_column_index) {
    return [score_column_index, type = float_type] (const raw::select_statement::result_row_type& r1, const raw::select_statement::result_row_type& r2) {
        auto& c1 = r1[score_column_index];
        auto& c2 = r2[score_column_index];
        auto f1 = c1 ? value_cast<float>(type->deserialize(*c1)) : std::numeric_limits<float>::quiet_NaN();
        auto f2 = c2 ? value_cast<float>(type->deserialize(*c2)) : std::numeric_limits<float>::quiet_NaN();
        if (std::isfinite(f1) && std::isfinite(f2)) {
            return f1 > f2;
        }
        return std::isfinite(f1);
    };
}

} // namespace cql3::statements
