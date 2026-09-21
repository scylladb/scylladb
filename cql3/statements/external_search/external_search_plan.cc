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

namespace cql3::statements {

namespace {

secondary_index::index resolve_index(functions::search_family family, data_dictionary::database db, const schema_ptr& schema,
        const column_definition& column) {
    auto cf = db.find_column_family(schema);
    auto indexes = cf.get_index_manager().list_indexes();

    if (family == functions::search_family::ann) {
        auto it = std::ranges::find_if(indexes, [&column] (const auto& index) {
            return secondary_index::vector_index::is_vector_index_on_column(index.metadata(), column.name_as_text());
        });
        if (it == indexes.end()) {
            throw exceptions::invalid_request_exception("ANN ordering by vector requires the column to be indexed using 'vector_index'");
        }
        return *it;
    }

    for (const auto& idx : indexes) {
        if (idx.supports_bm25_expression(column)) {
            return idx;
        }
    }
    throw exceptions::invalid_request_exception("No fulltext index found for full-text search query");
}

sstring no_search_message(const functions::external_search_function& fun) {
    return fun.family() == functions::search_family::ann
            ? seastar::format("{}() is not supported in the SELECT clause without a matching ANN ordering", fun.display_name())
            : seastar::format("{}() is not supported in the SELECT clause without matching ORDER BY and WHERE clauses", fun.display_name());
}

sstring column_mismatch_message(const functions::external_search_function& fun) {
    return fun.family() == functions::search_family::ann
            ? seastar::format("{}() in SELECT must reference the same column as the ANN ordering", fun.display_name())
            : seastar::format("{}() in SELECT must reference the same column as BM25() in WHERE and ORDER BY", fun.display_name());
}

bool has_other_restrictions(const restrictions::select_restrictions& restrictions) {
    return !restrictions.partition_key_restrictions_is_empty()
            || !restrictions::is_empty_restriction(restrictions.get_clustering_columns_restrictions())
            || !restrictions::is_empty_restriction(restrictions.get_nonprimary_key_restrictions());
}

} // anonymous namespace

sstring query_value_mismatch_message(functions::search_family family, std::string_view function_name) {
    return family == functions::search_family::ann
            ? seastar::format("{}() in SELECT must use the same query vector as the ANN ordering", function_name)
            : seastar::format("{}() in SELECT must use the same search term as BM25() in WHERE and ORDER BY", function_name);
}

bool search_source::rescores() const {
    return family == functions::search_family::ann && secondary_index::vector_index::is_rescoring_enabled(index.metadata().options());
}

const search_source* external_search_plan::find(functions::search_family family) const {
    auto it = std::ranges::find(_sources, family, &search_source::family);
    return it == _sources.end() ? nullptr : &*it;
}

search_source* external_search_plan::find(functions::search_family family) {
    return const_cast<search_source*>(std::as_const(*this).find(family));
}

search_source& external_search_plan::search_of(const expr::function_call& fc, const functions::external_search_function& fun,
        search_clause clause) {
    auto [column, query_value] = external_search::extract_call_arguments(fc, fun.display_name());

    auto it = std::ranges::find_if(_sources, [&] (const search_source& source) {
        return source.family == fun.family() && source.column == column;
    });
    if (it == _sources.end()) {
        if (clause == search_clause::restrictions) {
            throw exceptions::invalid_request_exception(seastar::format(
                    "{}() in WHERE names a search that the ORDER BY clause does not run", fun.display_name()));
        }
        if (clause == search_clause::ordering) {
            _sources.push_back(search_source{
                    .family = fun.family(),
                    .index = resolve_index(fun.family(), _db, _schema, *column),
                    .column = column,
                    .query_value = std::move(query_value),
            });
            return _sources.back();
        }
        const bool on_another_column = find(fun.family()) != nullptr;
        throw exceptions::invalid_request_exception(on_another_column ? column_mismatch_message(fun) : no_search_message(fun));
    }
    auto* source = &*it;

    if (clause == search_clause::restrictions) {
        // The relation's query value is compared by the family's own restriction check.
        return *source;
    }

    // Every call describes the one search of its family the rows are ranked by, so it has to use
    // the query value the ORDER BY clause does.
    const auto values_equal = external_search::unevaluated_equality(query_value, source->query_value);
    if (values_equal != external_search::equality::always) {
        if (values_equal == external_search::equality::never) {
            throw exceptions::invalid_request_exception(query_value_mismatch_message(fun.family(), fun.display_name()));
        }
        // Taken out of the selector tree, so nothing else registers a bind marker in this value.
        expr::fill_prepare_context(query_value, _ctx);
        source->deferred.push_back({std::move(query_value), sstring(fun.display_name())});
    }
    return *source;
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
        return replacement_for(*fun, candidate, search_of(*fc, *fun, clause));
    });
}

void external_search_plan::resolve_ordering(const expr::function_call& fc) {
    const auto* fun = functions::as_external_search_function(fc);
    // Only ANN() and BM25() rank rows, and both return the (score, rank) pair. A call naming one
    // value of a search does not rank by it; select_statement rejects such an ORDER BY.
    if (!fun || fun->value() != functions::search_value::score_and_rank) {
        return;
    }
    auto& source = search_of(fc, *fun, search_clause::ordering);
    if (source.rescores()) {
        // A rescoring index ordered the rows by the score it reported for a quantized vector, which
        // is not the requested order: the coordinator recomputes the similarity and sorts by it.
        _ordering_expr = ann_search::similarity_expression(source.index, source.column, source.query_value, _db, _schema);
    }
}

void external_search_plan::check_restrictions(const restrictions::select_restrictions& restrictions) {
    // select_restrictions holds out the relations whose left-hand side is a call to an external
    // search function; nothing else would apply them, so each has to name a search.
    const auto& scoring = restrictions.get_scoring_function_restrictions();
    for (const auto& binop : scoring) {
        const auto& fc = expr::as<expr::function_call>(binop.lhs);
        const auto* fun = functions::as_external_search_function(fc);
        throwing_assert(fun);
        search_of(fc, *fun, search_clause::restrictions);
    }
    if (_sources.empty()) {
        return;
    }

    auto& source = _sources.front();
    if (source.family == functions::search_family::ann) {
        // Threshold filtering, WHERE ANN(column, query_vector) > score, is not implemented. The
        // message names no function: the user's ANN() arrives here as ANN_SCORE() (see
        // prepare_external_search_relation_lhs()).
        if (!scoring.empty()) {
            throw exceptions::invalid_request_exception("Filtering by ANN similarity in the WHERE clause is not supported");
        }
        return;
    }

    if (scoring.empty()) {
        throw exceptions::invalid_request_exception("Full-text search queries require a WHERE BM25() > 0 clause");
    }
    if (scoring.size() > 1) {
        throw exceptions::invalid_request_exception("Full-text search queries support only one WHERE BM25() restriction");
    }
    source.deferred_where_term = bm25_search::validate_restriction(scoring.front(), source.query_value);
    if (has_other_restrictions(restrictions)) {
        throw exceptions::invalid_request_exception("Full-text search queries do not support additional WHERE restrictions");
    }
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
