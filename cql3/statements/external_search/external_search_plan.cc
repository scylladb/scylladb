/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "cql3/statements/external_search/external_search_plan.hh"
#include "cql3/statements/external_search/ann_search.hh"

#include "cql3/statements/external_search/external_function.hh"
#include "cql3/statements/external_search/fulltext_indexed_table_select_statement.hh"
#include "cql3/statements/external_search/vector_indexed_table_select_statement.hh"
#include "cql3/expr/expr-utils.hh"
#include "cql3/functions/scoring_fcts.hh"
#include "cql3/restrictions/statement_restrictions.hh"
#include "exceptions/exceptions.hh"
#include "index/secondary_index_manager.hh"
#include "index/vector_index.hh"
#include "types/types.hh"
#include "utils/log.hh"

#include <seastar/core/on_internal_error.hh>

#include <algorithm>
#include <cmath>
#include <ranges>
#include <utility>

namespace cql3::statements {

static logging::logger plan_log("external_search_plan");

namespace {

using functions::external_search_function;
using functions::search_family;
using functions::search_value;

/// What the query value of a search of this family is called in error messages.
std::string_view query_value_name(search_family family) {
    return family == search_family::ann ? "query vector" : "search term";
}

/// The error for a call in SELECT or WHERE that refers to a search the ORDER BY did not introduce.
sstring clause_mismatch_message(const external_search_function& fun, search_clause clause, bool kind_is_run) {
    if (clause == search_clause::restrictions) {
        return seastar::format("{}() in WHERE names a search that the ORDER BY clause does not run", fun.display_name());
    }
    if (kind_is_run) {
        return fun.family() == search_family::ann
                ? seastar::format("{}() in SELECT must reference the same column as the ANN ordering", fun.display_name())
                : seastar::format("{}() in SELECT must reference the same column as BM25() in WHERE and ORDER BY", fun.display_name());
    }
    return fun.family() == search_family::ann
            ? seastar::format("{}() is not supported in the SELECT clause without a matching ANN ordering", fun.display_name())
            : seastar::format("{}() is not supported in the SELECT clause without matching ORDER BY and WHERE clauses", fun.display_name());
}

/// The error for a call whose query value differs from the ORDER BY call's.
sstring agreement_message(const external_search_function& fun, std::string_view value_name) {
    return fun.family() == search_family::ann
            ? seastar::format("{}() in SELECT must use the same {} as the ANN ordering", fun.display_name(), value_name)
            : seastar::format("{}() in SELECT must use the same {} as BM25() in WHERE and ORDER BY", fun.display_name(), value_name);
}

/// The index that will answer a search of this family on this column.
secondary_index::index resolve_index(search_family family, data_dictionary::database db, const schema_ptr& schema,
        const column_definition& column) {
    auto cf = db.find_column_family(schema);
    auto indexes = cf.get_index_manager().list_indexes();

    if (family == search_family::ann) {
        auto it = std::ranges::find_if(indexes, [&column] (const auto& index) {
            return secondary_index::vector_index::is_vector_index_on_column(index.metadata(), column.name_as_text());
        });
        if (it == indexes.end()) {
            throw exceptions::invalid_request_exception("ANN ordering by vector requires the column to be indexed using 'vector_index'");
        }
        return *it;
    }

    auto it = std::ranges::find_if(indexes, [&column] (const auto& index) { return index.supports_bm25_expression(column); });
    if (it == indexes.end()) {
        throw exceptions::invalid_request_exception("No fulltext index found for full-text search query");
    }
    return *it;
}

/// The two statement classes take one family's ordering info each; a source holds the same data for
/// either family, so this is a field-by-field copy.
ann_ordering_info to_ann_ordering_info(const search_source& source) {
    return ann_ordering_info{
            .index = source.index,
            .prepared_ann_ordering = std::make_pair(source.column, source.query_value),
            .is_rescoring_enabled = source.is_rescoring_enabled,
            .temporaries = {.score = source.score_slot, .rank = source.rank_slot},
            .deferred_select_vectors = source.deferred
                    | std::views::transform([] (const deferred_query_value& deferred) { return deferred.value; })
                    | std::ranges::to<std::vector>(),
    };
}

bm25_ordering_info to_bm25_ordering_info(const search_source& source) {
    return bm25_ordering_info{
            .index = source.index,
            .search_term = source.query_value,
            .temporaries = {.score = source.score_slot, .rank = source.rank_slot, .fragment = source.fragment_slot},
            .deferred_select_terms = source.deferred
                    | std::views::transform([] (const deferred_query_value& deferred) {
                          return deferred_select_term{deferred.value, deferred.function_name};
                      })
                    | std::ranges::to<std::vector>(),
    };
}

} // anonymous namespace

external_search_plan::external_search_plan(data_dictionary::database db, schema_ptr schema, prepare_context& ctx,
        expr::temporary_allocator& temporaries_allocator)
    : _db(db)
    , _schema(std::move(schema))
    , _ctx(ctx)
    , _temporaries_allocator(temporaries_allocator) {
}

search_source& external_search_plan::claim(const expr::function_call& fc, const external_search_function& fun, search_clause clause) {
    auto [column, query_value] = external_search::extract_call_arguments(fc, fun.display_name());

    auto it = std::ranges::find_if(_sources, [&] (const search_source& source) {
        return source.family == fun.family() && source.column == column;
    });

    if (it == _sources.end()) {
        if (clause != search_clause::ordering) {
            // Only ORDER BY introduces a search. The message differs by whether the statement runs
            // no search of this family (a missing clause) or one on another column (a mismatch).
            const bool kind_is_run = std::ranges::any_of(
                    _sources, [&] (const search_source& source) { return source.family == fun.family(); });
            throw exceptions::invalid_request_exception(clause_mismatch_message(fun, clause, kind_is_run));
        }
        auto index = resolve_index(fun.family(), _db, _schema, *column);
        _sources.push_back(search_source{
                .family = fun.family(),
                .index = index,
                .column = column,
                .query_value = query_value,
                .is_rescoring_enabled = fun.family() == search_family::ann
                        && secondary_index::vector_index::is_rescoring_enabled(index.metadata().options()),
        });
        return _sources.back();
    }

    if (clause == search_clause::restrictions) {
        // A relation's query value is compared by the search's own restriction check; here it is
        // enough that a search exists for it.
        return *it;
    }

    // Every call sharing a source must use the same query value. With a bind marker on either
    // side that can only be checked at execution.
    const auto values_equal = external_search::unevaluated_equality(query_value, it->query_value);
    if (values_equal != external_search::equality::always) {
        if (values_equal == external_search::equality::never) {
            throw exceptions::invalid_request_exception(agreement_message(fun, query_value_name(fun.family())));
        }
        // The call is replaced by a temporary, so nothing else registers the bind markers in its
        // arguments.
        expr::fill_prepare_context(query_value, _ctx);
        it->deferred.push_back({std::move(query_value), sstring(fun.display_name())});
    }
    return *it;
}

expr::expression external_search_plan::deliver(const external_search_function& fun, const expr::expression& call_expr, search_source& source,
        bool& unnamed) {
    // A temporary replacing a whole call remembers it, for the selector's name. The two inside the
    // (score, rank) tuple remember nothing; that selector is named by bind_selectors().
    auto slot = [&] (std::optional<size_t>& index, data_type type, std::optional<expr::expression> replaced) {
        if (!index) {
            index = _temporaries_allocator.allocate();
        }
        return expr::expression(expr::temporary{.index = *index, .type = std::move(type), .replaced_expr = std::move(replaced)});
    };

    auto score = [&] (std::optional<expr::expression> replaced) -> expr::expression {
        if (!source.is_rescoring_enabled) {
            return slot(source.score_slot, float_type, std::move(replaced));
        }
        // A rescoring index scores a quantized vector, and the coordinator recomputes the
        // similarity from the stored one. The expression reads the fetched column and the query
        // vector, so it needs no temporary. It formats as the similarity function, not as the call.
        unnamed = true;
        return ann_search::similarity_expression(source.index, source.column, source.query_value, _db, _schema);
    };

    auto rank = [&] (std::optional<expr::expression> replaced) -> expr::expression {
        if (!source.is_rescoring_enabled) {
            return slot(source.rank_slot, int32_type, std::move(replaced));
        }
        // A rescoring index reorders the rows by the recomputed similarity, so the Vector Store's
        // rank no longer applies, and the rank in the new order is not known until all rows are
        // scored, which happens after the selectors. ANN() is rejected too, since its tuple contains
        // the rank.
        throw exceptions::invalid_request_exception(seastar::format(
                "{}() is not supported with a rescoring vector index: the rank is not available after rescoring",
                fun.display_name()));
    };

    switch (fun.value()) {
    case search_value::score:
        return score(call_expr);
    case search_value::rank:
        return rank(call_expr);
    case search_value::fragment:
        return slot(source.fragment_slot, utf8_type, call_expr);
    case search_value::score_and_rank:
        // A tuple of two temporaries does not format as the call.
        unnamed = true;
        return expr::expression(expr::tuple_constructor{
                .elements = {score(std::nullopt), rank(std::nullopt)},
                .type = functions::score_and_rank_type(),
        });
    }
    std::unreachable();
}

expr::expression external_search_plan::lower(const expr::expression& e, search_clause clause, bool& lowered_any, bool& unnamed) {
    return expr::search_and_replace(e, [&] (const expr::expression& candidate) -> std::optional<expr::expression> {
        const auto* fc = expr::as_if<expr::function_call>(&candidate);
        if (!fc) {
            return std::nullopt;
        }
        const auto* fun = functions::as_external_search_function(*fc);
        if (!fun) {
            return std::nullopt;
        }
        lowered_any = true;
        auto& source = claim(*fc, *fun, clause);
        return deliver(*fun, candidate, source, unnamed);
    });
}

void external_search_plan::bind_ordering(const expr::expression& prepared_ordering) {
    // A bare call: the rows are returned in the order the index ranked them, so nothing is
    // computed or sorted here, and no temporary is allocated unless SELECT asks for a value.
    if (const auto* fc = expr::as_if<expr::function_call>(&prepared_ordering)) {
        if (const auto* fun = functions::as_external_search_function(*fc)) {
            if (fun->value() == search_value::fragment) {
                throw exceptions::invalid_request_exception(seastar::format(
                        "{}() cannot rank rows: it is an excerpt of one, not a measure of it", fun->display_name()));
            }
            auto& source = claim(*fc, *fun, search_clause::ordering);
            if (source.is_rescoring_enabled) {
                // A rescoring index: the coordinator recomputes the similarity and sorts the rows by it.
                _ordering_expr = ann_search::similarity_expression(source.index, source.column, source.query_value, _db, _schema);
            }
            return;
        }
    }

    // Any other expression is lowered like one in the SELECT clause and becomes the score the rows
    // are sorted by.
    bool lowered_any = false;
    bool unnamed = false;
    auto ordering = lower(prepared_ordering, search_clause::ordering, lowered_any, unnamed);
    if (!lowered_any) {
        // The regular-ordering path skips a scoring ordering, so reject it here rather than let the
        // clause be silently ignored.
        throw exceptions::invalid_request_exception(
                "An ORDER BY expression must name at least one search, through ANN() or BM25()");
    }
    if (expr::type_of(ordering) != float_type) {
        throw exceptions::invalid_request_exception(seastar::format(
                "An ORDER BY expression over searches must be a score, but {} is {}",
                prepared_ordering, expr::type_of(ordering)->as_cql3_type()));
    }
    _ordering_expr = std::move(ordering);
}

void external_search_plan::bind_selectors(std::vector<selection::prepared_selector>& prepared_selectors) {
    for (auto& ps : prepared_selectors) {
        // An unaliased selector is named after the expression the user wrote. A temporary formats
        // as the call it replaced, but a tuple or a similarity function does not, so such a
        // selector is given the name explicitly, from a copy taken before lowering.
        const auto written = ps.expr;
        bool lowered_any = false;
        bool unnamed = false;

        ps.expr = lower(ps.expr, search_clause::selectors, lowered_any, unnamed);

        if (unnamed && !ps.alias) {
            ps.alias = ::make_shared<column_identifier>(fmt::format("{:result_set_metadata}", written), true);
        }
    }
}

void external_search_plan::bind_restrictions(const restrictions::statement_restrictions& restrictions) {
    for (const auto& binop : restrictions.get_scoring_function_restrictions()) {
        // statement_restrictions only diverts a relation whose left-hand side is a call to an
        // external function, so the cast cannot fail.
        const auto& fc = expr::as<expr::function_call>(binop.lhs);
        const auto* fun = functions::as_external_search_function(fc);
        if (!fun) {
            on_internal_error(plan_log, seastar::format("no search claimed the external function call {}", fc));
        }
        if (_sources.empty()) {
            throw exceptions::invalid_request_exception(
                    "A scoring function in the WHERE clause requires a matching ORDER BY clause");
        }
        claim(fc, *fun, search_clause::restrictions);
    }
}

::shared_ptr<select_statement> external_search_plan::make_statement(external_statement_args args) const {
    if (_sources.size() > 1) {
        throw exceptions::invalid_request_exception("Combining several searches in one query is not supported yet");
    }
    const auto& source = _sources.front();

    if (source.family == search_family::ann) {
        return vector_indexed_table_select_statement::prepare(_db, args.schema, args.bound_terms, args.parameters,
                std::move(args.selection), std::move(args.restrictions), std::move(args.group_by_cell_indices), args.is_reversed,
                std::move(args.ordering_comparator), std::move(args.limit), std::move(args.per_partition_limit), args.stats,
                to_ann_ordering_info(source), std::move(args.attrs));
    }
    return fulltext_indexed_table_select_statement::prepare(_db, args.schema, args.bound_terms, args.parameters,
            std::move(args.selection), std::move(args.restrictions), std::move(args.group_by_cell_indices), args.is_reversed,
            std::move(args.ordering_comparator), std::move(args.limit), std::move(args.per_partition_limit), args.stats,
            to_bm25_ordering_info(source), std::move(args.attrs));
}

select_statement::ordering_comparator_type descending_score_ordering_comparator(
        const expr::expression& score_expr, uint32_t column_index) {
    // Every score is a float; nothing in the types says so, so check it here rather than let the
    // comparator read another type's bytes as one.
    if (expr::type_of(score_expr) != float_type) {
        on_internal_error(plan_log,
                seastar::format("rows cannot be ranked by {}, which is {} rather than a score", score_expr, expr::type_of(score_expr)->name()));
    }
    return [column_index] (const raw::select_statement::result_row_type& r1, const raw::select_statement::result_row_type& r2) {
        auto& c1 = r1[column_index];
        auto& c2 = r2[column_index];
        auto f1 = c1 ? value_cast<float>(float_type->deserialize(*c1)) : std::numeric_limits<float>::quiet_NaN();
        auto f2 = c2 ? value_cast<float>(float_type->deserialize(*c2)) : std::numeric_limits<float>::quiet_NaN();
        if (std::isfinite(f1) && std::isfinite(f2)) {
            return f1 > f2;
        }
        // A row with no usable score sorts last, whichever way the other compares.
        return std::isfinite(f1);
    };
}

} // namespace cql3::statements
