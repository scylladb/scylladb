/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "cql3/statements/external_search/external_search_plan.hh"

#include "cql3/statements/external_search/ann_search.hh"
#include "index/vector_index.hh"

#include "cql3/expr/expr-utils.hh"
#include "cql3/functions/scoring_fcts.hh"
#include "cql3/selection/selection.hh"
#include "cql3/statements/external_search/external_function.hh"
#include "data_dictionary/data_dictionary.hh"
#include "exceptions/exceptions.hh"
#include "index/secondary_index_manager.hh"
#include "types/types.hh"

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

/// Orders by a score column of the result row, descending, rows without a usable score last.
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

} // anonymous namespace

std::optional<bm25_ordering_info> get_bm25_ordering_info(
        data_dictionary::database db,
        schema_ptr schema,
        const expr::function_call& fc) {

    if (!expr::is_native_function_call(fc, functions::BM25_FUNCTION_NAME)) {
        return std::nullopt;
    }
    auto [column, search_term] = external_search::extract_call_arguments(fc, "BM25");

    return bm25_ordering_info{resolve_index(functions::search_family::bm25, db, schema, *column), std::move(search_term)};
}

void prepare_bm25_selectors(std::vector<selection::prepared_selector>& prepared_selectors, std::optional<bm25_ordering_info>& ordering_info,
        expr::temporary_allocator& temporaries_allocator, prepare_context& ctx) {
    for (auto& ps : prepared_selectors) {
        const auto written = ps.expr;
        ps.expr = expr::search_and_replace(ps.expr, [&](const expr::expression& candidate) -> std::optional<expr::expression> {
            const auto* fc = expr::as_if<expr::function_call>(&candidate);
            if (!fc) {
                return std::nullopt;
            }
            const auto* fun = functions::as_external_search_function(*fc);
            if (!fun || fun->family() != functions::search_family::bm25) {
                return std::nullopt;
            }

            const auto function_name = fun->display_name();
            if (!ordering_info) {
                throw exceptions::invalid_request_exception(seastar::format(
                        "{}() is not supported in the SELECT clause without matching ORDER BY and WHERE clauses", function_name));
            }
            auto& info = *ordering_info;

            // Every call describes the one search the rows are ranked by, so it has to name the
            // column and the search term the other two clauses do.
            auto [col, sel_term] = external_search::extract_call_arguments(*fc, function_name);
            if (col->name_as_text() != info.index.target_column()) {
                throw exceptions::invalid_request_exception(
                        seastar::format("{}() in SELECT must reference the same column as BM25() in WHERE and ORDER BY", function_name));
            }

            const auto terms_equal = external_search::unevaluated_equality(sel_term, info.search_term);
            if (terms_equal != external_search::equality::always) {
                if (terms_equal == external_search::equality::never) {
                    throw exceptions::invalid_request_exception(
                            seastar::format("{}() in SELECT must use the same search term as BM25() in WHERE and ORDER BY", function_name));
                }
                // Lifted out of the selector tree, so nothing else registers a bind marker in this term.
                expr::fill_prepare_context(sel_term, ctx);
                info.deferred_select_terms.push_back({std::move(sel_term), sstring(function_name)});
            }

            return external_search::replace_search_call(fun->value(), candidate, info.temporaries, temporaries_allocator);
        });
        external_search::name_selector_as_written(ps, written);
    }
}

std::optional<ann_ordering_info> get_ann_ordering_info(
        data_dictionary::database db,
        schema_ptr schema,
        const expr::function_call& fc) {

    // Both 'ORDER BY ANN(column, query_vector)' and the legacy
    // 'ORDER BY column ANN OF query_vector' are parsed into an ann() call.
    if (!expr::is_native_function_call(fc, functions::ANN_FUNCTION_NAME)) {
        return std::nullopt;
    }

    // The call has been resolved, which for ann() infers the vector type and dimension of the query
    // vector from the ordered column - so a query vector of the wrong dimension and an argument
    // that is not a float vector have both been rejected already.
    auto [def, query_vector] = external_search::extract_call_arguments(fc, "ANN");

    raw::select_statement::prepared_ann_ordering_type prepared_ann_ordering = std::make_pair(def, std::move(query_vector));

    auto index = resolve_index(functions::search_family::ann, db, schema, *def);

    return ann_ordering_info{
        index,
        std::move(prepared_ann_ordering),
        secondary_index::vector_index::is_rescoring_enabled(index.metadata().options())
    };
}

void prepare_ann_selectors(std::vector<selection::prepared_selector>& prepared_selectors,
        std::optional<ann_ordering_info>& ordering_info, expr::temporary_allocator& temporaries_allocator,
        data_dictionary::database db, const schema_ptr& schema, prepare_context& ctx) {
    for (auto& ps : prepared_selectors) {
        const auto written = ps.expr;
        ps.expr = expr::search_and_replace(ps.expr, [&] (const expr::expression& candidate) -> std::optional<expr::expression> {
            const auto* fc = expr::as_if<expr::function_call>(&candidate);
            if (!fc) {
                return std::nullopt;
            }
            // Both 'ANN(column, query_vector)' and the legacy 'column ANN OF query_vector' parse
            // into an ann() call.
            const auto* fun = functions::as_external_search_function(*fc);
            if (!fun || fun->family() != functions::search_family::ann) {
                return std::nullopt;
            }

            const auto function_name = fun->display_name();
            if (!ordering_info) {
                throw exceptions::invalid_request_exception(seastar::format(
                        "{}() is not supported in the SELECT clause without a matching ANN ordering", function_name));
            }

            const auto& [ordering_column, ordering_vector] = ordering_info->prepared_ann_ordering;

            auto [col, sel_vector] = external_search::extract_call_arguments(*fc, function_name);
            if (col != ordering_column) {
                throw exceptions::invalid_request_exception(
                        seastar::format("{}() in SELECT must reference the same column as the ANN ordering", function_name));
            }

            const auto vectors_equal = external_search::unevaluated_equality(sel_vector, ordering_vector);
            if (vectors_equal != external_search::equality::always) {
                if (vectors_equal == external_search::equality::never) {
                    throw exceptions::invalid_request_exception(
                            seastar::format("{}() in SELECT must use the same query vector as the ANN ordering", function_name));
                }
                // Lifted out of the selector tree, so nothing else registers a bind marker in this vector.
                expr::fill_prepare_context(sel_vector, ctx);
                // Copied, not moved: the rescoring branch below builds the similarity from it.
                ordering_info->deferred_select_vectors.push_back({sel_vector, sstring(function_name)});
            }

            if (ordering_info->is_rescoring_enabled) {
                if (fun->value() != functions::search_value::score) {
                    // A rescoring index reorders the rows by the recomputed similarity, so the
                    // Vector Store's rank no longer applies, and the rank in the new order is not
                    // known until all rows are scored, which happens after the selectors. ANN() is
                    // rejected too, since its tuple contains the rank.
                    throw exceptions::invalid_request_exception(seastar::format(
                            "{}() is not supported with a rescoring vector index: the rank is not available after rescoring",
                            function_name));
                }

                // Every occurrence computes the similarity again, the hidden ordering selector included.
                return ann_search::similarity_expression(ordering_info->index, col, sel_vector, db, schema);
            }

            return external_search::replace_search_call(fun->value(), candidate, ordering_info->temporaries, temporaries_allocator);
        });
        external_search::name_selector_as_written(ps, written);
    }
}

select_statement::ordering_comparator_type rescored_similarity_ordering(
        std::vector<selection::prepared_selector>& prepared_selectors,
        const ann_ordering_info& ann_ordering_info,
        data_dictionary::database db,
        schema_ptr schema) {
    auto similarity = ann_search::similarity_expression(ann_ordering_info.index, ann_ordering_info.prepared_ann_ordering.first,
            ann_ordering_info.prepared_ann_ordering.second, db, schema);
    // The comparator reads the column as a float; every similarity function returns one, but
    // nothing in the types says so.
    throwing_assert(expr::type_of(similarity) == float_type);

    prepared_selectors.push_back(selection::prepared_selector{
        .expr = std::move(similarity),
        .alias = nullptr,
    });
    return descending_score_comparator(prepared_selectors.size() - 1);
}

void external_search_plan::resolve_ordering(const expr::function_call& fc) {
    _ann = get_ann_ordering_info(_db, _schema, fc);
    _bm25 = get_bm25_ordering_info(_db, _schema, fc);
}

void external_search_plan::replace_selectors(std::vector<selection::prepared_selector>& prepared_selectors) {
    prepare_bm25_selectors(prepared_selectors, _bm25, _temporaries_allocator, _ctx);
    prepare_ann_selectors(prepared_selectors, _ann, _temporaries_allocator, _db, _schema, _ctx);
}

} // namespace cql3::statements
