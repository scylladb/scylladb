/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "cql3/statements/external_search/external_search_plan.hh"

#include "cql3/expr/expr-utils.hh"
#include "cql3/functions/scoring_fcts.hh"
#include "cql3/selection/selection.hh"
#include "cql3/statements/external_search/external_function.hh"
#include "data_dictionary/data_dictionary.hh"
#include "exceptions/exceptions.hh"
#include "index/secondary_index_manager.hh"
#include "types/types.hh"

namespace cql3::statements {

std::optional<bm25_ordering_info> get_bm25_ordering_info(
        data_dictionary::database db,
        schema_ptr schema,
        const expr::function_call& fc) {

    if (!expr::is_native_function_call(fc, functions::BM25_FUNCTION_NAME)) {
        return std::nullopt;
    }
    auto [column, search_term] = external_search::extract_call_arguments(fc, "BM25");

    auto cf = db.find_column_family(schema);
    auto& sim = cf.get_index_manager();

    for (const auto& idx : sim.list_indexes()) {
        if (idx.supports_bm25_expression(*column)) {
            return bm25_ordering_info{idx, std::move(search_term)};
        }
    }

    throw exceptions::invalid_request_exception("No fulltext index found for full-text search query");
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

} // namespace cql3::statements
