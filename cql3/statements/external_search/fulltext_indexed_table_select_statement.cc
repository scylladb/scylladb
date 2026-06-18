/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "cql3/statements/external_search/fulltext_indexed_table_select_statement.hh"
#include "cql3/statements/raw/select_statement.hh"
#include "cql3/expr/expression.hh"
#include "cql3/expr/expr-utils.hh"
#include "cql3/restrictions/statement_restrictions.hh"
#include "index/secondary_index_manager.hh"
#include "data_dictionary/data_dictionary.hh"
#include "exceptions/exceptions.hh"

namespace cql3::statements {

std::optional<bm25_ordering_info> get_bm25_ordering_info(
        data_dictionary::database db,
        schema_ptr schema,
        lw_shared_ptr<const raw::select_statement::parameters> parameters,
        prepare_context& ctx) {

    if (parameters->orderings().empty()) {
        return std::nullopt;
    }

    auto [column_id, ordering] = parameters->orderings().front();
    auto* scoring_ord = std::get_if<raw::select_statement::scoring_function_ordering>(&ordering);
    if (!scoring_ord) {
        return std::nullopt;
    }

    // Prepare the scoring function expression to resolve column references and function
    auto prepared_expr = expr::prepare_expression(scoring_ord->func_expr, db, schema->ks_name(), schema.get(), nullptr);
    expr::fill_prepare_context(prepared_expr, ctx);

    // Verify this is a BM25 function call
    auto* fc = expr::as_if<expr::function_call>(&prepared_expr);
    if (!fc || !expr::is_bm25_function_call(*fc)) {
        throw exceptions::invalid_request_exception("Only BM25 scoring function is supported in ORDER BY");
    }

    // Extract the column from the first argument
    if (fc->args.empty()) {
        throw exceptions::invalid_request_exception("BM25 requires at least a column argument");
    }
    const auto* col_val = expr::as_if<expr::column_value>(&fc->args[0]);
    if (!col_val) {
        throw exceptions::invalid_request_exception("First argument to BM25 must be a column");
    }
    const column_definition* def = col_val->col;

    auto cf = db.find_column_family(schema);
    auto& sim = cf.get_index_manager();

    for (const auto& idx : sim.list_indexes()) {
        if (idx.supports_bm25_expression(*def)) {
            return bm25_ordering_info{idx};
        }
    }

    throw exceptions::invalid_request_exception("No fulltext index found for full-text search query");
}

::shared_ptr<cql3::statements::select_statement> fulltext_indexed_table_select_statement::prepare(data_dictionary::database db,
        schema_ptr schema, uint32_t bound_terms, lw_shared_ptr<const parameters> parameters,
        ::shared_ptr<selection::selection> selection, ::shared_ptr<const restrictions::statement_restrictions> restrictions,
        ::shared_ptr<std::vector<size_t>> group_by_cell_indices, bool is_reversed,
        ordering_comparator_type ordering_comparator, std::optional<expr::expression> limit,
        std::optional<expr::expression> per_partition_limit, cql_stats& stats,
        std::optional<bm25_ordering_info> ordering_info,
        std::unique_ptr<attributes> attrs) {

    if (!limit.has_value()) {
        throw exceptions::invalid_request_exception("Full-text search queries require a LIMIT");
    }

    if (per_partition_limit.has_value()) {
        throw exceptions::invalid_request_exception("Full-text search queries do not support per-partition limits");
    }

    if (selection->is_aggregate()) {
        throw exceptions::invalid_request_exception("Full-text search queries cannot be run with aggregation");
    }

    if (!ordering_info) {
        throw exceptions::invalid_request_exception("Full-text search queries require an ORDER BY BM25() clause");
    }

    const auto bm25_where_pred = [&](const expr::binary_operator& o) {
        if (!expr::is_bm25_function_call(o.lhs)) {
            return false;
        }
        const auto* fc = expr::as_if<expr::function_call>(&o.lhs);
        if (!fc || fc->args.empty()) {
            return false;
        }
        const auto* col_val = expr::as_if<expr::column_value>(&fc->args[0]);
        return col_val && ordering_info->index.supports_bm25_expression(*col_val->col);
    };
    const bool has_bm25_restriction =
            expr::find_binop(restrictions->get_nonprimary_key_restrictions(), bm25_where_pred) ||
            expr::find_binop(restrictions->get_clustering_columns_restrictions(), bm25_where_pred);
    if (!has_bm25_restriction) {
        throw exceptions::invalid_request_exception(
                "Full-text search queries require a WHERE BM25() clause on the same column used in ORDER BY BM25()");
    }

    return ::make_shared<cql3::statements::fulltext_indexed_table_select_statement>(
            schema,
            bound_terms,
            parameters,
            std::move(selection),
            std::move(restrictions),
            std::move(group_by_cell_indices),
            is_reversed,
            std::move(ordering_comparator),
            std::move(limit),
            std::move(per_partition_limit),
            stats,
            ordering_info->index,
            std::move(attrs));
}

fulltext_indexed_table_select_statement::fulltext_indexed_table_select_statement(schema_ptr schema, uint32_t bound_terms,
        lw_shared_ptr<const parameters> parameters, ::shared_ptr<selection::selection> selection,
        ::shared_ptr<const restrictions::statement_restrictions> restrictions,
        ::shared_ptr<std::vector<size_t>> group_by_cell_indices, bool is_reversed,
        ordering_comparator_type ordering_comparator, std::optional<expr::expression> limit,
        std::optional<expr::expression> per_partition_limit, cql_stats& stats,
        const secondary_index::index& index,
        std::unique_ptr<attributes> attrs)
    : external_index_select_statement{schema, bound_terms, parameters, selection, restrictions, group_by_cell_indices,
              is_reversed, ordering_comparator, limit, per_partition_limit, stats, index, std::move(attrs)} {
}

future<shared_ptr<cql_transport::messages::result_message>> fulltext_indexed_table_select_statement::do_execute(
        query_processor& qp, service::query_state& state, const query_options& options) const {
    throw exceptions::invalid_request_exception("Full-text search not implemented");
}

} // namespace cql3::statements
