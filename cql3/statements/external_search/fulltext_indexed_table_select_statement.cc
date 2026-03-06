/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "cql3/statements/external_search/fulltext_indexed_table_select_statement.hh"
#include "cql3/statements/external_search/external_score_provider.hh"
#include "cql3/statements/raw/select_statement.hh"
#include "cql3/expr/evaluate.hh"
#include "cql3/expr/expression.hh"
#include "cql3/expr/expr-utils.hh"
#include "cql3/query_processor.hh"
#include "cql3/restrictions/statement_restrictions.hh"
#include "index/secondary_index_manager.hh"
#include "data_dictionary/data_dictionary.hh"
#include "db/consistency_level_validations.hh"
#include "exceptions/exceptions.hh"
#include "types/types.hh"
#include "utils/assert.hh"

#include <seastar/core/future.hh>
#include <seastar/coroutine/exception.hh>

namespace cql3::statements {

namespace {

const column_definition* extract_column_from_first_argument(const expr::function_call& fc) {
    const auto* col_val = expr::as_if<expr::column_value>(&fc.args[0]);
    if (!col_val) {
        throw exceptions::invalid_request_exception("First argument to BM25 must be a column reference");
    }
    return col_val->col;
}

expr::expression extract_search_term_from_second_argument(const expr::function_call& fc) {
    expr::expression search_term = fc.args[1];
    if (expr::find_in_expression<expr::column_value>(search_term, [](const expr::column_value&) {
            return true;
        })) {
        throw exceptions::invalid_request_exception("Second argument to BM25() must not be a column reference");
    }
    return search_term;
}

} // anonymous namespace

namespace {

void validate_bm25_where_restriction(const expr::binary_operator& binop, const bm25_ordering_info& ordering_info) {
    const auto& fc = expr::as<expr::function_call>(binop.lhs);
    const auto& col = extract_column_from_first_argument(fc);
    if (col->name_as_text() != ordering_info.index.target_column()) {
        throw exceptions::invalid_request_exception("Full-text search queries must reference the same column in both WHERE and ORDER BY clauses");
    }

    if (binop.op != expr::oper_t::GT) {
        throw exceptions::invalid_request_exception(
                seastar::format("Unsupported \"{}\" relation for BM25 function restriction, only \">\" is supported", binop.op));
    }
    const auto* rhs_const = expr::as_if<expr::constant>(&binop.rhs);
    if (!rhs_const || rhs_const->is_null() || rhs_const->view().deserialize<float>(*float_type) != 0.0f) {
        throw exceptions::invalid_request_exception("BM25 function comparison value must be the literal 0");
    }

    const auto where_search_term = extract_search_term_from_second_argument(fc);

    // If both query terms are literals, reject mismatches at prepare time.
    // Bind-marker cases are caught at execute time.
    const auto* where_const = expr::as_if<expr::constant>(&where_search_term);
    const auto* order_const = expr::as_if<expr::constant>(&ordering_info.search_term);
    if (where_const && order_const && *where_const != *order_const) {
        throw exceptions::invalid_request_exception(
                "Full-text search queries must use the same search term in both WHERE and ORDER BY clauses");
    }
}

} // anonymous namespace

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
    if (!fc || !expr::is_native_function_call(*fc, "bm25")) {
        throw exceptions::invalid_request_exception("Only BM25 scoring function is supported in ORDER BY");
    }

    throwing_assert(fc->args.size() == 2);

    const auto* column = extract_column_from_first_argument(*fc);
    auto search_term = extract_search_term_from_second_argument(*fc);

    auto cf = db.find_column_family(schema);
    auto& sim = cf.get_index_manager();

    for (const auto& idx : sim.list_indexes()) {
        if (idx.supports_bm25_expression(*column)) {
            return bm25_ordering_info{idx, std::move(search_term)};
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

    const auto& scoring_restrictions = restrictions->get_scoring_function_restrictions();
    if (scoring_restrictions.empty()) {
        throw exceptions::invalid_request_exception("Full-text search queries require a WHERE BM25() > 0 clause");
    }
    if (scoring_restrictions.size() > 1) {
        throw exceptions::invalid_request_exception("Full-text search queries support only one WHERE BM25() restriction");
    }

    validate_bm25_where_restriction(scoring_restrictions.front(), *ordering_info);

    // Reject any WHERE restrictions beyond the single BM25 clause.
    // BM25 restrictions are excluded from `restrictions`.
    if (!restrictions->partition_key_restrictions_is_empty()
            || !restrictions::is_empty_restriction(restrictions->get_clustering_columns_restrictions())
            || !restrictions::is_empty_restriction(restrictions->get_nonprimary_key_restrictions())) {
        throw exceptions::invalid_request_exception(
                "Full-text search queries do not support additional WHERE restrictions");
    }

    // The external score provider needs primary key columns to match each
    // replica row against the vector-store results. Ensure they are fetched
    // even when the user did not select them (e.g. SELECT BM25(...) ...).
    if (ordering_info->external_value_index) {
        for (const auto& cdef : schema->partition_key_columns()) {
            selection->add_column_for_post_processing(cdef);
        }
        for (const auto& cdef : schema->clustering_key_columns()) {
            selection->add_column_for_post_processing(cdef);
        }
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
            std::move(*ordering_info),
            std::move(attrs));
}

fulltext_indexed_table_select_statement::fulltext_indexed_table_select_statement(schema_ptr schema, uint32_t bound_terms,
        lw_shared_ptr<const parameters> parameters, ::shared_ptr<selection::selection> selection,
        ::shared_ptr<const restrictions::statement_restrictions> restrictions,
        ::shared_ptr<std::vector<size_t>> group_by_cell_indices, bool is_reversed,
        ordering_comparator_type ordering_comparator, std::optional<expr::expression> limit,
        std::optional<expr::expression> per_partition_limit, cql_stats& stats,
        bm25_ordering_info ordering_info, std::unique_ptr<attributes> attrs)
    : external_index_select_statement{schema, bound_terms, parameters, selection, restrictions,
              group_by_cell_indices, is_reversed, ordering_comparator, limit, per_partition_limit,
              stats, ordering_info.index, std::move(attrs)}
    , _bm25_ordering_info{std::move(ordering_info)} {
}

future<shared_ptr<cql_transport::messages::result_message>> fulltext_indexed_table_select_statement::execute_search(
        query_processor& qp, service::query_state& state, const query_options& options, uint64_t limit) const {

    if (limit > max_fts_query_limit) {
        co_await coroutine::return_exception(exceptions::invalid_request_exception(
                fmt::format("Full-text search queries require a LIMIT that is not greater than {}. LIMIT was {}", max_fts_query_limit, limit)));
    }

    auto timeout = db::timeout_clock::now() + get_timeout(state.get_client_state(), options);
    auto aoe = abort_on_expiry(timeout);

    auto search_term_val = expr::evaluate(_bm25_ordering_info.search_term, options);
    if (search_term_val.is_null()) {
        co_await coroutine::return_exception(exceptions::invalid_request_exception("Full-text search query term must not be null"));
    }

    const auto& where_restriction = _restrictions->get_scoring_function_restrictions().front();
    const auto& fc = expr::as<expr::function_call>(where_restriction.lhs);
    const auto where_val = expr::evaluate(fc.args[1], options);
    if (where_val != search_term_val) {
        throw exceptions::invalid_request_exception(
                "Full-text search queries must use the same search term in both WHERE and ORDER BY clauses");
    }

    auto search_term_bytes = std::move(search_term_val).to_bytes();
    sstring search_term_text = value_cast<sstring>(utf8_type->deserialize(search_term_bytes));

    auto pkeys = co_await qp.vector_store_client().bm25(_schema->ks_name(), _index.metadata().name(), _schema, search_term_text, limit, aoe.abort_source());
    if (!pkeys.has_value()) {
        co_await coroutine::return_exception(
                exceptions::invalid_request_exception(std::visit(vector_search::vector_store_client::fts_error_visitor{}, pkeys.error())));
    }

    throwing_assert(pkeys->size() <= limit);

    auto provider = _bm25_ordering_info.external_value_index
                            ? std::make_unique<external_score_provider>(pkeys.value(), *_bm25_ordering_info.external_value_index, *_schema)
                            : nullptr;
    co_return co_await query_base_table(qp, state, options, pkeys.value(), timeout, std::move(provider));
}

} // namespace cql3::statements
