/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "cql3/statements/external_search/pattern_indexed_table_select_statement.hh"
#include "cql3/statements/raw/select_statement.hh"
#include "cql3/expr/evaluate.hh"
#include "cql3/expr/expression.hh"
#include "cql3/expr/expr-utils.hh"
#include "cql3/query_processor.hh"
#include "cql3/restrictions/statement_restrictions.hh"
#include "data_dictionary/data_dictionary.hh"
#include "db/consistency_level_validations.hh"
#include "exceptions/exceptions.hh"
#include "utils/assert.hh"

#include <seastar/core/future.hh>
#include <seastar/coroutine/exception.hh>

namespace cql3::statements {

namespace {

expr::binary_operator find_like_restriction(const restrictions::select_restrictions& select_restrictions, const secondary_index::index& index) {
    if (!select_restrictions.partition_key_restrictions_is_empty() || !restrictions::is_empty_restriction(select_restrictions.get_clustering_columns_restrictions())) {
        throw exceptions::invalid_request_exception("Pattern search queries do not support additional WHERE restrictions");
    }
    const auto& non_pk = select_restrictions.get_non_pk_restriction();
    if (non_pk.size() != 1) {
        throw exceptions::invalid_request_exception(
                seastar::format("Pattern search queries support exactly one LIKE restriction, on the indexed column {}, and no other WHERE restrictions", index.target_column()));
    }
    const auto& [column, restriction] = *non_pk.begin();
    if (column->name_as_text() != index.target_column()) {
        throw exceptions::invalid_request_exception(
                seastar::format("Pattern search queries must restrict the indexed column {}", index.target_column()));
    }
    auto factors = expr::boolean_factors(restriction);
    const auto* binop = factors.size() == 1 ? expr::as_if<expr::binary_operator>(&factors.front()) : nullptr;
    if (!binop || binop->op != expr::oper_t::LIKE) {
        throw exceptions::invalid_request_exception(
                seastar::format("Pattern search queries support exactly one LIKE restriction, on the indexed column {}, and no other WHERE restrictions", index.target_column()));
    }
    return *binop;
}

} // anonymous namespace

::shared_ptr<cql3::statements::select_statement> pattern_indexed_table_select_statement::prepare(data_dictionary::database db,
        schema_ptr schema, uint32_t bound_terms, lw_shared_ptr<const parameters> parameters,
        ::shared_ptr<selection::selection> selection, ::shared_ptr<const restrictions::select_restrictions> restrictions,
        ::shared_ptr<std::vector<size_t>> group_by_cell_indices, bool is_reversed,
        ordering_comparator_type ordering_comparator, std::optional<expr::expression> limit,
        std::optional<expr::expression> per_partition_limit, cql_stats& stats,
        const secondary_index::index& index,
        std::unique_ptr<attributes> attrs) {

    if (!limit.has_value()) {
        throw exceptions::invalid_request_exception("Pattern search queries require a LIMIT");
    }

    if (per_partition_limit.has_value()) {
        throw exceptions::invalid_request_exception("Pattern search queries do not support per-partition limits");
    }

    if (!parameters->orderings().empty()) {
        throw exceptions::invalid_request_exception("Pattern search queries do not support ORDER BY");
    }

    if (selection->is_aggregate() || !group_by_cell_indices->empty()) {
        throw exceptions::invalid_request_exception("Pattern search queries cannot be run with aggregation");
    }

    if (!restrictions->get_scoring_function_restrictions().empty()) {
        throw exceptions::invalid_request_exception("Pattern search queries cannot be combined with scoring functions");
    }

    const auto like = find_like_restriction(*restrictions, index);

    return ::make_shared<cql3::statements::pattern_indexed_table_select_statement>(
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
            index,
            like.rhs,
            std::move(attrs));
}

pattern_indexed_table_select_statement::pattern_indexed_table_select_statement(schema_ptr schema, uint32_t bound_terms,
        lw_shared_ptr<const parameters> parameters, ::shared_ptr<selection::selection> selection,
        ::shared_ptr<const restrictions::select_restrictions> restrictions,
        ::shared_ptr<std::vector<size_t>> group_by_cell_indices, bool is_reversed,
        ordering_comparator_type ordering_comparator, std::optional<expr::expression> limit,
        std::optional<expr::expression> per_partition_limit, cql_stats& stats,
        const secondary_index::index& index, expr::expression pattern, std::unique_ptr<attributes> attrs)
    : external_index_select_statement{schema, bound_terms, parameters, selection, restrictions,
              group_by_cell_indices, is_reversed, ordering_comparator, limit, per_partition_limit,
              stats, index, std::move(attrs)}
    , _pattern{std::move(pattern)} {
}

sstring pattern_indexed_table_select_statement::evaluate_pattern(const query_options& options) const {
    auto pattern = expr::evaluate(_pattern, options);
    if (pattern.is_null()) {
        throw exceptions::invalid_request_exception("The LIKE pattern of a pattern search query must not be null");
    }
    return value_cast<sstring>(utf8_type->deserialize(std::move(pattern).to_bytes()));
}

future<shared_ptr<cql_transport::messages::result_message>> pattern_indexed_table_select_statement::execute_search(
        query_processor& qp, service::query_state& state, const query_options& options, uint64_t limit) const {

    if (limit > max_pattern_query_limit) {
        co_await coroutine::return_exception(exceptions::invalid_request_exception(
                fmt::format("Pattern search queries require a LIMIT that is not greater than {}. LIMIT was {}", max_pattern_query_limit, limit)));
    }

    auto timeout = db::timeout_clock::now() + get_timeout(state.get_client_state(), options);
    auto aoe = abort_on_expiry(timeout);

    const auto pattern = evaluate_pattern(options);

    auto pkeys = co_await qp.vector_store_client().like(
            _schema->ks_name(), _index.metadata().name(), _schema, pattern, limit, aoe.abort_source());
    if (!pkeys.has_value()) {
        co_await coroutine::return_exception(
                exceptions::invalid_request_exception(std::visit(vector_search::vector_store_client::like_error_visitor{}, pkeys.error())));
    }

    if (pkeys->size() > limit) {
        pkeys->erase(pkeys->begin() + limit, pkeys->end());
    }

    auto table_results = co_await query_base_table(qp, state, options, timeout, pkeys.value());
    co_return co_await emit_result_set(std::move(table_results), options, nullptr);
}

} // namespace cql3::statements
