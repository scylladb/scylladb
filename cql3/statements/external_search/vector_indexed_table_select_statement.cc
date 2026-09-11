#include "ann_search.hh"
/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "cql3/statements/external_search/vector_indexed_table_select_statement.hh"
#include "cql3/statements/external_search/external_function.hh"
#include "cql3/statements/external_search/external_search_provider.hh"

#include "cql3/expr/evaluate.hh"
#include "cql3/expr/expr-utils.hh"
#include "cql3/functions/functions.hh"
#include "cql3/functions/scoring_fcts.hh"
#include "cql3/statements/raw/select_statement.hh"
#include "cql3/query_processor.hh"
#include "cql3/util.hh"

#include "db/consistency_level_validations.hh"
#include "exceptions/exceptions.hh"
#include "index/vector_index.hh"
#include "types/vector.hh"
#include "utils/assert.hh"

#include <seastar/core/future.hh>
#include <seastar/coroutine/exception.hh>


namespace cql3 {

namespace statements {

::shared_ptr<cql3::statements::select_statement> vector_indexed_table_select_statement::prepare(data_dictionary::database db, schema_ptr schema,
        uint32_t bound_terms, lw_shared_ptr<const parameters> parameters, ::shared_ptr<selection::selection> selection,
        ::shared_ptr<const restrictions::statement_restrictions> restrictions, ::shared_ptr<std::vector<size_t>> group_by_cell_indices, bool is_reversed,
        ordering_comparator_type ordering_comparator, std::optional<expr::expression> limit,
        std::optional<expr::expression> per_partition_limit, cql_stats& stats, ann_ordering_info ordering_info, std::unique_ptr<attributes> attrs) {

    // Filtering by similarity - WHERE ANN(column, query_vector) > score - is not implemented yet.
    // The message names no function: the user's ANN() arrives here as ANN_SCORE() (see
    // prepare_external_search_relation_lhs()).
    if (!restrictions->get_scoring_function_restrictions().empty()) {
        throw exceptions::invalid_request_exception("Filtering by ANN similarity in the WHERE clause is not supported");
    }

    // The score and the rank are matched to a row by primary key.
    if (ordering_info.temporaries.any()) {
        external_search::fetch_primary_key_columns(*selection, *schema);
    }

    auto prepared_filter = external_search::prepare_filter(*restrictions, parameters->allow_filtering());

    return ::make_shared<cql3::statements::vector_indexed_table_select_statement>(schema, bound_terms, parameters, std::move(selection), std::move(restrictions),
            std::move(group_by_cell_indices), is_reversed, std::move(ordering_comparator), std::move(limit),
            std::move(per_partition_limit), stats, std::move(ordering_info), std::move(prepared_filter), std::move(attrs));
}

vector_indexed_table_select_statement::vector_indexed_table_select_statement(schema_ptr schema, uint32_t bound_terms, lw_shared_ptr<const parameters> parameters,
        ::shared_ptr<selection::selection> selection, ::shared_ptr<const restrictions::statement_restrictions> restrictions,
        ::shared_ptr<std::vector<size_t>> group_by_cell_indices, bool is_reversed, ordering_comparator_type ordering_comparator,
        std::optional<expr::expression> limit, std::optional<expr::expression> per_partition_limit, cql_stats& stats,
        ann_ordering_info ordering_info, external_search::prepared_filter prepared_filter, std::unique_ptr<attributes> attrs)
    : external_index_select_statement{schema, bound_terms, parameters, selection, restrictions, group_by_cell_indices,
              is_reversed, ordering_comparator, limit, per_partition_limit, stats, ordering_info.index, std::move(attrs)}
    , _ann_ordering_info(std::move(ordering_info))
    , _prepared_filter(std::move(prepared_filter)) {

    if (!limit.has_value()) {
        throw exceptions::invalid_request_exception("Vector ANN queries must have a limit specified");
    }

    if (per_partition_limit.has_value()) {
        throw exceptions::invalid_request_exception("Vector ANN queries do not support per-partition limits");
    }

    if (selection->is_aggregate() || !group_by_cell_indices->empty()) {
        throw exceptions::invalid_request_exception("Vector ANN queries cannot be run with aggregation");
    }
}

future<shared_ptr<cql_transport::messages::result_message>> vector_indexed_table_select_statement::execute_search(
        query_processor& qp, service::query_state& state, const query_options& options, uint64_t limit) const {

    if (limit > max_ann_query_limit) {
        co_await coroutine::return_exception(exceptions::invalid_request_exception(
                fmt::format("Use of ANN OF in an ORDER BY clause requires a LIMIT that is not greater than {}. LIMIT was {}", max_ann_query_limit, limit)));
    }

    const auto& prepared_ann_ordering = _ann_ordering_info.prepared_ann_ordering;

    // Evaluated once: the vector searched with is the one the SELECT occurrences are checked against.
    const auto ordering_vector = expr::evaluate(prepared_ann_ordering.second, options);
    if (ordering_vector.is_null()) {
        // Before the agreement check, or a null would surface as a disagreement instead.
        co_await coroutine::return_exception(exceptions::invalid_request_exception(
                fmt::format("Unsupported null value for column {}", prepared_ann_ordering.first->name_as_text())));
    }

    for (const auto& selected_vector : _ann_ordering_info.deferred_select_vectors) {
        if (expr::evaluate(selected_vector, options) != ordering_vector) {
            co_await coroutine::return_exception(exceptions::invalid_request_exception(
                    "ANN() in SELECT must use the same query vector as the ANN ordering"));
        }
    }

    auto timeout = db::timeout_clock::now() + get_timeout(state.get_client_state(), options);
    auto aoe = abort_on_expiry(timeout);
    auto filter_json = _prepared_filter.to_json(options);
    const auto fetch = ann_search::candidates_wanted(_index, limit);
    auto pkeys = co_await ann_search::ask(qp.vector_store_client(), _schema->ks_name(), _index.metadata().name(), _schema,
            ann_search::query_vector(*prepared_ann_ordering.first, ordering_vector), fetch, filter_json, aoe.abort_source());

    if (pkeys.size() > limit && !_ann_ordering_info.is_rescoring_enabled) {
        pkeys.erase(pkeys.begin() + limit, pkeys.end());
    }

    auto table_results = co_await query_base_table(qp, state, options, timeout, pkeys);

    auto provider = std::optional<external_search::external_search_provider>{};
    if (table_results && (_ann_ordering_info.temporaries.score || _ann_ordering_info.temporaries.rank)) {
        // A rescoring index allocates neither temporary: there the similarity is computed from the
        // row's own vector instead.
        const auto* answers = &pkeys;
        const auto& read = table_results.value();
        auto rows = external_search::join_table_results(
                *read.rows, read.command->slice, *_schema, *_selection, std::span(&answers, 1), {});
        external_search::drop_unscored_rows(rows, std::span(&answers, 1));
        auto filled = std::vector<external_search::external_values>{};
        if (_ann_ordering_info.temporaries.score) {
            filled.push_back(external_search::external_values{
                    .temporary_index = *_ann_ordering_info.temporaries.score, .values = external_search::similarities_of(rows, 0, pkeys)});
        }
        if (_ann_ordering_info.temporaries.rank) {
            filled.push_back(external_search::external_values{
                    .temporary_index = *_ann_ordering_info.temporaries.rank, .values = external_search::ranks_of(rows, 0, pkeys)});
        }
        provider.emplace(std::move(filled), rows);
    }
    co_return co_await emit_result_set(std::move(table_results), options, provider ? &*provider : nullptr);
}

} // namespace statements

} // namespace cql3
