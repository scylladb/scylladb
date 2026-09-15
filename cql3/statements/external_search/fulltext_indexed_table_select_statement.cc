/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "cql3/statements/external_search/fulltext_indexed_table_select_statement.hh"
#include "cql3/statements/external_search/bm25_search.hh"
#include "cql3/statements/external_search/external_function.hh"
#include "cql3/statements/external_search/external_search_provider.hh"
#include "cql3/statements/raw/select_statement.hh"
#include "cql3/expr/evaluate.hh"
#include "cql3/expr/expression.hh"
#include "cql3/expr/expr-utils.hh"
#include "cql3/functions/scoring_fcts.hh"
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

/// The column the index is built on: the one the rows are ranked by and a fragment is generated from.
const column_definition& ranked_column(const schema& schema, const secondary_index::index& index) {
    const auto* cdef = schema.get_column_definition(to_bytes(index.target_column()));
    throwing_assert(cdef);
    return *cdef;
}

} // anonymous namespace

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

    if (selection->is_aggregate() || !group_by_cell_indices->empty()) {
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

    ordering_info->deferred_where_term = bm25_search::validate_restriction(
            scoring_restrictions.front(), ordering_info->index, ordering_info->search_term);

    // Reject any WHERE restrictions beyond the single BM25 clause.
    // BM25 restrictions are excluded from `restrictions`.
    if (!restrictions->partition_key_restrictions_is_empty()
            || !restrictions::is_empty_restriction(restrictions->get_clustering_columns_restrictions())
            || !restrictions::is_empty_restriction(restrictions->get_nonprimary_key_restrictions())) {
        throw exceptions::invalid_request_exception(
                "Full-text search queries do not support additional WHERE restrictions");
    }

    // The score and the rank are matched to a row by primary key.
    if (ordering_info->temporaries.score || ordering_info->temporaries.rank) {
        external_search::fetch_primary_key_columns(*selection, *schema);
    }

    // The index stores none of the text a fragment is generated from, so it has to be read from
    // every row even when the query does not select the column.
    if (ordering_info->temporaries.fragment) {
        selection->add_column_for_post_processing(ranked_column(*schema, ordering_info->index));
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

    if (_bm25_ordering_info.deferred_where_term
            && expr::evaluate(*_bm25_ordering_info.deferred_where_term, options) != search_term_val) {
        co_await coroutine::return_exception(exceptions::invalid_request_exception(
                "Full-text search queries must use the same search term in both WHERE and ORDER BY clauses"));
    }

    for (const auto& sel_term : _bm25_ordering_info.deferred_select_terms) {
        if (expr::evaluate(sel_term.term, options) != search_term_val) {
            co_await coroutine::return_exception(exceptions::invalid_request_exception(seastar::format(
                    "{}() in SELECT must use the same search term as BM25() in WHERE and ORDER BY", sel_term.function_name)));
        }
    }

    const auto search_term_text = bm25_search::query_term(search_term_val);

    auto pkeys = co_await bm25_search::ask(
            qp.vector_store_client(), _schema->ks_name(), _index.metadata().name(), _schema, search_term_text, limit, aoe.abort_source());

    throwing_assert(pkeys.size() <= limit);

    auto table_results = co_await query_base_table(qp, state, options, timeout, pkeys);

    const auto& temporaries = _bm25_ordering_info.temporaries;

    auto provider = std::optional<external_search::external_search_provider>{};
    if (table_results && temporaries.any()) {
        // A fragment does not exist until the index has been sent the rows' text.
        auto columns = std::vector<const column_definition*>{};
        auto text_column = std::optional<size_t>{};
        if (temporaries.fragment) {
            text_column = columns.size();
            columns.push_back(&ranked_column(*_schema, _bm25_ordering_info.index));
        }
        // The score and the rank are matched to a row by key; a fragment is matched by position,
        // and the key columns are read only when the score or the rank is selected.
        auto answers = std::vector<const vector_search::vector_store_client::primary_keys*>{};
        if (temporaries.score || temporaries.rank) {
            answers.push_back(&pkeys);
        }
        const auto& read = table_results.value();
        auto rows = external_search::join_table_results(*read.rows, read.command->slice, *_schema, *_selection, answers, columns);

        auto filled = std::vector<external_search::external_values>{};
        external_search::drop_unscored_rows(rows, answers);
        if (temporaries.score) {
            filled.push_back(external_search::external_values{
                    .temporary_index = *temporaries.score, .values = external_search::similarities_of(rows, 0, pkeys)});
        }
        if (temporaries.rank) {
            filled.push_back(external_search::external_values{
                    .temporary_index = *temporaries.rank, .values = external_search::ranks_of(rows, 0, pkeys)});
        }
        if (temporaries.fragment) {
            auto fragments = co_await bm25_search::highlights_of(
                    qp.vector_store_client(), *_schema, _index, search_term_text, rows, 0, aoe.abort_source());
            filled.push_back(external_search::external_values{
                    .temporary_index = *temporaries.fragment, .values = std::move(fragments)});
        }
        provider.emplace(std::move(filled), rows);
    }
    co_return co_await emit_result_set(std::move(table_results), options, provider ? &*provider : nullptr);
}

} // namespace cql3::statements
