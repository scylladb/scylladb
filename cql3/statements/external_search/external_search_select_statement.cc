/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "cql3/statements/external_search/external_search_select_statement.hh"
#include "cql3/statements/external_search/ann_search.hh"
#include "cql3/statements/external_search/bm25_search.hh"

#include "cql3/statements/external_search/external_function.hh"
#include "cql3/statements/external_search/external_search_provider.hh"
#include "cql3/expr/evaluate.hh"
#include "cql3/expr/expr-utils.hh"
#include "cql3/functions/scoring_fcts.hh"
#include "cql3/query_processor.hh"
#include "cql3/util.hh"
#include "cql3/restrictions/statement_restrictions.hh"
#include "index/vector_index.hh"
#include "types/vector.hh"
#include "utils/assert.hh"

#include <seastar/core/future.hh>
#include <seastar/coroutine/all.hh>
#include <seastar/coroutine/exception.hh>

#include <algorithm>
#include <map>
#include <ranges>

namespace cql3::statements {

namespace {

using functions::search_family;

using primary_keys = vector_search::vector_store_client::primary_keys;

bool is_ann(const std::vector<search_source>& sources) {
    return sources.front().family == search_family::ann;
}

/// The name of the query family in error messages.
std::string_view query_kind_name(const std::vector<search_source>& sources) {
    return is_ann(sources) ? "Vector ANN" : "Full-text search";
}

/// The evaluated query value of one search, in the form its index takes.
struct query_value {
    /// ANN: the query vector.
    std::vector<float> vector;
    /// BM25: the search term, kept as text because the highlight request needs it too.
    sstring term;
};

query_value evaluate_query_value(const search_source& source, const query_options& options) {
    auto value = expr::evaluate(source.query_value, options);
    if (value.is_null()) {
        throw exceptions::invalid_request_exception(source.family == search_family::ann
                        ? seastar::format("Unsupported null value for column {}", source.column->name_as_text())
                        : sstring("Full-text search query term must not be null"));
    }

    // Query values prepare could not compare because of a bind marker are compared now.
    for (const auto& deferred : source.deferred) {
        if (expr::evaluate(deferred.value, options) != value) {
            throw exceptions::invalid_request_exception(deferred.disagreement_message);
        }
    }

    if (source.family == search_family::bm25) {
        return query_value{.term = bm25_search::query_term(value)};
    }
    return query_value{.vector = ann_search::query_vector(*source.column, value)};
}

/// How many candidates one search of this statement is asked for.
uint64_t candidates_wanted(const search_source& source, uint64_t limit) {
    auto wanted = limit;
    if (source.family == search_family::ann) {
        wanted = ann_search::candidates_wanted(source.index, wanted);
    }
    return std::min(wanted, external_search_select_statement::max_query_limit);
}

/// Checks the WHERE clause the way each family of search requires.
void validate_restrictions(std::vector<search_source>& sources, const restrictions::statement_restrictions& restrictions) {
    const auto& scoring = restrictions.get_scoring_function_restrictions();

    auto& source = sources.front();
    if (source.family == search_family::ann) {
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
    if (auto deferred = bm25_search::validate_restriction(scoring.front(), source.index, source.query_value)) {
        source.deferred.push_back({std::move(*deferred),
                "Full-text search queries must use the same search term in both WHERE and ORDER BY clauses"});
    }

    // The BM25 relation is held out of `restrictions`, and a full-text index takes no other filter.
    if (!restrictions.partition_key_restrictions_is_empty()
            || !restrictions::is_empty_restriction(restrictions.get_clustering_columns_restrictions())
            || !restrictions::is_empty_restriction(restrictions.get_nonprimary_key_restrictions())) {
        throw exceptions::invalid_request_exception("Full-text search queries do not support additional WHERE restrictions");
    }
}

} // anonymous namespace

::shared_ptr<select_statement> external_search_select_statement::prepare(
        data_dictionary::database db, std::vector<search_source> sources, external_statement_args args) {

    if (!args.limit.has_value()) {
        throw exceptions::invalid_request_exception(is_ann(sources)
                        ? sstring("Vector ANN queries must have a limit specified")
                        : seastar::format("{} queries require a LIMIT", query_kind_name(sources)));
    }
    if (args.per_partition_limit.has_value()) {
        throw exceptions::invalid_request_exception(
                seastar::format("{} queries do not support per-partition limits", query_kind_name(sources)));
    }
    // Aggregation and GROUP BY are not allowed.
    if (args.selection->is_aggregate() || !args.group_by_cell_indices->empty()) {
        throw exceptions::invalid_request_exception(
                seastar::format("{} queries cannot be run with aggregation", query_kind_name(sources)));
    }

    validate_restrictions(sources, *args.restrictions);

    // A score or rank is selected, and it is matched to a row by primary key.
    if (std::ranges::any_of(sources, &search_source::needs_primary_key)) {
        external_search::fetch_primary_key_columns(*args.selection, *args.schema);
    }
    // The index generates an excerpt from text the query sends it, so the column has to be read
    // from every row even when the query does not select it.
    for (const auto& source : sources) {
        if (source.fragment_slot) {
            args.selection->add_column_for_post_processing(*source.column);
        }
    }

    auto prepared_filter = is_ann(sources)
            ? external_search::prepare_filter(*args.restrictions, args.parameters->allow_filtering())
            : external_search::prepared_filter{{}, args.parameters->allow_filtering()};

    return ::make_shared<external_search_select_statement>(std::move(sources), std::move(prepared_filter), std::move(args));
}

external_search_select_statement::external_search_select_statement(std::vector<search_source> sources,
        external_search::prepared_filter prepared_filter, external_statement_args args)
    : external_index_select_statement{args.schema, args.bound_terms, args.parameters, args.selection, args.restrictions,
              args.group_by_cell_indices, args.is_reversed, args.ordering_comparator, args.limit, args.per_partition_limit,
              args.stats, sources.front().index, std::move(args.attrs)}
    , _sources(std::move(sources))
    , _prepared_filter(std::move(prepared_filter)) {
}

std::string_view external_search_select_statement::index_search_type_name() const {
    return is_ann(_sources) ? "Vector Search" : "Full-Text Search";
}

future<::shared_ptr<cql_transport::messages::result_message>> external_search_select_statement::execute_search(
        query_processor& qp, service::query_state& state, const query_options& options, uint64_t limit) const {

    if (limit > max_query_limit) {
        co_await coroutine::return_exception(exceptions::invalid_request_exception(is_ann(_sources)
                        ? seastar::format("Use of ANN OF in an ORDER BY clause requires a LIMIT that is not greater than {}. LIMIT was {}",
                                  max_query_limit, limit)
                        : seastar::format("{} queries require a LIMIT that is not greater than {}. LIMIT was {}",
                                  query_kind_name(_sources), max_query_limit, limit)));
    }

    auto timeout = db::timeout_clock::now() + get_timeout(state.get_client_state(), options);
    auto aoe = abort_on_expiry(timeout);

    // Evaluate every query value before asking any index, so that a bad value fails the query
    // before a request is sent.
    auto query_values = std::vector<query_value>{};
    query_values.reserve(_sources.size());
    for (const auto& source : _sources) {
        query_values.push_back(evaluate_query_value(source, options));
    }

    auto& client = qp.vector_store_client();
    auto filter_json = _prepared_filter.to_json(options);

    // One result list per search.
    auto answers = std::vector<primary_keys>(_sources.size());
    {
        const auto& source = _sources.front();
        const auto wanted = candidates_wanted(source, limit);
        const auto& index_name = source.index.metadata().name();

        answers.front() = source.family == search_family::ann
                ? co_await ann_search::ask(client, _schema->ks_name(), index_name, _schema, query_values.front().vector, wanted,
                          filter_json, aoe.abort_source())
                : co_await bm25_search::ask(
                          client, _schema->ks_name(), index_name, _schema, query_values.front().term, wanted, aoe.abort_source());
    }

    auto candidates = answers.front();
    if (!needs_post_query_ordering() && candidates.size() > limit) {
        // The rows are returned in the index's order, so anything past the limit is not needed. A
        // query that sorts the rows itself keeps every candidate; the limit is applied after sorting.
        candidates.erase(candidates.begin() + limit, candidates.end());
    }

    auto read = co_await query_base_table(qp, state, options, timeout, candidates);

    auto provider = std::optional<external_search::external_search_provider>{};
    if (read && std::ranges::any_of(_sources, &search_source::is_selected)) {
        // Only a search whose score or rank is selected is matched to the rows: matching is by
        // primary key, and the key is only fetched for such a query. A search asked only for
        // excerpts neither adds nor drops rows.
        auto reported = std::vector<const vector_search::vector_store_client::primary_keys*>{};
        auto reported_of = std::vector<std::optional<size_t>>(_sources.size(), std::nullopt);
        auto columns = std::vector<const column_definition*>{};
        auto fragment_column_of = std::vector<std::optional<size_t>>(_sources.size(), std::nullopt);
        for (size_t i = 0; i < _sources.size(); ++i) {
            if (_sources[i].needs_primary_key()) {
                reported_of[i] = reported.size();
                reported.push_back(&answers[i]);
            }
            if (_sources[i].fragment_slot) {
                fragment_column_of[i] = columns.size();
                columns.push_back(_sources[i].column);
            }
        }

        // The excerpts are fetched now, from the text of the rows just read.
        const auto& table_read = read.value();
        auto rows = external_search::join_table_results(
                *table_read.rows, table_read.command->slice, *_schema, *_selection, reported, columns);
        external_search::drop_unscored_rows(rows, reported);

        auto filled = std::vector<external_search::external_values>{};
        for (size_t i = 0; i < _sources.size(); ++i) {
            const auto& source = _sources[i];
            if (source.score_slot) {
                filled.push_back(external_search::external_values{.temporary_index = *source.score_slot,
                        .values = external_search::similarities_of(rows, *reported_of[i], answers[i])});
            }
            if (source.rank_slot) {
                filled.push_back(external_search::external_values{
                        .temporary_index = *source.rank_slot, .values = external_search::ranks_of(rows, *reported_of[i], answers[i])});
            }
            if (source.fragment_slot) {
                auto excerpts = co_await bm25_search::highlights_of(client, *_schema, source.index, query_values[i].term, rows,
                        *fragment_column_of[i], aoe.abort_source());
                filled.push_back(external_search::external_values{
                        .temporary_index = *source.fragment_slot, .values = std::move(excerpts)});
            }
        }
        provider.emplace(std::move(filled), rows);
    }
    co_return co_await emit_result_set(std::move(read), options, provider ? &*provider : nullptr);
}

} // namespace cql3::statements
