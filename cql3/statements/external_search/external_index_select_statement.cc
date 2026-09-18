/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "cql3/statements/external_search/external_index_select_statement.hh"

#include "cql3/statements/index_latency.hh"
#include "cql3/query_processor.hh"
#include "db/consistency_level_validations.hh"
#include "query/query_result_merger.hh"
#include "service/storage_proxy.hh"
#include "utils/result_loop.hh"
#include "cql3/statements/external_search/ann_search.hh"
#include "cql3/expr/evaluate.hh"
#include "cql3/expr/expr-utils.hh"
#include "cql3/functions/functions.hh"
#include "cql3/functions/scoring_fcts.hh"
#include "vector_search/hybrid_search.hh"
#include "cql3/statements/external_search/external_function.hh"
#include "cql3/statements/external_search/values_provider.hh"
#include "cql3/statements/external_search/ann_search.hh"
#include "cql3/statements/raw/select_statement.hh"
#include "cql3/util.hh"
#include "db/config.hh"
#include "exceptions/exceptions.hh"
#include "index/vector_index.hh"
#include "schema/schema.hh"
#include "types/types.hh"
#include "types/vector.hh"
#include "utils/assert.hh"
#include "cql3/statements/external_search/bm25_search.hh"
#include "cql3/expr/evaluate.hh"
#include "cql3/expr/expr-utils.hh"
#include "cql3/expr/expression.hh"
#include "cql3/functions/scoring_fcts.hh"
#include "vector_search/hybrid_search.hh"
#include "cql3/restrictions/statement_restrictions.hh"
#include "cql3/statements/external_search/external_function.hh"
#include "cql3/statements/external_search/bm25_search.hh"
#include "cql3/statements/external_search/values_provider.hh"
#include "cql3/statements/raw/select_statement.hh"
#include "data_dictionary/data_dictionary.hh"
#include "exceptions/exceptions.hh"
#include "index/secondary_index_manager.hh"
#include "schema/schema.hh"
#include "types/types.hh"
#include "utils/assert.hh"

#include <seastar/core/future.hh>
#include <seastar/coroutine/exception.hh>
#include <cmath>
#include <seastar/coroutine/exception.hh>

template<typename T = void>
using coordinator_result = cql3::statements::select_statement::coordinator_result<T>;

namespace cql3::statements {

namespace {

template<typename C>
struct result_to_error_message_wrapper {
    C c;

    template<typename T>
    auto operator()(coordinator_result<T>&& arg) {
        if constexpr (std::is_void_v<T>) {
            if (arg) {
                return futurize_invoke(c);
            } else {
                return make_ready_future<typename futurize_t<std::invoke_result_t<C>>::value_type>(
                    ::make_shared<cql_transport::messages::result_message::exception>(std::move(arg).assume_error())
                );
            }
        } else {
            if (arg) {
                return futurize_invoke(c, std::move(arg).value());
            } else {
                return make_ready_future<typename futurize_t<std::invoke_result_t<C, T>>::value_type>(
                    ::make_shared<cql_transport::messages::result_message::exception>(std::move(arg).assume_error())
                );
            }
        }
    }
};

template<typename C>
auto wrap_result_to_error_message(C&& c) {
    return result_to_error_message_wrapper<C>{std::move(c)};
}

} // anonymous namespace

external_index_select_statement::external_index_select_statement(schema_ptr schema, uint32_t bound_terms,
        lw_shared_ptr<const parameters> parameters,
        ::shared_ptr<selection::selection> selection,
        ::shared_ptr<const restrictions::select_restrictions> restrictions,
        ::shared_ptr<std::vector<size_t>> group_by_cell_indices,
        bool is_reversed,
        ordering_comparator_type ordering_comparator,
        std::optional<expr::expression> limit,
        std::optional<expr::expression> per_partition_limit,
        cql_stats& stats,
        const secondary_index::index& index,
        std::unique_ptr<cql3::attributes> attrs)
    : select_statement{schema, bound_terms, parameters, selection, restrictions, group_by_cell_indices,
              is_reversed, ordering_comparator, limit, per_partition_limit, stats, std::move(attrs)}
    , _index{index} {
}

lw_shared_ptr<query::read_command> external_index_select_statement::prepare_command_for_base_query(
        query_processor& qp, service::query_state& state, const query_options& options, uint64_t fetch_limit) const {
    auto slice = make_partition_slice(options);
    return ::make_lw_shared<query::read_command>(_schema->id(), _schema->version(), std::move(slice), qp.proxy().get_max_result_size(slice),
            query::tombstone_limit(qp.proxy().get_tombstone_limit()),
            query::row_limit(get_inner_loop_limit(fetch_limit, _selection->is_aggregate())), query::partition_limit(query::max_partitions),
            _query_start_time_point, tracing::make_trace_info(state.get_trace_state()), query_id::create_null_id(), query::is_first_page::no,
            options.get_timestamp(state));
}

future<::shared_ptr<cql_transport::messages::result_message>> external_index_select_statement::emit_result_set(
        coordinator_result<base_table_read> table_results, const query_options& options,
        const cql3::selection::external_values_provider* provider) const {
    co_return co_await wrap_result_to_error_message([this, &options, provider](base_table_read read) {
        // process_results() trims the result set to this limit once post-query ordering has sorted it.
        read.command->set_row_limit(get_limit(options, _limit));
        return process_results(std::move(read.rows), read.command, options, _query_start_time_point, provider);
    })(std::move(table_results));
}

future<coordinator_result<external_index_select_statement::base_table_read>> external_index_select_statement::query_base_table(query_processor& qp,
        service::query_state& state, const query_options& options, lowres_clock::time_point timeout,
        std::span<const vector_search::search_candidate> candidates) const {

    // Read one row for every key the index returned. process_results() later applies the
    // user's LIMIT, and the provider may drop some rows, so fewer rows than this may reach the
    // client.
    auto command = prepare_command_for_base_query(qp, state, options, candidates.size());

    // For tables without clustering columns, we can optimize by querying
    // partition ranges instead of individual primary keys, since the
    // partition key alone uniquely identifies each row.
    if (_schema->clustering_key_size() == 0) {
        auto to_partition_ranges = [](std::span<const vector_search::search_candidate> candidates) -> std::vector<dht::partition_range> {
            std::vector<dht::partition_range> partition_ranges;
            std::ranges::transform(candidates, std::back_inserter(partition_ranges), [](const auto& candidate) {
                return dht::partition_range::make_singular(candidate.partition);
            });

            return partition_ranges;
        };
        auto rows = co_await query_partition_ranges(qp, state, options, command, timeout, to_partition_ranges(candidates));
        if (!rows) {
            co_return std::move(rows).as_failure();
        }
        co_return base_table_read{std::move(rows).value(), std::move(command)};
    }
    auto rows = co_await utils::result_map_reduce(
            candidates.begin(), candidates.end(),
            [&](this auto, auto& key) -> future<coordinator_result<foreign_ptr<lw_shared_ptr<query::result>>>> {
                auto cmd = ::make_lw_shared<query::read_command>(*command);
                cmd->slice._row_ranges = query::clustering_row_ranges{query::clustering_range::make_singular(key.clustering)};
                coordinator_result<service::storage_proxy::coordinator_query_result> rqr =
                        co_await qp.proxy().query_result(_schema, cmd, {dht::partition_range::make_singular(key.partition)}, options.get_consistency(),
                                {timeout, state.get_permit(), state.get_client_state(), state.get_trace_state()});
                if (!rqr) {
                    co_return std::move(rqr).as_failure();
                }
                co_return std::move(rqr.value().query_result);
            },
            query::result_merger{command->get_row_limit(), query::max_partitions});
    if (!rows) {
        co_return std::move(rows).as_failure();
    }
    co_return base_table_read{std::move(rows).value(), std::move(command)};
}

future<coordinator_result<foreign_ptr<lw_shared_ptr<query::result>>>> external_index_select_statement::query_partition_ranges(query_processor& qp,
        service::query_state& state, const query_options& options, lw_shared_ptr<query::read_command> command, lowres_clock::time_point timeout,
        std::vector<dht::partition_range> partition_ranges) const {

    coordinator_result<service::storage_proxy::coordinator_query_result> rqr = co_await qp.proxy()
            .query_result(_query_schema, command, std::move(partition_ranges), options.get_consistency(),
                    {timeout, state.get_permit(), state.get_client_state(), state.get_trace_state(), {}, {}, options.get_specific_options().node_local_only},
                    std::nullopt);
    if (!rqr) {
        co_return std::move(rqr).as_failure();
    }
    co_return std::move(rqr.value().query_result);
}

void external_index_select_statement::update_stats() const {
    ++_stats.secondary_index_reads;
    ++_stats.query_cnt(source_selector::USER, _ks_sel, cond_selector::NO_CONDITIONS, statement_type::SELECT);
}

void external_index_select_statement::setup_execute(service::query_state& state, const query_options& options) const {
    tracing::add_table_name(state.get_trace_state(), keyspace(), column_family());
    validate_for_read(options.get_consistency());
    _query_start_time_point = gc_clock::now();
    update_stats();
}

void external_index_select_statement::maybe_add_paging_warning(
        const ::shared_ptr<cql_transport::messages::result_message>& result, const query_options& options, uint64_t limit) const {
    auto page_size = options.get_page_size();
    if (page_size > 0 && (uint64_t)page_size < limit) {
        result->add_warning(fmt::format("Paging is not supported for {} queries. The entire result set has been returned.", index_search_type_name()));
    }
}

future<::shared_ptr<cql_transport::messages::result_message>> external_index_select_statement::do_execute(
        query_processor& qp, service::query_state& state, const query_options& options) const {
    auto limit = get_limit(options, _limit);

    auto result = co_await measure_index_latency(
            *_schema, _index, [this, &qp, &state, &options, &limit]() mutable -> future<::shared_ptr<cql_transport::messages::result_message>> {
                setup_execute(state, options);
                co_return co_await execute_search(qp, state, options, limit);
            });

    maybe_add_paging_warning(result, options, limit);
    co_return result;
}


::shared_ptr<cql3::statements::select_statement> fulltext_indexed_table_select_statement::prepare(data_dictionary::database db,
        schema_ptr schema, uint32_t bound_terms, lw_shared_ptr<const parameters> parameters,
        ::shared_ptr<selection::selection> selection, ::shared_ptr<const restrictions::select_restrictions> restrictions,
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

    ordering_info->deferred_where_term = bm25_search::validate_restriction(scoring_restrictions.front(), ordering_info->search_term);

    // Reject any WHERE restrictions beyond the single BM25 clause.
    // BM25 restrictions are excluded from `restrictions`.
    if (!restrictions->partition_key_restrictions_is_empty()
            || !restrictions::is_empty_restriction(restrictions->get_clustering_columns_restrictions())
            || !restrictions::is_empty_restriction(restrictions->get_nonprimary_key_restrictions())) {
        throw exceptions::invalid_request_exception(
                "Full-text search queries do not support additional WHERE restrictions");
    }

    // The score and the rank are matched to a row by primary key.
    if (ordering_info->temporaries.any()) {
        external_search::fetch_primary_key_columns(*selection, *schema);
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
        ::shared_ptr<const restrictions::select_restrictions> restrictions,
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

    auto requests = std::vector<vector_search::search_request>{};
    requests.push_back(vector_search::bm25_request{
            .keyspace = _schema->ks_name(), .index = _index.metadata().name(), .term = search_term_text, .limit = limit});
    auto searched = co_await vector_search::search_all(qp.vector_store_client(), _schema, std::move(requests), aoe.abort_source());
    if (!searched) {
        co_await coroutine::return_exception(
                exceptions::invalid_request_exception(std::visit(vector_search::vector_store_client::fts_error_visitor{}, searched.error())));
    }
    auto candidates = std::move(*searched);
    throwing_assert(candidates.size() <= limit);

    auto table_results = co_await query_base_table(qp, state, options, timeout, candidates);

    auto provider = std::optional<external_search::values_provider>{};
    if (table_results && _bm25_ordering_info.temporaries.any()) {
        const auto& read = table_results.value();
        auto rows = external_search::join_table_results(*read.rows, read.command->slice, *_schema, &candidates);
        provider.emplace(external_search::search_values_of(_bm25_ordering_info.temporaries, rows, 0, candidates), rows);
    }
    co_return co_await emit_result_set(std::move(table_results), options, provider ? &*provider : nullptr);
}


::shared_ptr<cql3::statements::select_statement> vector_indexed_table_select_statement::prepare(data_dictionary::database db, schema_ptr schema,
        uint32_t bound_terms, lw_shared_ptr<const parameters> parameters, ::shared_ptr<selection::selection> selection,
        ::shared_ptr<const restrictions::select_restrictions> restrictions, ::shared_ptr<std::vector<size_t>> group_by_cell_indices, bool is_reversed,
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
        ::shared_ptr<selection::selection> selection, ::shared_ptr<const restrictions::select_restrictions> restrictions,
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

    for (const auto& selected : _ann_ordering_info.deferred_select_vectors) {
        if (expr::evaluate(selected.vector, options) != ordering_vector) {
            co_await coroutine::return_exception(exceptions::invalid_request_exception(seastar::format(
                    "{}() in SELECT must use the same query vector as the ANN ordering", selected.function_name)));
        }
    }

    auto timeout = db::timeout_clock::now() + get_timeout(state.get_client_state(), options);
    auto aoe = abort_on_expiry(timeout);
    auto filter_json = _prepared_filter.to_json(options);
    const auto fetch = ann_search::candidates_wanted(_index, limit);
    auto requests = std::vector<vector_search::search_request>{};
    requests.push_back(vector_search::ann_request{.keyspace = _schema->ks_name(),
            .index = _index.metadata().name(),
            .vector = ann_search::query_vector(*prepared_ann_ordering.first, ordering_vector),
            .limit = fetch,
            .filter = std::move(filter_json)});
    auto searched = co_await vector_search::search_all(qp.vector_store_client(), _schema, std::move(requests), aoe.abort_source());
    if (!searched) {
        co_await coroutine::return_exception(
                exceptions::invalid_request_exception(std::visit(vector_search::vector_store_client::ann_error_visitor{}, searched.error())));
    }
    auto candidates = std::move(*searched);
    if (candidates.size() > limit && !_ann_ordering_info.is_rescoring_enabled) {
        candidates.erase(candidates.begin() + limit, candidates.end());
    }

    auto table_results = co_await query_base_table(qp, state, options, timeout, candidates);

    auto provider = std::optional<external_search::values_provider>{};
    if (table_results && _ann_ordering_info.temporaries.any()) {
        const auto& read = table_results.value();
        auto rows = external_search::join_table_results(*read.rows, read.command->slice, *_schema, &candidates);
        provider.emplace(external_search::search_values_of(_ann_ordering_info.temporaries, rows, 0, candidates), rows);
    }
    co_return co_await emit_result_set(std::move(table_results), options, provider ? &*provider : nullptr);
}

} // namespace cql3::statements
