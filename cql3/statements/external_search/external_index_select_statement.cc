/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "cql3/statements/external_search/external_index_select_statement.hh"
#include "cql3/statements/external_search/ann_search.hh"
#include "cql3/statements/external_search/bm25_search.hh"

#include "cql3/statements/external_search/external_function.hh"
#include "cql3/statements/external_search/values_provider.hh"
#include "cql3/expr/evaluate.hh"
#include "cql3/query_processor.hh"
#include "utils/assert.hh"
#include "vector_search/hybrid_search.hh"
#include "cql3/statements/index_latency.hh"
#include "db/consistency_level_validations.hh"
#include "query/query_result_merger.hh"
#include "service/storage_proxy.hh"
#include "utils/result_loop.hh"

#include <seastar/core/future.hh>
#include <seastar/coroutine/exception.hh>

#include <algorithm>

namespace cql3::statements {

namespace {

template<typename T = void>
using coordinator_result = cql3::statements::select_statement::coordinator_result<T>;

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

using functions::search_family;

bool is_ann(const std::vector<search_source>& sources) {
    return sources.front().family == search_family::ann;
}

/// The name of the kind of query, as the messages and the paging warning spell it.
std::string_view query_kind_name(const std::vector<search_source>& sources) {
    return is_ann(sources) ? "Vector search" : "Full-text search";
}

/// The evaluated query value of a search, in the form its index takes.
struct query_value {
    /// ANN: the query vector.
    std::vector<float> vector;
    /// BM25: the search term, kept as text because the highlight request needs it too.
    sstring term;
};

query_value evaluate_query_value(const search_source& source, const query_options& options) {
    auto value = expr::evaluate(source.query_value, options);
    if (value.is_null()) {
        // Before the agreement checks, or a null would surface as a disagreement instead.
        throw exceptions::invalid_request_exception(seastar::format("Unsupported null value for column {}", source.column->name_as_text()));
    }

    if (source.deferred_where_term && expr::evaluate(*source.deferred_where_term, options) != value) {
        throw exceptions::invalid_request_exception("Full-text search queries must use the same search term in both WHERE and ORDER BY clauses");
    }
    for (const auto& deferred : source.deferred) {
        if (expr::evaluate(deferred.value, options) != value) {
            throw exceptions::invalid_request_exception(query_value_mismatch_message(source.family, deferred.function_name));
        }
    }

    if (source.family == search_family::bm25) {
        return query_value{.term = bm25_search::query_term(value)};
    }
    return query_value{.vector = ann_search::query_vector(*source.column, value)};
}

/// How many candidates the search is asked for.
uint64_t candidates_wanted(const search_source& source, uint64_t limit) {
    return source.family == search_family::ann ? ann_search::candidates_wanted(source.index, limit) : limit;
}

} // anonymous namespace

::shared_ptr<select_statement> external_index_select_statement::prepare(
        std::vector<search_source> sources, external_statement_args args) {
    throwing_assert(!sources.empty());
    if (sources.size() > 1) {
        throw exceptions::invalid_request_exception("Combining several searches in one query is not supported yet");
    }

    if (!args.limit.has_value()) {
        throw exceptions::invalid_request_exception(
                seastar::format("{} queries must have a limit specified", query_kind_name(sources)));
    }
    if (args.per_partition_limit.has_value()) {
        throw exceptions::invalid_request_exception(
                seastar::format("{} queries do not support per-partition limits", query_kind_name(sources)));
    }
    if (args.selection->is_aggregate() || !args.group_by_cell_indices->empty()) {
        throw exceptions::invalid_request_exception(
                seastar::format("{} queries cannot be run with aggregation", query_kind_name(sources)));
    }

    // The results are matched to the rows by primary key.
    if (std::ranges::any_of(sources, &search_source::is_selected)) {
        external_search::fetch_primary_key_columns(*args.selection, *args.schema);
    }

    auto prepared_filter = is_ann(sources)
            ? external_search::prepare_filter(*args.restrictions, args.parameters->allow_filtering())
            : external_search::prepared_filter{{}, args.parameters->allow_filtering()};

    return ::make_shared<external_index_select_statement>(std::move(sources), std::move(prepared_filter), std::move(args));
}

external_index_select_statement::external_index_select_statement(std::vector<search_source> sources,
        external_search::prepared_filter prepared_filter, external_statement_args args)
    : select_statement{args.schema, args.bound_terms, args.parameters, args.selection, args.restrictions,
              args.group_by_cell_indices, args.is_reversed, args.ordering_comparator, args.limit, args.per_partition_limit,
              args.stats, std::move(args.attrs)}
    , _sources(std::move(sources))
    , _prepared_filter(std::move(prepared_filter)) {
}

future<::shared_ptr<cql_transport::messages::result_message>> external_index_select_statement::execute_search(
        query_processor& qp, service::query_state& state, const query_options& options, uint64_t limit) const {

    if (limit > max_query_limit) {
        // The ANN wording is pinned by test/cqlpy/cassandra_tests/vector_invalid_query_test.py.
        co_await coroutine::return_exception(exceptions::invalid_request_exception(is_ann(_sources)
                        ? seastar::format("Use of ANN OF in an ORDER BY clause requires a LIMIT that is not greater than {}. LIMIT was {}",
                                  max_query_limit, limit)
                        : seastar::format("{} queries require a LIMIT that is not greater than {}. LIMIT was {}",
                                  query_kind_name(_sources), max_query_limit, limit)));
    }

    auto timeout = db::timeout_clock::now() + get_timeout(state.get_client_state(), options);
    auto aoe = abort_on_expiry(timeout);

    const auto& source = _sources.front();
    const auto value = evaluate_query_value(source, options);

    auto& client = qp.vector_store_client();
    auto filter_json = _prepared_filter.to_json(options);
    const auto wanted = candidates_wanted(source, limit);
    const auto& index_name = source.index.metadata().name();

    auto requests = std::vector<vector_search::search_request>{};
    if (source.family == search_family::ann) {
        requests.push_back(vector_search::ann_request{
                .keyspace = _schema->ks_name(), .index = index_name, .vector = value.vector, .limit = wanted, .filter = std::move(filter_json)});
    } else {
        requests.push_back(vector_search::bm25_request{.keyspace = _schema->ks_name(), .index = index_name, .term = value.term, .limit = wanted});
    }
    auto searched = co_await vector_search::search_all(client, _schema, std::move(requests), aoe.abort_source());
    if (!searched) {
        co_await coroutine::return_exception(exceptions::invalid_request_exception(
                std::visit(vector_search::vector_store_client::ann_error_visitor{}, searched.error())));
    }
    auto candidates = std::move(*searched);
    if (!needs_post_query_ordering() && candidates.size() > limit) {
        // A query that sorts the rows itself keeps every candidate; its limit is applied after sorting.
        candidates.erase(candidates.begin() + limit, candidates.end());
    }

    auto table_results = co_await query_base_table(qp, state, options, timeout, candidates);

    auto provider = std::optional<external_search::values_provider>{};
    if (table_results && source.temporaries.any()) {
        const auto& read = table_results.value();
        auto rows = external_search::join_table_results(*read.rows, read.command->slice, *_schema, &candidates);
        provider.emplace(external_search::search_values_of(source.temporaries, rows, 0, candidates), rows);
    }
    co_return co_await emit_result_set(std::move(table_results), options, provider ? &*provider : nullptr);
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

    // Read one row for every key the searches returned. process_results() later applies the
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
        result->add_warning(fmt::format("Paging is not supported for {} queries. The entire result set has been returned.", query_kind_name(_sources)));
    }
}

future<::shared_ptr<cql_transport::messages::result_message>> external_index_select_statement::do_execute(
        query_processor& qp, service::query_state& state, const query_options& options) const {
    auto limit = get_limit(options, _limit);

    auto result = co_await measure_index_latency(
            *_schema, _sources.front().index, [this, &qp, &state, &options, &limit]() mutable -> future<::shared_ptr<cql_transport::messages::result_message>> {
                setup_execute(state, options);
                co_return co_await execute_search(qp, state, options, limit);
            });

    maybe_add_paging_warning(result, options, limit);
    co_return result;
}

} // namespace cql3::statements
