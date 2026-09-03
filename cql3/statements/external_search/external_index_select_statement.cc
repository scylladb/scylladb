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

#include <seastar/core/future.hh>

template<typename T = void>
using coordinator_result = cql3::statements::select_statement::coordinator_result<T>;

namespace cql3 {

namespace statements {

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
        ::shared_ptr<const restrictions::statement_restrictions> restrictions,
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

future<::shared_ptr<cql_transport::messages::result_message>> external_index_select_statement::query_base_table(query_processor& qp,
        service::query_state& state, const query_options& options, const std::vector<vector_search::primary_key>& pkeys, lowres_clock::time_point timeout,
        std::unique_ptr<cql3::selection::external_values_provider> provider) const {
    auto command = prepare_command_for_base_query(qp, state, options, pkeys.size());

    auto result = co_await query_base_table(qp, state, options, command, timeout, pkeys);

    command->set_row_limit(get_limit(options, _limit));

    co_return co_await wrap_result_to_error_message([this, command = std::move(command), &options, provider_ptr = provider.get()](auto query_result) {
        return process_results(std::move(query_result), command, options, _query_start_time_point, provider_ptr);
    })(std::move(result));
}

future<coordinator_result<foreign_ptr<lw_shared_ptr<query::result>>>> external_index_select_statement::query_base_table(query_processor& qp,
        service::query_state& state, const query_options& options, lw_shared_ptr<query::read_command> command, lowres_clock::time_point timeout,
        const std::vector<vector_search::primary_key>& pkeys) const {

    // For tables without clustering columns, we can optimize by querying
    // partition ranges instead of individual primary keys, since the
    // partition key alone uniquely identifies each row.
    if (_schema->clustering_key_size() == 0) {
        auto to_partition_ranges = [](const std::vector<vector_search::primary_key>& pkeys) -> std::vector<dht::partition_range> {
            std::vector<dht::partition_range> partition_ranges;
            std::ranges::transform(pkeys, std::back_inserter(partition_ranges), [](const auto& pkey) {
                return dht::partition_range::make_singular(pkey.partition);
            });

            return partition_ranges;
        };
        co_return co_await query_base_table(qp, state, options, std::move(command), timeout, to_partition_ranges(pkeys));
    }
    co_return co_await utils::result_map_reduce(
            pkeys.begin(), pkeys.end(),
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
}

future<coordinator_result<foreign_ptr<lw_shared_ptr<query::result>>>> external_index_select_statement::query_base_table(query_processor& qp,
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

} // namespace statements

} // namespace cql3
