#include "sc_select_statement.hh"

#include "query/query-request.hh"
#include "cql3/query_processor.hh"
#include "service/raft/strong_consistency/sc_storage_proxy.hh"
#include "replica/database.hh"

namespace cql3::statements::strong_consistency {
static logging::logger logger("sc_select_statement");

using result_message = cql_transport::messages::result_message;

future<::shared_ptr<result_message>> sc_select_statement::do_execute(query_processor& qp,
        service::query_state& state, 
        const query_options& options) const
{
    const auto key_ranges = _restrictions->get_partition_key_ranges(options);
    if (key_ranges.size() != 1 || !query::is_single_partition(key_ranges[0])) {
        throw exceptions::invalid_request_exception("Strongly consistent queries can only target a single partition");
    }
    const auto now = gc_clock::now();
    auto read_command = make_lw_shared<query::read_command>(
        _query_schema->id(),
        _query_schema->version(),
        make_partition_slice(options),
        query::max_result_size(query::result_memory_limiter::maximum_result_size),
        query::tombstone_limit(query::tombstone_limit::max),
        query::row_limit(get_inner_loop_limit(get_limit(options, _limit), _selection->is_aggregate())),
        query::partition_limit(query::max_partitions),
        now,
        tracing::make_trace_info(state.get_trace_state()),
        query_id::create_null_id(),
        query::is_first_page::no,
        options.get_timestamp(state));
    const auto timeout = db::timeout_clock::now() + get_timeout(state.get_client_state(), options);
    auto [proxy, holder] = qp.acquire_sc_storage_proxy();
    auto query_result = co_await proxy.get().query(*_query_schema, *read_command,
        key_ranges, state.get_trace_state(), timeout);
    if (const auto* redirect = query_result.get_if_redirect()) {
        const auto my_host_id = qp.db().real_database().get_token_metadata().get_topology().my_host_id();
        if (redirect->host != my_host_id) {
            throw exceptions::invalid_request_exception(format(
                "Strongly consistent queries can be executed only on the leader node, "
                "leader id {}, current host id {}",
                redirect->host, my_host_id));
        }
        auto&& func_values_cache = const_cast<cql3::query_options&>(options).take_cached_pk_function_calls();
        co_return qp.bounce_to_shard(redirect->shard, std::move(func_values_cache));
    }
    co_return co_await process_results(std::move(query_result).extract_result(), read_command, options, now);
}
}