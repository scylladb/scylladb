/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "select_statement.hh"

#include "db/consistency_level_type.hh"
#include "query/query-request.hh"
#include "cql3/query_processor.hh"
#include "service/strong_consistency/coordinator.hh"
#include "cql3/statements/strong_consistency/statement_helpers.hh"

namespace cql3::statements::strong_consistency {

using result_message = cql_transport::messages::result_message;

static void validate_consistency_level(const db::consistency_level& cl) {
    if (cl != db::consistency_level::QUORUM && cl != db::consistency_level::LOCAL_QUORUM &&
        cl != db::consistency_level::ONE && cl != db::consistency_level::LOCAL_ONE) {
        throw exceptions::invalid_request_exception("Strongly consistent reads must use QUORUM/LOCAL_QUORUM or ONE/LOCAL_ONE consistency level");
    }
}

future<::shared_ptr<result_message>> select_statement::do_execute(query_processor& qp,
        service::query_state& state, 
        const query_options& options) const
{
    validate_consistency_level(options.get_consistency());

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
    auto [coordinator, holder] = qp.acquire_strongly_consistent_coordinator();
    const auto token = key_ranges[0].start()->value().token();
    auto query_result = co_await coordinator.get().query(_query_schema, *read_command,
        key_ranges, state.get_trace_state(), timeout, state.get_client_state().get_abort_source());

    using namespace service::strong_consistency;
    if (const auto* redirect = get_if<need_redirect>(&query_result)) {
        bool is_write = false;
        co_return co_await redirect_statement(qp, options, redirect->target, timeout, is_write, coordinator.get().get_stats());
    }

    auto result = co_await process_results(get<lw_shared_ptr<query::result>>(std::move(query_result)),
        read_command, options, now);
    auto&& table = _query_schema->table();
    if (_may_use_token_aware_routing && table.uses_tablets() && state.get_client_state().is_protocol_extension_set(cql_transport::cql_protocol_extension::TABLETS_ROUTING_V1)) {
        auto erm = table.get_effective_replication_map();
        auto tablet_info = erm->check_locality(token, tablet_routing_source_replica(state.get_client_state(), *erm));
        if (tablet_info) {
            result->add_tablet_info(std::move(tablet_info->tablet_replicas), tablet_info->token_range);
        }
    }
    co_return result;
}

}
