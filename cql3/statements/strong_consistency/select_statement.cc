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
#include "replica/database.hh"
#include "service/strong_consistency/coordinator.hh"
#include "cql3/statements/strong_consistency/statement_helpers.hh"
#include "service/strong_consistency/groups_manager.hh"

namespace cql3::statements::strong_consistency {

using result_message = cql_transport::messages::result_message;

static service::strong_consistency::read_type parse_consistency_level(const db::consistency_level& cl) {
    switch (cl) {
    case db::consistency_level::QUORUM:
    case db::consistency_level::LOCAL_QUORUM:
        return service::strong_consistency::read_type::linearizable;
    case db::consistency_level::ONE:
    case db::consistency_level::LOCAL_ONE:
        return service::strong_consistency::read_type::non_linearizable;
    default:
        throw exceptions::invalid_request_exception("Strongly consistent reads must use QUORUM/LOCAL_QUORUM or ONE/LOCAL_ONE consistency level");
    }
}

future<::shared_ptr<result_message>> select_statement::do_execute(query_processor& qp,
        service::query_state& state, 
        const query_options& options) const
{
    auto read_type = parse_consistency_level(options.get_consistency());

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
    auto query_result = co_await coordinator.get().query(_query_schema, *read_command,
        key_ranges, read_type, state.get_trace_state(), timeout, state.get_client_state().get_abort_source());

    using namespace service::strong_consistency;
    if (auto* redirect = get_if<need_redirect>(&query_result)) {
        bool is_write = false;
        co_return co_await redirect_statement(qp, options, redirect->target, timeout, is_write, coordinator.get().get_stats(), std::move(redirect->on_forwarding_finished));
    }

    auto result = co_await process_results(get<lw_shared_ptr<query::result>>(std::move(query_result)),
        read_command, options, now);

    if (state.get_client_state().is_protocol_extension_set(cql_transport::cql_protocol_extension::TABLETS_ROUTING_V2_EXPERIMENTAL)) {
        if (!options.get_tablet_version_block().has_value()) {
            // V2 is negotiated but no block was parsed. process_execute_internal()
            // reads the block unconditionally whenever the V2 extension is set and
            // rejects the request with a protocol_exception if the byte is missing,
            // so the block is guaranteed present here. Reaching this point is a
            // server-side invariant violation, not a client error, hence on_internal_error.
            utils::on_internal_error(
                "The protocol extension tablets-routing-v2 requires that every EXECUTE request "
                "carry a tablet_version_block");
        }

        const auto& groups_manager = coordinator.get().get_groups_manager();
        const auto& table = _query_schema->table();
        const auto& token = key_ranges[0].start()->value().token();

        auto maybe_routing_info_v2 = groups_manager.check_tablet_version(table, token, *options.get_tablet_version_block());
        if (maybe_routing_info_v2) {
            result->add_tablet_info_v2(std::move(*maybe_routing_info_v2));
        }
    }

    co_return std::move(result);
}

}
