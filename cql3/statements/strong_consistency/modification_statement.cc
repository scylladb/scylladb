/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "modification_statement.hh"

#include "db/consistency_level_type.hh"
#include "db/timeout_clock.hh"
#include "transport/messages/result_message.hh"
#include "cql3/query_processor.hh"
#include "service/strong_consistency/coordinator.hh"
#include "service/strong_consistency/groups_manager.hh"
#include "exceptions/exceptions.hh"
#include "utils/error_injection.hh"

namespace cql3::statements::strong_consistency {

static logging::logger logger("sc_modification_statement");

using result_message = cql_transport::messages::result_message;

template <typename Base>
future<::shared_ptr<result_message>> strongly_consistent<Base>::do_execute(
        query_processor& qp, service::query_state& qs, const query_options& options) const {
    // Referred to through the base class, because some of the statement kinds
    // narrow the access of the overrides used below.
    const cql3::statements::modification_statement& stmt = *this;

    const auto cl = options.get_consistency();
    if (cl != db::consistency_level::QUORUM && cl != db::consistency_level::LOCAL_QUORUM) {
        throw exceptions::invalid_request_exception("Strongly consistent writes must use QUORUM/LOCAL_QUORUM consistency level");
    }

    auto timeout = db::timeout_clock::now() + stmt.get_timeout(qs.get_client_state(), options);
    auto json_cache = stmt.maybe_prepare_json_cache(options);
    const auto keys = stmt.build_partition_keys(options, json_cache);
    if (keys.size() != 1 || !query::is_single_partition(keys[0])) {
        throw exceptions::invalid_request_exception("Strongly consistent queries can only target a single partition");
    }

    auto [coordinator, holder] = qp.acquire_strongly_consistent_coordinator();
    const auto token = keys[0].start()->value().token();

    // The mutation is built inside the callback because the coordinator assigns
    // the timestamp, and calls back again whenever it retries.
    auto mutate_result = co_await coordinator.get().mutate(stmt.s,
        token,
        [&](api::timestamp_type ts) {
            const auto prefetch_data = update_parameters::prefetch_data(stmt.s);
            const auto ttl = stmt.get_time_to_live(options);
            const auto params = update_parameters(stmt.s, options, ts, ttl, prefetch_data);
            const auto ranges = stmt.create_clustering_ranges(options, json_cache);
            auto muts = stmt.apply_updates(keys, ranges, params, json_cache);
            if (muts.size() != 1) {
                on_internal_error(logger, ::format("statement on {}.{} has unexpected number of mutations {}",
                    stmt.keyspace(), stmt.column_family(), muts.size()));
            }
            return std::move(*muts.begin());
        }, timeout, qs.get_client_state().get_abort_source());

    using namespace service::strong_consistency;
    if (auto* redirect = get_if<need_redirect>(&mutate_result)) {
        bool is_write = true;
        co_return co_await redirect_statement(qp, options, redirect->target, timeout, is_write, coordinator.get().get_stats(), std::move(redirect->on_forwarding_finished));
    }
    utils::get_local_injector().inject("sc_modification_statement_timeout", [&] {
        throw exceptions::mutation_write_timeout_exception{"", "", options.get_consistency(), 0, 0, db::write_type::SIMPLE};
    });

    auto result = seastar::make_shared<result_message::void_message>();

    if (qs.get_client_state().is_protocol_extension_set(cql_transport::cql_protocol_extension::TABLETS_ROUTING_V2_EXPERIMENTAL)) {
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
        const auto& table = stmt.s->table();

        auto maybe_routing_info_v2 = groups_manager.check_tablet_version(table, token, *options.get_tablet_version_block());
        if (maybe_routing_info_v2) {
            result->add_tablet_info_v2(std::move(*maybe_routing_info_v2));
        }
    }

    co_return std::move(result);
}

template class strongly_consistent<cql3::statements::update_statement>;
template class strongly_consistent<cql3::statements::delete_statement>;
template class strongly_consistent<cql3::statements::insert_prepared_json_statement>;

}
