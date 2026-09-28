/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.1 and Apache-2.0)
 */

#include "cql3/statements/eventual_consistency/modification_executor.hh"

#include "transport/cql_protocol_extension.hh"
#include "transport/messages/result_message.hh"
#include "cql3/attributes.hh"
#include "cql3/query_processor.hh"
#include "cql3/query_options.hh"
#include "cql3/statements/cas_request.hh"
#include "cql3/statements/modification_statement.hh"
#include "data_dictionary/data_dictionary.hh"
#include "db/consistency_level_validations.hh"
#include "db/large_data_handler.hh"
#include "replica/database.hh"
#include "service/storage_proxy.hh"
#include "utils/error_injection.hh"
#include "utils/result.hh"

#include <boost/lexical_cast.hpp>

template<typename T = void>
using coordinator_result = exceptions::coordinator_result<T>;

namespace cql3::statements::eventual_consistency {

namespace {

future<::shared_ptr<cql_transport::messages::result_message>>
process_forced_rebounce(unsigned shard, query_processor& qp, const query_options& options) {
    static int64_t counter = {0};
    static logging::logger logger("modification_statement");
    if (counter <= 0) {
        const auto counter_opt = utils::get_local_injector().inject_parameter<decltype(counter)>("forced_bounce_to_shard_counter");
        decltype(counter) counter_value = 0;
        if (!counter_opt) {
            logger.warn("forced_bounce_to_shard_counter is not set. Using default value 1.");
        } else {
            try {
                counter_value = boost::lexical_cast<decltype(counter_value)>(*counter_opt);
            } catch (const boost::bad_lexical_cast& e) {
                logger.warn("Incorrect forced_bounce_to_shard_counter value: [{}]. Using default value 1.", *counter_opt);
            }
        }
        if (counter_value <= 0) {
            counter_value = 1;
        }
        counter = counter_value;
    }

    const auto prev_counter_value = counter;
    if (prev_counter_value <= 1) {
        logger.info("Disabling forced_bounce_to_shard_counter.");
        co_await utils::error_injection_type::disable_on_all("forced_bounce_to_shard_counter");
        counter = 0;
    } else {
        --counter;
    }

    // While counter > 1 select a different shard to re-bounce to.
    // On the last iteration, re-bounce to the correct shard.
    if (counter != 0) {
        const auto shard_num = this_smp_shard_count();
        const auto local_shard = this_shard_id();
        auto target_shard = local_shard + 1;
        if (target_shard == shard) {
            ++target_shard;
        }
        if (target_shard > shard_num - 1) {
            target_shard = 0;
        }
        shard = target_shard;
    }

    logger.info("Applying forced_bounce_to_shard_counter, re-bouncing to shard {}.", shard);
    co_return co_await make_ready_future<shared_ptr<cql_transport::messages::result_message>>(
        qp.bounce_to_shard(shard, std::move(const_cast<cql3::query_options&>(options).take_cached_pk_function_calls())));
}

future<coordinator_result<>>
execute_without_condition(const modification_statement& stmt, query_processor& qp, service::query_state& qs,
        const query_options& options, modification_spec&& spec, db::large_data_violation_type* violations) {
    auto cl = options.get_consistency();
    auto timeout = db::timeout_clock::now() + stmt.get_timeout(qs.get_client_state(), options);
    return get_mutations(stmt, qp, options, timeout, false, options.get_timestamp(qs), qs, std::move(spec)).then(
            [&stmt, cl, timeout, &qp, &qs, &options, violations] (auto mutations) {
        if (mutations.empty()) {
            return make_ready_future<coordinator_result<>>(bo::success());
        }

        return qp.proxy().mutate_with_triggers(std::move(mutations), cl, timeout, false, qs.get_trace_state(), qs.get_permit(), db::allow_per_partition_rate_limit::yes, stmt.is_raw_counter_shard_write(), {
            .node_local_only = options.get_specific_options().node_local_only,
            .bypass_large_data_guardrails = stmt.attrs->is_bypass_large_data_guardrails(),
            .violations_out = violations
        });
    });
}

future<::shared_ptr<cql_transport::messages::result_message>>
execute_with_condition(const modification_statement& stmt, query_processor& qp, service::query_state& qs,
        const query_options& options) {

    auto cl_for_learn = options.get_consistency();
    utils::result_with_exception_ptr<db::consistency_level> cl_for_paxos = options.check_serial_consistency();
    if (!cl_for_paxos) [[unlikely]] {
        return make_exception_future<shared_ptr<cql_transport::messages::result_message>>(std::move(cl_for_paxos).assume_error());
    }
    db::timeout_clock::time_point now = db::timeout_clock::now();
    const timeout_config& cfg = qs.get_client_state().get_timeout_config();

    auto statement_timeout = now + cfg.write_timeout; // All CAS networking operations run with write timeout.
    auto cas_timeout = now + cfg.cas_timeout;         // When to give up due to contention.
    auto read_timeout = now + cfg.read_timeout;       // When to give up on query.

    modification_spec spec(stmt, options);

    if (spec.keys.empty()) {
        throw exceptions::invalid_request_exception(format("Unrestricted partition key in a conditional {}",
                    stmt.type.is_update() ? "update" : "deletion"));
    }
    if (spec.ranges.empty()) {
        throw exceptions::invalid_request_exception(format("Unrestricted clustering key in a conditional {}",
                    stmt.type.is_update() ? "update" : "deletion"));
    }

    auto request = std::make_unique<cas_request>(stmt.s);
    auto* request_ptr = request.get();
    // cas_request can be used for batches as well single statements; Here we have just a single
    // modification in the list of CAS commands, since we're handling single-statement execution.
    request->add_row_update(stmt, std::move(spec), options);

    auto token = request->key()[0].start()->value().as_decorated_key().token();

    auto cas_shard = service::cas_shard(*stmt.s, token);

    if (utils::get_local_injector().is_enabled("forced_bounce_to_shard_counter")) {
        return process_forced_rebounce(cas_shard.shard(), qp, options);
    }
    if (!cas_shard.this_shard()) {
        return make_ready_future<shared_ptr<cql_transport::messages::result_message>>(
                qp.bounce_to_shard(cas_shard.shard(), std::move(const_cast<cql3::query_options&>(options).take_cached_pk_function_calls()))
            );
    }

    std::optional<locator::tablet_routing_info> tablet_info;

    auto&& table = stmt.s->table();
    if (stmt._may_use_token_aware_routing && qs.get_client_state().is_protocol_extension_set(cql_transport::cql_protocol_extension::TABLETS_ROUTING_V1)) {
        tablet_info = table.tablet_routing_info_for(token, qs.get_client_state().get_original_shard());
    }

    return qp.proxy().cas(stmt.s, std::move(cas_shard), *request_ptr, request->read_command(qp), request->key(),
            {read_timeout, qs.get_permit(), qs.get_client_state(), qs.get_trace_state()},
            std::move(cl_for_paxos).assume_value(), cl_for_learn, statement_timeout, cas_timeout, true, {},
            stmt.attrs->is_bypass_large_data_guardrails()).then([&stmt, request = std::move(request), tablet_info = std::move(tablet_info)] (service::storage_proxy::cas_result cas_result) mutable {
        auto result = request->build_cas_result_set(stmt.cas_result_metadata(), stmt.columns_of_cas_result_set(), cas_result.is_applied);
        if (tablet_info) {
            result->add_tablet_info(std::move(*tablet_info));
        }
        // Surface any coordinator-side large data guardrail soft limit violations
        // detected during the LWT to the client as a CQL warning.
        if (auto warning = db::large_data_soft_violation_warning(cas_result.large_data_violations); !warning.empty()) [[unlikely]] {
            result->add_warning(std::move(warning));
        }
        return result;
    });
}

} // namespace

future<::shared_ptr<cql_transport::messages::result_message>>
modification_executor::commit(const modification_statement& stmt, query_processor& qp, service::query_state& qs,
        const query_options& options) const {
    if (!qp.db().try_find_table(stmt.s->id())) {
        co_return coroutine::exception(
                std::make_exception_ptr(exceptions::invalid_request_exception(
                        format("unconfigured table {}", stmt.column_family()))));
    }

    tracing::add_table_name(qs.get_trace_state(), stmt.keyspace(), stmt.column_family());

    stmt.inc_cql_stats(qs.get_client_state().is_internal());

    const auto cl = options.get_consistency();
    const query_processor::write_consistency_guardrail_state guardrail_state = qp.check_write_consistency_levels_guardrail(cl);
    if (guardrail_state == query_processor::write_consistency_guardrail_state::FAIL) {
        co_return coroutine::exception(
                std::make_exception_ptr(exceptions::invalid_request_exception(
                        format("Write consistency level {} is forbidden by the current configuration "
                               "setting of write_consistency_levels_disallowed. Please use a different "
                               "consistency level, or remove {} from write_consistency_levels_disallowed "
                               "set in the configuration.", cl, cl))));
    }

    stmt.validate_primary_key(options);

    if (stmt.has_conditions()) {
        auto result = co_await execute_with_condition(stmt, qp, qs, options);
        if (guardrail_state == query_processor::write_consistency_guardrail_state::WARN) {
            result->add_warning(format("Using write consistency level {} listed on the "
                                       "write_consistency_levels_warned is not recommended.", cl));
        }
        co_return result;
    }

    modification_spec spec(stmt, options);

    bool keys_size_one = spec.keys.size() == 1;
    auto token = dht::token();
    if (keys_size_one) {
        token = spec.keys[0].start()->value().token();
    }

    auto violations = db::large_data_violation_type::none;
    auto res = co_await execute_without_condition(stmt, qp, qs, options, std::move(spec), &violations);

    if (!res) {
        co_return seastar::make_shared<cql_transport::messages::result_message::exception>(std::move(res).assume_error());
    }

    auto result = seastar::make_shared<cql_transport::messages::result_message::void_message>();
    if (guardrail_state == query_processor::write_consistency_guardrail_state::WARN) {
        result->add_warning(format("Using write consistency level {} listed on the "
                                   "write_consistency_levels_warned is not recommended.", cl));
    }
    // Surface any coordinator-side large data guardrail soft limit violations
    // detected during the write to the client as a CQL warning.
    if (auto warning = db::large_data_soft_violation_warning(violations); !warning.empty()) [[unlikely]] {
        result->add_warning(std::move(warning));
    }

    auto&& table = stmt.s->table();

    if (keys_size_one && stmt._may_use_token_aware_routing) {
        if (qs.get_client_state().is_protocol_extension_set(cql_transport::cql_protocol_extension::TABLETS_ROUTING_V2_EXPERIMENTAL)) {
            // We only return routing information for EXECUTE requests.
            // They will carry a tablet version block; QUERY requests
            // will not.
            if (options.get_tablet_version_block().has_value()) {
                auto tablet_info_v2 = table.tablet_routing_info_v2_for(token, *options.get_tablet_version_block());
                if (tablet_info_v2) {
                    result->add_tablet_info_v2(std::move(*tablet_info_v2));
                }
            }
        } else if (qs.get_client_state().is_protocol_extension_set(cql_transport::cql_protocol_extension::TABLETS_ROUTING_V1)) {
            auto tablet_info = table.tablet_routing_info_for(token, qs.get_client_state().get_original_shard());
            if (tablet_info.has_value()) {
                result->add_tablet_info(std::move(*tablet_info));
            }
        }
    }

    co_return std::move(result);
}

const modification_executor& modification_executor::instance() {
    static const modification_executor the_instance;
    return the_instance;
}

}
