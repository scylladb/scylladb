/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "modification_statement.hh"

#include "db/consistency_level_type.hh"
#include "db/timeout_clock.hh"
#include "service/strong_consistency/groups_manager.hh"
#include "transport/messages/result_message.hh"
#include "cql3/query_processor.hh"
#include "service/strong_consistency/coordinator.hh"
#include "cql3/statements/strong_consistency/statement_helpers.hh"
#include "exceptions/exceptions.hh"
#include "utils/error_injection.hh"

namespace cql3::statements::strong_consistency {
static logging::logger logger("sc_modification_statement");

modification_statement::modification_statement(shared_ptr<base_statement> statement)
    : cql_statement_opt_metadata(&timeout_config::write_timeout)
    , _statement(std::move(statement))
{
}

using result_message = cql_transport::messages::result_message;

future<shared_ptr<result_message>> modification_statement::execute(query_processor& qp, service::query_state& qs, 
    const query_options& options, std::optional<service::group0_guard> guard) const
{
    return execute_without_checking_exception_message(qp, qs, options, std::move(guard))
            .then(cql_transport::messages::propagate_exception_as_future<shared_ptr<result_message>>);
}

static void validate_consistency_level(const db::consistency_level& cl) {
    if (cl != db::consistency_level::QUORUM && cl != db::consistency_level::LOCAL_QUORUM) {
        throw exceptions::invalid_request_exception("Strongly consistent writes must use QUORUM/LOCAL_QUORUM consistency level");
    }
}

mutation modification_statement::get_mutation(const query_options& options, api::timestamp_type ts,
        base_statement::json_cache_opt& json_cache, const std::vector<dht::partition_range>& keys) const {
    const auto prefetch_data = update_parameters::prefetch_data(_statement->s);
    const auto ttl = _statement->get_time_to_live(options);
    const auto params = update_parameters(_statement->s, options, ts, ttl, prefetch_data);
    const auto ranges = _statement->create_clustering_ranges(options, json_cache);
    auto muts = _statement->apply_updates(keys, ranges, params, json_cache);
    if (muts.size() != 1) {
        on_internal_error(logger, ::format("statement '{}' has unexpected number of mutations {}",
            raw_cql_statement.linearize(), muts.size()));
    }
    return std::move(*muts.begin());
}

future<shared_ptr<result_message>> modification_statement::execute_without_checking_exception_message(
        query_processor& qp, service::query_state& qs, const query_options& options,
        std::optional<service::group0_guard> guard) const
{
    validate_consistency_level(options.get_consistency());

    auto timeout = db::timeout_clock::now() + _statement->get_timeout(qs.get_client_state(), options);
    auto json_cache = base_statement::json_cache_opt{};
    const auto keys = _statement->build_partition_keys(options, json_cache);
    if (keys.size() != 1 || !query::is_single_partition(keys[0])) {
        throw exceptions::invalid_request_exception("Strongly consistent queries can only target a single partition");
    }

    auto [coordinator, holder] = qp.acquire_strongly_consistent_coordinator();
    const auto token = keys[0].start()->value().token();

    auto mutate_result = co_await coordinator.get().mutate(_statement->s,
        token,
        [&](api::timestamp_type ts) {
            return get_mutation(options, ts, json_cache, keys);
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
        const auto& table = _statement->s->table();

        auto maybe_routing_info_v2 = groups_manager.check_tablet_version(table, token, *options.get_tablet_version_block());
        if (maybe_routing_info_v2) {
            result->add_tablet_info_v2(std::move(*maybe_routing_info_v2));
        }
    }

    co_return std::move(result);
}

future<> modification_statement::check_access(query_processor& qp, const service::client_state& state) const {
    return _statement->check_access(qp, state);
}

void modification_statement::validate(query_processor& qp, const service::client_state& state) const {
    _statement->validate(qp, state);
}

uint32_t modification_statement::get_bound_terms() const {
    return _statement->get_bound_terms();
}

bool modification_statement::depends_on(std::string_view ks_name, std::optional<std::string_view> cf_name) const {
    return _statement->depends_on(ks_name, cf_name);
}
}
