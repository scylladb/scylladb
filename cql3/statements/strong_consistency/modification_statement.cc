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
#include "cql3/statements/strong_consistency/statement_helpers.hh"
#include "exceptions/exceptions.hh"
#include "utils/error_injection.hh"
#include "cql3/statements/strong_consistency/modification_executor.hh"

namespace cql3::statements::strong_consistency {
static logging::logger logger("sc_modification_statement");

modification_statement::modification_statement(shared_ptr<base_statement> statement)
    : cql_statement(&timeout_config::write_timeout)
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

mutation get_mutation(const cql3::statements::modification_statement& stmt, const query_options& options,
        api::timestamp_type ts, const modification_spec& spec) {
    const auto prefetch_data = update_parameters::prefetch_data(stmt.s);
    const auto ttl = stmt.get_time_to_live(options);
    const auto params = update_parameters(stmt.s, options, ts, ttl, prefetch_data);
    auto muts = stmt.apply_updates(spec, params);
    if (muts.size() != 1) {
        on_internal_error(logger, ::format("statement '{}' has unexpected number of mutations {}",
            stmt.raw_cql_statement.linearize(), muts.size()));
    }
    return std::move(*muts.begin());
}

future<::shared_ptr<result_message>>
modification_executor::commit(const cql3::statements::modification_statement& stmt, query_processor& qp, service::query_state& qs,
        const query_options& options) const {
    validate_write_consistency_level(options.get_consistency());
    stmt.validate_primary_key(options);

    auto timeout = db::timeout_clock::now() + stmt.get_timeout(qs.get_client_state(), options);
    const modification_spec spec(stmt, options);
    if (spec.keys.size() != 1 || !query::is_single_partition(spec.keys[0])) {
        throw exceptions::invalid_request_exception("Strongly consistent queries can only target a single partition");
    }

    auto [coordinator, holder] = qp.acquire_strongly_consistent_coordinator();
    const auto token = spec.keys[0].start()->value().token();

    auto mutate_result = co_await coordinator.get().mutate(stmt.s,
        token,
        [&](api::timestamp_type ts) {
            return get_mutation(stmt, options, ts, spec);
        }, timeout, qs.get_client_state().get_abort_source(), tablet_version_block_for(qs, options));

    using namespace service::strong_consistency;
    if (auto* redirect = get_if<need_redirect>(&mutate_result)) {
        bool is_write = true;
        co_return co_await redirect_statement(qp, options, redirect->target, timeout, is_write, coordinator.get().get_stats(), std::move(redirect->on_forwarding_finished));
    }
    utils::get_local_injector().inject("sc_modification_statement_timeout", [&] {
        throw exceptions::mutation_write_timeout_exception{"", "", options.get_consistency(), 0, 0, db::write_type::SIMPLE};
    });

    auto result = seastar::make_shared<result_message::void_message>();
    if (auto& routing_info = get<coordinator::mutate_result>(mutate_result).routing_info) {
        result->add_tablet_info_v2(std::move(*routing_info));
    }
    co_return std::move(result);
}

const modification_executor& modification_executor::instance() {
    static const modification_executor the_instance;
    return the_instance;
}

future<shared_ptr<result_message>> modification_statement::execute_without_checking_exception_message(
        query_processor& qp, service::query_state& qs, const query_options& options,
        std::optional<service::group0_guard> guard) const
{
    return modification_executor::instance().commit(*_statement, qp, qs, options);
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
