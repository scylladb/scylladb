/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "modification_statement.hh"

#include "transport/messages/result_message.hh"
#include "cql3/query_processor.hh"
#include "service/strong_consistency/coordinator.hh"
#include "cql3/statements/strong_consistency/statement_helpers.hh"

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

future<shared_ptr<result_message>> modification_statement::execute_without_checking_exception_message(
        query_processor& qp, service::query_state& qs, const query_options& options,
        std::optional<service::group0_guard> guard) const
{
    auto json_cache = base_statement::json_cache_opt{};
    const auto keys = _statement->build_partition_keys(options, json_cache);
    if (keys.size() != 1 || !query::is_single_partition(keys[0])) {
        throw exceptions::invalid_request_exception("Strongly consistent queries can only target a single partition");
    }
    if (_statement->requires_read()) {
        throw exceptions::invalid_request_exception("Strongly consistent updates don't support data prefetch");
    }

    auto [coordinator, holder] = qp.acquire_strongly_consistent_coordinator();
    const auto mutate_result = co_await coordinator.get().mutate(_statement->s,
        keys[0].start()->value().token(),
        [&](api::timestamp_type ts) {
            const auto prefetch_data = update_parameters::prefetch_data(_statement->s);
            const auto ttl = _statement->get_time_to_live(options);
            const auto params = update_parameters(_statement->s, options, ts, ttl, prefetch_data);
            const auto ranges = _statement->create_clustering_ranges(options, json_cache);
            auto muts = _statement->apply_updates(keys, ranges, params, json_cache);
            if (muts.size() != 1) {
                on_internal_error(logger, ::format("statement '{}' has unexpected number of mutations {}",
                    raw_cql_statement, muts.size()));
            }
            return std::move(*muts.begin());
        });

    using namespace service::strong_consistency;
    if (const auto* redirect = get_if<need_redirect>(&mutate_result)) {
        co_return co_await redirect_statement(qp, options, redirect->target);
    }

    co_return seastar::make_shared<result_message::void_message>();
}

future<> modification_statement::check_access(query_processor& qp, const service::client_state& state) const {
    return _statement->check_access(qp, state);
}

uint32_t modification_statement::get_bound_terms() const {
    return _statement->get_bound_terms();
}

bool modification_statement::depends_on(std::string_view ks_name, std::optional<std::string_view> cf_name) const {
    return _statement->depends_on(ks_name, cf_name);
}
}
