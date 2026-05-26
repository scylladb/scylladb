/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "cql3/statements/modification_statement.hh"
#include "batch_statement.hh"

#include "db/timeout_clock.hh"
#include "transport/messages/result_message.hh"
#include "cql3/query_processor.hh"
#include "service/strong_consistency/coordinator.hh"
#include "cql3/statements/strong_consistency/statement_helpers.hh"
#include "exceptions/exceptions.hh"

namespace cql3::statements::strong_consistency {

static logging::logger logger("sc_batch_statement");

batch_statement::batch_statement(int bound_terms, type type_, std::vector<single_statement> statements, std::unique_ptr<attributes> attrs)
    : cql_statement_opt_metadata(&timeout_config::write_timeout)
    , _bound_terms(bound_terms)
    , _type(type_)
    , _statements(std::move(statements))
    , _attrs(std::move(attrs))
{
    validate();
}

batch_statement::batch_statement(type type_, std::vector<single_statement> statements, std::unique_ptr<attributes> attrs)
    : batch_statement(-1, type_, std::move(statements), std::move(attrs))
{
}

using result_message = cql_transport::messages::result_message;

future<shared_ptr<result_message>> batch_statement::execute(query_processor& qp, service::query_state& qs,
    const query_options& options, std::optional<service::group0_guard> guard) const
{
    return execute_without_checking_exception_message(qp, qs, options, std::move(guard))
            .then(cql_transport::messages::propagate_exception_as_future<shared_ptr<result_message>>);
}

future<shared_ptr<result_message>> batch_statement::execute_without_checking_exception_message(
        query_processor& qp, service::query_state& qs, const query_options& options,
        std::optional<service::group0_guard> guard) const
{
    if (_statements.empty()) {
        co_return seastar::make_shared<result_message::void_message>();
    }

    auto timeout = db::timeout_clock::now() + (_attrs->is_timeout_set() ? _attrs->get_timeout(options) : qs.get_client_state().get_timeout_config().write_timeout);

    // Build partition keys for all statements and validate they all target the same partition
    std::optional<dht::decorated_key> batch_key;
    schema_ptr batch_schema;

    struct statement_keys {
        cql3::statements::modification_statement::json_cache_opt json_cache;
        std::vector<dht::partition_range> keys;
    };
    std::vector<statement_keys> all_keys;
    all_keys.reserve(_statements.size());

    for (size_t i = 0; i < _statements.size(); ++i) {
        const auto& stmt = _statements[i].statement->inner_statement();
        const auto& statement_options = options.for_statement(i);
        auto json_cache = stmt.maybe_prepare_json_cache(statement_options);
        auto keys = stmt.build_partition_keys(statement_options, json_cache);

        if (keys.size() != 1 || !query::is_single_partition(keys[0])) {
            throw exceptions::invalid_request_exception("Each statement in a strongly consistent batch must target a single partition");
        }

        auto key = keys[0].start()->value().as_decorated_key();
        if (!batch_key) {
            batch_key = key;
            batch_schema = stmt.s;
        } else if (!batch_key->equal(*batch_schema, key)) {
            throw exceptions::invalid_request_exception("All statements in a strongly consistent batch must target the same partition");
        }

        all_keys.push_back(statement_keys{std::move(json_cache), std::move(keys)});
    }

    auto [coordinator, holder] = qp.acquire_strongly_consistent_coordinator();

    auto mutate_result = co_await coordinator.get().mutate(batch_schema,
        batch_key->token(),
        [&](api::timestamp_type ts) {
            std::optional<mutation> merged;
            for (size_t i = 0; i < _statements.size(); ++i) {
                const auto& statement_options = options.for_statement(i);
                auto m = _statements[i].statement->get_mutation(statement_options, ts, all_keys[i].json_cache, all_keys[i].keys);
                if (!merged) {
                    merged = std::move(m);
                } else {
                    merged->apply(std::move(m));
                }
            }
            if (!merged) {
                on_internal_error(logger, "batch produced no mutations");
            }
            return std::move(*merged);
        }, timeout, qs.get_client_state().get_abort_source());

    using namespace service::strong_consistency;
    if (auto* redirect = get_if<need_redirect>(&mutate_result)) {
        bool is_write = true;
        co_return co_await redirect_statement(qp, options, redirect->target, timeout, is_write, coordinator.get().get_stats(), std::move(redirect->on_forwarding_finished));
    }

    co_return seastar::make_shared<result_message::void_message>();
}

future<> batch_statement::check_access(query_processor& qp, const service::client_state& state) const {
    return parallel_for_each(_statements.begin(), _statements.end(), [&qp, &state] (auto&& s) {
        if (s.needs_authorization) {
            return s.statement->check_access(qp, state);
        } else {
            return make_ready_future<>();
        }
    });
}

uint32_t batch_statement::get_bound_terms() const {
    return _bound_terms;
}

bool batch_statement::depends_on(std::string_view ks_name, std::optional<std::string_view> cf_name) const {
    return std::ranges::any_of(_statements, [&ks_name, &cf_name] (auto&& s) { return s.statement->depends_on(ks_name, cf_name); });
}

void batch_statement::validate() const {
    if (_type == type::COUNTER) {
        throw exceptions::invalid_request_exception("Counter batches are not supported with strongly consistent tables");
    }

    schema_ptr batch_schema;
    for (const auto& s: _statements) {
        const auto& stmt = s.statement->inner_statement();
        if (!batch_schema) {
            batch_schema = stmt.s;
        } else if (batch_schema != stmt.s) {
            throw exceptions::invalid_request_exception("All statements in a strongly consistent batch must target the same table");
        }
    }
}

void batch_statement::validate(query_processor& qp, const service::client_state& state) const {
    for (const auto& s: _statements) {
        s.statement->validate(qp, state);
    }
}

}
