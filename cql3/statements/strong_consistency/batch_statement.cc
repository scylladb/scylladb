/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "cql3/statements/modification_statement.hh"
#include "cql3/update_parameters.hh"
#include "batch_statement.hh"

#include "db/timeout_clock.hh"
#include "transport/messages/result_message.hh"
#include "cql3/query_processor.hh"
#include "service/strong_consistency/coordinator.hh"
#include "cql3/statements/strong_consistency/statement_helpers.hh"
#include "exceptions/exceptions.hh"

namespace cql3::statements::strong_consistency {

static logging::logger logger("sc_batch_statement");

batch_statement::batch_statement(shared_ptr<inner_statement> batch)
    : cql_statement_opt_metadata(&timeout_config::write_timeout)
    , _batch(std::move(batch))
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
    const auto statements = _batch->get_batch_statements().value();
    if (statements.empty()) {
        co_return seastar::make_shared<result_message::void_message>();
    }

    auto timeout = db::timeout_clock::now() + statements[0]->get_timeout(qs.get_client_state(), options);

    // Build partition keys for all statements and validate they all target the same partition
    std::optional<dht::token> batch_token;
    schema_ptr batch_schema;

    struct statement_keys {
        modification_statement::json_cache_opt json_cache;
        std::vector<dht::partition_range> keys;
    };
    std::vector<statement_keys> all_keys;
    all_keys.reserve(statements.size());

    for (size_t i = 0; i < statements.size(); ++i) {
        const auto& stmt = statements[i];
        const auto& statement_options = options.for_statement(i);
        auto json_cache = stmt->maybe_prepare_json_cache(statement_options);
        auto keys = stmt->build_partition_keys(statement_options, json_cache);

        if (keys.size() != 1 || !query::is_single_partition(keys[0])) {
            throw exceptions::invalid_request_exception("Each statement in a strongly consistent batch must target a single partition");
        }

        auto token = keys[0].start()->value().token();
        if (!batch_token) {
            batch_token = token;
            batch_schema = stmt->s;
        } else if (*batch_token != token) {
            throw exceptions::invalid_request_exception("All statements in a strongly consistent batch must target the same partition");
        }

        all_keys.push_back(statement_keys{std::move(json_cache), std::move(keys)});
    }

    auto [coordinator, holder] = qp.acquire_strongly_consistent_coordinator();

    auto mutate_result = co_await coordinator.get().mutate(batch_schema,
        *batch_token,
        [&](api::timestamp_type ts) {
            std::optional<mutation> merged;
            for (size_t i = 0; i < statements.size(); ++i) {
                const auto& stmt = statements[i];
                const auto& statement_options = options.for_statement(i);
                const auto prefetch_data = update_parameters::prefetch_data(stmt->s);
                const auto ttl = stmt->get_time_to_live(statement_options);
                const auto params = update_parameters(stmt->s, statement_options, ts, ttl, prefetch_data);
                const auto ranges = stmt->create_clustering_ranges(statement_options, all_keys[i].json_cache);
                auto muts = stmt->apply_updates(all_keys[i].keys, ranges, params, all_keys[i].json_cache);

                for (auto&& m : muts) {
                    if (!merged) {
                        merged = std::move(m);
                    } else {
                        merged->apply(std::move(m));
                    }
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
        co_return co_await redirect_statement(qp, options, redirect->target, timeout, is_write, coordinator.get().get_stats(), std::move(redirect->on_node_resolved));
    }

    co_return seastar::make_shared<result_message::void_message>();
}

future<> batch_statement::check_access(query_processor& qp, const service::client_state& state) const {
    return _batch->check_access(qp, state);
}

uint32_t batch_statement::get_bound_terms() const {
    return _batch->get_bound_terms();
}

bool batch_statement::depends_on(std::string_view ks_name, std::optional<std::string_view> cf_name) const {
    return _batch->depends_on(ks_name, cf_name);
}

void batch_statement::validate(query_processor& qp, const service::client_state& state) const {
    if (_batch->has_conditions()) {
        throw exceptions::invalid_request_exception("Strongly consistent batches don't support conditional updates");
    }

    for (const auto& stmt: _batch->get_batch_statements().value()) {
        if (stmt->requires_read()) {
            throw exceptions::invalid_request_exception("Strongly consistent batches don't support data prefetch");
        }
        if (stmt->is_timestamp_set()) {
            throw exceptions::invalid_request_exception("Strongly consistent batches don't support user-provided timestamps");
        }
    }

    _batch->validate(qp, state);
}

std::optional<std::vector<shared_ptr<cql3::statements::modification_statement>>> batch_statement::get_batch_statements() const {
    return _batch->get_batch_statements();
}

}
