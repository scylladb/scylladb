/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "batch_statement.hh"

#include "db/timeout_clock.hh"
#include "transport/messages/result_message.hh"
#include "cql3/query_processor.hh"
#include "cql3/statements/modification_statement.hh"
#include "service/strong_consistency/coordinator.hh"
#include "cql3/statements/strong_consistency/statement_helpers.hh"
#include "exceptions/exceptions.hh"

namespace cql3::statements::strong_consistency {

static logging::logger logger("sc_batch_statement");

using result_message = cql_transport::messages::result_message;

batch_statement::batch_statement(int bound_terms, type type_, std::vector<single_statement> statements,
        std::unique_ptr<attributes> attrs, cql_stats& stats)
    : cql3::statements::batch_statement(bound_terms, type_, std::move(statements), std::move(attrs), stats)
{
    validate_strongly_consistent();
}

batch_statement::batch_statement(type type_, std::vector<single_statement> statements,
        std::unique_ptr<attributes> attrs, cql_stats& stats)
    : batch_statement(-1, type_, std::move(statements), std::move(attrs), stats)
{
}

void batch_statement::validate_strongly_consistent() const {
    if (_type == type::COUNTER) {
        throw exceptions::invalid_request_exception("Counter batches are not supported with strongly consistent tables");
    }

    schema_ptr batch_schema;
    for (const auto& s : get_statements()) {
        if (!batch_schema) {
            batch_schema = s.statement->s;
        } else if (batch_schema != s.statement->s) {
            throw exceptions::invalid_request_exception("All statements in a strongly consistent batch must target the same table");
        }
    }
}

future<shared_ptr<result_message>> batch_statement::do_execute(
        query_processor& qp, service::query_state& qs, const query_options& options,
        bool, api::timestamp_type) const
{
    // The local and timestamp arguments are unused: the batch is always
    // committed through the Raft group which owns the partition, and that
    // group assigns the timestamp.
    const auto& statements = get_statements();
    if (statements.empty()) {
        co_return seastar::make_shared<result_message::void_message>();
    }

    validate_write_consistency_level(options.get_consistency());

    auto timeout = db::timeout_clock::now() + get_timeout(qs.get_client_state(), options);

    // Build partition keys for all statements and validate they all target the same partition
    std::optional<dht::decorated_key> batch_key;
    schema_ptr batch_schema;

    struct statement_keys {
        cql3::statements::modification_statement::json_cache_opt json_cache;
        std::vector<dht::partition_range> keys;
    };
    std::vector<statement_keys> all_keys;
    all_keys.reserve(statements.size());

    for (size_t i = 0; i < statements.size(); ++i) {
        const auto& stmt = *statements[i].statement;
        const auto& statement_options = options.for_statement(i);
        auto json_cache = stmt.maybe_prepare_json_cache(statement_options);
        auto keys = stmt.build_partition_keys(statement_options, json_cache);

        if (keys.size() != 1 || !query::is_single_partition(keys[0])) {
            co_await coroutine::return_exception(exceptions::invalid_request_exception("Each statement in a strongly consistent batch must target a single partition"));
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

    // The mutations are built inside the callback because the coordinator
    // assigns the timestamp, and calls back again whenever it retries.
    auto mutate_result = co_await coordinator.get().mutate(batch_schema,
        batch_key->token(),
        [&](api::timestamp_type ts) {
            std::optional<mutation> merged;
            for (size_t i = 0; i < statements.size(); ++i) {
                const auto& stmt = *statements[i].statement;
                const auto& statement_options = options.for_statement(i);
                const auto prefetch_data = update_parameters::prefetch_data(stmt.s);
                const auto ttl = stmt.get_time_to_live(statement_options);
                const auto params = update_parameters(stmt.s, statement_options, ts, ttl, prefetch_data);
                const auto ranges = stmt.create_clustering_ranges(statement_options, all_keys[i].json_cache);
                auto muts = stmt.apply_updates(all_keys[i].keys, ranges, params, all_keys[i].json_cache);
                if (muts.size() != 1) {
                    on_internal_error(logger, ::format("statement {} on {}.{} has unexpected number of mutations {}",
                        i, stmt.keyspace(), stmt.column_family(), muts.size()));
                }
                auto& m = *muts.begin();
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

}
