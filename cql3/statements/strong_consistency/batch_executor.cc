/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "cql3/statements/strong_consistency/batch_executor.hh"

#include "cql3/attributes.hh"
#include "cql3/query_processor.hh"
#include "cql3/statements/batch_statement.hh"
#include "cql3/statements/modification_statement.hh"
#include "cql3/statements/strong_consistency/modification_executor.hh"
#include "cql3/statements/strong_consistency/statement_helpers.hh"
#include "db/timeout_clock.hh"
#include "exceptions/exceptions.hh"
#include "service/strong_consistency/coordinator.hh"
#include "transport/messages/result_message.hh"

namespace cql3::statements::strong_consistency {

static logging::logger logger("sc_batch_statement");

using result_message = cql_transport::messages::result_message;

void batch_executor::validate(const batch_statement& batch) const {
    const auto& attrs = batch.get_attrs();

    if (batch.batch_type() == batch_statement::type::COUNTER) {
        throw exceptions::invalid_request_exception("Counter batches are not supported with strongly consistent tables");
    }

    if (attrs.is_time_to_live_set()) {
        throw exceptions::invalid_request_exception("Global TTL on the BATCH statement is not supported.");
    }
    if (attrs.is_timestamp_set()) {
        throw exceptions::invalid_request_exception("Strongly consistent queries don't support user-provided timestamps");
    }

    schema_ptr batch_schema;
    for (const auto& s : batch.get_statements()) {
        const auto& stmt = *s.statement;
        if (!batch_schema) {
            batch_schema = stmt.s;
        } else if (batch_schema != stmt.s) {
            throw exceptions::invalid_request_exception("All statements in a strongly consistent batch must target the same table");
        }
    }
}

future<::shared_ptr<result_message>>
batch_executor::commit(const batch_statement& batch, query_processor& qp, service::query_state& qs,
        const query_options& options, bool local, api::timestamp_type now) const {
    const auto& statements = batch.get_statements();
    if (statements.empty()) {
        co_return seastar::make_shared<result_message::void_message>();
    }

    validate_write_consistency_level(options.get_consistency());

    auto timeout = db::timeout_clock::now() + batch.get_timeout(qs.get_client_state(), options);

    // Build partition keys for all statements and validate they all target the same partition
    std::optional<dht::decorated_key> batch_key;
    schema_ptr batch_schema;

    std::vector<modification_spec> specs;
    specs.reserve(statements.size());

    for (size_t i = 0; i < statements.size(); ++i) {
        const auto& stmt = *statements[i].statement;
        const auto& statement_options = options.for_statement(i);
        stmt.validate_primary_key(statement_options);
        modification_spec spec(stmt, statement_options);

        if (spec.keys.size() != 1 || !query::is_single_partition(spec.keys[0])) {
            co_await coroutine::return_exception(exceptions::invalid_request_exception("Each statement in a strongly consistent batch must target a single partition"));
        }

        auto key = spec.keys[0].start()->value().as_decorated_key();
        if (!batch_key) {
            batch_key = key;
            batch_schema = stmt.s;
        } else if (!batch_key->equal(*batch_schema, key)) {
            throw exceptions::invalid_request_exception("All statements in a strongly consistent batch must target the same partition");
        }

        specs.push_back(std::move(spec));
    }

    auto [coordinator, holder] = qp.acquire_strongly_consistent_coordinator();

    auto mutate_result = co_await coordinator.get().mutate(batch_schema,
        batch_key->token(),
        [&](api::timestamp_type ts) {
            std::optional<mutation> merged;
            for (size_t i = 0; i < statements.size(); ++i) {
                const auto& statement_options = options.for_statement(i);
                auto m = get_mutation(*statements[i].statement, statement_options, ts, specs[i]);
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
        }, timeout, qs.get_client_state().get_abort_source(),
        // Only EXECUTE requests carry a tablet version block; a BATCH doesn't, so there
        // is no routing information to hand back.
        std::nullopt);

    using namespace service::strong_consistency;
    if (auto* redirect = get_if<need_redirect>(&mutate_result)) {
        bool is_write = true;
        co_return co_await redirect_statement(qp, options, redirect->target, timeout, is_write, coordinator.get().get_stats(), std::move(redirect->on_forwarding_finished));
    }

    co_return seastar::make_shared<result_message::void_message>();
}

const batch_executor& batch_executor::instance() {
    static const batch_executor the_instance;
    return the_instance;
}

}
