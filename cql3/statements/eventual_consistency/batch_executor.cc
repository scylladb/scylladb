/*
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.1 and Apache-2.0)
 */

#include "cql3/statements/eventual_consistency/batch_executor.hh"

#include "cql3/attributes.hh"
#include "cql3/query_processor.hh"
#include "cql3/query_options.hh"
#include "cql3/statements/batch_statement.hh"
#include "cql3/statements/cas_request.hh"
#include "cql3/statements/eventual_consistency/modification_executor.hh"
#include "cql3/statements/modification_statement.hh"
#include "db/large_data_handler.hh"
#include "service/storage_proxy.hh"
#include "utils/result.hh"
#include "utils/unique_view.hh"

#include <ranges>

template<typename T = void>
using coordinator_result = exceptions::coordinator_result<T>;

namespace cql3::statements::eventual_consistency {

namespace {

future<utils::chunked_vector<mutation>> get_mutations(const batch_statement& batch,
        query_processor& qp, const query_options& options, db::timeout_clock::time_point timeout,
        bool local, api::timestamp_type now, service::query_state& query_state) {
    const auto& statements = batch.get_statements();
    // Do not process in parallel because operations like list append/prepend depend on execution order.
    using mutation_set_type = std::unordered_set<mutation, mutation_hash_by_key, mutation_equals_by_key>;
    mutation_set_type result;
    result.reserve(statements.size());
    for (size_t i = 0; i != statements.size(); ++i) {
        auto&& statement = statements[i].statement;
        statement->inc_cql_stats(query_state.get_client_state().is_internal());
        auto&& statement_options = options.for_statement(i);
        auto timestamp = batch.get_timestamp(now, statement_options);
        modification_spec spec(*statement, statement_options);
        auto more = co_await eventual_consistency::get_mutations(*statement, qp, statement_options, timeout, local, timestamp, query_state, std::move(spec));

        for (auto&& m : more) {
            // We want unordered_set::try_emplace(), but we don't have it
            auto pos = result.find(m);
            if (pos == result.end()) {
                result.emplace(std::move(m));
            } else {
                const_cast<mutation&>(*pos).apply(std::move(m)); // Won't change key
            }
        }
    }

    // can't use range adaptors, because we want to move
    auto vresult = utils::chunked_vector<mutation>();
    vresult.reserve(result.size());
    for (auto&& m : result) {
        vresult.push_back(std::move(m));
    }
    co_return vresult;
}

future<coordinator_result<>> execute_without_conditions(const batch_statement& batch,
        query_processor& qp,
        utils::chunked_vector<mutation> mutations,
        db::consistency_level cl,
        db::timeout_clock::time_point timeout,
        tracing::trace_state_ptr tr_state,
        service_permit permit,
        db::large_data_violation_type* violations) {
    // FIXME: do we need to do this?
#if 0
    // Extract each collection of cfs from it's IMutation and then lazily concatenate all of them into a single Iterable.
    Iterable<ColumnFamily> cfs = Iterables.concat(Iterables.transform(mutations, new Function<IMutation, Collection<ColumnFamily>>()
    {
        public Collection<ColumnFamily> apply(IMutation im)
        {
            return im.getColumnFamilies();
        }
    }));
#endif
    batch.verify_batch_size(qp, mutations);

    bool mutate_atomic = true;
    if (batch.batch_type() != batch_statement::type::LOGGED) {
        batch.stats().batches_pure_unlogged += 1;
        mutate_atomic = false;
    } else {
        if (mutations.size() > 1) {
            batch.stats().batches_pure_logged += 1;
        } else {
            batch.stats().batches_unlogged_from_logged += 1;
            mutate_atomic = false;
        }
    }
    return qp.proxy().mutate_with_triggers(std::move(mutations), cl, timeout, mutate_atomic, std::move(tr_state), std::move(permit), db::allow_per_partition_rate_limit::yes, false, {
        .violations_out = violations,
    });
}

future<shared_ptr<cql_transport::messages::result_message>> execute_with_conditions(const batch_statement& batch,
        query_processor& qp,
        const query_options& options,
        service::query_state& qs) {

    auto cl_for_learn = options.get_consistency();
    utils::result_with_exception_ptr<db::consistency_level> cl_for_paxos = options.check_serial_consistency();
    if (!cl_for_paxos) [[unlikely]] {
        return make_exception_future<shared_ptr<cql_transport::messages::result_message>>(std::move(cl_for_paxos).assume_error());
    }
    std::unique_ptr<cas_request> request;
    schema_ptr schema;

    db::timeout_clock::time_point now = db::timeout_clock::now();
    const timeout_config& cfg = qs.get_client_state().get_timeout_config();
    auto batch_timeout = now + cfg.write_timeout; // Statement timeout.
    auto cas_timeout = now + cfg.cas_timeout;     // Ballot contention timeout.
    auto read_timeout = now + cfg.read_timeout;   // Query timeout.

    computed_function_values cached_fn_calls;

    const auto& statements = batch.get_statements();
    for (size_t i = 0; i < statements.size(); ++i) {

        modification_statement& statement = *statements[i].statement;
        const query_options& statement_options = options.for_statement(i);

        statement.inc_cql_stats(qs.get_client_state().is_internal());
        modification_spec spec(statement, statement_options);
        // At most one key
        if (spec.keys.empty()) {
            continue;
        }
        if (!request) {
            schema = statement.s;
            request = std::make_unique<cas_request>(schema);
        } else if (spec.keys.size() != 1 || spec.keys.front().equal(request->key().front(), dht::ring_position_comparator(*schema)) == false) {
            throw exceptions::invalid_request_exception("BATCH with conditions cannot span multiple partitions");
        }
        cached_fn_calls.merge(std::move(const_cast<cql3::query_options&>(statement_options).take_cached_pk_function_calls()));

        request->add_row_update(statement, std::move(spec), statement_options);
    }
    if (!request) {
        throw exceptions::invalid_request_exception(format("Unrestricted partition key in a conditional BATCH"));
    }

    auto cas_shard = service::cas_shard(*statements[0].statement->s, request->key()[0].start()->value().as_decorated_key().token());
    if (!cas_shard.this_shard()) {
        return make_ready_future<shared_ptr<cql_transport::messages::result_message>>(
                qp.bounce_to_shard(cas_shard.shard(), std::move(cached_fn_calls))
            );
    }

    auto* request_ptr = request.get();
    return qp.proxy().cas(schema, std::move(cas_shard), *request_ptr, request->read_command(qp), request->key(),
            {read_timeout, qs.get_permit(), qs.get_client_state(), qs.get_trace_state()},
            std::move(cl_for_paxos).assume_value(), cl_for_learn, batch_timeout, cas_timeout).then([&batch, request = std::move(request)] (service::storage_proxy::cas_result cas_result) {
        auto result = request->build_cas_result_set(batch.cas_result_metadata(), batch.columns_of_cas_result_set(), cas_result.is_applied);
        // Surface any coordinator-side large data guardrail soft limit violations
        // detected during the LWT to the client as a CQL warning.
        if (auto warning = db::large_data_soft_violation_warning(cas_result.large_data_violations); !warning.empty()) [[unlikely]] {
            result->add_warning(std::move(warning));
        }
        return result;
    });
}

} // namespace

void batch_executor::validate(const batch_statement& batch) const {
    const auto& attrs = batch.get_attrs();
    const auto& statements = batch.get_statements();
    const auto type = batch.batch_type();

    if (attrs.is_time_to_live_set()) {
        throw exceptions::invalid_request_exception("Global TTL on the BATCH statement is not supported.");
    }

    bool timestamp_set = attrs.is_timestamp_set();
    if (timestamp_set) {
        if (batch.has_conditions()) {
            throw exceptions::invalid_request_exception("Cannot provide custom timestamp for conditional BATCH");
        }
        if (type == batch_statement::type::COUNTER) {
            throw exceptions::invalid_request_exception("Cannot provide custom timestamp for counter BATCH");
        }
    }

    bool has_counters = std::ranges::any_of(statements, [] (auto&& s) { return s.statement->is_counter(); });
    bool has_non_counters = !std::ranges::all_of(statements, [] (auto&& s) { return s.statement->is_counter(); });
    if (timestamp_set && has_counters) {
        throw exceptions::invalid_request_exception("Cannot provide custom timestamp for a BATCH containing counters");
    }
    if (timestamp_set && std::ranges::any_of(statements, [] (auto&& s) { return s.statement->is_timestamp_set(); })) {
        throw exceptions::invalid_request_exception("Timestamp must be set either on BATCH or individual statements");
    }
    if (type == batch_statement::type::COUNTER && has_non_counters) {
        throw exceptions::invalid_request_exception("Cannot include non-counter statement in a counter batch");
    }
    if (type == batch_statement::type::LOGGED && has_counters) {
        throw exceptions::invalid_request_exception("Cannot include a counter statement in a logged batch");
    }
    if (has_counters && has_non_counters) {
        throw exceptions::invalid_request_exception("Counter and non-counter mutations cannot exist in the same batch");
    }

    if (batch.has_conditions()
            && !statements.empty()
            && (std::ranges::distance(statements
                            | std::views::transform([] (auto&& s) { return s.statement->keyspace(); })
                            | utils::views::unique) != 1
                || (std::ranges::distance(statements
                        | std::views::transform([] (auto&& s) { return s.statement->column_family(); })
                        | utils::views::unique) != 1))) {
        throw exceptions::invalid_request_exception("BATCH with conditions cannot span multiple tables");
    }
    std::optional<bool> raw_counter;
    for (auto& s : statements) {
        if (raw_counter && s.statement->is_raw_counter_shard_write() != *raw_counter) {
            throw exceptions::invalid_request_exception("Cannot mix raw and regular counter statements in batch");
        }
        raw_counter = s.statement->is_raw_counter_shard_write();
    }
}

future<::shared_ptr<cql_transport::messages::result_message>>
batch_executor::commit(const batch_statement& batch, query_processor& qp, service::query_state& query_state,
        const query_options& options, bool local, api::timestamp_type now) const {
    // FIXME: we don't support nulls here
#if 0
    if (options.get_consistency() == null)
        throw new InvalidRequestException("Invalid empty consistency level");
    if (options.getSerialConsistency() == null)
        throw new InvalidRequestException("Invalid empty serial consistency level");
#endif

    const auto cl = options.get_consistency();
    const query_processor::write_consistency_guardrail_state guardrail_state = qp.check_write_consistency_levels_guardrail(cl);
    if (guardrail_state == query_processor::write_consistency_guardrail_state::FAIL) {
        return make_exception_future<shared_ptr<cql_transport::messages::result_message>>(
                exceptions::invalid_request_exception(
                        format("Write consistency level {} is forbidden by the current configuration "
                               "setting of write_consistency_levels_disallowed. Please use a different "
                               "consistency level, or remove {} from write_consistency_levels_disallowed "
                               "set in the configuration.", cl, cl)));
    }

    const auto& statements = batch.get_statements();
    for (size_t i = 0; i < statements.size(); ++i) {
        statements[i].statement->validate_primary_key(options.for_statement(i));
    }

    if (batch.has_conditions()) {
        ++batch.stats().cas_batches;
        batch.stats().statements_in_cas_batches += statements.size();
        return execute_with_conditions(batch, qp, options, query_state).then([guardrail_state, cl] (auto result) {
            if (guardrail_state == query_processor::write_consistency_guardrail_state::WARN) {
                result->add_warning(format("Using write consistency level {} listed on the "
                                           "write_consistency_levels_warned is not recommended.", cl));
            }
            return result;
        });
    }

    ++batch.stats().batches;
    batch.stats().statements_in_batches += statements.size();

    auto timeout = db::timeout_clock::now() + batch.get_timeout(query_state.get_client_state(), options);
    auto violations = make_lw_shared<db::large_data_violation_type>(db::large_data_violation_type::none);

    return get_mutations(batch, qp, options, timeout, local, now, query_state).then([&batch, &qp, cl, timeout, tr_state = query_state.get_trace_state(),
                    permit = query_state.get_permit(), violations] (utils::chunked_vector<mutation> ms) mutable {
        return execute_without_conditions(batch, qp, std::move(ms), cl, timeout, std::move(tr_state), std::move(permit), violations.get());
    }).then([guardrail_state, cl, violations] (coordinator_result<> res) {
        if (!res) {
            return make_ready_future<shared_ptr<cql_transport::messages::result_message>>(
                    seastar::make_shared<cql_transport::messages::result_message::exception>(std::move(res).assume_error()));
        }
        auto result = make_shared<cql_transport::messages::result_message::void_message>();
        if (guardrail_state == query_processor::write_consistency_guardrail_state::WARN) {
            result->add_warning(format("Using write consistency level {} listed on the "
                                       "write_consistency_levels_warned is not recommended.", cl));
        }
        if (auto warning = db::large_data_soft_violation_warning(*violations); !warning.empty()) [[unlikely]] {
            result->add_warning(std::move(warning));
        }
        return make_ready_future<shared_ptr<cql_transport::messages::result_message>>(std::move(result));
    });
}

const batch_executor& batch_executor::instance() {
    static const batch_executor the_instance;
    return the_instance;
}

}
