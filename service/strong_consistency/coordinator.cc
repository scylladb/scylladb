/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "coordinator.hh"
#include "exceptions/exceptions.hh"
#include "schema/schema.hh"
#include "replica/database.hh"
#include "locator/tablet_replication_strategy.hh"
#include "service/strong_consistency/state_machine.hh"
#include "service/strong_consistency/groups_manager.hh"
#include "idl/strong_consistency/state_machine.dist.hh"
#include "idl/strong_consistency/state_machine.dist.impl.hh"

#include <seastar/coroutine/exception.hh>

namespace service::strong_consistency {


static logging::logger logger("sc_coordinator");

static const locator::tablet_replica* find_replica(const locator::tablet_info& tinfo, locator::host_id id) {
    const auto it = std::ranges::find_if(tinfo.replicas,
        [&] (const locator::tablet_replica& r) {
            return r.host == id;
        });
    return it == tinfo.replicas.end() ? nullptr : &*it;
}

struct coordinator::operation_ctx {
    locator::effective_replication_map_ptr erm;
    raft_server raft_server;
    locator::tablet_id tablet_id;
    const locator::tablet_raft_info& raft_info;
    const locator::tablet_info& tablet_info;
};

// Create a context object for an operation on the tablet corresponding to
// the passed schema and token.
//
// Preconditions:
// * The Raft group corresponding to the tablet must exist on groups_manager.
//
// Exceptions:
// * If this function throws an exception, it's critical and unexpected.
//   Under normal circumstances, it shouldn't throw any exceptions.
auto coordinator::create_operation_ctx(const schema& schema, const dht::token& token) 
    -> future<value_or_redirect<operation_ctx>>
{
    auto erm = schema.table().get_effective_replication_map();
    if (const auto* tablet_aware_rs = erm->get_replication_strategy().maybe_as_tablet_aware();
        !tablet_aware_rs || 
        tablet_aware_rs->get_consistency() != data_dictionary::consistency_config_option::local)
    {
        on_internal_error(logger,
            format("Unexpected replication strategy '{}' with consistency '{}' for table {}.{}",
                erm->get_replication_strategy().get_type(),
                tablet_aware_rs
                    ? consistency_config_option_to_string(tablet_aware_rs->get_consistency())
                    : "<undefined>",
                schema.ks_name(), schema.cf_name()));
    }
    const auto this_replica = locator::tablet_replica {
        .host = erm->get_token_metadata().get_my_id(),
        .shard = this_shard_id()
    };
    const auto& tablet_map = erm->get_token_metadata().tablets().get_tablet_map(schema.id());
    const auto tablet_id = tablet_map.get_tablet_id(token);
    const auto& tablet_info = tablet_map.get_tablet_info(tablet_id);

    if (!contains(tablet_info.replicas, this_replica)) {
        const auto* target = find_replica(tablet_info, this_replica.host);
        co_return need_redirect{target ? *target : tablet_info.replicas.at(0)};
    }
    const auto& raft_info = tablet_map.get_tablet_raft_info(tablet_id);
    auto raft_server = co_await _groups_manager.acquire_server(raft_info.group_id);

    co_return operation_ctx {
        .erm = std::move(erm),
        .raft_server = std::move(raft_server),
        .tablet_id = tablet_id,
        .raft_info = raft_info,
        .tablet_info = tablet_info
    };
}

coordinator::coordinator(groups_manager& groups_manager, replica::database& db)
    : _groups_manager(groups_manager)
    , _db(db)
{
}

future<value_or_redirect<>> coordinator::mutate(schema_ptr schema,
        const dht::token& token,
        mutation_gen&& mutation_gen)
{
    auto op_result = co_await create_operation_ctx(*schema, token);
    if (const auto* redirect = get_if<need_redirect>(&op_result)) {
        co_return *redirect;
    }
    auto& op = get<operation_ctx>(op_result);

    while (true) {
        auto disposition = op.raft_server.begin_mutate();
        if (const auto* not_a_leader = get_if<raft::not_a_leader>(&disposition)) {
            const auto leader_host_id = locator::host_id{not_a_leader->leader.uuid()};
            const auto* target = find_replica(op.tablet_info, leader_host_id);
            if (!target) {
                on_internal_error(logger,
                    ::format("table {}.{}, tablet {}, current leader {} is not a replica, replicas {}",
                        schema->ks_name(), schema->cf_name(), op.tablet_id, 
                        leader_host_id, op.tablet_info.replicas));
            }
            co_return need_redirect{*target};
        }
        if (auto* wait_for_leader = get_if<raft_server::need_wait_for_leader>(&disposition)) {
            try {
                co_await std::move(wait_for_leader->future);
            } catch (const raft::request_aborted& ex) {
                // This can only happen when the Raft group started being removed.
                //
                // Unfortunately, for the time being, we cannot tell if it's because
                // the tablet is migrated, or because e.g. the table has been dropped.
                // If we retry the operation, we might very well end up in a deadlock.
                // To avoid that, we throw an exception.
                //
                // FIXME: Design something better once we have more information.
                logger.debug("mutate(): wait_for_leader, operation aborted {}, table {}.{}, tablet {}",
                    ex, schema->ks_name(), schema->cf_name(), op.tablet_id);
                // FIXME: Use a better exception type and error message.
                throw exceptions::server_exception("Raft group is being removed. "
                    "Retry the operation");
            }
            continue;
        }
        const auto [ts, term] = get<raft_server::timestamp_with_term>(disposition);

        const raft_command command {
            .mutation{mutation_gen(ts)}
        };
        raft::command raft_cmd;
        ser::serialize(raft_cmd, command);

        logger.debug("mutate(): add_entry({}), term {}",
            command.mutation.pretty_printer(schema), term);

        auto& group_state = op.raft_server._state;
        co_await utils::get_local_injector().inject("sc_coordinator_wait_before_adding_entry",
                utils::wait_for_message(5min));

        try {
            co_await op.raft_server.server().add_entry(std::move(raft_cmd),
                raft::wait_type::committed,
                &group_state.raft_ops_as);
            co_return std::monostate{};
        } catch (...) {
            auto ex = std::current_exception();
            if (try_catch<raft::request_aborted>(ex)) {
                logger.debug("mutate(): add_entry, got raft::request_aborted {}, table {}.{}, tablet {}, term {}",
                    ex, schema->ks_name(), schema->cf_name(), op.tablet_id, term);
                // According to the description of raft_server::add_entry,
                // this can only happen if the passed abort_source has been
                // triggered:
                //
                // ```
                // raft::request_aborted
                //     Thrown if abort is requested before the operation finishes.
                // ```
                //
                // This means that the Raft group is being removed from this
                // replica's groups_manager.
                //
                // Unfortunately, for the time being, we cannot tell if it's because
                // the tablet is being migrated, or because e.g. the table has been
                // dropped. If we retry the operation, we might very well end up in
                // a deadlock. To avoid that, we throw an exception.
                //
                // FIXME: Design something better once we have more information.
                // FIXME: Use a better exception type and error message.
                throw exceptions::server_exception("Raft group is being removed. "
                    "Retry the operation");
            } else if (try_catch<raft::stopped_error>(ex)) {
                // Holding raft_server.holder guarantees that the raft::server is not
                // aborted until the holder is released.

                on_internal_error(logger,
                    format("mutate(): add_entry, unexpected exception {}, table {}.{}, tablet {}, term {}", 
                        ex, schema->ks_name(), schema->cf_name(), op.tablet_id, term));
            } else if (try_catch<raft::not_a_leader>(ex) || try_catch<raft::dropped_entry>(ex)) {
                logger.debug("mutate(): add_entry, got retriable error {}, table {}.{}, tablet {}, term {}",
                    ex, schema->ks_name(), schema->cf_name(), op.tablet_id, term);

                continue;
            } else if (try_catch<raft::commit_status_unknown>(ex)) {
                logger.debug("mutate(): add_entry, got commit_status_unknown {}, table {}.{}, tablet {}, term {}",
                    ex, schema->ks_name(), schema->cf_name(), op.tablet_id, term);

                // FIXME: use a dedicated ERROR_CODE instead of SERVER_ERROR
                throw exceptions::server_exception(
                    "The outcome of this statement is unknown. It may or may not have been applied. "
                    "Retrying the statement may be necessary.");
            }

            // We know nothing about other errors, let the cql server convert them to SERVER_ERROR.
            throw;
        }
    }
}

auto coordinator::query(schema_ptr schema,
        const query::read_command& cmd,
        const dht::partition_range_vector& ranges,
        tracing::trace_state_ptr trace_state,
        db::timeout_clock::time_point timeout
    ) -> future<query_result_type>
{
    auto op_result = co_await create_operation_ctx(*schema, ranges[0].start()->value().token());
    if (const auto* redirect = get_if<need_redirect>(&op_result)) {
        co_return *redirect;
    }
    auto& op = get<operation_ctx>(op_result);
    auto& group_state = op.raft_server._state;

    auto aoe = abort_on_expiry(timeout);
    // If the abort_source passed to the read_barrier below gets triggered,
    // the exception thrown will always be raft::request_aborted with its
    // custom (and informative) message. There's no point in setting any
    // other exception or error message here. We cannot differentiate a timeout
    // from the Raft group being removed anyway (at least at this stage).
    auto sub = group_state.raft_ops_as.subscribe([&] noexcept { aoe.abort_source().request_abort(); });

    co_await utils::get_local_injector().inject("sc_coordinator_wait_before_query_read_barrier",
        utils::wait_for_message(5min));

    try {
        co_await op.raft_server.server().read_barrier(&aoe.abort_source());
    } catch (const raft::request_aborted& ex) {
        // According to the description of raft_server::add_entry,
        // this can only happen if the passed abort_source has been
        // triggered:
        //
        // ```
        // raft::request_aborted
        //     Thrown if abort is requested before the operation finishes.
        // ```
        //
        // Unfortunately, this also means that both timing out and
        // the Raft group being removed will have the same result.
        // That's why we need this if-else statement here.
        if (!group_state.raft_ops_as.abort_requested()) {
            // If the main abort_source hasn't been triggered yet,
            // that means the request hit a timeout.
            //
            // FIXME: Use a better exception type. The existing exceptions::read_timeout_exception
            // doesn't fit strong consistency that well.
            co_return coroutine::return_exception(exceptions::server_exception(
                ::format("Operation timed out for {}.{}", schema->ks_name(), schema->cf_name())
            ));
        } else {
            // If the abort_source has been triggered, that means that the Raft
            // group is being removed from this replica's groups_manager.
            //
            // Unfortunately, for the time being, we cannot tell if it's because
            // the tablet is being migrated or because e.g. the table has been
            // dropped. If we retry the operation, we might very well end up in
            // a deadlock. To avoid that, we throw an exception.
            //
            // FIXME: Design something better once we have more information.
            logger.debug("query(): read_barrier [table {}.{}, tablet {}] aborted. Command: {}. Reason: {}",
                schema->ks_name(), schema->cf_name(), op.tablet_id, cmd, ex);
            // FIXME: Use a better exception type and error message.
            co_return coroutine::return_exception(exceptions::server_exception("Raft group is being removed. "
                "Retry the operation"));
        }
    } catch (...) {
        logger.error("query() read barrier [table {}.{}, tablet {}], unexpected exception. Command: {}, Exception: {}",
            schema->ks_name(), schema->cf_name(), op.tablet_id, cmd, std::current_exception());
        throw;
    }

    auto [result, cache_temp] = co_await _db.query(schema, cmd,
        query::result_options::only_result(), ranges, trace_state, timeout);

    co_return std::move(result);
}

}
