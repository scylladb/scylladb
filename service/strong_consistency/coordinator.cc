/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "coordinator.hh"
#include "schema/schema.hh"
#include "replica/database.hh"
#include "locator/tablet_replication_strategy.hh"
#include "service/strong_consistency/state_machine.hh"
#include "service/strong_consistency/groups_manager.hh"
#include "idl/strong_consistency/state_machine.dist.hh"
#include "idl/strong_consistency/state_machine.dist.impl.hh"

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
            co_await std::move(wait_for_leader->future);
            continue;
        }
        const auto [ts, term] = get<raft_server::timestamp_with_term>(disposition);

        utils::chunked_vector<mutation> muts {mutation_gen(ts)};
        logger.debug("mutate(): add_entry({}), term {}", muts, term);
        const raft_command command {
            .mutations{muts.begin(), muts.end()}
        };
        raft::command raft_cmd;
        ser::serialize(raft_cmd, command);

        try {
            co_await op.raft_server.server().add_entry(std::move(raft_cmd),
                raft::wait_type::committed,
                nullptr);
            co_return std::monostate{};
        } catch (...) {
            auto ex = std::current_exception();
            if (try_catch<raft::request_aborted>(ex) || try_catch<raft::stopped_error>(ex)) {
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

    co_await op.raft_server.server().read_barrier(nullptr);

    auto [result, cache_temp] = co_await _db.query(schema, cmd,
        query::result_options::only_result(), ranges, trace_state, timeout);

    co_return std::move(result);
}

}
