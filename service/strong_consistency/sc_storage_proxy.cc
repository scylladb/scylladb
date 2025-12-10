/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "sc_storage_proxy.hh"
#include "schema/schema.hh"
#include "replica/database.hh"
#include "locator/tablet_replication_strategy.hh"
#include "service/strong_consistency/sc_state_machine.hh"
#include "service/strong_consistency/sc_groups_manager.hh"
#include "idl/sc_state_machine.dist.hh"
#include "idl/sc_state_machine.dist.impl.hh"

namespace service {

static logging::logger logger("sc_storage_proxy");

static const locator::tablet_replica* find_replica(const locator::tablet_info& tinfo, locator::host_id id) {
    const auto it = std::ranges::find_if(tinfo.replicas,
        [&] (const locator::tablet_replica& r) {
            return r.host == id;
        });
    return it == tinfo.replicas.end() ? nullptr : &*it;
}

struct sc_operation_ctx {
    locator::effective_replication_map_ptr erm;
    sc_raft_server raft_server;
    locator::tablet_id tablet_id;
    const locator::tablet_raft_info& raft_info;
    const locator::tablet_info& tablet_info;
};

static future<sc_operation_result<sc_operation_ctx>> create_sc_operation_ctx(const schema& schema,
        const dht::token& token,
        sc_groups_manager& sc_groups)
{
    auto erm = schema.table().get_effective_replication_map();
    if (const auto* tablet_aware_rs = erm->get_replication_strategy().maybe_as_tablet_aware();
        !tablet_aware_rs || 
        tablet_aware_rs->get_consistency() != data_dictionary::consistency_config_option::local)
    {
        on_internal_error(logger, "Unexpected replication strategy");
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
        co_return sc_operation_result<sc_operation_ctx>::redirect(target ? *target : tablet_info.replicas.at(0));
    }
    const auto& raft_info = tablet_map.get_tablet_raft_info(tablet_id);
    auto raft_server = co_await sc_groups.acquire_server(raft_info.group_id);

    co_return sc_operation_result<sc_operation_ctx>::result({
        .erm = std::move(erm),
        .raft_server = std::move(raft_server),
        .tablet_id = tablet_id,
        .raft_info = raft_info,
        .tablet_info = tablet_info
    });
}

sc_storage_proxy::sc_storage_proxy(sc_groups_manager& sc_groups, replica::database& db)
    : _sc_groups(sc_groups)
    , _db(db)
{
}

future<sc_operation_result<>> sc_storage_proxy::mutate(schema_ptr schema, 
        const dht::token& token,
        mutatation_gen&& mutatation_gen)
{
    auto sc_op_result = co_await create_sc_operation_ctx(*schema, token, _sc_groups);
    if (const auto* redirect = sc_op_result.get_if_redirect()) {
        co_return sc_operation_result<>::redirect(*redirect);
    }
    auto sc_op = std::move(sc_op_result).extract_result();

    while (true) {
        const auto [term, ts] = co_await sc_op.raft_server.begin_mutate();
        const auto current_term = sc_op.raft_server.server().get_current_term();
        if (term != current_term) {
            continue;
        }
        if (!ts) {
            const auto leader_id = sc_op.raft_server.server().current_leader();
            if (!leader_id) {
                on_internal_error(logger,
                    ::format("table {}.{}, tablet {}, leader is not set",
                        schema->ks_name(), schema->cf_name(), sc_op.tablet_id));
            }
            const auto leader_host_id = locator::host_id{leader_id.uuid()};
            const auto* target = find_replica(sc_op.tablet_info, leader_host_id);
            if (!target) {
                on_internal_error(logger,
                    ::format("table {}.{}, tablet {}, current leader {} is not a replica, replicas {}",
                        schema->ks_name(), schema->cf_name(), sc_op.tablet_id, 
                        leader_host_id, sc_op.tablet_info.replicas));
            }
            co_return sc_operation_result<>::redirect(*target);
        }

        const sc_raft_command command {
            .mutation{mutatation_gen(*ts)}
        };
        raft::command raft_cmd;
        ser::serialize(raft_cmd, command);

        logger.debug("mutate(): add_entry({}), term {}",
            command.mutation.pretty_printer(schema), current_term);
        try {
            co_await sc_op.raft_server.server().add_entry(std::move(raft_cmd),
                raft::wait_type::committed,
                nullptr);
            co_return sc_operation_result<>::result();
        } catch (...) {
            auto ex = std::current_exception();
            if (try_catch<raft::request_aborted>(ex) || try_catch<raft::stopped_error>(ex)) {
                // Holding raft_server.holder guarantees that the raft::server is not
                // aborted until the holder is released.

                on_internal_error(logger,
                    format("mutate(): add_entry, unexpected exception {}, table {}.{}, tablet {}, term {}", 
                        ex, schema->ks_name(), schema->cf_name(), sc_op.tablet_id, current_term));
            } else if (try_catch<raft::not_a_leader>(ex) || try_catch<raft::dropped_entry>(ex)) {
                logger.debug("mutate(): add_entry, got retriable error {}, table {}.{}, tablet {}, term {}",
                    ex, schema->ks_name(), schema->cf_name(), sc_op.tablet_id, current_term);

                continue;
            } else if (try_catch<raft::commit_status_unknown>(ex)) {
                logger.debug("mutate(): add_entry, got commit_status_unknown {}, table {}.{}, tablet {}, term {}",
                    ex, schema->ks_name(), schema->cf_name(), sc_op.tablet_id, current_term);

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

auto sc_storage_proxy::query(schema_ptr schema,
        const query::read_command& cmd,
        const dht::partition_range_vector& ranges,
        tracing::trace_state_ptr trace_state,
        db::timeout_clock::time_point timeout
    ) -> future<query_result_type>
{
    throw std::runtime_error("not implemented");
}
}
