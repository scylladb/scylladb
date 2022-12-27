/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#include "service/raft/group0_state_machine.hh"
#include "mutation/atomic_cell.hh"
#include "cql3/selection/selection.hh"
#include "dht/i_partitioner.hh"
#include "dht/token.hh"
#include "message/messaging_service.hh"
#include "mutation/canonical_mutation.hh"
#include "service/broadcast_tables/experimental/query_result.hh"
#include "schema_mutations.hh"
#include "frozen_schema.hh"
#include "serialization_visitors.hh"
#include "serializer.hh"
#include "serializer_impl.hh"
#include "idl/uuid.dist.hh"
#include "idl/uuid.dist.impl.hh"
#include "idl/frozen_schema.dist.hh"
#include "idl/frozen_schema.dist.impl.hh"
#include "idl/experimental/broadcast_tables_lang.dist.hh"
#include "idl/experimental/broadcast_tables_lang.dist.impl.hh"
#include "service/storage_service.hh"
#include "idl/storage_service.dist.hh"
#include "idl/group0_state_machine.dist.hh"
#include "idl/group0_state_machine.dist.impl.hh"
#include "service/migration_manager.hh"
#include "db/system_keyspace.hh"
#include "service/storage_proxy.hh"
#include "service/raft/raft_group0_client.hh"
#include "partition_slice_builder.hh"
#include "timestamp.hh"
#include "utils/overloaded_functor.hh"
#include <optional>

namespace service {

static logging::logger slogger("group0_raft_sm");

static mutation extract_history_mutation(std::vector<canonical_mutation>& muts, const data_dictionary::database db) {
    auto s = db.find_schema(db::system_keyspace::NAME, db::system_keyspace::GROUP0_HISTORY);
    auto it = std::find_if(muts.begin(), muts.end(), [history_table_id = s->id()]
            (canonical_mutation& m) { return m.column_family_id() == history_table_id; });
    if (it == muts.end()) {
        on_internal_error(slogger, "group0 history table mutation not found");
    }
    auto res = it->to_mutation(s);
    muts.erase(it);
    return res;
}

static mutation convert_history_mutation(canonical_mutation m, const data_dictionary::database db) {
    return m.to_mutation(db.find_schema(db::system_keyspace::NAME, db::system_keyspace::GROUP0_HISTORY));
}

future<> group0_state_machine::apply(std::vector<raft::command_cref> command) {
    slogger.trace("apply() is called with {} commands", command.size());
    for (auto&& c : command) {
        auto is = ser::as_input_stream(c);
        auto cmd = ser::deserialize(is, boost::type<group0_command>{});

        slogger.trace("cmd: prev_state_id: {}, new_state_id: {}, creator_addr: {}, creator_id: {}",
                cmd.prev_state_id, cmd.new_state_id, cmd.creator_addr, cmd.creator_id);
        slogger.trace("cmd.history_append: {}", cmd.history_append);

        auto read_apply_mutex_holder = co_await get_units(_client._read_apply_mutex, 1);

        if (cmd.prev_state_id) {
            auto last_group0_state_id = co_await db::system_keyspace::get_last_group0_state_id();
            if (*cmd.prev_state_id != last_group0_state_id) {
                // This command used obsolete state. Make it a no-op.
                // BTW. on restart, all commands after last snapshot descriptor become no-ops even when they originally weren't no-ops.
                // This is because we don't restart from snapshot descriptor, but using current state of the tables so the last state ID
                // is the one given by the last command.
                // Similar thing may happen when we pull group0 state in transfer_snapshot - we pull the latest state of remote tables,
                // not state at the snapshot descriptor.
                slogger.trace("cmd.prev_state_id ({}) different than last group 0 state ID in history table ({})",
                        cmd.prev_state_id, last_group0_state_id);
                continue;
            }
        } else {
            slogger.trace("unconditional modification, cmd.new_state_id: {}", cmd.new_state_id);
        }

        // We assume that `cmd.change` was constructed using group0 state which was observed *after* `cmd.prev_state_id` was obtained.
        // It is now important that we apply the change *before* we append the group0 state ID to the history table.
        //
        // If we crash before appending the state ID, when we reapply the command after restart, the change will be applied because
        // the state ID was not yet appended so the above check will pass.

        // TODO: reapplication of a command after a crash may require contacting a quorum (we need to learn that the command
        // is committed from a leader). But we may want to ensure that group 0 state is consistent after restart even without
        // access to quorum, which means we cannot allow partially applied commands. We need to ensure that either the entire
        // change is applied and the state ID is updated or none of this happens.
        // E.g. use a write-ahead-entry which contains all this information and make sure it's replayed during restarts.

        co_await std::visit(make_visitor(
        [&] (schema_change& chng) -> future<> {
            return _mm.merge_schema_from(netw::messaging_service::msg_addr(std::move(cmd.creator_addr)), std::move(chng.mutations));
        },
        [&] (broadcast_table_query& query) -> future<> {
            auto result = co_await service::broadcast_tables::execute_broadcast_table_query(_sp, query.query, cmd.new_state_id);
            _client.set_query_result(cmd.new_state_id, std::move(result));
        },
        [&] (topology_change& chng) -> future<> {
           return _ss.topology_transition(_sp, cmd.creator_addr, std::move(chng.mutations));
        }
        ), cmd.change);

        co_await _sp.mutate_locally({convert_history_mutation(std::move(cmd.history_append), _sp.data_dictionary())}, nullptr);
    }
}

future<raft::snapshot_id> group0_state_machine::take_snapshot() {
    return make_ready_future<raft::snapshot_id>(raft::snapshot_id::create_random_id());
}

void group0_state_machine::drop_snapshot(raft::snapshot_id id) {
    (void) id;
}

future<> group0_state_machine::load_snapshot(raft::snapshot_id id) {
    // topology_state_load applies persisted state machine state into
    // memory and thus needs to be protected with apply mutex
    auto read_apply_mutex_holder = co_await get_units(_client._read_apply_mutex, 1);
    co_await _ss.topology_state_load();
}

future<> group0_state_machine::transfer_snapshot(gms::inet_address from, raft::snapshot_descriptor snp) {
    // Note that this may bring newer state than the group0 state machine raft's
    // log, so some raft entries may be double applied, but since the state
    // machine is idempotent it is not a problem.

    slogger.trace("transfer snapshot from {} index {} snp id {}", from, snp.idx, snp.id);
    netw::messaging_service::msg_addr addr{from, 0};
    // (Ab)use MIGRATION_REQUEST to also transfer group0 history table mutation besides schema tables mutations.
    auto [_, cm] = co_await _mm._messaging.send_migration_request(addr, netw::schema_pull_options { .group0_snapshot_transfer = true });
    if (!cm) {
        // If we're running this code then remote supports Raft group 0, so it should also support canonical mutations
        // (which were introduced a long time ago).
        on_internal_error(slogger, "Expected MIGRATION_REQUEST to return canonical mutations");
    }

    auto topology_snp = co_await ser::storage_service_rpc_verbs::send_raft_pull_topology_snapshot(&_mm._messaging, addr, service::raft_topology_pull_params{});

    auto history_mut = extract_history_mutation(*cm, _sp.data_dictionary());

    // TODO ensure atomicity of snapshot application in presence of crashes (see TODO in `apply`)

    auto read_apply_mutex_holder = co_await get_units(_client._read_apply_mutex, 1);

    co_await _mm.merge_schema_from(addr, std::move(*cm));

    if (!topology_snp.mutations.empty()) {
        co_await _ss.merge_topology_snapshot(std::move(topology_snp));
    }

    co_await _sp.mutate_locally({std::move(history_mut)}, nullptr);
}

future<> group0_state_machine::abort() {
    return make_ready_future<>();
}

} // end of namespace service
