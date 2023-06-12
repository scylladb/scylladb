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
#include "seastar/core/on_internal_error.hh"
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
#include <boost/range/algorithm/transform.hpp>
#include <optional>
#include "db/config.hh"

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

    struct merger {
        std::vector<group0_command> cmd_to_merge;
        std::optional<mutation> merged_history_mutation;
        utils::UUID last_group0_state_id;
        group0_state_machine& sm;
        size_t size = 0;
        semaphore_units<> read_apply_mutex_holder;
        const size_t max_command_size;
        merger(group0_state_machine& sm_, utils::UUID id, semaphore_units<> mux) : last_group0_state_id(id)
            , sm(sm_)
            , read_apply_mutex_holder(std::move(mux))
            , max_command_size(sm._sp.data_dictionary().get_config().commitlog_segment_size_in_mb() * 1024 * 1024 / 2) {}

        size_t cmd_size(group0_command& cmd) {
            if (holds_alternative<broadcast_table_query>(cmd.change)) {
                return 0;
            }
            auto r = get_command_mutations(cmd) | boost::adaptors::transformed([] (const canonical_mutation& m) { return m.representation().size(); });
            return std::accumulate(std::begin(r), std::end(r), size_t(0));
        }
        bool can_merge(group0_command& cmd, size_t s) {
            if (!cmd_to_merge.empty()) {
                // broadcast table commands or different type of commands cannot be merged
                if (cmd_to_merge[0].change.index() != cmd.change.index() || holds_alternative<broadcast_table_query>(cmd.change)) {
                    return false;
                }
            }

            // Check that merged command will not be larger than half of commitlog segment.
            // Merged command can be, in fact, much smaller but better to be safe than sorry.
            // Skip the check for the first command.
            if (size && size + s > max_command_size) {
                return false;
            }

            return true;
        }
        void add(group0_command&& cmd, size_t added_size) {
            slogger.trace("add to merging set new_state_id: {}", cmd.new_state_id);
            auto m = convert_history_mutation(std::move(cmd.history_append), sm._sp.data_dictionary());
            last_group0_state_id = cmd.new_state_id;
            cmd_to_merge.push_back(std::move(cmd));
            size += added_size;
            if (merged_history_mutation) {
                merged_history_mutation->apply(std::move(m));
            } else {
                merged_history_mutation = std::move(m);
            }
        }

        std::vector<canonical_mutation>& get_command_mutations(group0_command& cmd) {
            return std::visit(make_visitor(
                [] (schema_change& chng) -> std::vector<canonical_mutation>& {
                    return chng.mutations;
                },
                [] (broadcast_table_query& query) -> std::vector<canonical_mutation>& {
                    on_internal_error(slogger, "trying to merge broadcast table command");
                },
                [] (topology_change& chng) -> std::vector<canonical_mutation>& {
                    return chng.mutations;
                }
            ), cmd.change);
        }

        std::pair<group0_command, mutation> merge() {
            auto& cmd = cmd_to_merge.back(); // use metadata from the last merged command
            slogger.trace("merge new_state_id: {}", cmd.new_state_id);
            using mutation_set_type = std::unordered_set<mutation, mutation_hash_by_key, mutation_equals_by_key>;
            std::unordered_map<table_id, mutation_set_type> mutations;

            if (cmd_to_merge.size() > 1) {
                // skip merging if there is only one command
                for (auto&& c : cmd_to_merge) {
                    for (auto&& cm : get_command_mutations(c)) {
                        auto schema = sm._sp.data_dictionary().find_schema(cm.column_family_id());
                        auto m = cm.to_mutation(schema);
                        auto& tbl_muts = mutations[cm.column_family_id()];
                        auto it = tbl_muts.find(m);
                        if (it == tbl_muts.end()) {
                            tbl_muts.emplace(std::move(m));
                        } else {
                            const_cast<mutation&>(*it).apply(std::move(m)); // Won't change key
                        }
                    }
                }

                std::vector<canonical_mutation> ms;
                for (auto&& tables : mutations) {
                    for (auto&& partitions : tables.second) {
                        ms.push_back(canonical_mutation(partitions));
                    }
                }

                get_command_mutations(cmd) = std::move(ms);
            }
            auto res = std::make_pair(std::move(cmd), std::move(merged_history_mutation).value());
            cmd_to_merge.clear();
            merged_history_mutation.reset();
            return res;
        }

        future<> apply(group0_command cmd, mutation history) {
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
                return sm._mm.merge_schema_from(netw::messaging_service::msg_addr(std::move(cmd.creator_addr)), std::move(chng.mutations));
            },
            [&] (broadcast_table_query& query) -> future<> {
                auto result = co_await service::broadcast_tables::execute_broadcast_table_query(sm._sp, query.query, cmd.new_state_id);
                sm._client.set_query_result(cmd.new_state_id, std::move(result));
            },
            [&] (topology_change& chng) -> future<> {
            return sm._ss.topology_transition(sm._sp, sm._cdc_gen_svc, cmd.creator_addr, std::move(chng.mutations));
            }
            ), cmd.change);

            co_await sm._sp.mutate_locally({std::move(history)}, nullptr);
        }

        future<> merge_and_apply() {
            auto [c, h] = merge();
            return apply(std::move(c), std::move(h));
        }

        bool empty() const {
            return cmd_to_merge.empty();
        }

        utils::UUID last_id() const {
            return last_group0_state_id;
        }
    };

    auto read_apply_mutex_holder = co_await _client.hold_read_apply_mutex();

    merger m(*this, co_await db::system_keyspace::get_last_group0_state_id(), std::move(read_apply_mutex_holder));

    for (auto&& c : command) {
        auto is = ser::as_input_stream(c);
        auto cmd = ser::deserialize(is, boost::type<group0_command>{});

        slogger.trace("cmd: prev_state_id: {}, new_state_id: {}, creator_addr: {}, creator_id: {}",
                cmd.prev_state_id, cmd.new_state_id, cmd.creator_addr, cmd.creator_id);
        slogger.trace("cmd.history_append: {}", cmd.history_append);

        if (cmd.prev_state_id) {
            if (*cmd.prev_state_id != m.last_id()) {
                // This command used obsolete state. Make it a no-op.
                // BTW. on restart, all commands after last snapshot descriptor become no-ops even when they originally weren't no-ops.
                // This is because we don't restart from snapshot descriptor, but using current state of the tables so the last state ID
                // is the one given by the last command.
                // Similar thing may happen when we pull group0 state in transfer_snapshot - we pull the latest state of remote tables,
                // not state at the snapshot descriptor.
                slogger.trace("cmd.prev_state_id ({}) different than last group 0 state ID in history table ({})",
                        cmd.prev_state_id, m.last_id());
                continue;
            }
        } else {
            slogger.trace("unconditional modification, cmd.new_state_id: {}", cmd.new_state_id);
        }

        auto size = m.cmd_size(cmd);
        if (!m.can_merge(cmd, size)) {
            co_await m.merge_and_apply();
        }

        m.add(std::move(cmd), size);
    }

    if (!m.empty()) {
        // apply remainder
        co_await m.merge_and_apply();
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
    auto read_apply_mutex_holder = co_await _client.hold_read_apply_mutex();
    co_await _ss.topology_state_load(_cdc_gen_svc);
    _ss._topology_state_machine.event.signal();
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

    auto read_apply_mutex_holder = co_await _client.hold_read_apply_mutex();

    co_await _mm.merge_schema_from(addr, std::move(*cm));

    if (!topology_snp.topology_mutations.empty()) {
        co_await _ss.merge_topology_snapshot(std::move(topology_snp));
    }

    co_await _sp.mutate_locally({std::move(history_mut)}, nullptr);
}

future<> group0_state_machine::abort() {
    return make_ready_future<>();
}

} // end of namespace service
