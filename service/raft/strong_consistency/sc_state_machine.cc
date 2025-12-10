/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "sc_state_machine.hh"
#include "serializer_impl.hh"
#include "idl/sc_state_machine.dist.hh"
#include "idl/sc_state_machine.dist.impl.hh"
#include "db/system_keyspace.hh"
#include "replica/database.hh"
#include "mutation/async_utils.hh"
#include "db/commitlog/commitlog.hh"

namespace service {
    static logging::logger logger("sc_sm");

    class sc_state_machine: public raft_state_machine {
        static inline const gc_clock::duration history_gc_duration = gc_clock::duration {
            std::chrono::duration_cast<gc_clock::duration>(std::chrono::weeks{1})
        };

        table_id _table_id;
        locator::tablet_id _tablet_id;
        raft::group_id _group_id;
        sstring _group_key;
        replica::database& _db;
        db::system_keyspace& _sys_ks;

        future<> apply(mutation mut) {
            const auto frozen_mut = co_await freeze_gently(mut);
            co_await _db.apply(mut.schema(), frozen_mut,
                tracing::trace_state_ptr(),
                db::commitlog::force_sync::no,
                db::timeout_clock::time_point::max());
        }
    public:
        sc_state_machine(table_id table_id,
            locator::tablet_id tablet_id,
            raft::group_id gid,
            replica::database& db, 
            db::system_keyspace& sys_ks)
            : _table_id(table_id)
            , _tablet_id(tablet_id)
            , _group_id(gid)
            , _group_key(::format("{}", _group_id))
            , _db(db)
            , _sys_ks(sys_ks)
        {
        }

        future<> apply(std::vector<raft::command_cref> command) override {
            try {
                auto last_state_id = co_await _sys_ks.get_last_group0_state_id(_group_key);
                std::vector<future<>> futures;
                for (auto&& c : command) {
                    auto is = ser::as_input_stream(c);
                    const auto cmd = ser::deserialize(is, std::type_identity<sc_raft_command>{});
                    if (cmd.prev_state_id != last_state_id) {
                        logger.debug("table {}, tablet {}, group id {}: cmd.prev_state_id ({}) "
                            "different than the last state ID in history table ({})",
                            _table_id, _tablet_id, _group_id, cmd.prev_state_id, last_state_id);
                        continue;
                    }
                    last_state_id = cmd.new_state_id;
                    for (const auto& cm : cmd.mutations) {
                        const auto s = _db.find_column_family(cm.column_family_id()).schema();

                        // FIXME: can't we convert from canonical_mutation to frozen_mutaton more efficiently
                        // than deserialize/serialize?
                        auto f = to_mutation_gently(cm, s)
                            .then([this](mutation m){ return apply(std::move(m)); })
                            .then([this, state = cmd.new_state_id] {
                                auto hist_mut = _sys_ks.make_group0_history_state_id_mutation(
                                    state,
                                    history_gc_duration,
                                    "",
                                    _group_key);
                                return apply(std::move(hist_mut));
                            });
                        futures.push_back(std::move(f));
                    }
                }

                // We don't need atomicity here:
                //   * the Raft server will not advance applied_idx until all commands
                //     in the batch have been applied;
                //   * mutations are idempotent — a subsequent sm::apply attempt can
                //     safely reapply all of them;
                //   * no reader should observe partially applied data.
                co_await when_all_succeed(std::move(futures));
            } catch (replica::no_such_column_family& e) {
                throw std::runtime_error(::format(
                    "table {}, tablet {}, group id {}: error while applying mutations {}",
                    _table_id, _tablet_id, _group_id, e));
            }
        }

        future<raft::snapshot_id> take_snapshot() override {
            throw std::runtime_error("take_snapshot() not implemented");
        }

        void drop_snapshot(raft::snapshot_id id) override {
            throw std::runtime_error("drop_snapshot() not implemented");
        }

        future<> load_snapshot(raft::snapshot_id id) override {
            return make_ready_future<>();
        }

        future<> abort() override {
            return make_ready_future<>();
        }

        future<> transfer_snapshot(raft::server_id from_id, raft::snapshot_descriptor snp) override {
            throw std::runtime_error("transfer_snapshot() not implemented");
        }
    };

    std::unique_ptr<raft_state_machine> make_sc_state_machine(
        table_id table_id,
        locator::tablet_id tablet_id,
        raft::group_id gid,
        replica::database& db, 
        db::system_keyspace& sys_ks) {
        return std::make_unique<sc_state_machine>(table_id, tablet_id, gid, db, sys_ks);
    }
};