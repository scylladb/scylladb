/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "state_machine.hh"
#include "serializer_impl.hh"
#include "idl/strong_consistency/state_machine.dist.hh"
#include "idl/strong_consistency/state_machine.dist.impl.hh"
#include "replica/database.hh"
#include "mutation/frozen_mutation.hh"
#include "service/migration_manager.hh"
#include "utils/error_injection.hh"

using namespace std::chrono_literals;

namespace service::strong_consistency {

class state_machine : public raft_state_machine {
    locator::global_tablet_id _tablet;
    raft::group_id _group_id;
    raft_group_registry& _raft_gr;
    replica::database& _db;
    db::system_keyspace& _sys_ks;

public:
    state_machine(locator::global_tablet_id tablet,
        raft::group_id gid,
        raft_group_registry& raft_gr,
        replica::database& db, db::system_keyspace& sys_ks)
        : _tablet(tablet)
        , _group_id(gid)
        , _raft_gr(raft_gr)
        , _db(db)
        , _sys_ks(sys_ks)
    {
    }

    future<> apply(std::vector<raft::command_cref> command) override {
        auto maybe_upgrade = [this] (frozen_mutation&& mut) -> future<frozen_mutation> {
            auto schema = _db.find_schema(_tablet.table);
            if (mut.schema_version() == schema->version()) {
                co_return mut;
            }
            const auto& cm = co_await service::get_column_mapping(_sys_ks, mut.column_family_id(), mut.schema_version());
            co_return freeze(mut.unfreeze_upgrading(schema, cm));
        };

        try {
            co_await utils::get_local_injector().inject("strong_consistency_state_machine_wait_before_apply", utils::wait_for_message(5min));
            co_await _raft_gr.group0().read_barrier(nullptr);
            utils::chunked_vector<frozen_mutation> muts;
            muts.reserve(command.size());
            for (const auto& c: command) {
                auto is = ser::as_input_stream(c);
                auto cmd = ser::deserialize(is, std::type_identity<raft_command>{});
                auto mut = co_await maybe_upgrade(std::move(cmd.mutation));
                muts.push_back(std::move(mut));
            }
            co_await _db.apply(std::move(muts), db::no_timeout);
        } catch (...) {
            throw std::runtime_error(::format(
                "tablet {}, group id {}: error while applying mutations {}",
                _tablet, _group_id, std::current_exception()));
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

std::unique_ptr<raft_state_machine> make_state_machine(locator::global_tablet_id tablet,
    raft::group_id gid,
    raft_group_registry& raft_gr,
    replica::database& db, db::system_keyspace& sys_ks)
{
    return std::make_unique<state_machine>(tablet, gid, raft_gr, db, sys_ks);
}

};