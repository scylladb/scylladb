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

namespace service::strong_consistency {

class state_machine : public raft_state_machine {
    locator::global_tablet_id _tablet;
    raft::group_id _group_id;
    replica::database& _db;

public:
    state_machine(locator::global_tablet_id tablet,
        raft::group_id gid,
        replica::database& db)
        : _tablet(tablet)
        , _group_id(gid)
        , _db(db)
    {
    }

    future<> apply(std::vector<raft::command_cref> command) override {
        try {
            utils::chunked_vector<frozen_mutation> muts;
            muts.reserve(command.size());
            for (const auto& c: command) {
                auto is = ser::as_input_stream(c);
                auto cmd = ser::deserialize(is, std::type_identity<raft_command>{});
                muts.insert(muts.end(), std::make_move_iterator(cmd.mutations.begin()), std::make_move_iterator(cmd.mutations.end()));
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
    replica::database& db)
{
    return std::make_unique<state_machine>(tablet, gid, db);
}

};