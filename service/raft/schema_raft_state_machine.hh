/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#pragma once

#include "raft/raft.hh"
#include "utils/UUID_gen.hh"
#include "service/raft/raft_state_machine.hh"

namespace service {
class migration_manager;

class migration_manager;

// Raft state machine implementation for managing schema changes.
// NOTE: schema raft server is always instantiated on shard 0.
class schema_raft_state_machine : public raft_state_machine {
    migration_manager& _mm;
public:
    schema_raft_state_machine(migration_manager& mm) : _mm(mm) {}
    future<> apply(std::vector<raft::command_cref> command) override;
    future<raft::snapshot_id> take_snapshot() override;
    void drop_snapshot(raft::snapshot_id id) override;
    future<> load_snapshot(raft::snapshot_id id) override;
    future<> transfer_snapshot(gms::inet_address from, raft::snapshot_id snp) override;
    future<> abort() override;
};

} // end of namespace service
