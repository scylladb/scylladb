/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */
#pragma once

#include "raft/raft.hh"

// Raft state machine implementation for managing schema changes.
// NOTE: schema raft server is always instantiated on shard 0.
class schema_raft_state_machine : public raft::state_machine {
public:
    // schema raft server always belongs to a special pre-defined raft group with id = UUID(0, 0)
    // TODO: 0-raft group id should be unique for each scylla cluster
    static constexpr raft::group_id gid;

    future<> apply(std::vector<raft::command_cref> command) override;
    future<raft::snapshot_id> take_snapshot() override;
    void drop_snapshot(raft::snapshot_id id) override;
    future<> load_snapshot(raft::snapshot_id id) override;
    future<> abort() override;
};
