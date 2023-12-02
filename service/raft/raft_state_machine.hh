/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#pragma once

#include "gms/inet_address.hh"
#include "raft/raft.hh"

namespace service {

// Scylla specific extension for raft state machine
// Snapshot transfer is delegated to a state machine implementation
class raft_state_machine : public raft::state_machine {
public:
    virtual future<> transfer_snapshot(raft::server_id from_id, raft::snapshot_descriptor snp) = 0;
};

} // end of namespace service
