/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include "raft/raft.hh"

namespace service {

// Scylla specific extension for raft state machine
// Snapshot transfer is delegated to a state machine implementation
class raft_state_machine : public raft::state_machine {
public:
    virtual future<> transfer_snapshot(raft::server_id from_id, raft::snapshot_descriptor snp) = 0;

    // Called when a snapshot fetched by transfer_snapshot() will not be loaded
    // via load_snapshot() - e.g. the Raft FSM rejected it as outdated or the
    // application failed. Allows the state machine to release any resources it
    // acquired in transfer_snapshot() and kept pending for load_snapshot().
    // The default is a no-op for implementations that don't hold cross-phase state.
    virtual future<> abort_snapshot_transfer(raft::snapshot_id id) {
        return make_ready_future<>();
    }
};

} // end of namespace service
