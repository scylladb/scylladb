/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "sc_state_machine.hh"

namespace service {
    class sc_state_machine: public raft_state_machine {
        future<> apply(std::vector<raft::command_cref> command) override {
            throw std::runtime_error("not implemented");
        }

        future<raft::snapshot_id> take_snapshot() override {
            throw std::runtime_error("not implemented");
        }

        void drop_snapshot(raft::snapshot_id id) override {
            throw std::runtime_error("not implemented");
        }

        future<> load_snapshot(raft::snapshot_id id) override {
            throw std::runtime_error("not implemented");
        }

        future<> abort() override {
            throw std::runtime_error("not implemented");
        }

        future<> transfer_snapshot(raft::server_id from_id, raft::snapshot_descriptor snp) override {
            throw std::runtime_error("not implemented");
        }
    };

    std::unique_ptr<raft_state_machine> make_sc_state_machine() {
        return std::make_unique<sc_state_machine>();
    }
};