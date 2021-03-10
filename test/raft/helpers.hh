/*
 * Copyright (c) 2021, Arm Limited and affiliates. All rights reserved.
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

//
// Helper functions for raft tests
//

#pragma once

#define BOOST_TEST_MODULE raft

#include <boost/test/unit_test.hpp>
#include "test/lib/log.hh"
#include "serializer_impl.hh"
#include <limits>

#include "raft/fsm.hh"

using raft::term_t, raft::index_t, raft::server_id, raft::log_entry;
using seastar::make_lw_shared;

void election_threshold(raft::fsm& fsm) {
    // Election threshold should be strictly less than
    // minimal randomized election timeout to make tests
    // stable, but enough to disable "stable leader" rule.
    for (int i = 0; i < raft::ELECTION_TIMEOUT.count(); i++) {
        fsm.tick();
    }
}

void election_timeout(raft::fsm& fsm) {
    for (int i = 0; i <= 2 * raft::ELECTION_TIMEOUT.count(); i++) {
        fsm.tick();
    }
}

struct failure_detector: public raft::failure_detector {
    bool alive = true;
    bool is_alive(raft::server_id from) override {
        return alive;
    }
};

template <typename T> void add_entry(raft::log& log, T cmd) {
    log.emplace_back(make_lw_shared<log_entry>(log_entry{log.last_term(), log.next_idx(), cmd}));
}

raft::snapshot log_snapshot(raft::log& log, index_t idx) {
    return raft::snapshot{.idx = idx, .term = log.last_term(), .config = log.get_snapshot().config};
}

template <typename T>
raft::command create_command(T val) {
    raft::command command;
    ser::serialize(command, val);

    return std::move(command);
}

raft::fsm_config fsm_cfg{.append_request_threshold = 1, .enable_prevoting = false};

class fsm_debug : public raft::fsm {
public:
    using raft::fsm::fsm;
    const raft::follower_progress& get_progress(server_id id) {
        raft::follower_progress* progress = leader_state().tracker.find(id);
        return *progress;
    }
};

using raft_routing_map = std::unordered_map<raft::server_id, raft::fsm*>;

void
communicate_impl(std::function<bool()> stop_pred, raft_routing_map& map) {
    // To enable tracing, set:
    // global_logger_registry().set_all_loggers_level(seastar::log_level::trace);
    //
    bool has_traffic;
    do {
        has_traffic = false;
        for (auto e : map) {
            raft::fsm& from = *e.second;
            bool has_output;
            for (auto output = from.get_output(); !output.empty(); output = from.get_output()) {
                if (stop_pred()) {
                    return;
                }
                for (auto&& m : output.messages) {
                    has_traffic = true;
                    auto it = map.find(m.first);
                    if (it == map.end()) {
                        // The node is not available, drop the message
                        continue;
                    }
                    raft::fsm& to = *(it->second);
                    std::visit([&from, &to](auto&& m) { to.step(from.id(), std::move(m)); },
                        std::move(m.second));
                    if (stop_pred()) {
                        return;
                    }
                }
            }
        }
    } while (has_traffic);
}

template <typename... Args>
void communicate_until(std::function<bool()> stop_pred, Args&&... args) {
    raft_routing_map map;
    auto add_map_entry = [&map](raft::fsm& fsm) -> void {
        map.emplace(fsm.id(), &fsm);
    };
    (add_map_entry(args), ...);
    communicate_impl(stop_pred, map);
}

template <typename... Args>
void communicate(Args&&... args) {
    return communicate_until([]() { return false; }, std::forward<Args>(args)...);
}

template <typename... Args>
raft::fsm* select_leader(Args&&... args) {
    raft::fsm* leader = nullptr;
    auto assign_leader = [&leader](raft::fsm& fsm) {
        if (fsm.is_leader()) {
            leader = &fsm;
            return false;
        }
        return true;
    };
    (assign_leader(args) && ...);
    BOOST_CHECK(leader);
    return leader;
}


raft::server_id id() {
    static int id = 0;
    return raft::server_id{utils::UUID(0, ++id)};
}

raft::server_address_set address_set(std::initializer_list<raft::server_id> ids) {
    raft::server_address_set set;
    for (auto id : ids) {
        set.emplace(raft::server_address{.id = id});
    }
    return set;
}

