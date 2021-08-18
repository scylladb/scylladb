/*
 * Copyright (C) 2021-present ScyllaDB
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

#include <boost/test/unit_test.hpp>
#include "test/lib/log.hh"
#include "test/lib/random_utils.hh"
#include "serializer_impl.hh"
#include <limits>

#include "raft/fsm.hh"

using seastar::make_lw_shared;

void election_threshold(raft::fsm& fsm);
void election_timeout(raft::fsm& fsm);
void make_candidate(raft::fsm& fsm);

struct trivial_failure_detector: public raft::failure_detector {
    bool is_alive(raft::server_id from) override {
        return true;
    }
};
extern struct trivial_failure_detector trivial_failure_detector;

class discrete_failure_detector: public raft::failure_detector {
    bool _is_alive = true;
    std::unordered_set<raft::server_id> _dead;
public:
    bool is_alive(raft::server_id id) override {
        return _is_alive && !_dead.contains(id);
    }
    void mark_dead(raft::server_id id) { _dead.emplace(id); }
    void mark_alive(raft::server_id id) { _dead.erase(id); }
    void mark_all_dead() { _is_alive = false; }
    void mark_all_alive() { _is_alive = true; }
};

template <typename T> void add_entry(raft::log& log, T cmd) {
    log.emplace_back(make_lw_shared<raft::log_entry>(raft::log_entry{log.last_term(), log.next_idx(), cmd}));
}

raft::snapshot log_snapshot(raft::log& log, raft::index_t idx);

template <typename T>
raft::command create_command(T val) {
    raft::command command;
    ser::serialize(command, val);

    return command;
}

extern raft::fsm_config fsm_cfg;
extern raft::fsm_config fsm_cfg_pre;

class fsm_debug : public raft::fsm {
public:
    using raft::fsm::fsm;
    void become_follower(raft::server_id leader) {
        raft::fsm::become_follower(leader);
    }
    const raft::follower_progress& get_progress(raft::server_id id) {
        raft::follower_progress* progress = leader_state().tracker.find(id);
        return *progress;
    }
    raft::log& get_log() {
        return raft::fsm::get_log();
    }

    bool leadership_transfer_active() const {
        assert(is_leader());
        return bool(leader_state().stepdown);
    }
};

// NOTE: it doesn't compare data contents, just the data type
bool compare_log_entry(raft::log_entry_ptr le1, raft::log_entry_ptr le2);
bool compare_log_entries(raft::log& log1, raft::log& log2, size_t from, size_t to);
using raft_routing_map = std::unordered_map<raft::server_id, raft::fsm*>;

bool deliver(raft_routing_map& routes, raft::server_id from,
        std::pair<raft::server_id, raft::rpc_message> m);
void deliver(raft_routing_map& routes, raft::server_id from,
        std::vector<std::pair<raft::server_id, raft::rpc_message>> msgs);

void
communicate_impl(std::function<bool()> stop_pred, raft_routing_map& map);

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


raft::server_id id();
raft::server_address_set address_set(std::vector<raft::server_id> ids);
fsm_debug create_follower(raft::server_id id, raft::log log,
        raft::failure_detector& fd = trivial_failure_detector);


// Raft uses UUID 0 as special case.
// Convert local 0-based integer id to raft +1 UUID
utils::UUID to_raft_uuid(size_t int_id);
raft::server_id to_raft_id(size_t int_id);

// NOTE: can_vote = true
raft::server_address to_server_address(size_t int_id);
size_t to_int_id(utils::UUID uuid);
// Return true upon a random event with given probability
bool rolladice(float probability = 1.0/2.0);
