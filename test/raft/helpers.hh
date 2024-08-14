/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

//
// Helper functions for raft tests
//

#pragma once

#include "utils/assert.hh"
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

raft::snapshot_descriptor log_snapshot(raft::log& log, raft::index_t idx);

template <typename T>
raft::command create_command(T val) {
    raft::command command;
    ser::serialize(command, val);

    return command;
}

extern raft::fsm_config fsm_cfg;
extern raft::fsm_config fsm_cfg_pre;

struct sm_events_container {
    seastar::condition_variable sm_events;
};

class fsm_debug : public sm_events_container, public raft::fsm {
public:
    using raft::fsm::fsm;

    explicit fsm_debug(raft::server_id id, raft::term_t current_term, raft::server_id voted_for, raft::log log,
            raft::failure_detector& failure_detector, raft::fsm_config conf)
        : sm_events_container()
        , fsm(id, current_term, voted_for, std::move(log), raft::index_t{0}, failure_detector, conf, sm_events) {
    }

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
        SCYLLA_ASSERT(is_leader());
        return bool(leader_state().stepdown);
    }
};

// NOTE: it doesn't compare data contents, just the data type
bool compare_log_entry(raft::log_entry_ptr le1, raft::log_entry_ptr le2);
bool compare_log_entries(raft::log& log1, raft::log& log2, raft::index_t from, raft::index_t to);
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
raft::config_member_set config_set(std::vector<raft::server_id> ids);
fsm_debug create_follower(raft::server_id id, raft::log log,
        raft::failure_detector& fd = trivial_failure_detector);


// Raft uses UUID 0 as special case.
// Convert local 0-based integer id to raft +1 UUID
utils::UUID to_raft_uuid(size_t int_id);
raft::server_id to_raft_id(size_t int_id);

raft::server_address to_server_address(size_t int_id);
// NOTE: can_vote = true
raft::config_member to_config_member(size_t int_id);

size_t to_int_id(utils::UUID uuid);
// Return true upon a random event with given probability
bool rolladice(float probability = 1.0/2.0);

// Invokes an abortable function `f` on shard `shard` using abort source `as` from the current shard.
// It's not safe to use `as` directly on a different shard. This function routes the abort requests
// from `as` to the other shard.
future<> invoke_abortable_on(unsigned shard, noncopyable_function<future<>(abort_source&)> f, abort_source& as);

// Server address with given ID and empty `info`.
raft::server_address server_addr_from_id(raft::server_id);
// Config member with given ID, empty `info`, a voter.
raft::config_member config_member_from_id(raft::server_id);

// Make a non-joint configuration from a given set of IDs by setting empty `server_info`s and `can_vote = true`.
raft::configuration config_from_ids(std::vector<raft::server_id>);
