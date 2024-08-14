/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#undef SEASTAR_TESTING_MAIN
#include "utils/to_string.hh"
#include "replication.hh"

seastar::logger tlogger("test");

seastar::semaphore snapshot_sync(0);
// application of a snapshot with that id will be delayed until snapshot_sync is signaled
raft::snapshot_id delay_apply_snapshot{utils::UUID(0, 0xdeadbeaf)};
// sending of a snapshot with that id will be delayed until snapshot_sync is signaled
raft::snapshot_id delay_send_snapshot{utils::UUID(0xdeadbeaf, 0)};

std::vector<raft::server_id> to_raft_id_vec(std::vector<node_id> nodes) noexcept {
    std::vector<raft::server_id> ret;
    for (auto node: nodes) {
        ret.push_back(raft::server_id{to_raft_uuid(node.id)});
    }
    return ret;
}

raft::server_address_set address_set(std::vector<node_id> nodes) noexcept {
    return address_set(to_raft_id_vec(nodes));
}

raft::config_member_set config_set(std::vector<node_id> nodes) noexcept {
    return config_set(to_raft_id_vec(nodes));
}

size_t test_case::get_first_val() {
    // Count existing leader snap index and entries, if present
    size_t first_val = 0;
    if (initial_leader < initial_states.size()) {
        first_val += initial_states[initial_leader].le.size();
    }
    if (initial_leader < initial_snapshots.size()) {
        first_val = initial_snapshots[initial_leader].snap.idx.value();
    }
    return first_val;
}

std::mt19937 random_generator() noexcept {
    auto& gen = seastar::testing::local_random_engine;
    return std::mt19937(gen());
}

int rand() noexcept {
    static thread_local std::uniform_int_distribution<int> dist(0, std::numeric_limits<uint8_t>::max());
    static thread_local auto gen = random_generator();

    return dist(gen);
}

size_t apply_changes(raft::server_id id, const std::vector<raft::command_cref>& commands,
        lw_shared_ptr<hasher_int> hasher) {
    size_t entries = 0;
    tlogger.debug("sm::apply_changes[{}] got {} entries", id, commands.size());

    for (auto&& d : commands) {
        auto is = ser::as_input_stream(d);
        int n = ser::deserialize(is, boost::type<int>());
        if (n != dummy_command) {
            entries++;
            hasher->update(n);      // running hash (values and snapshots)
            tlogger.debug("{}: apply_changes {}", id, n);
        }
    }
    return entries;
}

std::vector<raft::log_entry> create_log(std::vector<log_entry> list, raft::index_t start_idx) {
    std::vector<raft::log_entry> log;

    raft::index_t i = start_idx;
    for (auto e : list) {
        if (std::holds_alternative<int>(e.data)) {
            log.push_back(raft::log_entry{raft::term_t(e.term), i++,
                    create_command(std::get<int>(e.data))});
        } else {
            log.push_back(raft::log_entry{raft::term_t(e.term), i++,
                    std::get<raft::configuration>(e.data)});
        }
    }

    return log;
}
