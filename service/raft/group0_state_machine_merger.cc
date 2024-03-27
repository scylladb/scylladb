/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#include "db/config.hh"
#include "db/system_keyspace.hh"
#include "service/raft/group0_state_machine_merger.hh"

namespace service {

static logging::logger slogger("group0_raft_sm_merger");

static mutation convert_history_mutation(canonical_mutation m, const data_dictionary::database db) {
    return m.to_mutation(db.find_schema(db::system_keyspace::NAME, db::system_keyspace::GROUP0_HISTORY));
}

group0_state_machine_merger::group0_state_machine_merger(utils::UUID id, semaphore_units<> mux, size_t max_command_size, data_dictionary::database db)
    : _last_group0_state_id(id)
    , _read_apply_mutex_holder(std::move(mux))
    , _max_command_size(max_command_size)
    , _db{std::move(db)} {}

size_t group0_state_machine_merger::cmd_size(group0_command& cmd) {
    if (holds_alternative<broadcast_table_query>(cmd.change)) {
        return 0;
    }
    auto r = get_command_mutations(cmd) | boost::adaptors::transformed([] (const canonical_mutation& m) { return m.representation().size(); });
    return std::accumulate(std::begin(r), std::end(r), size_t(0));
}

bool group0_state_machine_merger::can_merge(group0_command& cmd, size_t s) const {
    if (!_cmd_to_merge.empty()) {
        // broadcast table commands or different type of commands cannot be merged
        if (_cmd_to_merge[0].change.index() != cmd.change.index() || holds_alternative<broadcast_table_query>(cmd.change)) {
            return false;
        }
    }

    // Check that merged command will not be larger than half of commitlog segment.
    // Merged command can be, in fact, much smaller but better to be safe than sorry.
    // Skip the check for the first command.
    if (_size && _size + s > _max_command_size) {
        return false;
    }

    return true;
}

void group0_state_machine_merger::add(group0_command&& cmd, size_t added_size) {
    slogger.trace("add to merging set new_state_id: {}", cmd.new_state_id);
    auto m = convert_history_mutation(std::move(cmd.history_append), _db);
    // Set `last_group0_state_id` to the maximum of the current value and `cmd.new_state_id`,
    // but make sure we compare them the same way timeuuids are compared in clustering keys
    // (i.e. in the same order that the history table is sorted).
    if (utils::timeuuid_tri_compare(_last_group0_state_id, cmd.new_state_id) < 0) {
        _last_group0_state_id = cmd.new_state_id;
    }
    _cmd_to_merge.push_back(std::move(cmd));
    _size += added_size;
    if (_merged_history_mutation) {
        _merged_history_mutation->apply(std::move(m));
    } else {
        _merged_history_mutation = std::move(m);
    }
}

std::vector<canonical_mutation>& group0_state_machine_merger::get_command_mutations(group0_command& cmd) {
    return std::visit(make_visitor(
        [] (schema_change& chng) -> std::vector<canonical_mutation>& {
            return chng.mutations;
        },
        [] (broadcast_table_query& query) -> std::vector<canonical_mutation>& {
            on_internal_error(slogger, "trying to merge broadcast table command");
        },
        [] (topology_change& chng) -> std::vector<canonical_mutation>& {
            return chng.mutations;
        },
        [] (mixed_change& chng) -> std::vector<canonical_mutation>& {
            return chng.mutations;
        },
        [] (write_mutations& muts) -> std::vector<canonical_mutation>& {
            return muts.mutations;
        }
    ), cmd.change);
}

std::pair<group0_command, mutation> group0_state_machine_merger::merge() {
    auto& cmd = _cmd_to_merge.back(); // use metadata from the last merged command
    slogger.trace("merge new_state_id: {}", cmd.new_state_id);
    using mutation_set_type = std::unordered_set<mutation, mutation_hash_by_key, mutation_equals_by_key>;
    std::unordered_map<table_id, mutation_set_type> mutations;

    if (_cmd_to_merge.size() > 1) {
        // skip merging if there is only one command
        for (auto&& c : _cmd_to_merge) {
            for (auto&& cm : get_command_mutations(c)) {
                auto schema = _db.find_schema(cm.column_family_id());
                auto m = cm.to_mutation(schema);
                auto& tbl_muts = mutations[cm.column_family_id()];
                auto it = tbl_muts.find(m);
                if (it == tbl_muts.end()) {
                    tbl_muts.emplace(std::move(m));
                } else {
                    const_cast<mutation&>(*it).apply(std::move(m)); // Won't change key
                }
            }
        }

        std::vector<canonical_mutation> ms;
        for (auto&& tables : mutations) {
            for (auto&& partitions : tables.second) {
                ms.push_back(canonical_mutation(partitions));
            }
        }

        get_command_mutations(cmd) = std::move(ms);
    }
    auto res = std::make_pair(std::move(cmd), std::move(_merged_history_mutation).value());
    _cmd_to_merge.clear();
    _merged_history_mutation.reset();
    return res;
}

} // end of namespace service
