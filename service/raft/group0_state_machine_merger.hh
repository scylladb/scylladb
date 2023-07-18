/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#pragma once

#include "data_dictionary/data_dictionary.hh"
#include "service/raft/group0_state_machine.hh"
#include "service/storage_proxy.hh"

#include <boost/range/algorithm/transform.hpp>

namespace service {

/**
 * Since most group0 commands are just mutations it is easy to combine them
 * before passing them to a subsystem they are destined to since it is more
 * efficient. The logic that handles those mutations in a subsystem will
 * run once for each batch of commands instead of for each individual
 * command. This is especially useful when a node catches up to a leader and
 * gets a lot of commands together.
 *
 * The `group0_state_machine_merger` does exactly that. It combines commands
 * into a single command if possible, but it preserves an order between commands,
 * so each time it encounters a command to a different subsystem it flushes already
 * combined batch and starts a new one. This extra safety assumes that
 * there are dependencies between subsystems managed by group0, so the order
 * matters. It may be not the case now, but we prefer to be on a safe side.
 *
 * Broadcast table commands are not mutations, so they are never combined.
 */
class group0_state_machine_merger {
    std::vector<group0_command> _cmd_to_merge;
    std::optional<mutation> _merged_history_mutation;
    utils::UUID _last_group0_state_id;
    size_t _size = 0;
    semaphore_units<> _read_apply_mutex_holder;
    const size_t _max_command_size;
    const data_dictionary::database _db;

public:
    group0_state_machine_merger(utils::UUID id, semaphore_units<> mux, size_t max_command_size, data_dictionary::database db);

    // Returns size in bytes of mutations stored in the command.
    // Broadcast table commands have size 0.
    static size_t cmd_size(group0_command& cmd);

    // Returns true if the command can be merged with the current batch.
    // Command can be merged if it is of the same type as commands in the current batch
    // and the size of the batch will not exceed the limit.
    // Broadcast table commands cannot be merged with any other type of commands.
    bool can_merge(group0_command& cmd, size_t s) const;

    // Adds a command to the current batch.
    void add(group0_command&& cmd, size_t added_size);

    // Returns mutations stored in the command.
    // It must not be called for broadcast table commands.
    static std::vector<canonical_mutation>& get_command_mutations(group0_command& cmd);

    // Returns a command that contains all mutations from the current batch and
    // merged history mutation.
    // Empties the current batch.
    std::pair<group0_command, mutation> merge();

    bool empty() const {
        return _cmd_to_merge.empty();
    }

    utils::UUID last_id() const {
        return _last_group0_state_id;
    }
};

} // end of namespace service
