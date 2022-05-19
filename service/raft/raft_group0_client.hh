/*
 * Copyright (C) 2022-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <memory>
#include <seastar/core/semaphore.hh>
#include <seastar/core/abort_source.hh>
#include "service/raft/raft_group_registry.hh"
#include "utils/UUID.hh"
#include "timestamp.hh"
#include "gc_clock.hh"
#include "service/raft/group0_state_machine.hh"
#include "db/system_keyspace.hh"
#include "utils/fb_utilities.hh"

namespace service {
// Obtaining this object means that all previously finished operations on group 0 are visible on this node.

// It is also required in order to perform group 0 changes
// See `group0_guard::impl` for more detailed explanations.
class group0_guard {
    friend class raft_group0_client;
    struct impl;
    std::unique_ptr<impl> _impl;

    group0_guard(std::unique_ptr<impl>);

public:
    ~group0_guard();
    group0_guard(group0_guard&&) noexcept;

    utils::UUID observed_group0_state_id() const;
    utils::UUID new_group0_state_id() const;

    // Use this timestamp when creating group 0 mutations.
    api::timestamp_type write_timestamp() const;
};

class group0_concurrent_modification : public std::runtime_error {
public:
    group0_concurrent_modification()
        : std::runtime_error("Failed to apply group 0 change due to concurrent modification")
    {}
};

// Singleton that exists only on shard zero. Used to post commands to group zero
class raft_group0_client {
    friend class group0_state_machine;
    service::raft_group_registry& _raft_gr;
    // See `group0_guard::impl` for explanation of the purpose of these locks.
    semaphore _read_apply_mutex = semaphore(1);
    semaphore _operation_mutex = semaphore(1);

    gc_clock::duration _history_gc_duration = gc_clock::duration{std::chrono::duration_cast<gc_clock::duration>(std::chrono::weeks{1})};
public:
    raft_group0_client(service::raft_group_registry& raft_gr) : _raft_gr(raft_gr) {}

    future<> add_entry(group0_command group0_cmd, group0_guard guard, seastar::abort_source* as = nullptr);
    bool is_enabled() {
        return _raft_gr.is_enabled();
    }
    future<group0_guard> start_operation(seastar::abort_source* as = nullptr);

    group0_command prepare_command(schema_change change, group0_guard& guard, std::string_view description);

    // for test only
    void set_history_gc_duration(gc_clock::duration d);
    semaphore& operation_mutex();
};

}
