/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/core/gate.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/rwlock.hh>
#include <seastar/core/condition-variable.hh>
#include "seastarx.hh"

#include "compaction/compaction_fwd.hh"
#include "compaction/compaction_backlog_manager.hh"
#include "gc_clock.hh"

namespace sstables { class test_env_compaction_manager; }

namespace compaction {

struct compaction_state {
private:
    // Used both by compaction tasks that refer to the compaction_state
    // and by any function running under run_with_compaction_disabled().
<<<<<<< HEAD
    seastar::gate gate;
||||||| parent of 623027b62b (compaction_manager: don't fail disabling compaction on a stopped manager)
    seastar::named_gate gate;
=======
    // It is held through compaction_manager::hold_compaction_state_gate(),
    // which fails with an abort once the manager is stopped.
    seastar::named_gate gate;
<<<<<<< HEAD
>>>>>>> 623027b62b (compaction_manager: don't fail disabling compaction on a stopped manager)
||||||| parent of 235bfb885e (compaction_manager: remove: wait for a gate closed by stop)
=======
    // The result of closing the gate, which both stopping the manager and removing
    // the table wait for, see compaction_manager::close_compaction_state_gate().
    std::optional<seastar::shared_future<>> gate_closed;
>>>>>>> 235bfb885e (compaction_manager: remove: wait for a gate closed by stop)

<<<<<<< HEAD
    // Prevents table from running major and minor compaction at the same time.
||||||| parent of 623027b62b (compaction_manager: don't fail disabling compaction on a stopped manager)
    // Serializes major compaction selection against regular compaction selection.
    // Major takes write lock; regular takes read lock.
=======
    friend class compaction_manager;
    friend class compaction_reenabler;
    friend class sstables::test_env_compaction_manager;

public:

    // Serializes major compaction selection against regular compaction selection.
    // Major takes write lock; regular takes read lock.
>>>>>>> 623027b62b (compaction_manager: don't fail disabling compaction on a stopped manager)
    seastar::rwlock lock;

    // Raised by any function running under run_with_compaction_disabled();
    long compaction_disabled_counter = 0;

    // Signaled whenever a compaction task completes.
    condition_variable compaction_done;

    std::optional<compaction_backlog_tracker> backlog_tracker;

    std::unordered_set<sstables::shared_sstable> sstables_requiring_cleanup;
    compaction::owned_ranges_ptr owned_ranges_ptr;

    gc_clock::time_point last_regular_compaction;

    explicit compaction_state(table_state& t);
    compaction_state(compaction_state&&) = delete;
    ~compaction_state();

    bool compaction_disabled() const noexcept {
        return compaction_disabled_counter > 0;
    }
};

} // namespace compaction_manager
