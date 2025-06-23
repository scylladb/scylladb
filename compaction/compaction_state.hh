/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/core/gate.hh>
#include <seastar/core/rwlock.hh>
#include <seastar/core/condition-variable.hh>
#include "seastarx.hh"

#include "compaction/compaction_fwd.hh"
#include "compaction/compaction_backlog_manager.hh"
#include "gc_clock.hh"

namespace compaction {

// There's 1:1 relationship between compaction_grop_view and compaction_state.
// Two or more compaction_group_view can be served by the same instance of sstable::sstable_set,
// so it's not safe to track any sstable state here.
struct compaction_state {
    // Used both by compaction tasks that refer to the compaction_state
    // and by any function running under run_with_compaction_disabled().
    seastar::named_gate gate;

    // Used for synchronizing selection of sstable for compaction.
    // Write lock is held when getting sstable list, feeding them into strategy, and registering compacting sstables.
    // The lock prevents two concurrent compaction tasks from picking the same sstables. And it also helps major
    // to synchronize with minor, such that major doesn't miss any sstable.
    seastar::rwlock lock;

    // Raised by any function running under run_with_compaction_disabled();
    long compaction_disabled_counter = 0;

    // Signaled whenever a compaction task completes.
    condition_variable compaction_done;

    // Used only with vnodes, will not work with tablets. Can be removed once vnodes are gone.
    std::unordered_set<sstables::shared_sstable> sstables_requiring_cleanup;
    compaction::owned_ranges_ptr owned_ranges_ptr;

    gc_clock::time_point last_regular_compaction;

    explicit compaction_state(compaction_group_view& t);
    compaction_state(compaction_state&&) = delete;
    ~compaction_state();

    bool compaction_disabled() const noexcept {
        return compaction_disabled_counter > 0;
    }
};

} // namespace compaction_manager
