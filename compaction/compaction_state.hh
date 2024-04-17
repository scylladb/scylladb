/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
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

struct compaction_state {
    // Used both by compaction tasks that refer to the compaction_state
    // and by any function running under run_with_compaction_disabled().
    seastar::gate gate;

    // Prevents table from running major and minor compaction at the same time.
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
