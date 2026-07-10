/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <seastar/core/gate.hh>
#include <seastar/core/rwlock.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/condition-variable.hh>
#include "seastarx.hh"

#include "compaction/compaction_fwd.hh"
#include "compaction/compaction_backlog_manager.hh"
#include "gc_clock.hh"

namespace compaction {

// There's 1:1 relationship between compaction_grop_view and compaction_state.
// Two or more compaction_group_view can be served by the same instance of sstable::sstable_set,
// so it's not safe to track any sstable state here.
//
// Locking
// =======
//
// Two locks govern concurrency for compaction selection and sstable set mutation:
//
// 1) lock (rwlock)
//
//    Serializes major compaction against regular compaction selection.
//    Major compaction takes the write lock to ensure no regular compaction
//    is in the middle of selecting candidates -- so major sees all sstables.
//    Regular compaction takes the read lock, allowing multiple regular
//    compactions to select concurrently.
//
//    Lock ordering: lock is taken BEFORE sstable_set_lock when both are needed.
//
// 2) sstable_set_lock (semaphore, count=1)
//
//    Protects the atomicity of:
//      (a) regular compaction's: snapshot capture + eligibility filter + registration
//      (b) split/rewrite replacer's: on_compaction_completion (which mutates the
//          sstable set and deregisters old sstables from _compacting_sstables)
//
//    Without this lock, the following race is possible:
//      - Regular picker captures a snapshot of main_sstables (contains X).
//      - Split's replacer fires: moves X out of main_sstables AND deregisters
//        X from _compacting_sstables.
//      - Regular picker's filter runs on the stale snapshot, finds X eligible
//        (since it was deregistered), and selects it.
//      - Regular compaction completes, tries to remove X from main_sstables
//        where it no longer exists -> on_internal_error.
//
//    The sstable_set_lock ensures that (a) and (b) do not interleave.
//
// Lock ordering (when both are acquired):
//    lock -> sstable_set_lock
//
struct compaction_state {
    // Used both by compaction tasks that refer to the compaction_state
    // and by any function running under run_with_compaction_disabled().
    seastar::named_gate gate;

    // Serializes major compaction selection against regular compaction selection.
    // Major takes write lock; regular takes read lock.
    seastar::rwlock lock;

    // Protects the view's sstable set ownership. Serializes sstable set reads
    // (snapshot + filter + registration) against sstable set mutations
    // (on_compaction_completion which moves sstables between groups/views).
    seastar::semaphore sstable_set_lock{1};

    // Compations like major need to work on all sstables in the unrepaired
    // set, no matter if the sstable is being repaired or not. The
    // incremental_repair_lock lock is introduced to serialize repair and such
    // compactions. This lock guarantees that no sstables are being repaired.
    // Note that the minor compactions do not need to take this lock because
    // they ignore sstables that are being repaired.
    seastar::rwlock incremental_repair_lock;

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

} // namespace compaction
