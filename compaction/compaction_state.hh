/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <seastar/core/gate.hh>
#include <seastar/core/rwlock.hh>
#include <seastar/core/condition-variable.hh>
#include "seastarx.hh"

#include <memory>
#include <unordered_set>

#include "compaction/compaction_fwd.hh"
#include "compaction/compaction_backlog_manager.hh"
#include "sstables/shared_sstable.hh"
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

    // Cleanup tracking is used only with vnodes (never with tablets) and only
    // while a cleanup is in progress. To avoid paying for it on every
    // compaction_state -- there is one per compaction group, and they are
    // long-lived -- it is held behind a lazily-allocated pointer that stays
    // null until the first sstable is marked as requiring cleanup, and is
    // released again once the set becomes empty.
    //
    // sizeof(cleanup_state) is ~64 bytes (an unordered_set plus an
    // owned_ranges_ptr), so this keeps that out of the common case where no
    // cleanup is running, shrinking compaction_state from 344 to 288 bytes.
    struct cleanup_state {
        // Set of sstables that still require cleanup.
        std::unordered_set<sstables::shared_sstable> sstables_requiring_cleanup;
        // Owned token ranges to keep while cleaning the above sstables.
        compaction::owned_ranges_ptr owned_ranges_ptr;
    };
private:
    std::unique_ptr<cleanup_state> _cleanup_state;

public:
    // Read-only view of the sstables requiring cleanup; empty when no cleanup
    // is in progress.
    //
    // Lifetime: the returned reference aliases the lazily-allocated
    // cleanup_state and is only valid until the next cleanup-state mutation.
    // Erasing the last sstable (erase_sstable_requiring_cleanup) frees the
    // cleanup_state, so a reference held across such a mutation dangles. All
    // current callers consume it synchronously (iterate or copy out) before any
    // mutation; do not retain it.
    //
    // Note: the accessors that construct/destroy the cleanup_state (and its
    // unordered_set / owned_ranges_ptr) are defined out-of-line in
    // compaction_manager.cc, next to the destructor, so that including this
    // header does not require the complete sstable/dht::token types.
    const std::unordered_set<sstables::shared_sstable>& sstables_requiring_cleanup() const;

    // Whether any sstable currently requires cleanup.
    bool has_sstables_requiring_cleanup() const {
        return _cleanup_state && !_cleanup_state->sstables_requiring_cleanup.empty();
    }

    // Whether the given sstable currently requires cleanup.
    bool requires_cleanup(const sstables::shared_sstable& sst) const;

    // The owned ranges associated with the in-progress cleanup, or null when no
    // cleanup is in progress.
    //
    // Returned by value (a cheap lw_shared_ptr refcount bump) rather than by
    // reference: the underlying owned_ranges_ptr lives in the lazily-allocated
    // cleanup_state, which is freed when the cleanup set drains. Handing out a
    // copy keeps the ranges alive for the caller independently of that
    // lifetime, so it cannot dangle.
    compaction::owned_ranges_ptr cleanup_owned_ranges() const;

    // Mark an sstable as requiring cleanup, allocating the cleanup state on
    // demand.
    void insert_sstable_requiring_cleanup(const sstables::shared_sstable& sst);

    // Remove an sstable from the cleanup set if present, returning whether it
    // was present. Releases the cleanup state (including the owned ranges) once
    // the set becomes empty.
    bool erase_sstable_requiring_cleanup(const sstables::shared_sstable& sst);

    // Record the owned ranges to retain while cleaning up. Allocates the
    // cleanup state on demand.
    void set_cleanup_owned_ranges(compaction::owned_ranges_ptr ranges);

    gc_clock::time_point last_regular_compaction;

    explicit compaction_state(compaction_group_view& t);
    compaction_state(compaction_state&&) = delete;
    ~compaction_state();

    bool compaction_disabled() const noexcept {
        return compaction_disabled_counter > 0;
    }
};

} // namespace compaction
