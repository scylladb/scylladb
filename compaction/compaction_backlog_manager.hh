/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <unordered_set>
#include <memory>
#include "sstables/shared_sstable.hh"
#include "timestamp.hh"

class compaction_backlog_manager;
class compaction_controller;

// Read and write progress are provided by structures present in progress_manager.hh
// However, we don't want to be tied to their lifetimes and for that reason we will not
// manipulate them directly in the backlog manager.
//
// The structure that register those progress structures - the monitors - will let us know when does
// the sstable-side tracking structures go out of scope. And in that case we can do something about
// it.
//
// These objects lifetime are tied to the SSTable shared object lifetime and are expected to be kept
// alive until the SSTable is no longer being written/compacted. In other words, they are expected
// to be kept alive until one of compaction_backlog_tracker's add_sstable, remove_sstable or
// revert_charges are called.
//
// The reason for us to bundle this into the add_sstable/remove_sstable lifetime, instead of dealing
// with them separately and removing when the data transfer is done is that if we unregister a
// progrees manager and only later on add/remove the corresponding SSTable we can see large valley
// or peaks in the backlog.
//
// Those valleys/peaks originate from the fact that we may be removing/adding a fair chunk of
// backlog that will only be corrected much later, when we call add_sstable / remove_sstable.
// Because of that, we want to keep the backlog originating from the progress manager around for as
// long as possible, up to the moment the sstable list is fixed. The compaction object hosting this
// will certainly be gone by then.
struct backlog_write_progress_manager {
    virtual uint64_t written() const = 0;
    virtual api::timestamp_type maximum_timestamp() const = 0;
    virtual unsigned level() const = 0;
    virtual ~backlog_write_progress_manager() {}
};

struct backlog_read_progress_manager {
    virtual uint64_t compacted() const = 0;
    virtual ~backlog_read_progress_manager() {}
};

// Manages one individual source of compaction backlog, usually a column family.
class compaction_backlog_tracker {
public:
    using ongoing_writes = std::unordered_map<sstables::shared_sstable, backlog_write_progress_manager*>;
    using ongoing_compactions = std::unordered_map<sstables::shared_sstable, backlog_read_progress_manager*>;

    struct impl {
        // FIXME: Should provide strong exception safety guarantees
        virtual void replace_sstables(const std::vector<sstables::shared_sstable>& old_ssts, const std::vector<sstables::shared_sstable>& new_ssts) = 0;
        virtual double backlog(const ongoing_writes& ow, const ongoing_compactions& oc) const = 0;
        virtual ~impl() { }
    };

    compaction_backlog_tracker(std::unique_ptr<impl> impl) : _impl(std::move(impl)) {}
    compaction_backlog_tracker(compaction_backlog_tracker&&);
    compaction_backlog_tracker& operator=(compaction_backlog_tracker&&) = delete;
    compaction_backlog_tracker(const compaction_backlog_tracker&) = delete;
    ~compaction_backlog_tracker();

    double backlog() const;
    // FIXME: Should provide strong exception safety guarantees
    void replace_sstables(const std::vector<sstables::shared_sstable>& old_ssts, const std::vector<sstables::shared_sstable>& new_ssts);
    void register_partially_written_sstable(sstables::shared_sstable sst, backlog_write_progress_manager& wp);
    void register_compacting_sstable(sstables::shared_sstable sst, backlog_read_progress_manager& rp);
    void copy_ongoing_charges(compaction_backlog_tracker& new_bt, bool move_read_charges = true) const;
    void revert_charges(sstables::shared_sstable sst);

    void disable() {
        _impl = {};
        _ongoing_writes = {};
        _ongoing_compactions = {};
    }
private:
    // Returns true if this SSTable can be added or removed from the tracker.
    bool sstable_belongs_to_tracker(const sstables::shared_sstable& sst);
    bool disabled() const noexcept { return !_impl; }
    std::unique_ptr<impl> _impl;
    // We keep track of this so that we can transfer to a new tracker if the compaction strategy is
    // changed in the middle of a compaction.
    ongoing_writes _ongoing_writes;
    ongoing_compactions _ongoing_compactions;
    compaction_backlog_manager* _manager = nullptr;
    friend class compaction_backlog_manager;
};

// System-wide manager for compaction backlog.
//
// The number of backlogs is the same as the number of column families, which can be very high.
// As an optimization we could keep column families that are not undergoing any change in a special
// list and only iterate over the ones that are changing.
//
// However, the calculation of the backlog is not that expensive: I have benchmarked the cost of
// computing the backlog of 3500 column families to less than 200 microseconds, and that is only
// requested periodically, for relatively large periods of hundreds of milliseconds.
//
// Keeping a static part for the backlog complicates the code significantly, though, so this will
// be left for a future optimization.
class compaction_backlog_manager {
    std::unordered_set<compaction_backlog_tracker*> _backlog_trackers;
    void remove_backlog_tracker(compaction_backlog_tracker* tracker);
    compaction_controller* _compaction_controller;
    friend class compaction_backlog_tracker;
public:
    ~compaction_backlog_manager();
    compaction_backlog_manager(compaction_controller& controller) : _compaction_controller(&controller) {}
    double backlog() const;
    void register_backlog_tracker(compaction_backlog_tracker& tracker);
};
