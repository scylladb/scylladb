/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "replica/database.hh"
#include "sstables/shared_sstable.hh"

#include <seastar/core/abort_source.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/semaphore.hh>

namespace db::view {

class view_update_generator {
public:
    static constexpr size_t registration_queue_size = 5;
    static constexpr size_t sstable_batch_size = 4;

private:
    replica::database& _db;
    seastar::abort_source _as;
    future<> _started = make_ready_future<>();
    seastar::condition_variable _pending_sstables;
    named_semaphore _registration_sem{registration_queue_size, named_semaphore_exception_factory{"view update generator"}};
    // We need a node-based container which provides stable iterators and references.
    std::map<lw_shared_ptr<replica::table>, circular_buffer<sstables::shared_sstable>> _sstables_with_tables;
    std::unordered_map<lw_shared_ptr<replica::table>, std::vector<sstables::shared_sstable>> _sstables_to_move;
    metrics::metric_groups _metrics;
public:
    view_update_generator(replica::database& db) : _db(db) {
        setup_metrics();
        discover_staging_sstables();
    }

    future<> start();
    future<> stop();
    future<> register_staging_sstable(sstables::shared_sstable sst, lw_shared_ptr<replica::table> table);

    ssize_t available_register_units() const { return _registration_sem.available_units(); }
private:
    bool should_throttle() const;
    void setup_metrics();
    void discover_staging_sstables();
};

}
