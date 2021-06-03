/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "database.hh"
#include "sstables/sstables.hh"
#include "db/view/view_updating_consumer.hh"

#include <seastar/core/abort_source.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/semaphore.hh>

namespace db::view {

class view_update_generator {
public:
    static constexpr size_t registration_queue_size = 5;

private:
    database& _db;
    seastar::abort_source _as;
    future<> _started = make_ready_future<>();
    seastar::condition_variable _pending_sstables;
    named_semaphore _registration_sem{registration_queue_size, named_semaphore_exception_factory{"view update generator"}};
    struct sstable_with_table {
        sstables::shared_sstable sst;
        lw_shared_ptr<table> t;
        sstable_with_table(sstables::shared_sstable sst, lw_shared_ptr<table> t) : sst(std::move(sst)), t(std::move(t)) { }
    };
    std::unordered_map<lw_shared_ptr<table>, std::vector<sstables::shared_sstable>> _sstables_with_tables;
    std::unordered_map<lw_shared_ptr<table>, std::vector<sstables::shared_sstable>> _sstables_to_move;
    metrics::metric_groups _metrics;
public:
    view_update_generator(database& db) : _db(db) {
        setup_metrics();
    }

    future<> start();
    future<> stop();
    future<> register_staging_sstable(sstables::shared_sstable sst, lw_shared_ptr<table> table);

    ssize_t available_register_units() const { return _registration_sem.available_units(); }
private:
    bool should_throttle() const;
    void setup_metrics();
};

}
