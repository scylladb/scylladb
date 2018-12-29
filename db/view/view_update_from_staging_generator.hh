/*
 * Copyright (C) 2018 ScyllaDB
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

class view_update_from_staging_generator {
    static constexpr size_t registration_queue_size = 5;
    database& _db;
    service::storage_proxy& _proxy;
    seastar::abort_source _as;
    future<> _started = make_ready_future<>();
    seastar::condition_variable _pending_sstables;
    semaphore _registration_sem{registration_queue_size};
    struct sstable_with_table {
        sstables::shared_sstable sst;
        lw_shared_ptr<table> t;
        sstable_with_table(sstables::shared_sstable sst, lw_shared_ptr<table> t) : sst(sst), t(t) { }
    };
    std::deque<sstable_with_table> _sstables_with_tables;
public:
    view_update_from_staging_generator(database& db, service::storage_proxy& proxy) : _db(db), _proxy(proxy) { }

    future<> start();
    future<> stop();
    future<> register_staging_sstable(sstables::shared_sstable sst, lw_shared_ptr<table> table);
};

}
