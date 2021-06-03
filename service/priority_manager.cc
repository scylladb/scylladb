/*
 * Copyright 2016-present ScyllaDB
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
#include "priority_manager.hh"
#include <seastar/core/reactor.hh>

namespace service {
priority_manager& get_local_priority_manager() {
    static thread_local priority_manager pm = priority_manager();
    return pm;
}

priority_manager::priority_manager()
    : _commitlog_priority(engine().register_one_priority_class("commitlog", 1000))
    , _mt_flush_priority(engine().register_one_priority_class("memtable_flush", 1000))
    , _streaming_priority(engine().register_one_priority_class("streaming", 200))
    , _sstable_query_read(engine().register_one_priority_class("query", 1000))
    , _compaction_priority(engine().register_one_priority_class("compaction", 1000))
{}

}
