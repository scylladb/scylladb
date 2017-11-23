/*
 * Copyright 2016 ScyllaDB
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

#include <seastar/core/future.hh>
#include <seastar/core/distributed.hh>
#include <seastar/core/reactor.hh>

#include "seastarx.hh"

namespace service {
class priority_manager {
    ::io_priority_class _commitlog_priority;
    ::io_priority_class _mt_flush_priority;
    ::io_priority_class _stream_read_priority;
    ::io_priority_class _stream_write_priority;
    ::io_priority_class _sstable_query_read;
    ::io_priority_class _compaction_priority;

public:
    const ::io_priority_class&
    commitlog_priority() {
        return _commitlog_priority;
    }

    const ::io_priority_class&
    memtable_flush_priority() {
        return _mt_flush_priority;
    }

    const ::io_priority_class&
    streaming_read_priority() {
        return _stream_read_priority;
    }

    const ::io_priority_class&
    streaming_write_priority() {
        return _stream_write_priority;
    }

    const ::io_priority_class&
    sstable_query_read_priority() {
        return _sstable_query_read;
    }

    const ::io_priority_class&
    compaction_priority() {
        return _compaction_priority;
    }

    priority_manager()
        : _commitlog_priority(engine().register_one_priority_class("commitlog", 1000))
        , _mt_flush_priority(engine().register_one_priority_class("memtable_flush", 1000))
        , _stream_read_priority(engine().register_one_priority_class("streaming_read", 200))
        , _stream_write_priority(engine().register_one_priority_class("streaming_write", 200))
        , _sstable_query_read(engine().register_one_priority_class("query", 1000))
        , _compaction_priority(engine().register_one_priority_class("compaction", 1000))

    {}
};

priority_manager& get_local_priority_manager();
const inline ::io_priority_class&
get_local_commitlog_priority() {
    return get_local_priority_manager().commitlog_priority();
}

const inline ::io_priority_class&
get_local_memtable_flush_priority() {
    return get_local_priority_manager().memtable_flush_priority();
}

const inline ::io_priority_class&
get_local_streaming_read_priority() {
    return get_local_priority_manager().streaming_read_priority();
}

const inline ::io_priority_class&
get_local_streaming_write_priority() {
    return get_local_priority_manager().streaming_write_priority();
}

const inline ::io_priority_class&
get_local_sstable_query_read_priority() {
    return get_local_priority_manager().sstable_query_read_priority();
}

const inline ::io_priority_class&
get_local_compaction_priority() {
    return get_local_priority_manager().compaction_priority();
}
}
