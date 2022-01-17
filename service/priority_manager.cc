/*
 * Copyright 2016-present ScyllaDB
 */
/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#include "priority_manager.hh"
#include <seastar/core/reactor.hh>

namespace service {
priority_manager& get_local_priority_manager() {
    static thread_local priority_manager pm = priority_manager();
    return pm;
}

priority_manager::priority_manager()
    : _commitlog_priority(::io_priority_class::register_one("commitlog", 1000))
    , _mt_flush_priority(::io_priority_class::register_one("memtable_flush", 1000))
    , _streaming_priority(::io_priority_class::register_one("streaming", 200))
    , _sstable_query_read(::io_priority_class::register_one("query", 1000))
    , _compaction_priority(::io_priority_class::register_one("compaction", 1000))
{}

}
