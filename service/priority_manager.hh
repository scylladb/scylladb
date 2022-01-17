/*
 * Copyright 2016-present ScyllaDB
 */
/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/file.hh>

#include "seastarx.hh"

namespace service {
class priority_manager {
    ::io_priority_class _commitlog_priority;
    ::io_priority_class _mt_flush_priority;
    ::io_priority_class _streaming_priority;
    ::io_priority_class _sstable_query_read;
    ::io_priority_class _compaction_priority;

public:
    const ::io_priority_class&
    commitlog_priority() const {
        return _commitlog_priority;
    }

    const ::io_priority_class&
    memtable_flush_priority() const {
        return _mt_flush_priority;
    }

    const ::io_priority_class&
    streaming_priority() const {
        return _streaming_priority;
    }

    const ::io_priority_class&
    sstable_query_read_priority() const {
        return _sstable_query_read;
    }

    const ::io_priority_class&
    compaction_priority() const {
        return _compaction_priority;
    }

    priority_manager();
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
get_local_streaming_priority() {
    return get_local_priority_manager().streaming_priority();
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
