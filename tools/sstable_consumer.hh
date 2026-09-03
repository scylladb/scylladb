/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "seastarx.hh"

#include <seastar/core/loop.hh>

namespace sstables {
class sstable;
}

class partition_start;
class static_row;
class clustering_row;
class range_tombstone_change;
class partition_end;

// stop_iteration::no -> continue consuming sstable content
class sstable_consumer {
public:
    virtual ~sstable_consumer() = default;
    // called at the very start
    virtual future<> consume_stream_start() = 0;
    // stop_iteration::yes -> consume_end_of_sstable() - skip sstable content
    // sstable parameter is nullptr when merging multiple sstables
    virtual future<stop_iteration> consume_sstable_start(const sstables::sstable* const) = 0;
    // stop_iteration::yes -> consume(partition_end) - skip partition content
    virtual future<stop_iteration> consume(partition_start&&) = 0;
    // stop_iteration::yes -> consume(partition_end) - skip remaining partition content
    virtual future<stop_iteration> consume(static_row&&) = 0;
    // stop_iteration::yes -> consume(partition_end) - skip remaining partition content
    virtual future<stop_iteration> consume(clustering_row&&) = 0;
    // stop_iteration::yes -> consume(partition_end) - skip remaining partition content
    virtual future<stop_iteration> consume(range_tombstone_change&&) = 0;
    // FIXME: implement in the consumers once the sstable format carries token
    // range tombstones. Until the Scylla.db component holds them, an sstable
    // stream never contains one, so this is unreachable.
    virtual future<stop_iteration> consume(token_range_tombstone&&) {
        return make_ready_future<stop_iteration>(stop_iteration::no);
    }
    // stop_iteration::yes -> consume_end_of_sstable() - skip remaining partitions in sstable
    virtual future<stop_iteration> consume(partition_end&&) = 0;
    // stop_iteration::yes -> full stop - skip remaining sstables
    virtual future<stop_iteration> consume_sstable_end() = 0;
    // called at the very end
    virtual future<> consume_stream_end() = 0;
};
