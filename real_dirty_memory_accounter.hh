/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "replica/memtable.hh"
#include "row_cache.hh"
#include "replica/dirty_memory_manager.hh"

// # Flush control overview
//
// It is good for memtables to grow as big as possible before they are flushed
// to disk, because this reduces the number of sstables, which in turn reduces
// the number of disk reads on queries and/or reduces the necessary compaction
// work.
//
// On the other hand, the flush should start early enough to leave enough space
// in RAM for new data incoming during the flush. Otherwise RAM could be
// overfilled and the database would have to be throttled until some space is
// freed.
//
// It is good for a memtable flush to happen as slow as possible, because it's
// a non-interactive task, which should take as little resources from
// interactive tasks as possible.
//
// On the other hand, the flush has to progress fast enough to keep up with
// incoming writes, otherwise the size of data in RAM will keep growing
// until RAM is overfilled.
//
// The balance of the above is kept by dirty_memory_manager and flush_controller.
// They attempt to make flushes as delayed and slow as possible without risking
// an OOM situation.
//
// # Flush control implementation
//
// Flush delay and speed is based on some formulas involving total available RAM,
// total ("real") memtable memory and "unspooled" (not flushed to disk yet) memtable
// memory. See dirty_memory_manager for details.
//
// ("Dirty" is a term borrowed from general caching terminology, which usually means
// cache entries which were modified and have to be written back before being discarded.
// In Scylla, "dirty memory" simply means "memory taken up by memtable data").
//
// (In some context the implementation might find it more natural to talk about
// "spooled" memory instead of "unspooled". "Spooled" is just the difference between
// "real" and "unspooled").
//
// Every memtable is kept in its own LSA region which tracks exact (the allocator is
// the source of truth about memory usage) "real" changes for that memtable.
// While the memtable is active, its "unspooled" is (obviously) equal to its "real".
//
// Once flush starts, remaining "unspooled" is tracked by flush_memory_accounter.
// As flush_reader reads a memtable and passes its data to an sstable writer,
// it asks flush_memory_accounter to decrement "unspooled" memory counters.
//
// The accounting by flush_memory_accounter is not exact (for example it
// doesn't account the memtable tree nodes, only the contents stored there) but
// it doesn't have to. It is only used by flush control formulas, so it only
// has to be accurate enough for the heurisitcs to work.
//
// When memtable flush is finished, the amount of "unspooled" memory is corrected
// from its inexact value to now-exact 0.
//
// After a memtable flush finishes, the memtable has to be merged into the
// cache, to update or invalidate existing cache entries, so combined RAM data
// stays up to date with sstables after the memtable disappears.
//
// During this merge, "real" decreases, and this has to be accounted for.
// Unfortunately we can't rely on LSA's counters (which are the source of
// truth for memory accounting) for "real", because the merge
// requires the memtable LSA region to be merged into the cache LSA region.
// As soon as the merge starts, all memtable data is considered a part of the cache
// by the LSA.
//
// So, similarly as with flush_memory_accounter earlier, we have real_dirty_memory_accounter
// whose job is to incrementally decrease "real" as data is merged into the cache.
// This might also be slightly inexact.
//
// As row_cache::update() progresses, it estimates the amount of processed memory
// as well as it can, and asks real_dirty_memory_accounter to "unpin" it from "real".
//
// Once the merge finishes, the "real" for the merged memtable is reduced from
// its slightly inexact value to now-exact 0.
//
class real_dirty_memory_accounter {
    replica::dirty_memory_manager& _mgr;
    cache_tracker& _tracker;
    uint64_t _bytes;
    uint64_t _uncommitted = 0;
public:
    real_dirty_memory_accounter(replica::dirty_memory_manager& mgr, cache_tracker& tracker, size_t size);
    real_dirty_memory_accounter(replica::memtable& m, cache_tracker& tracker);
    ~real_dirty_memory_accounter();
    real_dirty_memory_accounter(real_dirty_memory_accounter&& c);
    real_dirty_memory_accounter(const real_dirty_memory_accounter& c) = delete;
    // Needs commit() to take effect, or when this object is destroyed.
    void unpin_memory(uint64_t bytes) { _uncommitted += bytes; }
    void commit();
};

inline
real_dirty_memory_accounter::real_dirty_memory_accounter(replica::dirty_memory_manager& mgr, cache_tracker& tracker, size_t size)
    : _mgr(mgr)
    , _tracker(tracker)
    , _bytes(size) {
    _mgr.pin_real_dirty_memory(_bytes);
}

inline
real_dirty_memory_accounter::real_dirty_memory_accounter(replica::memtable& m, cache_tracker& tracker)
    : real_dirty_memory_accounter(m.get_dirty_memory_manager(), tracker, m.occupancy().used_space())
{ }

inline
real_dirty_memory_accounter::~real_dirty_memory_accounter() {
    _mgr.unpin_real_dirty_memory(_bytes);
}

inline
real_dirty_memory_accounter::real_dirty_memory_accounter(real_dirty_memory_accounter&& c)
    : _mgr(c._mgr), _tracker(c._tracker), _bytes(c._bytes), _uncommitted(c._uncommitted) {
    c._bytes = 0;
    c._uncommitted = 0;
}

inline
void real_dirty_memory_accounter::commit() {
    auto bytes = std::exchange(_uncommitted, 0);
    // this should never happen - if it does it is a bug. But we'll try to recover and log
    // instead of asserting. Once it happens, though, it can keep happening until the update is
    // done. So using metrics is better-suited than printing to the logs
    if (bytes > _bytes) {
        _tracker.pinned_dirty_memory_overload(bytes - _bytes);
    }
    auto delta = std::min(bytes, _bytes);
    _bytes -= delta;
    _mgr.unpin_real_dirty_memory(delta);
}
