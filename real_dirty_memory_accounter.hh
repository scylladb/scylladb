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

#include "memtable.hh"
#include "row_cache.hh"
#include "dirty_memory_manager.hh"

// makes sure that cache update handles real dirty memory correctly.
class real_dirty_memory_accounter {
    dirty_memory_manager& _mgr;
    cache_tracker& _tracker;
    uint64_t _bytes;
public:
    real_dirty_memory_accounter(dirty_memory_manager& mgr, cache_tracker& tracker, size_t size);
    real_dirty_memory_accounter(memtable& m, cache_tracker& tracker);
    ~real_dirty_memory_accounter();
    real_dirty_memory_accounter(real_dirty_memory_accounter&& c);
    real_dirty_memory_accounter(const real_dirty_memory_accounter& c) = delete;
    void unpin_memory(uint64_t bytes);
};

inline
real_dirty_memory_accounter::real_dirty_memory_accounter(dirty_memory_manager& mgr, cache_tracker& tracker, size_t size)
    : _mgr(mgr)
    , _tracker(tracker)
    , _bytes(size) {
    _mgr.pin_real_dirty_memory(_bytes);
}

inline
real_dirty_memory_accounter::real_dirty_memory_accounter(memtable& m, cache_tracker& tracker)
    : real_dirty_memory_accounter(m.get_dirty_memory_manager(), tracker, m.occupancy().used_space())
{ }

inline
real_dirty_memory_accounter::~real_dirty_memory_accounter() {
    _mgr.unpin_real_dirty_memory(_bytes);
}

inline
real_dirty_memory_accounter::real_dirty_memory_accounter(real_dirty_memory_accounter&& c)
    : _mgr(c._mgr), _tracker(c._tracker), _bytes(c._bytes) {
    c._bytes = 0;
}

inline
void real_dirty_memory_accounter::unpin_memory(uint64_t bytes) {
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
