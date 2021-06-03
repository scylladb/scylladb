/*
 * Copyright (C) 2017-present ScyllaDB
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

#include "utils/allocation_strategy.hh"

class failure_injecting_allocation_strategy : public allocation_strategy {
    allocation_strategy& _delegate;
    uint64_t _alloc_count;
    uint64_t _fail_at = std::numeric_limits<uint64_t>::max();
public:
    failure_injecting_allocation_strategy(allocation_strategy& delegate) : _delegate(delegate) {}

    virtual void* alloc(migrate_fn mf, size_t size, size_t alignment) override {
        if (_alloc_count >= _fail_at) {
            stop_failing();
            throw std::bad_alloc();
        }
        ++_alloc_count;
        return _delegate.alloc(mf, size, alignment);
    }

    virtual void free(void* ptr, size_t size) override {
        _delegate.free(ptr, size);
    }

    virtual void free(void* ptr) override {
        _delegate.free(ptr);
    }

    virtual size_t object_memory_size_in_allocator(const void* obj) const noexcept override {
        return _delegate.object_memory_size_in_allocator(obj);
    }

    // Counts allocation attempts which are not failed due to fail_at().
    uint64_t alloc_count() const {
        return _alloc_count;
    }

    void fail_after(uint64_t count) {
        _fail_at = _alloc_count + count;
    }

    void stop_failing() {
        _fail_at = std::numeric_limits<uint64_t>::max();
    }
};
