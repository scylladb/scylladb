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

#include <cstdint>
#include <seastar/core/semaphore.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/future.hh>
#include "seastarx.hh"
#include <unordered_set>
#include <boost/filesystem.hpp>
#include <gms/inet_address.hh>

namespace db {
namespace hints {

class resource_manager {
    const size_t _max_send_in_flight_memory;
    const size_t _min_send_hint_budget;
    seastar::semaphore _send_limiter;

    uint64_t _size_of_hints_in_progress = 0;

public:
    static constexpr uint64_t max_size_of_hints_in_progress = 10 * 1024 * 1024; // 10MB
    static constexpr size_t hint_segment_size_in_mb = 32;
    static constexpr size_t max_hints_per_ep_size_mb = 128; // 4 files 32MB each
    static constexpr size_t max_hints_send_queue_length = 128;
    static size_t max_shard_disk_space_size;

public:
    resource_manager()
        : _max_send_in_flight_memory(std::max(memory::stats().total_memory() / 10, max_hints_send_queue_length))
        , _min_send_hint_budget(_max_send_in_flight_memory / max_hints_send_queue_length)
        , _send_limiter(_max_send_in_flight_memory)
    {}

    future<semaphore_units<semaphore_default_exception_factory>> get_send_units_for(size_t buf_size);

    bool too_many_hints_in_progress() const {
        return _size_of_hints_in_progress > max_size_of_hints_in_progress;
    }

    uint64_t size_of_hints_in_progress() const {
        return _size_of_hints_in_progress;
    }

    inline void inc_size_of_hints_in_progress(int64_t delta) {
        _size_of_hints_in_progress += delta;
    }

    inline void dec_size_of_hints_in_progress(int64_t delta) {
        _size_of_hints_in_progress -= delta;
    }
};

}
}
