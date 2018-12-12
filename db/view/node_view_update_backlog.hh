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

#include "db/view/view_update_backlog.hh"

#include <seastar/core/cacheline.hh>
#include <seastar/core/lowres_clock.hh>

#include <atomic>
#include <chrono>
#include <new>

namespace db::view {

/**
 * An atomic view update backlog representation, safe to update from multiple shards.
 * It is legal for a stale current max value to be returned.
 */
class node_update_backlog {
    using clock = seastar::lowres_clock;
    struct per_shard_backlog {
        // Multiply by 2 to defeat the prefetcher
        alignas(seastar::cache_line_size * 2) std::atomic<update_backlog> backlog = update_backlog::no_backlog();

        update_backlog load() const {
            return backlog.load(std::memory_order_relaxed);
        }
    };
    std::vector<per_shard_backlog> _backlogs;
    std::chrono::milliseconds _interval;
    std::atomic<clock::time_point> _last_update;
    std::atomic<update_backlog> _max;

public:
    explicit node_update_backlog(size_t shards, std::chrono::milliseconds interval)
            : _backlogs(shards)
            , _interval(interval)
            , _last_update(clock::now() - _interval)
            , _max(update_backlog::no_backlog()) {
    }

    update_backlog add_fetch(unsigned shard, update_backlog backlog);

    // Exposed for testing only.
    update_backlog load() const {
        return _max.load(std::memory_order_relaxed);
    }
};

}
