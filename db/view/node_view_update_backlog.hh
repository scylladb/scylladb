/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
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
