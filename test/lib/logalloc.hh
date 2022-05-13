/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "utils/logalloc.hh"

#include <seastar/core/sharded.hh>

namespace tests::logalloc {

// Must live in a seastar thread
class sharded_tracker {
    bool _need_stop = false;
public:
    explicit sharded_tracker(uint64_t available_memory, uint64_t min_free_memory, std::optional<::logalloc::tracker::config> cfg = {}) {
        ::logalloc::prime_segment_pool(available_memory, min_free_memory).get();
        if (cfg) {
            ::logalloc::shard_tracker().configure(*cfg);
            _need_stop = true;
        }
    }
    explicit sharded_tracker(::logalloc::tracker::config cfg)
        : sharded_tracker(memory::stats().total_memory(), memory::min_free_memory(), std::move(cfg)) {
    }
    sharded_tracker()
        : sharded_tracker(memory::stats().total_memory(), memory::min_free_memory(), {}) {
    }
    ~sharded_tracker() {
        if (_need_stop) {
            ::logalloc::shard_tracker().stop().get();
        }
    }

    ::logalloc::tracker& operator*() {
        return ::logalloc::shard_tracker();
    }

    ::logalloc::tracker* operator->() {
        return &::logalloc::shard_tracker();
    }
};

}
