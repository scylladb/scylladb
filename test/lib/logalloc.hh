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

inline ::logalloc::tracker::config default_config() {
    return ::logalloc::tracker::config(128'000'000, 0);
}

// Must live in a seastar thread
class sharded_tracker {
    sharded<::logalloc::tracker> _tracker;
public:
    explicit sharded_tracker(::logalloc::tracker::config cfg) {
        _tracker.start(cfg).get();
    }
    sharded_tracker()
        : sharded_tracker(default_config()) {
    }
    ~sharded_tracker() {
        _tracker.stop().get();
    }

    sharded<::logalloc::tracker>& get() {
        return _tracker;
    }

    ::logalloc::tracker& operator*() {
        return _tracker.local();
    }

    ::logalloc::tracker* operator->() {
        return &_tracker.local();
    }
};

}
