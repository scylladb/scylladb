/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <compare>
#include <cstddef>
#include <limits>
#include <chrono>
#include "db/timeout_clock.hh"

namespace db::view {

/**
 * The view update backlog represents the pending view data that a base replica
 * maintains. It is the maximum of the memory backlog - how much memory pending
 * view updates are consuming out of the their allocated quota - and the disk
 * backlog - how much view hints are consuming. The size of a backlog is relative
 * to its maximum size.
 */
class update_backlog {
    static constexpr float admission_control_threshold = 0.8;
    size_t _current;
    size_t _max;

public:
    update_backlog(size_t current, size_t max)
            : _current(current), _max(max) {
        if (max == 0) {
            // We might have received an invalid backlog in a message from an old node,
            // where we didn't check the max. In this case, fall back to the empty backlog.
            _current = 0;
            _max = std::numeric_limits<size_t>::max();
        }
    }
    update_backlog() = delete;

    // Returns the number of bytes in the backlog divided by the maximum number of bytes
    // that the backlog can hold before employing admission control. While the backlog
    // is below the threshold, the coordinator will slow down the view updates up to
    // node_update_backlog::calculate_throttling_delay()::delay_limit_us. Above the threshold,
    // the coordinator will reject the writes that would increase the backlog. On the
    // replica, the writes will start failing only after reaching the hard limit '_max'.
    float relative_size() const {
        return float(_current) / float(_max) / admission_control_threshold;
    }

    const size_t& get_current_bytes() const {
        return _current;
    }

    const size_t& get_max_bytes() const {
        return _max;
    }

    std::partial_ordering operator<=>(const update_backlog &rhs) const {
        return relative_size() <=> rhs.relative_size();
    }
    bool operator==(const update_backlog& rhs) const {
        return relative_size() == rhs.relative_size();
    }

    static update_backlog no_backlog() {
        return update_backlog{0, std::numeric_limits<size_t>::max()};
    }
};

}
