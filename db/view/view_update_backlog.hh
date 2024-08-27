/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
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
    // calculate_view_update_throttling_delay()::delay_limit_us. Above the threshold,
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

// View updates are asynchronous, and because of this limiting their concurrency requires
// a special approach. The current algorithm places all of the pending view updates in the backlog
// and artificially slows down new responses to coordinator requests based on how full the backlog is.
// This function calculates how much a request should be slowed down based on the backlog's fullness.
// The equation is basically: delay(in seconds) = view_fullness_ratio^3
// The more full the backlog gets the more aggressively the requests are slowed down.
// The delay is limited to the amount of time left until timeout.
// After the timeout the request fails, so there's no point in waiting longer than that.
// The second argument defines this timeout point - we can't delay the request more than this time point.
// See: https://www.scylladb.com/2018/12/04/worry-free-ingestion-flow-control/
std::chrono::microseconds calculate_view_update_throttling_delay(
    update_backlog backlog,
    db::timeout_clock::time_point timeout);
}
