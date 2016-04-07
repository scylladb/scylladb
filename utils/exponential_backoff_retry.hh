
/*
 * Copyright (C) 2015 ScyllaDB
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

#include "core/sleep.hh"
#include <chrono>

// Implements retry policy that exponentially increases sleep time between retries.
class exponential_backoff_retry {
    std::chrono::milliseconds _base_sleep_time;
    std::chrono::milliseconds _sleep_time;
    std::chrono::milliseconds _max_sleep_time;
public:
    exponential_backoff_retry(std::chrono::milliseconds base_sleep_time, std::chrono::milliseconds max_sleep_time)
        : _base_sleep_time(std::min(base_sleep_time, max_sleep_time))
        , _sleep_time(_base_sleep_time)
        , _max_sleep_time(max_sleep_time) {}

    future<> retry() {
        auto old_sleep_time = _sleep_time;
        // calculating sleep time seconds for the next retry.
        _sleep_time = std::min(_sleep_time * 2, _max_sleep_time);

        return sleep(old_sleep_time);
    }

    // Return sleep time in seconds to be used for next retry.
    std::chrono::milliseconds sleep_time() const {
        return _sleep_time;
    }

    // Reset sleep time to base sleep time.
    void reset() {
        _sleep_time = _base_sleep_time;
    }
};
