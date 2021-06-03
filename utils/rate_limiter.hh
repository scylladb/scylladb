/*
 * Copyright (C) 2015-present ScyllaDB
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

#include <seastar/core/timer.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/seastar.hh>
#include "seastarx.hh"

namespace utils {

/**
 * 100% naive rate limiter. Consider it a placeholder
 * Will let you process X "units" per second, then reset this every s.
 * Obviously, accuracy is virtually non-existant and steady rate will fluctuate.
 */
class rate_limiter {
private:
    timer<lowres_clock> _timer;
    size_t _units_per_s;
    semaphore _sem {0};

    void on_timer();
public:
    rate_limiter(size_t rate);
    future<> reserve(size_t u);
};

}
