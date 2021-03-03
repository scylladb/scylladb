/*
 * Copyright (C) 2019 pengjian.uestc @ gmail.com
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

#include <seastar/core/metrics_registration.hh>
#include "seastarx.hh"
#include "utils/estimated_histogram.hh"
#include "utils/histogram.hh"

namespace redis {

class stats {
public:
    stats();
    struct counter_type {
        uint64_t _counter = 0;
        utils::estimated_histogram _latency;
    };

    struct {
        counter_type _get;
        counter_type _set;
        counter_type _del;
        counter_type _ping;
        counter_type _select;
    } api_operations;
    
    uint64_t _connects = 0;
    uint64_t _connections = 0;
    uint64_t _requests_served = 0;
    uint64_t _requests_serving = 0;
    utils::estimated_histogram _estimated_requests_latency;
    utils::timed_rate_moving_average_and_histogram _requests;
private:
    seastar::metrics::metric_groups _metrics;
};

}
