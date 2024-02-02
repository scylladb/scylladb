/*
 * Copyright (C) 2019 pengjian.uestc @ gmail.com
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <cstdint>

#include <seastar/core/metrics_registration.hh>
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
