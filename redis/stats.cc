/*
 * Copyright (C) 2019 pengjian.uestc @ gmail.com
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "redis/stats.hh"

#include <seastar/core/metrics.hh>

namespace redis {

const char* REDIS_METRICS = "redis";

stats::stats() : api_operations{} {
    seastar::metrics::label op("op");
    _metrics.add_group("redis", {
#define OPERATION(name, CamelCaseName) \
            seastar::metrics::make_total_operations("operation", api_operations.name._counter, \
                seastar::metrics::description("number of operations via redis API"), {op(CamelCaseName)}), \
            seastar::metrics::make_histogram("op_latency", \
                seastar::metrics::description("Latency histogram of an operation via redis API"), {op(CamelCaseName)}, [this]{return api_operations.name._latency.get_histogram(1,20);}),
            OPERATION(_get, "get")
            OPERATION(_set, "set")
            OPERATION(_del, "del")
            OPERATION(_ping, "ping")
            OPERATION(_select, "select")
    });
    _metrics.add_group("redis", {
        seastar::metrics::make_counter("redis-connections", _connects,
            seastar::metrics::description("Counts a number of client connections.")),
        seastar::metrics::make_gauge("current_connections", _connections,
            seastar::metrics::description("Holds a current number of client connections.")),
        seastar::metrics::make_counter("requests_served", _requests_served,
            seastar::metrics::description("Counts a number of served requests.")),
        seastar::metrics::make_gauge("requests_serving", _requests_serving,
            seastar::metrics::description("Holds a number of requests that are being processed right now.")),
        seastar::metrics::make_histogram("requests_latency", seastar::metrics::description("The general requests latency histogram"), [this]{ return _estimated_requests_latency.get_histogram(16, 20);}),
    });
}

}
