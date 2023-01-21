/*
 * Copyright 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "seastarx.hh"
#include <seastar/core/sharded.hh>
#include <seastar/core/abort_source.hh>
#include <seastar/core/semaphore.hh>
#include "data_dictionary/data_dictionary.hh"

namespace gms {
class gossiper;
}

namespace replica {
class database;
}

namespace service {
    class storage_proxy;
}

namespace alternator {

// expiration_service is a sharded service responsible for cleaning up expired
// items in all tables with per-item expiration enabled. Currently, this means
// Alternator tables with TTL configured via a UpdateTimeToLeave request.
class expiration_service final : public seastar::peering_sharded_service<expiration_service> {
public:
    // Object holding per-shard statistics related to the expiration service.
    // While this object is alive, these metrics are also registered to be
    // visible by the metrics REST API, with the "expiration_" prefix.
    class stats {
    public:
        stats();
        uint64_t scan_passes = 0;
        uint64_t scan_table = 0;
        uint64_t items_deleted = 0;
        uint64_t secondary_ranges_scanned = 0;
    private:
        // The metric_groups object holds this stat object's metrics registered
        // as long as the stats object is alive.
        seastar::metrics::metric_groups _metrics;
    };
private:
    data_dictionary::database _db;
    service::storage_proxy& _proxy;
    gms::gossiper& _gossiper;
    // _end is set by start(), and resolves when the the background service
    // started by it ends. To ask the background service to end, _abort_source
    // should be triggered. stop() below uses both _abort_source and _end.
    std::optional<future<>> _end;
    abort_source _abort_source;
    // Ensures that at most 1 page of scan results at a time is processed by the TTL service
    named_semaphore _page_sem{1, named_semaphore_exception_factory{"alternator_ttl"}};
    bool shutting_down() { return _abort_source.abort_requested(); }
    stats _expiration_stats;
public:
    // sharded_service<expiration_service>::start() creates this object on
    // all shards, so calls this constructor on each shard. Later, the
    // additional start() function should be invoked on all shards.
    expiration_service(data_dictionary::database, service::storage_proxy&, gms::gossiper&);
    future<> start();
    future<> run();
    // sharded_service<expiration_service>::stop() calls the following stop()
    // method on each shard. This stop() asks the service on this shard to
    // shut down as quickly as it can. The returned future indicates when the
    // service is no longer running.
    // stop() may be called even before start(), but may only be called once -
    // calling it twice will result in an exception.
    future<> stop();
};

} // namespace alternator
