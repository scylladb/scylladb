/*
 * Copyright 2021-present ScyllaDB
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
 * You should have received a copy of the GNU Affero General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "seastarx.hh"
#include <seastar/core/sharded.hh>
#include <seastar/core/abort_source.hh>
#include <seastar/core/semaphore.hh>

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
    replica::database& _db;
    service::storage_proxy& _proxy;
    // _end is set by start(), and resolves when the the background service
    // started by it ends. To ask the background service to end, _abort_source
    // should be triggered. stop() below uses both _abort_source and _end.
    std::optional<future<>> _end;
    abort_source _abort_source;
    // Ensures that at most 1 page of scan results at a time is processed by the TTL service
    named_semaphore _page_sem{1, named_semaphore_exception_factory{"alternator_ttl"}};
    bool shutting_down() { return _abort_source.abort_requested(); }
public:
    // sharded_service<expiration_service>::start() creates this object on
    // all shards, so calls this constructor on each shard. Later, the
    // additional start() function should be invoked on all shards.
    expiration_service(replica::database&, service::storage_proxy&);
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
