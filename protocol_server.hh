/*
 * Copyright (C) 2021-present ScyllaDB
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

#include "seastarx.hh"
#include <seastar/core/future.hh>
#include <vector>

// Abstraction for a server serving some kind of user-facing protocol.
class protocol_server {
public:
    virtual ~protocol_server() = default;
    /// Name of the server, can be different or the same as than the protocol it serves.
    virtual sstring name() const = 0;
    /// Name of the protocol served.
    virtual sstring protocol() const = 0;
    /// Version of the protocol served (if any -- not all protocols are versioned).
    virtual sstring protocol_version() const = 0;
    /// Addresses the server is listening on, should be empty when server is not running.
    virtual std::vector<socket_address> listen_addresses() const = 0;
    /// Check if the server is running.
    virtual bool is_server_running() const = 0;
    /// Start the server.
    /// Can be called multiple times, in any state of the server.
    virtual future<> start_server() = 0;
    /// Stop the server.
    /// Can be called multiple times, in any state of the server.
    /// This variant is used on shutdown and therefore it must succeed.
    virtual future<> stop_server() = 0;
    /// Stop the server. Weaker variant of \ref stop_server().
    /// Can be called multiple times, in any state of the server.
    /// This variant is used by the REST API so failure is acceptable.
    virtual future<> request_stop_server() = 0;
};
