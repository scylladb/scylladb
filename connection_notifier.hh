/*
 * Copyright (C) 2019 ScyllaDB
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

#include "gms/inet_address.hh"
#include <seastar/core/sstring.hh>
#include <optional>

enum class client_type {
    cql = 0,
    thrift,
    alternator,
};

// Representation of a row in `system.clients'. std::optionals are for nullable cells.
struct client_data {
    gms::inet_address ip;
    int32_t port;
    client_type ct;
    int32_t shard_id;  /// ID of server-side shard which is processing the connection.

    // `optional' column means that it's nullable (possibly because it's
    // unimplemented yet). If you want to fill ("implement") any of them,
    // remember to update the query in `notify_new_client()'.
    std::optional<sstring> connection_stage;
    std::optional<sstring> driver_name;
    std::optional<sstring> driver_version;
    std::optional<sstring> hostname;
    std::optional<int32_t> protocol_version;
    std::optional<sstring> ssl_cipher_suite;
    std::optional<bool> ssl_enabled;
    std::optional<sstring> ssl_protocol;
    std::optional<sstring> username;
};

future<> notify_new_client(client_data cd);
future<> notify_disconnected_client(gms::inet_address addr, client_type ct, int port);

future<> clear_clientlist();
