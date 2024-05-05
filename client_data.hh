/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#pragma once

#include <seastar/net/inet_address.hh>
#include <seastar/core/sstring.hh>
#include "seastarx.hh"

#include <optional>

enum class client_type {
    cql = 0,
    thrift,
    alternator,
};

sstring to_string(client_type ct);

enum class client_connection_stage {
    established = 0,
    authenticating,
    ready,
};

sstring to_string(client_connection_stage ct);

// Representation of a row in `system.clients'. std::optionals are for nullable cells.
struct client_data {
    net::inet_address ip;
    int32_t port;
    client_type ct = client_type::cql;
    client_connection_stage connection_stage = client_connection_stage::established;
    int32_t shard_id;  /// ID of server-side shard which is processing the connection.

    std::optional<sstring> driver_name;
    std::optional<sstring> driver_version;
    std::optional<sstring> hostname;
    std::optional<int32_t> protocol_version;
    std::optional<sstring> ssl_cipher_suite;
    std::optional<bool> ssl_enabled;
    std::optional<sstring> ssl_protocol;
    std::optional<sstring> username;

    sstring stage_str() const { return to_string(connection_stage); }
    sstring client_type_str() const { return to_string(ct); }
};
