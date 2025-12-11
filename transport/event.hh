/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#pragma once

#include "gms/inet_address.hh"

#include <seastar/core/sstring.hh>
#include <seastar/net/api.hh>
#include "service/client_routes.hh"

namespace cql_transport {

class event {
public:
    enum class event_type { TOPOLOGY_CHANGE, STATUS_CHANGE, SCHEMA_CHANGE, CLIENT_ROUTES_CHANGE };

    const event_type type;
private:
    event(const event_type& type_);
public:
    class topology_change;
    class status_change;
    class schema_change;
    class client_routes_change;
};

class event::topology_change : public event {
public:
    enum class change_type { NEW_NODE, REMOVED_NODE, MOVED_NODE };

    const change_type change;
    const socket_address node;

    topology_change(change_type change, const socket_address& node);

    static topology_change new_node(const gms::inet_address& host, uint16_t port);

    static topology_change removed_node(const gms::inet_address& host, uint16_t port);
};

class event::status_change : public event {
public:
    enum class status_type { UP, DOWN };

    const status_type status;
    const socket_address node;

    status_change(status_type status, const socket_address& node);

    static status_change node_up(const gms::inet_address& host, uint16_t port);

    static status_change node_down(const gms::inet_address& host, uint16_t port);
};

class event::schema_change : public event {
public:
    enum class change_type { CREATED, UPDATED, DROPPED };
    enum class target_type { KEYSPACE, TABLE, TYPE, FUNCTION, AGGREGATE };

    const change_type change;
    const target_type target;

    // Every target is followed by at least a keyspace.
    const sstring keyspace;

    // Target types other than keyspace have a list of arguments.
    const std::vector<sstring> arguments;

    schema_change(change_type change, target_type target, sstring keyspace, std::vector<sstring> arguments);

    template <typename... Ts>
    schema_change(change_type change, target_type target, sstring keyspace, Ts... arguments)
        : schema_change(change, target, keyspace, std::vector<sstring>{std::move(arguments)...}) {}
};

class event::client_routes_change : public event {
public:
    enum class change_type { UPDATE_NODES };

    const change_type change;
private:
    const std::set<service::client_routes_service::client_route_key> client_route_keys;
public:
    client_routes_change(const std::set<service::client_routes_service::client_route_key>& client_route_keys)
        : event(event_type::CLIENT_ROUTES_CHANGE)
        , change(change_type::UPDATE_NODES)
        , client_route_keys(client_route_keys)
    { }

    std::vector<sstring> get_connection_ids() const {
        std::vector<sstring> v;
        v.reserve(client_route_keys.size());
        for (const auto& k : client_route_keys) {
            v.emplace_back(k.connection_id);
        }
        return v;
    }

    std::vector<sstring> get_host_ids() const {
        std::vector<sstring> v;
        v.reserve(client_route_keys.size());
        for (const auto& k : client_route_keys) {
            v.emplace_back(fmt::to_string(k.host_id));
        }
        return v;
    }
};

}
