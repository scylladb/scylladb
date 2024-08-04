/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "utils/assert.hh"
#include "transport/event.hh"

namespace cql_transport {

event::event(const event_type& type_)
    : type{type_}
{ }

event::topology_change::topology_change(change_type change, const socket_address& node)
    : event{event_type::TOPOLOGY_CHANGE}
    , change{change}
    , node{node}
{ }

event::topology_change event::topology_change::new_node(const gms::inet_address& host, uint16_t port)
{
    return topology_change{change_type::NEW_NODE, socket_address{host, port}};
}

event::topology_change event::topology_change::removed_node(const gms::inet_address& host, uint16_t port)
{
    return topology_change{change_type::REMOVED_NODE, socket_address{host, port}};
}

event::status_change::status_change(status_type status, const socket_address& node)
    : event{event_type::STATUS_CHANGE}
    , status{status}
    , node{node}
{ }

event::status_change event::status_change::node_up(const gms::inet_address& host, uint16_t port)
{
    return status_change{status_type::UP, socket_address{host, port}};
}

event::status_change event::status_change::node_down(const gms::inet_address& host, uint16_t port)
{
    return status_change{status_type::DOWN, socket_address{host, port}};
}

event::schema_change::schema_change(change_type change, target_type target, sstring keyspace, std::vector<sstring> arguments)
    : event(event_type::SCHEMA_CHANGE)
    , change(change)
    , target(target)
    , keyspace(std::move(keyspace))
    , arguments(std::move(arguments))
{
    switch (target) {
    case event::schema_change::target_type::KEYSPACE:
        SCYLLA_ASSERT(this->arguments.empty());
        break;
    case event::schema_change::target_type::TYPE:
    case event::schema_change::target_type::TABLE:
        // just the name
        SCYLLA_ASSERT(this->arguments.size() == 1);
        break;
    case event::schema_change::target_type::FUNCTION:
    case event::schema_change::target_type::AGGREGATE:
        // at least the name
        SCYLLA_ASSERT(this->arguments.size() >= 1);
        break;
    }
}
}
