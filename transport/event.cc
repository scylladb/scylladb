/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright (C) 2015 ScyllaDB
 *
 * Modified by ScyllaDB
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

#include "transport/event.hh"

namespace cql_transport {

event::event(const event_type& type_)
    : type{type_}
{ }

event::topology_change::topology_change(change_type change, const ipv4_addr& node)
    : event{event_type::TOPOLOGY_CHANGE}
    , change{change}
    , node{node}
{ }

event::topology_change event::topology_change::new_node(const gms::inet_address& host, uint16_t port)
{
    return topology_change{change_type::NEW_NODE, ipv4_addr{host.raw_addr(), port}};
}

event::topology_change event::topology_change::removed_node(const gms::inet_address& host, uint16_t port)
{
    return topology_change{change_type::REMOVED_NODE, ipv4_addr{host.raw_addr(), port}};
}

event::status_change::status_change(status_type status, const ipv4_addr& node)
    : event{event_type::STATUS_CHANGE}
    , status{status}
    , node{node}
{ }

event::status_change event::status_change::node_up(const gms::inet_address& host, uint16_t port)
{
    return status_change{status_type::UP, ipv4_addr{host.raw_addr(), port}};
}

event::status_change event::status_change::node_down(const gms::inet_address& host, uint16_t port)
{
    return status_change{status_type::DOWN, ipv4_addr{host.raw_addr(), port}};
}

event::schema_change::schema_change(const change_type change_, const target_type target_, const sstring& keyspace_, const std::experimental::optional<sstring>& table_or_type_or_function_)
    : event{event_type::SCHEMA_CHANGE}
    , change{change_}
    , target{target_}
    , keyspace{keyspace_}
    , table_or_type_or_function{table_or_type_or_function_}
{
    if (target != target_type::KEYSPACE) {
        if (!table_or_type_or_function_) {
            throw std::invalid_argument("Table or type should be set for non-keyspace schema change events");
        }
    }
}

event::schema_change::schema_change(const change_type change_, const sstring keyspace_)
    : schema_change{change_, target_type::KEYSPACE, keyspace_, std::experimental::optional<sstring>{}}
{ }

}
