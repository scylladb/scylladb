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

#pragma once

#include "gms/inet_address.hh"

#include <seastar/core/sstring.hh>
#include <seastar/net/api.hh>

#include <experimental/optional>
#include <stdexcept>

namespace cql_transport {

class event {
public:
    enum class event_type { TOPOLOGY_CHANGE, STATUS_CHANGE, SCHEMA_CHANGE };

    const event_type type;
private:
    event(const event_type& type_);
public:
    class topology_change;
    class status_change;
    class schema_change;
};

class event::topology_change : public event {
public:
    enum class change_type { NEW_NODE, REMOVED_NODE, MOVED_NODE };

    const change_type change;
    const ipv4_addr node;

    topology_change(change_type change, const ipv4_addr& node);

    static topology_change new_node(const gms::inet_address& host, uint16_t port);

    static topology_change removed_node(const gms::inet_address& host, uint16_t port);
};

class event::status_change : public event {
public:
    enum class status_type { UP, DOWN };

    const status_type status;
    const ipv4_addr node;

    status_change(status_type status, const ipv4_addr& node);

    static status_change node_up(const gms::inet_address& host, uint16_t port);

    static status_change node_down(const gms::inet_address& host, uint16_t port);
};

class event::schema_change : public event {
public:
    enum class change_type { CREATED, UPDATED, DROPPED };
    enum class target_type { KEYSPACE, TABLE, TYPE };

    const change_type change;
    const target_type target;
    const sstring keyspace;
    const std::experimental::optional<sstring> table_or_type_or_function;

    schema_change(const change_type change_, const target_type target_, const sstring& keyspace_, const std::experimental::optional<sstring>& table_or_type_or_function_);

    schema_change(const change_type change_, const sstring keyspace_);
};

}
