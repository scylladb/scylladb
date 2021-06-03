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
 * Copyright (C) 2015-present ScyllaDB
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

#include <vector>
#include <seastar/core/rwlock.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/util/defer.hh>
#include "utils/atomic_vector.hh"

class keyspace_metadata;
class view_ptr;
class user_type_impl;
using user_type = seastar::shared_ptr<const user_type_impl>;
class schema;
using schema_ptr = seastar::lw_shared_ptr<const schema>;

#include "timestamp.hh"

#include "seastarx.hh"

class mutation;
class schema;

namespace service {

class migration_listener {
public:
    virtual ~migration_listener()
    {}

    // The callback runs inside seastar thread
    virtual void on_create_keyspace(const sstring& ks_name) = 0;
    virtual void on_create_column_family(const sstring& ks_name, const sstring& cf_name) = 0;
    virtual void on_create_user_type(const sstring& ks_name, const sstring& type_name) = 0;
    virtual void on_create_function(const sstring& ks_name, const sstring& function_name) = 0;
    virtual void on_create_aggregate(const sstring& ks_name, const sstring& aggregate_name) = 0;
    virtual void on_create_view(const sstring& ks_name, const sstring& view_name) = 0;

    // The callback runs inside seastar thread
    virtual void on_update_keyspace(const sstring& ks_name) = 0;
    virtual void on_update_column_family(const sstring& ks_name, const sstring& cf_name, bool columns_changed) = 0;
    virtual void on_update_user_type(const sstring& ks_name, const sstring& type_name) = 0;
    virtual void on_update_function(const sstring& ks_name, const sstring& function_name) = 0;
    virtual void on_update_aggregate(const sstring& ks_name, const sstring& aggregate_name) = 0;
    virtual void on_update_view(const sstring& ks_name, const sstring& view_name, bool columns_changed) = 0;

    // The callback runs inside seastar thread
    virtual void on_drop_keyspace(const sstring& ks_name) = 0;
    virtual void on_drop_column_family(const sstring& ks_name, const sstring& cf_name) = 0;
    virtual void on_drop_user_type(const sstring& ks_name, const sstring& type_name) = 0;
    virtual void on_drop_function(const sstring& ks_name, const sstring& function_name) = 0;
    virtual void on_drop_aggregate(const sstring& ks_name, const sstring& aggregate_name) = 0;
    virtual void on_drop_view(const sstring& ks_name, const sstring& view_name) = 0;

    // The callback runs inside seastar thread
    // called before adding/updating/dropping column family. 
    // listener can add additional type altering mutations if he knows what he is doing. 
    virtual void on_before_create_column_family(const schema&, std::vector<mutation>&, api::timestamp_type) {}
    virtual void on_before_update_column_family(const schema& new_schema, const schema& old_schema, std::vector<mutation>&, api::timestamp_type) {}
    virtual void on_before_drop_column_family(const schema&, std::vector<mutation>&, api::timestamp_type) {}

    class only_view_notifications;
    class empty_listener;
};

class migration_listener::only_view_notifications : public migration_listener {
public:
    void on_create_keyspace(const sstring& ks_name) override {}
    void on_create_column_family(const sstring& ks_name, const sstring& cf_name) override {}
    void on_create_user_type(const sstring& ks_name, const sstring& type_name) override {}
    void on_create_function(const sstring& ks_name, const sstring& function_name) override {}
    void on_create_aggregate(const sstring& ks_name, const sstring& aggregate_name) override {}

    void on_update_keyspace(const sstring& ks_name) override {}
    void on_update_column_family(const sstring& ks_name, const sstring& cf_name, bool columns_changed) override {}
    void on_update_user_type(const sstring& ks_name, const sstring& type_name) override {}
    void on_update_function(const sstring& ks_name, const sstring& function_name) override {}
    void on_update_aggregate(const sstring& ks_name, const sstring& aggregate_name) override {}

    void on_drop_keyspace(const sstring& ks_name) override {}
    void on_drop_column_family(const sstring& ks_name, const sstring& cf_name) override {}
    void on_drop_user_type(const sstring& ks_name, const sstring& type_name) override {}
    void on_drop_function(const sstring& ks_name, const sstring& function_name) override {}
    void on_drop_aggregate(const sstring& ks_name, const sstring& aggregate_name) override {}
};

class migration_listener::empty_listener : public only_view_notifications {
public:
    void on_create_view(const sstring& ks_name, const sstring& view_name) override {};
    void on_update_view(const sstring& ks_name, const sstring& view_name, bool columns_changed) override {};
    void on_drop_view(const sstring& ks_name, const sstring& view_name) override {};
};

class migration_notifier {
private:
    atomic_vector<migration_listener*> _listeners;

public:
    /// Register a migration listener on current shard.
    void register_listener(migration_listener* listener);

    /// Unregister a migration listener on current shard.
    future<> unregister_listener(migration_listener* listener);

    future<> create_keyspace(const lw_shared_ptr<keyspace_metadata>& ksm);
    future<> create_column_family(const schema_ptr& cfm);
    future<> create_user_type(const user_type& type);
    future<> create_view(const view_ptr& view);
    future<> update_keyspace(const lw_shared_ptr<keyspace_metadata>& ksm);
    future<> update_column_family(const schema_ptr& cfm, bool columns_changed);
    future<> update_user_type(const user_type& type);
    future<> update_view(const view_ptr& view, bool columns_changed);
    future<> drop_keyspace(const sstring& ks_name);
    future<> drop_column_family(const schema_ptr& cfm);
    future<> drop_user_type(const user_type& type);
    future<> drop_view(const view_ptr& view);

    void before_create_column_family(const schema&, std::vector<mutation>&, api::timestamp_type);
    void before_update_column_family(const schema& new_schema, const schema& old_schema, std::vector<mutation>&, api::timestamp_type);
    void before_drop_column_family(const schema&, std::vector<mutation>&, api::timestamp_type);
};

}
