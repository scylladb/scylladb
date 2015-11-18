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
 * Copyright 2015 Cloudius Systems
 *
 * Modified by Cloudius Systems
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

#include "service/migration_listener.hh"
#include "gms/endpoint_state.hh"
#include "db/schema_tables.hh"
#include "core/distributed.hh"
#include "gms/inet_address.hh"
#include "utils/UUID.hh"

#include <vector>

namespace service {

class migration_manager {
    std::vector<migration_listener*> _listeners;

    static const std::chrono::milliseconds migration_delay;
public:
    migration_manager();

    /// Register a migration listener on current shard.
    void register_listener(migration_listener* listener);

    /// Unregister a migration listener on current shard.
    void unregister_listener(migration_listener* listener);

    future<> schedule_schema_pull(const gms::inet_address& endpoint, const gms::endpoint_state& state);

    future<> maybe_schedule_schema_pull(const utils::UUID& their_version, const gms::inet_address& endpoint);

    future<> submit_migration_task(const gms::inet_address& endpoint);

    static future<> notify_create_keyspace(const lw_shared_ptr<keyspace_metadata>& ksm);

    static future<> notify_create_column_family(schema_ptr cfm);

    static future<> notify_update_keyspace(const lw_shared_ptr<keyspace_metadata>& ksm);

    static future<> notify_update_column_family(schema_ptr cfm);

    static future<> notify_drop_keyspace(sstring ks_name);

    static future<> notify_drop_column_family(schema_ptr cfm);

    bool should_pull_schema_from(const gms::inet_address& endpoint);

    future<> announce_new_keyspace(lw_shared_ptr<keyspace_metadata> ksm, bool announce_locally = false);

    future<> announce_new_keyspace(lw_shared_ptr<keyspace_metadata> ksm, api::timestamp_type timestamp, bool announce_locally);

    future<> announce_column_family_update(schema_ptr cfm, bool from_thrift, bool announce_locally = false);

    future<> announce_new_column_family(schema_ptr cfm, bool announce_locally = false);

    future<> announce_keyspace_drop(const sstring& ks_name, bool announce_locally = false);

    future<> announce_column_family_drop(const sstring& ks_name, const sstring& cf_name, bool announce_locally = false);

    /**
     * actively announce a new version to active hosts via rpc
     * @param schema The schema mutation to be applied
     */
    static future<> announce(mutation schema, bool announce_locally);

    static future<> announce(std::vector<mutation> mutations, bool announce_locally);

    static future<> push_schema_mutation(const gms::inet_address& endpoint, const std::vector<mutation>& schema);

    // Returns a future on the local application of the schema
    static future<> announce(std::vector<mutation> schema);

    static future<> passive_announce(utils::UUID version);

    future<> stop();

    bool is_ready_for_bootstrap();
};

extern distributed<migration_manager> _the_migration_manager;

inline distributed<migration_manager>& get_migration_manager() {
    return _the_migration_manager;
}

inline migration_manager& get_local_migration_manager() {
    return _the_migration_manager.local();
}

}
