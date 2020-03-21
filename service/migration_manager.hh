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

#include <type_traits>
#include "service/migration_listener.hh"
#include "gms/endpoint_state.hh"
#include "db/schema_tables.hh"
#include <seastar/core/distributed.hh>
#include "gms/inet_address.hh"
#include "message/msg_addr.hh"
#include "utils/UUID.hh"
#include "utils/serialized_action.hh"

#include <vector>

namespace service {

GCC6_CONCEPT(
    template<typename M>
    concept bool MergeableMutation = std::is_same<M, canonical_mutation>::value || std::is_same<M, frozen_mutation>::value;
)

class migration_manager : public seastar::async_sharded_service<migration_manager> {
private:
    migration_notifier& _notifier;

    std::unordered_map<netw::msg_addr, serialized_action, netw::msg_addr::hash> _schema_pulls;
    std::vector<gms::feature::listener_registration> _feature_listeners;
    seastar::gate _background_tasks;
    static const std::chrono::milliseconds migration_delay;
    gms::feature_service& _feat;
    seastar::abort_source _as;
    bool _cluster_upgraded = false;
    seastar::condition_variable _wait_cluster_upgraded;
public:
    migration_manager(migration_notifier&, gms::feature_service&);

    migration_notifier& get_notifier() { return _notifier; }
    const migration_notifier& get_notifier() const { return _notifier; }

    future<> schedule_schema_pull(const gms::inet_address& endpoint, const gms::endpoint_state& state);

    future<> maybe_schedule_schema_pull(const utils::UUID& their_version, const gms::inet_address& endpoint);

    future<> submit_migration_task(const gms::inet_address& endpoint);

    // Makes sure that this node knows about all schema changes known by "nodes" that were made prior to this call.
    future<> sync_schema(const database& db, const std::vector<gms::inet_address>& nodes);

    // Fetches schema from remote node and applies it locally.
    // Differs from submit_migration_task() in that all errors are propagated.
    // Coalesces requests.
    future<> merge_schema_from(netw::msg_addr);
    future<> do_merge_schema_from(netw::msg_addr);

    // Merge mutations received from src.
    // Keep mutations alive around whole async operation.
    future<> merge_schema_from(netw::msg_addr src, const std::vector<canonical_mutation>& mutations);
    // Deprecated. The canonical mutation should be used instead.
    future<> merge_schema_from(netw::msg_addr src, const std::vector<frozen_mutation>& mutations);

    template<typename M>
    GCC6_CONCEPT(requires MergeableMutation<M>)
    future<> merge_schema_in_background(netw::msg_addr src, const std::vector<M>& mutations) {
        return with_gate(_background_tasks, [this, src, &mutations] {
            return merge_schema_from(src, mutations);
        });
    }

    bool should_pull_schema_from(const gms::inet_address& endpoint);
    bool has_compatible_schema_tables_version(const gms::inet_address& endpoint);

    future<> announce_keyspace_update(lw_shared_ptr<keyspace_metadata> ksm, bool announce_locally = false);

    future<> announce_keyspace_update(lw_shared_ptr<keyspace_metadata> ksm, api::timestamp_type timestamp, bool announce_locally);

    future<> announce_new_keyspace(lw_shared_ptr<keyspace_metadata> ksm, bool announce_locally = false);

    future<> announce_new_keyspace(lw_shared_ptr<keyspace_metadata> ksm, api::timestamp_type timestamp, bool announce_locally);

    future<> announce_column_family_update(schema_ptr cfm, bool from_thrift, std::vector<view_ptr>&& view_updates, bool announce_locally = false);

    future<> announce_new_column_family(schema_ptr cfm, bool announce_locally = false);

    future<> announce_new_column_family(schema_ptr cfm, api::timestamp_type timestamp, bool announce_locally = false);

    future<> announce_new_type(user_type new_type, bool announce_locally = false);

    future<> announce_new_function(shared_ptr<cql3::functions::user_function> func, bool announce_locally);

    future<> announce_function_drop(shared_ptr<cql3::functions::user_function> func, bool announce_locally);

    future<> announce_type_update(user_type updated_type, bool announce_locally = false);

    future<> announce_keyspace_drop(const sstring& ks_name, bool announce_locally = false);

    class drop_views_tag;
    using drop_views = bool_class<drop_views_tag>;
    future<> announce_column_family_drop(const sstring& ks_name, const sstring& cf_name, bool announce_locally = false, drop_views drop_views = drop_views::no);

    future<> announce_type_drop(user_type dropped_type, bool announce_locally = false);

    future<> announce_new_view(view_ptr view, bool announce_locally = false);

    future<> announce_view_update(view_ptr view, bool announce_locally = false);

    future<> announce_view_drop(const sstring& ks_name, const sstring& cf_name, bool announce_locally = false);

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

    /**
     * Known peers in the cluster have the same schema version as us.
     */
    bool have_schema_agreement();

    void init_messaging_service();
private:
    future<> uninit_messaging_service();

    static future<> include_keyspace_and_announce(
            const keyspace_metadata& keyspace, std::vector<mutation> mutations, bool announce_locally);

    static future<> do_announce_new_type(user_type new_type, bool announce_locally);
};

inline distributed<migration_manager> _the_migration_manager;

inline distributed<migration_manager>& get_migration_manager() {
    return _the_migration_manager;
}

inline migration_manager& get_local_migration_manager() {
    return _the_migration_manager.local();
}

// Returns schema of given version, either from cache or from remote node identified by 'from'.
// Doesn't affect current node's schema in any way.
future<schema_ptr> get_schema_definition(table_schema_version, netw::msg_addr from);

// Returns schema of given version, either from cache or from remote node identified by 'from'.
// The returned schema may not be synchronized. See schema::is_synced().
// Intended to be used in the read path.
future<schema_ptr> get_schema_for_read(table_schema_version, netw::msg_addr from);

// Returns schema of given version, either from cache or from remote node identified by 'from'.
// Ensures that this node is synchronized with the returned schema. See schema::is_synced().
// Intended to be used in the write path, which relies on synchronized schema.
future<schema_ptr> get_schema_for_write(table_schema_version, netw::msg_addr from);

}
