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

#include <type_traits>
#include "service/migration_listener.hh"
#include "gms/endpoint_state.hh"
#include <seastar/core/distributed.hh>
#include <seastar/core/abort_source.hh>
#include <seastar/core/gate.hh>
#include "gms/inet_address.hh"
#include "gms/feature.hh"
#include "message/msg_addr.hh"
#include "utils/UUID.hh"
#include "utils/serialized_action.hh"

#include <vector>

class canonical_mutation;
class frozen_mutation;
namespace cql3 { namespace functions { class user_function; }}
namespace netw { class messaging_service; }

namespace service {

template<typename M>
concept MergeableMutation = std::is_same<M, canonical_mutation>::value || std::is_same<M, frozen_mutation>::value;

class migration_manager : public seastar::async_sharded_service<migration_manager>,
                            public seastar::peering_sharded_service<migration_manager> {
private:
    migration_notifier& _notifier;

    std::unordered_map<netw::msg_addr, serialized_action, netw::msg_addr::hash> _schema_pulls;
    std::vector<gms::feature::listener_registration> _feature_listeners;
    seastar::gate _background_tasks;
    static const std::chrono::milliseconds migration_delay;
    gms::feature_service& _feat;
    netw::messaging_service& _messaging;
    seastar::abort_source _as;
public:
    migration_manager(migration_notifier&, gms::feature_service&, netw::messaging_service& ms);

    migration_notifier& get_notifier() { return _notifier; }
    const migration_notifier& get_notifier() const { return _notifier; }

    future<> schedule_schema_pull(const gms::inet_address& endpoint, const gms::endpoint_state& state);

    future<> maybe_schedule_schema_pull(const utils::UUID& their_version, const gms::inet_address& endpoint);

    future<> submit_migration_task(const gms::inet_address& endpoint, bool can_ignore_down_node = true);

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
    requires MergeableMutation<M>
    future<> merge_schema_in_background(netw::msg_addr src, const std::vector<M>& mutations) {
        return with_gate(_background_tasks, [this, src, &mutations] {
            return merge_schema_from(src, mutations);
        });
    }

    bool should_pull_schema_from(const gms::inet_address& endpoint);
    bool has_compatible_schema_tables_version(const gms::inet_address& endpoint);

    future<> announce_keyspace_update(lw_shared_ptr<keyspace_metadata> ksm);

    future<> announce_new_keyspace(lw_shared_ptr<keyspace_metadata> ksm);

    future<> announce_new_keyspace(lw_shared_ptr<keyspace_metadata> ksm, api::timestamp_type timestamp);

    // The timestamp parameter can be used to ensure that all nodes update their internal tables' schemas
    // with identical timestamps, which can prevent an undeeded schema exchange
    future<> announce_column_family_update(schema_ptr cfm, bool from_thrift, std::vector<view_ptr>&& view_updates, std::optional<api::timestamp_type> timestamp);

    future<> announce_new_column_family(schema_ptr cfm);

    future<> announce_new_column_family(schema_ptr cfm, api::timestamp_type timestamp);

    future<> announce_new_type(user_type new_type);

    future<> announce_new_function(shared_ptr<cql3::functions::user_function> func);

    future<> announce_function_drop(shared_ptr<cql3::functions::user_function> func);

    future<> announce_type_update(user_type updated_type);

    future<> announce_keyspace_drop(const sstring& ks_name);

    class drop_views_tag;
    using drop_views = bool_class<drop_views_tag>;
    future<> announce_column_family_drop(const sstring& ks_name, const sstring& cf_name, drop_views drop_views = drop_views::no);

    future<> announce_type_drop(user_type dropped_type);

    future<> announce_new_view(view_ptr view);

    future<> announce_view_update(view_ptr view);

    future<> announce_view_drop(const sstring& ks_name, const sstring& cf_name);

    /**
     * actively announce a new version to active hosts via rpc
     * @param schema The schema mutation to be applied
     */
    // Returns a future on the local application of the schema
    future<> announce(std::vector<mutation> schema);

    static future<> passive_announce(utils::UUID version);

    future<> stop();

    /**
     * Known peers in the cluster have the same schema version as us.
     */
    bool have_schema_agreement();

    void init_messaging_service();
private:
    future<> uninit_messaging_service();

    future<> include_keyspace_and_announce(
            const keyspace_metadata& keyspace, std::vector<mutation> mutations);

    future<> do_announce_new_type(user_type new_type);

    future<> push_schema_mutation(const gms::inet_address& endpoint, const std::vector<mutation>& schema);

public:
    future<> maybe_sync(const schema_ptr& s, netw::msg_addr endpoint);

    // Returns schema of given version, either from cache or from remote node identified by 'from'.
    // The returned schema may not be synchronized. See schema::is_synced().
    // Intended to be used in the read path.
    future<schema_ptr> get_schema_for_read(table_schema_version, netw::msg_addr from, netw::messaging_service& ms);

    // Returns schema of given version, either from cache or from remote node identified by 'from'.
    // Ensures that this node is synchronized with the returned schema. See schema::is_synced().
    // Intended to be used in the write path, which relies on synchronized schema.
    future<schema_ptr> get_schema_for_write(table_schema_version, netw::msg_addr from, netw::messaging_service& ms);
};

future<column_mapping> get_column_mapping(utils::UUID table_id, table_schema_version v);

}
