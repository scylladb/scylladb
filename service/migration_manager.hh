/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
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
#include "gms/i_endpoint_state_change_subscriber.hh"
#include "message/msg_addr.hh"
#include "schema_fwd.hh"
#include "utils/serialized_action.hh"
#include "service/raft/raft_group_registry.hh"
#include "service/raft/raft_group0_client.hh"

#include <vector>

class canonical_mutation;
class frozen_mutation;
namespace cql3 {
namespace functions { class user_function; class user_aggregate; }
}
namespace netw { class messaging_service; }

namespace gms {

class gossiper;
enum class application_state;
class versioned_value;

}

namespace db {
class system_keyspace;
}

namespace service {

class storage_proxy;

template<typename M>
concept MergeableMutation = std::is_same<M, canonical_mutation>::value || std::is_same<M, frozen_mutation>::value;

class migration_manager : public seastar::async_sharded_service<migration_manager>,
                            public gms::i_endpoint_state_change_subscriber,
                            public seastar::peering_sharded_service<migration_manager> {
private:
    migration_notifier& _notifier;

    std::unordered_map<netw::msg_addr, serialized_action, netw::msg_addr::hash> _schema_pulls;
    std::vector<gms::feature::listener_registration> _feature_listeners;
    seastar::gate _background_tasks;
    static const std::chrono::milliseconds migration_delay;
    gms::feature_service& _feat;
    netw::messaging_service& _messaging;
    service::storage_proxy& _storage_proxy;
    gms::gossiper& _gossiper;
    seastar::abort_source _as;
    service::raft_group0_client& _group0_client;
    sharded<db::system_keyspace>& _sys_ks;
    serialized_action _schema_push;
    table_schema_version _schema_version_to_publish;

    friend class group0_state_machine; // needed for access to _messaging
    size_t _concurrent_ddl_retries;
public:
    migration_manager(migration_notifier&, gms::feature_service&, netw::messaging_service& ms, service::storage_proxy&, gms::gossiper& gossiper, service::raft_group0_client& group0_client, sharded<db::system_keyspace>& sysks);

    migration_notifier& get_notifier() { return _notifier; }
    const migration_notifier& get_notifier() const { return _notifier; }
    service::storage_proxy& get_storage_proxy() { return _storage_proxy; }
    const service::storage_proxy& get_storage_proxy() const { return _storage_proxy; }

    future<> submit_migration_task(const gms::inet_address& endpoint, bool can_ignore_down_node = true);

    // Makes sure that this node knows about all schema changes known by "nodes" that were made prior to this call.
    future<> sync_schema(const replica::database& db, const std::vector<gms::inet_address>& nodes);

    // Fetches schema from remote node and applies it locally.
    // Differs from submit_migration_task() in that all errors are propagated.
    // Coalesces requests.
    future<> merge_schema_from(netw::msg_addr);
    future<> do_merge_schema_from(netw::msg_addr);
    future<> reload_schema();

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

    std::vector<mutation> prepare_keyspace_update_announcement(lw_shared_ptr<keyspace_metadata> ksm, api::timestamp_type);

    std::vector<mutation> prepare_new_keyspace_announcement(lw_shared_ptr<keyspace_metadata> ksm, api::timestamp_type);


    // The timestamp parameter can be used to ensure that all nodes update their internal tables' schemas
    // with identical timestamps, which can prevent an undeeded schema exchange
    future<std::vector<mutation>> prepare_column_family_update_announcement(schema_ptr cfm, bool from_thrift, std::vector<view_ptr> view_updates, api::timestamp_type ts);

    future<std::vector<mutation>> prepare_new_column_family_announcement(schema_ptr cfm, api::timestamp_type timestamp);

    future<std::vector<mutation>> prepare_new_type_announcement(user_type new_type, api::timestamp_type);

    future<std::vector<mutation>> prepare_new_function_announcement(shared_ptr<cql3::functions::user_function> func, api::timestamp_type);

    future<std::vector<mutation>> prepare_new_aggregate_announcement(shared_ptr<cql3::functions::user_aggregate> aggregate, api::timestamp_type);

    future<std::vector<mutation>> prepare_function_drop_announcement(shared_ptr<cql3::functions::user_function> func, api::timestamp_type);

    future<std::vector<mutation>> prepare_aggregate_drop_announcement(shared_ptr<cql3::functions::user_aggregate> aggregate, api::timestamp_type);

    future<std::vector<mutation>> prepare_update_type_announcement(user_type updated_type, api::timestamp_type);

    std::vector<mutation> prepare_keyspace_drop_announcement(const sstring& ks_name, api::timestamp_type);

    class drop_views_tag;
    using drop_views = bool_class<drop_views_tag>;
    future<std::vector<mutation>> prepare_column_family_drop_announcement(const sstring& ks_name, const sstring& cf_name, api::timestamp_type, drop_views drop_views = drop_views::no);

    future<std::vector<mutation>> prepare_type_drop_announcement(user_type dropped_type, api::timestamp_type);

    future<std::vector<mutation>> prepare_new_view_announcement(view_ptr view, api::timestamp_type);

    future<std::vector<mutation>> prepare_view_update_announcement(view_ptr view, api::timestamp_type);

    future<std::vector<mutation>> prepare_view_drop_announcement(const sstring& ks_name, const sstring& cf_name, api::timestamp_type);

    // The function needs to be called if the user wants to read most up-to-date group 0 state (including schema state)
    // (the function ensures that all previously finished group0 operations are visible on this node) or to write it.
    //
    // Call this *before* reading group 0 state (e.g. when performing a schema change, call this before validation).
    // Use `group0_guard::write_timestamp()` when creating mutations which modify group 0 (e.g. schema tables mutations).
    //
    // Call ONLY on shard 0.
    // Requires a quorum of nodes to be available in order to finish.
    future<group0_guard> start_group0_operation();

    // Apply a group 0 change.
    // The future resolves after the change is applied locally.
    future<> announce(std::vector<mutation> schema, group0_guard, std::string_view description = "");

    void passive_announce(table_schema_version version);

    future<> drain();
    future<> stop();

    /**
     * Known peers in the cluster have the same schema version as us.
     */
    bool have_schema_agreement();

    void init_messaging_service();

    // Maximum number of retries one should attempt when trying to perform
    // a DDL statement and getting `group0_concurrent_modification` exception.
    size_t get_concurrent_ddl_retries() const { return _concurrent_ddl_retries; }
private:
    future<> uninit_messaging_service();

    future<std::vector<mutation>> include_keyspace(const keyspace_metadata& keyspace, std::vector<mutation> mutations);
    future<std::vector<mutation>> do_prepare_new_type_announcement(user_type new_type, api::timestamp_type);

    future<> push_schema_mutation(const gms::inet_address& endpoint, const std::vector<mutation>& schema);

    future<> passive_announce();

    void schedule_schema_pull(const gms::inet_address& endpoint, const gms::endpoint_state& state);

    future<> maybe_schedule_schema_pull(const table_schema_version& their_version, const gms::inet_address& endpoint);

    future<> announce_with_raft(std::vector<mutation> schema, group0_guard, std::string_view description);
    future<> announce_without_raft(std::vector<mutation> schema, group0_guard);

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

private:
    virtual future<> on_join(gms::inet_address endpoint, gms::endpoint_state ep_state) override;
    virtual future<> on_change(gms::inet_address endpoint, gms::application_state state, const gms::versioned_value& value) override;
    virtual future<> on_alive(gms::inet_address endpoint, gms::endpoint_state state) override;
    virtual future<> on_dead(gms::inet_address endpoint, gms::endpoint_state state) override { return make_ready_future(); }
    virtual future<> on_remove(gms::inet_address endpoint) override { return make_ready_future(); }
    virtual future<> on_restart(gms::inet_address endpoint, gms::endpoint_state state) override { return make_ready_future(); }
    virtual future<> before_change(gms::inet_address endpoint, gms::endpoint_state current_state, gms::application_state new_statekey, const gms::versioned_value& newvalue) override { return make_ready_future(); }

public:
    // For tests only.
    void set_concurrent_ddl_retries(size_t);
};

future<column_mapping> get_column_mapping(table_id, table_schema_version v);

}
