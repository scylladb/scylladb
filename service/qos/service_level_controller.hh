/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/timer.hh>
#include "seastar/util/bool_class.hh"
#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/distributed.hh>
#include <seastar/core/abort_source.hh>
#include "seastarx.hh"
#include "auth/role_manager.hh"
#include "auth/service.hh"
#include <map>
#include "qos_common.hh"
#include "service/endpoint_lifecycle_subscriber.hh"
#include "qos_configuration_change_subscriber.hh"
#include "service/raft/raft_group0_client.hh"
#include "service/raft/raft_group_registry.hh"

namespace db {
    class system_keyspace;
    class system_distributed_keyspace;
}
namespace cql3 {
    class query_processor;
}
namespace service {
    class storage_service;
}

namespace qos {
/**
 *  a structure to hold a service level
 *  data and configuration.
 */
struct service_level {
     service_level_options slo;
     bool marked_for_deletion;
     bool is_static;
};

using update_both_cache_levels = bool_class<class update_both_cache_levels_tag>;

/**
 *  The service_level_controller class is an implementation of the service level
 *  controller design.
 *  It is logically divided into 2 parts:
 *      1. Global controller which is responsible for all of the data and plumbing
 *      manipulation.
 *      2. Local controllers that act upon the data and facilitates execution in
 *      the service level context
 *
 *  Definitions:
 *  service level - User creates service level with some parameters (timeout/workload type).
 *                  Then it can be attached to some role, but it's not guaranteed the role
 *                  will effectively get exactly the same parameter values 
 *                  as its assigned service level.
 *
 *  effective service level - Represents exact values of role's parameters. It's created by
 *                            merging all service levels attached to all roles which are
 *                            granted to the user.
 *
 *  Example:
 *      Roles: user_role, r1, r2
 *          GRANT ROLE r1 TO user_role
 *          GRANT ROLE r2 TO r1
 *      Service levels:
 *          - sl1 (timeout: 1h, workload type: batch)
 *          - sl2 (timeout: 30 min)
 *          - ATTACH SERVICE LEVEL sl1 TO user_role
 *          - ATTACH SERVICE LEVEL sl2 TO r2
 *
 *      Effective service level of user_role is (timeout: 30 min, workload_type: batch).
 *      It gets created by merging all service levels (sl1, sl2) which are attached to
 *      all roles granted to the user (user_role, r1, r2).
 *
 *  For more detail check qos_common.hh (service_level_options::merge_with()) and
 *  docs/dev/service_levels.md
 */
class service_level_controller : public peering_sharded_service<service_level_controller>, public service::endpoint_lifecycle_subscriber {
public:
    class service_level_distributed_data_accessor {
    public:
        virtual future<qos::service_levels_info> get_service_levels() const = 0;
        virtual future<qos::service_levels_info> get_service_level(sstring service_level_name) const = 0;
        virtual future<> set_service_level(sstring service_level_name, qos::service_level_options slo, service::group0_batch& mc) const = 0;
        virtual future<> drop_service_level(sstring service_level_name, service::group0_batch& mc) const = 0;
        virtual future<> commit_mutations(service::group0_batch&& mc, abort_source& as) const = 0;

        virtual bool is_v2() const = 0;
        // Returns v2(raft) data accessor. If data accessor is already a raft one, returns nullptr.
        virtual ::shared_ptr<service_level_distributed_data_accessor> upgrade_to_v2(cql3::query_processor& qp, service::raft_group0_client& group0_client) const = 0;
    };
    using service_level_distributed_data_accessor_ptr = ::shared_ptr<service_level_distributed_data_accessor>;

private:
    struct global_controller_data {
        service_levels_info  static_configurations{};
        int schedg_group_cnt = 0;
        int io_priority_cnt = 0;
        service_level_options default_service_level_config;
        // The below future is used to serialize work so no reordering can occur.
        // This is needed so for example: delete(x), add(x) will not reverse yielding
        // a completely different result than the one intended.
        future<> work_future = make_ready_future();
        semaphore notifications_serializer = semaphore(1);
        future<> distributed_data_update = make_ready_future();
        abort_source dist_data_update_aborter;
        abort_source group0_aborter;
    };

    std::unique_ptr<global_controller_data> _global_controller_db;

    static constexpr shard_id global_controller = 0;

    // service level name -> service_level object
    std::map<sstring, service_level> _service_levels_db;
    // role name -> effective service_level_options 
    std::map<sstring, service_level_options> _effective_service_levels_db;
    service_level _default_service_level;
    service_level_distributed_data_accessor_ptr _sl_data_accessor;
    sharded<auth::service>& _auth_service;
    locator::shared_token_metadata& _token_metadata;
    std::chrono::time_point<seastar::lowres_clock> _last_successful_config_update;
    unsigned _logged_intervals;
    atomic_vector<qos_configuration_change_subscriber*> _subscribers;
    optimized_optional<abort_source::subscription> _early_abort_subscription;
    void do_abort() noexcept;
public:
    service_level_controller(sharded<auth::service>& auth_service, locator::shared_token_metadata& tm, abort_source& as, service_level_options default_service_level_config);

    /**
     * this function must be called *once* from any shard before any other functions are called.
     * No other function should be called before the future returned by the function is resolved.
     * @return a future that resolves when the initialization is over.
     */
    future<> start();

    void set_distributed_data_accessor(service_level_distributed_data_accessor_ptr sl_data_accessor);

    /**
     * Reloads data accessor, this is used to align it with service level version
     * stored in scylla_local table.
     */
    future<> reload_distributed_data_accessor(cql3::query_processor&, service::raft_group0_client&, db::system_keyspace&, db::system_distributed_keyspace&);

    /**
     *  Adds a service level configuration if it doesn't exists, and updates
     *  an the existing one if it does exist.
     *  Handles both, static and non static service level configurations.
     * @param name - the service level name.
     * @param slo - the service level configuration
     * @param is_static - is this configuration static or not
     */
    future<> add_service_level(sstring name, service_level_options slo, bool is_static = false);

    /**
     *  Removes a service level configuration if it exists.
     *  Handles both, static and non static service level configurations.
     * @param name - the service level name.
     * @param remove_static - indicates if it is a removal of a static configuration
     * or not.
     */
    future<> remove_service_level(sstring name, bool remove_static);

    /**
     * stops all ongoing operations if exists
     * @return a future that is resolved when all operations has stopped
     */
    future<> stop();

    void abort_group0_operations();

    /**
     * Start legacy update loop if RAFT_SERVICE_LEVELS_CHANGE feature is not enabled yet 
     * or the cluster is in recovery mode
     * or the cluster hasn't been migrated to raft topology.
     *
     * The update loop check the distributed data 
     * for changes in a constant interval and updates
     * the service_levels configuration in accordance (adds, removes, or updates
     * service levels as necessary).
     * @param interval_f - lambda function which returns a interval in milliseconds.
                           The interval is time to check the distributed data.
     * @return a future that is resolved when the update loop stops.
     */
    void maybe_start_legacy_update_from_distributed_data(std::function<steady_clock_type::duration()> interval_f, service::storage_service& storage_service, service::raft_group0_client& group0_client);

    /**
     * Request abort of update loop.
     * Must be called on shard 0.
     */
    void stop_legacy_update_from_distributed_data();

    /**
     * Updates the service level cache from the distributed data store.
     * Must be executed on shard 0.
     * @return a future that is resolved when the update is done
     */
    future<> update_service_levels_cache();

    /**
     * Updates effective service levels cache.
     * The method uses service levels cache (_service_levels_db)
     * and data from auth tables.
     * Must be executed on shard 0.
     * @return a future that is resolved when the update is done
     */
    future<> update_effective_service_levels_cache();

    /**
     * Service levels cache consists of two levels: service levels cache and effective service levels cache
     * The second one is dependent on the first one.
     *
     * update_both_cache_levels::yes - updates both levels of the cache
     * update_both_cache_levels::no  - update only effective service levels cache
     */
    future<> update_cache(update_both_cache_levels update_both_cache_levels = update_both_cache_levels::yes);

    future<> add_distributed_service_level(sstring name, service_level_options slo, bool if_not_exsists, service::group0_batch& mc);
    future<> alter_distributed_service_level(sstring name, service_level_options slo, service::group0_batch& mc);
    future<> drop_distributed_service_level(sstring name, bool if_exists, service::group0_batch& mc);
    future<service_levels_info> get_distributed_service_levels();
    future<service_levels_info> get_distributed_service_level(sstring service_level_name);

    /**
     * Returns the service level options **in effect** for a user having the given
     * collection of roles.
     * @param roles - the collection of roles to consider
     * @return the effective service level options - they may in particular be a combination
     *         of options from multiple service levels
     */
    future<std::optional<service_level_options>> find_effective_service_level(const sstring& role_name);

    /**
     * Gets the service level data by name.
     * @param service_level_name - the name of the requested service level
     * @return the service level data if it exists (in the local controller) or
     * get_service_level("default") otherwise.
     */
    service_level& get_service_level(sstring service_level_name) {
        auto sl_it = _service_levels_db.find(service_level_name);
        if (sl_it == _service_levels_db.end() || sl_it->second.marked_for_deletion) {
            sl_it = _service_levels_db.find(default_service_level_name);
        }
        return sl_it->second;
    }

    future<> commit_mutations(::service::group0_batch&& mc) {
        if (_sl_data_accessor->is_v2()) {
            return _sl_data_accessor->commit_mutations(std::move(mc), _global_controller_db->group0_aborter);
        }
        return make_ready_future();
    }

    /**
     * Returns true if service levels module is running under raft
     */
    bool is_v2() const;

    void upgrade_to_v2(cql3::query_processor& qp, service::raft_group0_client& group0_client);

    /**
     * Migrate data from `system_distributed.service_levels` to `system.service_levels_v2`
     */
    static future<> migrate_to_v2(size_t nodes_count, db::system_keyspace& sys_ks, cql3::query_processor& qp, service::raft_group0_client& group0_client, abort_source& as);
private:
    /**
     *  Adds a service level configuration if it doesn't exists, and updates
     *  an the existing one if it does exist.
     *  Handles both, static and non static service level configurations.
     * @param name - the service level name.
     * @param slo - the service level configuration
     * @param is_static - is this configuration static or not
     */
    future<> do_add_service_level(sstring name, service_level_options slo, bool is_static = false);

    /**
     *  Removes a service level configuration if it exists.
     *  Handles both, static and non static service level configurations.
     * @param name - the service level name.
     * @param remove_static - indicates if it is a removal of a static configuration
     * or not.
     */
    future<> do_remove_service_level(sstring name, bool remove_static);

    /**
     * The notify functions are used by the global service level controller
     * to propagate configuration changes to the local controllers.
     * the returned future is resolved when the local controller is done acting
     * on the notification. updates are done in sequence so their meaning will not
     * change due to execution reordering.
     */
    future<> notify_service_level_added(sstring name, service_level sl_data);
    future<> notify_service_level_updated(sstring name, service_level_options slo);
    future<> notify_service_level_removed(sstring name);
    future<> notify_effective_service_levels_cache_reloaded();

    enum class  set_service_level_op_type {
        add_if_not_exists,
        add,
        alter
    };

    future<> set_distributed_service_level(sstring name, service_level_options slo, set_service_level_op_type op_type, service::group0_batch& mc);
public:

    /**
     *  Register a subscriber for service level configuration changes
     *  notifications
     * @param subscriber - a pointer to the subscriber.
     *
     * Note: the caller is responsible to keep the pointed to object alive until performing
     * a call to service_level_controller::unregister_subscriber()).
     *
     * Note 2: the subscription is per shard.
     */
    void register_subscriber(qos_configuration_change_subscriber* subscriber);
    future<> unregister_subscriber(qos_configuration_change_subscriber* subscriber);

    static sstring default_service_level_name;

    virtual void on_join_cluster(const gms::inet_address& endpoint) override;
    virtual void on_leave_cluster(const gms::inet_address& endpoint, const locator::host_id& hid) override;
    virtual void on_up(const gms::inet_address& endpoint) override;
    virtual void on_down(const gms::inet_address& endpoint) override;
};

future<shared_ptr<service_level_controller::service_level_distributed_data_accessor>> 
get_service_level_distributed_data_accessor_for_current_version(
    db::system_keyspace& sys_ks,
    db::system_distributed_keyspace& sys_dist_ks,
    cql3::query_processor& qp, service::raft_group0_client& group0_client
);

}
