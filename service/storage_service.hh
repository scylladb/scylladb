/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <seastar/core/shared_future.hh>
#include "gms/endpoint_state.hh"
#include "gms/i_endpoint_state_change_subscriber.hh"
#include "schema/schema_fwd.hh"
#include "service/endpoint_lifecycle_subscriber.hh"
#include "service/topology_guard.hh"
#include "locator/abstract_replication_strategy.hh"
#include "locator/tablets.hh"
#include "locator/tablet_metadata_guard.hh"
#include "inet_address_vectors.hh"
#include <seastar/core/distributed.hh>
#include <seastar/core/condition-variable.hh>
#include "dht/token_range_endpoints.hh"
#include <seastar/core/sleep.hh>
#include "gms/application_state.hh"
#include <seastar/core/semaphore.hh>
#include <seastar/core/gate.hh>
#include "replica/database_fwd.hh"
#include "streaming/stream_reason.hh"
#include <seastar/core/distributed.hh>
#include "service/migration_listener.hh"
#include <seastar/core/metrics_registration.hh>
#include <seastar/core/rwlock.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/lowres_clock.hh>
#include "cdc/generation_id.hh"
#include "db/system_keyspace.hh"
#include "raft/raft.hh"
#include "node_ops/id.hh"
#include "raft/server.hh"
#include "service/raft/raft_address_map.hh"
#include "service/topology_state_machine.hh"
#include "service/tablet_allocator.hh"

class node_ops_cmd_request;
class node_ops_cmd_response;
struct node_ops_ctl;
class node_ops_info;
enum class node_ops_cmd : uint32_t;
class repair_service;
class protocol_server;

namespace cql3 { class query_processor; }

namespace cql_transport { class controller; }

namespace cdc {
class generation_service;
}

namespace streaming {
class stream_manager;
}

namespace db {
class system_distributed_keyspace;
class system_keyspace;
class batchlog_manager;
}

namespace netw {
class messaging_service;
}

namespace dht {
class boot_strapper;
class range_streamer;
}

namespace gms {
class feature_service;
class gossiper;
};

namespace service {

class storage_service;
class storage_proxy;
class migration_manager;
class raft_group0;
class group0_guard;
class group0_info;

struct join_node_request_params;
struct join_node_request_result;
struct join_node_response_params;
struct join_node_response_result;

enum class disk_error { regular, commit };

class node_ops_meta_data;

using start_hint_manager = seastar::bool_class<class start_hint_manager_tag>;
using loosen_constraints = seastar::bool_class<class loosen_constraints_tag>;

/**
 * This abstraction contains the token/identifier of this node
 * on the identifier space. This token gets gossiped around.
 * This class will also maintain histograms of the load information
 * of other nodes in the cluster.
 */
class storage_service : public service::migration_listener, public gms::i_endpoint_state_change_subscriber,
        public seastar::async_sharded_service<storage_service>, public seastar::peering_sharded_service<storage_service> {
private:
    using token = dht::token;
    using token_range_endpoints = dht::token_range_endpoints;
    using endpoint_details = dht::endpoint_details;
    using boot_strapper = dht::boot_strapper;
    using token_metadata = locator::token_metadata;
    using shared_token_metadata = locator::shared_token_metadata;
    using token_metadata_ptr = locator::token_metadata_ptr;
    using mutable_token_metadata_ptr = locator::mutable_token_metadata_ptr;
    using token_metadata_lock = locator::token_metadata_lock;
    using application_state = gms::application_state;
    using inet_address = gms::inet_address;
    using versioned_value = gms::versioned_value;

    struct tablet_operation {
        sstring name;
        shared_future<> done;
    };

    using tablet_op_registry = std::unordered_map<locator::global_tablet_id, tablet_operation>;

    abort_source& _abort_source;
    gms::feature_service& _feature_service;
    distributed<replica::database>& _db;
    gms::gossiper& _gossiper;
    sharded<netw::messaging_service>& _messaging;
    sharded<service::migration_manager>& _migration_manager;
    cql3::query_processor& _qp;
    sharded<repair_service>& _repair;
    sharded<streaming::stream_manager>& _stream_manager;
    sharded<locator::snitch_ptr>& _snitch;

    // Engaged on shard 0 before `join_cluster`.
    service::raft_group0* _group0;

    sstring _operation_in_progress;
    seastar::metrics::metric_groups _metrics;
    using client_shutdown_hook = noncopyable_function<void()>;
    std::vector<protocol_server*> _protocol_servers;
    std::vector<std::any> _listeners;
    gate _async_gate;

    condition_variable _tablet_split_monitor_event;
    std::deque<table_id> _tablet_split_candidates;
    future<> _tablet_split_monitor = make_ready_future<>();

    std::unordered_map<node_ops_id, node_ops_meta_data> _node_ops;
    std::list<std::optional<node_ops_id>> _node_ops_abort_queue;
    seastar::condition_variable _node_ops_abort_cond;
    named_semaphore _node_ops_abort_sem{1, named_semaphore_exception_factory{"node_ops_abort_sem"}};
    future<> _node_ops_abort_thread;
    void node_ops_insert(node_ops_id, gms::inet_address coordinator, std::list<inet_address> ignore_nodes,
                         std::function<future<>()> abort_func);
    future<> node_ops_update_heartbeat(node_ops_id ops_uuid);
    future<> node_ops_done(node_ops_id ops_uuid);
    future<> node_ops_abort(node_ops_id ops_uuid);
    void node_ops_signal_abort(std::optional<node_ops_id> ops_uuid);
    future<> node_ops_abort_thread();
    future<> do_tablet_operation(locator::global_tablet_id tablet,
                                 sstring op_name,
                                 std::function<future<>(locator::tablet_metadata_guard&)> op);
    future<> stream_tablet(locator::global_tablet_id);
    future<> cleanup_tablet(locator::global_tablet_id);
    inet_address host2ip(locator::host_id) const;
    // Handler for table load stats RPC.
    future<locator::load_stats> load_stats_for_tablet_based_tables();
    future<> process_tablet_split_candidate(table_id);
    void register_tablet_split_candidate(table_id) noexcept;
    future<> run_tablet_split_monitor();
public:
    storage_service(abort_source& as, distributed<replica::database>& db,
        gms::gossiper& gossiper,
        sharded<db::system_keyspace>&,
        sharded<db::system_distributed_keyspace>&,
        gms::feature_service& feature_service,
        sharded<service::migration_manager>& mm,
        locator::shared_token_metadata& stm,
        locator::effective_replication_map_factory& erm_factory,
        sharded<netw::messaging_service>& ms,
        sharded<repair_service>& repair,
        sharded<streaming::stream_manager>& stream_manager,
        endpoint_lifecycle_notifier& elc_notif,
        sharded<db::batchlog_manager>& bm,
        sharded<locator::snitch_ptr>& snitch,
        sharded<service::tablet_allocator>& tablet_allocator,
        sharded<cdc::generation_service>& cdc_gs,
        cql3::query_processor& qp);

    // Needed by distributed<>
    future<> stop();
    void init_messaging_service(bool raft_topology_change_enabled);
    future<> uninit_messaging_service();

    future<> load_tablet_metadata();
    void start_tablet_split_monitor();
private:
    using acquire_merge_lock = bool_class<class acquire_merge_lock_tag>;

    // Token metadata changes are serialized
    // using the schema_tables merge_lock.
    //
    // Must be called on shard 0.
    future<token_metadata_lock> get_token_metadata_lock() noexcept;

    // Acquire the token_metadata lock and get a mutable_token_metadata_ptr.
    // Pass that ptr to \c func, and when successfully done,
    // replicate it to all cores.
    //
    // By default the merge_lock (that is unified with the token_metadata_lock)
    // is acquired for mutating the token_metadata.  Pass acquire_merge_lock::no
    // when called from paths that already acquire the merge_lock, like
    // db::schema_tables::do_merge_schema.
    //
    // Note: must be called on shard 0.
    future<> mutate_token_metadata(std::function<future<> (mutable_token_metadata_ptr)> func, acquire_merge_lock aml = acquire_merge_lock::yes) noexcept;

    // Update pending ranges locally and then replicate to all cores.
    // Should be serialized under token_metadata_lock.
    // Must be called on shard 0.
    future<> update_topology_change_info(mutable_token_metadata_ptr tmptr, sstring reason);
    future<> update_topology_change_info(sstring reason, acquire_merge_lock aml = acquire_merge_lock::yes);
    future<> keyspace_changed(const sstring& ks_name);
    void register_metrics();
    future<> snitch_reconfigured();

    future<mutable_token_metadata_ptr> get_mutable_token_metadata_ptr() noexcept {
        return _shared_token_metadata.get()->clone_async().then([] (token_metadata tm) {
            // bump the token_metadata ring_version
            // to invalidate cached token/replication mappings
            // when the modified token_metadata is committed.
            tm.invalidate_cached_rings();
            return make_ready_future<mutable_token_metadata_ptr>(make_token_metadata_ptr(std::move(tm)));
        });
    }

    sharded<db::batchlog_manager>& get_batchlog_manager() noexcept {
        return _batchlog_manager;
    }

    friend struct ::node_ops_ctl;
public:

    const gms::gossiper& gossiper() const noexcept {
        return _gossiper;
    };

    gms::gossiper& gossiper() noexcept {
        return _gossiper;
    };

    locator::effective_replication_map_factory& get_erm_factory() noexcept {
        return _erm_factory;
    }

    const locator::effective_replication_map_factory& get_erm_factory() const noexcept {
        return _erm_factory;
    }

    token_metadata_ptr get_token_metadata_ptr() const noexcept {
        return _shared_token_metadata.get();
    }

    const locator::token_metadata& get_token_metadata() const noexcept {
        return *_shared_token_metadata.get();
    }

private:
    inet_address get_broadcast_address() const noexcept {
        return get_token_metadata_ptr()->get_topology().my_address();
    }
    bool is_me(inet_address addr) const noexcept {
        return get_token_metadata_ptr()->get_topology().is_me(addr);
    }
    bool is_me(locator::host_id id) const noexcept {
        return get_token_metadata_ptr()->get_topology().is_me(id);
    }

    /* This abstraction maintains the token/endpoint metadata information */
    shared_token_metadata& _shared_token_metadata;
    locator::effective_replication_map_factory& _erm_factory;

public:
    std::chrono::milliseconds get_ring_delay();
    enum class mode { NONE, STARTING, JOINING, BOOTSTRAP, NORMAL, LEAVING, DECOMMISSIONED, MOVING, DRAINING, DRAINED, MAINTENANCE };
private:
    mode _operation_mode = mode::NONE;
    /* Used for tracking drain progress */

    endpoint_lifecycle_notifier& _lifecycle_notifier;
    sharded<db::batchlog_manager>& _batchlog_manager;

public:
    // should only be called via JMX
    future<> stop_gossiping();

    // should only be called via JMX
    future<> start_gossiping();

    // should only be called via JMX
    future<bool> is_gossip_running();

    void register_protocol_server(protocol_server& server) {
        _protocol_servers.push_back(&server);
    }

    // All pointers are valid.
    const std::vector<protocol_server*>& protocol_servers() const {
        return _protocol_servers;
    }
private:
    future<> shutdown_protocol_servers();

    struct replacement_info {
        std::unordered_set<token> tokens;
        locator::endpoint_dc_rack dc_rack;
        locator::host_id host_id;
        gms::inet_address address;
    };
    future<replacement_info> prepare_replacement_info(std::unordered_set<gms::inet_address> initial_contact_nodes,
            const std::unordered_map<gms::inet_address, sstring>& loaded_peer_features);

    void run_replace_ops(std::unordered_set<token>& bootstrap_tokens, replacement_info replace_info);
    void run_bootstrap_ops(std::unordered_set<token>& bootstrap_tokens);

    future<> wait_for_ring_to_settle();

public:

    static std::unordered_set<gms::inet_address> parse_node_list(sstring comma_separated_list, const locator::token_metadata& tm);

    future<> check_for_endpoint_collision(std::unordered_set<gms::inet_address> initial_contact_nodes,
            const std::unordered_map<gms::inet_address, sstring>& loaded_peer_features);

    future<> join_cluster(sharded<db::system_distributed_keyspace>& sys_dist_ks, sharded<service::storage_proxy>& proxy,
            sharded<gms::gossiper>& gossiper_ptr, start_hint_manager start_hm, gms::generation_type new_generation);

    void set_group0(service::raft_group0&, bool raft_experimental_topology);

    future<> init_address_map(raft_address_map& address_map, gms::generation_type new_generation);

    future<> uninit_address_map();
    bool is_topology_coordinator_enabled() const;

    future<> drain_on_shutdown();

    future<> stop_transport();

    future<> wait_for_group0_stop();

private:
    bool should_bootstrap();
    bool is_replacing();
    bool is_first_node();
    future<> join_token_ring(sharded<db::system_distributed_keyspace>& sys_dist_ks,
            sharded<service::storage_proxy>& proxy,
            sharded<gms::gossiper>& gossiper,
            std::unordered_set<gms::inet_address> initial_contact_nodes,
            std::unordered_set<gms::inet_address> loaded_endpoints,
            std::unordered_map<gms::inet_address, sstring> loaded_peer_features,
            std::chrono::milliseconds,
            start_hint_manager start_hm,
            gms::generation_type new_generation);
    future<> start_sys_dist_ks();
public:

    future<> rebuild(sstring source_dc);

private:
    void set_mode(mode m);
    // Can only be called on shard-0
    future<> mark_existing_views_as_built();

    // Stream data for which we become a new replica.
    // Before that, if we're not replacing another node, inform other nodes about our chosen tokens
    // and wait for RING_DELAY ms so that we receive new writes from coordinators during streaming.
    future<> bootstrap(std::unordered_set<token>& bootstrap_tokens, std::optional<cdc::generation_id>& cdc_gen_id, const std::optional<replacement_info>& replacement_info);

public:
    future<std::unordered_map<dht::token_range, inet_address_vector_replica_set>> get_range_to_address_map(locator::effective_replication_map_ptr erm) const;
    future<std::unordered_map<dht::token_range, inet_address_vector_replica_set>> get_range_to_address_map(locator::effective_replication_map_ptr erm,
            const std::vector<token>& sorted_tokens) const;

    /**
     * The same as {@code describeRing(String)} but converts TokenRange to the String for JMX compatibility
     *
     * @param keyspace The keyspace to fetch information about
     *
     * @return a List of TokenRange(s) converted to String for the given keyspace
     */

    /*
     * describeRingJMX will be implemented in the API
     * It is left here just as a marker that there is no need to implement it
     * here
     */
    //std::vector<sstring> describeRingJMX(const sstring& keyspace) const {

    future<std::vector<token_range_endpoints>> describe_ring(const sstring& keyspace, bool include_only_local_dc = false) const;

    future<std::vector<dht::token_range_endpoints>> describe_ring_for_table(const sstring& keyspace_name, const sstring& table_name) const;

    /**
     * Retrieve a map of tokens to endpoints, including the bootstrapping ones.
     *
     * @return a map of tokens to endpoints in ascending order
     */
    std::map<token, inet_address> get_token_to_endpoint_map();

    /**
     * Construct the range to endpoint mapping based on the true view
     * of the world.
     * @param ranges
     * @return mapping of ranges to the replicas responsible for them.
    */
    future<std::unordered_map<dht::token_range, inet_address_vector_replica_set>> construct_range_to_endpoint_map(
            locator::effective_replication_map_ptr erm,
            const dht::token_range_vector& ranges) const;
public:
    virtual future<> on_join(gms::inet_address endpoint, gms::endpoint_state_ptr ep_state, gms::permit_id) override;
    /*
     * Handle the reception of a new particular ApplicationState for a particular endpoint. Note that the value of the
     * ApplicationState has not necessarily "changed" since the last known value, if we already received the same update
     * from somewhere else.
     *
     * onChange only ever sees one ApplicationState piece change at a time (even if many ApplicationState updates were
     * received at the same time), so we perform a kind of state machine here. We are concerned with two events: knowing
     * the token associated with an endpoint, and knowing its operation mode. Nodes can start in either bootstrap or
     * normal mode, and from bootstrap mode can change mode to normal. A node in bootstrap mode needs to have
     * pendingranges set in TokenMetadata; a node in normal mode should instead be part of the token ring.
     *
     * Normal progression of ApplicationState.STATUS values for a node should be like this:
     * STATUS_BOOTSTRAPPING,token
     *   if bootstrapping. stays this way until all files are received.
     * STATUS_NORMAL,token
     *   ready to serve reads and writes.
     * STATUS_LEFT,token
     *   set after decommission is completed.
     *
     * Other STATUS values that may be seen (possibly anywhere in the normal progression):
     * REMOVING_TOKEN,deadtoken
     *   set if the node is dead and is being removed by its REMOVAL_COORDINATOR
     * REMOVED_TOKEN,deadtoken
     *   set if the node is dead and has been removed by its REMOVAL_COORDINATOR
     *
     * Note: Any time a node state changes from STATUS_NORMAL, it will not be visible to new nodes. So it follows that
     * you should never bootstrap a new node during a removenode, decommission or move.
     */
    virtual future<> on_change(gms::inet_address endpoint, const gms::application_state_map& states, gms::permit_id) override;
    virtual future<> on_alive(gms::inet_address endpoint, gms::endpoint_state_ptr state, gms::permit_id) override;
    virtual future<> on_dead(gms::inet_address endpoint, gms::endpoint_state_ptr state, gms::permit_id) override;
    virtual future<> on_remove(gms::inet_address endpoint, gms::permit_id) override;
    virtual future<> on_restart(gms::inet_address endpoint, gms::endpoint_state_ptr state, gms::permit_id) override;

public:
    // For migration_listener
    virtual void on_create_keyspace(const sstring& ks_name) override { keyspace_changed(ks_name).get(); }
    virtual void on_create_column_family(const sstring& ks_name, const sstring& cf_name) override {}
    virtual void on_create_user_type(const sstring& ks_name, const sstring& type_name) override {}
    virtual void on_create_function(const sstring& ks_name, const sstring& function_name) override {}
    virtual void on_create_aggregate(const sstring& ks_name, const sstring& aggregate_name) override {}
    virtual void on_create_view(const sstring& ks_name, const sstring& view_name) override {}

    virtual void on_update_keyspace(const sstring& ks_name) override { keyspace_changed(ks_name).get(); }
    virtual void on_update_column_family(const sstring& ks_name, const sstring& cf_name, bool) override {}
    virtual void on_update_user_type(const sstring& ks_name, const sstring& type_name) override {}
    virtual void on_update_function(const sstring& ks_name, const sstring& function_name) override {}
    virtual void on_update_aggregate(const sstring& ks_name, const sstring& aggregate_name) override {}
    virtual void on_update_view(const sstring& ks_name, const sstring& view_name, bool columns_changed) override {}
    virtual void on_update_tablet_metadata() override;

    virtual void on_drop_keyspace(const sstring& ks_name) override { keyspace_changed(ks_name).get(); }
    virtual void on_drop_column_family(const sstring& ks_name, const sstring& cf_name) override {}
    virtual void on_drop_user_type(const sstring& ks_name, const sstring& type_name) override {}
    virtual void on_drop_function(const sstring& ks_name, const sstring& function_name) override {}
    virtual void on_drop_aggregate(const sstring& ks_name, const sstring& aggregate_name) override {}
    virtual void on_drop_view(const sstring& ks_name, const sstring& view_name) override {}
private:
    db::system_keyspace::peer_info get_peer_info_for_update(inet_address endpoint);
    db::system_keyspace::peer_info get_peer_info_for_update(inet_address endpoint, const gms::application_state_map& app_state_map);

    std::unordered_set<token> get_tokens_for(inet_address endpoint);
    std::optional<locator::endpoint_dc_rack> get_dc_rack_for(const gms::endpoint_state& ep_state);
    std::optional<locator::endpoint_dc_rack> get_dc_rack_for(inet_address endpoint);
private:
    // Should be serialized under token_metadata_lock.
    future<> replicate_to_all_cores(mutable_token_metadata_ptr tmptr) noexcept;
    sharded<db::system_keyspace>& _sys_ks;
    sharded<db::system_distributed_keyspace>& _sys_dist_ks;
    locator::snitch_signal_slot_t _snitch_reconfigure;
    sharded<service::tablet_allocator>& _tablet_allocator;
    sharded<cdc::generation_service>& _cdc_gens;
private:
    /**
     * Handle node bootstrap
     *
     * @param endpoint bootstrapping node
     */
    future<> handle_state_bootstrap(inet_address endpoint, gms::permit_id);

    /**
     * Handle node move to normal state. That is, node is entering token ring and participating
     * in reads.
     *
     * @param endpoint node
     */
    future<> handle_state_normal(inet_address endpoint, gms::permit_id);

    /**
     * Handle node leaving the ring. This will happen when a node is decommissioned
     *
     * @param endpoint If reason for leaving is decommission, endpoint is the leaving node.
     * @param pieces STATE_LEFT,token
     */
    future<> handle_state_left(inet_address endpoint, std::vector<sstring> pieces, gms::permit_id);

    /**
     * Handle notification that a node being actively removed from the ring via 'removenode'
     *
     * @param endpoint node
     * @param pieces is REMOVED_TOKEN (node is gone)
     */
    future<> handle_state_removed(inet_address endpoint, std::vector<sstring> pieces, gms::permit_id);

private:
    future<> excise(std::unordered_set<token> tokens, inet_address endpoint, gms::permit_id);
    future<> excise(std::unordered_set<token> tokens, inet_address endpoint, long expire_time, gms::permit_id);

    /** unlike excise we just need this endpoint gone without going through any notifications **/
    future<> remove_endpoint(inet_address endpoint, gms::permit_id pid);

    void add_expire_time_if_found(inet_address endpoint, int64_t expire_time);

    int64_t extract_expire_time(const std::vector<sstring>& pieces) const {
        return std::stoll(pieces[2]);
    }

    /**
     * Finds living endpoints responsible for the given ranges
     *
     * @param erm the keyspace effective_replication_map ranges belong to
     * @param ranges the ranges to find sources for
     * @return multimap of addresses to ranges the address is responsible for
     */
    future<std::unordered_multimap<inet_address, dht::token_range>> get_new_source_ranges(locator::vnode_effective_replication_map_ptr erm, const dht::token_range_vector& ranges) const;

    future<> removenode_with_stream(gms::inet_address leaving_node, frozen_topology_guard, shared_ptr<abort_source> as_ptr);
    future<> removenode_add_ranges(lw_shared_ptr<dht::range_streamer> streamer, gms::inet_address leaving_node);

    // needs to be modified to accept either a keyspace or ARS.
    future<std::unordered_multimap<dht::token_range, inet_address>> get_changed_ranges_for_leaving(locator::vnode_effective_replication_map_ptr erm, inet_address endpoint);

    future<> maybe_reconnect_to_preferred_ip(inet_address ep, inet_address local_ip);
public:

    sstring get_release_version();

    sstring get_schema_version();

    future<std::unordered_map<sstring, std::vector<sstring>>> describe_schema_versions();


    /**
     * Get all ranges an endpoint is responsible for (by keyspace effective_replication_map)
     * Replication strategy's get_ranges() guarantees that no wrap-around range is returned.
     * @param ep endpoint we are interested in.
     * @return ranges for the specified endpoint.
     */
    dht::token_range_vector get_ranges_for_endpoint(const locator::effective_replication_map_ptr& erm, const gms::inet_address& ep) const;

    /**
     * Get all ranges that span the ring given a set
     * of tokens. All ranges are in sorted order of
     * ranges.
     * @return ranges in sorted order
    */
    future<dht::token_range_vector> get_all_ranges(const std::vector<token>& sorted_tokens) const;
    /**
     * This method returns the N endpoints that are responsible for storing the
     * specified key i.e for replication.
     *
     * @param keyspaceName keyspace name also known as keyspace
     * @param cf Column family name
     * @param key key for which we need to find the endpoint
     * @return the endpoint responsible for this key
     */
    inet_address_vector_replica_set get_natural_endpoints(const sstring& keyspace,
            const sstring& cf, const sstring& key) const;

    /**
     * @return Vector of Token ranges (_not_ keys!) together with estimated key count,
     *      breaking up the data this node is responsible for into pieces of roughly keys_per_split
     */
    std::vector<std::pair<dht::token_range, uint64_t>> get_splits(const sstring& ks_name,
            const sstring& cf_name,
            wrapping_interval<dht::token> range,
            uint32_t keys_per_split);
public:
    future<> decommission();

private:
    /**
     * Broadcast leaving status and update local _token_metadata accordingly
     */
    future<> leave_ring();
    future<> unbootstrap();

public:
    future<> move(sstring new_token) {
        // FIXME: getPartitioner().getTokenFactory().validate(newToken);
        return move(dht::token::from_sstring(new_token));
    }

private:
    /**
     * move the node to new token or find a new token to boot to according to load
     *
     * @param newToken new token to boot to, or if null, find balanced token to boot to
     *
     * @throws IOException on any I/O operation error
     */
    future<> move(token new_token);
public:

    /**
     * Get the status of a token removal.
     */
    future<sstring> get_removal_status();

    /**
     * Force a remove operation to complete. This may be necessary if a remove operation
     * blocks forever due to node/stream failure. removeToken() must be called
     * first, this is a last resort measure.  No further attempt will be made to restore replicas.
     */
    future<> force_remove_completion();

public:
    /**
     * Remove a node that has died, attempting to restore the replica count.
     * If the node is alive, decommission should be attempted.  If decommission
     * fails, then removeToken should be called.  If we fail while trying to
     * restore the replica count, finally forceRemoveCompleteion should be
     * called to forcibly remove the node without regard to replica count.
     *
     * @param hostIdString token for the node
     */
    future<> removenode(locator::host_id host_id, std::list<locator::host_id_or_endpoint> ignore_nodes);
    future<node_ops_cmd_response> node_ops_cmd_handler(gms::inet_address coordinator, std::optional<locator::host_id> coordinator_host_id, node_ops_cmd_request req);
    void node_ops_cmd_check(gms::inet_address coordinator, const node_ops_cmd_request& req);
    future<> node_ops_cmd_heartbeat_updater(node_ops_cmd cmd, node_ops_id uuid, std::list<gms::inet_address> nodes, lw_shared_ptr<bool> heartbeat_updater_done);
    void on_node_ops_registered(node_ops_id);

    future<mode> get_operation_mode();

    /**
     * Shuts node off to writes, empties memtables and the commit log.
     * There are two differences between drain and the normal shutdown hook:
     * - Drain waits for in-progress streaming to complete
     * - Drain flushes *all* columnfamilies (shutdown hook only flushes non-durable CFs)
     */
    future<> drain();

    // Recalculates schema digests on this node from contents of tables on disk.
    future<> reload_schema();

    future<std::map<gms::inet_address, float>> get_ownership();

    future<std::map<gms::inet_address, float>> effective_ownership(sstring keyspace_name, sstring table_name);

    // Must run on shard 0.
    future<> check_and_repair_cdc_streams();

private:
    promise<> _drain_finished;
    std::optional<shared_future<>> _transport_stopped;
    future<> do_drain();
    /**
     * Seed data to the endpoints that will be responsible for it at the future
     *
     * @param rangesToStreamByKeyspace keyspaces and data ranges with endpoints included for each
     * @return async Future for whether stream was success
     */
    future<> stream_ranges(std::unordered_map<sstring, std::unordered_multimap<dht::token_range, inet_address>> ranges_to_stream_by_keyspace);

public:
    int32_t get_exception_count();

    template <typename Func>
    auto run_with_api_lock(sstring operation, Func&& func) {
        return container().invoke_on(0, [operation = std::move(operation),
                func = std::forward<Func>(func)] (storage_service& ss) mutable {
            if (!ss._operation_in_progress.empty()) {
                throw std::runtime_error(format("Operation {} is in progress, try again", ss._operation_in_progress));
            }
            ss._operation_in_progress = std::move(operation);
            return func(ss).finally([&ss] {
                ss._operation_in_progress = sstring();
            });
        });
    }

    template <typename Func>
    auto run_with_no_api_lock(Func&& func) {
        return container().invoke_on(0, [func = std::forward<Func>(func)] (storage_service& ss) mutable {
            return func(ss);
        });
    }
private:
    void do_isolate_on_error(disk_error type);
    future<> isolate();

    future<> notify_down(inet_address endpoint);
    future<> notify_left(inet_address endpoint);
    future<> notify_up(inet_address endpoint);
    future<> notify_joined(inet_address endpoint);
    future<> notify_cql_change(inet_address endpoint, bool ready);
    future<> remove_rpc_client_with_ignored_topology(inet_address endpoint);
public:
    future<bool> is_cleanup_allowed(sstring keyspace);
    bool is_repair_based_node_ops_enabled(streaming::stream_reason reason);
    future<> update_fence_version(token_metadata::version_t version);

private:
    std::unordered_set<gms::inet_address> _normal_state_handled_on_boot;
    bool is_normal_state_handled_on_boot(gms::inet_address);
    future<> wait_for_normal_state_handled_on_boot();

    friend class group0_state_machine;

    bool _raft_experimental_topology = false;
    enum class topology_change_kind {
        // The node is still starting and didn't determine yet which ops kind to use
        unknown,
        // The node uses legacy, gossip-based topology operations
        legacy,
        // The node is in the process of upgrading to raft-based topology operations
        upgrading_to_raft,
        // The node uses raft-based topology operations
        raft
    };
    // The _topology_change_kind_enabled variable is first initialized in `join_cluster`.
    // After the node successfully joins, the control over the variable is yielded
    // to `topology_state_load`, so that it can control it during the upgrade from gossiper
    // based topology to raft-based topology.
    // FIXME: This boolean flag is mostly needed because, shortly after starting
    // a new group 0, the state of `system.topology` is empty, and updating the
    // `_topology_change_kind_enabled` variable from group 0 state would lead
    // to wrong results. This could be fixed by writing an initial `system.topology`
    // state before creating the group 0 (also making sure to set the correct
    // group 0 state id so that the timestamps of further mutations are correct).
    bool _manage_topology_change_kind_from_group0 = false;
    topology_change_kind _topology_change_kind_enabled = topology_change_kind::unknown;

    // Throws an exception if the node is either starting and didn't determine which
    // topology operations to use, or if it is in the process of upgrade to topology
    // on raft. The name only serves for display purposes (i.e. it will be included
    // in the exception, if one is thrown).
    void check_ability_to_perform_topology_operation(std::string_view operation_name) const;

    topology_change_kind upgrade_state_to_topology_op_kind(topology::upgrade_state_type upgrade_state) const;

public:
    bool raft_topology_change_enabled() const {
        return _topology_change_kind_enabled == topology_change_kind::raft;
    }
    bool legacy_topology_change_enabled() const {
        return _topology_change_kind_enabled == topology_change_kind::legacy;
    }

private:
    future<> _raft_state_monitor = make_ready_future<>();
    // This fibers monitors raft state and start/stops the topology change
    // coordinator fiber
    future<> raft_state_monitor_fiber(raft::server&, sharded<db::system_distributed_keyspace>& sys_dist_ks);

     // State machine that is responsible for topology change
    topology_state_machine _topology_state_machine;

    future<> _topology_change_coordinator = make_ready_future<>();
    future<> topology_change_coordinator_fiber(raft::server&, raft::term_t, cdc::generation_service&, sharded<db::system_distributed_keyspace>&, abort_source&);

    // Those futures hold results of streaming for various operations
    std::optional<shared_future<>> _bootstrap_result;
    std::optional<shared_future<>> _decommission_result;
    std::optional<shared_future<>> _rebuild_result;
    std::unordered_map<raft::server_id, std::optional<shared_future<>>> _remove_result;
    tablet_op_registry _tablet_ops;
    struct {
        raft::term_t term{0};
        uint64_t last_index{0};
    } _raft_topology_cmd_handler_state;
    class raft_ip_address_updater;
    // Represents a subscription to gossiper on_change events,
    // updating the raft data structures that depend on
    // IP addresses (raft_address_map, token_metadata.topology, erm-s),
    // as well as the system.peers table.
    shared_ptr<raft_ip_address_updater> _raft_ip_address_updater;

    std::unordered_set<raft::server_id> find_raft_nodes_from_hoeps(const std::list<locator::host_id_or_endpoint>& hoeps);

    future<raft_topology_cmd_result> raft_topology_cmd_handler(raft::term_t term, uint64_t cmd_index, const raft_topology_cmd& cmd);

    future<> raft_initialize_discovery_leader(const join_node_request_params& params);
    future<> raft_decommission();
    future<> raft_removenode(locator::host_id host_id, std::list<locator::host_id_or_endpoint> ignore_nodes_params);
    future<> raft_rebuild(sstring source_dc);
    future<> raft_check_and_repair_cdc_streams();
    future<> update_topology_with_local_metadata(raft::server&);

public:
    // This is called on all nodes for each new command received through raft
    // raft_group0_client::_read_apply_mutex must be held
    // Precondition: the topology mutations were already written to disk; the function only transitions the in-memory state machine.
    // Public for `reload_raft_topology_state` REST API.
    future<> topology_transition();

    future<> do_cluster_cleanup();

    // Starts the upgrade procedure to topology on raft.
    // Must be called on shard 0.
    future<> start_upgrade_to_raft_topology();

    // Must be called on shard 0.
    topology::upgrade_state_type get_topology_upgrade_state() const;

    node_state get_node_state(locator::host_id id);

    // Waits for topology state in which none of tablets has replaced_id as a replica.
    // Must be called on shard 0.
    future<> await_tablets_rebuilt(raft::server_id replaced_id);
private:
    // Tracks progress of the upgrade to topology coordinator.
    future<> _upgrade_to_topology_coordinator_fiber = make_ready_future<>();
    future<> track_upgrade_progress_to_topology_coordinator(sharded<db::system_distributed_keyspace>& sys_dist_ks, sharded<service::storage_proxy>& proxy);

public:
    future<> move_tablet(table_id, dht::token, locator::tablet_replica src, locator::tablet_replica dst, loosen_constraints force = loosen_constraints::no);
    future<> set_tablet_balancing_enabled(bool);

    // In the maintenance mode, other nodes won't be available thus we disabled joining
    // the token ring and the token metadata won't be populated with the local node's endpoint.
    // When a CQL query is executed it checks the `token_metadata` structure and fails if it is empty.
    //
    // This method initialises `token_metadata` with the local node as the only node in the token ring.
    // It is incompatible with the `join_cluster` method.
    future<> start_maintenance_mode();

private:
    future<std::vector<canonical_mutation>> get_system_mutations(schema_ptr schema);
    future<std::vector<canonical_mutation>> get_system_mutations(const sstring& ks_name, const sstring& cf_name);
    // Synchronizes the local node state (token_metadata, system.peers/system.local tables,
    // gossiper) to align it with the other raft topology nodes.
    // Optional target_node can be provided to restrict the synchronization to the specified node.
    future<> sync_raft_topology_nodes(mutable_token_metadata_ptr tmptr, std::optional<locator::host_id> target_node, std::unordered_set<raft::server_id> prev_normal);
    // load topology state machine snapshot into memory
    // raft_group0_client::_read_apply_mutex must be held
    future<> topology_state_load();
    // Applies received raft snapshot to local state machine persistent storage
    // raft_group0_client::_read_apply_mutex must be held
    future<> merge_topology_snapshot(raft_topology_snapshot snp);

    std::vector<canonical_mutation> build_mutation_from_join_params(const join_node_request_params& params, service::group0_guard& guard);
    std::unordered_set<raft::server_id> ignored_nodes_from_join_params(const join_node_request_params& params);

    future<join_node_request_result> join_node_request_handler(join_node_request_params params);
    future<join_node_response_result> join_node_response_handler(join_node_response_params params);
    shared_promise<> _join_node_request_done;
    shared_promise<> _join_node_group0_started;
    shared_promise<> _join_node_response_done;
    semaphore _join_node_response_handler_mutex{1};

    future<> _sstable_cleanup_fiber = make_ready_future<>();
    future<> sstable_cleanup_fiber(raft::server& raft, sharded<service::storage_proxy>& proxy) noexcept;
    // Waits for a topology request with a given ID to complete and return non empty error string
    // if request completes with an error
    future<sstring> wait_for_topology_request_completion(utils::UUID id);

    // We need to be able to abort all group0 operation during shutdown, so we need special abort source for that
    abort_source _group0_as;

    friend class join_node_rpc_handshaker;
};

}

template <>
struct fmt::formatter<service::storage_service::mode> : fmt::formatter<std::string_view> {
    template <typename FormatContext>
    auto format(service::storage_service::mode mode, FormatContext& ctx) const {
        std::string_view name;
        using enum service::storage_service::mode;
        switch (mode) {
        case NONE:           name = "STARTING"; break;
        case STARTING:       name = "STARTING"; break;
        case NORMAL:         name = "NORMAL"; break;
        case JOINING:        name = "JOINING"; break;
        case BOOTSTRAP:      name = "BOOTSTRAP"; break;
        case LEAVING:        name = "LEAVING"; break;
        case DECOMMISSIONED: name = "DECOMMISSIONED"; break;
        case MOVING:         name = "MOVING"; break;
        case DRAINING:       name = "DRAINING"; break;
        case DRAINED:        name = "DRAINED"; break;
        case MAINTENANCE:    name = "MAINTENANCE"; break;
        }
        return fmt::format_to(ctx.out(), "{}", name);
    }
};
