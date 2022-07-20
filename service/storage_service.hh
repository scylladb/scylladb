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

#include "gms/i_endpoint_state_change_subscriber.hh"
#include "service/endpoint_lifecycle_subscriber.hh"
#include "locator/abstract_replication_strategy.hh"
#include "inet_address_vectors.hh"
#include <seastar/core/distributed.hh>
#include <seastar/core/condition-variable.hh>
#include "dht/i_partitioner.hh"
#include "dht/token_range_endpoints.hh"
#include <seastar/core/sleep.hh>
#include "gms/application_state.hh"
#include <seastar/core/semaphore.hh>
#include <seastar/core/gate.hh>
#include "utils/fb_utilities.hh"
#include "replica/database_fwd.hh"
#include "db/schema_features.hh"
#include "streaming/stream_state.hh"
#include "streaming/stream_reason.hh"
#include <seastar/core/distributed.hh>
#include "utils/disk-error-handler.hh"
#include "service/migration_listener.hh"
#include "protocol_server.hh"
#include "gms/feature_service.hh"
#include <seastar/core/metrics_registration.hh>
#include <seastar/core/rwlock.hh>
#include "sstables/version.hh"
#include "sstables/shared_sstable.hh"
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/lowres_clock.hh>
#include "locator/snitch_base.hh"
#include "cdc/generation_id.hh"

class node_ops_cmd_request;
class node_ops_cmd_response;
class node_ops_info;
enum class node_ops_cmd : uint32_t;
class repair_service;

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

enum class disk_error { regular, commit };

struct storage_service_config {
    size_t available_memory;
};

class node_ops_meta_data {
    utils::UUID _ops_uuid;
    gms::inet_address _coordinator;
    std::function<future<> ()> _abort;
    shared_ptr<abort_source> _abort_source;
    std::function<void ()> _signal;
    shared_ptr<node_ops_info> _ops;
    seastar::timer<lowres_clock> _watchdog;
    std::chrono::seconds _watchdog_interval{120};
    bool _aborted = false;
public:
    explicit node_ops_meta_data(
            utils::UUID ops_uuid,
            gms::inet_address coordinator,
            shared_ptr<node_ops_info> ops,
            std::function<future<> ()> abort_func,
            std::function<void ()> signal_func);
    shared_ptr<node_ops_info> get_ops_info();
    shared_ptr<abort_source> get_abort_source();
    future<> abort();
    void update_watchdog();
    void cancel_watchdog();
};

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

    abort_source& _abort_source;
    gms::feature_service& _feature_service;
    distributed<replica::database>& _db;
    gms::gossiper& _gossiper;
    sharded<netw::messaging_service>& _messaging;
    sharded<service::migration_manager>& _migration_manager;
    sharded<repair_service>& _repair;
    sharded<streaming::stream_manager>& _stream_manager;

    // Engaged on shard 0 after `join_cluster`.
    service::raft_group0* _group0;

    sstring _operation_in_progress;
    seastar::metrics::metric_groups _metrics;
    using client_shutdown_hook = noncopyable_function<void()>;
    std::vector<protocol_server*> _protocol_servers;
    std::vector<std::any> _listeners;
    gate _async_gate;

    std::unordered_map<utils::UUID, node_ops_meta_data> _node_ops;
    std::list<std::optional<utils::UUID>> _node_ops_abort_queue;
    seastar::condition_variable _node_ops_abort_cond;
    named_semaphore _node_ops_abort_sem{1, named_semaphore_exception_factory{"node_ops_abort_sem"}};
    future<> _node_ops_abort_thread;
    future<> node_ops_update_heartbeat(utils::UUID ops_uuid);
    future<> node_ops_done(utils::UUID ops_uuid);
    future<> node_ops_abort(utils::UUID ops_uuid);
    void node_ops_singal_abort(std::optional<utils::UUID> ops_uuid);
    future<> node_ops_abort_thread();
public:
    storage_service(abort_source& as, distributed<replica::database>& db,
        gms::gossiper& gossiper,
        sharded<db::system_keyspace>&,
        gms::feature_service& feature_service,
        storage_service_config config,
        sharded<service::migration_manager>& mm,
        locator::shared_token_metadata& stm,
        locator::effective_replication_map_factory& erm_factory,
        sharded<netw::messaging_service>& ms,
        sharded<repair_service>& repair,
        sharded<streaming::stream_manager>& stream_manager,
        endpoint_lifecycle_notifier& elc_notif,
        sharded<db::batchlog_manager>& bm);

    // Needed by distributed<>
    future<> stop();
    void init_messaging_service();
    future<> uninit_messaging_service();

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
    future<> update_pending_ranges(mutable_token_metadata_ptr tmptr, sstring reason);
    future<> update_pending_ranges(sstring reason, acquire_merge_lock aml = acquire_merge_lock::yes);
    future<> keyspace_changed(const sstring& ks_name);
    void register_metrics();
    future<> snitch_reconfigured();
    future<> update_topology(inet_address endpoint);

    future<mutable_token_metadata_ptr> get_mutable_token_metadata_ptr() noexcept {
        return get_token_metadata_ptr()->clone_async().then([] (token_metadata tm) {
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

public:

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
    inet_address get_broadcast_address() const {
        return utils::fb_utilities::get_broadcast_address();
    }
    /* This abstraction maintains the token/endpoint metadata information */
    shared_token_metadata& _shared_token_metadata;
    locator::effective_replication_map_factory& _erm_factory;

public:
    std::chrono::milliseconds get_ring_delay();
private:

    std::unordered_set<inet_address> _replicating_nodes;

    std::optional<inet_address> _removing_node;

public:
    enum class mode { NONE, STARTING, JOINING, BOOTSTRAP, NORMAL, LEAVING, DECOMMISSIONED, MOVING, DRAINING, DRAINED };
private:
    mode _operation_mode = mode::NONE;
    friend std::ostream& operator<<(std::ostream& os, const mode& mode);
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
    future<> do_stop_ms();
    future<> shutdown_protocol_servers();

    // Tokens and the CDC streams timestamp of the replaced node.
    using replacement_info = std::unordered_set<token>;
    future<replacement_info> prepare_replacement_info(std::unordered_set<gms::inet_address> initial_contact_nodes,
            const std::unordered_map<gms::inet_address, sstring>& loaded_peer_features);

    void run_replace_ops(std::unordered_set<token>& bootstrap_tokens);
    void run_bootstrap_ops(std::unordered_set<token>& bootstrap_tokens);

    std::list<gms::inet_address> get_ignore_dead_nodes_for_replace();
    future<> wait_for_ring_to_settle(std::chrono::milliseconds delay);

public:

    future<> check_for_endpoint_collision(std::unordered_set<gms::inet_address> initial_contact_nodes,
            const std::unordered_map<gms::inet_address, sstring>& loaded_peer_features);

    /*!
     * \brief Init the messaging service part of the service.
     *
     * This is the first part of the initialization, call this method
     * first.
     *
     * After this method is completed, it is ok to start the storage_service
     * API.
     * \see init_server_without_the_messaging_service_part
     */
    future<> init_messaging_service_part();
    /*!
     * \brief Uninit the messaging service part of the service.
     */
    future<> uninit_messaging_service_part();

    /*!
     * \brief complete the server initialization
     *
     * The storage_service initialization is done in two parts.
     *
     * It is safe to start the API after init_messaging_service_part
     * completed
     *
     * Must be called on shard 0.
     *
     * \see init_messaging_service_part
     */
    future<> join_cluster(cdc::generation_service& cdc_gen_service,
            sharded<db::system_distributed_keyspace>& sys_dist_ks, sharded<service::storage_proxy>& proxy, service::raft_group0&);

    future<> drain_on_shutdown();

    future<> stop_transport();

private:
    bool should_bootstrap();
    std::optional<gms::inet_address> get_replace_address();
    bool is_replacing();
    bool is_first_node();
    future<> join_token_ring(cdc::generation_service& cdc_gen_service,
            sharded<db::system_distributed_keyspace>& sys_dist_ks,
            sharded<service::storage_proxy>& proxy,
            std::unordered_set<gms::inet_address> initial_contact_nodes,
            std::unordered_set<gms::inet_address> loaded_endpoints,
            std::unordered_map<gms::inet_address, sstring> loaded_peer_features,
            std::chrono::milliseconds);
    future<> start_sys_dist_ks();
public:

    future<> rebuild(sstring source_dc);

private:
    void set_mode(mode m);
    future<> mark_existing_views_as_built(sharded<db::system_distributed_keyspace>&);

    // Stream data for which we become a new replica.
    // Before that, if we're not replacing another node, inform other nodes about our chosen tokens
    // and wait for RING_DELAY ms so that we receive new writes from coordinators during streaming.
    future<> bootstrap(cdc::generation_service& cdc_gen_service, std::unordered_set<token>& bootstrap_tokens, std::optional<cdc::generation_id>& cdc_gen_id);

public:
    /**
     * Return the rpc address associated with an endpoint as a string.
     * @param endpoint The endpoint to get rpc address for
     * @return the rpc address
     */
    sstring get_rpc_address(const inet_address& endpoint) const;

    std::unordered_map<dht::token_range, inet_address_vector_replica_set> get_range_to_address_map(const sstring& keyspace) const;

    std::unordered_map<dht::token_range, inet_address_vector_replica_set> get_range_to_address_map_in_local_dc(
            const sstring& keyspace) const;

    std::vector<token> get_tokens_in_local_dc() const;

    std::unordered_map<dht::token_range, inet_address_vector_replica_set> get_range_to_address_map(const sstring& keyspace,
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
    std::unordered_map<dht::token_range, inet_address_vector_replica_set> construct_range_to_endpoint_map(
            const sstring& keyspace,
            const dht::token_range_vector& ranges) const;
public:
    virtual future<> on_join(gms::inet_address endpoint, gms::endpoint_state ep_state) override;
    virtual future<> before_change(gms::inet_address endpoint, gms::endpoint_state current_state, gms::application_state new_state_key, const gms::versioned_value& new_value) override;
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
     * STATUS_LEAVING,token
     *   get ready to leave the cluster as part of a decommission
     * STATUS_LEFT,token
     *   set after decommission is completed.
     *
     * Other STATUS values that may be seen (possibly anywhere in the normal progression):
     * STATUS_MOVING,newtoken
     *   set if node is currently moving to a new token in the ring
     * REMOVING_TOKEN,deadtoken
     *   set if the node is dead and is being removed by its REMOVAL_COORDINATOR
     * REMOVED_TOKEN,deadtoken
     *   set if the node is dead and has been removed by its REMOVAL_COORDINATOR
     *
     * Note: Any time a node state changes from STATUS_NORMAL, it will not be visible to new nodes. So it follows that
     * you should never bootstrap a new node during a removenode, decommission or move.
     */
    virtual future<> on_change(inet_address endpoint, application_state state, const versioned_value& value) override;
    virtual future<> on_alive(gms::inet_address endpoint, gms::endpoint_state state) override;
    virtual future<> on_dead(gms::inet_address endpoint, gms::endpoint_state state) override;
    virtual future<> on_remove(gms::inet_address endpoint) override;
    virtual future<> on_restart(gms::inet_address endpoint, gms::endpoint_state state) override;

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

    virtual void on_drop_keyspace(const sstring& ks_name) override { keyspace_changed(ks_name).get(); }
    virtual void on_drop_column_family(const sstring& ks_name, const sstring& cf_name) override {}
    virtual void on_drop_user_type(const sstring& ks_name, const sstring& type_name) override {}
    virtual void on_drop_function(const sstring& ks_name, const sstring& function_name) override {}
    virtual void on_drop_aggregate(const sstring& ks_name, const sstring& aggregate_name) override {}
    virtual void on_drop_view(const sstring& ks_name, const sstring& view_name) override {}
private:
    template <typename T>
    future<> update_table(gms::inet_address endpoint, sstring col, T value);
    future<> update_peer_info(inet_address endpoint);
    future<> do_update_system_peers_table(gms::inet_address endpoint, const application_state& state, const versioned_value& value);

    std::unordered_set<token> get_tokens_for(inet_address endpoint);
private:
    // Should be serialized under token_metadata_lock.
    future<> replicate_to_all_cores(mutable_token_metadata_ptr tmptr) noexcept;
    sharded<db::system_keyspace>& _sys_ks;
    locator::snitch_signal_slot_t _snitch_reconfigure;
    std::unordered_set<gms::inet_address> _replacing_nodes_pending_ranges_updater;
private:
    /**
     * Handle node bootstrap
     *
     * @param endpoint bootstrapping node
     */
    future<> handle_state_bootstrap(inet_address endpoint);

    /**
     * Handle node move to normal state. That is, node is entering token ring and participating
     * in reads.
     *
     * @param endpoint node
     */
    future<> handle_state_normal(inet_address endpoint);

    /**
     * Handle node preparing to leave the ring
     *
     * @param endpoint node
     */
    future<> handle_state_leaving(inet_address endpoint);

    /**
     * Handle node leaving the ring. This will happen when a node is decommissioned
     *
     * @param endpoint If reason for leaving is decommission, endpoint is the leaving node.
     * @param pieces STATE_LEFT,token
     */
    future<> handle_state_left(inet_address endpoint, std::vector<sstring> pieces);

    /**
     * Handle node moving inside the ring.
     *
     * @param endpoint moving endpoint address
     * @param pieces STATE_MOVING, token
     */
    void handle_state_moving(inet_address endpoint, std::vector<sstring> pieces);

    /**
     * Handle notification that a node being actively removed from the ring via 'removenode'
     *
     * @param endpoint node
     * @param pieces either REMOVED_TOKEN (node is gone) or REMOVING_TOKEN (replicas need to be restored)
     */
    future<> handle_state_removing(inet_address endpoint, std::vector<sstring> pieces);

    /**
     * Handle notification that a node is replacing another node.
     *
     * @param endpoint node
     */
    future<> handle_state_replacing(inet_address endpoint);

    future<>
    handle_state_replacing_update_pending_ranges(mutable_token_metadata_ptr tmptr, inet_address replacing_node);

private:
    future<> excise(std::unordered_set<token> tokens, inet_address endpoint);
    future<> excise(std::unordered_set<token> tokens, inet_address endpoint, long expire_time);

    /** unlike excise we just need this endpoint gone without going through any notifications **/
    future<> remove_endpoint(inet_address endpoint);

    void add_expire_time_if_found(inet_address endpoint, int64_t expire_time);

    int64_t extract_expire_time(const std::vector<sstring>& pieces) const {
        return std::stoll(pieces[2]);
    }

    /**
     * Finds living endpoints responsible for the given ranges
     *
     * @param keyspaceName the keyspace ranges belong to
     * @param ranges the ranges to find sources for
     * @return multimap of addresses to ranges the address is responsible for
     */
    std::unordered_multimap<inet_address, dht::token_range> get_new_source_ranges(const sstring& keyspaceName, const dht::token_range_vector& ranges) const;
public:
    future<> confirm_replication(inet_address node);

private:

    /**
     * Sends a notification to a node indicating we have finished replicating data.
     *
     * @param remote node to send notification to
     */
    future<> send_replication_notification(inet_address remote);

    /**
     * Called when an endpoint is removed from the ring. This function checks
     * whether this node becomes responsible for new ranges as a
     * consequence and streams data if needed.
     *
     * This is rather ineffective, but it does not matter so much
     * since this is called very seldom
     *
     * @param endpoint the node that left
     */
    future<> restore_replica_count(inet_address endpoint, inet_address notify_endpoint);
    future<> removenode_with_stream(gms::inet_address leaving_node, shared_ptr<abort_source> as_ptr);
    future<> removenode_add_ranges(lw_shared_ptr<dht::range_streamer> streamer, gms::inet_address leaving_node);

    // needs to be modified to accept either a keyspace or ARS.
    future<std::unordered_multimap<dht::token_range, inet_address>> get_changed_ranges_for_leaving(sstring keyspace_name, inet_address endpoint);

public:

    sstring get_release_version();

    sstring get_schema_version();

    future<std::unordered_map<sstring, std::vector<sstring>>> describe_schema_versions();


    /**
     * Get all ranges an endpoint is responsible for (by keyspace)
     * Replication strategy's get_ranges() guarantees that no wrap-around range is returned.
     * @param ep endpoint we are interested in.
     * @return ranges for the specified endpoint.
     */
    dht::token_range_vector get_ranges_for_endpoint(const sstring& name, const gms::inet_address& ep) const;

    /**
     * Get all ranges that span the ring given a set
     * of tokens. All ranges are in sorted order of
     * ranges.
     * @return ranges in sorted order
    */
    dht::token_range_vector get_all_ranges(const std::vector<token>& sorted_tokens) const;
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
     * This method returns the N endpoints that are responsible for storing the
     * specified key i.e for replication.
     *
     * @param keyspaceName keyspace name also known as keyspace
     * @param pos position for which we need to find the endpoint
     * @return the endpoint responsible for this token
     */
    inet_address_vector_replica_set  get_natural_endpoints(const sstring& keyspace, const token& pos) const;

    /**
     * @return Vector of Token ranges (_not_ keys!) together with estimated key count,
     *      breaking up the data this node is responsible for into pieces of roughly keys_per_split
     */
    std::vector<std::pair<dht::token_range, uint64_t>> get_splits(const sstring& ks_name,
            const sstring& cf_name,
            range<dht::token> range,
            uint32_t keys_per_split);
public:
    future<> decommission();

private:
    /**
     * Broadcast leaving status and update local _token_metadata accordingly
     */
    future<> start_leaving();
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
    future<> removenode(sstring host_id_string, std::list<gms::inet_address> ignore_nodes);
    future<node_ops_cmd_response> node_ops_cmd_handler(gms::inet_address coordinator, node_ops_cmd_request req);
    void node_ops_cmd_check(gms::inet_address coordinator, const node_ops_cmd_request& req);
    future<> node_ops_cmd_heartbeat_updater(node_ops_cmd cmd, utils::UUID uuid, std::list<gms::inet_address> nodes, lw_shared_ptr<bool> heartbeat_updater_done);

    future<mode> get_operation_mode();

    /**
     * Shuts node off to writes, empties memtables and the commit log.
     * There are two differences between drain and the normal shutdown hook:
     * - Drain waits for in-progress streaming to complete
     * - Drain flushes *all* columnfamilies (shutdown hook only flushes non-durable CFs)
     */
    future<> drain();

    future<std::map<gms::inet_address, float>> get_ownership();

    future<std::map<gms::inet_address, float>> effective_ownership(sstring keyspace_name);

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
public:
    future<bool> is_cleanup_allowed(sstring keyspace);
    bool is_repair_based_node_ops_enabled(streaming::stream_reason reason);
};

}
