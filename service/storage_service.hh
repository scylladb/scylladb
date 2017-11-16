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
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015 ScyllaDB
 *
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

#include "auth/service.hh"
#include "gms/i_endpoint_state_change_subscriber.hh"
#include "service/endpoint_lifecycle_subscriber.hh"
#include "locator/token_metadata.hh"
#include "gms/gossiper.hh"
#include "utils/UUID_gen.hh"
#include "core/distributed.hh"
#include "dht/i_partitioner.hh"
#include "dht/boot_strapper.hh"
#include "dht/token_range_endpoints.hh"
#include "core/sleep.hh"
#include "gms/application_state.hh"
#include "db/system_keyspace.hh"
#include "core/semaphore.hh"
#include "utils/fb_utilities.hh"
#include "utils/serialized_action.hh"
#include "database.hh"
#include "streaming/stream_state.hh"
#include "streaming/stream_plan.hh"
#include <seastar/core/distributed.hh>
#include "disk-error-handler.hh"
#include "gms/feature.hh"

namespace cql_transport {
    class cql_server;
}
class thrift_server;

namespace service {

class load_broadcaster;
class storage_service;

extern distributed<storage_service> _the_storage_service;
inline distributed<storage_service>& get_storage_service() {
    return _the_storage_service;
}
inline storage_service& get_local_storage_service() {
    return _the_storage_service.local();
}

int get_generation_number();

enum class disk_error { regular, commit };

/**
 * This abstraction contains the token/identifier of this node
 * on the identifier space. This token gets gossiped around.
 * This class will also maintain histograms of the load information
 * of other nodes in the cluster.
 */
class storage_service : public service::migration_listener, public gms::i_endpoint_state_change_subscriber, public seastar::async_sharded_service<storage_service> {
public:
    struct snapshot_details {
        int64_t live;
        int64_t total;
        sstring cf;
        sstring ks;
    };
private:
    using token = dht::token;
    using token_range_endpoints = dht::token_range_endpoints;
    using endpoint_details = dht::endpoint_details;
    using boot_strapper = dht::boot_strapper;
    using token_metadata = locator::token_metadata;
    using application_state = gms::application_state;
    using inet_address = gms::inet_address;
    using versioned_value = gms::versioned_value;
#if 0
    private static final Logger logger = LoggerFactory.getLogger(StorageService.class);

    /* JMX notification serial number counter */
    private final AtomicLong notificationSerialNumber = new AtomicLong();
#endif
    distributed<database>& _db;
    sharded<auth::service>& _auth_service;
    int _update_jobs{0};
    // Note that this is obviously only valid for the current shard. Users of
    // this facility should elect a shard to be the coordinator based on any
    // given objective criteria
    //
    // It shouldn't be impossible to actively serialize two callers if the need
    // ever arise.
    bool _loading_new_sstables = false;
    shared_ptr<load_broadcaster> _lb;
    shared_ptr<distributed<cql_transport::cql_server>> _cql_server;
    shared_ptr<distributed<thrift_server>> _thrift_server;
    sstring _operation_in_progress;
    bool _force_remove_completion = false;
    bool _ms_stopped = false;
    bool _stream_manager_stopped = false;
public:
    storage_service(distributed<database>& db, sharded<auth::service>&);
    void isolate_on_error();
    void isolate_on_commit_error();

    // Needed by distributed<>
    future<> stop();
    void init_messaging_service();
    void uninit_messaging_service();

private:
    void do_update_pending_ranges();

public:
    future<> keyspace_changed(const sstring& ks_name);
    future<> update_pending_ranges();

    const locator::token_metadata& get_token_metadata() const {
        return _token_metadata;
    }

    locator::token_metadata& get_token_metadata() {
        return _token_metadata;
    }

    future<> gossip_snitch_info();

    void set_load_broadcaster(shared_ptr<load_broadcaster> lb);
    shared_ptr<load_broadcaster>& get_load_broadcaster();

    distributed<database>& db() {
        return _db;
    }

private:
    bool is_auto_bootstrap();
    inet_address get_broadcast_address() const {
        return utils::fb_utilities::get_broadcast_address();
    }
    /* This abstraction maintains the token/endpoint metadata information */
    token_metadata _token_metadata;
    token_metadata _shadow_token_metadata;
public:
    std::chrono::milliseconds get_ring_delay();
    gms::versioned_value::factory value_factory;
#if 0
    public volatile VersionedValue.VersionedValueFactory valueFactory = new VersionedValue.VersionedValueFactory(getPartitioner());

    private Thread drainOnShutdown = null;

    public static final StorageService instance = new StorageService();

    public static IPartitioner getPartitioner()
    {
        return DatabaseDescriptor.getPartitioner();
    }
#endif
public:
    dht::token_range_vector get_local_ranges(const sstring& keyspace_name) {
        return get_ranges_for_endpoint(keyspace_name, get_broadcast_address());
    }
#if 0
    public Collection<Range<Token>> getPrimaryRanges(String keyspace)
    {
        return getPrimaryRangesForEndpoint(keyspace, FBUtilities.getBroadcastAddress());
    }

    public Collection<Range<Token>> getPrimaryRangesWithinDC(String keyspace)
    {
        return getPrimaryRangeForEndpointWithinDC(keyspace, FBUtilities.getBroadcastAddress());
    }

    private CassandraDaemon daemon;
#endif
private:

    std::unordered_set<inet_address> _replicating_nodes;

    std::experimental::optional<inet_address> _removing_node;

    /* Are we starting this node in bootstrap mode? */
    bool _is_bootstrap_mode;

    /* we bootstrap but do NOT join the ring unless told to do so */
    // FIXME: System.getProperty("cassandra.write_survey", "false")
    bool _is_survey_mode = false;

    bool _initialized;

    bool _joined = false;

public:
    enum class mode { STARTING, NORMAL, JOINING, LEAVING, DECOMMISSIONED, MOVING, DRAINING, DRAINED };
private:
    mode _operation_mode = mode::STARTING;
    friend std::ostream& operator<<(std::ostream& os, const mode& mode);
#if 0
    /* the probability for tracing any particular request, 0 disables tracing and 1 enables for all */
    private double traceProbability = 0.0;
#endif
    /* Used for tracking drain progress */
public:
    struct drain_progress {
        int32_t total_cfs;
        int32_t remaining_cfs;

        drain_progress& operator+=(const drain_progress& other) {
            total_cfs += other.total_cfs;
            remaining_cfs += other.remaining_cfs;
            return *this;
        }
    };
private:
    drain_progress _drain_progress{};
#if 0

    private static final AtomicInteger nextRepairCommand = new AtomicInteger();
#endif


    std::vector<endpoint_lifecycle_subscriber*> _lifecycle_subscribers;

#if 0
    private static final BackgroundActivityMonitor bgMonitor = new BackgroundActivityMonitor();

    private final ObjectName jmxObjectName;

#endif
private:
    std::unordered_set<token> _bootstrap_tokens;

    gms::feature _range_tombstones_feature;
    gms::feature _large_partitions_feature;
    gms::feature _materialized_views_feature;
    gms::feature _counters_feature;
    gms::feature _indexes_feature;
    gms::feature _digest_multipartition_read_feature;
    gms::feature _correct_counter_order_feature;
    gms::feature _schema_tables_v3;
public:
    void enable_all_features() {
        _range_tombstones_feature.enable();
        _large_partitions_feature.enable();
        _materialized_views_feature.enable();
        _counters_feature.enable();
        _indexes_feature.enable();
        _digest_multipartition_read_feature.enable();
        _correct_counter_order_feature.enable();
        _schema_tables_v3.enable();
    }

    void finish_bootstrapping() {
        _is_bootstrap_mode = false;
    }

    /** This method updates the local token on disk  */
    void set_tokens(std::unordered_set<token> tokens);
    void set_gossip_tokens(const std::unordered_set<dht::token>& local_tokens);
#if 0

    public void registerDaemon(CassandraDaemon daemon)
    {
        this.daemon = daemon;
    }
#endif

    void register_subscriber(endpoint_lifecycle_subscriber* subscriber);

    void unregister_subscriber(endpoint_lifecycle_subscriber* subscriber);

    // should only be called via JMX
    future<> stop_gossiping();

    // should only be called via JMX
    future<> start_gossiping();

    // should only be called via JMX
    future<bool> is_gossip_running();

    // should only be called via JMX
    future<> start_rpc_server();

    future<> stop_rpc_server();

    future<bool> is_rpc_server_running();

    future<> start_native_transport();

    future<> stop_native_transport();

    future<bool> is_native_transport_running();

private:
    future<> do_stop_rpc_server();
    future<> do_stop_native_transport();
    future<> do_stop_ms();
    future<> do_stop_stream_manager();
#if 0
    public void stopTransports()
    {
        if (isInitialized())
        {
            logger.error("Stopping gossiper");
            stopGossiping();
        }
        if (isRPCServerRunning())
        {
            logger.error("Stopping RPC server");
            stopRPCServer();
        }
        if (isNativeTransportRunning())
        {
            logger.error("Stopping native transport");
            stopNativeTransport();
        }
    }
#endif
private:
    future<> shutdown_client_servers();
#if 0
    public void stopClient()
    {
        Gossiper.instance.unregister(this);
        Gossiper.instance.stop();
        MessagingService.instance().shutdown();
        // give it a second so that task accepted before the MessagingService shutdown gets submitted to the stage (to avoid RejectedExecutionException)
        Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
        StageManager.shutdownNow();
    }
#endif
public:
    future<bool> is_initialized();
#if 0

    public void stopDaemon()
    {
        if (daemon == null)
            throw new IllegalStateException("No configured daemon");
        daemon.deactivate();
    }
#endif
public:
    future<std::unordered_set<token>> prepare_replacement_info();

    future<> check_for_endpoint_collision();
#if 0

    // for testing only
    public void unsafeInitialize() throws ConfigurationException
    {
        _initialized = true;
        Gossiper.instance.register(this);
        Gossiper.instance.start((int) (System.currentTimeMillis() / 1000)); // needed for node-ring gathering.
        Gossiper.instance.addLocalApplicationState(ApplicationState.NET_VERSION, valueFactory.networkVersion());
        if (!MessagingService.instance().isListening())
            MessagingService.instance().listen(FBUtilities.getLocalAddress());
    }
#endif
public:
    future<> init_server() {
        return init_server(get_ring_delay().count());
    }

    future<> init_server(int delay);

    future<> drain_on_shutdown();

    future<> stop_transport();

    void flush_column_families();
#if 0
    /**
     * In the event of forceful termination we need to remove the shutdown hook to prevent hanging (OOM for instance)
     */
    public void removeShutdownHook()
    {
        if (drainOnShutdown != null)
            Runtime.getRuntime().removeShutdownHook(drainOnShutdown);
    }
#endif
private:
    bool should_bootstrap();
    void prepare_to_join(std::vector<inet_address> loaded_endpoints);
    void register_features();
    void join_token_ring(int delay);
public:
    future<> join_ring();
    bool is_joined();

    future<> rebuild(sstring source_dc);

#if 0
    public void setStreamThroughputMbPerSec(int value)
    {
        DatabaseDescriptor.setStreamThroughputOutboundMegabitsPerSec(value);
        logger.info("setstreamthroughput: throttle set to {}", value);
    }

    public int getStreamThroughputMbPerSec()
    {
        return DatabaseDescriptor.getStreamThroughputOutboundMegabitsPerSec();
    }

    public int getCompactionThroughputMbPerSec()
    {
        return DatabaseDescriptor.getCompactionThroughputMbPerSec();
    }

    public void setCompactionThroughputMbPerSec(int value)
    {
        DatabaseDescriptor.setCompactionThroughputMbPerSec(value);
    }

    public boolean isIncrementalBackupsEnabled()
    {
        return DatabaseDescriptor.isIncrementalBackupsEnabled();
    }

    public void setIncrementalBackupsEnabled(boolean value)
    {
        DatabaseDescriptor.setIncrementalBackupsEnabled(value);
    }
#endif

private:
    void set_mode(mode m, bool log);
    void set_mode(mode m, sstring msg, bool log);
public:
    void bootstrap(std::unordered_set<token> tokens);

    bool is_bootstrap_mode() {
        return _is_bootstrap_mode;
    }

#if 0

    public TokenMetadata getTokenMetadata()
    {
        return _token_metadata;
    }

    /**
     * Increment about the known Compaction severity of the events in this node
     */
    public void reportSeverity(double incr)
    {
        bgMonitor.incrCompactionSeverity(incr);
    }

    public void reportManualSeverity(double incr)
    {
        bgMonitor.incrManualSeverity(incr);
    }

    public double getSeverity(InetAddress endpoint)
    {
        return bgMonitor.getSeverity(endpoint);
    }

    /**
     * for a keyspace, return the ranges and corresponding listen addresses.
     * @param keyspace
     * @return the endpoint map
     */
    public Map<List<String>, List<String>> getRangeToEndpointMap(String keyspace)
    {
        /* All the ranges for the tokens */
        Map<List<String>, List<String>> map = new HashMap<>();
        for (Map.Entry<Range<Token>,List<InetAddress>> entry : getRangeToAddressMap(keyspace).entrySet())
        {
            map.put(entry.getKey().asList(), stringify(entry.getValue()));
        }
        return map;
    }
#endif
    /**
     * Return the rpc address associated with an endpoint as a string.
     * @param endpoint The endpoint to get rpc address for
     * @return the rpc address
     */
    sstring get_rpc_address(const inet_address& endpoint) const;
#if 0
    /**
     * for a keyspace, return the ranges and corresponding RPC addresses for a given keyspace.
     * @param keyspace
     * @return the endpoint map
     */
    public Map<List<String>, List<String>> getRangeToRpcaddressMap(String keyspace)
    {
        /* All the ranges for the tokens */
        Map<List<String>, List<String>> map = new HashMap<>();
        for (Map.Entry<Range<Token>, List<InetAddress>> entry : getRangeToAddressMap(keyspace).entrySet())
        {
            List<String> rpcaddrs = new ArrayList<>(entry.getValue().size());
            for (InetAddress endpoint: entry.getValue())
            {
                rpcaddrs.add(getRpcaddress(endpoint));
            }
            map.put(entry.getKey().asList(), rpcaddrs);
        }
        return map;
    }

    public Map<List<String>, List<String>> getPendingRangeToEndpointMap(String keyspace)
    {
        // some people just want to get a visual representation of things. Allow null and set it to the first
        // non-system keyspace.
        if (keyspace == null)
            keyspace = Schema.instance.getNonSystemKeyspaces().get(0);

        Map<List<String>, List<String>> map = new HashMap<>();
        for (Map.Entry<Range<Token>, Collection<InetAddress>> entry : _token_metadata.getPendingRanges(keyspace).entrySet())
        {
            List<InetAddress> l = new ArrayList<>(entry.getValue());
            map.put(entry.getKey().asList(), stringify(l));
        }
        return map;
    }
#endif
    std::unordered_map<dht::token_range, std::vector<inet_address>> get_range_to_address_map(const sstring& keyspace) const;

    std::unordered_map<dht::token_range, std::vector<inet_address>> get_range_to_address_map_in_local_dc(
            const sstring& keyspace) const;

    std::vector<token> get_tokens_in_local_dc() const;

    bool is_local_dc(const inet_address& targetHost) const;

    std::unordered_map<dht::token_range, std::vector<inet_address>> get_range_to_address_map(const sstring& keyspace,
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

#if 0

    /**
     * The same as {@code describeRing(String)} but considers only the part of the ring formed by nodes in the local DC.
     */
    public List<TokenRange> describeLocalRing(String keyspace) throws InvalidRequestException
    {
        return describeRing(keyspace, true);
    }
#endif
    std::vector<token_range_endpoints> describe_ring(const sstring& keyspace, bool include_only_local_dc = false) const;

    /**
     * Retrieve a map of tokens to endpoints, including the bootstrapping ones.
     *
     * @return a map of tokens to endpoints in ascending order
     */
    std::map<token, inet_address> get_token_to_endpoint_map();

#if 0

    public String getLocalHostId()
    {
        return getTokenMetadata().getHostId(FBUtilities.getBroadcastAddress()).toString();
    }

    public Map<String, String> getHostIdMap()
    {
        Map<String, String> mapOut = new HashMap<>();
        for (Map.Entry<InetAddress, UUID> entry : getTokenMetadata().getEndpointToHostIdMapForReading().entrySet())
            mapOut.put(entry.getKey().getHostAddress(), entry.getValue().toString());
        return mapOut;
    }
#endif
    /**
     * Construct the range to endpoint mapping based on the true view
     * of the world.
     * @param ranges
     * @return mapping of ranges to the replicas responsible for them.
    */
    std::unordered_map<dht::token_range, std::vector<inet_address>> construct_range_to_endpoint_map(
            const sstring& keyspace,
            const dht::token_range_vector& ranges) const;
public:
    virtual void on_join(gms::inet_address endpoint, gms::endpoint_state ep_state) override;
    virtual void before_change(gms::inet_address endpoint, gms::endpoint_state current_state, gms::application_state new_state_key, const gms::versioned_value& new_value) override;
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
    virtual void on_change(inet_address endpoint, application_state state, const versioned_value& value) override;
    virtual void on_alive(gms::inet_address endpoint, gms::endpoint_state state) override;
    virtual void on_dead(gms::inet_address endpoint, gms::endpoint_state state) override;
    virtual void on_remove(gms::inet_address endpoint) override;
    virtual void on_restart(gms::inet_address endpoint, gms::endpoint_state state) override;

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
    void update_peer_info(inet_address endpoint);
    void do_update_system_peers_table(gms::inet_address endpoint, const application_state& state, const versioned_value& value);
    sstring get_application_state_value(inet_address endpoint, application_state appstate);
    std::unordered_set<token> get_tokens_for(inet_address endpoint);
    future<> replicate_to_all_cores();
    future<> do_replicate_to_all_cores();
    serialized_action _replicate_action;
private:
    /**
     * Replicates token_metadata contents on shard0 instance to other shards.
     *
     * Should be serialized.
     * Should run on shard 0 only.
     *
     * @return a ready future when replication is complete.
     */
    future<> replicate_tm_only();

    /**
     * Handle node bootstrap
     *
     * @param endpoint bootstrapping node
     */
    void handle_state_bootstrap(inet_address endpoint);

    /**
     * Handle node move to normal state. That is, node is entering token ring and participating
     * in reads.
     *
     * @param endpoint node
     */
    void handle_state_normal(inet_address endpoint);

    /**
     * Handle node preparing to leave the ring
     *
     * @param endpoint node
     */
    void handle_state_leaving(inet_address endpoint);

    /**
     * Handle node leaving the ring. This will happen when a node is decommissioned
     *
     * @param endpoint If reason for leaving is decommission, endpoint is the leaving node.
     * @param pieces STATE_LEFT,token
     */
    void handle_state_left(inet_address endpoint, std::vector<sstring> pieces);

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
    void handle_state_removing(inet_address endpoint, std::vector<sstring> pieces);

private:
    void excise(std::unordered_set<token> tokens, inet_address endpoint);
    void excise(std::unordered_set<token> tokens, inet_address endpoint, long expire_time);

    /** unlike excise we just need this endpoint gone without going through any notifications **/
    void remove_endpoint(inet_address endpoint);

    void add_expire_time_if_found(inet_address endpoint, int64_t expire_time);

    int64_t extract_expire_time(const std::vector<sstring>& pieces) {
        return std::stoll(pieces[2]);
    }

    /**
     * Finds living endpoints responsible for the given ranges
     *
     * @param keyspaceName the keyspace ranges belong to
     * @param ranges the ranges to find sources for
     * @return multimap of addresses to ranges the address is responsible for
     */
    std::unordered_multimap<inet_address, dht::token_range> get_new_source_ranges(const sstring& keyspaceName, const dht::token_range_vector& ranges);
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

    // needs to be modified to accept either a keyspace or ARS.
    std::unordered_multimap<dht::token_range, inet_address> get_changed_ranges_for_leaving(sstring keyspace_name, inet_address endpoint);
public:
    /** raw load value */
    double get_load();

    sstring get_load_string();

    future<std::map<sstring, double>> get_load_map();

#if 0
    public final void deliverHints(String host) throws UnknownHostException
    {
        HintedHandOffManager.instance.scheduleHintDelivery(host);
    }
#endif
public:
    future<std::unordered_set<dht::token>> get_local_tokens();

#if 0
    /* These methods belong to the MBean interface */

    public List<String> getTokens()
    {
        return getTokens(FBUtilities.getBroadcastAddress());
    }

    public List<String> getTokens(String endpoint) throws UnknownHostException
    {
        return getTokens(InetAddress.getByName(endpoint));
    }

    private List<String> getTokens(InetAddress endpoint)
    {
        List<String> strTokens = new ArrayList<>();
        for (Token tok : getTokenMetadata().getTokens(endpoint))
            strTokens.add(tok.toString());
        return strTokens;
    }
#endif

    sstring get_release_version();

    sstring get_schema_version();

    future<std::unordered_map<sstring, std::vector<sstring>>> describe_schema_versions();

#if 0
    public List<String> getLeavingNodes()
    {
        return stringify(_token_metadata.getLeavingEndpoints());
    }

    public List<String> getMovingNodes()
    {
        List<String> endpoints = new ArrayList<>();

        for (Pair<Token, InetAddress> node : _token_metadata.getMovingEndpoints())
        {
            endpoints.add(node.right.getHostAddress());
        }

        return endpoints;
    }

    public List<String> getJoiningNodes()
    {
        return stringify(_token_metadata.getBootstrapTokens().valueSet());
    }

    public List<String> getLiveNodes()
    {
        return stringify(Gossiper.instance.getLiveMembers());
    }

    public List<String> getUnreachableNodes()
    {
        return stringify(Gossiper.instance.getUnreachableMembers());
    }

    private List<String> stringify(Iterable<InetAddress> endpoints)
    {
        List<String> stringEndpoints = new ArrayList<>();
        for (InetAddress ep : endpoints)
        {
            stringEndpoints.add(ep.getHostAddress());
        }
        return stringEndpoints;
    }

    public int forceKeyspaceCleanup(String keyspaceName, String... columnFamilies) throws IOException, ExecutionException, InterruptedException
    {
        if (keyspaceName.equals(SystemKeyspace.NAME))
            throw new RuntimeException("Cleanup of the system keyspace is neither necessary nor wise");

        CompactionManager.AllSSTableOpStatus status = CompactionManager.AllSSTableOpStatus.SUCCESSFUL;
        for (ColumnFamilyStore cfStore : getValidColumnFamilies(false, false, keyspaceName, columnFamilies))
        {
            CompactionManager.AllSSTableOpStatus oneStatus = cfStore.forceCleanup();
            if (oneStatus != CompactionManager.AllSSTableOpStatus.SUCCESSFUL)
                status = oneStatus;
        }
        return status.statusCode;
    }

    public int scrub(boolean disableSnapshot, boolean skipCorrupted, String keyspaceName, String... columnFamilies) throws IOException, ExecutionException, InterruptedException
    {
        CompactionManager.AllSSTableOpStatus status = CompactionManager.AllSSTableOpStatus.SUCCESSFUL;
        for (ColumnFamilyStore cfStore : getValidColumnFamilies(false, false, keyspaceName, columnFamilies))
        {
            CompactionManager.AllSSTableOpStatus oneStatus = cfStore.scrub(disableSnapshot, skipCorrupted);
            if (oneStatus != CompactionManager.AllSSTableOpStatus.SUCCESSFUL)
                status = oneStatus;
        }
        return status.statusCode;
    }

    public int upgradeSSTables(String keyspaceName, boolean excludeCurrentVersion, String... columnFamilies) throws IOException, ExecutionException, InterruptedException
    {
        CompactionManager.AllSSTableOpStatus status = CompactionManager.AllSSTableOpStatus.SUCCESSFUL;
        for (ColumnFamilyStore cfStore : getValidColumnFamilies(true, true, keyspaceName, columnFamilies))
        {
            CompactionManager.AllSSTableOpStatus oneStatus = cfStore.sstablesRewrite(excludeCurrentVersion);
            if (oneStatus != CompactionManager.AllSSTableOpStatus.SUCCESSFUL)
                status = oneStatus;
        }
        return status.statusCode;
    }

    public void forceKeyspaceCompaction(String keyspaceName, String... columnFamilies) throws IOException, ExecutionException, InterruptedException
    {
        for (ColumnFamilyStore cfStore : getValidColumnFamilies(true, false, keyspaceName, columnFamilies))
        {
            cfStore.forceMajorCompaction();
        }
    }

#endif
    /**
     * Takes the snapshot for all keyspaces. A snapshot name must be specified.
     *
     * @param tag the tag given to the snapshot; may not be null or empty
     */
    future<> take_snapshot(sstring tag) {
        return take_snapshot(tag, {});
    }

    /**
     * Takes the snapshot for the given keyspaces. A snapshot name must be specified.
     *
     * @param tag the tag given to the snapshot; may not be null or empty
     * @param keyspaceNames the names of the keyspaces to snapshot; empty means "all."
     */
    future<> take_snapshot(sstring tag, std::vector<sstring> keyspace_names);

    /**
     * Takes the snapshot of a specific column family. A snapshot name must be specified.
     *
     * @param keyspaceName the keyspace which holds the specified column family
     * @param columnFamilyName the column family to snapshot
     * @param tag the tag given to the snapshot; may not be null or empty
     */
    future<> take_column_family_snapshot(sstring ks_name, sstring cf_name, sstring tag);
#if 0

    private Keyspace getValidKeyspace(String keyspaceName) throws IOException
    {
        if (!Schema.instance.getKeyspaces().contains(keyspaceName))
        {
            throw new IOException("Keyspace " + keyspaceName + " does not exist");
        }
        return Keyspace.open(keyspaceName);
    }
#endif

    /**
     * Remove the snapshot with the given name from the given keyspaces.
     * If no tag is specified we will remove all snapshots.
     */
    future<> clear_snapshot(sstring tag, std::vector<sstring> keyspace_names);

    future<std::unordered_map<sstring, std::vector<snapshot_details>>> get_snapshot_details();

    future<int64_t> true_snapshots_size();
#if 0

    /**
     * @param allowIndexes Allow index CF names to be passed in
     * @param autoAddIndexes Automatically add secondary indexes if a CF has them
     * @param keyspaceName keyspace
     * @param cfNames CFs
     * @throws java.lang.IllegalArgumentException when given CF name does not exist
     */
    public Iterable<ColumnFamilyStore> getValidColumnFamilies(boolean allowIndexes, boolean autoAddIndexes, String keyspaceName, String... cfNames) throws IOException
    {
        Keyspace keyspace = getValidKeyspace(keyspaceName);
        Set<ColumnFamilyStore> valid = new HashSet<>();

        if (cfNames.length == 0)
        {
            // all stores are interesting
            for (ColumnFamilyStore cfStore : keyspace.getColumnFamilyStores())
            {
                valid.add(cfStore);
                if (autoAddIndexes)
                {
                    for (SecondaryIndex si : cfStore.indexManager.getIndexes())
                    {
                        if (si.getIndexCfs() != null) {
                            logger.info("adding secondary index {} to operation", si.getIndexName());
                            valid.add(si.getIndexCfs());
                        }
                    }

                }
            }
            return valid;
        }
        // filter out interesting stores
        for (String cfName : cfNames)
        {
            //if the CF name is an index, just flush the CF that owns the index
            String baseCfName = cfName;
            String idxName = null;
            if (cfName.contains(".")) // secondary index
            {
                if(!allowIndexes)
                {
                   logger.warn("Operation not allowed on secondary Index table ({})", cfName);
                    continue;
                }

                String[] parts = cfName.split("\\.", 2);
                baseCfName = parts[0];
                idxName = parts[1];
            }

            ColumnFamilyStore cfStore = keyspace.getColumnFamilyStore(baseCfName);
            if (idxName != null)
            {
                Collection< SecondaryIndex > indexes = cfStore.indexManager.getIndexesByNames(new HashSet<>(Arrays.asList(cfName)));
                if (indexes.isEmpty())
                    logger.warn(String.format("Invalid index specified: %s/%s. Proceeding with others.", baseCfName, idxName));
                else
                    valid.add(Iterables.get(indexes, 0).getIndexCfs());
            }
            else
            {
                valid.add(cfStore);
                if(autoAddIndexes)
                {
                    for(SecondaryIndex si : cfStore.indexManager.getIndexes())
                    {
                        if (si.getIndexCfs() != null) {
                            logger.info("adding secondary index {} to operation", si.getIndexName());
                            valid.add(si.getIndexCfs());
                        }
                    }
                }
            }
        }
        return valid;
    }

    /**
     * Flush all memtables for a keyspace and column families.
     * @param keyspaceName
     * @param columnFamilies
     * @throws IOException
     */
    public void forceKeyspaceFlush(String keyspaceName, String... columnFamilies) throws IOException
    {
        for (ColumnFamilyStore cfStore : getValidColumnFamilies(true, false, keyspaceName, columnFamilies))
        {
            logger.debug("Forcing flush on keyspace {}, CF {}", keyspaceName, cfStore.name);
            cfStore.forceBlockingFlush();
        }
    }

    /**
     * Sends JMX notification to subscribers.
     *
     * @param type Message type
     * @param message Message itself
     * @param userObject Arbitrary object to attach to notification
     */
    public void sendNotification(String type, String message, Object userObject)
    {
        Notification jmxNotification = new Notification(type, jmxObjectName, notificationSerialNumber.incrementAndGet(), message);
        jmxNotification.setUserData(userObject);
        sendNotification(jmxNotification);
    }

    public int repairAsync(String keyspace, Map<String, String> repairSpec)
    {
        RepairOption option = RepairOption.parse(repairSpec, getPartitioner());
        // if ranges are not specified
        if (option.getRanges().isEmpty())
        {
            if (option.isPrimaryRange())
            {
                // when repairing only primary range, neither dataCenters nor hosts can be set
                if (option.getDataCenters().isEmpty() && option.getHosts().isEmpty())
                    option.getRanges().addAll(getPrimaryRanges(keyspace));
                    // except dataCenters only contain local DC (i.e. -local)
                else if (option.getDataCenters().size() == 1 && option.getDataCenters().contains(DatabaseDescriptor.getLocalDataCenter()))
                    option.getRanges().addAll(getPrimaryRangesWithinDC(keyspace));
                else
                    throw new IllegalArgumentException("You need to run primary range repair on all nodes in the cluster.");
            }
            else
            {
                option.getRanges().addAll(getLocalRanges(keyspace));
            }
        }
        return forceRepairAsync(keyspace, option);
    }

    @Deprecated
    public int forceRepairAsync(String keyspace,
                                boolean isSequential,
                                Collection<String> dataCenters,
                                Collection<String> hosts,
                                boolean primaryRange,
                                boolean fullRepair,
                                String... columnFamilies)
    {
        return forceRepairAsync(keyspace, isSequential ? RepairParallelism.SEQUENTIAL : RepairParallelism.PARALLEL, dataCenters, hosts, primaryRange, fullRepair, columnFamilies);
    }

    @Deprecated
    public int forceRepairAsync(String keyspace,
                                RepairParallelism parallelismDegree,
                                Collection<String> dataCenters,
                                Collection<String> hosts,
                                boolean primaryRange,
                                boolean fullRepair,
                                String... columnFamilies)
    {
        if (FBUtilities.isWindows() && parallelismDegree != RepairParallelism.PARALLEL)
        {
            logger.warn("Snapshot-based repair is not yet supported on Windows.  Reverting to parallel repair.");
            parallelismDegree = RepairParallelism.PARALLEL;
        }

        RepairOption options = new RepairOption(parallelismDegree, primaryRange, !fullRepair, false, 1, Collections.<Range<Token>>emptyList());
        if (dataCenters != null)
        {
            options.getDataCenters().addAll(dataCenters);
        }
        if (hosts != null)
        {
            options.getHosts().addAll(hosts);
        }
        if (columnFamilies != null)
        {
            for (String columnFamily : columnFamilies)
            {
                options.getColumnFamilies().add(columnFamily);
            }
        }
        return forceRepairAsync(keyspace, options);
    }

    public int forceRepairAsync(String keyspace,
                                boolean isSequential,
                                boolean isLocal,
                                boolean primaryRange,
                                boolean fullRepair,
                                String... columnFamilies)
    {
        Set<String> dataCenters = null;
        if (isLocal)
        {
            dataCenters = Sets.newHashSet(DatabaseDescriptor.getLocalDataCenter());
        }
        return forceRepairAsync(keyspace, isSequential, dataCenters, null, primaryRange, fullRepair, columnFamilies);
    }

    public int forceRepairRangeAsync(String beginToken,
                                     String endToken,
                                     String keyspaceName,
                                     boolean isSequential,
                                     Collection<String> dataCenters,
                                     Collection<String> hosts,
                                     boolean fullRepair,
                                     String... columnFamilies)
    {
        return forceRepairRangeAsync(beginToken, endToken, keyspaceName, isSequential ? RepairParallelism.SEQUENTIAL : RepairParallelism.PARALLEL, dataCenters, hosts, fullRepair, columnFamilies);
    }

    public int forceRepairRangeAsync(String beginToken,
                                     String endToken,
                                     String keyspaceName,
                                     RepairParallelism parallelismDegree,
                                     Collection<String> dataCenters,
                                     Collection<String> hosts,
                                     boolean fullRepair,
                                     String... columnFamilies)
    {
        if (FBUtilities.isWindows() && parallelismDegree != RepairParallelism.PARALLEL)
        {
            logger.warn("Snapshot-based repair is not yet supported on Windows.  Reverting to parallel repair.");
            parallelismDegree = RepairParallelism.PARALLEL;
        }
        Collection<Range<Token>> repairingRange = createRepairRangeFrom(beginToken, endToken);

        RepairOption options = new RepairOption(parallelismDegree, false, !fullRepair, false, 1, repairingRange);
        options.getDataCenters().addAll(dataCenters);
        if (hosts != null)
        {
            options.getHosts().addAll(hosts);
        }
        if (columnFamilies != null)
        {
            for (String columnFamily : columnFamilies)
            {
                options.getColumnFamilies().add(columnFamily);
            }
        }

        logger.info("starting user-requested repair of range {} for keyspace {} and column families {}",
                    repairingRange, keyspaceName, columnFamilies);
        return forceRepairAsync(keyspaceName, options);
    }

    public int forceRepairRangeAsync(String beginToken,
                                     String endToken,
                                     String keyspaceName,
                                     boolean isSequential,
                                     boolean isLocal,
                                     boolean fullRepair,
                                     String... columnFamilies)
    {
        Set<String> dataCenters = null;
        if (isLocal)
        {
            dataCenters = Sets.newHashSet(DatabaseDescriptor.getLocalDataCenter());
        }
        return forceRepairRangeAsync(beginToken, endToken, keyspaceName, isSequential, dataCenters, null, fullRepair, columnFamilies);
    }

    /**
     * Create collection of ranges that match ring layout from given tokens.
     *
     * @param beginToken beginning token of the range
     * @param endToken end token of the range
     * @return collection of ranges that match ring layout in TokenMetadata
     */
    @SuppressWarnings("unchecked")
    @VisibleForTesting
    Collection<Range<Token>> createRepairRangeFrom(String beginToken, String endToken)
    {
        Token parsedBeginToken = getPartitioner().getTokenFactory().fromString(beginToken);
        Token parsedEndToken = getPartitioner().getTokenFactory().fromString(endToken);

        // Break up given range to match ring layout in TokenMetadata
        ArrayList<Range<Token>> repairingRange = new ArrayList<>();

        ArrayList<Token> tokens = new ArrayList<>(_token_metadata.sortedTokens());
        if (!tokens.contains(parsedBeginToken))
        {
            tokens.add(parsedBeginToken);
        }
        if (!tokens.contains(parsedEndToken))
        {
            tokens.add(parsedEndToken);
        }
        // tokens now contain all tokens including our endpoints
        Collections.sort(tokens);

        int start = tokens.indexOf(parsedBeginToken), end = tokens.indexOf(parsedEndToken);
        for (int i = start; i != end; i = (i+1) % tokens.size())
        {
            Range<Token> range = new Range<>(tokens.get(i), tokens.get((i+1) % tokens.size()));
            repairingRange.add(range);
        }

        return repairingRange;
    }

    public int forceRepairAsync(String keyspace, RepairOption options)
    {
        if (options.getRanges().isEmpty() || Keyspace.open(keyspace).getReplicationStrategy().getReplicationFactor() < 2)
            return 0;

        int cmd = nextRepairCommand.incrementAndGet();
        new Thread(createRepairTask(cmd, keyspace, options)).start();
        return cmd;
    }

    private Thread createQueryThread(final int cmd, final UUID sessionId)
    {
        return new Thread(new WrappedRunnable()
        {
            // Query events within a time interval that overlaps the last by one second. Ignore duplicates. Ignore local traces.
            // Wake up upon local trace activity. Query when notified of trace activity with a timeout that doubles every two timeouts.
            public void runMayThrow() throws Exception
            {
                TraceState state = Tracing.instance.get(sessionId);
                if (state == null)
                    throw new Exception("no tracestate");

                String format = "select event_id, source, activity from %s.%s where session_id = ? and event_id > ? and event_id < ?;";
                String query = String.format(format, TraceKeyspace.NAME, TraceKeyspace.EVENTS);
                SelectStatement statement = (SelectStatement) QueryProcessor.parseStatement(query).prepare().statement;

                ByteBuffer sessionIdBytes = ByteBufferUtil.bytes(sessionId);
                InetAddress source = FBUtilities.getBroadcastAddress();

                HashSet<UUID>[] seen = new HashSet[] { new HashSet<UUID>(), new HashSet<UUID>() };
                int si = 0;
                UUID uuid;

                long tlast = System.currentTimeMillis(), tcur;

                TraceState.Status status;
                long minWaitMillis = 125;
                long maxWaitMillis = 1000 * 1024L;
                long timeout = minWaitMillis;
                boolean shouldDouble = false;

                while ((status = state.waitActivity(timeout)) != TraceState.Status.STOPPED)
                {
                    if (status == TraceState.Status.IDLE)
                    {
                        timeout = shouldDouble ? Math.min(timeout * 2, maxWaitMillis) : timeout;
                        shouldDouble = !shouldDouble;
                    }
                    else
                    {
                        timeout = minWaitMillis;
                        shouldDouble = false;
                    }
                    ByteBuffer tminBytes = ByteBufferUtil.bytes(UUIDGen.minTimeUUID(tlast - 1000));
                    ByteBuffer tmaxBytes = ByteBufferUtil.bytes(UUIDGen.maxTimeUUID(tcur = System.currentTimeMillis()));
                    QueryOptions options = QueryOptions.forInternalCalls(ConsistencyLevel.ONE, Lists.newArrayList(sessionIdBytes, tminBytes, tmaxBytes));
                    ResultMessage.Rows rows = statement.execute(QueryState.forInternalCalls(), options);
                    UntypedResultSet result = UntypedResultSet.create(rows.result);

                    for (UntypedResultSet.Row r : result)
                    {
                        if (source.equals(r.getInetAddress("source")))
                            continue;
                        if ((uuid = r.getUUID("event_id")).timestamp() > (tcur - 1000) * 10000)
                            seen[si].add(uuid);
                        if (seen[si == 0 ? 1 : 0].contains(uuid))
                            continue;
                        String message = String.format("%s: %s", r.getInetAddress("source"), r.getString("activity"));
                        sendNotification("repair", message, new int[]{cmd, ActiveRepairService.Status.RUNNING.ordinal()});
                    }
                    tlast = tcur;

                    si = si == 0 ? 1 : 0;
                    seen[si].clear();
                }
            }
        });
    }

    private FutureTask<Object> createRepairTask(final int cmd, final String keyspace, final RepairOption options)
    {
        if (!options.getDataCenters().isEmpty() && options.getDataCenters().contains(DatabaseDescriptor.getLocalDataCenter()))
        {
            throw new IllegalArgumentException("the local data center must be part of the repair");
        }

        return new FutureTask<>(new WrappedRunnable()
        {
            protected void runMayThrow() throws Exception
            {
                final TraceState traceState;

                String[] columnFamilies = options.getColumnFamilies().toArray(new String[options.getColumnFamilies().size()]);
                Iterable<ColumnFamilyStore> validColumnFamilies = getValidColumnFamilies(false, false, keyspace, columnFamilies);

                final long startTime = System.currentTimeMillis();
                String message = String.format("Starting repair command #%d, repairing keyspace %s with %s", cmd, keyspace, options);
                logger.info(message);
                sendNotification("repair", message, new int[]{cmd, ActiveRepairService.Status.STARTED.ordinal()});
                if (options.isTraced())
                {
                    StringBuilder cfsb = new StringBuilder();
                    for (ColumnFamilyStore cfs : validColumnFamilies)
                        cfsb.append(", ").append(cfs.keyspace.getName()).append(".").append(cfs.name);

                    UUID sessionId = Tracing.instance.newSession(Tracing.TraceType.REPAIR);
                    traceState = Tracing.instance.begin("repair", ImmutableMap.of("keyspace", keyspace, "columnFamilies", cfsb.substring(2)));
                    Tracing.traceRepair(message);
                    traceState.enableActivityNotification();
                    traceState.setNotificationHandle(new int[]{ cmd, ActiveRepairService.Status.RUNNING.ordinal() });
                    Thread queryThread = createQueryThread(cmd, sessionId);
                    queryThread.setName("RepairTracePolling");
                    queryThread.start();
                }
                else
                {
                    traceState = null;
                }

                final Set<InetAddress> allNeighbors = new HashSet<>();
                Map<Range, Set<InetAddress>> rangeToNeighbors = new HashMap<>();
                for (Range<Token> range : options.getRanges())
                {
                    try
                    {
                        Set<InetAddress> neighbors = ActiveRepairService.getNeighbors(keyspace, range, options.getDataCenters(), options.getHosts());
                        rangeToNeighbors.put(range, neighbors);
                        allNeighbors.addAll(neighbors);
                    }
                    catch (IllegalArgumentException e)
                    {
                        logger.error("Repair failed:", e);
                        sendNotification("repair", e.getMessage(), new int[]{cmd, ActiveRepairService.Status.FINISHED.ordinal()});
                        return;
                    }
                }

                // Validate columnfamilies
                List<ColumnFamilyStore> columnFamilyStores = new ArrayList<>();
                try
                {
                    Iterables.addAll(columnFamilyStores, validColumnFamilies);
                }
                catch (IllegalArgumentException e)
                {
                    sendNotification("repair", e.getMessage(), new int[]{cmd, ActiveRepairService.Status.FINISHED.ordinal()});
                    return;
                }

                final UUID parentSession;
                long repairedAt;
                try
                {
                    parentSession = ActiveRepairService.instance.prepareForRepair(allNeighbors, options, columnFamilyStores);
                    repairedAt = ActiveRepairService.instance.getParentRepairSession(parentSession).repairedAt;
                }
                catch (Throwable t)
                {
                    sendNotification("repair", String.format("Repair failed with error %s", t.getMessage()), new int[]{cmd, ActiveRepairService.Status.FINISHED.ordinal()});
                    return;
                }

                // Set up RepairJob executor for this repair command.
                final ListeningExecutorService executor = MoreExecutors.listeningDecorator(new JMXConfigurableThreadPoolExecutor(options.getJobThreads(),
                                                                                                                           Integer.MAX_VALUE,
                                                                                                                           TimeUnit.SECONDS,
                                                                                                                           new LinkedBlockingQueue<Runnable>(),
                                                                                                                           new NamedThreadFactory("Repair#" + cmd),
                                                                                                                           "internal"));

                List<ListenableFuture<RepairSessionResult>> futures = new ArrayList<>(options.getRanges().size());
                String[] cfnames = new String[columnFamilyStores.size()];
                for (int i = 0; i < columnFamilyStores.size(); i++)
                {
                    cfnames[i] = columnFamilyStores.get(i).name;
                }
                for (Range<Token> range : options.getRanges())
                {
                    final RepairSession session = ActiveRepairService.instance.submitRepairSession(parentSession,
                                                                      range,
                                                                      keyspace,
                                                                      options.getParallelism(),
                                                                      rangeToNeighbors.get(range),
                                                                      repairedAt,
                                                                      executor,
                                                                      cfnames);
                    if (session == null)
                        continue;
                    // After repair session completes, notify client its result
                    Futures.addCallback(session, new FutureCallback<RepairSessionResult>()
                    {
                        public void onSuccess(RepairSessionResult result)
                        {
                            String message = String.format("Repair session %s for range %s finished", session.getId(), session.getRange().toString());
                            logger.info(message);
                            sendNotification("repair", message, new int[]{cmd, ActiveRepairService.Status.SESSION_SUCCESS.ordinal()});
                        }

                        public void onFailure(Throwable t)
                        {
                            String message = String.format("Repair session %s for range %s failed with error %s", session.getId(), session.getRange().toString(), t.getMessage());
                            logger.error(message, t);
                            sendNotification("repair", message, new int[]{cmd, ActiveRepairService.Status.SESSION_FAILED.ordinal()});
                        }
                    });
                    futures.add(session);
                }

                // After all repair sessions completes(successful or not),
                // run anticompaction if necessary and send finish notice back to client
                final ListenableFuture<List<RepairSessionResult>> allSessions = Futures.successfulAsList(futures);
                Futures.addCallback(allSessions, new FutureCallback<List<RepairSessionResult>>()
                {
                    public void onSuccess(List<RepairSessionResult> result)
                    {
                        // filter out null(=failed) results and get successful ranges
                        Collection<Range<Token>> successfulRanges = new ArrayList<>();
                        for (RepairSessionResult sessionResult : result)
                        {
                            if (sessionResult != null)
                            {
                                successfulRanges.add(sessionResult.range);
                            }
                        }
                        try
                        {
                            ActiveRepairService.instance.finishParentSession(parentSession, allNeighbors, successfulRanges);
                        }
                        catch (Exception e)
                        {
                            logger.error("Error in incremental repair", e);
                        }
                        repairComplete();
                    }

                    public void onFailure(Throwable t)
                    {
                        repairComplete();
                    }

                    private void repairComplete()
                    {
                        String duration = DurationFormatUtils.formatDurationWords(System.currentTimeMillis() - startTime, true, true);
                        String message = String.format("Repair command #%d finished in %s", cmd, duration);
                        sendNotification("repair", message,
                                         new int[]{cmd, ActiveRepairService.Status.FINISHED.ordinal()});
                        logger.info(message);
                        if (options.isTraced())
                        {
                            traceState.setNotificationHandle(null);
                            // Because DebuggableThreadPoolExecutor#afterExecute and this callback
                            // run in a nondeterministic order (within the same thread), the
                            // TraceState may have been nulled out at this point. The TraceState
                            // should be traceState, so just set it without bothering to check if it
                            // actually was nulled out.
                            Tracing.instance.set(traceState);
                            Tracing.traceRepair(message);
                            Tracing.instance.stopSession();
                        }
                        executor.shutdownNow();
                    }
                });
            }
        }, null);
    }

    public void forceTerminateAllRepairSessions() {
        ActiveRepairService.instance.terminateSessions();
    }

    /* End of MBean interface methods */

    /**
     * Get the "primary ranges" for the specified keyspace and endpoint.
     * "Primary ranges" are the ranges that the node is responsible for storing replica primarily.
     * The node that stores replica primarily is defined as the first node returned
     * by {@link AbstractReplicationStrategy#calculateNaturalEndpoints}.
     *
     * @param keyspace Keyspace name to check primary ranges
     * @param ep endpoint we are interested in.
     * @return primary ranges for the specified endpoint.
     */
    public Collection<Range<Token>> getPrimaryRangesForEndpoint(String keyspace, InetAddress ep)
    {
        AbstractReplicationStrategy strategy = Keyspace.open(keyspace).getReplicationStrategy();
        Collection<Range<Token>> primaryRanges = new HashSet<>();
        TokenMetadata metadata = _token_metadata.cloneOnlyTokenMap();
        for (Token token : metadata.sortedTokens())
        {
            List<InetAddress> endpoints = strategy.calculateNaturalEndpoints(token, metadata);
            if (endpoints.size() > 0 && endpoints.get(0).equals(ep))
                primaryRanges.add(new Range<>(metadata.getPredecessor(token), token));
        }
        return primaryRanges;
    }

    /**
     * Get the "primary ranges" within local DC for the specified keyspace and endpoint.
     *
     * @see #getPrimaryRangesForEndpoint(String, java.net.InetAddress)
     * @param keyspace Keyspace name to check primary ranges
     * @param referenceEndpoint endpoint we are interested in.
     * @return primary ranges within local DC for the specified endpoint.
     */
    public Collection<Range<Token>> getPrimaryRangeForEndpointWithinDC(String keyspace, InetAddress referenceEndpoint)
    {
        TokenMetadata metadata = _token_metadata.cloneOnlyTokenMap();
        String localDC = DatabaseDescriptor.getEndpointSnitch().getDatacenter(referenceEndpoint);
        Collection<InetAddress> localDcNodes = metadata.getTopology().getDatacenterEndpoints().get(localDC);
        AbstractReplicationStrategy strategy = Keyspace.open(keyspace).getReplicationStrategy();

        Collection<Range<Token>> localDCPrimaryRanges = new HashSet<>();
        for (Token token : metadata.sortedTokens())
        {
            List<InetAddress> endpoints = strategy.calculateNaturalEndpoints(token, metadata);
            for (InetAddress endpoint : endpoints)
            {
                if (localDcNodes.contains(endpoint))
                {
                    if (endpoint.equals(referenceEndpoint))
                    {
                        localDCPrimaryRanges.add(new Range<>(metadata.getPredecessor(token), token));
                    }
                    break;
                }
            }
        }

        return localDCPrimaryRanges;
    }
#endif
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
    std::vector<gms::inet_address> get_natural_endpoints(const sstring& keyspace,
            const sstring& cf, const sstring& key) const;
#if 0
    public List<InetAddress> getNaturalEndpoints(String keyspaceName, ByteBuffer key)
    {
        return getNaturalEndpoints(keyspaceName, getPartitioner().getToken(key));
    }
#endif
    /**
     * This method returns the N endpoints that are responsible for storing the
     * specified key i.e for replication.
     *
     * @param keyspaceName keyspace name also known as keyspace
     * @param pos position for which we need to find the endpoint
     * @return the endpoint responsible for this token
     */
    std::vector<gms::inet_address>  get_natural_endpoints(const sstring& keyspace, const token& pos) const;
#if 0
    /**
     * This method attempts to return N endpoints that are responsible for storing the
     * specified key i.e for replication.
     *
     * @param keyspace keyspace name also known as keyspace
     * @param key key for which we need to find the endpoint
     * @return the endpoint responsible for this key
     */
    public List<InetAddress> getLiveNaturalEndpoints(Keyspace keyspace, ByteBuffer key)
    {
        return getLiveNaturalEndpoints(keyspace, getPartitioner().decorateKey(key));
    }

    public List<InetAddress> getLiveNaturalEndpoints(Keyspace keyspace, RingPosition pos)
    {
        List<InetAddress> endpoints = keyspace.getReplicationStrategy().getNaturalEndpoints(pos);
        List<InetAddress> liveEps = new ArrayList<>(endpoints.size());

        for (InetAddress endpoint : endpoints)
        {
            if (FailureDetector.instance.isAlive(endpoint))
                liveEps.add(endpoint);
        }

        return liveEps;
    }

    public void setLoggingLevel(String classQualifier, String rawLevel) throws Exception
    {
        ch.qos.logback.classic.Logger logBackLogger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(classQualifier);

        // if both classQualifer and rawLevel are empty, reload from configuration
        if (StringUtils.isBlank(classQualifier) && StringUtils.isBlank(rawLevel) )
        {
            JMXConfiguratorMBean jmxConfiguratorMBean = JMX.newMBeanProxy(ManagementFactory.getPlatformMBeanServer(),
                    new ObjectName("ch.qos.logback.classic:Name=default,Type=ch.qos.logback.classic.jmx.JMXConfigurator"),
                    JMXConfiguratorMBean.class);
            jmxConfiguratorMBean.reloadDefaultConfiguration();
            return;
        }
        // classQualifer is set, but blank level given
        else if (StringUtils.isNotBlank(classQualifier) && StringUtils.isBlank(rawLevel) )
        {
            if (logBackLogger.getLevel() != null || hasAppenders(logBackLogger))
                logBackLogger.setLevel(null);
            return;
        }

        ch.qos.logback.classic.Level level = ch.qos.logback.classic.Level.toLevel(rawLevel);
        logBackLogger.setLevel(level);
        logger.info("set log level to {} for classes under '{}' (if the level doesn't look like '{}' then the logger couldn't parse '{}')", level, classQualifier, rawLevel, rawLevel);
    }

    /**
     * @return the runtime logging levels for all the configured loggers
     */
    @Override
    public Map<String,String>getLoggingLevels() {
        Map<String, String> logLevelMaps = Maps.newLinkedHashMap();
        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
        for (ch.qos.logback.classic.Logger logger : lc.getLoggerList())
        {
            if(logger.getLevel() != null || hasAppenders(logger))
                logLevelMaps.put(logger.getName(), logger.getLevel().toString());
        }
        return logLevelMaps;
    }

    private boolean hasAppenders(ch.qos.logback.classic.Logger logger) {
        Iterator<Appender<ILoggingEvent>> it = logger.iteratorForAppenders();
        return it.hasNext();
    }
#endif
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
    void leave_ring();
    void unbootstrap();

public:
    future<> move(sstring new_token) {
        // FIXME: getPartitioner().getTokenFactory().validate(newToken);
        return move(dht::global_partitioner().from_sstring(new_token));
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

    class range_relocator {
    private:
        streaming::stream_plan _stream_plan;

    public:
        range_relocator(std::unordered_set<token> tokens, std::vector<sstring> keyspace_names)
            : _stream_plan("Relocation") {
            calculate_to_from_streams(std::move(tokens), std::move(keyspace_names));
        }

    private:
        void calculate_to_from_streams(std::unordered_set<token> new_tokens, std::vector<sstring> keyspace_names);

    public:
        future<> stream() {
            return _stream_plan.execute().discard_result();
        }

        bool streams_needed() {
            return !_stream_plan.is_empty();
        }
    };


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
    future<> removenode(sstring host_id_string);

    future<sstring> get_operation_mode();

    future<bool> is_starting();

    drain_progress get_drain_progress() const {
        return _drain_progress;
    }

    /**
     * Shuts node off to writes, empties memtables and the commit log.
     * There are two differences between drain and the normal shutdown hook:
     * - Drain waits for in-progress streaming to complete
     * - Drain flushes *all* columnfamilies (shutdown hook only flushes non-durable CFs)
     */
    future<> drain();

#if 0
    // Never ever do this at home. Used by tests.
    IPartitioner setPartitionerUnsafe(IPartitioner newPartitioner)
    {
        IPartitioner oldPartitioner = DatabaseDescriptor.getPartitioner();
        DatabaseDescriptor.setPartitioner(newPartitioner);
        valueFactory = new VersionedValue.VersionedValueFactory(getPartitioner());
        return oldPartitioner;
    }

    TokenMetadata setTokenMetadataUnsafe(TokenMetadata tmd)
    {
        TokenMetadata old = _token_metadata;
        _token_metadata = tmd;
        return old;
    }

    public void truncate(String keyspace, String columnFamily) throws TimeoutException, IOException
    {
        try
        {
            StorageProxy.truncateBlocking(keyspace, columnFamily);
        }
        catch (UnavailableException e)
        {
            throw new IOException(e.getMessage());
        }
    }
#endif
public:
    future<std::map<gms::inet_address, float>> get_ownership();

    future<std::map<gms::inet_address, float>> effective_ownership(sstring keyspace_name);
#if 0
    /**
     * Calculates ownership. If there are multiple DC's and the replication strategy is DC aware then ownership will be
     * calculated per dc, i.e. each DC will have total ring ownership divided amongst its nodes. Without replication
     * total ownership will be a multiple of the number of DC's and this value will then go up within each DC depending
     * on the number of replicas within itself. For DC unaware replication strategies, ownership without replication
     * will be 100%.
     *
     * @throws IllegalStateException when node is not configured properly.
     */
    public LinkedHashMap<InetAddress, Float> effectiveOwnership(String keyspace) throws IllegalStateException
    {
    	
    	if (keyspace != null)
    	{
    		Keyspace keyspaceInstance = Schema.instance.getKeyspaceInstance(keyspace);
			if(keyspaceInstance == null)
				throw new IllegalArgumentException("The keyspace " + keyspace + ", does not exist");
    		
    		if(keyspaceInstance.getReplicationStrategy() instanceof LocalStrategy)
				throw new IllegalStateException("Ownership values for keyspaces with LocalStrategy are meaningless");
    	}
    	else
    	{
        	List<String> nonSystemKeyspaces = Schema.instance.getNonSystemKeyspaces();
        	
        	//system_traces is a non-system keyspace however it needs to be counted as one for this process
        	int specialTableCount = 0;
        	if (nonSystemKeyspaces.contains("system_traces"))
			{
        		specialTableCount += 1;
			}
        	if (nonSystemKeyspaces.size() > specialTableCount) 	   		
        		throw new IllegalStateException("Non-system keyspaces don't have the same replication settings, effective ownership information is meaningless");
        	
        	keyspace = "system_traces";
    	}
    	
        TokenMetadata metadata = _token_metadata.cloneOnlyTokenMap();

        Collection<Collection<InetAddress>> endpointsGroupedByDc = new ArrayList<>();
        // mapping of dc's to nodes, use sorted map so that we get dcs sorted
        SortedMap<String, Collection<InetAddress>> sortedDcsToEndpoints = new TreeMap<>();
        sortedDcsToEndpoints.putAll(metadata.getTopology().getDatacenterEndpoints().asMap());
        for (Collection<InetAddress> endpoints : sortedDcsToEndpoints.values())
            endpointsGroupedByDc.add(endpoints);

        Map<Token, Float> tokenOwnership = getPartitioner().describeOwnership(_token_metadata.sortedTokens());
        LinkedHashMap<InetAddress, Float> finalOwnership = Maps.newLinkedHashMap();

        // calculate ownership per dc
        for (Collection<InetAddress> endpoints : endpointsGroupedByDc)
        {
            // calculate the ownership with replication and add the endpoint to the final ownership map
            for (InetAddress endpoint : endpoints)
            {
                float ownership = 0.0f;
                for (Range<Token> range : getRangesForEndpoint(keyspace, endpoint))
                {
                    if (tokenOwnership.containsKey(range.right))
                        ownership += tokenOwnership.get(range.right);
                }
                finalOwnership.put(endpoint, ownership);
            }
        }
        return finalOwnership;
    }


    private boolean hasSameReplication(List<String> list)
    {
        if (list.isEmpty())
            return false;

        for (int i = 0; i < list.size() -1; i++)
        {
            KSMetaData ksm1 = Schema.instance.getKSMetaData(list.get(i));
            KSMetaData ksm2 = Schema.instance.getKSMetaData(list.get(i + 1));
            if (!ksm1.strategyClass.equals(ksm2.strategyClass) ||
                    !Iterators.elementsEqual(ksm1.strategyOptions.entrySet().iterator(),
                                             ksm2.strategyOptions.entrySet().iterator()))
                return false;
        }
        return true;
    }

    public List<String> getKeyspaces()
    {
        List<String> keyspaceNamesList = new ArrayList<>(Schema.instance.getKeyspaces());
        return Collections.unmodifiableList(keyspaceNamesList);
    }

    public List<String> getNonSystemKeyspaces()
    {
        List<String> keyspaceNamesList = new ArrayList<>(Schema.instance.getNonSystemKeyspaces());
        return Collections.unmodifiableList(keyspaceNamesList);
    }

    public void updateSnitch(String epSnitchClassName, Boolean dynamic, Integer dynamicUpdateInterval, Integer dynamicResetInterval, Double dynamicBadnessThreshold) throws ClassNotFoundException
    {
        IEndpointSnitch oldSnitch = DatabaseDescriptor.getEndpointSnitch();

        // new snitch registers mbean during construction
        IEndpointSnitch newSnitch;
        try
        {
            newSnitch = FBUtilities.construct(epSnitchClassName, "snitch");
        }
        catch (ConfigurationException e)
        {
            throw new ClassNotFoundException(e.getMessage());
        }
        if (dynamic)
        {
            DatabaseDescriptor.setDynamicUpdateInterval(dynamicUpdateInterval);
            DatabaseDescriptor.setDynamicResetInterval(dynamicResetInterval);
            DatabaseDescriptor.setDynamicBadnessThreshold(dynamicBadnessThreshold);
            newSnitch = new DynamicEndpointSnitch(newSnitch);
        }

        // point snitch references to the new instance
        DatabaseDescriptor.setEndpointSnitch(newSnitch);
        for (String ks : Schema.instance.getKeyspaces())
        {
            Keyspace.open(ks).getReplicationStrategy().snitch = newSnitch;
        }

        if (oldSnitch instanceof DynamicEndpointSnitch)
            ((DynamicEndpointSnitch)oldSnitch).unregisterMBean();
    }
#endif

private:
    /**
     * Seed data to the endpoints that will be responsible for it at the future
     *
     * @param rangesToStreamByKeyspace keyspaces and data ranges with endpoints included for each
     * @return async Future for whether stream was success
     */
    future<> stream_ranges(std::unordered_map<sstring, std::unordered_multimap<dht::token_range, inet_address>> ranges_to_stream_by_keyspace);

public:
    /**
     * Calculate pair of ranges to stream/fetch for given two range collections
     * (current ranges for keyspace and ranges after move to new token)
     *
     * @param current collection of the ranges by current token
     * @param updated collection of the ranges after token is changed
     * @return pair of ranges to stream/fetch for given current and updated range collections
     */
    std::pair<std::unordered_set<dht::token_range>, std::unordered_set<dht::token_range>>
    calculate_stream_and_fetch_ranges(const dht::token_range_vector& current, const dht::token_range_vector& updated);
#if 0
    public void bulkLoad(String directory)
    {
        try
        {
            bulkLoadInternal(directory).get();
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public String bulkLoadAsync(String directory)
    {
        return bulkLoadInternal(directory).planId.toString();
    }

    private StreamResultFuture bulkLoadInternal(String directory)
    {
        File dir = new File(directory);

        if (!dir.exists() || !dir.isDirectory())
            throw new IllegalArgumentException("Invalid directory " + directory);

        SSTableLoader.Client client = new SSTableLoader.Client()
        {
            public void init(String keyspace)
            {
                try
                {
                    setPartitioner(DatabaseDescriptor.getPartitioner());
                    for (Map.Entry<Range<Token>, List<InetAddress>> entry : StorageService.instance.getRangeToAddressMap(keyspace).entrySet())
                    {
                        Range<Token> range = entry.getKey();
                        for (InetAddress endpoint : entry.getValue())
                            addRangeForEndpoint(range, endpoint);
                    }
                }
                catch (Exception e)
                {
                    throw new RuntimeException(e);
                }
            }

            public CFMetaData getCFMetaData(String keyspace, String cfName)
            {
                return Schema.instance.getCFMetaData(keyspace, cfName);
            }
        };

        SSTableLoader loader = new SSTableLoader(dir, client, new OutputHandler.LogOutput());
        return loader.stream();
    }
#endif
public:
    int32_t get_exception_count();
#if 0
    public void rescheduleFailedDeletions()
    {
        SSTableDeletingTask.rescheduleFailedTasks();
    }
#endif
    /**
     * Load new SSTables not currently tracked by the system
     *
     * This can be called, for instance, after copying a batch of SSTables to a CF directory.
     *
     * This should not be called in parallel for the same keyspace / column family, and doing
     * so will throw an std::runtime_exception.
     *
     * @param ks_name the keyspace in which to search for new SSTables.
     * @param cf_name the column family in which to search for new SSTables.
     * @return a future<> when the operation finishes.
     */
    future<> load_new_sstables(sstring ks_name, sstring cf_name);
#if 0
    /**
     * #{@inheritDoc}
     */
    public List<String> sampleKeyRange() // do not rename to getter - see CASSANDRA-4452 for details
    {
        List<DecoratedKey> keys = new ArrayList<>();
        for (Keyspace keyspace : Keyspace.nonSystem())
        {
            for (Range<Token> range : getPrimaryRangesForEndpoint(keyspace.getName(), FBUtilities.getBroadcastAddress()))
                keys.addAll(keySamples(keyspace.getColumnFamilyStores(), range));
        }

        List<String> sampledKeys = new ArrayList<>(keys.size());
        for (DecoratedKey key : keys)
            sampledKeys.add(key.getToken().toString());
        return sampledKeys;
    }

    public void rebuildSecondaryIndex(String ksName, String cfName, String... idxNames)
    {
        ColumnFamilyStore.rebuildSecondaryIndex(ksName, cfName, idxNames);
    }

    public void resetLocalSchema() throws IOException
    {
        MigrationManager.resetLocalSchema();
    }

    public void setTraceProbability(double probability)
    {
        this.traceProbability = probability;
    }

    public double getTraceProbability()
    {
        return traceProbability;
    }

    public void disableAutoCompaction(String ks, String... columnFamilies) throws IOException
    {
        for (ColumnFamilyStore cfs : getValidColumnFamilies(true, true, ks, columnFamilies))
        {
            cfs.disableAutoCompaction();
        }
    }

    public void enableAutoCompaction(String ks, String... columnFamilies) throws IOException
    {
        for (ColumnFamilyStore cfs : getValidColumnFamilies(true, true, ks, columnFamilies))
        {
            cfs.enableAutoCompaction();
        }
    }

    /** Returns the name of the cluster */
    public String getClusterName()
    {
        return DatabaseDescriptor.getClusterName();
    }

    /** Returns the cluster partitioner */
    public String getPartitionerName()
    {
        return DatabaseDescriptor.getPartitionerName();
    }

    public int getTombstoneWarnThreshold()
    {
        return DatabaseDescriptor.getTombstoneWarnThreshold();
    }

    public void setTombstoneWarnThreshold(int threshold)
    {
        DatabaseDescriptor.setTombstoneWarnThreshold(threshold);
    }

    public int getTombstoneFailureThreshold()
    {
        return DatabaseDescriptor.getTombstoneFailureThreshold();
    }

    public void setTombstoneFailureThreshold(int threshold)
    {
        DatabaseDescriptor.setTombstoneFailureThreshold(threshold);
    }

    public int getBatchSizeFailureThreshold()
    {
        return DatabaseDescriptor.getBatchSizeFailThresholdInKB();
    }

    public void setBatchSizeFailureThreshold(int threshold)
    {
        DatabaseDescriptor.setBatchSizeFailThresholdInKB(threshold);
    }

    public void setHintedHandoffThrottleInKB(int throttleInKB)
    {
        DatabaseDescriptor.setHintedHandoffThrottleInKB(throttleInKB);
        logger.info(String.format("Updated hinted_handoff_throttle_in_kb to %d", throttleInKB));
    }
#endif

    template <typename Func>
    auto run_with_api_lock(sstring operation, Func&& func) {
        return get_storage_service().invoke_on(0, [operation = std::move(operation),
                func = std::forward<Func>(func)] (storage_service& ss) mutable {
            if (!ss._operation_in_progress.empty()) {
                throw std::runtime_error(sprint("Operation %s is in progress, try again", ss._operation_in_progress));
            }
            ss._operation_in_progress = std::move(operation);
            return func(ss).finally([&ss] {
                ss._operation_in_progress = sstring();
            });
        });
    }

    template <typename Func>
    auto run_with_no_api_lock(Func&& func) {
        return get_storage_service().invoke_on(0, [func = std::forward<Func>(func)] (storage_service& ss) mutable {
            return func(ss);
        });
    }
private:
    void do_isolate_on_error(disk_error type);
    utils::UUID _local_host_id;
public:
    utils::UUID get_local_id() { return _local_host_id; }

    static sstring get_config_supported_features();

    bool cluster_supports_range_tombstones() {
        return bool(_range_tombstones_feature);
    }

    bool cluster_supports_large_partitions() const {
        return bool(_large_partitions_feature);
    }

    bool cluster_supports_materialized_views() const {
        return bool(_materialized_views_feature);
    }

    bool cluster_supports_counters() const {
        return bool(_counters_feature);
    }

    bool cluster_supports_indexes() const {
        return bool(_indexes_feature);
    }

    bool cluster_supports_digest_multipartition_reads() const {
        return bool(_digest_multipartition_read_feature);
    }

    bool cluster_supports_correct_counter_order() const {
        return bool(_correct_counter_order_feature);
    }

    const gms::feature& cluster_supports_schema_tables_v3() const {
        return _schema_tables_v3;
    }
};

inline future<> init_storage_service(distributed<database>& db, sharded<auth::service>& auth_service) {
    return service::get_storage_service().start(std::ref(db), std::ref(auth_service));
}

inline future<> deinit_storage_service() {
    return service::get_storage_service().stop();
}

}
