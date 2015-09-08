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
 * Modified by Cloudius Systems.
 * Copyright 2015 Cloudius Systems.
 *
 */

#pragma once

#include "gms/i_endpoint_state_change_subscriber.hh"
#include "service/endpoint_lifecycle_subscriber.hh"
#include "locator/token_metadata.hh"
#include "gms/gossiper.hh"
#include "utils/UUID_gen.hh"
#include "core/distributed.hh"
#include "dht/i_partitioner.hh"
#include "dht/boot_strapper.hh"
#include "core/sleep.hh"
#include "gms/application_state.hh"
#include "db/system_keyspace.hh"
#include "core/semaphore.hh"
#include "utils/fb_utilities.hh"
#include "database.hh"
#include <seastar/core/distributed.hh>

namespace service {

/**
 * This abstraction contains the token/identifier of this node
 * on the identifier space. This token gets gossiped around.
 * This class will also maintain histograms of the load information
 * of other nodes in the cluster.
 */
class storage_service : public gms::i_endpoint_state_change_subscriber, public seastar::async_sharded_service<storage_service> {
    using token = dht::token;
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
public:
    storage_service(distributed<database>& db)
        : _db(db) {
    }
    static int RING_DELAY; // delay after which we assume ring has stablized

    // Needed by distributed<>
    future<> stop();

    const locator::token_metadata& get_token_metadata() const {
        return _token_metadata;
    }

    locator::token_metadata& get_token_metadata() {
        return _token_metadata;
    }

    void gossip_snitch_info();

private:
    bool is_auto_bootstrap();
    inet_address get_broadcast_address() {
        return utils::fb_utilities::get_broadcast_address();
    }
    static int get_ring_delay() {
#if 0
        String newdelay = System.getProperty("cassandra.ring_delay_ms");
        if (newdelay != null)
        {
            logger.info("Overriding RING_DELAY to {}ms", newdelay);
            return Integer.parseInt(newdelay);
        }
        else
#endif
            return 5 * 1000;
    }
    /* This abstraction maintains the token/endpoint metadata information */
    token_metadata _token_metadata;
public:
    gms::versioned_value::versioned_value_factory value_factory;
#if 0
    public volatile VersionedValue.VersionedValueFactory valueFactory = new VersionedValue.VersionedValueFactory(getPartitioner());

    private Thread drainOnShutdown = null;

    public static final StorageService instance = new StorageService();

    public static IPartitioner getPartitioner()
    {
        return DatabaseDescriptor.getPartitioner();
    }

    public Collection<Range<Token>> getLocalRanges(String keyspaceName)
    {
        return getRangesForEndpoint(keyspaceName, FBUtilities.getBroadcastAddress());
    }

    public Collection<Range<Token>> getPrimaryRanges(String keyspace)
    {
        return getPrimaryRangesForEndpoint(keyspace, FBUtilities.getBroadcastAddress());
    }

    public Collection<Range<Token>> getPrimaryRangesWithinDC(String keyspace)
    {
        return getPrimaryRangeForEndpointWithinDC(keyspace, FBUtilities.getBroadcastAddress());
    }

    private final Set<InetAddress> replicatingNodes = Collections.synchronizedSet(new HashSet<InetAddress>());
    private CassandraDaemon daemon;

    private InetAddress removingNode;

#endif

private:
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

    /* Used for tracking drain progress */
    private volatile int totalCFs, remainingCFs;

    private static final AtomicInteger nextRepairCommand = new AtomicInteger();
#endif

    std::vector<endpoint_lifecycle_subscriber*> _lifecycle_subscribers;

#if 0
    private static final BackgroundActivityMonitor bgMonitor = new BackgroundActivityMonitor();

    private final ObjectName jmxObjectName;

#endif
private:
    std::unordered_set<token> _bootstrap_tokens;

public:
    void finish_bootstrapping() {
        _is_bootstrap_mode = false;
    }

    /** This method updates the local token on disk  */
    void set_tokens(std::unordered_set<token> tokens);
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

    bool is_rpc_server_running();

    future<> start_native_transport();

    future<> stop_native_transport();

    bool is_native_transport_running();

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

    private void shutdownClientServers()
    {
        stopRPCServer();
        stopNativeTransport();
    }

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
    bool is_initialized() {
        return _initialized;
    }
#if 0

    public void stopDaemon()
    {
        if (daemon == null)
            throw new IllegalStateException("No configured daemon");
        daemon.deactivate();
    }
#endif
public:
    std::unordered_set<token> prepare_replacement_info();

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
        return init_server(RING_DELAY);
    }

    future<> init_server(int delay);
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
    future<> prepare_to_join();
    void join_token_ring(int delay);
public:
    future<> join_ring();
    bool is_joined() {
        return _joined;
    }

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

    /**
     * Return the rpc address associated with an endpoint as a string.
     * @param endpoint The endpoint to get rpc address for
     * @return the rpc address
     */
    public String getRpcaddress(InetAddress endpoint)
    {
        if (endpoint.equals(FBUtilities.getBroadcastAddress()))
            return DatabaseDescriptor.getBroadcastRpcAddress().getHostAddress();
        else if (Gossiper.instance.getEndpointStateForEndpoint(endpoint).getApplicationState(ApplicationState.RPC_ADDRESS) == null)
            return endpoint.getHostAddress();
        else
            return Gossiper.instance.getEndpointStateForEndpoint(endpoint).getApplicationState(ApplicationState.RPC_ADDRESS).value;
    }

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

    public Map<Range<Token>, List<InetAddress>> getRangeToAddressMap(String keyspace)
    {
        return getRangeToAddressMap(keyspace, _token_metadata.sortedTokens());
    }

    public Map<Range<Token>, List<InetAddress>> getRangeToAddressMapInLocalDC(String keyspace)
    {
        Predicate<InetAddress> isLocalDC = new Predicate<InetAddress>()
        {
            public boolean apply(InetAddress address)
            {
                return isLocalDC(address);
            }
        };

        Map<Range<Token>, List<InetAddress>> origMap = getRangeToAddressMap(keyspace, getTokensInLocalDC());
        Map<Range<Token>, List<InetAddress>> filteredMap = Maps.newHashMap();
        for (Map.Entry<Range<Token>, List<InetAddress>> entry : origMap.entrySet())
        {
            List<InetAddress> endpointsInLocalDC = Lists.newArrayList(Collections2.filter(entry.getValue(), isLocalDC));
            filteredMap.put(entry.getKey(), endpointsInLocalDC);
        }

        return filteredMap;
    }

    private List<Token> getTokensInLocalDC()
    {
        List<Token> filteredTokens = Lists.newArrayList();
        for (Token token : _token_metadata.sortedTokens())
        {
            InetAddress endpoint = _token_metadata.getEndpoint(token);
            if (isLocalDC(endpoint))
                filteredTokens.add(token);
        }
        return filteredTokens;
    }

    private boolean isLocalDC(InetAddress targetHost)
    {
        String remoteDC = DatabaseDescriptor.getEndpointSnitch().getDatacenter(targetHost);
        String localDC = DatabaseDescriptor.getEndpointSnitch().getDatacenter(FBUtilities.getBroadcastAddress());
        return remoteDC.equals(localDC);
    }

    private Map<Range<Token>, List<InetAddress>> getRangeToAddressMap(String keyspace, List<Token> sortedTokens)
    {
        // some people just want to get a visual representation of things. Allow null and set it to the first
        // non-system keyspace.
        if (keyspace == null)
            keyspace = Schema.instance.getNonSystemKeyspaces().get(0);

        List<Range<Token>> ranges = getAllRanges(sortedTokens);
        return constructRangeToEndpointMap(keyspace, ranges);
    }


    /**
     * The same as {@code describeRing(String)} but converts TokenRange to the String for JMX compatibility
     *
     * @param keyspace The keyspace to fetch information about
     *
     * @return a List of TokenRange(s) converted to String for the given keyspace
     */
    public List<String> describeRingJMX(String keyspace) throws IOException
    {
        List<TokenRange> tokenRanges;
        try
        {
            tokenRanges = describeRing(keyspace);
        }
        catch (InvalidRequestException e)
        {
            throw new IOException(e.getMessage());
        }
        List<String> result = new ArrayList<>(tokenRanges.size());

        for (TokenRange tokenRange : tokenRanges)
            result.add(tokenRange.toString());

        return result;
    }

    /**
     * The TokenRange for a given keyspace.
     *
     * @param keyspace The keyspace to fetch information about
     *
     * @return a List of TokenRange(s) for the given keyspace
     *
     * @throws InvalidRequestException if there is no ring information available about keyspace
     */
    public List<TokenRange> describeRing(String keyspace) throws InvalidRequestException
    {
        return describeRing(keyspace, false);
    }

    /**
     * The same as {@code describeRing(String)} but considers only the part of the ring formed by nodes in the local DC.
     */
    public List<TokenRange> describeLocalRing(String keyspace) throws InvalidRequestException
    {
        return describeRing(keyspace, true);
    }

    private List<TokenRange> describeRing(String keyspace, boolean includeOnlyLocalDC) throws InvalidRequestException
    {
        if (!Schema.instance.getKeyspaces().contains(keyspace))
            throw new InvalidRequestException("No such keyspace: " + keyspace);

        if (keyspace == null || Keyspace.open(keyspace).getReplicationStrategy() instanceof LocalStrategy)
            throw new InvalidRequestException("There is no ring for the keyspace: " + keyspace);

        List<TokenRange> ranges = new ArrayList<>();
        Token.TokenFactory tf = getPartitioner().getTokenFactory();

        Map<Range<Token>, List<InetAddress>> rangeToAddressMap =
                includeOnlyLocalDC
                        ? getRangeToAddressMapInLocalDC(keyspace)
                        : getRangeToAddressMap(keyspace);

        for (Map.Entry<Range<Token>, List<InetAddress>> entry : rangeToAddressMap.entrySet())
        {
            Range range = entry.getKey();
            List<InetAddress> addresses = entry.getValue();
            List<String> endpoints = new ArrayList<>(addresses.size());
            List<String> rpc_endpoints = new ArrayList<>(addresses.size());
            List<EndpointDetails> epDetails = new ArrayList<>(addresses.size());

            for (InetAddress endpoint : addresses)
            {
                EndpointDetails details = new EndpointDetails();
                details.host = endpoint.getHostAddress();
                details.datacenter = DatabaseDescriptor.getEndpointSnitch().getDatacenter(endpoint);
                details.rack = DatabaseDescriptor.getEndpointSnitch().getRack(endpoint);

                endpoints.add(details.host);
                rpc_endpoints.add(getRpcaddress(endpoint));

                epDetails.add(details);
            }

            TokenRange tr = new TokenRange(tf.toString(range.left.getToken()), tf.toString(range.right.getToken()), endpoints)
                                    .setEndpoint_details(epDetails)
                                    .setRpc_endpoints(rpc_endpoints);

            ranges.add(tr);
        }

        return ranges;
    }

    public Map<String, String> getTokenToEndpointMap()
    {
        Map<Token, InetAddress> mapInetAddress = _token_metadata.getNormalAndBootstrappingTokenToEndpointMap();
        // in order to preserve tokens in ascending order, we use LinkedHashMap here
        Map<String, String> mapString = new LinkedHashMap<>(mapInetAddress.size());
        List<Token> tokens = new ArrayList<>(mapInetAddress.keySet());
        Collections.sort(tokens);
        for (Token token : tokens)
        {
            mapString.put(token.toString(), mapInetAddress.get(token).getHostAddress());
        }
        return mapString;
    }

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

    /**
     * Construct the range to endpoint mapping based on the true view
     * of the world.
     * @param ranges
     * @return mapping of ranges to the replicas responsible for them.
    */
    private Map<Range<Token>, List<InetAddress>> constructRangeToEndpointMap(String keyspace, List<Range<Token>> ranges)
    {
        Map<Range<Token>, List<InetAddress>> rangeToEndpointMap = new HashMap<>(ranges.size());
        for (Range<Token> range : ranges)
        {
            rangeToEndpointMap.put(range, Keyspace.open(keyspace).getReplicationStrategy().getNaturalEndpoints(range.right));
        }
        return rangeToEndpointMap;
    }
#endif
public:
    virtual void on_join(gms::inet_address endpoint, gms::endpoint_state ep_state) override;
    virtual void before_change(gms::inet_address endpoint, gms::endpoint_state current_state, gms::application_state new_state_key, gms::versioned_value new_value) override;
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
    virtual void on_change(inet_address endpoint, application_state state, versioned_value value) override;
    virtual void on_alive(gms::inet_address endpoint, gms::endpoint_state state) override;
    virtual void on_dead(gms::inet_address endpoint, gms::endpoint_state state) override;
    virtual void on_remove(gms::inet_address endpoint) override;
    virtual void on_restart(gms::inet_address endpoint, gms::endpoint_state state) override;
private:
    void update_peer_info(inet_address endpoint);
    void do_update_system_peers_table(gms::inet_address endpoint, const application_state& state, const versioned_value& value);
    sstring get_application_state_value(inet_address endpoint, application_state appstate);
    std::unordered_set<token> get_tokens_for(inet_address endpoint);
    future<> replicate_to_all_cores();
    semaphore _replicate_task{1};
private:
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

#if 0
    private void excise(Collection<Token> tokens, InetAddress endpoint)
    {
        logger.info("Removing tokens {} for {}", tokens, endpoint);
        HintedHandOffManager.instance.deleteHintsForEndpoint(endpoint);
        removeEndpoint(endpoint);
        _token_metadata.removeEndpoint(endpoint);
        _token_metadata.removeBootstrapTokens(tokens);

        for (IEndpointLifecycleSubscriber subscriber : lifecycleSubscribers)
            subscriber.onLeaveCluster(endpoint);
        PendingRangeCalculatorService.instance.update();
    }

    private void excise(Collection<Token> tokens, InetAddress endpoint, long expireTime)
    {
        addExpireTimeIfFound(endpoint, expireTime);
        excise(tokens, endpoint);
    }
#endif

private:
    /** unlike excise we just need this endpoint gone without going through any notifications **/
    void remove_endpoint(inet_address endpoint);
#if 0
    protected void addExpireTimeIfFound(InetAddress endpoint, long expireTime)
    {
        if (expireTime != 0L)
        {
            Gossiper.instance.addExpireTimeForEndpoint(endpoint, expireTime);
        }
    }

    protected long extractExpireTime(String[] pieces)
    {
        return Long.parseLong(pieces[2]);
    }

    /**
     * Finds living endpoints responsible for the given ranges
     *
     * @param keyspaceName the keyspace ranges belong to
     * @param ranges the ranges to find sources for
     * @return multimap of addresses to ranges the address is responsible for
     */
    private Multimap<InetAddress, Range<Token>> getNewSourceRanges(String keyspaceName, Set<Range<Token>> ranges)
    {
        InetAddress myAddress = FBUtilities.getBroadcastAddress();
        Multimap<Range<Token>, InetAddress> rangeAddresses = Keyspace.open(keyspaceName).getReplicationStrategy().getRangeAddresses(_token_metadata.cloneOnlyTokenMap());
        Multimap<InetAddress, Range<Token>> sourceRanges = HashMultimap.create();
        IFailureDetector failureDetector = FailureDetector.instance;

        // find alive sources for our new ranges
        for (Range<Token> range : ranges)
        {
            Collection<InetAddress> possibleRanges = rangeAddresses.get(range);
            IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
            List<InetAddress> sources = snitch.getSortedListByProximity(myAddress, possibleRanges);

            assert (!sources.contains(myAddress));

            for (InetAddress source : sources)
            {
                if (failureDetector.isAlive(source))
                {
                    sourceRanges.put(source, range);
                    break;
                }
            }
        }
        return sourceRanges;
    }

    /**
     * Sends a notification to a node indicating we have finished replicating data.
     *
     * @param remote node to send notification to
     */
    private void sendReplicationNotification(InetAddress remote)
    {
        // notify the remote token
        MessageOut msg = new MessageOut(MessagingService.Verb.REPLICATION_FINISHED);
        IFailureDetector failureDetector = FailureDetector.instance;
        if (logger.isDebugEnabled())
            logger.debug("Notifying {} of replication completion\n", remote);
        while (failureDetector.isAlive(remote))
        {
            AsyncOneResponse iar = MessagingService.instance().sendRR(msg, remote);
            try
            {
                iar.get(DatabaseDescriptor.getRpcTimeout(), TimeUnit.MILLISECONDS);
                return; // done
            }
            catch(TimeoutException e)
            {
                // try again
            }
        }
    }

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
    private void restoreReplicaCount(InetAddress endpoint, final InetAddress notifyEndpoint)
    {
        Multimap<String, Map.Entry<InetAddress, Collection<Range<Token>>>> rangesToFetch = HashMultimap.create();

        InetAddress myAddress = FBUtilities.getBroadcastAddress();

        for (String keyspaceName : Schema.instance.getNonSystemKeyspaces())
        {
            Multimap<Range<Token>, InetAddress> changedRanges = getChangedRangesForLeaving(keyspaceName, endpoint);
            Set<Range<Token>> myNewRanges = new HashSet<>();
            for (Map.Entry<Range<Token>, InetAddress> entry : changedRanges.entries())
            {
                if (entry.getValue().equals(myAddress))
                    myNewRanges.add(entry.getKey());
            }
            Multimap<InetAddress, Range<Token>> sourceRanges = getNewSourceRanges(keyspaceName, myNewRanges);
            for (Map.Entry<InetAddress, Collection<Range<Token>>> entry : sourceRanges.asMap().entrySet())
            {
                rangesToFetch.put(keyspaceName, entry);
            }
        }

        StreamPlan stream = new StreamPlan("Restore replica count");
        for (String keyspaceName : rangesToFetch.keySet())
        {
            for (Map.Entry<InetAddress, Collection<Range<Token>>> entry : rangesToFetch.get(keyspaceName))
            {
                InetAddress source = entry.getKey();
                InetAddress preferred = SystemKeyspace.getPreferredIP(source);
                Collection<Range<Token>> ranges = entry.getValue();
                if (logger.isDebugEnabled())
                    logger.debug("Requesting from {} ranges {}", source, StringUtils.join(ranges, ", "));
                stream.requestRanges(source, preferred, keyspaceName, ranges);
            }
        }
        StreamResultFuture future = stream.execute();
        Futures.addCallback(future, new FutureCallback<StreamState>()
        {
            public void onSuccess(StreamState finalState)
            {
                sendReplicationNotification(notifyEndpoint);
            }

            public void onFailure(Throwable t)
            {
                logger.warn("Streaming to restore replica count failed", t);
                // We still want to send the notification
                sendReplicationNotification(notifyEndpoint);
            }
        });
    }

    // needs to be modified to accept either a keyspace or ARS.
    private Multimap<Range<Token>, InetAddress> getChangedRangesForLeaving(String keyspaceName, InetAddress endpoint)
    {
        // First get all ranges the leaving endpoint is responsible for
        Collection<Range<Token>> ranges = getRangesForEndpoint(keyspaceName, endpoint);

        if (logger.isDebugEnabled())
            logger.debug("Node {} ranges [{}]", endpoint, StringUtils.join(ranges, ", "));

        Map<Range<Token>, List<InetAddress>> currentReplicaEndpoints = new HashMap<>(ranges.size());

        // Find (for each range) all nodes that store replicas for these ranges as well
        TokenMetadata metadata = _token_metadata.cloneOnlyTokenMap(); // don't do this in the loop! #7758
        for (Range<Token> range : ranges)
            currentReplicaEndpoints.put(range, Keyspace.open(keyspaceName).getReplicationStrategy().calculateNaturalEndpoints(range.right, metadata));

        TokenMetadata temp = _token_metadata.cloneAfterAllLeft();

        // endpoint might or might not be 'leaving'. If it was not leaving (that is, removenode
        // command was used), it is still present in temp and must be removed.
        if (temp.isMember(endpoint))
            temp.removeEndpoint(endpoint);

        Multimap<Range<Token>, InetAddress> changedRanges = HashMultimap.create();

        // Go through the ranges and for each range check who will be
        // storing replicas for these ranges when the leaving endpoint
        // is gone. Whoever is present in newReplicaEndpoints list, but
        // not in the currentReplicaEndpoints list, will be needing the
        // range.
        for (Range<Token> range : ranges)
        {
            Collection<InetAddress> newReplicaEndpoints = Keyspace.open(keyspaceName).getReplicationStrategy().calculateNaturalEndpoints(range.right, temp);
            newReplicaEndpoints.removeAll(currentReplicaEndpoints.get(range));
            if (logger.isDebugEnabled())
                if (newReplicaEndpoints.isEmpty())
                    logger.debug("Range {} already in all replicas", range);
                else
                    logger.debug("Range {} will be responsibility of {}", range, StringUtils.join(newReplicaEndpoints, ", "));
            changedRanges.putAll(range, newReplicaEndpoints);
        }

        return changedRanges;
    }
#endif
public:
    /** raw load value */
    double get_load();

    sstring get_load_string();

    std::map<sstring, sstring> get_load_map();

#if 0
    public final void deliverHints(String host) throws UnknownHostException
    {
        HintedHandOffManager.instance.scheduleHintDelivery(host);
    }
#endif
public:
    std::unordered_set<dht::token> get_local_tokens();

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

    /**
     * Takes the snapshot for the given keyspaces. A snapshot name must be specified.
     *
     * @param tag the tag given to the snapshot; may not be null or empty
     * @param keyspaceNames the names of the keyspaces to snapshot; empty means "all."
     */
    public void takeSnapshot(String tag, String... keyspaceNames) throws IOException
    {
        if (operationMode == Mode.JOINING)
            throw new IOException("Cannot snapshot until bootstrap completes");
        if (tag == null || tag.equals(""))
            throw new IOException("You must supply a snapshot name.");

        Iterable<Keyspace> keyspaces;
        if (keyspaceNames.length == 0)
        {
            keyspaces = Keyspace.all();
        }
        else
        {
            ArrayList<Keyspace> t = new ArrayList<>(keyspaceNames.length);
            for (String keyspaceName : keyspaceNames)
                t.add(getValidKeyspace(keyspaceName));
            keyspaces = t;
        }

        // Do a check to see if this snapshot exists before we actually snapshot
        for (Keyspace keyspace : keyspaces)
            if (keyspace.snapshotExists(tag))
                throw new IOException("Snapshot " + tag + " already exists.");


        for (Keyspace keyspace : keyspaces)
            keyspace.snapshot(tag, null);
    }

    /**
     * Takes the snapshot of a specific column family. A snapshot name must be specified.
     *
     * @param keyspaceName the keyspace which holds the specified column family
     * @param columnFamilyName the column family to snapshot
     * @param tag the tag given to the snapshot; may not be null or empty
     */
    public void takeColumnFamilySnapshot(String keyspaceName, String columnFamilyName, String tag) throws IOException
    {
        if (keyspaceName == null)
            throw new IOException("You must supply a keyspace name");
        if (operationMode == Mode.JOINING)
            throw new IOException("Cannot snapshot until bootstrap completes");

        if (columnFamilyName == null)
            throw new IOException("You must supply a table name");
        if (columnFamilyName.contains("."))
            throw new IllegalArgumentException("Cannot take a snapshot of a secondary index by itself. Run snapshot on the table that owns the index.");

        if (tag == null || tag.equals(""))
            throw new IOException("You must supply a snapshot name.");

        Keyspace keyspace = getValidKeyspace(keyspaceName);
        if (keyspace.snapshotExists(tag))
            throw new IOException("Snapshot " + tag + " already exists.");

        keyspace.snapshot(tag, columnFamilyName);
    }

    private Keyspace getValidKeyspace(String keyspaceName) throws IOException
    {
        if (!Schema.instance.getKeyspaces().contains(keyspaceName))
        {
            throw new IOException("Keyspace " + keyspaceName + " does not exist");
        }
        return Keyspace.open(keyspaceName);
    }

    /**
     * Remove the snapshot with the given name from the given keyspaces.
     * If no tag is specified we will remove all snapshots.
     */
    public void clearSnapshot(String tag, String... keyspaceNames) throws IOException
    {
        if(tag == null)
            tag = "";

        Set<String> keyspaces = new HashSet<>();
        for (String dataDir : DatabaseDescriptor.getAllDataFileLocations())
        {
            for(String keyspaceDir : new File(dataDir).list())
            {
                // Only add a ks if it has been specified as a param, assuming params were actually provided.
                if (keyspaceNames.length > 0 && !Arrays.asList(keyspaceNames).contains(keyspaceDir))
                    continue;
                keyspaces.add(keyspaceDir);
            }
        }

        for (String keyspace : keyspaces)
            Keyspace.clearSnapshot(tag, keyspace);

        if (logger.isDebugEnabled())
            logger.debug("Cleared out snapshot directories");
    }

    public Map<String, TabularData> getSnapshotDetails()
    {
        Map<String, TabularData> snapshotMap = new HashMap<>();
        for (Keyspace keyspace : Keyspace.all())
        {
            if (SystemKeyspace.NAME.equals(keyspace.getName()))
                continue;

            for (ColumnFamilyStore cfStore : keyspace.getColumnFamilyStores())
            {
                for (Map.Entry<String, Pair<Long,Long>> snapshotDetail : cfStore.getSnapshotDetails().entrySet())
                {
                    TabularDataSupport data = (TabularDataSupport)snapshotMap.get(snapshotDetail.getKey());
                    if (data == null)
                    {
                        data = new TabularDataSupport(SnapshotDetailsTabularData.TABULAR_TYPE);
                        snapshotMap.put(snapshotDetail.getKey(), data);
                    }

                    SnapshotDetailsTabularData.from(snapshotDetail.getKey(), keyspace.getName(), cfStore.getColumnFamilyName(), snapshotDetail, data);
                }
            }
        }
        return snapshotMap;
    }

    public long trueSnapshotsSize()
    {
        long total = 0;
        for (Keyspace keyspace : Keyspace.all())
        {
            if (SystemKeyspace.NAME.equals(keyspace.getName()))
                continue;

            for (ColumnFamilyStore cfStore : keyspace.getColumnFamilyStores())
            {
                total += cfStore.trueSnapshotsSize();
            }
        }

        return total;
    }

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
     * @param ep endpoint we are interested in.
     * @return ranges for the specified endpoint.
     */
    std::vector<range<token>> get_ranges_for_endpoint(const sstring& name, const gms::inet_address& ep) const {
        return _db.local().find_keyspace(name).get_replication_strategy().get_ranges(ep);
    }

#if 0
    /**
     * Get all ranges that span the ring given a set
     * of tokens. All ranges are in sorted order of
     * ranges.
     * @return ranges in sorted order
    */
    public List<Range<Token>> getAllRanges(List<Token> sortedTokens)
    {
        if (logger.isDebugEnabled())
            logger.debug("computing ranges for {}", StringUtils.join(sortedTokens, ", "));

        if (sortedTokens.isEmpty())
            return Collections.emptyList();
        int size = sortedTokens.size();
        List<Range<Token>> ranges = new ArrayList<>(size + 1);
        for (int i = 1; i < size; ++i)
        {
            Range<Token> range = new Range<>(sortedTokens.get(i - 1), sortedTokens.get(i));
            ranges.add(range);
        }
        Range<Token> range = new Range<>(sortedTokens.get(size - 1), sortedTokens.get(0));
        ranges.add(range);

        return ranges;
    }

    /**
     * This method returns the N endpoints that are responsible for storing the
     * specified key i.e for replication.
     *
     * @param keyspaceName keyspace name also known as keyspace
     * @param cf Column family name
     * @param key key for which we need to find the endpoint
     * @return the endpoint responsible for this key
     */
    public List<InetAddress> getNaturalEndpoints(String keyspaceName, String cf, String key)
    {
        CFMetaData cfMetaData = Schema.instance.getKSMetaData(keyspaceName).cfMetaData().get(cf);
        return getNaturalEndpoints(keyspaceName, getPartitioner().getToken(cfMetaData.getKeyValidator().fromString(key)));
    }

    public List<InetAddress> getNaturalEndpoints(String keyspaceName, ByteBuffer key)
    {
        return getNaturalEndpoints(keyspaceName, getPartitioner().getToken(key));
    }

    /**
     * This method returns the N endpoints that are responsible for storing the
     * specified key i.e for replication.
     *
     * @param keyspaceName keyspace name also known as keyspace
     * @param pos position for which we need to find the endpoint
     * @return the endpoint responsible for this token
     */
    public List<InetAddress> getNaturalEndpoints(String keyspaceName, RingPosition pos)
    {
        return Keyspace.open(keyspaceName).getReplicationStrategy().getNaturalEndpoints(pos);
    }

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

    /**
     * @return list of Token ranges (_not_ keys!) together with estimated key count,
     *      breaking up the data this node is responsible for into pieces of roughly keysPerSplit
     */
    public List<Pair<Range<Token>, Long>> getSplits(String keyspaceName, String cfName, Range<Token> range, int keysPerSplit)
    {
        Keyspace t = Keyspace.open(keyspaceName);
        ColumnFamilyStore cfs = t.getColumnFamilyStore(cfName);
        List<DecoratedKey> keys = keySamples(Collections.singleton(cfs), range);

        long totalRowCountEstimate = cfs.estimatedKeysForRange(range);

        // splitCount should be much smaller than number of key samples, to avoid huge sampling error
        int minSamplesPerSplit = 4;
        int maxSplitCount = keys.size() / minSamplesPerSplit + 1;
        int splitCount = Math.max(1, Math.min(maxSplitCount, (int)(totalRowCountEstimate / keysPerSplit)));

        List<Token> tokens = keysToTokens(range, keys);
        return getSplits(tokens, splitCount, cfs);
    }

    private List<Pair<Range<Token>, Long>> getSplits(List<Token> tokens, int splitCount, ColumnFamilyStore cfs)
    {
        double step = (double) (tokens.size() - 1) / splitCount;
        Token prevToken = tokens.get(0);
        List<Pair<Range<Token>, Long>> splits = Lists.newArrayListWithExpectedSize(splitCount);
        for (int i = 1; i <= splitCount; i++)
        {
            int index = (int) Math.round(i * step);
            Token token = tokens.get(index);
            Range<Token> range = new Range<>(prevToken, token);
            // always return an estimate > 0 (see CASSANDRA-7322)
            splits.add(Pair.create(range, Math.max(cfs.metadata.getMinIndexInterval(), cfs.estimatedKeysForRange(range))));
            prevToken = token;
        }
        return splits;
    }

    private List<Token> keysToTokens(Range<Token> range, List<DecoratedKey> keys)
    {
        List<Token> tokens = Lists.newArrayListWithExpectedSize(keys.size() + 2);
        tokens.add(range.left);
        for (DecoratedKey key : keys)
            tokens.add(key.getToken());
        tokens.add(range.right);
        return tokens;
    }

    private List<DecoratedKey> keySamples(Iterable<ColumnFamilyStore> cfses, Range<Token> range)
    {
        List<DecoratedKey> keys = new ArrayList<>();
        for (ColumnFamilyStore cfs : cfses)
            Iterables.addAll(keys, cfs.keySamples(range));
        FBUtilities.sortSampledKeys(keys, range);
        return keys;
    }

    /**
     * Broadcast leaving status and update local _token_metadata accordingly
     */
    private void startLeaving()
    {
        Gossiper.instance.addLocalApplicationState(ApplicationState.STATUS, valueFactory.leaving(getLocalTokens()));
        _token_metadata.addLeavingEndpoint(FBUtilities.getBroadcastAddress());
        PendingRangeCalculatorService.instance.update();
    }
#endif

    future<> decommission();

#if 0
    private void leaveRing()
    {
        SystemKeyspace.setBootstrapState(SystemKeyspace.BootstrapState.NEEDS_BOOTSTRAP);
        _token_metadata.removeEndpoint(FBUtilities.getBroadcastAddress());
        PendingRangeCalculatorService.instance.update();

        Gossiper.instance.addLocalApplicationState(ApplicationState.STATUS, valueFactory.left(getLocalTokens(),Gossiper.computeExpireTime()));
        int delay = Math.max(RING_DELAY, Gossiper.intervalInMillis * 2);
        logger.info("Announcing that I have left the ring for {}ms", delay);
        Uninterruptibles.sleepUninterruptibly(delay, TimeUnit.MILLISECONDS);
    }

    private void unbootstrap(Runnable onFinish)
    {
        Map<String, Multimap<Range<Token>, InetAddress>> rangesToStream = new HashMap<>();

        for (String keyspaceName : Schema.instance.getNonSystemKeyspaces())
        {
            Multimap<Range<Token>, InetAddress> rangesMM = getChangedRangesForLeaving(keyspaceName, FBUtilities.getBroadcastAddress());

            if (logger.isDebugEnabled())
                logger.debug("Ranges needing transfer are [{}]", StringUtils.join(rangesMM.keySet(), ","));

            rangesToStream.put(keyspaceName, rangesMM);
        }

        setMode(Mode.LEAVING, "replaying batch log and streaming data to other nodes", true);

        // Start with BatchLog replay, which may create hints but no writes since this is no longer a valid endpoint.
        Future<?> batchlogReplay = BatchlogManager.instance.startBatchlogReplay();
        Future<StreamState> streamSuccess = streamRanges(rangesToStream);

        // Wait for batch log to complete before streaming hints.
        logger.debug("waiting for batch log processing.");
        try
        {
            batchlogReplay.get();
        }
        catch (ExecutionException | InterruptedException e)
        {
            throw new RuntimeException(e);
        }

        setMode(Mode.LEAVING, "streaming hints to other nodes", true);

        Future<StreamState> hintsSuccess = streamHints();

        // wait for the transfer runnables to signal the latch.
        logger.debug("waiting for stream acks.");
        try
        {
            streamSuccess.get();
            hintsSuccess.get();
        }
        catch (ExecutionException | InterruptedException e)
        {
            throw new RuntimeException(e);
        }
        logger.debug("stream acks all received.");
        leaveRing();
        onFinish.run();
    }

    private Future<StreamState> streamHints()
    {
        // StreamPlan will not fail if there are zero files to transfer, so flush anyway (need to get any in-memory hints, as well)
        ColumnFamilyStore hintsCF = Keyspace.open(SystemKeyspace.NAME).getColumnFamilyStore(SystemKeyspace.HINTS);
        FBUtilities.waitOnFuture(hintsCF.forceFlush());

        // gather all live nodes in the cluster that aren't also leaving
        List<InetAddress> candidates = new ArrayList<>(StorageService.instance.getTokenMetadata().cloneAfterAllLeft().getAllEndpoints());
        candidates.remove(FBUtilities.getBroadcastAddress());
        for (Iterator<InetAddress> iter = candidates.iterator(); iter.hasNext(); )
        {
            InetAddress address = iter.next();
            if (!FailureDetector.instance.isAlive(address))
                iter.remove();
        }

        if (candidates.isEmpty())
        {
            logger.warn("Unable to stream hints since no live endpoints seen");
            return Futures.immediateFuture(null);
        }
        else
        {
            // stream to the closest peer as chosen by the snitch
            DatabaseDescriptor.getEndpointSnitch().sortByProximity(FBUtilities.getBroadcastAddress(), candidates);
            InetAddress hintsDestinationHost = candidates.get(0);
            InetAddress preferred = SystemKeyspace.getPreferredIP(hintsDestinationHost);

            // stream all hints -- range list will be a singleton of "the entire ring"
            Token token = StorageService.getPartitioner().getMinimumToken();
            List<Range<Token>> ranges = Collections.singletonList(new Range<>(token, token));

            return new StreamPlan("Hints").transferRanges(hintsDestinationHost,
                                                          preferred,
                                                          SystemKeyspace.NAME,
                                                          ranges,
                                                          SystemKeyspace.HINTS)
                                          .execute();
        }
    }

    public void move(String newToken) throws IOException
    {
        try
        {
            getPartitioner().getTokenFactory().validate(newToken);
        }
        catch (ConfigurationException e)
        {
            throw new IOException(e.getMessage());
        }
        move(getPartitioner().getTokenFactory().fromString(newToken));
    }

    /**
     * move the node to new token or find a new token to boot to according to load
     *
     * @param newToken new token to boot to, or if null, find balanced token to boot to
     *
     * @throws IOException on any I/O operation error
     */
    private void move(Token newToken) throws IOException
    {
        if (newToken == null)
            throw new IOException("Can't move to the undefined (null) token.");

        if (_token_metadata.sortedTokens().contains(newToken))
            throw new IOException("target token " + newToken + " is already owned by another node.");

        // address of the current node
        InetAddress localAddress = FBUtilities.getBroadcastAddress();

        // This doesn't make any sense in a vnodes environment.
        if (getTokenMetadata().getTokens(localAddress).size() > 1)
        {
            logger.error("Invalid request to move(Token); This node has more than one token and cannot be moved thusly.");
            throw new UnsupportedOperationException("This node has more than one token and cannot be moved thusly.");
        }

        List<String> keyspacesToProcess = Schema.instance.getNonSystemKeyspaces();

        PendingRangeCalculatorService.instance.blockUntilFinished();
        // checking if data is moving to this node
        for (String keyspaceName : keyspacesToProcess)
        {
            if (_token_metadata.getPendingRanges(keyspaceName, localAddress).size() > 0)
                throw new UnsupportedOperationException("data is currently moving to this node; unable to leave the ring");
        }

        Gossiper.instance.addLocalApplicationState(ApplicationState.STATUS, valueFactory.moving(newToken));
        setMode(Mode.MOVING, String.format("Moving %s from %s to %s.", localAddress, getLocalTokens().iterator().next(), newToken), true);

        setMode(Mode.MOVING, String.format("Sleeping %s ms before start streaming/fetching ranges", RING_DELAY), true);
        Uninterruptibles.sleepUninterruptibly(RING_DELAY, TimeUnit.MILLISECONDS);

        RangeRelocator relocator = new RangeRelocator(Collections.singleton(newToken), keyspacesToProcess);

        if (relocator.streamsNeeded())
        {
            setMode(Mode.MOVING, "fetching new ranges and streaming old ranges", true);
            try
            {
                relocator.stream().get();
            }
            catch (ExecutionException | InterruptedException e)
            {
                throw new RuntimeException("Interrupted while waiting for stream/fetch ranges to finish: " + e.getMessage());
            }
        }
        else
        {
            setMode(Mode.MOVING, "No ranges to fetch/stream", true);
        }

        set_tokens(Collections.singleton(newToken)); // setting new token as we have everything settled

        if (logger.isDebugEnabled())
            logger.debug("Successfully moved to new token {}", getLocalTokens().iterator().next());
    }

    private class RangeRelocator
    {
        private final StreamPlan streamPlan = new StreamPlan("Relocation");

        private RangeRelocator(Collection<Token> tokens, List<String> keyspaceNames)
        {
            calculateToFromStreams(tokens, keyspaceNames);
        }

        private void calculateToFromStreams(Collection<Token> newTokens, List<String> keyspaceNames)
        {
            InetAddress localAddress = FBUtilities.getBroadcastAddress();
            IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
            TokenMetadata tokenMetaCloneAllSettled = _token_metadata.cloneAfterAllSettled();
            // clone to avoid concurrent modification in calculateNaturalEndpoints
            TokenMetadata tokenMetaClone = _token_metadata.cloneOnlyTokenMap();

            for (String keyspace : keyspaceNames)
            {
                logger.debug("Calculating ranges to stream and request for keyspace {}", keyspace);
                for (Token newToken : newTokens)
                {
                    // replication strategy of the current keyspace (aka table)
                    AbstractReplicationStrategy strategy = Keyspace.open(keyspace).getReplicationStrategy();

                    // getting collection of the currently used ranges by this keyspace
                    Collection<Range<Token>> currentRanges = getRangesForEndpoint(keyspace, localAddress);
                    // collection of ranges which this node will serve after move to the new token
                    Collection<Range<Token>> updatedRanges = strategy.getPendingAddressRanges(tokenMetaClone, newToken, localAddress);

                    // ring ranges and endpoints associated with them
                    // this used to determine what nodes should we ping about range data
                    Multimap<Range<Token>, InetAddress> rangeAddresses = strategy.getRangeAddresses(tokenMetaClone);

                    // calculated parts of the ranges to request/stream from/to nodes in the ring
                    Pair<Set<Range<Token>>, Set<Range<Token>>> rangesPerKeyspace = calculateStreamAndFetchRanges(currentRanges, updatedRanges);

                    /**
                     * In this loop we are going through all ranges "to fetch" and determining
                     * nodes in the ring responsible for data we are interested in
                     */
                    Multimap<Range<Token>, InetAddress> rangesToFetchWithPreferredEndpoints = ArrayListMultimap.create();
                    for (Range<Token> toFetch : rangesPerKeyspace.right)
                    {
                        for (Range<Token> range : rangeAddresses.keySet())
                        {
                            if (range.contains(toFetch))
                            {
                                List<InetAddress> endpoints = null;

                                if (RangeStreamer.useStrictConsistency)
                                {
                                    Set<InetAddress> oldEndpoints = Sets.newHashSet(rangeAddresses.get(range));
                                    Set<InetAddress> newEndpoints = Sets.newHashSet(strategy.calculateNaturalEndpoints(toFetch.right, tokenMetaCloneAllSettled));

                                    //Due to CASSANDRA-5953 we can have a higher RF then we have endpoints.
                                    //So we need to be careful to only be strict when endpoints == RF
                                    if (oldEndpoints.size() == strategy.getReplicationFactor())
                                    {
                                        oldEndpoints.removeAll(newEndpoints);

                                        //No relocation required
                                        if (oldEndpoints.isEmpty())
                                            continue;

                                        assert oldEndpoints.size() == 1 : "Expected 1 endpoint but found " + oldEndpoints.size();
                                    }

                                    endpoints = Lists.newArrayList(oldEndpoints.iterator().next());
                                }
                                else
                                {
                                    endpoints = snitch.getSortedListByProximity(localAddress, rangeAddresses.get(range));
                                }

                                // storing range and preferred endpoint set
                                rangesToFetchWithPreferredEndpoints.putAll(toFetch, endpoints);
                            }
                        }

                        Collection<InetAddress> addressList = rangesToFetchWithPreferredEndpoints.get(toFetch);
                        if (addressList == null || addressList.isEmpty())
                            continue;

                        if (RangeStreamer.useStrictConsistency)
                        {
                            if (addressList.size() > 1)
                                throw new IllegalStateException("Multiple strict sources found for " + toFetch);

                            InetAddress sourceIp = addressList.iterator().next();
                            if (Gossiper.instance.isEnabled() && !Gossiper.instance.getEndpointStateForEndpoint(sourceIp).isAlive())
                                throw new RuntimeException("A node required to move the data consistently is down ("+sourceIp+").  If you wish to move the data from a potentially inconsistent replica, restart the node with -Dcassandra.consistent.rangemovement=false");
                        }
                    }

                    // calculating endpoints to stream current ranges to if needed
                    // in some situations node will handle current ranges as part of the new ranges
                    Multimap<InetAddress, Range<Token>> endpointRanges = HashMultimap.create();
                    for (Range<Token> toStream : rangesPerKeyspace.left)
                    {
                        Set<InetAddress> currentEndpoints = ImmutableSet.copyOf(strategy.calculateNaturalEndpoints(toStream.right, tokenMetaClone));
                        Set<InetAddress> newEndpoints = ImmutableSet.copyOf(strategy.calculateNaturalEndpoints(toStream.right, tokenMetaCloneAllSettled));
                        logger.debug("Range: {} Current endpoints: {} New endpoints: {}", toStream, currentEndpoints, newEndpoints);
                        for (InetAddress address : Sets.difference(newEndpoints, currentEndpoints))
                        {
                            logger.debug("Range {} has new owner {}", toStream, address);
                            endpointRanges.put(address, toStream);
                        }
                    }

                    // stream ranges
                    for (InetAddress address : endpointRanges.keySet())
                    {
                        logger.debug("Will stream range {} of keyspace {} to endpoint {}", endpointRanges.get(address), keyspace, address);
                        InetAddress preferred = SystemKeyspace.getPreferredIP(address);
                        streamPlan.transferRanges(address, preferred, keyspace, endpointRanges.get(address));
                    }

                    // stream requests
                    Multimap<InetAddress, Range<Token>> workMap = RangeStreamer.getWorkMap(rangesToFetchWithPreferredEndpoints, keyspace);
                    for (InetAddress address : workMap.keySet())
                    {
                        logger.debug("Will request range {} of keyspace {} from endpoint {}", workMap.get(address), keyspace, address);
                        InetAddress preferred = SystemKeyspace.getPreferredIP(address);
                        streamPlan.requestRanges(address, preferred, keyspace, workMap.get(address));
                    }

                    logger.debug("Keyspace {}: work map {}.", keyspace, workMap);
                }
            }
        }

        public Future<StreamState> stream()
        {
            return streamPlan.execute();
        }

        public boolean streamsNeeded()
        {
            return !streamPlan.isEmpty();
        }
    }

    /**
     * Get the status of a token removal.
     */
    public String getRemovalStatus()
    {
        if (removingNode == null) {
            return "No token removals in process.";
        }
        return String.format("Removing token (%s). Waiting for replication confirmation from [%s].",
                             _token_metadata.getToken(removingNode),
                             StringUtils.join(replicatingNodes, ","));
    }

    /**
     * Force a remove operation to complete. This may be necessary if a remove operation
     * blocks forever due to node/stream failure. removeToken() must be called
     * first, this is a last resort measure.  No further attempt will be made to restore replicas.
     */
    public void forceRemoveCompletion()
    {
        if (!replicatingNodes.isEmpty()  || !_token_metadata.getLeavingEndpoints().isEmpty())
        {
            logger.warn("Removal not confirmed for for {}", StringUtils.join(this.replicatingNodes, ","));
            for (InetAddress endpoint : _token_metadata.getLeavingEndpoints())
            {
                UUID hostId = _token_metadata.getHostId(endpoint);
                Gossiper.instance.advertiseTokenRemoved(endpoint, hostId);
                excise(_token_metadata.getTokens(endpoint), endpoint);
            }
            replicatingNodes.clear();
            removingNode = null;
        }
        else
        {
            logger.warn("No tokens to force removal on, call 'removenode' first");
        }
    }
#endif
    /**
     * Remove a node that has died, attempting to restore the replica count.
     * If the node is alive, decommission should be attempted.  If decommission
     * fails, then removeToken should be called.  If we fail while trying to
     * restore the replica count, finally forceRemoveCompleteion should be
     * called to forcibly remove the node without regard to replica count.
     *
     * @param hostIdString token for the node
     */
    future<> remove_node(sstring host_id_string);

#if 0
    public void confirmReplication(InetAddress node)
    {
        // replicatingNodes can be empty in the case where this node used to be a removal coordinator,
        // but restarted before all 'replication finished' messages arrived. In that case, we'll
        // still go ahead and acknowledge it.
        if (!replicatingNodes.isEmpty())
        {
            replicatingNodes.remove(node);
        }
        else
        {
            logger.info("Received unexpected REPLICATION_FINISHED message from {}. Was this node recently a removal coordinator?", node);
        }
    }
#endif
    sstring get_operation_mode();

    bool is_starting();
#if 0
    public String getDrainProgress()
    {
        return String.format("Drained %s/%s ColumnFamilies", remainingCFs, totalCFs);
    }
#endif

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
    std::map<gms::inet_address, float> get_ownership() const;

    std::map<gms::inet_address, float> effective_ownership(sstring keyspace) const;
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

    /**
     * Seed data to the endpoints that will be responsible for it at the future
     *
     * @param rangesToStreamByKeyspace keyspaces and data ranges with endpoints included for each
     * @return async Future for whether stream was success
     */
    private Future<StreamState> streamRanges(Map<String, Multimap<Range<Token>, InetAddress>> rangesToStreamByKeyspace)
    {
        // First, we build a list of ranges to stream to each host, per table
        Map<String, Map<InetAddress, List<Range<Token>>>> sessionsToStreamByKeyspace = new HashMap<>();
        for (Map.Entry<String, Multimap<Range<Token>, InetAddress>> entry : rangesToStreamByKeyspace.entrySet())
        {
            String keyspace = entry.getKey();
            Multimap<Range<Token>, InetAddress> rangesWithEndpoints = entry.getValue();

            if (rangesWithEndpoints.isEmpty())
                continue;

            Map<InetAddress, List<Range<Token>>> rangesPerEndpoint = new HashMap<>();
            for (Map.Entry<Range<Token>, InetAddress> endPointEntry : rangesWithEndpoints.entries())
            {
                Range<Token> range = endPointEntry.getKey();
                InetAddress endpoint = endPointEntry.getValue();

                List<Range<Token>> curRanges = rangesPerEndpoint.get(endpoint);
                if (curRanges == null)
                {
                    curRanges = new LinkedList<>();
                    rangesPerEndpoint.put(endpoint, curRanges);
                }
                curRanges.add(range);
            }

            sessionsToStreamByKeyspace.put(keyspace, rangesPerEndpoint);
        }

        StreamPlan streamPlan = new StreamPlan("Unbootstrap");
        for (Map.Entry<String, Map<InetAddress, List<Range<Token>>>> entry : sessionsToStreamByKeyspace.entrySet())
        {
            String keyspaceName = entry.getKey();
            Map<InetAddress, List<Range<Token>>> rangesPerEndpoint = entry.getValue();

            for (Map.Entry<InetAddress, List<Range<Token>>> rangesEntry : rangesPerEndpoint.entrySet())
            {
                List<Range<Token>> ranges = rangesEntry.getValue();
                InetAddress newEndpoint = rangesEntry.getKey();
                InetAddress preferred = SystemKeyspace.getPreferredIP(newEndpoint);

                // TODO each call to transferRanges re-flushes, this is potentially a lot of waste
                streamPlan.transferRanges(newEndpoint, preferred, keyspaceName, ranges);
            }
        }
        return streamPlan.execute();
    }

    /**
     * Calculate pair of ranges to stream/fetch for given two range collections
     * (current ranges for keyspace and ranges after move to new token)
     *
     * @param current collection of the ranges by current token
     * @param updated collection of the ranges after token is changed
     * @return pair of ranges to stream/fetch for given current and updated range collections
     */
    public Pair<Set<Range<Token>>, Set<Range<Token>>> calculateStreamAndFetchRanges(Collection<Range<Token>> current, Collection<Range<Token>> updated)
    {
        Set<Range<Token>> toStream = new HashSet<>();
        Set<Range<Token>> toFetch  = new HashSet<>();


        for (Range r1 : current)
        {
            boolean intersect = false;
            for (Range r2 : updated)
            {
                if (r1.intersects(r2))
                {
                    // adding difference ranges to fetch from a ring
                    toStream.addAll(r1.subtract(r2));
                    intersect = true;
                }
            }
            if (!intersect)
            {
                toStream.add(r1); // should seed whole old range
            }
        }

        for (Range r2 : updated)
        {
            boolean intersect = false;
            for (Range r1 : current)
            {
                if (r2.intersects(r1))
                {
                    // adding difference ranges to fetch from a ring
                    toFetch.addAll(r2.subtract(r1));
                    intersect = true;
                }
            }
            if (!intersect)
            {
                toFetch.add(r2); // should fetch whole old range
            }
        }

        return Pair.create(toStream, toFetch);
    }

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
    int32_t get_exception_count();
#if 0
    public void rescheduleFailedDeletions()
    {
        SSTableDeletingTask.rescheduleFailedTasks();
    }

    /**
     * #{@inheritDoc}
     */
    public void loadNewSSTables(String ksName, String cfName)
    {
        ColumnFamilyStore.loadNewSSTables(ksName, cfName);
    }

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
};

extern distributed<storage_service> _the_storage_service;

inline distributed<storage_service>& get_storage_service() {
    return _the_storage_service;
}

inline storage_service& get_local_storage_service() {
    return _the_storage_service.local();
}

inline future<std::vector<dht::token>> sorted_tokens() {
    return smp::submit_to(0, [] {
        return get_local_storage_service().get_token_metadata().sorted_tokens();
    });
}
inline future<std::vector<dht::token>> get_tokens(const gms::inet_address& addr) {
    return smp::submit_to(0, [addr] {
        return get_local_storage_service().get_token_metadata().get_tokens(addr);
    });
}

inline future<std::map<dht::token, gms::inet_address>> get_token_to_endpoint() {
    return smp::submit_to(0, [] {
        return get_local_storage_service().get_token_metadata().get_token_to_endpoint();
    });
}

inline future<> init_storage_service(distributed<database>& db) {
    return service::get_storage_service().start(std::ref(db)).then([] {
        print("Start Storage service ...\n");
    });
}

inline future<> deinit_storage_service() {
    return service::get_storage_service().stop();
}

}
