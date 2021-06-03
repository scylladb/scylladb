/*
 * Copyright (C) 2015-present ScyllaDB
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

#include "utils/UUID.hh"
#include "token_metadata.hh"
#include <optional>
#include "locator/snitch_base.hh"
#include "locator/abstract_replication_strategy.hh"
#include "log.hh"
#include "partition_range_compat.hh"
#include <unordered_map>
#include <algorithm>
#include <boost/icl/interval.hpp>
#include <boost/icl/interval_map.hpp>
#include <seastar/core/coroutine.hh>
#include <boost/range/adaptors.hpp>

namespace locator {

static logging::logger tlogger("token_metadata");

template <typename C, typename V>
static void remove_by_value(C& container, V value) {
    for (auto it = container.begin(); it != container.end();) {
        if (it->second == value) {
            it = container.erase(it);
        } else {
            it++;
        }
    }
}

class token_metadata_impl final {
public:
    using UUID = utils::UUID;
    using inet_address = gms::inet_address;
private:
    /**
     * Maintains token to endpoint map of every node in the cluster.
     * Each Token is associated with exactly one Address, but each Address may have
     * multiple tokens.  Hence, the BiMultiValMap collection.
     */
    // FIXME: have to be BiMultiValMap
    std::unordered_map<token, inet_address> _token_to_endpoint_map;

    /** Maintains endpoint to host ID map of every node in the cluster */
    std::unordered_map<inet_address, utils::UUID> _endpoint_to_host_id_map;

    std::unordered_map<token, inet_address> _bootstrap_tokens;
    std::unordered_set<inet_address> _leaving_endpoints;
    // The map between the existing node to be replaced and the replacing node
    std::unordered_map<inet_address, inet_address> _replacing_endpoints;

    std::unordered_map<sstring, boost::icl::interval_map<token, std::unordered_set<inet_address>>> _pending_ranges_interval_map;

    std::vector<token> _sorted_tokens;

    topology _topology;

    long _ring_version = 0;

    // Note: if any member is added to this class
    // clone_async() must be updated to copy that member.

    void sort_tokens();

    using tokens_iterator = tokens_iterator_impl;

public:
    token_metadata_impl(std::unordered_map<token, inet_address> token_to_endpoint_map, std::unordered_map<inet_address, utils::UUID> endpoints_map, topology topology);
    token_metadata_impl() noexcept {};
    token_metadata_impl(const token_metadata_impl&) = default;
    token_metadata_impl(token_metadata_impl&&) noexcept = default;
    const std::vector<token>& sorted_tokens() const;
    future<> update_normal_token(token token, inet_address endpoint);
    future<> update_normal_tokens(std::unordered_set<token> tokens, inet_address endpoint);
    future<> update_normal_tokens(const std::unordered_map<inet_address, std::unordered_set<token>>& endpoint_tokens);
    void update_normal_tokens_sync(std::unordered_set<token> tokens, inet_address endpoint);
    void update_normal_tokens_sync(const std::unordered_map<inet_address, std::unordered_set<token>>& endpoint_tokens);
    const token& first_token(const token& start) const;
    size_t first_token_index(const token& start) const;
    std::optional<inet_address> get_endpoint(const token& token) const;
    std::vector<token> get_tokens(const inet_address& addr) const;
    const std::unordered_map<token, inet_address>& get_token_to_endpoint() const {
        return _token_to_endpoint_map;
    }

    const std::unordered_set<inet_address>& get_leaving_endpoints() const {
        return _leaving_endpoints;
    }

    const std::unordered_map<token, inet_address>& get_bootstrap_tokens() const {
        return _bootstrap_tokens;
    }

    void update_topology(inet_address ep) {
        _topology.update_endpoint(ep);
    }

    tokens_iterator tokens_end() const;

    /**
     * Creates an iterable range of the sorted tokens starting at the token next
     * after the given one.
     *
     * @param start A token that will define the beginning of the range
     *
     * @return The requested range (see the description above)
     */
    boost::iterator_range<tokens_iterator> ring_range(const token& start, bool include_min = false) const;

    boost::iterator_range<tokens_iterator> ring_range(
        const std::optional<dht::partition_range::bound>& start, bool include_min = false) const;

    topology& get_topology() {
        return _topology;
    }

    const topology& get_topology() const {
        return _topology;
    }

    void debug_show() const;
#if 0
    private static final Logger logger = LoggerFactory.getLogger(TokenMetadata.class);

    /**
     * Maintains token to endpoint map of every node in the cluster.
     * Each Token is associated with exactly one Address, but each Address may have
     * multiple tokens.  Hence, the BiMultiValMap collection.
     */
    private final BiMultiValMap<Token, InetAddress> tokenToEndpointMap;

    /** Maintains endpoint to host ID map of every node in the cluster */
    private final BiMap<InetAddress, UUID> _endpoint_to_host_id_map;

    // Prior to CASSANDRA-603, we just had <tt>Map<Range, InetAddress> pendingRanges<tt>,
    // which was added to when a node began bootstrap and removed from when it finished.
    //
    // This is inadequate when multiple changes are allowed simultaneously.  For example,
    // suppose that there is a ring of nodes A, C and E, with replication factor 3.
    // Node D bootstraps between C and E, so its pending ranges will be E-A, A-C and C-D.
    // Now suppose node B bootstraps between A and C at the same time. Its pending ranges
    // would be C-E, E-A and A-B. Now both nodes need to be assigned pending range E-A,
    // which we would be unable to represent with the old Map.  The same thing happens
    // even more obviously for any nodes that boot simultaneously between same two nodes.
    //
    // So, we made two changes:
    //
    // First, we changed pendingRanges to a <tt>Multimap<Range, InetAddress></tt> (now
    // <tt>Map<String, Multimap<Range, InetAddress>></tt>, because replication strategy
    // and options are per-KeySpace).
    //
    // Second, we added the bootstrapTokens and leavingEndpoints collections, so we can
    // rebuild pendingRanges from the complete information of what is going on, when
    // additional changes are made mid-operation.
    //
    // Finally, note that recording the tokens of joining nodes in bootstrapTokens also
    // means we can detect and reject the addition of multiple nodes at the same token
    // before one becomes part of the ring.
    private final BiMultiValMap<Token, InetAddress> bootstrapTokens = new BiMultiValMap<Token, InetAddress>();
    // (don't need to record Token here since it's still part of tokenToEndpointMap until it's done leaving)
    private final Set<InetAddress> leavingEndpoints = new HashSet<InetAddress>();
    // this is a cache of the calculation from {tokenToEndpointMap, bootstrapTokens, leavingEndpoints}

    // nodes which are migrating to the new tokens in the ring
    private final Set<Pair<Token, InetAddress>> _moving_endpoints = new HashSet<Pair<Token, InetAddress>>();

    /* Use this lock for manipulating the token map */
    private final ReadWriteLock lock = new ReentrantReadWriteLock(true);
    private volatile ArrayList<Token> sortedTokens;

    private final Topology topology;

    private static final Comparator<InetAddress> inetaddressCmp = new Comparator<InetAddress>()
    {
        public int compare(InetAddress o1, InetAddress o2)
        {
            return ByteBuffer.wrap(o1.getAddress()).compareTo(ByteBuffer.wrap(o2.getAddress()));
        }
    };

    // signals replication strategies that nodes have joined or left the ring and they need to recompute ownership
    private volatile long ringVersion = 0;

    public TokenMetadata()
    {
        this(SortedBiMultiValMap.<Token, InetAddress>create(null, inetaddressCmp),
             HashBiMap.<InetAddress, UUID>create(),
             new Topology());
    }

    private TokenMetadata(BiMultiValMap<Token, InetAddress> tokenToEndpointMap, BiMap<InetAddress, UUID> endpointsMap, Topology topology)
    {
        this.tokenToEndpointMap = tokenToEndpointMap;
        this.topology = topology;
        _endpoint_to_host_id_map = endpointsMap;
        sortedTokens = sortTokens();
    }

    private ArrayList<Token> sortTokens()
    {
        return new ArrayList<Token>(tokenToEndpointMap.keySet());
    }

    /** @return the number of nodes bootstrapping into source's primary range */
    public int pendingRangeChanges(InetAddress source)
    {
        int n = 0;
        Collection<Range<Token>> sourceRanges = getPrimaryRangesFor(getTokens(source));
        lock.readLock().lock();
        try
        {
            for (Token token : _bootstrap_tokens.keySet())
                for (Range<Token> range : sourceRanges)
                    if (range.contains(token))
                        n++;
        }
        finally
        {
            lock.readLock().unlock();
        }
        return n;
    }

    /**
     * Update token map with a single token/endpoint pair in normal state.
     */
    public void updateNormalToken(Token token, InetAddress endpoint)
    {
        updateNormalTokens(Collections.singleton(token), endpoint);
    }

    public void updateNormalTokens(Collection<Token> tokens, InetAddress endpoint)
    {
        Multimap<InetAddress, Token> endpointTokens = HashMultimap.create();
        for (Token token : tokens)
            endpointTokens.put(endpoint, token);
        updateNormalTokens(endpointTokens);
    }

    /**
     * Update token map with a set of token/endpoint pairs in normal state.
     *
     * Prefer this whenever there are multiple pairs to update, as each update (whether a single or multiple)
     * is expensive (CASSANDRA-3831).
     *
     * @param endpointTokens
     */
    public void updateNormalTokens(Multimap<InetAddress, Token> endpointTokens)
    {
        if (endpointTokens.isEmpty())
            return;

        lock.writeLock().lock();
        try
        {
            boolean shouldSortTokens = false;
            for (InetAddress endpoint : endpointTokens.keySet())
            {
                Collection<Token> tokens = endpointTokens.get(endpoint);

                assert tokens != null && !tokens.isEmpty();

                _bootstrap_tokens.removeValue(endpoint);
                tokenToEndpointMap.removeValue(endpoint);
                topology.addEndpoint(endpoint);
                _leaving_endpoints.remove(endpoint);
                removeFromMoving(endpoint); // also removing this endpoint from moving

                for (Token token : tokens)
                {
                    InetAddress prev = tokenToEndpointMap.put(token, endpoint);
                    if (!endpoint.equals(prev))
                    {
                        if (prev != null)
                            logger.warn("Token {} changing ownership from {} to {}", token, prev, endpoint);
                        shouldSortTokens = true;
                    }
                }
            }

            if (shouldSortTokens)
                sortedTokens = sortTokens();
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }
#endif

    /**
     * Store an end-point to host ID mapping.  Each ID must be unique, and
     * cannot be changed after the fact.
     *
     * @param hostId
     * @param endpoint
     */
    void update_host_id(const UUID& host_id, inet_address endpoint);

    /** Return the unique host ID for an end-point. */
    UUID get_host_id(inet_address endpoint) const;

    /// Return the unique host ID for an end-point or nullopt if not found.
    std::optional<UUID> get_host_id_if_known(inet_address endpoint) const;

    /** Return the end-point for a unique host ID */
    std::optional<inet_address> get_endpoint_for_host_id(UUID host_id) const;

    /** @return a copy of the endpoint-to-id map for read-only operations */
    const std::unordered_map<inet_address, utils::UUID>& get_endpoint_to_host_id_map_for_reading() const;

    void add_bootstrap_token(token t, inet_address endpoint);

    void add_bootstrap_tokens(std::unordered_set<token> tokens, inet_address endpoint);

    void remove_bootstrap_tokens(std::unordered_set<token> tokens);

    void add_leaving_endpoint(inet_address endpoint);
    void del_leaving_endpoint(inet_address endpoint);
public:
    void remove_endpoint(inet_address endpoint);
#if 0

    public Collection<Token> getTokens(InetAddress endpoint)
    {
        assert endpoint != null;
        assert isMember(endpoint); // don't want to return nulls

        lock.readLock().lock();
        try
        {
            return new ArrayList<Token>(tokenToEndpointMap.inverse().get(endpoint));
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    @Deprecated
    public Token getToken(InetAddress endpoint)
    {
        return getTokens(endpoint).iterator().next();
    }

#endif

    bool is_member(inet_address endpoint) const;

    bool is_leaving(inet_address endpoint) const;

    // Is this node being replaced by another node
    bool is_being_replaced(inet_address endpoint) const;

    // Is any node being replaced by another node
    bool is_any_node_being_replaced() const;

    void add_replacing_endpoint(inet_address existing_node, inet_address replacing_node);

    void del_replacing_endpoint(inet_address existing_node);
#if 0
    private final AtomicReference<TokenMetadata> cachedTokenMap = new AtomicReference<TokenMetadata>();
#endif
public:

    /**
     * Create a full copy of token_metadata_impl using asynchronous continuations.
     * The caller must ensure that the cloned object will not change if
     * the function yields.
     */
    future<token_metadata_impl> clone_async() const noexcept;

    /**
     * Create a copy of TokenMetadata with only tokenToEndpointMap. That is, pending ranges,
     * bootstrap tokens and leaving endpoints are not included in the copy.
     */
    token_metadata_impl clone_only_token_map_sync() const {
        return token_metadata_impl(this->_token_to_endpoint_map, this->_endpoint_to_host_id_map, this->_topology);
    }
#if 0

    /**
     * Return a cached TokenMetadata with only tokenToEndpointMap, i.e., the same as cloneOnlyTokenMap but
     * uses a cached copy that is invalided when the ring changes, so in the common case
     * no extra locking is required.
     *
     * Callers must *NOT* mutate the returned metadata object.
     */
    public TokenMetadata cachedOnlyTokenMap()
    {
        TokenMetadata tm = cachedTokenMap.get();
        if (tm != null)
            return tm;

        // synchronize to prevent thundering herd (CASSANDRA-6345)
        synchronized (this)
        {
            if ((tm = cachedTokenMap.get()) != null)
                return tm;

            tm = cloneOnlyTokenMap();
            cachedTokenMap.set(tm);
            return tm;
        }
    }
#endif
    /**
     * Create a copy of TokenMetadata with only tokenToEndpointMap. That is, pending ranges,
     * bootstrap tokens and leaving endpoints are not included in the copy.
     * The caller must ensure that the cloned object will not change if
     * the function yields.
     */
    future<token_metadata_impl> clone_only_token_map(bool clone_sorted_tokens = true) const noexcept;

    /**
     * Create a copy of TokenMetadata with tokenToEndpointMap reflecting situation after all
     * current leave operations have finished.
     *
     * @return new token metadata
     */
    future<token_metadata_impl> clone_after_all_left() const noexcept {
      return clone_only_token_map(false).then([this] (token_metadata_impl all_left_metadata) {
        for (auto endpoint : _leaving_endpoints) {
            all_left_metadata.remove_endpoint(endpoint);
        }
        all_left_metadata.sort_tokens();

        return std::move(all_left_metadata);
      });
    }

    /**
     * Destroy the token_metadata members using continuations
     * to prevent reactor stalls.
     */
    future<> clear_gently() noexcept;

public:
    dht::token_range_vector get_primary_ranges_for(std::unordered_set<token> tokens) const;

    dht::token_range_vector get_primary_ranges_for(token right) const;
    static boost::icl::interval<token>::interval_type range_to_interval(range<dht::token> r);
    static range<dht::token> interval_to_range(boost::icl::interval<token>::interval_type i);

private:
    void set_pending_ranges(const sstring& keyspace_name, std::unordered_multimap<range<token>, inet_address> new_pending_ranges, can_yield);

public:
    bool has_pending_ranges(sstring keyspace_name, inet_address endpoint) const;

     /**
     * Calculate pending ranges according to bootsrapping and leaving nodes. Reasoning is:
     *
     * (1) When in doubt, it is better to write too much to a node than too little. That is, if
     * there are multiple nodes moving, calculate the biggest ranges a node could have. Cleaning
     * up unneeded data afterwards is better than missing writes during movement.
     * (2) When a node leaves, ranges for other nodes can only grow (a node might get additional
     * ranges, but it will not lose any of its current ranges as a result of a leave). Therefore
     * we will first remove _all_ leaving tokens for the sake of calculation and then check what
     * ranges would go where if all nodes are to leave. This way we get the biggest possible
     * ranges with regard current leave operations, covering all subsets of possible final range
     * values.
     * (3) When a node bootstraps, ranges of other nodes can only get smaller. Without doing
     * complex calculations to see if multiple bootstraps overlap, we simply base calculations
     * on the same token ring used before (reflecting situation after all leave operations have
     * completed). Bootstrapping nodes will be added and removed one by one to that metadata and
     * checked what their ranges would be. This will give us the biggest possible ranges the
     * node could have. It might be that other bootstraps make our actual final ranges smaller,
     * but it does not matter as we can clean up the data afterwards.
     *
     * NOTE: This is heavy and ineffective operation. This will be done only once when a node
     * changes state in the cluster, so it should be manageable.
     */
    future<> update_pending_ranges(
            const token_metadata& unpimplified_this,
            const abstract_replication_strategy& strategy, const sstring& keyspace_name);
    void calculate_pending_ranges_for_leaving(
        const token_metadata& unpimplified_this,
        const abstract_replication_strategy& strategy,
        std::unordered_multimap<range<token>, inet_address>& new_pending_ranges,
        mutable_token_metadata_ptr all_left_metadata) const;
    void calculate_pending_ranges_for_bootstrap(
        const abstract_replication_strategy& strategy,
        std::unordered_multimap<range<token>, inet_address>& new_pending_ranges,
        mutable_token_metadata_ptr all_left_metadata) const;
    void calculate_pending_ranges_for_replacing(
        const token_metadata& unpimplified_this,
        const abstract_replication_strategy& strategy,
        std::unordered_multimap<range<token>, inet_address>& new_pending_ranges) const;
public:

    token get_predecessor(token t) const;

#if 0
    public Token getSuccessor(Token token)
    {
        List tokens = sortedTokens();
        int index = Collections.binarySearch(tokens, token);
        assert index >= 0 : token + " not found in " + StringUtils.join(tokenToEndpointMap.keySet(), ", ");
        return (Token) ((index == (tokens.size() - 1)) ? tokens.get(0) : tokens.get(index + 1));
    }

    /** @return a copy of the bootstrapping tokens map */
    public BiMultiValMap<Token, InetAddress> getBootstrapTokens()
    {
        lock.readLock().lock();
        try
        {
            return new BiMultiValMap<Token, InetAddress>(_bootstrap_tokens);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

#endif
    // Returns nodes that are officially part of the ring. It does not include
    // node that is still joining the cluster, e.g., a node that is still
    // streaming data before it finishes the bootstrap process and turns into
    // NORMAL status.
    std::vector<inet_address> get_all_endpoints() const {
        auto tmp = boost::copy_range<std::unordered_set<gms::inet_address>>(_token_to_endpoint_map | boost::adaptors::map_values);
        return std::vector<inet_address>(tmp.begin(), tmp.end());
    }

    /* Returns the number of different endpoints that own tokens in the ring.
     * Bootstrapping tokens are not taken into account. */
    size_t count_normal_token_owners() const;

#if 0
    public Set<InetAddress> getAllEndpoints()
    {
        lock.readLock().lock();
        try
        {
            return ImmutableSet.copyOf(_endpoint_to_host_id_map.keySet());
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    /** caller should not modify _leaving_endpoints */
    public Set<InetAddress> getLeavingEndpoints()
    {
        lock.readLock().lock();
        try
        {
            return ImmutableSet.copyOf(_leaving_endpoints);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    /**
     * Endpoints which are migrating to the new tokens
     * @return set of addresses of moving endpoints
     */
    public Set<Pair<Token, InetAddress>> getMovingEndpoints()
    {
        lock.readLock().lock();
        try
        {
            return ImmutableSet.copyOf(_moving_endpoints);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    public static int firstTokenIndex(final ArrayList ring, Token start, boolean insertMin)
    {
        assert ring.size() > 0;
        // insert the minimum token (at index == -1) if we were asked to include it and it isn't a member of the ring
        int i = Collections.binarySearch(ring, start);
        if (i < 0)
        {
            i = (i + 1) * (-1);
            if (i >= ring.size())
                i = insertMin ? -1 : 0;
        }
        return i;
    }

    public static Token firstToken(final ArrayList<Token> ring, Token start)
    {
        return ring.get(firstTokenIndex(ring, start, false));
    }

    /**
     * iterator over the Tokens in the given ring, starting with the token for the node owning start
     * (which does not have to be a Token in the ring)
     * @param includeMin True if the minimum token should be returned in the ring even if it has no owner.
     */
    public static Iterator<Token> ringIterator(final ArrayList<Token> ring, Token start, boolean includeMin)
    {
        if (ring.isEmpty())
            return includeMin ? Iterators.singletonIterator(StorageService.getPartitioner().getMinimumToken())
                              : Iterators.<Token>emptyIterator();

        final boolean insertMin = includeMin && !ring.get(0).isMinimum();
        final int startIndex = firstTokenIndex(ring, start, insertMin);
        return new AbstractIterator<Token>()
        {
            int j = startIndex;
            protected Token computeNext()
            {
                if (j < -1)
                    return endOfData();
                try
                {
                    // return minimum for index == -1
                    if (j == -1)
                        return StorageService.getPartitioner().getMinimumToken();
                    // return ring token for other indexes
                    return ring.get(j);
                }
                finally
                {
                    j++;
                    if (j == ring.size())
                        j = insertMin ? -1 : 0;
                    if (j == startIndex)
                        // end iteration
                        j = -2;
                }
            }
        };
    }

    /** used by tests */
    public void clearUnsafe()
    {
        lock.writeLock().lock();
        try
        {
            tokenToEndpointMap.clear();
            _endpoint_to_host_id_map.clear();
            _bootstrap_tokens.clear();
            _leaving_endpoints.clear();
            sortedTokens.clear();
            topology.clear();
            invalidateCachedRings();
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        lock.readLock().lock();
        try
        {
            Set<InetAddress> eps = tokenToEndpointMap.inverse().keySet();

            if (!eps.isEmpty())
            {
                sb.append("Normal Tokens:");
                sb.append(System.getProperty("line.separator"));
                for (InetAddress ep : eps)
                {
                    sb.append(ep);
                    sb.append(":");
                    sb.append(tokenToEndpointMap.inverse().get(ep));
                    sb.append(System.getProperty("line.separator"));
                }
            }

            if (!_bootstrap_tokens.isEmpty())
            {
                sb.append("Bootstrapping Tokens:" );
                sb.append(System.getProperty("line.separator"));
                for (Map.Entry<Token, InetAddress> entry : _bootstrap_tokens.entrySet())
                {
                    sb.append(entry.getValue()).append(":").append(entry.getKey());
                    sb.append(System.getProperty("line.separator"));
                }
            }

            if (!_leaving_endpoints.isEmpty())
            {
                sb.append("Leaving Endpoints:");
                sb.append(System.getProperty("line.separator"));
                for (InetAddress ep : _leaving_endpoints)
                {
                    sb.append(ep);
                    sb.append(System.getProperty("line.separator"));
                }
            }

            if (!_pending_ranges.isEmpty())
            {
                sb.append("Pending Ranges:");
                sb.append(System.getProperty("line.separator"));
                sb.append(printPendingRanges());
            }
        }
        finally
        {
            lock.readLock().unlock();
        }

        return sb.toString();
    }
#endif
public:
    // returns empty vector if keyspace_name not found.
    inet_address_vector_topology_change pending_endpoints_for(const token& token, const sstring& keyspace_name) const;
#if 0
    /**
     * @deprecated retained for benefit of old tests
     */
    public Collection<InetAddress> getWriteEndpoints(Token token, String keyspaceName, Collection<InetAddress> naturalEndpoints)
    {
        return ImmutableList.copyOf(Iterables.concat(naturalEndpoints, pendingEndpointsFor(token, keyspaceName)));
    }
#endif

public:
    /** @return an endpoint to token multimap representation of tokenToEndpointMap (a copy) */
    std::multimap<inet_address, token> get_endpoint_to_token_map_for_reading() const;
    /**
     * @return a (stable copy, won't be modified) Token to Endpoint map for all the normal and bootstrapping nodes
     *         in the cluster.
     */
    std::map<token, inet_address> get_normal_and_bootstrapping_token_to_endpoint_map() const;

#if 0
    /**
     * @return the Topology map of nodes to DCs + Racks
     *
     * This is only allowed when a copy has been made of TokenMetadata, to avoid concurrent modifications
     * when Topology methods are subsequently used by the caller.
     */
    public Topology getTopology()
    {
        assert this != StorageService.instance.getTokenMetadata();
        return topology;
    }

    public long getRingVersion()
    {
        return ringVersion;
    }

    public void invalidateCachedRings()
    {
        ringVersion++;
        cachedTokenMap.set(null);
    }

    /**
     * Tracks the assignment of racks and endpoints in each datacenter for all the "normal" endpoints
     * in this TokenMetadata. This allows faster calculation of endpoints in NetworkTopologyStrategy.
     */
    public static class Topology
    {
        /** multi-map of DC to endpoints in that DC */
        private final Multimap<String, InetAddress> dcEndpoints;
        /** map of DC to multi-map of rack to endpoints in that rack */
        private final Map<String, Multimap<String, InetAddress>> dcRacks;
        /** reverse-lookup map for endpoint to current known dc/rack assignment */
        private final Map<InetAddress, Pair<String, String>> currentLocations;

        protected Topology()
        {
            dcEndpoints = HashMultimap.create();
            dcRacks = new HashMap<String, Multimap<String, InetAddress>>();
            currentLocations = new HashMap<InetAddress, Pair<String, String>>();
        }

        protected void clear()
        {
            dcEndpoints.clear();
            dcRacks.clear();
            currentLocations.clear();
        }

        /**
         * construct deep-copy of other
         */
        protected Topology(Topology other)
        {
            dcEndpoints = HashMultimap.create(other.dcEndpoints);
            dcRacks = new HashMap<String, Multimap<String, InetAddress>>();
            for (String dc : other.dcRacks.keySet())
                dcRacks.put(dc, HashMultimap.create(other.dcRacks.get(dc)));
            currentLocations = new HashMap<InetAddress, Pair<String, String>>(other.currentLocations);
        }

        /**
         * Stores current DC/rack assignment for ep
         */
        protected void addEndpoint(InetAddress ep)
        {
            IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
            String dc = snitch.getDatacenter(ep);
            String rack = snitch.getRack(ep);
            Pair<String, String> current = currentLocations.get(ep);
            if (current != null)
            {
                if (current.left.equals(dc) && current.right.equals(rack))
                    return;
                dcRacks.get(current.left).remove(current.right, ep);
                dcEndpoints.remove(current.left, ep);
            }

            dcEndpoints.put(dc, ep);

            if (!dcRacks.containsKey(dc))
                dcRacks.put(dc, HashMultimap.<String, InetAddress>create());
            dcRacks.get(dc).put(rack, ep);

            currentLocations.put(ep, Pair.create(dc, rack));
        }

        /**
         * Removes current DC/rack assignment for ep
         */
        protected void removeEndpoint(InetAddress ep)
        {
            if (!currentLocations.containsKey(ep))
                return;
            Pair<String, String> current = currentLocations.remove(ep);
            dcEndpoints.remove(current.left, ep);
            dcRacks.get(current.left).remove(current.right, ep);
        }

        /**
         * @return multi-map of DC to endpoints in that DC
         */
        public Multimap<String, InetAddress> getDatacenterEndpoints()
        {
            return dcEndpoints;
        }

        /**
         * @return map of DC to multi-map of rack to endpoints in that rack
         */
        public Map<String, Multimap<String, InetAddress>> getDatacenterRacks()
        {
            return dcRacks;
        }
    }
#endif
    long get_ring_version() const {
        return _ring_version;
    }

    void invalidate_cached_rings() {
        ++_ring_version;
        //cachedTokenMap.set(null);
    }

    friend class token_metadata;
};

class tokens_iterator_impl {
public:
    using iterator_category = std::input_iterator_tag;
    using value_type = token;
    using difference_type = std::ptrdiff_t;
    using pointer = token*;
    using reference = token&;
private:
    tokens_iterator_impl(std::vector<token>::const_iterator it, size_t pos)
    : _cur_it(it), _ring_pos(pos), _insert_min(false) {}

public:
    tokens_iterator_impl(const token& start, const token_metadata_impl* token_metadata, bool include_min = false)
    : _token_metadata(token_metadata) {
        _cur_it = _token_metadata->sorted_tokens().begin() + _token_metadata->first_token_index(start);
        _insert_min = include_min && *_token_metadata->sorted_tokens().begin() != dht::minimum_token();
        if (_token_metadata->sorted_tokens().empty()) {
            _min = true;
        }
    }

    bool operator==(const tokens_iterator_impl& it) const {
        return _min == it._min && _cur_it == it._cur_it;
    }

    bool operator!=(const tokens_iterator_impl& it) const {
        return _min != it._min || _cur_it != it._cur_it;
    }

    const token& operator*() {
        if (_min) {
            return _min_token;
        } else {
            return *_cur_it;
        }
    }

    tokens_iterator_impl& operator++() {
        if (!_min) {
            if (_ring_pos >= _token_metadata->sorted_tokens().size()) {
                _cur_it = _token_metadata->sorted_tokens().end();
            } else {
                ++_cur_it;
                ++_ring_pos;

                if (_cur_it == _token_metadata->sorted_tokens().end()) {
                    _cur_it = _token_metadata->sorted_tokens().begin();
                    _min = _insert_min;
                }
            }
        } else {
            _min = false;
        }
        return *this;
    }

private:
    std::vector<token>::const_iterator _cur_it;
    //
    // position on the token ring starting from token corresponding to
    // "start"
    //
    size_t _ring_pos = 0;
    bool _insert_min;
    bool _min = false;
    const token _min_token = dht::minimum_token();
    const token_metadata_impl* _token_metadata = nullptr;

    friend class token_metadata_impl;
};

inline
token_metadata_impl::tokens_iterator
token_metadata_impl::tokens_end() const {
    return tokens_iterator(sorted_tokens().end(), sorted_tokens().size());
}

inline
boost::iterator_range<token_metadata_impl::tokens_iterator>
token_metadata_impl::ring_range(const token& start, bool include_min) const {
    auto begin = tokens_iterator(start, this, include_min);
    auto end = tokens_end();
    return boost::make_iterator_range(begin, end);
}

token_metadata_impl::token_metadata_impl(std::unordered_map<token, inet_address> token_to_endpoint_map, std::unordered_map<inet_address, utils::UUID> endpoints_map, topology topology) :
    _token_to_endpoint_map(token_to_endpoint_map), _endpoint_to_host_id_map(endpoints_map), _topology(topology) {
    sort_tokens();
}

future<token_metadata_impl> token_metadata_impl::clone_async() const noexcept {
    return clone_only_token_map().then([this] (token_metadata_impl ret) {
      return do_with(std::move(ret), [this] (token_metadata_impl& ret) {
        ret._bootstrap_tokens.reserve(_bootstrap_tokens.size());
        return do_for_each(_bootstrap_tokens, [&ret] (const auto& p) {
            ret._bootstrap_tokens.emplace(p);
        }).then([this, &ret] {
            ret._leaving_endpoints = _leaving_endpoints;
            ret._replacing_endpoints = _replacing_endpoints;
        }).then([this, &ret] {
            return do_for_each(_pending_ranges_interval_map,
                    [this, &ret] (const auto& p) {
                ret._pending_ranges_interval_map.emplace(p);
            });
        }).then([this, &ret] {
            ret._ring_version = _ring_version;
            return make_ready_future<token_metadata_impl>(std::move(ret));
        });
      });
    });
}

future<token_metadata_impl> token_metadata_impl::clone_only_token_map(bool clone_sorted_tokens) const noexcept {
    return do_with(token_metadata_impl(), [this, clone_sorted_tokens] (token_metadata_impl& ret) {
        ret._token_to_endpoint_map.reserve(_token_to_endpoint_map.size());
        return do_for_each(_token_to_endpoint_map, [&ret] (const auto& p) {
            ret._token_to_endpoint_map.emplace(p);
        }).then([this, &ret] {
            ret._endpoint_to_host_id_map = _endpoint_to_host_id_map;
        }).then([this, &ret] {
            ret._topology = _topology;
        }).then([this, &ret, clone_sorted_tokens] {
            if (clone_sorted_tokens) {
                ret._sorted_tokens = _sorted_tokens;
            }
            return make_ready_future<token_metadata_impl>(std::move(ret));
        });
    });
}

template <typename Container>
static future<> clear_container_gently(Container& c) noexcept;

// The vector elements we use here (token / inet_address) have trivial destructors
// so they can be safely cleared in bulk
template <typename T, typename Container = std::vector<T>>
static future<> clear_container_gently(Container& vect) noexcept {
    vect.clear();
    return make_ready_future<>();
}

template <typename Container>
static future<> clear_container_gently(Container& c) noexcept {
    for (auto b = c.begin(); b != c.end(); b = c.erase(b)) {
        co_await make_ready_future<>(); // maybe yield
    }
}

template <typename Container>
static future<> clear_nested_container_gently(Container& c) noexcept {
    for (auto b = c.begin(); b != c.end(); b = c.erase(b)) {
        co_await clear_container_gently(b->second);
    }
}

future<> token_metadata_impl::clear_gently() noexcept {
    co_await clear_container_gently(_token_to_endpoint_map);
    co_await clear_container_gently(_endpoint_to_host_id_map);
    co_await clear_container_gently(_bootstrap_tokens);
    co_await clear_container_gently(_leaving_endpoints);
    co_await clear_container_gently(_replacing_endpoints);
    co_await clear_container_gently(_pending_ranges_interval_map);
    co_await clear_container_gently(_sorted_tokens);
    co_await _topology.clear_gently();
    co_return;
}

void token_metadata_impl::sort_tokens() {
    std::vector<token> sorted;
    sorted.reserve(_token_to_endpoint_map.size());

    for (auto&& i : _token_to_endpoint_map) {
        sorted.push_back(i.first);
    }

    std::sort(sorted.begin(), sorted.end());

    _sorted_tokens = std::move(sorted);
}

const std::vector<token>& token_metadata_impl::sorted_tokens() const {
    return _sorted_tokens;
}

std::vector<token> token_metadata_impl::get_tokens(const inet_address& addr) const {
    std::vector<token> res;
    for (auto&& i : _token_to_endpoint_map) {
        if (i.second == addr) {
            res.push_back(i.first);
        }
    }
    std::sort(res.begin(), res.end());
    return res;
}
/**
 * Update token map with a single token/endpoint pair in normal state.
 */
future<> token_metadata_impl::update_normal_token(token t, inet_address endpoint)
{
    return update_normal_tokens(std::unordered_set<token>({t}), endpoint);
}

future<> token_metadata_impl::update_normal_tokens(std::unordered_set<token> tokens, inet_address endpoint) {
    if (tokens.empty()) {
        co_return;
    }
    std::unordered_map<inet_address, std::unordered_set<token>> endpoint_tokens ({{endpoint, std::move(tokens)}});
    co_return co_await update_normal_tokens(endpoint_tokens);
}

void token_metadata_impl::update_normal_tokens_sync(std::unordered_set<token> tokens, inet_address endpoint) {
    if (tokens.empty()) {
        return;
    }
    std::unordered_map<inet_address, std::unordered_set<token>> endpoint_tokens ({{endpoint, std::move(tokens)}});
    update_normal_tokens_sync(endpoint_tokens);
}

// Note: The sync version of this function `update_normal_tokens_sync`
// must be kept in sync with this function if any change is made.
future<> token_metadata_impl::update_normal_tokens(const std::unordered_map<inet_address, std::unordered_set<token>>& endpoint_tokens) {
    if (endpoint_tokens.empty()) {
        co_return;
    }

    bool should_sort_tokens = false;
    for (auto&& i : endpoint_tokens) {
        inet_address endpoint = i.first;
        const auto& tokens = i.second;

        if (tokens.empty()) {
            auto msg = format("tokens is empty in update_normal_tokens");
            tlogger.error("{}", msg);
            throw std::runtime_error(msg);
        }

        for(auto it = _token_to_endpoint_map.begin(), ite = _token_to_endpoint_map.end(); it != ite;) {
            co_await make_ready_future<>(); // maybe yield
            if(it->second == endpoint) {
                it = _token_to_endpoint_map.erase(it);
            } else {
                ++it;
            }
        }

        _topology.add_endpoint(endpoint);
        remove_by_value(_bootstrap_tokens, endpoint);
        _leaving_endpoints.erase(endpoint);
        invalidate_cached_rings();
        for (const token& t : tokens)
        {
            co_await make_ready_future<>(); // maybe yield
            auto prev = _token_to_endpoint_map.insert(std::pair<token, inet_address>(t, endpoint));
            should_sort_tokens |= prev.second; // new token inserted -> sort
            if (prev.first->second != endpoint) {
                tlogger.debug("Token {} changing ownership from {} to {}", t, prev.first->second, endpoint);
                prev.first->second = endpoint;
            }
        }
    }

    if (should_sort_tokens) {
        sort_tokens();
    }
    co_return;
}

/**
 * Update token map with a set of token/endpoint pairs in normal state.
 *
 * Prefer this whenever there are multiple pairs to update, as each update (whether a single or multiple)
 * is expensive (CASSANDRA-3831).
 *
 * @param endpointTokens
 *
 * Note: The futurized version of this function `update_normal_tokens`
 * must be kept in sync with this function if any change is made.
 *
 * This version is meant to be deprecated when the whole interface
 * will be futurized.
 */
void token_metadata_impl::update_normal_tokens_sync(const std::unordered_map<inet_address, std::unordered_set<token>>& endpoint_tokens) {
    if (endpoint_tokens.empty()) {
        return;
    }

    bool should_sort_tokens = false;
    for (auto&& i : endpoint_tokens) {
        inet_address endpoint = i.first;
        const auto& tokens = i.second;

        if (tokens.empty()) {
            auto msg = format("tokens is empty in update_normal_tokens_sync");
            tlogger.error("{}", msg);
            throw std::runtime_error(msg);
        }

        for(auto it = _token_to_endpoint_map.begin(), ite = _token_to_endpoint_map.end(); it != ite;) {
            if(it->second == endpoint) {
                it = _token_to_endpoint_map.erase(it);
            } else {
                ++it;
            }
        }

        _topology.add_endpoint(endpoint);
        remove_by_value(_bootstrap_tokens, endpoint);
        _leaving_endpoints.erase(endpoint);
        invalidate_cached_rings();
        for (const token& t : tokens)
        {
            auto prev = _token_to_endpoint_map.insert(std::pair<token, inet_address>(t, endpoint));
            should_sort_tokens |= prev.second; // new token inserted -> sort
            if (prev.first->second != endpoint) {
                tlogger.debug("Token {} changing ownership from {} to {}", t, prev.first->second, endpoint);
                prev.first->second = endpoint;
            }
        }
    }

    if (should_sort_tokens) {
        sort_tokens();
    }
}

size_t token_metadata_impl::first_token_index(const token& start) const {
    if (_sorted_tokens.empty()) {
        auto msg = format("sorted_tokens is empty in first_token_index!");
        tlogger.error("{}", msg);
        throw std::runtime_error(msg);
    }
    auto it = std::lower_bound(_sorted_tokens.begin(), _sorted_tokens.end(), start);
    if (it == _sorted_tokens.end()) {
        return 0;
    } else {
        return std::distance(_sorted_tokens.begin(), it);
    }
}

const token& token_metadata_impl::first_token(const token& start) const {
    return _sorted_tokens[first_token_index(start)];
}

std::optional<inet_address> token_metadata_impl::get_endpoint(const token& token) const {
    auto it = _token_to_endpoint_map.find(token);
    if (it == _token_to_endpoint_map.end()) {
        return std::nullopt;
    } else {
        return it->second;
    }
}

void token_metadata_impl::debug_show() const {
    auto reporter = std::make_shared<timer<lowres_clock>>();
    reporter->set_callback ([reporter, this] {
        fmt::print("Endpoint -> Token\n");
        for (auto x : _token_to_endpoint_map) {
            fmt::print("inet_address={}, token={}\n", x.second, x.first);
        }
        fmt::print("Endpoint -> UUID\n");
        for (auto x : _endpoint_to_host_id_map) {
            fmt::print("inet_address={}, uuid={}\n", x.first, x.second);
        }
        fmt::print("Sorted Token\n");
        for (auto x : _sorted_tokens) {
            fmt::print("token={}\n", x);
        }
    });
    reporter->arm_periodic(std::chrono::seconds(1));
}

void token_metadata_impl::update_host_id(const UUID& host_id, inet_address endpoint) {
#if 0
    assert host_id != null;
    assert endpoint != null;

    InetAddress storedEp = _endpoint_to_host_id_map.inverse().get(host_id);
    if (storedEp != null) {
        if (!storedEp.equals(endpoint) && (FailureDetector.instance.isAlive(storedEp))) {
            throw new RuntimeException(String.format("Host ID collision between active endpoint %s and %s (id=%s)",
                                                     storedEp,
                                                     endpoint,
                                                     host_id));
        }
    }

    UUID storedId = _endpoint_to_host_id_map.get(endpoint);
    // if ((storedId != null) && (!storedId.equals(host_id)))
        tlogger.warn("Changing {}'s host ID from {} to {}", endpoint, storedId, host_id);
#endif
    _endpoint_to_host_id_map[endpoint] = host_id;
}

utils::UUID token_metadata_impl::get_host_id(inet_address endpoint) const {
    if (!_endpoint_to_host_id_map.contains(endpoint)) {
        throw std::runtime_error(format("host_id for endpoint {} is not found", endpoint));
    }
    return _endpoint_to_host_id_map.at(endpoint);
}

std::optional<utils::UUID> token_metadata_impl::get_host_id_if_known(inet_address endpoint) const {
    auto it = _endpoint_to_host_id_map.find(endpoint);
    if (it == _endpoint_to_host_id_map.end()) {
        return { };
    }
    return it->second;
}

std::optional<inet_address> token_metadata_impl::get_endpoint_for_host_id(UUID host_id) const {
    auto beg = _endpoint_to_host_id_map.cbegin();
    auto end = _endpoint_to_host_id_map.cend();
    auto it = std::find_if(beg, end, [host_id] (auto x) {
        return x.second == host_id;
    });
    if (it == end) {
        return {};
    } else {
        return (*it).first;
    }
}

const std::unordered_map<inet_address, utils::UUID>& token_metadata_impl::get_endpoint_to_host_id_map_for_reading() const{
    return _endpoint_to_host_id_map;
}

bool token_metadata_impl::is_member(inet_address endpoint) const {
    return _topology.has_endpoint(endpoint);
}

void token_metadata_impl::add_bootstrap_token(token t, inet_address endpoint) {
    std::unordered_set<token> tokens{t};
    add_bootstrap_tokens(tokens, endpoint);
}

boost::iterator_range<token_metadata_impl::tokens_iterator>
token_metadata_impl::ring_range(
    const std::optional<dht::partition_range::bound>& start,
    bool include_min) const
{
    auto r = ring_range(start ? start->value().token() : dht::minimum_token(), include_min);

    if (!r.empty()) {
        // We should skip the first token if it's excluded by the range.
        if (start
            && !start->is_inclusive()
            && !start->value().has_key()
            && start->value().token() == *r.begin())
        {
            r.pop_front();
        }
    }

    return r;
}

void token_metadata_impl::add_bootstrap_tokens(std::unordered_set<token> tokens, inet_address endpoint) {
    for (auto t : tokens) {
        auto old_endpoint = _bootstrap_tokens.find(t);
        if (old_endpoint != _bootstrap_tokens.end() && (*old_endpoint).second != endpoint) {
            auto msg = format("Bootstrap Token collision between {} and {} (token {}", (*old_endpoint).second, endpoint, t);
            throw std::runtime_error(msg);
        }

        auto old_endpoint2 = _token_to_endpoint_map.find(t);
        if (old_endpoint2 != _token_to_endpoint_map.end() && (*old_endpoint2).second != endpoint) {
            auto msg = format("Bootstrap Token collision between {} and {} (token {}", (*old_endpoint2).second, endpoint, t);
            throw std::runtime_error(msg);
        }
    }

    // Unfortunately, std::remove_if does not work with std::map
    for (auto it = _bootstrap_tokens.begin(); it != _bootstrap_tokens.end();) {
        if ((*it).second == endpoint) {
            it = _bootstrap_tokens.erase(it);
        } else {
            it++;
        }
    }

    for (auto t : tokens) {
        _bootstrap_tokens[t] = endpoint;
    }
}

void token_metadata_impl::remove_bootstrap_tokens(std::unordered_set<token> tokens) {
    if (tokens.empty()) {
        tlogger.warn("tokens is empty in remove_bootstrap_tokens!");
        return;
    }
    for (auto t : tokens) {
        _bootstrap_tokens.erase(t);
    }
}

bool token_metadata_impl::is_leaving(inet_address endpoint) const {
    return _leaving_endpoints.contains(endpoint);
}

bool token_metadata_impl::is_being_replaced(inet_address endpoint) const {
    return _replacing_endpoints.contains(endpoint);
}

bool token_metadata_impl::is_any_node_being_replaced() const {
    return !_replacing_endpoints.empty();
}

void token_metadata_impl::remove_endpoint(inet_address endpoint) {
    remove_by_value(_bootstrap_tokens, endpoint);
    remove_by_value(_token_to_endpoint_map, endpoint);
    _topology.remove_endpoint(endpoint);
    _leaving_endpoints.erase(endpoint);
    del_replacing_endpoint(endpoint);
    _endpoint_to_host_id_map.erase(endpoint);
    invalidate_cached_rings();
}

token token_metadata_impl::get_predecessor(token t) const {
    auto& tokens = sorted_tokens();
    auto it = std::lower_bound(tokens.begin(), tokens.end(), t);
    if (it == tokens.end() || *it != t) {
        auto msg = format("token error in get_predecessor!");
        tlogger.error("{}", msg);
        throw std::runtime_error(msg);
    }
    if (it == tokens.begin()) {
        // If the token is the first element, its preprocessor is the last element
        return tokens.back();
    } else {
        return *(--it);
    }
}

dht::token_range_vector token_metadata_impl::get_primary_ranges_for(std::unordered_set<token> tokens) const {
    dht::token_range_vector ranges;
    ranges.reserve(tokens.size() + 1); // one of the ranges will wrap
    for (auto right : tokens) {
        auto left = get_predecessor(right);
        ::compat::unwrap_into(
                wrapping_range<token>(range_bound<token>(left, false), range_bound<token>(right)),
                dht::token_comparator(),
                [&] (auto&& rng) { ranges.push_back(std::move(rng)); });
    }
    return ranges;
}

dht::token_range_vector token_metadata_impl::get_primary_ranges_for(token right) const {
    return get_primary_ranges_for(std::unordered_set<token>{right});
}

boost::icl::interval<token>::interval_type
token_metadata_impl::range_to_interval(range<dht::token> r) {
    bool start_inclusive = false;
    bool end_inclusive = false;
    token start = dht::minimum_token();
    token end = dht::maximum_token();

    if (r.start()) {
        start = r.start()->value();
        start_inclusive = r.start()->is_inclusive();
    }

    if (r.end()) {
        end = r.end()->value();
        end_inclusive = r.end()->is_inclusive();
    }

    if (start_inclusive == false && end_inclusive == false) {
        return boost::icl::interval<token>::open(std::move(start), std::move(end));
    } else if (start_inclusive == false && end_inclusive == true) {
        return boost::icl::interval<token>::left_open(std::move(start), std::move(end));
    } else if (start_inclusive == true && end_inclusive == false) {
        return boost::icl::interval<token>::right_open(std::move(start), std::move(end));
    } else {
        return boost::icl::interval<token>::closed(std::move(start), std::move(end));
    }
}

range<dht::token>
token_metadata_impl::interval_to_range(boost::icl::interval<token>::interval_type i) {
    bool start_inclusive;
    bool end_inclusive;
    auto bounds = i.bounds().bits();
    if (bounds == boost::icl::interval_bounds::static_open) {
        start_inclusive = false;
        end_inclusive = false;
    } else if (bounds == boost::icl::interval_bounds::static_left_open) {
        start_inclusive = false;
        end_inclusive = true;
    } else if (bounds == boost::icl::interval_bounds::static_right_open) {
        start_inclusive = true;
        end_inclusive = false;
    } else if (bounds == boost::icl::interval_bounds::static_closed) {
        start_inclusive = true;
        end_inclusive = true;
    } else {
        throw std::runtime_error("Invalid boost::icl::interval<token> bounds");
    }
    return range<dht::token>({{i.lower(), start_inclusive}}, {{i.upper(), end_inclusive}});
}

void token_metadata_impl::set_pending_ranges(const sstring& keyspace_name,
        std::unordered_multimap<range<token>, inet_address> new_pending_ranges,
        can_yield can_yield) {
    if (new_pending_ranges.empty()) {
        _pending_ranges_interval_map.erase(keyspace_name);
        return;
    }
    std::unordered_map<range<token>, std::unordered_set<inet_address>> map;
    for (const auto& x : new_pending_ranges) {
        if (can_yield) {
            seastar::thread::maybe_yield();
        }
        map[x.first].emplace(x.second);
    }

    // construct a interval map to speed up the search
    boost::icl::interval_map<token, std::unordered_set<inet_address>> interval_map;
    for (auto& m : map) {
        if (can_yield) {
            seastar::thread::maybe_yield();
        }
        interval_map +=
                std::make_pair(range_to_interval(m.first), std::move(m.second));
    }
    _pending_ranges_interval_map[keyspace_name] = std::move(interval_map);
}


bool
token_metadata_impl::has_pending_ranges(sstring keyspace_name, inet_address endpoint) const {
    const auto it = _pending_ranges_interval_map.find(keyspace_name);
    if (it == _pending_ranges_interval_map.end()) {
        return false;
    }
    for (const auto& item : it->second) {
        const auto& nodes = item.second;
        if (nodes.contains(endpoint)) {
            return true;
        }
    }
    return false;
}

// Called from a seastar thread
void token_metadata_impl::calculate_pending_ranges_for_leaving(
        const token_metadata& unpimplified_this,
        const abstract_replication_strategy& strategy,
        std::unordered_multimap<range<token>, inet_address>& new_pending_ranges,
        mutable_token_metadata_ptr all_left_metadata) const {
    std::unordered_multimap<inet_address, dht::token_range> address_ranges = strategy.get_address_ranges(unpimplified_this, can_yield::yes);
    // get all ranges that will be affected by leaving nodes
    std::unordered_set<range<token>> affected_ranges;
    for (auto endpoint : _leaving_endpoints) {
        auto r = address_ranges.equal_range(endpoint);
        for (auto x = r.first; x != r.second; x++) {
            affected_ranges.emplace(x->second);
        }
    }
    // for each of those ranges, find what new nodes will be responsible for the range when
    // all leaving nodes are gone.
    auto metadata = token_metadata(std::make_unique<token_metadata_impl>(clone_only_token_map().get0()));
    auto affected_ranges_size = affected_ranges.size();
    tlogger.debug("In calculate_pending_ranges: affected_ranges.size={} stars", affected_ranges_size);
    for (const auto& r : affected_ranges) {
        auto t = r.end() ? r.end()->value() : dht::maximum_token();
        auto current_endpoints = strategy.calculate_natural_endpoints(t, metadata, can_yield::yes);
        auto new_endpoints = strategy.calculate_natural_endpoints(t, *all_left_metadata, can_yield::yes);
        std::vector<inet_address> diff;
        std::sort(current_endpoints.begin(), current_endpoints.end());
        std::sort(new_endpoints.begin(), new_endpoints.end());
        std::set_difference(new_endpoints.begin(), new_endpoints.end(),
            current_endpoints.begin(), current_endpoints.end(), std::back_inserter(diff));
        for (auto& ep : diff) {
            new_pending_ranges.emplace(r, ep);
        }
    }
    metadata.clear_gently().get();
    tlogger.debug("In calculate_pending_ranges: affected_ranges.size={} ends", affected_ranges_size);
}

// Called from a seastar thread
void token_metadata_impl::calculate_pending_ranges_for_replacing(
        const token_metadata& unpimplified_this,
        const abstract_replication_strategy& strategy,
        std::unordered_multimap<range<token>, inet_address>& new_pending_ranges) const {
    if (_replacing_endpoints.empty()) {
        return;
    }
    auto address_ranges = strategy.get_address_ranges(unpimplified_this, can_yield::yes);
    for (const auto& node : _replacing_endpoints) {
        auto existing_node = node.first;
        auto replacing_node = node.second;
        for (const auto& x : address_ranges) {
            seastar::thread::maybe_yield();
            if (x.first == existing_node) {
                tlogger.debug("Node {} replaces {} for range {}", replacing_node, existing_node, x.second);
                new_pending_ranges.emplace(x.second, replacing_node);
            }
        }
    }
}

// Called from a seastar thread
void token_metadata_impl::calculate_pending_ranges_for_bootstrap(
        const abstract_replication_strategy& strategy,
        std::unordered_multimap<range<token>, inet_address>& new_pending_ranges,
        mutable_token_metadata_ptr all_left_metadata) const {
    // For each of the bootstrapping nodes, simply add and remove them one by one to
    // allLeftMetadata and check in between what their ranges would be.
    std::unordered_multimap<inet_address, token> bootstrap_addresses;
    for (auto& x : _bootstrap_tokens) {
        bootstrap_addresses.emplace(x.second, x.first);
    }

    // TODO: share code with unordered_multimap_to_unordered_map
    std::unordered_map<inet_address, std::unordered_set<token>> tmp;
    for (auto& x : bootstrap_addresses) {
        auto& addr = x.first;
        auto& t = x.second;
        tmp[addr].insert(t);
    }
    for (auto& x : tmp) {
        auto& endpoint = x.first;
        auto& tokens = x.second;
        all_left_metadata->update_normal_tokens(tokens, endpoint).get();
        for (auto& x : strategy.get_address_ranges(*all_left_metadata, endpoint, can_yield::yes)) {
            new_pending_ranges.emplace(x.second, endpoint);
        }
        all_left_metadata->_impl->remove_endpoint(endpoint);
    }
    all_left_metadata->_impl->sort_tokens();
}

future<> token_metadata_impl::update_pending_ranges(
        const token_metadata& unpimplified_this,
        const abstract_replication_strategy& strategy, const sstring& keyspace_name) {
    tlogger.debug("calculate_pending_ranges: keyspace_name={}, bootstrap_tokens={}, leaving nodes={}, replacing_endpoints={}",
        keyspace_name, _bootstrap_tokens, _leaving_endpoints, _replacing_endpoints);
    if (_bootstrap_tokens.empty() && _leaving_endpoints.empty() && _replacing_endpoints.empty()) {
        tlogger.debug("No bootstrapping, leaving nodes, replacing nodes -> empty pending ranges for {}", keyspace_name);
        set_pending_ranges(keyspace_name, std::unordered_multimap<range<token>, inet_address>(), can_yield::no);
        return make_ready_future<>();
    }

    return async([this, &unpimplified_this, &strategy, keyspace_name] () mutable {
        std::unordered_multimap<range<token>, inet_address> new_pending_ranges;
        calculate_pending_ranges_for_replacing(unpimplified_this, strategy, new_pending_ranges);
        // Copy of metadata reflecting the situation after all leave operations are finished.
        auto all_left_metadata = make_token_metadata_ptr(std::make_unique<token_metadata_impl>(clone_after_all_left().get0()));
        calculate_pending_ranges_for_leaving(unpimplified_this, strategy, new_pending_ranges, all_left_metadata);
        // At this stage newPendingRanges has been updated according to leave operations. We can
        // now continue the calculation by checking bootstrapping nodes.
        calculate_pending_ranges_for_bootstrap(strategy, new_pending_ranges, all_left_metadata);
        all_left_metadata->clear_gently().get();

        // At this stage newPendingRanges has been updated according to leaving and bootstrapping nodes.
        set_pending_ranges(keyspace_name, std::move(new_pending_ranges), can_yield::yes);
    });

}

size_t token_metadata_impl::count_normal_token_owners() const {
    std::set<inet_address> eps;
    for (auto [t, ep]: _token_to_endpoint_map) {
        eps.insert(ep);
    }
    return eps.size();
}

void token_metadata_impl::add_leaving_endpoint(inet_address endpoint) {
     _leaving_endpoints.emplace(endpoint);
}

void token_metadata_impl::del_leaving_endpoint(inet_address endpoint) {
     _leaving_endpoints.erase(endpoint);
}

void token_metadata_impl::add_replacing_endpoint(inet_address existing_node, inet_address replacing_node) {
    tlogger.info("Added node {} as pending replacing endpoint which replaces existing node {}",
            replacing_node, existing_node);
    _replacing_endpoints[existing_node] = replacing_node;
}

void token_metadata_impl::del_replacing_endpoint(inet_address existing_node) {
    if (_replacing_endpoints.contains(existing_node)) {
        tlogger.info("Removed node {} as pending replacing endpoint which replaces existing node {}",
                _replacing_endpoints[existing_node], existing_node);
    }
    _replacing_endpoints.erase(existing_node);
}

inet_address_vector_topology_change token_metadata_impl::pending_endpoints_for(const token& token, const sstring& keyspace_name) const {
    // Fast path 0: pending ranges not found for this keyspace_name
    const auto pr_it = _pending_ranges_interval_map.find(keyspace_name);
    if (pr_it == _pending_ranges_interval_map.end()) {
        return {};
    }

    // Fast path 1: empty pending ranges for this keyspace_name
    const auto& ks_map = pr_it->second;
    if (ks_map.empty()) {
        return {};
    }

    // Slow path: lookup pending ranges
    inet_address_vector_topology_change endpoints;
    auto interval = range_to_interval(range<dht::token>(token));
    const auto it = ks_map.find(interval);
    if (it != ks_map.end()) {
        // interval_map does not work with std::vector, convert to std::vector of ips
        endpoints = inet_address_vector_topology_change(it->second.begin(), it->second.end());
    }
    return endpoints;
}

std::map<token, inet_address> token_metadata_impl::get_normal_and_bootstrapping_token_to_endpoint_map() const {
    std::map<token, inet_address> ret(_token_to_endpoint_map.begin(), _token_to_endpoint_map.end());
    ret.insert(_bootstrap_tokens.begin(), _bootstrap_tokens.end());
    return ret;
}

std::multimap<inet_address, token> token_metadata_impl::get_endpoint_to_token_map_for_reading() const {
    std::multimap<inet_address, token> cloned;
    for (const auto& x : _token_to_endpoint_map) {
        cloned.emplace(x.second, x.first);
    }
    return cloned;
}

token_metadata::tokens_iterator::tokens_iterator(token_metadata_impl::tokens_iterator i)
        : _impl(std::make_unique<impl_type>(std::move(i))) {
}

token_metadata::tokens_iterator::tokens_iterator(const tokens_iterator& x)
        : _impl(std::make_unique<impl_type>(*x._impl)) {
}

token_metadata::tokens_iterator&
token_metadata::tokens_iterator::operator=(const tokens_iterator& that) {
    *this = tokens_iterator(that);
    return *this;
}

token_metadata::tokens_iterator::~tokens_iterator() = default;

bool
token_metadata::tokens_iterator::operator==(const tokens_iterator& it) const {
    return *_impl == *it._impl;
}

bool
token_metadata::tokens_iterator::operator!=(const tokens_iterator& it) const {
    return *_impl != *it._impl;
}

const token&
token_metadata::tokens_iterator::operator*() {
    return **_impl;
}

token_metadata::tokens_iterator&
token_metadata::tokens_iterator::operator++() {
    ++*_impl;
    return *this;
}

token_metadata::token_metadata(std::unique_ptr<token_metadata_impl> impl)
    : _impl(std::move(impl)) {
}

token_metadata::token_metadata(std::unordered_map<token, inet_address> token_to_endpoint_map, std::unordered_map<inet_address, utils::UUID> endpoints_map, topology topology)
        : _impl(std::make_unique<token_metadata_impl>(std::move(token_to_endpoint_map), std::move(endpoints_map), std::move(topology))) {
}

token_metadata::token_metadata()
        : _impl(std::make_unique<token_metadata_impl>()) {
}

token_metadata::~token_metadata() = default;


token_metadata::token_metadata(const token_metadata& tm)
    : _impl(std::make_unique<token_metadata_impl>(*tm._impl)) {
}

token_metadata::token_metadata(token_metadata&&) noexcept = default;

token_metadata&
token_metadata::operator=(const token_metadata& that) {
    *this = token_metadata(that);
    return *this;
}

token_metadata& token_metadata::token_metadata::operator=(token_metadata&&) noexcept = default;

const std::vector<token>&
token_metadata::sorted_tokens() const {
    return _impl->sorted_tokens();
}

future<>
token_metadata::update_normal_token(token token, inet_address endpoint) {
    return _impl->update_normal_token(token, endpoint);
}

future<>
token_metadata::update_normal_tokens(std::unordered_set<token> tokens, inet_address endpoint) {
    return _impl->update_normal_tokens(std::move(tokens), endpoint);
}

future<>
token_metadata::update_normal_tokens(const std::unordered_map<inet_address, std::unordered_set<token>>& endpoint_tokens) {
    return _impl->update_normal_tokens(endpoint_tokens);
}

void
token_metadata::update_normal_tokens_sync(std::unordered_set<token> tokens, inet_address endpoint) {
    _impl->update_normal_tokens_sync(std::move(tokens), endpoint);
}

void
token_metadata::update_normal_tokens_sync(const std::unordered_map<inet_address, std::unordered_set<token>>& endpoint_tokens) {
    _impl->update_normal_tokens_sync(endpoint_tokens);
}

const token&
token_metadata::first_token(const token& start) const {
    return _impl->first_token(start);
}

size_t
token_metadata::first_token_index(const token& start) const {
    return _impl->first_token_index(start);
}

std::optional<inet_address>
token_metadata::get_endpoint(const token& token) const {
    return _impl->get_endpoint(token);
}

std::vector<token>
token_metadata::get_tokens(const inet_address& addr) const {
    return _impl->get_tokens(addr);
}

const std::unordered_map<token, inet_address>&
token_metadata::get_token_to_endpoint() const {
    return _impl->get_token_to_endpoint();
}

const std::unordered_set<inet_address>&
token_metadata::get_leaving_endpoints() const {
    return _impl->get_leaving_endpoints();
}

const std::unordered_map<token, inet_address>&
token_metadata::get_bootstrap_tokens() const {
    return _impl->get_bootstrap_tokens();
}

void
token_metadata::update_topology(inet_address ep) {
    _impl->update_topology(ep);
}

token_metadata::tokens_iterator
token_metadata::tokens_end() const {
    return tokens_iterator(_impl->tokens_end());
}

boost::iterator_range<token_metadata::tokens_iterator>
token_metadata::ring_range(const token& start, bool include_min) const {
    auto impl_range = _impl->ring_range(start, include_min);
    return boost::make_iterator_range(
            tokens_iterator(std::move(impl_range.begin())),
            tokens_iterator(std::move(impl_range.end())));
}

boost::iterator_range<token_metadata::tokens_iterator>
token_metadata::ring_range(
        const std::optional<dht::partition_range::bound>& start, bool include_min) const {
    auto impl_range = _impl->ring_range(start, include_min);
    return boost::make_iterator_range(
            tokens_iterator(std::move(impl_range.begin())),
            tokens_iterator(std::move(impl_range.end())));
}

topology&
token_metadata::get_topology() {
    return _impl->get_topology();
}

const topology&
token_metadata::get_topology() const {
    return _impl->get_topology();
}

void
token_metadata::debug_show() const {
    _impl->debug_show();
}

void
token_metadata::update_host_id(const UUID& host_id, inet_address endpoint) {
    _impl->update_host_id(host_id, endpoint);
}

token_metadata::UUID
token_metadata::get_host_id(inet_address endpoint) const {
    return _impl->get_host_id(endpoint);
}

std::optional<token_metadata::UUID>
token_metadata::get_host_id_if_known(inet_address endpoint) const {
    return _impl->get_host_id_if_known(endpoint);
}

std::optional<token_metadata::inet_address>
token_metadata::get_endpoint_for_host_id(UUID host_id) const {
    return _impl->get_endpoint_for_host_id(host_id);
}

const std::unordered_map<inet_address, utils::UUID>&
token_metadata::get_endpoint_to_host_id_map_for_reading() const {
    return _impl->get_endpoint_to_host_id_map_for_reading();
}

void
token_metadata::add_bootstrap_token(token t, inet_address endpoint) {
    _impl->add_bootstrap_token(t, endpoint);
}

void
token_metadata::add_bootstrap_tokens(std::unordered_set<token> tokens, inet_address endpoint) {
    _impl->add_bootstrap_tokens(std::move(tokens), endpoint);
}

void
token_metadata::remove_bootstrap_tokens(std::unordered_set<token> tokens) {
    _impl->remove_bootstrap_tokens(std::move(tokens));
}

void
token_metadata::add_leaving_endpoint(inet_address endpoint) {
    _impl->add_leaving_endpoint(endpoint);
}

void
token_metadata::del_leaving_endpoint(inet_address endpoint) {
    _impl->del_leaving_endpoint(endpoint);
}

void
token_metadata::remove_endpoint(inet_address endpoint) {
    _impl->remove_endpoint(endpoint);
    _impl->sort_tokens();
}

bool
token_metadata::is_member(inet_address endpoint) const {
    return _impl->is_member(endpoint);
}

bool
token_metadata::is_leaving(inet_address endpoint) const {
    return _impl->is_leaving(endpoint);
}

bool
token_metadata::is_being_replaced(inet_address endpoint) const {
    return _impl->is_being_replaced(endpoint);
}

bool
token_metadata::is_any_node_being_replaced() const {
    return _impl->is_any_node_being_replaced();
}

void token_metadata::add_replacing_endpoint(inet_address existing_node, inet_address replacing_node) {
    _impl->add_replacing_endpoint(existing_node, replacing_node);
}

void token_metadata::del_replacing_endpoint(inet_address existing_node) {
    _impl->del_replacing_endpoint(existing_node);
}

future<token_metadata> token_metadata::clone_async() const noexcept {
    return _impl->clone_async().then([] (token_metadata_impl impl) {
        return make_ready_future<token_metadata>(std::make_unique<token_metadata_impl>(std::move(impl)));
    });
}

token_metadata
token_metadata::clone_only_token_map_sync() const {
    return token_metadata(std::make_unique<token_metadata_impl>(_impl->clone_only_token_map_sync()));
}

future<token_metadata>
token_metadata::clone_only_token_map() const noexcept {
    return _impl->clone_only_token_map().then([] (token_metadata_impl impl) {
        return token_metadata(std::make_unique<token_metadata_impl>(std::move(impl)));
    });
}

future<token_metadata>
token_metadata::clone_after_all_left() const noexcept {
    return _impl->clone_after_all_left().then([] (token_metadata_impl impl) {
        return token_metadata(std::make_unique<token_metadata_impl>(std::move(impl)));
    });
}

future<> token_metadata::clear_gently() noexcept {
    return _impl->clear_gently();
}

dht::token_range_vector
token_metadata::get_primary_ranges_for(std::unordered_set<token> tokens) const {
    return _impl->get_primary_ranges_for(std::move(tokens));
}

dht::token_range_vector
token_metadata::get_primary_ranges_for(token right) const {
    return _impl->get_primary_ranges_for(right);
}

boost::icl::interval<token>::interval_type
token_metadata::range_to_interval(range<dht::token> r) {
    return token_metadata_impl::range_to_interval(std::move(r));
}

range<dht::token>
token_metadata::interval_to_range(boost::icl::interval<token>::interval_type i) {
    return token_metadata_impl::interval_to_range(std::move(i));
}

bool
token_metadata::has_pending_ranges(sstring keyspace_name, inet_address endpoint) const {
    return _impl->has_pending_ranges(std::move(keyspace_name), endpoint);
}

future<>
token_metadata::update_pending_ranges(const abstract_replication_strategy& strategy, const sstring& keyspace_name) {
    return _impl->update_pending_ranges(*this, strategy, keyspace_name);
}

token
token_metadata::get_predecessor(token t) const {
    return _impl->get_predecessor(t);
}

std::vector<inet_address>
token_metadata::get_all_endpoints() const {
    return _impl->get_all_endpoints();
}

size_t
token_metadata::count_normal_token_owners() const {
    return _impl->count_normal_token_owners();
}

inet_address_vector_topology_change
token_metadata::pending_endpoints_for(const token& token, const sstring& keyspace_name) const {
    return _impl->pending_endpoints_for(token, keyspace_name);
}

std::multimap<inet_address, token>
token_metadata::get_endpoint_to_token_map_for_reading() const {
    return _impl->get_endpoint_to_token_map_for_reading();
}

std::map<token, inet_address>
token_metadata::get_normal_and_bootstrapping_token_to_endpoint_map() const {
    return _impl->get_normal_and_bootstrapping_token_to_endpoint_map();
}

long
token_metadata::get_ring_version() const {
    return _impl->get_ring_version();
}

void
token_metadata::invalidate_cached_rings() {
    _impl->invalidate_cached_rings();
}

/////////////////// class topology /////////////////////////////////////////////
inline future<> topology::clear_gently() noexcept {
    co_await clear_nested_container_gently(_dc_endpoints);
    co_await clear_nested_container_gently(_dc_racks);
    co_await clear_container_gently(_current_locations);
    co_return;
}

topology::topology(const topology& other) {
    _dc_endpoints = other._dc_endpoints;
    _dc_racks = other._dc_racks;
    _current_locations = other._current_locations;
}

void topology::add_endpoint(const inet_address& ep)
{
    auto& snitch = i_endpoint_snitch::get_local_snitch_ptr();
    sstring dc = snitch->get_datacenter(ep);
    sstring rack = snitch->get_rack(ep);
    auto current = _current_locations.find(ep);

    if (current != _current_locations.end()) {
        if (current->second.dc == dc && current->second.rack == rack) {
            return;
        }
        remove_endpoint(ep);
    }

    _dc_endpoints[dc].insert(ep);
    _dc_racks[dc][rack].insert(ep);
    _current_locations[ep] = {dc, rack};
}

void topology::update_endpoint(inet_address ep) {
    if (!_current_locations.contains(ep) || !locator::i_endpoint_snitch::snitch_instance().local_is_initialized()) {
        return;
    }

    add_endpoint(ep);
}

void topology::remove_endpoint(inet_address ep)
{
    auto cur_dc_rack = _current_locations.find(ep);

    if (cur_dc_rack == _current_locations.end()) {
        return;
    }

    _dc_endpoints[cur_dc_rack->second.dc].erase(ep);

    auto& racks = _dc_racks[cur_dc_rack->second.dc];
    auto& eps = racks[cur_dc_rack->second.rack];
    eps.erase(ep);
    if (eps.empty()) {
        racks.erase(cur_dc_rack->second.rack);
    }

    _current_locations.erase(cur_dc_rack);
}

bool topology::has_endpoint(inet_address ep) const
{
    return _current_locations.contains(ep);
}

const endpoint_dc_rack& topology::get_location(const inet_address& ep) const {
    return _current_locations.at(ep);
}

/////////////////// class topology end /////////////////////////////////////////

future<token_metadata_lock> shared_token_metadata::get_lock() noexcept {
    return get_units(_sem, 1);
}

future<> shared_token_metadata::mutate_token_metadata(seastar::noncopyable_function<future<> (token_metadata&)> func) {
    auto lk = co_await get_lock();
    auto tm = co_await _shared->clone_async();
    co_await func(tm);
    set(make_token_metadata_ptr(std::move(tm)));
}

} // namespace locator
