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
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#pragma once

#include <map>
#include <unordered_set>
#include <boost/bimap.hpp>
#include "gms/inet_address.hh"
#include "dht/i_partitioner.hh"
#include "utils/UUID.hh"
#include <experimental/optional>

namespace locator {

using inet_address = gms::inet_address;
using token = dht::token;

class topology {

};

class token_metadata final {
    /**
     * Maintains token to endpoint map of every node in the cluster.
     * Each Token is associated with exactly one Address, but each Address may have
     * multiple tokens.  Hence, the BiMultiValMap collection.
     */
    // FIXME: have to be BiMultiValMap
    std::map<token, inet_address> _token_to_endpoint_map;

    /** Maintains endpoint to host ID map of every node in the cluster */
    boost::bimap<inet_address, utils::UUID> _endpoint_to_host_id_map;

    std::vector<token> _sorted_tokens;

    topology _topology;

    std::vector<token> sort_tokens();

    token_metadata(std::map<token, inet_address> token_to_endpoint_map, boost::bimap<inet_address, utils::UUID> endpoints_map, topology topology);
public:
    token_metadata() {};
    const std::vector<token>& sorted_tokens() const;
    void update_normal_token(token token, inet_address endpoint);
    void update_normal_tokens(std::unordered_set<token> tokens, inet_address endpoint);
    void update_normal_tokens(std::unordered_map<inet_address, std::unordered_set<token>>& endpoint_tokens);
    const token& first_token(const token& start);
    size_t first_token_index(const token& start);
    std::experimental::optional<inet_address> get_endpoint(const token& token) const;
    std::vector<token> get_tokens(const inet_address& addr) const;
    const std::map<token, inet_address>& get_token_to_endpoint() const {
        return _token_to_endpoint_map;
    }
#if 0
    private static final Logger logger = LoggerFactory.getLogger(TokenMetadata.class);

    /**
     * Maintains token to endpoint map of every node in the cluster.
     * Each Token is associated with exactly one Address, but each Address may have
     * multiple tokens.  Hence, the BiMultiValMap collection.
     */
    private final BiMultiValMap<Token, InetAddress> tokenToEndpointMap;

    /** Maintains endpoint to host ID map of every node in the cluster */
    private final BiMap<InetAddress, UUID> endpointToHostIdMap;

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
    private final ConcurrentMap<String, Multimap<Range<Token>, InetAddress>> pendingRanges = new ConcurrentHashMap<String, Multimap<Range<Token>, InetAddress>>();

    // nodes which are migrating to the new tokens in the ring
    private final Set<Pair<Token, InetAddress>> movingEndpoints = new HashSet<Pair<Token, InetAddress>>();

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
        endpointToHostIdMap = endpointsMap;
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
            for (Token token : bootstrapTokens.keySet())
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

                bootstrapTokens.removeValue(endpoint);
                tokenToEndpointMap.removeValue(endpoint);
                topology.addEndpoint(endpoint);
                leavingEndpoints.remove(endpoint);
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

    /**
     * Store an end-point to host ID mapping.  Each ID must be unique, and
     * cannot be changed after the fact.
     *
     * @param hostId
     * @param endpoint
     */
    public void updateHostId(UUID hostId, InetAddress endpoint)
    {
        assert hostId != null;
        assert endpoint != null;

        lock.writeLock().lock();
        try
        {
            InetAddress storedEp = endpointToHostIdMap.inverse().get(hostId);
            if (storedEp != null)
            {
                if (!storedEp.equals(endpoint) && (FailureDetector.instance.isAlive(storedEp)))
                {
                    throw new RuntimeException(String.format("Host ID collision between active endpoint %s and %s (id=%s)",
                                                             storedEp,
                                                             endpoint,
                                                             hostId));
                }
            }

            UUID storedId = endpointToHostIdMap.get(endpoint);
            if ((storedId != null) && (!storedId.equals(hostId)))
                logger.warn("Changing {}'s host ID from {} to {}", endpoint, storedId, hostId);
    
            endpointToHostIdMap.forcePut(endpoint, hostId);
        }
        finally
        {
            lock.writeLock().unlock();
        }

    }

    /** Return the unique host ID for an end-point. */
    public UUID getHostId(InetAddress endpoint)
    {
        lock.readLock().lock();
        try
        {
            return endpointToHostIdMap.get(endpoint);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    /** Return the end-point for a unique host ID */
    public InetAddress getEndpointForHostId(UUID hostId)
    {
        lock.readLock().lock();
        try
        {
            return endpointToHostIdMap.inverse().get(hostId);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    /** @return a copy of the endpoint-to-id map for read-only operations */
    public Map<InetAddress, UUID> getEndpointToHostIdMapForReading()
    {
        lock.readLock().lock();
        try
        {
            Map<InetAddress, UUID> readMap = new HashMap<InetAddress, UUID>();
            readMap.putAll(endpointToHostIdMap);
            return readMap;
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    @Deprecated
    public void addBootstrapToken(Token token, InetAddress endpoint)
    {
        addBootstrapTokens(Collections.singleton(token), endpoint);
    }

    public void addBootstrapTokens(Collection<Token> tokens, InetAddress endpoint)
    {
        assert tokens != null && !tokens.isEmpty();
        assert endpoint != null;

        lock.writeLock().lock();
        try
        {

            InetAddress oldEndpoint;

            for (Token token : tokens)
            {
                oldEndpoint = bootstrapTokens.get(token);
                if (oldEndpoint != null && !oldEndpoint.equals(endpoint))
                    throw new RuntimeException("Bootstrap Token collision between " + oldEndpoint + " and " + endpoint + " (token " + token);

                oldEndpoint = tokenToEndpointMap.get(token);
                if (oldEndpoint != null && !oldEndpoint.equals(endpoint))
                    throw new RuntimeException("Bootstrap Token collision between " + oldEndpoint + " and " + endpoint + " (token " + token);
            }

            bootstrapTokens.removeValue(endpoint);

            for (Token token : tokens)
                bootstrapTokens.put(token, endpoint);
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    public void removeBootstrapTokens(Collection<Token> tokens)
    {
        assert tokens != null && !tokens.isEmpty();

        lock.writeLock().lock();
        try
        {
            for (Token token : tokens)
                bootstrapTokens.remove(token);
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    public void addLeavingEndpoint(InetAddress endpoint)
    {
        assert endpoint != null;

        lock.writeLock().lock();
        try
        {
            leavingEndpoints.add(endpoint);
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    /**
     * Add a new moving endpoint
     * @param token token which is node moving to
     * @param endpoint address of the moving node
     */
    public void addMovingEndpoint(Token token, InetAddress endpoint)
    {
        assert endpoint != null;

        lock.writeLock().lock();

        try
        {
            movingEndpoints.add(Pair.create(token, endpoint));
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    public void removeEndpoint(InetAddress endpoint)
    {
        assert endpoint != null;

        lock.writeLock().lock();
        try
        {
            bootstrapTokens.removeValue(endpoint);
            tokenToEndpointMap.removeValue(endpoint);
            topology.removeEndpoint(endpoint);
            leavingEndpoints.remove(endpoint);
            endpointToHostIdMap.remove(endpoint);
            sortedTokens = sortTokens();
            invalidateCachedRings();
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    /**
     * Remove pair of token/address from moving endpoints
     * @param endpoint address of the moving node
     */
    public void removeFromMoving(InetAddress endpoint)
    {
        assert endpoint != null;

        lock.writeLock().lock();
        try
        {
            for (Pair<Token, InetAddress> pair : movingEndpoints)
            {
                if (pair.right.equals(endpoint))
                {
                    movingEndpoints.remove(pair);
                    break;
                }
            }

            invalidateCachedRings();
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

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

    public boolean isMember(InetAddress endpoint)
    {
        assert endpoint != null;

        lock.readLock().lock();
        try
        {
            return tokenToEndpointMap.inverse().containsKey(endpoint);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    public boolean isLeaving(InetAddress endpoint)
    {
        assert endpoint != null;

        lock.readLock().lock();
        try
        {
            return leavingEndpoints.contains(endpoint);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    public boolean isMoving(InetAddress endpoint)
    {
        assert endpoint != null;

        lock.readLock().lock();

        try
        {
            for (Pair<Token, InetAddress> pair : movingEndpoints)
            {
                if (pair.right.equals(endpoint))
                    return true;
            }

            return false;
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    private final AtomicReference<TokenMetadata> cachedTokenMap = new AtomicReference<TokenMetadata>();

    /**
     * Create a copy of TokenMetadata with only tokenToEndpointMap. That is, pending ranges,
     * bootstrap tokens and leaving endpoints are not included in the copy.
     */
    public TokenMetadata cloneOnlyTokenMap()
    {
        lock.readLock().lock();
        try
        {
            return new TokenMetadata(SortedBiMultiValMap.<Token, InetAddress>create(tokenToEndpointMap, null, inetaddressCmp),
                                     HashBiMap.create(endpointToHostIdMap),
                                     new Topology(topology));
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

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

    /**
     * Create a copy of TokenMetadata with tokenToEndpointMap reflecting situation after all
     * current leave operations have finished.
     *
     * @return new token metadata
     */
    public TokenMetadata cloneAfterAllLeft()
    {
        lock.readLock().lock();
        try
        {
            TokenMetadata allLeftMetadata = cloneOnlyTokenMap();

            for (InetAddress endpoint : leavingEndpoints)
                allLeftMetadata.removeEndpoint(endpoint);

            return allLeftMetadata;
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    /**
     * Create a copy of TokenMetadata with tokenToEndpointMap reflecting situation after all
     * current leave, and move operations have finished.
     *
     * @return new token metadata
     */
    public TokenMetadata cloneAfterAllSettled()
    {
        lock.readLock().lock();

        try
        {
            TokenMetadata metadata = cloneOnlyTokenMap();

            for (InetAddress endpoint : leavingEndpoints)
                metadata.removeEndpoint(endpoint);


            for (Pair<Token, InetAddress> pair : movingEndpoints)
                metadata.updateNormalToken(pair.left, pair.right);

            return metadata;
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    public InetAddress getEndpoint(Token token)
    {
        lock.readLock().lock();
        try
        {
            return tokenToEndpointMap.get(token);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    public Collection<Range<Token>> getPrimaryRangesFor(Collection<Token> tokens)
    {
        Collection<Range<Token>> ranges = new ArrayList<Range<Token>>(tokens.size());
        for (Token right : tokens)
            ranges.add(new Range<Token>(getPredecessor(right), right));
        return ranges;
    }

    @Deprecated
    public Range<Token> getPrimaryRangeFor(Token right)
    {
        return getPrimaryRangesFor(Arrays.asList(right)).iterator().next();
    }

    public ArrayList<Token> sortedTokens()
    {
        return sortedTokens;
    }

    private Multimap<Range<Token>, InetAddress> getPendingRangesMM(String keyspaceName)
    {
        Multimap<Range<Token>, InetAddress> map = pendingRanges.get(keyspaceName);
        if (map == null)
        {
            map = HashMultimap.create();
            Multimap<Range<Token>, InetAddress> priorMap = pendingRanges.putIfAbsent(keyspaceName, map);
            if (priorMap != null)
                map = priorMap;
        }
        return map;
    }

    /** a mutable map may be returned but caller should not modify it */
    public Map<Range<Token>, Collection<InetAddress>> getPendingRanges(String keyspaceName)
    {
        return getPendingRangesMM(keyspaceName).asMap();
    }

    public List<Range<Token>> getPendingRanges(String keyspaceName, InetAddress endpoint)
    {
        List<Range<Token>> ranges = new ArrayList<Range<Token>>();
        for (Map.Entry<Range<Token>, InetAddress> entry : getPendingRangesMM(keyspaceName).entries())
        {
            if (entry.getValue().equals(endpoint))
            {
                ranges.add(entry.getKey());
            }
        }
        return ranges;
    }

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
    public void calculatePendingRanges(AbstractReplicationStrategy strategy, String keyspaceName)
    {
        lock.readLock().lock();
        try
        {
            Multimap<Range<Token>, InetAddress> newPendingRanges = HashMultimap.create();

            if (bootstrapTokens.isEmpty() && leavingEndpoints.isEmpty() && movingEndpoints.isEmpty())
            {
                if (logger.isDebugEnabled())
                    logger.debug("No bootstrapping, leaving or moving nodes -> empty pending ranges for {}", keyspaceName);

                pendingRanges.put(keyspaceName, newPendingRanges);
                return;
            }

            Multimap<InetAddress, Range<Token>> addressRanges = strategy.getAddressRanges();

            // Copy of metadata reflecting the situation after all leave operations are finished.
            TokenMetadata allLeftMetadata = cloneAfterAllLeft();

            // get all ranges that will be affected by leaving nodes
            Set<Range<Token>> affectedRanges = new HashSet<Range<Token>>();
            for (InetAddress endpoint : leavingEndpoints)
                affectedRanges.addAll(addressRanges.get(endpoint));

            // for each of those ranges, find what new nodes will be responsible for the range when
            // all leaving nodes are gone.
            TokenMetadata metadata = cloneOnlyTokenMap(); // don't do this in the loop! #7758
            for (Range<Token> range : affectedRanges)
            {
                Set<InetAddress> currentEndpoints = ImmutableSet.copyOf(strategy.calculateNaturalEndpoints(range.right, metadata));
                Set<InetAddress> newEndpoints = ImmutableSet.copyOf(strategy.calculateNaturalEndpoints(range.right, allLeftMetadata));
                newPendingRanges.putAll(range, Sets.difference(newEndpoints, currentEndpoints));
            }

            // At this stage newPendingRanges has been updated according to leave operations. We can
            // now continue the calculation by checking bootstrapping nodes.

            // For each of the bootstrapping nodes, simply add and remove them one by one to
            // allLeftMetadata and check in between what their ranges would be.
            Multimap<InetAddress, Token> bootstrapAddresses = bootstrapTokens.inverse();
            for (InetAddress endpoint : bootstrapAddresses.keySet())
            {
                Collection<Token> tokens = bootstrapAddresses.get(endpoint);

                allLeftMetadata.updateNormalTokens(tokens, endpoint);
                for (Range<Token> range : strategy.getAddressRanges(allLeftMetadata).get(endpoint))
                    newPendingRanges.put(range, endpoint);
                allLeftMetadata.removeEndpoint(endpoint);
            }

            // At this stage newPendingRanges has been updated according to leaving and bootstrapping nodes.
            // We can now finish the calculation by checking moving nodes.

            // For each of the moving nodes, we do the same thing we did for bootstrapping:
            // simply add and remove them one by one to allLeftMetadata and check in between what their ranges would be.
            for (Pair<Token, InetAddress> moving : movingEndpoints)
            {
                InetAddress endpoint = moving.right; // address of the moving node

                //  moving.left is a new token of the endpoint
                allLeftMetadata.updateNormalToken(moving.left, endpoint);

                for (Range<Token> range : strategy.getAddressRanges(allLeftMetadata).get(endpoint))
                {
                    newPendingRanges.put(range, endpoint);
                }

                allLeftMetadata.removeEndpoint(endpoint);
            }

            pendingRanges.put(keyspaceName, newPendingRanges);

            if (logger.isDebugEnabled())
                logger.debug("Pending ranges:\n{}", (pendingRanges.isEmpty() ? "<empty>" : printPendingRanges()));
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    public Token getPredecessor(Token token)
    {
        List tokens = sortedTokens();
        int index = Collections.binarySearch(tokens, token);
        assert index >= 0 : token + " not found in " + StringUtils.join(tokenToEndpointMap.keySet(), ", ");
        return (Token) (index == 0 ? tokens.get(tokens.size() - 1) : tokens.get(index - 1));
    }

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
            return new BiMultiValMap<Token, InetAddress>(bootstrapTokens);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    public Set<InetAddress> getAllEndpoints()
    {
        lock.readLock().lock();
        try
        {
            return ImmutableSet.copyOf(endpointToHostIdMap.keySet());
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    /** caller should not modify leavingEndpoints */
    public Set<InetAddress> getLeavingEndpoints()
    {
        lock.readLock().lock();
        try
        {
            return ImmutableSet.copyOf(leavingEndpoints);
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
            return ImmutableSet.copyOf(movingEndpoints);
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
            endpointToHostIdMap.clear();
            bootstrapTokens.clear();
            leavingEndpoints.clear();
            pendingRanges.clear();
            movingEndpoints.clear();
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

            if (!bootstrapTokens.isEmpty())
            {
                sb.append("Bootstrapping Tokens:" );
                sb.append(System.getProperty("line.separator"));
                for (Map.Entry<Token, InetAddress> entry : bootstrapTokens.entrySet())
                {
                    sb.append(entry.getValue()).append(":").append(entry.getKey());
                    sb.append(System.getProperty("line.separator"));
                }
            }

            if (!leavingEndpoints.isEmpty())
            {
                sb.append("Leaving Endpoints:");
                sb.append(System.getProperty("line.separator"));
                for (InetAddress ep : leavingEndpoints)
                {
                    sb.append(ep);
                    sb.append(System.getProperty("line.separator"));
                }
            }

            if (!pendingRanges.isEmpty())
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

    private String printPendingRanges()
    {
        StringBuilder sb = new StringBuilder();

        for (Map.Entry<String, Multimap<Range<Token>, InetAddress>> entry : pendingRanges.entrySet())
        {
            for (Map.Entry<Range<Token>, InetAddress> rmap : entry.getValue().entries())
            {
                sb.append(rmap.getValue()).append(":").append(rmap.getKey());
                sb.append(System.getProperty("line.separator"));
            }
        }

        return sb.toString();
    }

    public Collection<InetAddress> pendingEndpointsFor(Token token, String keyspaceName)
    {
        Map<Range<Token>, Collection<InetAddress>> ranges = getPendingRanges(keyspaceName);
        if (ranges.isEmpty())
            return Collections.emptyList();

        Set<InetAddress> endpoints = new HashSet<InetAddress>();
        for (Map.Entry<Range<Token>, Collection<InetAddress>> entry : ranges.entrySet())
        {
            if (entry.getKey().contains(token))
                endpoints.addAll(entry.getValue());
        }

        return endpoints;
    }

    /**
     * @deprecated retained for benefit of old tests
     */
    public Collection<InetAddress> getWriteEndpoints(Token token, String keyspaceName, Collection<InetAddress> naturalEndpoints)
    {
        return ImmutableList.copyOf(Iterables.concat(naturalEndpoints, pendingEndpointsFor(token, keyspaceName)));
    }

    /** @return an endpoint to token multimap representation of tokenToEndpointMap (a copy) */
    public Multimap<InetAddress, Token> getEndpointToTokenMapForReading()
    {
        lock.readLock().lock();
        try
        {
            Multimap<InetAddress, Token> cloned = HashMultimap.create();
            for (Map.Entry<Token, InetAddress> entry : tokenToEndpointMap.entrySet())
                cloned.put(entry.getValue(), entry.getKey());
            return cloned;
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    /**
     * @return a (stable copy, won't be modified) Token to Endpoint map for all the normal and bootstrapping nodes
     *         in the cluster.
     */
    public Map<Token, InetAddress> getNormalAndBootstrappingTokenToEndpointMap()
    {
        lock.readLock().lock();
        try
        {
            Map<Token, InetAddress> map = new HashMap<Token, InetAddress>(tokenToEndpointMap.size() + bootstrapTokens.size());
            map.putAll(tokenToEndpointMap);
            map.putAll(bootstrapTokens);
            return map;
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

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
};

}
