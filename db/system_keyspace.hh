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
 * Modified by Cloudius Systems
 * Copyright 2015 Cloudius Systems
 */

#pragma once

#include <unordered_map>
#include <utility>
#include "schema.hh"
#include "db/schema_tables.hh"
#include "utils/UUID.hh"
#include "gms/inet_address.hh"
#include "query-result-set.hh"
#include "locator/token_metadata.hh"
#include "db_clock.hh"
#include "db/commitlog/replay_position.hh"

namespace service {

class storage_proxy;

}

namespace cql3 {
    class query_processor;
}

namespace db {
namespace system_keyspace {

static constexpr auto NAME = "system";
static constexpr auto HINTS = "hints";
static constexpr auto BATCHLOG = "batchlog";
static constexpr auto PAXOS = "paxos";
static constexpr auto BUILT_INDEXES = "IndexInfo";
static constexpr auto LOCAL = "local";
static constexpr auto PEERS = "peers";
static constexpr auto PEER_EVENTS = "peer_events";
static constexpr auto RANGE_XFERS = "range_xfers";
static constexpr auto COMPACTIONS_IN_PROGRESS = "compactions_in_progress";
static constexpr auto COMPACTION_HISTORY = "compaction_history";
static constexpr auto SSTABLE_ACTIVITY = "sstable_activity";
static constexpr auto SIZE_ESTIMATES = "size_estimates";


extern schema_ptr hints();
extern schema_ptr batchlog();
extern schema_ptr built_indexes(); // TODO (from Cassandra): make private

future<> setup(distributed<database>& db, distributed<cql3::query_processor>& qp);
future<> update_schema_version(utils::UUID version);
future<> update_tokens(std::unordered_set<dht::token> tokens);
future<> update_tokens(gms::inet_address ep, std::unordered_set<dht::token> tokens);

future<> update_preferred_ip(gms::inet_address ep, gms::inet_address preferred_ip);

template <typename Value>
future<> update_peer_info(gms::inet_address ep, sstring column_name, Value value);

future<> remove_endpoint(gms::inet_address ep);

future<> update_hints_dropped(gms::inet_address ep, utils::UUID time_period, int value);

std::vector<schema_ptr> all_tables();
void make(database& db, bool durable);

future<foreign_ptr<lw_shared_ptr<reconcilable_result>>>
query_mutations(service::storage_proxy& proxy, const sstring& cf_name);

// Returns all data from given system table.
// Intended to be used by code which is not performance critical.
future<lw_shared_ptr<query::result_set>> query(service::storage_proxy& proxy, const sstring& cf_name);

// Returns a slice of given system table.
// Intended to be used by code which is not performance critical.
future<lw_shared_ptr<query::result_set>> query(
    service::storage_proxy& proxy,
    const sstring& cf_name,
    const dht::decorated_key& key,
    query::clustering_range row_ranges = query::clustering_range::make_open_ended_both_sides());

/**
 * Return a map of IP addresses containing a map of dc and rack info
 */
std::unordered_map<gms::inet_address, locator::endpoint_dc_rack>
load_dc_rack_info();

#if 0

    private static volatile Map<UUID, Pair<ReplayPosition, Long>> truncationRecords;

    public enum BootstrapState
    {
        NEEDS_BOOTSTRAP,
        COMPLETED,
        IN_PROGRESS
    }

    private static DecoratedKey decorate(ByteBuffer key)
    {
        return StorageService.getPartitioner().decorateKey(key);
    }

    public static void finishStartup()
    {
        setupVersion();
        LegacySchemaTables.saveSystemKeyspaceSchema();
    }

    private static void setupVersion()
    {
        String req = "INSERT INTO system.%s (key, release_version, cql_version, thrift_version, native_protocol_version, data_center, rack, partitioner) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
        executeOnceInternal(String.format(req, LOCAL),
                            LOCAL,
                            FBUtilities.getReleaseVersionString(),
                            QueryProcessor.CQL_VERSION.toString(),
                            cassandraConstants.VERSION,
                            String.valueOf(Server.CURRENT_VERSION),
                            snitch.getDatacenter(FBUtilities.getBroadcastAddress()),
                            snitch.getRack(FBUtilities.getBroadcastAddress()),
                            DatabaseDescriptor.getPartitioner().getClass().getName());
    }

    /**
     * Write compaction log, except columfamilies under system keyspace.
     *
     * @param cfs cfs to compact
     * @param toCompact sstables to compact
     * @return compaction task id or null if cfs is under system keyspace
     */
    public static UUID startCompaction(ColumnFamilyStore cfs, Iterable<SSTableReader> toCompact)
    {
        if (NAME.equals(cfs.keyspace.getName()))
            return null;

        UUID compactionId = UUIDGen.getTimeUUID();
        Iterable<Integer> generations = Iterables.transform(toCompact, new Function<SSTableReader, Integer>()
        {
            public Integer apply(SSTableReader sstable)
            {
                return sstable.descriptor.generation;
            }
        });
        String req = "INSERT INTO system.%s (id, keyspace_name, columnfamily_name, inputs) VALUES (?, ?, ?, ?)";
        executeInternal(String.format(req, COMPACTIONS_IN_PROGRESS), compactionId, cfs.keyspace.getName(), cfs.name, Sets.newHashSet(generations));
        forceBlockingFlush(COMPACTIONS_IN_PROGRESS);
        return compactionId;
    }

    /**
     * Deletes the entry for this compaction from the set of compactions in progress.  The compaction does not need
     * to complete successfully for this to be called.
     * @param taskId what was returned from {@code startCompaction}
     */
    public static void finishCompaction(UUID taskId)
    {
        assert taskId != null;

        executeInternal(String.format("DELETE FROM system.%s WHERE id = ?", COMPACTIONS_IN_PROGRESS), taskId);
        forceBlockingFlush(COMPACTIONS_IN_PROGRESS);
    }

    /**
     * Returns a Map whose keys are KS.CF pairs and whose values are maps from sstable generation numbers to the
     * task ID of the compaction they were participating in.
     */
    public static Map<Pair<String, String>, Map<Integer, UUID>> getUnfinishedCompactions()
    {
        String req = "SELECT * FROM system.%s";
        UntypedResultSet resultSet = executeInternal(String.format(req, COMPACTIONS_IN_PROGRESS));

        Map<Pair<String, String>, Map<Integer, UUID>> unfinishedCompactions = new HashMap<>();
        for (UntypedResultSet.Row row : resultSet)
        {
            String keyspace = row.getString("keyspace_name");
            String columnfamily = row.getString("columnfamily_name");
            Set<Integer> inputs = row.getSet("inputs", Int32Type.instance);
            UUID taskID = row.getUUID("id");

            Pair<String, String> kscf = Pair.create(keyspace, columnfamily);
            Map<Integer, UUID> generationToTaskID = unfinishedCompactions.get(kscf);
            if (generationToTaskID == null)
                generationToTaskID = new HashMap<>(inputs.size());

            for (Integer generation : inputs)
                generationToTaskID.put(generation, taskID);

            unfinishedCompactions.put(kscf, generationToTaskID);
        }
        return unfinishedCompactions;
    }

    public static void discardCompactionsInProgress()
    {
        ColumnFamilyStore compactionLog = Keyspace.open(NAME).getColumnFamilyStore(COMPACTIONS_IN_PROGRESS);
        compactionLog.truncateBlocking();
    }

    public static void updateCompactionHistory(String ksname,
                                               String cfname,
                                               long compactedAt,
                                               long bytesIn,
                                               long bytesOut,
                                               Map<Integer, Long> rowsMerged)
    {
        // don't write anything when the history table itself is compacted, since that would in turn cause new compactions
        if (ksname.equals("system") && cfname.equals(COMPACTION_HISTORY))
            return;
        String req = "INSERT INTO system.%s (id, keyspace_name, columnfamily_name, compacted_at, bytes_in, bytes_out, rows_merged) VALUES (?, ?, ?, ?, ?, ?, ?)";
        executeInternal(String.format(req, COMPACTION_HISTORY), UUIDGen.getTimeUUID(), ksname, cfname, ByteBufferUtil.bytes(compactedAt), bytesIn, bytesOut, rowsMerged);
    }

    public static TabularData getCompactionHistory() throws OpenDataException
    {
        UntypedResultSet queryResultSet = executeInternal(String.format("SELECT * from system.%s", COMPACTION_HISTORY));
        return CompactionHistoryTabularData.from(queryResultSet);
    }
#endif
    future<> save_truncation_record(cql3::query_processor&, const column_family&, db_clock::time_point truncated_at, const db::replay_position&);
    future<> remove_truncation_record(cql3::query_processor&, utils::UUID);
    future<db::replay_position> get_truncated_position(cql3::query_processor&, utils::UUID);
    future<db_clock::time_point> get_truncated_at(cql3::query_processor&, utils::UUID);

#if 0

    /**
     * Record tokens being used by another node
     */
    public static synchronized void updateTokens(InetAddress ep, Collection<Token> tokens)
    {
        if (ep.equals(FBUtilities.getBroadcastAddress()))
        {
            removeEndpoint(ep);
            return;
        }

        String req = "INSERT INTO system.%s (peer, tokens) VALUES (?, ?)";
        executeInternal(String.format(req, PEERS), ep, tokensAsSet(tokens));
    }

    public static synchronized void updatePreferredIP(InetAddress ep, InetAddress preferred_ip)
    {
        String req = "INSERT INTO system.%s (peer, preferred_ip) VALUES (?, ?)";
        executeInternal(String.format(req, PEERS), ep, preferred_ip);
        forceBlockingFlush(PEERS);
    }

    public static synchronized void updatePeerInfo(InetAddress ep, String columnName, Object value)
    {
        if (ep.equals(FBUtilities.getBroadcastAddress()))
            return;

        String req = "INSERT INTO system.%s (peer, %s) VALUES (?, ?)";
        executeInternal(String.format(req, PEERS, columnName), ep, value);
    }

    public static synchronized void updateHintsDropped(InetAddress ep, UUID timePeriod, int value)
    {
        // with 30 day TTL
        String req = "UPDATE system.%s USING TTL 2592000 SET hints_dropped[ ? ] = ? WHERE peer = ?";
        executeInternal(String.format(req, PEER_EVENTS), timePeriod, value, ep);
    }

    public static synchronized void updateSchemaVersion(UUID version)
    {
        String req = "INSERT INTO system.%s (key, schema_version) VALUES ('%s', ?)";
        executeInternal(String.format(req, LOCAL, LOCAL), version);
    }

    private static Set<String> tokensAsSet(Collection<Token> tokens)
    {
        Token.TokenFactory factory = StorageService.getPartitioner().getTokenFactory();
        Set<String> s = new HashSet<>(tokens.size());
        for (Token tk : tokens)
            s.add(factory.toString(tk));
        return s;
    }

    private static Collection<Token> deserializeTokens(Collection<String> tokensStrings)
    {
        Token.TokenFactory factory = StorageService.getPartitioner().getTokenFactory();
        List<Token> tokens = new ArrayList<>(tokensStrings.size());
        for (String tk : tokensStrings)
            tokens.add(factory.fromString(tk));
        return tokens;
    }

    /**
     * Remove stored tokens being used by another node
     */
    public static synchronized void removeEndpoint(InetAddress ep)
    {
        String req = "DELETE FROM system.%s WHERE peer = ?";
        executeInternal(String.format(req, PEERS), ep);
    }

    /**
     * This method is used to update the System Keyspace with the new tokens for this node
    */
    public static synchronized void updateTokens(Collection<Token> tokens)
    {
        assert !tokens.isEmpty() : "removeEndpoint should be used instead";
        String req = "INSERT INTO system.%s (key, tokens) VALUES ('%s', ?)";
        executeInternal(String.format(req, LOCAL, LOCAL), tokensAsSet(tokens));
        forceBlockingFlush(LOCAL);
    }

    /**
     * Convenience method to update the list of tokens in the local system keyspace.
     *
     * @param addTokens tokens to add
     * @param rmTokens tokens to remove
     * @return the collection of persisted tokens
     */
    public static synchronized Collection<Token> updateLocalTokens(Collection<Token> addTokens, Collection<Token> rmTokens)
    {
        Collection<Token> tokens = getSavedTokens();
        tokens.removeAll(rmTokens);
        tokens.addAll(addTokens);
        updateTokens(tokens);
        return tokens;
    }
#endif

#if o
    /**
     * Return a map of stored tokens to IP addresses
     *
     */
    public static SetMultimap<InetAddress, Token> loadTokens()
    {
        SetMultimap<InetAddress, Token> tokenMap = HashMultimap.create();
        for (UntypedResultSet.Row row : executeInternal("SELECT peer, tokens FROM system." + PEERS))
        {
            InetAddress peer = row.getInetAddress("peer");
            if (row.has("tokens"))
                tokenMap.putAll(peer, deserializeTokens(row.getSet("tokens", UTF8Type.instance)));
        }

        return tokenMap;
    }

    /**
     * Return a map of store host_ids to IP addresses
     *
     */
    public static Map<InetAddress, UUID> loadHostIds()
    {
        Map<InetAddress, UUID> hostIdMap = new HashMap<>();
        for (UntypedResultSet.Row row : executeInternal("SELECT peer, host_id FROM system." + PEERS))
        {
            InetAddress peer = row.getInetAddress("peer");
            if (row.has("host_id"))
            {
                hostIdMap.put(peer, row.getUUID("host_id"));
            }
        }
        return hostIdMap;
    }

    /**
     * Get preferred IP for given endpoint if it is known. Otherwise this returns given endpoint itself.
     *
     * @param ep endpoint address to check
     * @return Preferred IP for given endpoint if present, otherwise returns given ep
     */
    public static InetAddress getPreferredIP(InetAddress ep)
    {
        String req = "SELECT preferred_ip FROM system.%s WHERE peer=?";
        UntypedResultSet result = executeInternal(String.format(req, PEERS), ep);
        if (!result.isEmpty() && result.one().has("preferred_ip"))
            return result.one().getInetAddress("preferred_ip");
        return ep;
    }

    /**
     * Return a map of IP addresses containing a map of dc and rack info
     */
    public static Map<InetAddress, Map<String,String>> loadDcRackInfo()
    {
        Map<InetAddress, Map<String, String>> result = new HashMap<>();
        for (UntypedResultSet.Row row : executeInternal("SELECT peer, data_center, rack from system." + PEERS))
        {
            InetAddress peer = row.getInetAddress("peer");
            if (row.has("data_center") && row.has("rack"))
            {
                Map<String, String> dcRack = new HashMap<>();
                dcRack.put("data_center", row.getString("data_center"));
                dcRack.put("rack", row.getString("rack"));
                result.put(peer, dcRack);
            }
        }
        return result;
    }

    /**
     * One of three things will happen if you try to read the system keyspace:
     * 1. files are present and you can read them: great
     * 2. no files are there: great (new node is assumed)
     * 3. files are present but you can't read them: bad
     * @throws ConfigurationException
     */
    public static void checkHealth() throws ConfigurationException
    {
        Keyspace keyspace;
        try
        {
            keyspace = Keyspace.open(NAME);
        }
        catch (AssertionError err)
        {
            // this happens when a user switches from OPP to RP.
            ConfigurationException ex = new ConfigurationException("Could not read system keyspace!");
            ex.initCause(err);
            throw ex;
        }
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(LOCAL);

        String req = "SELECT cluster_name FROM system.%s WHERE key='%s'";
        UntypedResultSet result = executeInternal(String.format(req, LOCAL, LOCAL));

        if (result.isEmpty() || !result.one().has("cluster_name"))
        {
            // this is a brand new node
            if (!cfs.getSSTables().isEmpty())
                throw new ConfigurationException("Found system keyspace files, but they couldn't be loaded!");

            // no system files.  this is a new node.
            req = "INSERT INTO system.%s (key, cluster_name) VALUES ('%s', ?)";
            executeInternal(String.format(req, LOCAL, LOCAL), DatabaseDescriptor.getClusterName());
            return;
        }

        String savedClusterName = result.one().getString("cluster_name");
        if (!DatabaseDescriptor.getClusterName().equals(savedClusterName))
            throw new ConfigurationException("Saved cluster name " + savedClusterName + " != configured name " + DatabaseDescriptor.getClusterName());
    }

#endif
    std::unordered_set<dht::token> get_saved_tokens();
#if 0

    public static int incrementAndGetGeneration()
    {
        String req = "SELECT gossip_generation FROM system.%s WHERE key='%s'";
        UntypedResultSet result = executeInternal(String.format(req, LOCAL, LOCAL));

        int generation;
        if (result.isEmpty() || !result.one().has("gossip_generation"))
        {
            // seconds-since-epoch isn't a foolproof new generation
            // (where foolproof is "guaranteed to be larger than the last one seen at this ip address"),
            // but it's as close as sanely possible
            generation = (int) (System.currentTimeMillis() / 1000);
        }
        else
        {
            // Other nodes will ignore gossip messages about a node that have a lower generation than previously seen.
            final int storedGeneration = result.one().getInt("gossip_generation") + 1;
            final int now = (int) (System.currentTimeMillis() / 1000);
            if (storedGeneration >= now)
            {
                logger.warn("Using stored Gossip Generation {} as it is greater than current system time {}.  See CASSANDRA-3654 if you experience problems",
                            storedGeneration, now);
                generation = storedGeneration;
            }
            else
            {
                generation = now;
            }
        }

        req = "INSERT INTO system.%s (key, gossip_generation) VALUES ('%s', ?)";
        executeInternal(String.format(req, LOCAL, LOCAL), generation);
        forceBlockingFlush(LOCAL);

        return generation;
    }

    public static BootstrapState getBootstrapState()
    {
        String req = "SELECT bootstrapped FROM system.%s WHERE key='%s'";
        UntypedResultSet result = executeInternal(String.format(req, LOCAL, LOCAL));

        if (result.isEmpty() || !result.one().has("bootstrapped"))
            return BootstrapState.NEEDS_BOOTSTRAP;

        return BootstrapState.valueOf(result.one().getString("bootstrapped"));
    }

    public static boolean bootstrapComplete()
    {
        return getBootstrapState() == BootstrapState.COMPLETED;
    }

    public static boolean bootstrapInProgress()
    {
        return getBootstrapState() == BootstrapState.IN_PROGRESS;
    }

    public static void setBootstrapState(BootstrapState state)
    {
        String req = "INSERT INTO system.%s (key, bootstrapped) VALUES ('%s', ?)";
        executeInternal(String.format(req, LOCAL, LOCAL), state.name());
        forceBlockingFlush(LOCAL);
    }

    public static boolean isIndexBuilt(String keyspaceName, String indexName)
    {
        ColumnFamilyStore cfs = Keyspace.open(NAME).getColumnFamilyStore(BUILT_INDEXES);
        QueryFilter filter = QueryFilter.getNamesFilter(decorate(ByteBufferUtil.bytes(keyspaceName)),
                                                        BUILT_INDEXES,
                                                        FBUtilities.singleton(cfs.getComparator().makeCellName(indexName), cfs.getComparator()),
                                                        System.currentTimeMillis());
        return ColumnFamilyStore.removeDeleted(cfs.getColumnFamily(filter), Integer.MAX_VALUE) != null;
    }

    public static void setIndexBuilt(String keyspaceName, String indexName)
    {
        ColumnFamily cf = ArrayBackedSortedColumns.factory.create(NAME, BUILT_INDEXES);
        cf.addColumn(new BufferCell(cf.getComparator().makeCellName(indexName), ByteBufferUtil.EMPTY_BYTE_BUFFER, FBUtilities.timestampMicros()));
        new Mutation(NAME, ByteBufferUtil.bytes(keyspaceName), cf).apply();
    }

    public static void setIndexRemoved(String keyspaceName, String indexName)
    {
        Mutation mutation = new Mutation(NAME, ByteBufferUtil.bytes(keyspaceName));
        mutation.delete(BUILT_INDEXES, BuiltIndexes.comparator.makeCellName(indexName), FBUtilities.timestampMicros());
        mutation.apply();
    }
#endif

    /**
     * Read the host ID from the system keyspace, creating (and storing) one if
     * none exists.
     */
    future<utils::UUID> get_local_host_id();

    /**
     * Sets the local host ID explicitly.  Should only be called outside of SystemTable when replacing a node.
     */
    future<utils::UUID> set_local_host_id(const utils::UUID& host_id);

#if 0

    public static PaxosState loadPaxosState(ByteBuffer key, CFMetaData metadata)
    {
        String req = "SELECT * FROM system.%s WHERE row_key = ? AND cf_id = ?";
        UntypedResultSet results = executeInternal(String.format(req, PAXOS), key, metadata.cfId);
        if (results.isEmpty())
            return new PaxosState(key, metadata);
        UntypedResultSet.Row row = results.one();
        Commit promised = row.has("in_progress_ballot")
                        ? new Commit(key, row.getUUID("in_progress_ballot"), ArrayBackedSortedColumns.factory.create(metadata))
                        : Commit.emptyCommit(key, metadata);
        // either we have both a recently accepted ballot and update or we have neither
        Commit accepted = row.has("proposal")
                        ? new Commit(key, row.getUUID("proposal_ballot"), ColumnFamily.fromBytes(row.getBytes("proposal")))
                        : Commit.emptyCommit(key, metadata);
        // either most_recent_commit and most_recent_commit_at will both be set, or neither
        Commit mostRecent = row.has("most_recent_commit")
                          ? new Commit(key, row.getUUID("most_recent_commit_at"), ColumnFamily.fromBytes(row.getBytes("most_recent_commit")))
                          : Commit.emptyCommit(key, metadata);
        return new PaxosState(promised, accepted, mostRecent);
    }

    public static void savePaxosPromise(Commit promise)
    {
        String req = "UPDATE system.%s USING TIMESTAMP ? AND TTL ? SET in_progress_ballot = ? WHERE row_key = ? AND cf_id = ?";
        executeInternal(String.format(req, PAXOS),
                        UUIDGen.microsTimestamp(promise.ballot),
                        paxosTtl(promise.update.metadata),
                        promise.ballot,
                        promise.key,
                        promise.update.id());
    }

    public static void savePaxosProposal(Commit proposal)
    {
        executeInternal(String.format("UPDATE system.%s USING TIMESTAMP ? AND TTL ? SET proposal_ballot = ?, proposal = ? WHERE row_key = ? AND cf_id = ?", PAXOS),
                        UUIDGen.microsTimestamp(proposal.ballot),
                        paxosTtl(proposal.update.metadata),
                        proposal.ballot,
                        proposal.update.toBytes(),
                        proposal.key,
                        proposal.update.id());
    }

    private static int paxosTtl(CFMetaData metadata)
    {
        // keep paxos state around for at least 3h
        return Math.max(3 * 3600, metadata.getGcGraceSeconds());
    }

    public static void savePaxosCommit(Commit commit)
    {
        // We always erase the last proposal (with the commit timestamp to no erase more recent proposal in case the commit is old)
        // even though that's really just an optimization  since SP.beginAndRepairPaxos will exclude accepted proposal older than the mrc.
        String cql = "UPDATE system.%s USING TIMESTAMP ? AND TTL ? SET proposal_ballot = null, proposal = null, most_recent_commit_at = ?, most_recent_commit = ? WHERE row_key = ? AND cf_id = ?";
        executeInternal(String.format(cql, PAXOS),
                        UUIDGen.microsTimestamp(commit.ballot),
                        paxosTtl(commit.update.metadata),
                        commit.ballot,
                        commit.update.toBytes(),
                        commit.key,
                        commit.update.id());
    }

    /**
     * Returns a RestorableMeter tracking the average read rate of a particular SSTable, restoring the last-seen rate
     * from values in system.sstable_activity if present.
     * @param keyspace the keyspace the sstable belongs to
     * @param table the table the sstable belongs to
     * @param generation the generation number for the sstable
     */
    public static RestorableMeter getSSTableReadMeter(String keyspace, String table, int generation)
    {
        String cql = "SELECT * FROM system.%s WHERE keyspace_name=? and columnfamily_name=? and generation=?";
        UntypedResultSet results = executeInternal(String.format(cql, SSTABLE_ACTIVITY), keyspace, table, generation);

        if (results.isEmpty())
            return new RestorableMeter();

        UntypedResultSet.Row row = results.one();
        double m15rate = row.getDouble("rate_15m");
        double m120rate = row.getDouble("rate_120m");
        return new RestorableMeter(m15rate, m120rate);
    }

    /**
     * Writes the current read rates for a given SSTable to system.sstable_activity
     */
    public static void persistSSTableReadMeter(String keyspace, String table, int generation, RestorableMeter meter)
    {
        // Store values with a one-day TTL to handle corner cases where cleanup might not occur
        String cql = "INSERT INTO system.%s (keyspace_name, columnfamily_name, generation, rate_15m, rate_120m) VALUES (?, ?, ?, ?, ?) USING TTL 864000";
        executeInternal(String.format(cql, SSTABLE_ACTIVITY),
                        keyspace,
                        table,
                        generation,
                        meter.fifteenMinuteRate(),
                        meter.twoHourRate());
    }

    /**
     * Clears persisted read rates from system.sstable_activity for SSTables that have been deleted.
     */
    public static void clearSSTableReadMeter(String keyspace, String table, int generation)
    {
        String cql = "DELETE FROM system.%s WHERE keyspace_name=? AND columnfamily_name=? and generation=?";
        executeInternal(String.format(cql, SSTABLE_ACTIVITY), keyspace, table, generation);
    }
#endif
} // namespace system_keyspace
} // namespace db
