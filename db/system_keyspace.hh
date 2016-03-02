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
#include <map>

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

table_schema_version generate_schema_version(utils::UUID table_id);

// Only for testing.
void minimal_setup(distributed<database>& db, distributed<cql3::query_processor>& qp);

future<> init_local_cache();
future<> deinit_local_cache();
future<> setup(distributed<database>& db, distributed<cql3::query_processor>& qp);
future<> update_schema_version(utils::UUID version);
future<> update_tokens(std::unordered_set<dht::token> tokens);
future<> update_tokens(gms::inet_address ep, std::unordered_set<dht::token> tokens);

future<> update_preferred_ip(gms::inet_address ep, gms::inet_address preferred_ip);
future<std::unordered_map<gms::inet_address, gms::inet_address>> get_preferred_ips();

template <typename Value>
future<> update_peer_info(gms::inet_address ep, sstring column_name, Value value);

future<> remove_endpoint(gms::inet_address ep);

future<> update_hints_dropped(gms::inet_address ep, utils::UUID time_period, int value);

std::vector<schema_ptr> all_tables();
void make(database& db, bool durable, bool volatile_testing_only = false);

future<foreign_ptr<lw_shared_ptr<reconcilable_result>>>
query_mutations(distributed<service::storage_proxy>& proxy, const sstring& cf_name);

// Returns all data from given system table.
// Intended to be used by code which is not performance critical.
future<lw_shared_ptr<query::result_set>> query(distributed<service::storage_proxy>& proxy, const sstring& cf_name);

// Returns a slice of given system table.
// Intended to be used by code which is not performance critical.
future<lw_shared_ptr<query::result_set>> query(
    distributed<service::storage_proxy>& proxy,
    const sstring& cf_name,
    const dht::decorated_key& key,
    query::clustering_range row_ranges = query::clustering_range::make_open_ended_both_sides());

/**
 * Return a map of IP addresses containing a map of dc and rack info
 */
std::unordered_map<gms::inet_address, locator::endpoint_dc_rack>
load_dc_rack_info();

#if 0
    public static KSMetaData definition()
    {
        Iterable<CFMetaData> tables =
            Iterables.concat(LegacySchemaTables.All,
                             Arrays.asList(BuiltIndexes,
                                           Hints,
                                           Batchlog,
                                           Paxos,
                                           Local,
                                           Peers,
                                           PeerEvents,
                                           RangeXfers,
                                           CompactionsInProgress,
                                           CompactionHistory,
                                           SSTableActivity));
        return new KSMetaData(NAME, LocalStrategy.class, Collections.<String, String>emptyMap(), true, tables);
    }

    private static volatile Map<UUID, Pair<ReplayPosition, Long>> truncationRecords;
    private static volatile Map<UUID, Pair<ReplayPosition, Long>> truncationRecords;
#endif

enum class bootstrap_state {
    NEEDS_BOOTSTRAP,
    COMPLETED,
    IN_PROGRESS,
    DECOMMISSIONED
};

#if 0
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

    public static TabularData getCompactionHistory() throws OpenDataException
    {
        UntypedResultSet queryResultSet = executeInternal(String.format("SELECT * from system.%s", COMPACTION_HISTORY));
        return CompactionHistoryTabularData.from(queryResultSet);
    }
#endif
    struct compaction_history_entry {
        utils::UUID id;
        sstring ks;
        sstring cf;
        int64_t compacted_at = 0;
        int64_t bytes_in = 0;
        int64_t bytes_out = 0;
        // Key: number of rows merged
        // Value: counter
        std::unordered_map<int32_t, int64_t> rows_merged;
    };

    future<> update_compaction_history(sstring ksname, sstring cfname, int64_t compacted_at, int64_t bytes_in, int64_t bytes_out,
                                       std::unordered_map<int32_t, int64_t> rows_merged);
    future<std::vector<compaction_history_entry>> get_compaction_history();

    typedef std::vector<db::replay_position> replay_positions;

    future<> save_truncation_record(const column_family&, db_clock::time_point truncated_at, db::replay_position);
    future<> save_truncation_records(const column_family&, db_clock::time_point truncated_at, replay_positions);
    future<> remove_truncation_record(utils::UUID);
    future<replay_positions> get_truncated_position(utils::UUID);
    future<db::replay_position> get_truncated_position(utils::UUID, uint32_t shard);
    future<db_clock::time_point> get_truncated_at(utils::UUID);

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
#endif

    /**
     * Convenience method to update the list of tokens in the local system keyspace.
     *
     * @param addTokens tokens to add
     * @param rmTokens tokens to remove
     * @return the collection of persisted tokens
     */
    future<std::unordered_set<dht::token>> update_local_tokens(
        const std::unordered_set<dht::token> add_tokens,
        const std::unordered_set<dht::token> rm_tokens);

    /**
     * Return a map of stored tokens to IP addresses
     *
     */
    future<std::unordered_map<gms::inet_address, std::unordered_set<dht::token>>> load_tokens();

    /**
     * Return a map of store host_ids to IP addresses
     *
     */
    future<std::unordered_map<gms::inet_address, utils::UUID>> load_host_ids();

    future<std::unordered_set<dht::token>> get_saved_tokens();

future<int> increment_and_get_generation();
bool bootstrap_complete();
bool bootstrap_in_progress();
bootstrap_state get_bootstrap_state();
bool was_decommissioned();
future<> set_bootstrap_state(bootstrap_state state);

#if 0
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

    api::timestamp_type schema_creation_timestamp();
} // namespace system_keyspace
} // namespace db
