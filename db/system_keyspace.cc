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

#include <boost/range/algorithm_ext/push_back.hpp>
#include <boost/range/adaptor/transformed.hpp>

#include "system_keyspace.hh"
#include "types.hh"
#include "service/storage_service.hh"
#include "service/storage_proxy.hh"
#include "service/client_state.hh"
#include "service/query_state.hh"
#include "cql3/query_options.hh"
#include "cql3/query_processor.hh"
#include "utils/fb_utilities.hh"
#include "dht/i_partitioner.hh"
#include "version.hh"
#include "thrift/server.hh"
#include "exceptions/exceptions.hh"
#include "cql3/query_processor.hh"
#include "db/serializer.hh"
#include "query_context.hh"
#include "partition_slice_builder.hh"
#include "db/config.hh"
#include "schema_builder.hh"

using days = std::chrono::duration<int, std::ratio<24 * 3600>>;

namespace db {

std::unique_ptr<query_context> qctx = {};

namespace system_keyspace {

// Currently, the type variables (uuid_type, etc.) are thread-local reference-
// counted shared pointers. This forces us to also make the built in schemas
// below thread-local as well.
// We return schema_ptr, not schema&, because that's the "tradition" in our
// other code.
// We hide the thread_local variable inside a function, because if we later
// we remove the thread_local, we'll start having initialization order
// problems (we need the type variables to be constructed first), and using
// functions will solve this problem. So we use functions right now.

schema_ptr hints() {
    static thread_local auto hints = [] {
        schema_builder builder(make_lw_shared(schema(generate_legacy_id(NAME, HINTS), NAME, HINTS,
        // partition key
        {{"target_id", uuid_type}},
        // clustering key
        {{"hint_id", timeuuid_type}, {"message_version", int32_type}},
        // regular columns
        {{"mutation", bytes_type}},
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "hints awaiting delivery"
        // FIXME: the original Java code also had:
        // operations on resulting CFMetaData:
        //    .compactionStrategyOptions(Collections.singletonMap("enabled", "false"))
       )));
       builder.set_gc_grace_seconds(0);
       return builder.build(schema_builder::compact_storage::yes);
    }();
    return hints;
}

schema_ptr batchlog() {
    static thread_local auto batchlog = [] {
        schema_builder builder(make_lw_shared(schema(generate_legacy_id(NAME, BATCHLOG), NAME, BATCHLOG,
        // partition key
        {{"id", uuid_type}},
        // clustering key
        {},
        // regular columns
        {{"data", bytes_type}, {"version", int32_type}, {"written_at", timestamp_type}},
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "batches awaiting replay"
        // FIXME: the original Java code also had:
        // operations on resulting CFMetaData:
        //    .compactionStrategyOptions(Collections.singletonMap("min_threshold", "2"))
       )));
       builder.set_gc_grace_seconds(0);
       return builder.build();
    }();
    return batchlog;
}

/*static*/ schema_ptr paxos() {
    static thread_local auto paxos = [] {
        schema_builder builder(make_lw_shared(schema(generate_legacy_id(NAME, PAXOS), NAME, PAXOS,
        // partition key
        {{"row_key", bytes_type}},
        // clustering key
        {{"cf_id", uuid_type}},
        // regular columns
        {{"in_progress_ballot", timeuuid_type}, {"most_recent_commit", bytes_type}, {"most_recent_commit_at", timeuuid_type}, {"proposal", bytes_type}, {"proposal_ballot", timeuuid_type}},
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "in-progress paxos proposals"
        // FIXME: the original Java code also had:
        // operations on resulting CFMetaData:
        //    .compactionStrategyClass(LeveledCompactionStrategy.class);
       )));
       return builder.build();
    }();
    return paxos;
}

schema_ptr built_indexes() {
    static thread_local auto built_indexes = [] {
        schema_builder builder(make_lw_shared(schema(generate_legacy_id(NAME, BUILT_INDEXES), NAME, BUILT_INDEXES,
        // partition key
        {{"table_name", utf8_type}},
        // clustering key
        {{"index_name", utf8_type}},
        // regular columns
        {},
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "built column indexes"
       )));
       return builder.build(schema_builder::compact_storage::yes);
    }();
    return built_indexes;
}

/*static*/ schema_ptr local() {
    static thread_local auto local = [] {
        schema_builder builder(make_lw_shared(schema(generate_legacy_id(NAME, LOCAL), NAME, LOCAL,
        // partition key
        {{"key", utf8_type}},
        // clustering key
        {},
        // regular columns
        {
                {"bootstrapped", utf8_type},
                {"cluster_name", utf8_type},
                {"cql_version", utf8_type},
                {"data_center", utf8_type},
                {"gossip_generation", int32_type},
                {"host_id", uuid_type},
                {"native_protocol_version", utf8_type},
                {"partitioner", utf8_type},
                {"rack", utf8_type},
                {"release_version", utf8_type},
                {"schema_version", uuid_type},
                {"thrift_version", utf8_type},
                {"tokens", set_type_impl::get_instance(utf8_type, false)},
                {"truncated_at", map_type_impl::get_instance(uuid_type, bytes_type, false)},
        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "information about the local node"
       )));
       return builder.build();
    }();
    return local;
}

/*static*/ schema_ptr peers() {
    static thread_local auto peers = [] {
        schema_builder builder(make_lw_shared(schema(generate_legacy_id(NAME, PEERS), NAME, PEERS,
        // partition key
        {{"peer", inet_addr_type}},
        // clustering key
        {},
        // regular columns
        {
                {"data_center", utf8_type},
                {"host_id", uuid_type},
                {"preferred_ip", inet_addr_type},
                {"rack", utf8_type},
                {"release_version", utf8_type},
                {"rpc_address", inet_addr_type},
                {"schema_version", uuid_type},
                {"tokens", set_type_impl::get_instance(utf8_type, false)},
        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "information about known peers in the cluster"
       )));
       return builder.build();
    }();
    return peers;
}

/*static*/ schema_ptr peer_events() {
    static thread_local auto peer_events = [] {
        schema_builder builder(make_lw_shared(schema(generate_legacy_id(NAME, PEER_EVENTS), NAME, PEER_EVENTS,
        // partition key
        {{"peer", inet_addr_type}},
        // clustering key
        {},
        // regular columns
        {
            {"hints_dropped", map_type_impl::get_instance(uuid_type, int32_type, false)},
        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "events related to peers"
       )));
       return builder.build();
    }();
    return peer_events;
}

/*static*/ schema_ptr range_xfers() {
    static thread_local auto range_xfers = [] {
        schema_builder builder(make_lw_shared(schema(generate_legacy_id(NAME, RANGE_XFERS), NAME, RANGE_XFERS,
        // partition key
        {{"token_bytes", bytes_type}},
        // clustering key
        {},
        // regular columns
        {{"requested_at", timestamp_type}},
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "ranges requested for transfer"
       )));
       return builder.build();
    }();
    return range_xfers;
}

/*static*/ schema_ptr compactions_in_progress() {
    static thread_local auto compactions_in_progress = [] {
        schema_builder builder(make_lw_shared(schema(generate_legacy_id(NAME, COMPACTIONS_IN_PROGRESS), NAME, COMPACTIONS_IN_PROGRESS,
        // partition key
        {{"id", uuid_type}},
        // clustering key
        {},
        // regular columns
        {
            {"columnfamily_name", utf8_type},
            {"inputs", set_type_impl::get_instance(int32_type, false)},
            {"keyspace_name", utf8_type},
        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "unfinished compactions"
        )));
       return builder.build();
    }();
    return compactions_in_progress;
}

/*static*/ schema_ptr compaction_history() {
    static thread_local auto compaction_history = [] {
        schema_builder builder(make_lw_shared(schema(generate_legacy_id(NAME, COMPACTION_HISTORY), NAME, COMPACTION_HISTORY,
        // partition key
        {{"id", uuid_type}},
        // clustering key
        {},
        // regular columns
        {
            {"bytes_in", long_type},
            {"bytes_out", long_type},
            {"columnfamily_name", utf8_type},
            {"compacted_at", timestamp_type},
            {"keyspace_name", utf8_type},
            {"rows_merged", map_type_impl::get_instance(int32_type, long_type, false)},
        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "week-long compaction history"
        )));
        builder.set_default_time_to_live(std::chrono::duration_cast<std::chrono::seconds>(days(7)));
        return builder.build();
    }();
    return compaction_history;
}

/*static*/ schema_ptr sstable_activity() {
    static thread_local auto sstable_activity = [] {
        schema_builder builder(make_lw_shared(schema(generate_legacy_id(NAME, SSTABLE_ACTIVITY), NAME, SSTABLE_ACTIVITY,
        // partition key
        {
            {"keyspace_name", utf8_type},
            {"columnfamily_name", utf8_type},
            {"generation", int32_type},
        },
        // clustering key
        {},
        // regular columns
        {
            {"rate_120m", double_type},
            {"rate_15m", double_type},
        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "historic sstable read rates"
       )));
       return builder.build();
    }();
    return sstable_activity;
}

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

#endif

static future<> setup_version() {
    sstring req = "INSERT INTO system.%s (key, release_version, cql_version, thrift_version, native_protocol_version, data_center, rack, partitioner) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
    auto& snitch = locator::i_endpoint_snitch::get_local_snitch_ptr();

    return execute_cql(req, db::system_keyspace::LOCAL,
                             sstring(db::system_keyspace::LOCAL),
                             version::release(),
                             cql3::query_processor::CQL_VERSION,
                             org::apache::cassandra::thrift_version,
                             to_sstring(version::native_protocol()),
                             snitch->get_datacenter(utils::fb_utilities::get_broadcast_address()),
                             snitch->get_rack(utils::fb_utilities::get_broadcast_address()),
                             sstring(dht::global_partitioner().name())
    ).discard_result();
}

future<> check_health();
future<> force_blocking_flush(sstring cfname);

// Changing the real load_dc_rack_info into a future would trigger a tidal wave of futurization that would spread
// even into simple string operations like get_rack() / get_dc(). We will cache those at startup, and then change
// our view of it every time we do updates on those values.
//
// The cache must be distributed, because the values themselves may not update atomically, so a shard reading that
// is different than the one that wrote, may see a corrupted value. invoke_on_all will be used to guarantee that all
// updates are propagated correctly.
struct local_cache {
    std::unordered_map<gms::inet_address, locator::endpoint_dc_rack> _cached_dc_rack_info;
    future<> stop() {
        return make_ready_future<>();
    }
};
static distributed<local_cache> _local_cache;

static future<> build_dc_rack_info() {
    return _local_cache.start().then([] {
        engine().at_exit([] {
            _local_cache.stop();
        });

        return execute_cql("SELECT peer, data_center, rack from system.%s", PEERS).then([] (::shared_ptr<cql3::untyped_result_set> msg) {
            return do_for_each(*msg, [] (auto& row) {
                // Not ideal to assume ipv4 here, but currently this is what the cql types wraps.
                net::ipv4_address peer = row.template get_as<net::ipv4_address>("peer");
                if (!row.has("data_center") || !row.has("rack")) {
                    return make_ready_future<>();
                }
                gms::inet_address gms_addr(std::move(peer));
                sstring dc = row.template get_as<sstring>("data_center");
                sstring rack = row.template get_as<sstring>("rack");

                locator::endpoint_dc_rack  element = { dc, rack };
                return _local_cache.invoke_on_all([gms_addr = std::move(gms_addr), element = std::move(element)] (local_cache& lc) {
                    lc._cached_dc_rack_info.emplace(gms_addr, element);
                });
            });
        });
    });
}

future<> setup(distributed<database>& db, distributed<cql3::query_processor>& qp) {
    auto new_ctx = std::make_unique<query_context>(db, qp);
    qctx.swap(new_ctx);
    assert(!new_ctx);
    return setup_version().then([&db] {
        return update_schema_version(db.local().get_version());
    }).then([] {
        return build_dc_rack_info();
    }).then([] {
        return check_health();
    }).then([] {
        return db::legacy_schema_tables::save_system_keyspace_schema();
    });
}

#if 0
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


typedef std::pair<db::replay_position, db_clock::time_point> truncation_entry;
typedef std::unordered_map<utils::UUID, truncation_entry> truncation_map;
static thread_local std::experimental::optional<truncation_map> truncation_records;

future<> save_truncation_record(cql3::query_processor& qp, const column_family& cf, db_clock::time_point truncated_at, const db::replay_position& rp) {
    db::serializer<replay_position> rps(rp);
    bytes buf(bytes::initialized_later(), sizeof(db_clock::rep) + rps.size());
    data_output out(buf);
    rps(out);
    out.write<db_clock::rep>(truncated_at.time_since_epoch().count());

    map_type_impl::native_type tmp;
    tmp.emplace_back(boost::any{ cf.schema()->id() }, boost::any{ buf });

    sstring req = sprint("UPDATE system.%s SET truncated_at = truncated_at + ? WHERE key = '%s'", LOCAL, LOCAL);
    return qp.execute_internal(req, {tmp}).then([&qp](auto rs) {
        truncation_records = {};
        return force_blocking_flush(LOCAL);
    });
}

/**
 * This method is used to remove information about truncation time for specified column family
 */
future<> remove_truncation_record(cql3::query_processor& qp, utils::UUID id) {
    sstring req = sprint("DELETE truncated_at[?] from system.%s WHERE key = '%s'", LOCAL, LOCAL);
    return qp.execute_internal(req, {id}).then([&qp](auto rs) {
        truncation_records = {};
        return force_blocking_flush(LOCAL);
    });
}

static future<truncation_entry> get_truncation_record(cql3::query_processor& qp, utils::UUID cf_id) {
    if (!truncation_records) {
        sstring req = sprint("SELECT truncated_at FROM system.%s WHERE key = '%s'", LOCAL, LOCAL);
        return qp.execute_internal(req).then([&qp, cf_id](::shared_ptr<cql3::untyped_result_set> rs) {
            truncation_map tmp;
            if (!rs->empty() && rs->one().has("truncated_set")) {
                auto map = rs->one().get_map<utils::UUID, bytes>("truncated_at");
                for (auto& p : map) {
                    truncation_entry e;
                    data_input in(p.second);
                    e.first = db::serializer<replay_position>::read(in);
                    e.second = db_clock::time_point(db_clock::duration(in.read<db_clock::rep>()));
                    tmp[p.first] = e;
                }
            }
            truncation_records = std::move(tmp);
            return get_truncation_record(qp, cf_id);
        });
    }
    return make_ready_future<truncation_entry>((*truncation_records)[cf_id]);
}

future<db::replay_position> get_truncated_position(cql3::query_processor& qp, utils::UUID cf_id) {
    return get_truncation_record(qp, cf_id).then([](truncation_entry e) {
        return make_ready_future<db::replay_position>(e.first);
    });
}

future<db_clock::time_point> get_truncated_at(cql3::query_processor& qp, utils::UUID cf_id) {
    return get_truncation_record(qp, cf_id).then([](truncation_entry e) {
        return make_ready_future<db_clock::time_point>(e.second);
    });
}


set_type_impl::native_type prepare_tokens(std::unordered_set<dht::token>& tokens) {
    set_type_impl::native_type tset;
    for (auto& t: tokens) {
        tset.push_back(boost::any(dht::global_partitioner().to_sstring(t)));
    }
    return tset;
}

/**
 * Record tokens being used by another node
 */
future<> update_tokens(gms::inet_address ep, std::unordered_set<dht::token> tokens)
{
    if (ep == utils::fb_utilities::get_broadcast_address()) {
        return remove_endpoint(ep);
    }

    sstring req = "INSERT INTO system.%s (peer, tokens) VALUES (?, ?)";
    return execute_cql(req, PEERS, ep, prepare_tokens(tokens)).discard_result().then([] {
        return force_blocking_flush(PEERS);
    });
}

future<> update_preferred_ip(gms::inet_address ep, gms::inet_address preferred_ip) {
    sstring req = "INSERT INTO system.%s (peer, preferred_ip) VALUES (?, ?)";
    return execute_cql(req, PEERS, ep, preferred_ip).discard_result().then([] {
        return force_blocking_flush(PEERS);
    });
}

template <typename Value>
static future<> update_cached_values(gms::inet_address ep, sstring column_name, Value value) {
    return make_ready_future<>();
}

template <>
future<> update_cached_values(gms::inet_address ep, sstring column_name, sstring value) {
    return _local_cache.invoke_on_all([ep = std::move(ep),
                                       column_name = std::move(column_name),
                                       value = std::move(value)] (local_cache& lc) {
        if (column_name == "data_center") {
            lc._cached_dc_rack_info[ep].dc = value;
        } else if (column_name == "rack") {
            lc._cached_dc_rack_info[ep].rack = value;
        }
        return make_ready_future<>();
    });
}

template <typename Value>
future<> update_peer_info(gms::inet_address ep, sstring column_name, Value value) {
    if (ep == utils::fb_utilities::get_broadcast_address()) {
        return make_ready_future<>();
    }

    return update_cached_values(ep, column_name, value).then([ep, column_name, value] {
        sstring clause = sprint("(peer, %s) VALUES (?, ?)", column_name);
        sstring req = "INSERT INTO system.%s " + clause;
        return execute_cql(req, PEERS, ep.addr(), value).discard_result();
    });
}
// sets are not needed, since tokens are updated by another method
template future<> update_peer_info<sstring>(gms::inet_address ep, sstring column_name, sstring);
template future<> update_peer_info<utils::UUID>(gms::inet_address ep, sstring column_name, utils::UUID);
template future<> update_peer_info<net::ipv4_address>(gms::inet_address ep, sstring column_name, net::ipv4_address);

future<> update_hints_dropped(gms::inet_address ep, utils::UUID time_period, int value) {
    // with 30 day TTL
    sstring req = "UPDATE system.%s USING TTL 2592000 SET hints_dropped[ ? ] = ? WHERE peer = ?";
    return execute_cql(req, PEER_EVENTS, time_period, value, ep).discard_result();
}

future<> update_schema_version(utils::UUID version) {
    sstring req = "INSERT INTO system.%s (key, schema_version) VALUES (?, ?)";
    return execute_cql(req, LOCAL, sstring(LOCAL), version).discard_result();
}

#if 0

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

#endif
/**
 * Remove stored tokens being used by another node
 */
future<> remove_endpoint(gms::inet_address ep) {
    return _local_cache.invoke_on_all([ep] (local_cache& lc) {
        lc._cached_dc_rack_info.erase(ep);
    }).then([ep] {
        sstring req = "DELETE FROM system.%s WHERE peer = ?";
        return execute_cql(req, PEERS, ep).discard_result();
    });
}

    /**
     * This method is used to update the System Keyspace with the new tokens for this node
    */
future<> update_tokens(std::unordered_set<dht::token> tokens) {
    if (tokens.empty()) {
        throw std::invalid_argument("remove_endpoint should be used instead");
    }

    sstring req = "INSERT INTO system.%s (key, tokens) VALUES (?, ?)";
    return execute_cql(req, LOCAL, sstring(LOCAL), prepare_tokens(tokens)).discard_result().then([] {
        return force_blocking_flush(LOCAL);
    });
}

#if 0

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

future<> force_blocking_flush(sstring cfname) {
    if (!qctx) {
        return make_ready_future<>();
    }

    return qctx->_db.invoke_on_all([cfname = std::move(cfname)](database& db) {
        // if (!Boolean.getBoolean("cassandra.unsafesystem"))
        column_family& cf = db.find_column_family(NAME, cfname);
        return cf.flush();
    });
}

#if 0
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

#endif
/**
 * One of three things will happen if you try to read the system keyspace:
 * 1. files are present and you can read them: great
 * 2. no files are there: great (new node is assumed)
 * 3. files are present but you can't read them: bad
 */
future<> check_health() {
    using namespace transport::messages;
    sstring req = "SELECT cluster_name FROM system.%s WHERE key=?";
    return execute_cql(req, LOCAL, sstring(LOCAL)).then([] (::shared_ptr<cql3::untyped_result_set> msg) {
        if (msg->empty() || !msg->one().has("cluster_name")) {
            // this is a brand new node
            sstring ins_req = "INSERT INTO system.%s (key, cluster_name) VALUES (?, ?)";
            return execute_cql(ins_req, LOCAL, sstring(LOCAL), qctx->db().get_config().cluster_name()).discard_result();
        } else {
            auto saved_cluster_name = msg->one().get_as<sstring>("cluster_name");
            auto cluster_name = qctx->db().get_config().cluster_name();

            if (cluster_name != saved_cluster_name) {
                throw exceptions::configuration_exception("Saved cluster name " + saved_cluster_name + " != configured name " + cluster_name);
            }

            return make_ready_future<>();
        }
    });
}

#if 0
    public static Collection<Token> getSavedTokens()
    {
        String req = "SELECT tokens FROM system.%s WHERE key='%s'";
        UntypedResultSet result = executeInternal(String.format(req, LOCAL, LOCAL));
        return result.isEmpty() || !result.one().has("tokens")
             ? Collections.<Token>emptyList()
             : deserializeTokens(result.one().getSet("tokens", UTF8Type.instance));
    }

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
#endif

#if 0
future<> set_bootstrap_state(bootstrap_state state) {
    sstring req = "INSERT INTO system.%s (key, bootstrapped) VALUES ('%s', '%s')";
    return execute_cql(req, LOCAL, LOCAL, state.name()).discard_result().then([] {
        return force_blocking_flush(LOCAL);
    });
}
#endif

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

    /**
     * Read the host ID from the system keyspace, creating (and storing) one if
     * none exists.
     */
    public static UUID getLocalHostId()
    {
        String req = "SELECT host_id FROM system.%s WHERE key='%s'";
        UntypedResultSet result = executeInternal(String.format(req, LOCAL, LOCAL));

        // Look up the Host UUID (return it if found)
        if (!result.isEmpty() && result.one().has("host_id"))
            return result.one().getUUID("host_id");

        // ID not found, generate a new one, persist, and then return it.
        UUID hostId = UUID.randomUUID();
        logger.warn("No host ID found, created {} (Note: This should happen exactly once per node).", hostId);
        return setLocalHostId(hostId);
    }

    /**
     * Sets the local host ID explicitly.  Should only be called outside of SystemTable when replacing a node.
     */
    public static UUID setLocalHostId(UUID hostId)
    {
        String req = "INSERT INTO system.%s (key, host_id) VALUES ('%s', ?)";
        executeInternal(String.format(req, LOCAL, LOCAL), hostId);
        return hostId;
    }

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

std::vector<schema_ptr> all_tables() {
    std::vector<schema_ptr> r;
    auto legacy_tables = db::legacy_schema_tables::all_tables();
    std::copy(legacy_tables.begin(), legacy_tables.end(), std::back_inserter(r));
    r.push_back(built_indexes());
    r.push_back(hints());
    r.push_back(batchlog());
    r.push_back(paxos());
    r.push_back(local());
    r.push_back(peers());
    r.push_back(peer_events());
    r.push_back(range_xfers());
    r.push_back(compactions_in_progress());
    r.push_back(compaction_history());
    r.push_back(sstable_activity());
    return r;
}

void make(database& db, bool durable) {
    auto ksm = make_lw_shared<keyspace_metadata>(NAME,
            "org.apache.cassandra.locator.LocalStrategy",
            std::map<sstring, sstring>{},
            durable
            );
    auto kscfg = db.make_keyspace_config(*ksm);
    keyspace _ks{ksm, std::move(kscfg)};
    auto rs(locator::abstract_replication_strategy::create_replication_strategy(NAME, "LocalStrategy", service::get_local_storage_service().get_token_metadata(), ksm->strategy_options()));
    _ks.set_replication_strategy(std::move(rs));
    db.add_keyspace(NAME, std::move(_ks));
    auto& ks = db.find_keyspace(NAME);
    for (auto&& table : all_tables()) {
        db.add_column_family(table, ks.make_column_family_config(*table));
    }
}

future<utils::UUID> get_local_host_id() {
    using namespace transport::messages;
    sstring req = "SELECT host_id FROM system.%s WHERE key=?";
    return execute_cql(req, LOCAL, sstring(LOCAL)).then([] (::shared_ptr<cql3::untyped_result_set> msg) {
        auto new_id = [] {
            auto host_id = utils::make_random_uuid();
            return make_ready_future<utils::UUID>(host_id);
        };
        if (msg->empty() || !msg->one().has("host_id")) {
            return new_id();
        }

        auto host_id = msg->one().get_as<utils::UUID>("host_id");
        return make_ready_future<utils::UUID>(host_id);
    });
}

future<utils::UUID> set_local_host_id(const utils::UUID& host_id) {
    sstring req = "INSERT INTO system.%s (key, host_id) VALUES (?, ?)";
    return execute_cql(req, LOCAL, sstring(LOCAL), host_id).then([] (auto msg) {
        return force_blocking_flush(LOCAL);
    }).then([host_id] {
        return host_id;
    });
}

std::unordered_map<gms::inet_address, locator::endpoint_dc_rack>
load_dc_rack_info() {
    return _local_cache.local()._cached_dc_rack_info;
}

future<foreign_ptr<lw_shared_ptr<reconcilable_result>>>
query_mutations(service::storage_proxy& proxy, const sstring& cf_name) {
    database& db = proxy.get_db().local();
    schema_ptr schema = db.find_schema(db::system_keyspace::NAME, cf_name);
    auto slice = partition_slice_builder(*schema).build();
    auto cmd = make_lw_shared<query::read_command>(schema->id(), std::move(slice), std::numeric_limits<uint32_t>::max());
    return proxy.query_mutations_locally(cmd, query::full_partition_range);
}

future<lw_shared_ptr<query::result_set>>
query(service::storage_proxy& proxy, const sstring& cf_name) {
    database& db = proxy.get_db().local();
    schema_ptr schema = db.find_schema(db::system_keyspace::NAME, cf_name);
    auto slice = partition_slice_builder(*schema).build();
    auto cmd = make_lw_shared<query::read_command>(schema->id(), std::move(slice), std::numeric_limits<uint32_t>::max());
    return proxy.query(schema, cmd, {query::full_partition_range}, db::consistency_level::ONE).then([schema, cmd] (auto&& result) {
        return make_lw_shared(query::result_set::from_raw_result(schema, cmd->slice, *result));
    });
}

future<lw_shared_ptr<query::result_set>>
query(service::storage_proxy& proxy, const sstring& cf_name, const dht::decorated_key& key, query::clustering_range row_range)
{
    auto&& db = proxy.get_db().local();
    auto schema = db.find_schema(db::system_keyspace::NAME, cf_name);
    auto slice = partition_slice_builder(*schema)
        .with_range(std::move(row_range))
        .build();
    auto cmd = make_lw_shared<query::read_command>(schema->id(), std::move(slice), query::max_rows);
    return proxy.query(schema, cmd, {query::partition_range::make_singular(key)}, db::consistency_level::ONE).then([schema, cmd] (auto&& result) {
        return make_lw_shared(query::result_set::from_raw_result(schema, cmd->slice, *result));
    });
}

} // namespace system_keyspace
} // namespace db
