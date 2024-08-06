/*
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include <boost/range/algorithm.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include <boost/functional/hash.hpp>
#include <boost/icl/interval_map.hpp>
#include <fmt/ranges.h>

#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/core/on_internal_error.hh>
#include "system_keyspace.hh"
#include "cql3/untyped_result_set.hh"
#include "cql3/query_processor.hh"
#include "partition_slice_builder.hh"
#include "db/config.hh"
#include "gms/feature_service.hh"
#include "system_keyspace_view_types.hh"
#include "schema/schema_builder.hh"
#include "utils/assert.hh"
#include "utils/hashers.hh"
#include "log.hh"
#include <seastar/core/enum.hh>
#include "gms/inet_address.hh"
#include "message/messaging_service.hh"
#include "mutation_query.hh"
#include "db/timeout_clock.hh"
#include "sstables/sstables.hh"
#include "db/schema_tables.hh"
#include "gms/generation-number.hh"
#include "service/storage_service.hh"
#include "service/paxos/paxos_state.hh"
#include "query-result-set.hh"
#include "idl/frozen_mutation.dist.hh"
#include "idl/frozen_mutation.dist.impl.hh"
#include "service/topology_state_machine.hh"
#include "sstables/generation_type.hh"
#include "cdc/generation.hh"
#include "replica/tablets.hh"
#include "replica/query.hh"
#include "types/types.hh"
#include "service/raft/raft_group0_client.hh"

#include <unordered_map>

using days = std::chrono::duration<int, std::ratio<24 * 3600>>;

namespace db {
namespace {
    const auto set_null_sharder = schema_builder::register_static_configurator([](const sstring& ks_name, const sstring& cf_name, schema_static_props& props) {
        // tables in the "system" keyspace which need to use null sharder
        static const std::unordered_set<sstring> tables = {
                // empty
        };
        if (ks_name == system_keyspace::NAME && tables.contains(cf_name)) {
            props.use_null_sharder = true;
        }
    });
    const auto set_wait_for_sync_to_commitlog = schema_builder::register_static_configurator([](const sstring& ks_name, const sstring& cf_name, schema_static_props& props) {
        static const std::unordered_set<sstring> tables = {
            system_keyspace::PAXOS,
        };
        if (ks_name == system_keyspace::NAME && tables.contains(cf_name)) {
            props.wait_for_sync_to_commitlog = true;
        }
    });
    const auto set_use_schema_commitlog = schema_builder::register_static_configurator([](const sstring& ks_name, const sstring& cf_name, schema_static_props& props) {
        static const std::unordered_set<sstring> tables = {
            schema_tables::SCYLLA_TABLE_SCHEMA_HISTORY,
            system_keyspace::BROADCAST_KV_STORE,
            system_keyspace::CDC_GENERATIONS_V3,
            system_keyspace::RAFT,
            system_keyspace::RAFT_SNAPSHOTS,
            system_keyspace::RAFT_SNAPSHOT_CONFIG,
            system_keyspace::GROUP0_HISTORY,
            system_keyspace::DISCOVERY,
            system_keyspace::TABLETS,
            system_keyspace::TOPOLOGY,
            system_keyspace::TOPOLOGY_REQUESTS,
            system_keyspace::LOCAL,
            system_keyspace::PEERS,
            system_keyspace::SCYLLA_LOCAL,
            system_keyspace::COMMITLOG_CLEANUPS,
            system_keyspace::SERVICE_LEVELS_V2,
            system_keyspace::ROLES,
            system_keyspace::ROLE_MEMBERS,
            system_keyspace::ROLE_ATTRIBUTES,
            system_keyspace::ROLE_PERMISSIONS,
            system_keyspace::v3::CDC_LOCAL
        };
        if (ks_name == system_keyspace::NAME && tables.contains(cf_name)) {
            props.enable_schema_commitlog();
        }
    });
}

static logging::logger slogger("system_keyspace");
static const api::timestamp_type creation_timestamp = api::new_timestamp();

api::timestamp_type system_keyspace::schema_creation_timestamp() {
    return creation_timestamp;
}

// Increase whenever changing schema of any system table.
// FIXME: Make automatic by calculating from schema structure.
static const uint16_t version_sequence_number = 1;

table_schema_version system_keyspace::generate_schema_version(::table_id table_id, uint16_t offset) {
    md5_hasher h;
    feed_hash(h, table_id);
    feed_hash(h, version_sequence_number + offset);
    return table_schema_version(utils::UUID_gen::get_name_UUID(h.finalize()));
}

// Currently, the type variables (uuid_type, etc.) are thread-local reference-
// counted shared pointers. This forces us to also make the built in schemas
// below thread-local as well.
// We return schema_ptr, not schema&, because that's the "tradition" in our
// other code.
// We hide the thread_local variable inside a function, because if we later
// we remove the thread_local, we'll start having initialization order
// problems (we need the type variables to be constructed first), and using
// functions will solve this problem. So we use functions right now.


schema_ptr system_keyspace::hints() {
    static thread_local auto hints = [] {
        schema_builder builder(generate_legacy_id(NAME, HINTS), NAME, HINTS,
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
       );
       builder.set_gc_grace_seconds(0);
       builder.set_compaction_strategy_options({{ "enabled", "false" }});
       builder.with_version(generate_schema_version(builder.uuid()));
       return builder.build(schema_builder::compact_storage::yes);
    }();
    return hints;
}

schema_ptr system_keyspace::batchlog() {
    static thread_local auto batchlog = [] {
        schema_builder builder(generate_legacy_id(NAME, BATCHLOG), NAME, BATCHLOG,
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
       );
       builder.set_gc_grace_seconds(0);
       builder.with_version(generate_schema_version(builder.uuid()));
       return builder.build(schema_builder::compact_storage::no);
    }();
    return batchlog;
}

/*static*/ schema_ptr system_keyspace::paxos() {
    static thread_local auto paxos = [] {
        // FIXME: switch to the new schema_builder interface (with_column(...), etc)
        schema_builder builder(generate_legacy_id(NAME, PAXOS), NAME, PAXOS,
        // partition key
        {{"row_key", bytes_type}}, // byte representation of a row key that hashes to the same token as original
        // clustering key
        {{"cf_id", uuid_type}},
        // regular columns
        {
            {"promise", timeuuid_type},
            {"most_recent_commit", bytes_type}, // serialization format is defined by frozen_mutation idl
            {"most_recent_commit_at", timeuuid_type},
            {"proposal", bytes_type}, // serialization format is defined by frozen_mutation idl
            {"proposal_ballot", timeuuid_type},
        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "in-progress paxos proposals"
        // FIXME: the original Java code also had:
        // operations on resulting CFMetaData:
        //    .compactionStrategyClass(LeveledCompactionStrategy.class);
       );
       builder.set_gc_grace_seconds(0);
       builder.with_version(generate_schema_version(builder.uuid()));
       return builder.build(schema_builder::compact_storage::no);
    }();
    return paxos;
}

thread_local data_type cdc_generation_ts_id_type = tuple_type_impl::get_instance({timestamp_type, timeuuid_type});

schema_ptr system_keyspace::topology() {
    static thread_local auto schema = [] {
        auto id = generate_legacy_id(NAME, TOPOLOGY);
        return schema_builder(NAME, TOPOLOGY, std::optional(id))
            .with_column("key", utf8_type, column_kind::partition_key)
            .with_column("host_id", uuid_type, column_kind::clustering_key)
            .with_column("datacenter", utf8_type)
            .with_column("rack", utf8_type)
            .with_column("tokens", set_type_impl::get_instance(utf8_type, true))
            .with_column("node_state", utf8_type)
            .with_column("release_version", utf8_type)
            .with_column("topology_request", utf8_type)
            .with_column("replaced_id", uuid_type)
            .with_column("rebuild_option", utf8_type)
            .with_column("num_tokens", int32_type)
            .with_column("tokens_string", utf8_type)
            .with_column("shard_count", int32_type)
            .with_column("ignore_msb", int32_type)
            .with_column("cleanup_status", utf8_type)
            .with_column("supported_features", set_type_impl::get_instance(utf8_type, true))
            .with_column("request_id", timeuuid_type)
            .with_column("ignore_nodes", set_type_impl::get_instance(uuid_type, true), column_kind::static_column)
            .with_column("new_cdc_generation_data_uuid", timeuuid_type, column_kind::static_column)
            .with_column("new_keyspace_rf_change_ks_name", utf8_type, column_kind::static_column)
            .with_column("new_keyspace_rf_change_data", map_type_impl::get_instance(utf8_type, utf8_type, false), column_kind::static_column)
            .with_column("version", long_type, column_kind::static_column)
            .with_column("fence_version", long_type, column_kind::static_column)
            .with_column("transition_state", utf8_type, column_kind::static_column)
            .with_column("committed_cdc_generations", set_type_impl::get_instance(cdc_generation_ts_id_type, true), column_kind::static_column)
            .with_column("unpublished_cdc_generations", set_type_impl::get_instance(cdc_generation_ts_id_type, true), column_kind::static_column)
            .with_column("global_topology_request", utf8_type, column_kind::static_column)
            .with_column("global_topology_request_id", timeuuid_type, column_kind::static_column)
            .with_column("enabled_features", set_type_impl::get_instance(utf8_type, true), column_kind::static_column)
            .with_column("session", uuid_type, column_kind::static_column)
            .with_column("tablet_balancing_enabled", boolean_type, column_kind::static_column)
            .with_column("upgrade_state", utf8_type, column_kind::static_column)
            .set_comment("Current state of topology change machine")
            .with_version(generate_schema_version(id))
            .build();
    }();
    return schema;
}

schema_ptr system_keyspace::topology_requests() {
    static thread_local auto schema = [] {
        auto id = generate_legacy_id(NAME, TOPOLOGY_REQUESTS);
        return schema_builder(NAME, TOPOLOGY_REQUESTS, std::optional(id))
            .with_column("id", timeuuid_type, column_kind::partition_key)
            .with_column("initiating_host", uuid_type)
            .with_column("request_type", utf8_type)
            .with_column("start_time", timestamp_type)
            .with_column("done", boolean_type)
            .with_column("error", utf8_type)
            .with_column("end_time", timestamp_type)
            .set_comment("Topology request tracking")
            .with_version(generate_schema_version(id))
            .build();
    }();
    return schema;
}

extern thread_local data_type cdc_streams_set_type;

/* An internal table used by nodes to store CDC generation data.
 * Written to by Raft Group 0. */
schema_ptr system_keyspace::cdc_generations_v3() {
    thread_local auto schema = [] {
        auto id = generate_legacy_id(NAME, CDC_GENERATIONS_V3);
        return schema_builder(NAME, CDC_GENERATIONS_V3, {id})
            /* This is a single-partition table with key 'cdc_generations'. */
            .with_column("key", utf8_type, column_kind::partition_key)
            /* The unique identifier of this generation. */
            .with_column("id", timeuuid_type, column_kind::clustering_key)
            /* The generation describes a mapping from all tokens in the token ring to a set of stream IDs.
             * This mapping is built from a bunch of smaller mappings, each describing how tokens in a
             * subrange of the token ring are mapped to stream IDs; these subranges together cover the entire
             * token ring. Each such range-local mapping is represented by a row of this table. The second
             * column of the clustering key of the row is the end of the range being described by this row.
             * The start of this range is the range_end of the previous row (in the clustering order, which
             * is the integer order) or of the last row with the same id value if this is the first row with
             * such id. */
            .with_column("range_end", long_type, column_kind::clustering_key)
            /* The set of streams mapped to in this range.  The number of streams mapped to a single range in
             * a CDC generation is bounded from above by the number of shards on the owner of that range in
             * the token ring. In other words, the number of elements of this set is bounded by the maximum
             * of the number of shards over all nodes. The serialized size is obtained by counting about 20B
             * for each stream. For example, if all nodes in the cluster have at most 128 shards, the
             * serialized size of this set will be bounded by ~2.5 KB. */
            .with_column("streams", cdc_streams_set_type)
            /* The value of the `ignore_msb` sharding parameter of the node which was the owner of this token
             * range when the generation was first created. Together with the set of streams above it fully
             * describes the mapping for this particular range. */
            .with_column("ignore_msb", byte_type)
            .with_version(system_keyspace::generate_schema_version(id))
            .build();
    }();
    return schema;
}

schema_ptr system_keyspace::raft() {
    static thread_local auto schema = [] {
        auto id = generate_legacy_id(NAME, RAFT);
        return schema_builder(NAME, RAFT, std::optional(id))
            .with_column("group_id", timeuuid_type, column_kind::partition_key)
            // raft log part
            .with_column("index", long_type, column_kind::clustering_key)
            .with_column("term", long_type)
            .with_column("data", bytes_type) // decltype(raft::log_entry::data) - serialized variant
            // persisted term and vote
            .with_column("vote_term", long_type, column_kind::static_column)
            .with_column("vote", uuid_type, column_kind::static_column)
            // id of the most recent persisted snapshot
            .with_column("snapshot_id", uuid_type, column_kind::static_column)
            .with_column("commit_idx", long_type, column_kind::static_column)

            .set_comment("Persisted RAFT log, votes and snapshot info")
            .with_version(generate_schema_version(id))
            .build();
    }();
    return schema;
}

// Note that this table does not include actula user snapshot data since it's dependent
// on user-provided state machine and could be stored anywhere else in any other form.
// This should be seen as a snapshot descriptor, instead.
schema_ptr system_keyspace::raft_snapshots() {
    static thread_local auto schema = [] {
        auto id = generate_legacy_id(NAME, RAFT_SNAPSHOTS);
        return schema_builder(NAME, RAFT_SNAPSHOTS, std::optional(id))
            .with_column("group_id", timeuuid_type, column_kind::partition_key)
            .with_column("snapshot_id", uuid_type)
            // Index and term of last entry in the snapshot
            .with_column("idx", long_type)
            .with_column("term", long_type)

            .set_comment("Persisted RAFT snapshot descriptors info")
            .with_version(generate_schema_version(id))
            .build();
    }();
    return schema;
}

schema_ptr system_keyspace::raft_snapshot_config() {
    static thread_local auto schema = [] {
        auto id = generate_legacy_id(system_keyspace::NAME, RAFT_SNAPSHOT_CONFIG);
        return schema_builder(system_keyspace::NAME, RAFT_SNAPSHOT_CONFIG, std::optional(id))
            .with_column("group_id", timeuuid_type, column_kind::partition_key)
            .with_column("disposition", ascii_type, column_kind::clustering_key) // can be 'CURRENT` or `PREVIOUS'
            .with_column("server_id", uuid_type, column_kind::clustering_key)
            .with_column("can_vote", boolean_type)

            .set_comment("RAFT configuration for the latest snapshot descriptor")
            .with_version(generate_schema_version(id))
            .build();
    }();
    return schema;
}

schema_ptr system_keyspace::repair_history() {
    static thread_local auto schema = [] {
        auto id = generate_legacy_id(NAME, REPAIR_HISTORY);
        return schema_builder(NAME, REPAIR_HISTORY, std::optional(id))
            .with_column("table_uuid", uuid_type, column_kind::partition_key)
            // The time is repair start time
            .with_column("repair_time", timestamp_type, column_kind::clustering_key)
            .with_column("repair_uuid", uuid_type, column_kind::clustering_key)
            // The token range is (range_start, range_end]
            .with_column("range_start", long_type, column_kind::clustering_key)
            .with_column("range_end", long_type, column_kind::clustering_key)
            .with_column("keyspace_name", utf8_type, column_kind::static_column)
            .with_column("table_name", utf8_type, column_kind::static_column)
            .set_comment("Record repair history")
            .with_version(generate_schema_version(id))
            .build();
    }();
    return schema;
}

schema_ptr system_keyspace::built_indexes() {
    static thread_local auto built_indexes = [] {
        schema_builder builder(generate_legacy_id(NAME, BUILT_INDEXES), NAME, BUILT_INDEXES,
        // partition key
        {{"table_name", utf8_type}}, // table_name here is the name of the keyspace - don't be fooled
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
       );
       builder.set_gc_grace_seconds(0);
       builder.with_version(generate_schema_version(builder.uuid()));
       return builder.build(schema_builder::compact_storage::yes);
    }();
    return built_indexes;
}

/*static*/ schema_ptr system_keyspace::local() {
    static thread_local auto local = [] {
        schema_builder builder(generate_legacy_id(NAME, LOCAL), NAME, LOCAL,
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
                {"tokens", set_type_impl::get_instance(utf8_type, true)},
                {"truncated_at", map_type_impl::get_instance(uuid_type, bytes_type, true)},
                // The following 3 columns are only present up until 2.1.8 tables
                {"rpc_address", inet_addr_type},
                {"broadcast_address", inet_addr_type},
                {"listen_address", inet_addr_type},
                // This column represents advertised local features (i.e. the features
                // advertised by the node via gossip after passing the feature check
                // against remote features in the cluster)
                {"supported_features", utf8_type},
                {"scylla_cpu_sharding_algorithm", utf8_type},
                {"scylla_nr_shards", int32_type},
                {"scylla_msb_ignore", int32_type},

        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "information about the local node"
       );
       builder.set_gc_grace_seconds(0);
       builder.with_version(generate_schema_version(builder.uuid()));
       builder.remove_column("scylla_cpu_sharding_algorithm");
       builder.remove_column("scylla_nr_shards");
       builder.remove_column("scylla_msb_ignore");
       builder.remove_column("thrift_version");
       return builder.build(schema_builder::compact_storage::no);
    }();
    return local;
}

/*static*/ schema_ptr system_keyspace::peers() {
    constexpr uint16_t schema_version_offset = 0;
    static thread_local auto peers = [] {
        schema_builder builder(generate_legacy_id(NAME, PEERS), NAME, PEERS,
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
                {"tokens", set_type_impl::get_instance(utf8_type, true)},
                {"supported_features", utf8_type},
        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "information about known peers in the cluster"
       );
       builder.set_gc_grace_seconds(0);
       builder.with_version(generate_schema_version(builder.uuid(), schema_version_offset));
       return builder.build(schema_builder::compact_storage::no);
    }();
    return peers;
}

/*static*/ schema_ptr system_keyspace::peer_events() {
    static thread_local auto peer_events = [] {
        schema_builder builder(generate_legacy_id(NAME, PEER_EVENTS), NAME, PEER_EVENTS,
        // partition key
        {{"peer", inet_addr_type}},
        // clustering key
        {},
        // regular columns
        {
            {"hints_dropped", map_type_impl::get_instance(uuid_type, int32_type, true)},
        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "events related to peers"
       );
       builder.set_gc_grace_seconds(0);
       builder.with_version(generate_schema_version(builder.uuid()));
       return builder.build(schema_builder::compact_storage::no);
    }();
    return peer_events;
}

/*static*/ schema_ptr system_keyspace::range_xfers() {
    static thread_local auto range_xfers = [] {
        schema_builder builder(generate_legacy_id(NAME, RANGE_XFERS), NAME, RANGE_XFERS,
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
       );
       builder.set_gc_grace_seconds(0);
       builder.with_version(generate_schema_version(builder.uuid()));
       return builder.build(schema_builder::compact_storage::no);
    }();
    return range_xfers;
}

/*static*/ schema_ptr system_keyspace::compactions_in_progress() {
    static thread_local auto compactions_in_progress = [] {
        schema_builder builder(generate_legacy_id(NAME, COMPACTIONS_IN_PROGRESS), NAME, COMPACTIONS_IN_PROGRESS,
        // partition key
        {{"id", uuid_type}},
        // clustering key
        {},
        // regular columns
        {
            {"columnfamily_name", utf8_type},
            {"inputs", set_type_impl::get_instance(int32_type, true)},
            {"keyspace_name", utf8_type},
        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "unfinished compactions"
        );
       builder.with_version(generate_schema_version(builder.uuid()));
       return builder.build(schema_builder::compact_storage::no);
    }();
    return compactions_in_progress;
}

/*static*/ schema_ptr system_keyspace::compaction_history() {
    static thread_local auto compaction_history = [] {
        schema_builder builder(generate_legacy_id(NAME, COMPACTION_HISTORY), NAME, COMPACTION_HISTORY,
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
            {"rows_merged", map_type_impl::get_instance(int32_type, long_type, true)},
        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "week-long compaction history"
        );
        builder.set_default_time_to_live(std::chrono::duration_cast<std::chrono::seconds>(days(7)));
        builder.with_version(generate_schema_version(builder.uuid()));
        return builder.build(schema_builder::compact_storage::no);
    }();
    return compaction_history;
}

/*static*/ schema_ptr system_keyspace::sstable_activity() {
    static thread_local auto sstable_activity = [] {
        schema_builder builder(generate_legacy_id(NAME, SSTABLE_ACTIVITY), NAME, SSTABLE_ACTIVITY,
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
       );
       builder.with_version(generate_schema_version(builder.uuid()));
       return builder.build(schema_builder::compact_storage::no);
    }();
    return sstable_activity;
}

schema_ptr system_keyspace::size_estimates() {
    static thread_local auto size_estimates = [] {
        schema_builder builder(generate_legacy_id(NAME, SIZE_ESTIMATES), NAME, SIZE_ESTIMATES,
            // partition key
            {{"keyspace_name", utf8_type}},
            // clustering key
            {{"table_name", utf8_type}, {"range_start", utf8_type}, {"range_end", utf8_type}},
            // regular columns
            {
                {"mean_partition_size", long_type},
                {"partitions_count", long_type},
            },
            // static columns
            {},
            // regular column name type
            utf8_type,
            // comment
            "per-table primary range size estimates"
            );
        builder.set_gc_grace_seconds(0);
        builder.with_version(generate_schema_version(builder.uuid()));
        return builder.build(schema_builder::compact_storage::no);
    }();
    return size_estimates;
}

/*static*/ schema_ptr system_keyspace::large_partitions() {
    static thread_local auto large_partitions = [] {
        schema_builder builder(generate_legacy_id(NAME, LARGE_PARTITIONS), NAME, LARGE_PARTITIONS,
        // partition key
        {{"keyspace_name", utf8_type}, {"table_name", utf8_type}},
        // clustering key
        {
            {"sstable_name", utf8_type},
            {"partition_size", reversed_type_impl::get_instance(long_type)},
            {"partition_key", utf8_type}
        }, // CLUSTERING ORDER BY (partition_size DESC)
        // regular columns
        {
            {"rows", long_type},
            {"compaction_time", timestamp_type},
            {"range_tombstones", long_type},
            {"dead_rows", long_type}
        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "partitions larger than specified threshold"
        );
        builder.set_gc_grace_seconds(0);
        builder.with_version(generate_schema_version(builder.uuid()));
        // FIXME re-enable caching for this and the other two
        // system.large_* tables once
        // https://github.com/scylladb/scylla/issues/3288 is fixed
        builder.set_caching_options(caching_options::get_disabled_caching_options());
        return builder.build(schema_builder::compact_storage::no);
    }();
    return large_partitions;
}

schema_ptr system_keyspace::large_rows() {
    static thread_local auto large_rows = [] {
        auto id = generate_legacy_id(NAME, LARGE_ROWS);
        return schema_builder(NAME, LARGE_ROWS, std::optional(id))
                .with_column("keyspace_name", utf8_type, column_kind::partition_key)
                .with_column("table_name", utf8_type, column_kind::partition_key)
                .with_column("sstable_name", utf8_type, column_kind::clustering_key)
                // We want the large rows first, so use reversed_type_impl
                .with_column("row_size", reversed_type_impl::get_instance(long_type), column_kind::clustering_key)
                .with_column("partition_key", utf8_type, column_kind::clustering_key)
                .with_column("clustering_key", utf8_type, column_kind::clustering_key)
                .with_column("compaction_time", timestamp_type)
                .set_comment("rows larger than specified threshold")
                .with_version(generate_schema_version(id))
                .set_gc_grace_seconds(0)
                .set_caching_options(caching_options::get_disabled_caching_options())
                .build();
    }();
    return large_rows;
}

schema_ptr system_keyspace::large_cells() {
    constexpr uint16_t schema_version_offset = 1; // collection_elements
    static thread_local auto large_cells = [] {
        auto id = generate_legacy_id(NAME, LARGE_CELLS);
        return schema_builder(NAME, LARGE_CELLS, id)
                .with_column("keyspace_name", utf8_type, column_kind::partition_key)
                .with_column("table_name", utf8_type, column_kind::partition_key)
                .with_column("sstable_name", utf8_type, column_kind::clustering_key)
                // We want the larger cells first, so use reversed_type_impl
                .with_column("cell_size", reversed_type_impl::get_instance(long_type), column_kind::clustering_key)
                .with_column("partition_key", utf8_type, column_kind::clustering_key)
                .with_column("clustering_key", utf8_type, column_kind::clustering_key)
                .with_column("column_name", utf8_type, column_kind::clustering_key)
                // regular rows
                .with_column("collection_elements", long_type)
                .with_column("compaction_time", timestamp_type)
                .set_comment("cells larger than specified threshold")
                .with_version(generate_schema_version(id, schema_version_offset))
                .set_gc_grace_seconds(0)
                .set_caching_options(caching_options::get_disabled_caching_options())
                .build();
    }();
    return large_cells;
}

static constexpr auto schema_gc_grace = std::chrono::duration_cast<std::chrono::seconds>(days(7)).count();

/*static*/ schema_ptr system_keyspace::scylla_local() {
    static thread_local auto scylla_local = [] {
        schema_builder builder(generate_legacy_id(NAME, SCYLLA_LOCAL), NAME, SCYLLA_LOCAL,
        // partition key
        {{"key", utf8_type}},
        // clustering key
        {},
        // regular columns
        {
                {"value", utf8_type},
        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "Scylla specific information about the local node"
       );
       // scylla_local may store a replicated tombstone related to schema
       // (see `make_group0_schema_version_mutation`), so we use nonzero gc grace.
       builder.set_gc_grace_seconds(schema_gc_grace);
       builder.with_version(generate_schema_version(builder.uuid()));
       return builder.build(schema_builder::compact_storage::no);
    }();
    return scylla_local;
}

schema_ptr system_keyspace::v3::batches() {
    static thread_local auto schema = [] {
        schema_builder builder(generate_legacy_id(NAME, BATCHES), NAME, BATCHES,
        // partition key
        {{"id", timeuuid_type}},
        // clustering key
        {},
        // regular columns
        {{"mutations", list_type_impl::get_instance(bytes_type, true)}, {"version", int32_type}},
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "batches awaiting replay"
       );
       builder.set_gc_grace_seconds(0);
       // FIXME: the original Java code also had:
       //.copy(new LocalPartitioner(TimeUUIDType.instance))
       builder.set_gc_grace_seconds(0);
       builder.set_compaction_strategy(sstables::compaction_strategy_type::size_tiered);
       builder.set_compaction_strategy_options({{"min_threshold", "2"}});
       builder.with_version(generate_schema_version(builder.uuid()));
       return builder.build(schema_builder::compact_storage::no);
    }();
    return schema;
}

schema_ptr system_keyspace::v3::built_indexes() {
    // identical to ours, but ours otoh is a mix-in of the 3.x series cassandra one
    return db::system_keyspace::built_indexes();
}

schema_ptr system_keyspace::v3::local() {
    static thread_local auto schema = [] {
        schema_builder builder(generate_legacy_id(NAME, LOCAL), NAME, LOCAL,
        // partition key
        {{"key", utf8_type}},
        // clustering key
        {},
        // regular columns
        {
                {"bootstrapped", utf8_type},
                {"broadcast_address", inet_addr_type},
                {"cluster_name", utf8_type},
                {"cql_version", utf8_type},
                {"data_center", utf8_type},
                {"gossip_generation", int32_type},
                {"host_id", uuid_type},
                {"listen_address", inet_addr_type},
                {"native_protocol_version", utf8_type},
                {"partitioner", utf8_type},
                {"rack", utf8_type},
                {"release_version", utf8_type},
                {"rpc_address", inet_addr_type},
                {"schema_version", uuid_type},
                {"thrift_version", utf8_type},
                {"tokens", set_type_impl::get_instance(utf8_type, true)},
                {"truncated_at", map_type_impl::get_instance(uuid_type, bytes_type, true)},
        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "information about the local node"
       );
       builder.set_gc_grace_seconds(0);
       builder.with_version(generate_schema_version(builder.uuid()));
       return builder.build(schema_builder::compact_storage::no);
    }();
    return schema;
}

schema_ptr system_keyspace::v3::truncated() {
    static thread_local auto local = [] {
        schema_builder builder(generate_legacy_id(NAME, TRUNCATED), NAME, TRUNCATED,
        // partition key
        {{"table_uuid", uuid_type}},
        // clustering key
        {{"shard", int32_type}},
        // regular columns
        {
                {"position", int32_type},
                {"segment_id", long_type}
        },
        // static columns
        {
                {"truncated_at", timestamp_type},
        },
        // regular column name type
        utf8_type,
        // comment
        "information about table truncation"
       );
       builder.set_gc_grace_seconds(0);
       builder.with_version(generate_schema_version(builder.uuid()));
       return builder.build(schema_builder::compact_storage::no);
    }();
    return local;
}

thread_local data_type replay_position_type = tuple_type_impl::get_instance({long_type, int32_type});

schema_ptr system_keyspace::v3::commitlog_cleanups() {
    static thread_local auto local = [] {
        schema_builder builder(generate_legacy_id(NAME, COMMITLOG_CLEANUPS), NAME, COMMITLOG_CLEANUPS,
        // partition key
        {{"shard", int32_type}},
        // clustering key
        {
            {"position", replay_position_type},
            {"table_uuid", uuid_type},
            {"start_token_exclusive", long_type},
            {"end_token_inclusive", long_type},
        },
        // regular columns
        {},
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "information about cleanups, for filtering commitlog replay"
       );
       builder.with_version(generate_schema_version(builder.uuid()));
       return builder.build(schema_builder::compact_storage::no);
    }();
    return local;
}

schema_ptr system_keyspace::v3::peers() {
    // identical
    return db::system_keyspace::peers();
}

schema_ptr system_keyspace::v3::peer_events() {
    // identical
    return db::system_keyspace::peer_events();
}

schema_ptr system_keyspace::v3::range_xfers() {
    // identical
    return db::system_keyspace::range_xfers();
}

schema_ptr system_keyspace::v3::compaction_history() {
    // identical
    return db::system_keyspace::compaction_history();
}

schema_ptr system_keyspace::v3::sstable_activity() {
    // identical
    return db::system_keyspace::sstable_activity();
}

schema_ptr system_keyspace::v3::size_estimates() {
    // identical
    return db::system_keyspace::size_estimates();
}

schema_ptr system_keyspace::v3::large_partitions() {
    // identical
    return db::system_keyspace::large_partitions();
}

schema_ptr system_keyspace::v3::scylla_local() {
    // identical
    return db::system_keyspace::scylla_local();
}

schema_ptr system_keyspace::v3::available_ranges() {
    static thread_local auto schema = [] {
        schema_builder builder(generate_legacy_id(NAME, AVAILABLE_RANGES), NAME, AVAILABLE_RANGES,
        // partition key
        {{"keyspace_name", utf8_type}},
        // clustering key
        {},
        // regular columns
        {{"ranges", set_type_impl::get_instance(bytes_type, true)}},
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "available keyspace/ranges during bootstrap/replace that are ready to be served"
       );
       builder.set_gc_grace_seconds(0);
       builder.with_version(generate_schema_version(builder.uuid()));
       return builder.build();
    }();
    return schema;
}

schema_ptr system_keyspace::v3::views_builds_in_progress() {
    static thread_local auto schema = [] {
        schema_builder builder(generate_legacy_id(NAME, VIEWS_BUILDS_IN_PROGRESS), NAME, VIEWS_BUILDS_IN_PROGRESS,
        // partition key
        {{"keyspace_name", utf8_type}},
        // clustering key
        {{"view_name", utf8_type}},
        // regular columns
        {{"last_token", utf8_type}, {"generation_number", int32_type}},
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "views builds current progress"
       );
       builder.set_gc_grace_seconds(0);
       builder.with_version(generate_schema_version(builder.uuid()));
       return builder.build();
    }();
    return schema;
}

schema_ptr system_keyspace::v3::built_views() {
    static thread_local auto schema = [] {
        schema_builder builder(generate_legacy_id(NAME, BUILT_VIEWS), NAME, BUILT_VIEWS,
        // partition key
        {{"keyspace_name", utf8_type}},
        // clustering key
        {{"view_name", utf8_type}},
        // regular columns
        {},
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "built views"
       );
       builder.set_gc_grace_seconds(0);
       builder.with_version(generate_schema_version(builder.uuid()));
       return builder.build();
    }();
    return schema;
}

schema_ptr system_keyspace::v3::scylla_views_builds_in_progress() {
    static thread_local auto schema = [] {
        auto id = generate_legacy_id(NAME, SCYLLA_VIEWS_BUILDS_IN_PROGRESS);
        return schema_builder(NAME, SCYLLA_VIEWS_BUILDS_IN_PROGRESS, std::make_optional(id))
                .with_column("keyspace_name", utf8_type, column_kind::partition_key)
                .with_column("view_name", utf8_type, column_kind::clustering_key)
                .with_column("cpu_id", int32_type, column_kind::clustering_key)
                .with_column("next_token", utf8_type)
                .with_column("generation_number", int32_type)
                .with_column("first_token", utf8_type)
                .with_version(generate_schema_version(id))
                .build();
    }();
    return schema;
}

/*static*/ schema_ptr system_keyspace::v3::cdc_local() {
    static thread_local auto cdc_local = [] {
        schema_builder builder(generate_legacy_id(NAME, CDC_LOCAL), NAME, CDC_LOCAL,
        // partition key
        {{"key", utf8_type}},
        // clustering key
        {},
        // regular columns
        {
                /* Every node announces the identifier of the newest known CDC generation to other nodes.
                 * The identifier consists of two things: a timestamp (which is the generation's timestamp,
                 * denoting the time point from which it starts operating) and an UUID (randomly generated
                 * when the generation is created).
                 * This identifier is persisted here and restored on node restart.
                 *
                 * Some identifiers - identifying generations created in older clusters - have only the timestamp.
                 * For these the uuid column is empty.
                 */
                {"streams_timestamp", timestamp_type},
                {"uuid", uuid_type},

        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "CDC-specific information that the local node stores"
       );
       builder.set_gc_grace_seconds(0);
       builder.with_version(generate_schema_version(builder.uuid()));
       return builder.build(schema_builder::compact_storage::no);
    }();
    return cdc_local;
}

schema_ptr system_keyspace::group0_history() {
    static thread_local auto schema = [] {
        auto id = generate_legacy_id(NAME, GROUP0_HISTORY);
        return schema_builder(NAME, GROUP0_HISTORY, id)
            // this is a single-partition table with key 'history'
            .with_column("key", utf8_type, column_kind::partition_key)
            // group0 state timeuuid, descending order
            .with_column("state_id", reversed_type_impl::get_instance(timeuuid_type), column_kind::clustering_key)
            // human-readable description of the change
            .with_column("description", utf8_type)

            .set_comment("History of Raft group 0 state changes")
            .with_version(generate_schema_version(id))
            .build();
    }();
    return schema;
}

schema_ptr system_keyspace::discovery() {
    static thread_local auto schema = [] {
        auto id = generate_legacy_id(NAME, DISCOVERY);
        return schema_builder(NAME, DISCOVERY, id)
            // This is a single-partition table with key 'peers'
            .with_column("key", utf8_type, column_kind::partition_key)
            // Peer ip address
            .with_column("ip_addr", inet_addr_type, column_kind::clustering_key)
            // The ID of the group 0 server on that peer.
            // May be unknown during discovery, then it's set to UUID 0.
            .with_column("raft_server_id", uuid_type)
            .set_comment("State of cluster discovery algorithm: the set of discovered peers")
            .with_version(generate_schema_version(id))
            .build();
    }();
    return schema;
}

schema_ptr system_keyspace::broadcast_kv_store() {
    static thread_local auto schema = [] {
        auto id = generate_legacy_id(NAME, BROADCAST_KV_STORE);
        return schema_builder(NAME, BROADCAST_KV_STORE, id)
            .with_column("key", utf8_type, column_kind::partition_key)
            .with_column("value", utf8_type)
            .with_version(generate_schema_version(id))
            .build();
    }();
    return schema;
}

schema_ptr system_keyspace::sstables_registry() {
    static thread_local auto schema = [] {
        auto id = generate_legacy_id(NAME, SSTABLES_REGISTRY);
        return schema_builder(NAME, SSTABLES_REGISTRY, id)
            .with_column("location", utf8_type, column_kind::partition_key)
            .with_column("generation", timeuuid_type, column_kind::clustering_key)
            .with_column("status", utf8_type)
            .with_column("state", utf8_type)
            .with_column("version", utf8_type)
            .with_column("format", utf8_type)
            .set_comment("SSTables ownership table")
            .with_version(generate_schema_version(id))
            .build();
    }();
    return schema;
}

schema_ptr system_keyspace::tablets() {
    static thread_local auto schema = replica::make_tablets_schema();
    return schema;
}

schema_ptr system_keyspace::service_levels_v2() {
    static thread_local auto schema = [] {
        auto id = generate_legacy_id(NAME, SERVICE_LEVELS_V2);
        return schema_builder(NAME, SERVICE_LEVELS_V2, id)
                .with_column("service_level", utf8_type, column_kind::partition_key)
                .with_column("timeout", duration_type)
                .with_column("workload_type", utf8_type)
                .with_version(db::system_keyspace::generate_schema_version(id))
                .build();
    }();
    return schema;
}

schema_ptr system_keyspace::roles() {
    static thread_local auto schema = [] {
        schema_builder builder(generate_legacy_id(NAME, ROLES), NAME, ROLES,
        // partition key
        {{"role", utf8_type}},
        // clustering key
        {},
        // regular columns
        {
            {"can_login", boolean_type},
            {"is_superuser", boolean_type},
            {"member_of", set_type_impl::get_instance(utf8_type, true)},
            {"salted_hash", utf8_type}
        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "roles for authentication and RBAC"
        );
        builder.with_version(system_keyspace::generate_schema_version(builder.uuid()));
        return builder.build();
    }();
    return schema;
}

schema_ptr system_keyspace::role_members() {
    static thread_local auto schema = [] {
        schema_builder builder(generate_legacy_id(NAME, ROLE_MEMBERS), NAME, ROLE_MEMBERS,
        // partition key
        {{"role", utf8_type}},
        // clustering key
        {{"member", utf8_type}},
        // regular columns
        {},
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "joins users and their granted roles in RBAC"
        );
        builder.with_version(system_keyspace::generate_schema_version(builder.uuid()));
        return builder.build();
    }();
    return schema;
}

schema_ptr system_keyspace::role_attributes() {
    static thread_local auto schema = [] {
        schema_builder builder(generate_legacy_id(NAME, ROLE_ATTRIBUTES), NAME, ROLE_ATTRIBUTES,
        // partition key
        {{"role", utf8_type}},
        // clustering key
        {{"name", utf8_type}},
        // regular columns
        {
            {"value", utf8_type}
        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "role permissions in RBAC"
        );
        builder.with_version(system_keyspace::generate_schema_version(builder.uuid()));
        return builder.build();
    }();
    return schema;
}

schema_ptr system_keyspace::role_permissions() {
    static thread_local auto schema = [] {
        schema_builder builder(generate_legacy_id(NAME, ROLE_PERMISSIONS), NAME, ROLE_PERMISSIONS,
        // partition key
        {{"role", utf8_type}},
        // clustering key
        {{"resource", utf8_type}},
        // regular columns
        {
            {"permissions", set_type_impl::get_instance(utf8_type, true)}
        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "role permissions for CassandraAuthorizer"
        );
        builder.with_version(system_keyspace::generate_schema_version(builder.uuid()));
        return builder.build();
    }();
    return schema;
}

schema_ptr system_keyspace::legacy::hints() {
    static thread_local auto schema = [] {
        schema_builder builder(generate_legacy_id(NAME, HINTS), NAME, HINTS,
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
        "*DEPRECATED* hints awaiting delivery"
       );
       builder.set_gc_grace_seconds(0);
       builder.set_compaction_strategy(sstables::compaction_strategy_type::size_tiered);
       builder.set_compaction_strategy_options({{"enabled", "false"}});
       builder.with_version(generate_schema_version(builder.uuid()));
       builder.with(schema_builder::compact_storage::yes);
       return builder.build();
    }();
    return schema;
}

schema_ptr system_keyspace::legacy::batchlog() {
    static thread_local auto schema = [] {
        schema_builder builder(generate_legacy_id(NAME, BATCHLOG), NAME, BATCHLOG,
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
        "*DEPRECATED* batchlog entries"
       );
       builder.set_gc_grace_seconds(0);
       builder.set_compaction_strategy(sstables::compaction_strategy_type::size_tiered);
       builder.set_compaction_strategy_options({{"min_threshold", "2"}});
       builder.with(schema_builder::compact_storage::no);
       builder.with_version(generate_schema_version(builder.uuid()));
       return builder.build();
    }();
    return schema;
}

schema_ptr system_keyspace::legacy::keyspaces() {
    static thread_local auto schema = [] {
        schema_builder builder(generate_legacy_id(NAME, KEYSPACES), NAME, KEYSPACES,
        // partition key
        {{"keyspace_name", utf8_type}},
        // clustering key
        {},
        // regular columns
        {
         {"durable_writes", boolean_type},
         {"strategy_class", utf8_type},
         {"strategy_options", utf8_type}
        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "*DEPRECATED* keyspace definitions"
       );
       builder.set_gc_grace_seconds(schema_gc_grace);
       builder.with(schema_builder::compact_storage::yes);
       builder.with_version(generate_schema_version(builder.uuid()));
       return builder.build();
    }();
    return schema;
}

schema_ptr system_keyspace::legacy::column_families() {
    static thread_local auto schema = [] {
        schema_builder builder(generate_legacy_id(NAME, COLUMNFAMILIES), NAME, COLUMNFAMILIES,
        // partition key
        {{"keyspace_name", utf8_type}},
        // clustering key
        {{"columnfamily_name", utf8_type}},
        // regular columns
        {
         {"bloom_filter_fp_chance", double_type},
         {"caching", utf8_type},
         {"cf_id", uuid_type},
         {"comment", utf8_type},
         {"compaction_strategy_class", utf8_type},
         {"compaction_strategy_options", utf8_type},
         {"comparator", utf8_type},
         {"compression_parameters", utf8_type},
         {"default_time_to_live", int32_type},
         {"default_validator", utf8_type},
         {"dropped_columns",  map_type_impl::get_instance(utf8_type, long_type, true)},
         {"gc_grace_seconds", int32_type},
         {"is_dense", boolean_type},
         {"key_validator", utf8_type},
         {"max_compaction_threshold", int32_type},
         {"max_index_interval", int32_type},
         {"memtable_flush_period_in_ms", int32_type},
         {"min_compaction_threshold", int32_type},
         {"min_index_interval", int32_type},
         {"speculative_retry", utf8_type},
         {"subcomparator", utf8_type},
         {"type", utf8_type},
         // The following 4 columns are only present up until 2.1.8 tables
         {"key_aliases", utf8_type},
         {"value_alias", utf8_type},
         {"column_aliases", utf8_type},
         {"index_interval", int32_type},},
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "*DEPRECATED* table definitions"
       );
       builder.set_gc_grace_seconds(schema_gc_grace);
       builder.with(schema_builder::compact_storage::no);
       builder.with_version(generate_schema_version(builder.uuid()));
       return builder.build();
    }();
    return schema;
}

schema_ptr system_keyspace::legacy::columns() {
    static thread_local auto schema = [] {
        schema_builder builder(generate_legacy_id(NAME, COLUMNS), NAME, COLUMNS,
        // partition key
        {{"keyspace_name", utf8_type}},
        // clustering key
        {{"columnfamily_name", utf8_type}, {"column_name", utf8_type}},
        // regular columns
        {
            {"component_index", int32_type},
            {"index_name", utf8_type},
            {"index_options", utf8_type},
            {"index_type", utf8_type},
            {"type", utf8_type},
            {"validator", utf8_type},
        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "column definitions"
        );
        builder.set_gc_grace_seconds(schema_gc_grace);
        builder.with(schema_builder::compact_storage::no);
        builder.with_version(generate_schema_version(builder.uuid()));
        return builder.build();
    }();
    return schema;
}

schema_ptr system_keyspace::legacy::triggers() {
    static thread_local auto schema = [] {
        schema_builder builder(generate_legacy_id(NAME, TRIGGERS), NAME, TRIGGERS,
        // partition key
        {{"keyspace_name", utf8_type}},
        // clustering key
        {{"columnfamily_name", utf8_type}, {"trigger_name", utf8_type}},
        // regular columns
        {
            {"trigger_options",  map_type_impl::get_instance(utf8_type, utf8_type, true)},
        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "trigger definitions"
        );
        builder.set_gc_grace_seconds(schema_gc_grace);
        builder.with(schema_builder::compact_storage::no);
        builder.with_version(generate_schema_version(builder.uuid()));
        return builder.build();
    }();
    return schema;
}

schema_ptr system_keyspace::legacy::usertypes() {
    static thread_local auto schema = [] {
        schema_builder builder(generate_legacy_id(NAME, USERTYPES), NAME, USERTYPES,
        // partition key
        {{"keyspace_name", utf8_type}},
        // clustering key
        {{"type_name", utf8_type}},
        // regular columns
        {
            {"field_names", list_type_impl::get_instance(utf8_type, true)},
            {"field_types", list_type_impl::get_instance(utf8_type, true)},
        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "user defined type definitions"
        );
        builder.set_gc_grace_seconds(schema_gc_grace);
        builder.with(schema_builder::compact_storage::no);
        builder.with_version(generate_schema_version(builder.uuid()));
        return builder.build();
    }();
    return schema;
}

schema_ptr system_keyspace::legacy::functions() {
    /**
     * Note: we have our own "legacy" version of this table (in schema_tables),
     * but it is (afaik) not used, and differs slightly from the origin one.
     * This is based on the origin schema, since we're more likely to encounter
     * installations of that to migrate, rather than our own (if we dont use the table).
     */
    static thread_local auto schema = [] {
        schema_builder builder(generate_legacy_id(NAME, FUNCTIONS), NAME, FUNCTIONS,
        // partition key
        {{"keyspace_name", utf8_type}},
        // clustering key
        {{"function_name", utf8_type},{"signature", list_type_impl::get_instance(utf8_type, false)}},
        // regular columns
        {
            {"argument_names", list_type_impl::get_instance(utf8_type, true)},
            {"argument_types", list_type_impl::get_instance(utf8_type, true)},
            {"body", utf8_type},
            {"language", utf8_type},
            {"return_type", utf8_type},
            {"called_on_null_input", boolean_type},
        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "*DEPRECATED* user defined type definitions"
        );
        builder.set_gc_grace_seconds(schema_gc_grace);
        builder.with(schema_builder::compact_storage::no);
        builder.with_version(generate_schema_version(builder.uuid()));
        return builder.build();
    }();
    return schema;
}

schema_ptr system_keyspace::legacy::aggregates() {
    static thread_local auto schema = [] {
        schema_builder builder(generate_legacy_id(NAME, AGGREGATES), NAME, AGGREGATES,
        // partition key
        {{"keyspace_name", utf8_type}},
        // clustering key
        {{"aggregate_name", utf8_type},{"signature", list_type_impl::get_instance(utf8_type, false)}},
        // regular columns
        {
            {"argument_types", list_type_impl::get_instance(utf8_type, true)},
            {"final_func", utf8_type},
            {"initcond", bytes_type},
            {"return_type", utf8_type},
            {"state_func", utf8_type},
            {"state_type", utf8_type},
        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "*DEPRECATED* user defined aggregate definition"
        );
        builder.set_gc_grace_seconds(schema_gc_grace);
        builder.with(schema_builder::compact_storage::no);
        builder.with_version(generate_schema_version(builder.uuid()));
        return builder.build();
    }();
    return schema;
}

future<system_keyspace::local_info> system_keyspace::load_local_info() {
    auto msg = co_await execute_cql(format("SELECT host_id, cluster_name FROM system.{} WHERE key=?", LOCAL), sstring(LOCAL));

    local_info ret;
    if (!msg->empty()) {
        auto& row = msg->one();
        if (row.has("host_id")) {
            ret.host_id = locator::host_id(row.get_as<utils::UUID>("host_id"));
        }
        if (row.has("cluster_name")) {
            ret.cluster_name = row.get_as<sstring>("cluster_name");
        }
    }

    co_return ret;
}

future<> system_keyspace::save_local_info(local_info sysinfo, locator::endpoint_dc_rack location, gms::inet_address broadcast_address, gms::inet_address broadcast_rpc_address) {
    auto& cfg = _db.get_config();
    sstring req = fmt::format("INSERT INTO system.{} (key, host_id, cluster_name, release_version, cql_version, native_protocol_version, data_center, rack, partitioner, rpc_address, broadcast_address, listen_address) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                    , db::system_keyspace::LOCAL);

    return execute_cql(req, sstring(db::system_keyspace::LOCAL),
                            sysinfo.host_id.uuid(),
                            sysinfo.cluster_name,
                            version::release(),
                            cql3::query_processor::CQL_VERSION,
                            to_sstring(unsigned(cql_serialization_format::latest().protocol_version())),
                            location.dc,
                            location.rack,
                            sstring(cfg.partitioner()),
                            broadcast_rpc_address,
                            broadcast_address,
                            sysinfo.listen_address.addr()
    ).discard_result();
}

future<> system_keyspace::save_local_supported_features(const std::set<std::string_view>& feats) {
    static const auto req = format("INSERT INTO system.{} (key, supported_features) VALUES (?, ?)", LOCAL);
    return execute_cql(req,
        sstring(db::system_keyspace::LOCAL),
        fmt::to_string(fmt::join(feats, ","))).discard_result();
}

// The cache must be distributed, because the values themselves may not update atomically, so a shard reading that
// is different than the one that wrote, may see a corrupted value. invoke_on_all will be used to guarantee that all
// updates are propagated correctly.
struct local_cache {
    system_keyspace::bootstrap_state _state;
};

future<> system_keyspace::peers_table_read_fixup() {
    SCYLLA_ASSERT(this_shard_id() == 0);
    if (_peers_table_read_fixup_done) {
        co_return;
    }
    _peers_table_read_fixup_done = true;

    const auto cql = format("SELECT peer, host_id, WRITETIME(host_id) as ts from system.{}", PEERS);
    std::unordered_map<utils::UUID, std::pair<net::inet_address, int64_t>> map{};
    const auto cql_result = co_await execute_cql(cql);
    for (const auto& row : *cql_result) {
        const auto peer = row.get_as<net::inet_address>("peer");
        if (!row.has("host_id")) {
            slogger.error("Peer {} has no host_id in system.{}, the record is broken, removing it",
                peer, system_keyspace::PEERS);
            co_await remove_endpoint(gms::inet_address{peer});
            continue;
        }
        const auto host_id = row.get_as<utils::UUID>("host_id");
        const auto ts = row.get_as<int64_t>("ts");
        const auto it = map.find(host_id);
        if (it == map.end()) {
            map.insert({host_id, {peer, ts}});
            continue;
        }
        if (it->second.second >= ts) {
            slogger.error("Peer {} with host_id {} has newer IP {} in system.{}, the record is stale, removing it",
                peer, host_id, it->second.first, system_keyspace::PEERS);
            co_await remove_endpoint(gms::inet_address{peer});
        } else {
            slogger.error("Peer {} with host_id {} has newer IP {} in system.{}, the record is stale, removing it",
                it->second.first, host_id, peer, system_keyspace::PEERS);
            co_await remove_endpoint(gms::inet_address{it->second.first});
            it->second = {peer, ts};
        }
    }
}

future<> system_keyspace::build_bootstrap_info() {
    sstring req = format("SELECT bootstrapped FROM system.{} WHERE key = ? ", LOCAL);
    return execute_cql(req, sstring(LOCAL)).then([this] (auto msg) {
        static auto state_map = std::unordered_map<sstring, bootstrap_state>({
            { "NEEDS_BOOTSTRAP", bootstrap_state::NEEDS_BOOTSTRAP },
            { "COMPLETED", bootstrap_state::COMPLETED },
            { "IN_PROGRESS", bootstrap_state::IN_PROGRESS },
            { "DECOMMISSIONED", bootstrap_state::DECOMMISSIONED }
        });
        bootstrap_state state = bootstrap_state::NEEDS_BOOTSTRAP;

        if (!msg->empty() && msg->one().has("bootstrapped")) {
            state = state_map.at(msg->one().template get_as<sstring>("bootstrapped"));
        }
        return container().invoke_on_all([state] (auto& sys_ks) {
            sys_ks._cache->_state = state;
        });
    });
}

}

namespace db {

// Read system.truncate table and cache last truncation time in `table` object for each table on every shard
future<std::unordered_map<table_id, db_clock::time_point>> system_keyspace::load_truncation_times() {
    std::unordered_map<table_id, db_clock::time_point> result;
    if (!_db.get_config().ignore_truncation_record.is_set()) {
        sstring req = format("SELECT DISTINCT table_uuid, truncated_at from system.{}", TRUNCATED);
        auto result_set = co_await execute_cql(req);
        for (const auto& row: *result_set) {
            const auto table_uuid = table_id(row.get_as<utils::UUID>("table_uuid"));
            const auto ts = row.get_as<db_clock::time_point>("truncated_at");
            result[table_uuid] = ts;
        }
    }
    co_return result;
}

future<> system_keyspace::drop_truncation_rp_records() {
    sstring req = format("SELECT table_uuid, shard, segment_id from system.{}", TRUNCATED);
    auto rs = co_await execute_cql(req);

    bool any = false;
    co_await coroutine::parallel_for_each(rs->begin(), rs->end(), [&] (const cql3::untyped_result_set_row& row) -> future<> {
        auto table_uuid = table_id(row.get_as<utils::UUID>("table_uuid"));
        auto shard = row.get_as<int32_t>("shard");
        auto segment_id = row.get_as<int64_t>("segment_id");

        if (segment_id != 0) {
            any = true;
            sstring req = format("UPDATE system.{} SET segment_id = 0, position = 0 WHERE table_uuid = {} AND shard = {}", TRUNCATED, table_uuid, shard);
            co_await execute_cql(req);
        }
    });
    if (any) {
        co_await force_blocking_flush(TRUNCATED);
    }
}

future<> system_keyspace::save_truncation_record(const replica::column_family& cf, db_clock::time_point truncated_at, db::replay_position rp) {
    sstring req = format("INSERT INTO system.{} (table_uuid, shard, position, segment_id, truncated_at) VALUES(?,?,?,?,?)", TRUNCATED);
    co_await _qp.execute_internal(req, {cf.schema()->id().uuid(), int32_t(rp.shard_id()), int32_t(rp.pos), int64_t(rp.base_id()), truncated_at}, cql3::query_processor::cache_internal::yes);
    // Flush the table so that the value is available on boot before commitlog replay.
    // Commit log replay depends on truncation records to determine the minimum replay position.
    co_await force_blocking_flush(TRUNCATED);
}

future<replay_positions> system_keyspace::get_truncated_positions(table_id cf_id) {
    replay_positions result;
    if (_db.get_config().ignore_truncation_record.is_set()) {
        co_return result;
    }
    const auto req = format("SELECT * from system.{} WHERE table_uuid = ?", TRUNCATED);
    auto result_set = co_await execute_cql(req, {cf_id.uuid()});
    result.reserve(result_set->size());
    for (const auto& row: *result_set) {
        result.emplace_back(row.get_as<int32_t>("shard"),
            row.get_as<int64_t>("segment_id"),
            row.get_as<int32_t>("position"));
    }
    co_return result;
}

future<> system_keyspace::drop_all_commitlog_cleanup_records() {
    // In this function we want to clear the entire COMMITLOG_CLEANUPS table.
    //
    // We can't use TRUNCATE, since it's a system table. So we have to delete each partition.
    //
    // The partition key is the shard number. If we knew how many shards there were in
    // the previous boot cycle, we could just issue DELETEs for 1..N.
    //
    // But we don't know that here, so we have to SELECT the set of partition keys,
    // and issue DELETEs on that.
    sstring req = format("SELECT shard from system.{}", COMMITLOG_CLEANUPS);
    auto rs = co_await execute_cql(req);

    co_await coroutine::parallel_for_each(rs->begin(), rs->end(), [&] (const cql3::untyped_result_set_row& row) -> future<> {
        auto shard = row.get_as<int32_t>("shard");
        co_await execute_cql(format("DELETE FROM system.{} WHERE shard = {}", COMMITLOG_CLEANUPS, shard));
    });
}

future<> system_keyspace::drop_old_commitlog_cleanup_records(replay_position min_position) {
    auto pos = make_tuple_value(replay_position_type, tuple_type_impl::native_type({
        int64_t(min_position.base_id()),
        int32_t(min_position.pos)
    }));
    sstring req = format("DELETE FROM system.{} WHERE shard = ? AND position < ?", COMMITLOG_CLEANUPS);
    co_await _qp.execute_internal(req, {int32_t(min_position.shard_id()), pos}, cql3::query_processor::cache_internal::yes);
}

future<> system_keyspace::save_commitlog_cleanup_record(table_id table, dht::token_range tr, db::replay_position rp) {
    auto [start_token_exclusive, end_token_inclusive] = canonical_token_range(tr);
    auto pos = make_tuple_value(replay_position_type, tuple_type_impl::native_type({int64_t(rp.base_id()), int32_t(rp.pos)}));
    sstring req = format("INSERT INTO system.{} (shard, position, table_uuid, start_token_exclusive, end_token_inclusive) VALUES(?,?,?,?,?)", COMMITLOG_CLEANUPS);
    co_await _qp.execute_internal(req, {int32_t(rp.shard_id()), pos, table.uuid(), start_token_exclusive, end_token_inclusive}, cql3::query_processor::cache_internal::yes);
}

std::pair<int64_t, int64_t> system_keyspace::canonical_token_range(dht::token_range tr) {
    // closed_full_range represents a full interval using only regular token values. (No infinities).
    auto closed_full_range = dht::token_range::make({dht::first_token()}, dht::token::from_int64(std::numeric_limits<int64_t>::max()));
    // By intersecting with closed_full_range we get rid of all the crazy infinities that can be represented by dht::token_range.
    auto finite_tr = tr.intersection(closed_full_range, dht::token_comparator());
    if (!finite_tr) {
        // If we got here, the interval was degenerate, with only infinities.
        // So we return an empty (x, x] interval.
        // We arbitrarily choose `min` as the `x`.
        //
        // Note: (x, x] is interpreted by the interval classes from `interval.hh` as the
        // *full* (wrapping) interval, not an empty interval, so be careful about this if you ever
        // want to implement a conversion from the output of this function back to `dht::token_range`.
        // Nota bene, this `interval.hh` convention means that there is no way to represent an empty
        // interval, so it is objectively bad.
        //
        // Note: (x, x] is interpreted by boost::icl as an empty interval, so it doesn't need any special
        // treatment before use in `boost::icl::interval_map`.
        return {std::numeric_limits<int64_t>::min(), std::numeric_limits<int64_t>::min()};
    }
    // After getting rid of possible infinities, we only have to adjust the openness of bounds.
    int64_t start_token_exclusive = dht::token::to_int64(finite_tr->start().value().value());
    if (finite_tr->start()->is_inclusive()) {
        start_token_exclusive -= 1;
    }
    int64_t end_token_inclusive = dht::token::to_int64(finite_tr->end().value().value());
    if (!finite_tr->end()->is_inclusive()) {
        end_token_inclusive -= 1;
    }
    return {start_token_exclusive, end_token_inclusive};
}

size_t system_keyspace::commitlog_cleanup_map_hash::operator()(const std::pair<table_id, int32_t>& p) const {
    size_t seed = 0;
    boost::hash_combine(seed, std::hash<utils::UUID>()(p.first.uuid()));
    boost::hash_combine(seed, std::hash<int32_t>()(p.second));
    return seed;
}

struct system_keyspace::commitlog_cleanup_local_map::impl {
    boost::icl::interval_map<
        int64_t,
        db::replay_position,
        boost::icl::partial_absorber,
        std::less,
        boost::icl::inplace_max,
        boost::icl::inter_section,
        boost::icl::left_open_interval<int64_t>
    > _map;
};

system_keyspace::commitlog_cleanup_local_map::~commitlog_cleanup_local_map() {
}
system_keyspace::commitlog_cleanup_local_map::commitlog_cleanup_local_map()
    : _pimpl(std::make_unique<impl>())
{}
std::optional<db::replay_position> system_keyspace::commitlog_cleanup_local_map::get(int64_t token) const {
    if (auto it = _pimpl->_map.find(token); it != _pimpl->_map.end()) {
        return it->second;
    }
    return std::nullopt;
}

future<system_keyspace::commitlog_cleanup_map> system_keyspace::get_commitlog_cleanup_records() {
    commitlog_cleanup_map ret;
    const auto req = format("SELECT * from system.{}", COMMITLOG_CLEANUPS);
    auto result_set = co_await execute_cql(req);
    for (const auto& row: *result_set) {
        auto table = table_id(row.get_as<utils::UUID>("table_uuid"));
        auto shard = row.get_as<int32_t>("shard");
        auto start_token_exclusive = row.get_as<int64_t>("start_token_exclusive");
        auto end_token_inclusive = row.get_as<int64_t>("end_token_inclusive");
        auto pos_tuple = value_cast<tuple_type_impl::native_type>(replay_position_type->deserialize(row.get_view("position")));
        auto rp = db::replay_position(
            shard,
            value_cast<int64_t>(pos_tuple[0]),
            value_cast<int32_t>(pos_tuple[1])
        );
        auto& inner_map = ret.try_emplace(std::make_pair(table, shard)).first->second;
        inner_map._pimpl->_map += std::make_pair(boost::icl::left_open_interval<int64_t>(start_token_exclusive, end_token_inclusive), rp);
    }
    co_return ret;
}

static set_type_impl::native_type deserialize_set_column(const schema& s, const cql3::untyped_result_set_row& row, const char* name) {
    auto blob = row.get_blob(name);
    auto cdef = s.get_column_definition(name);
    auto deserialized = cdef->type->deserialize(blob);
    return value_cast<set_type_impl::native_type>(deserialized);
}

static set_type_impl::native_type prepare_tokens(const std::unordered_set<dht::token>& tokens) {
    set_type_impl::native_type tset;
    for (auto& t: tokens) {
        tset.push_back(t.to_sstring());
    }
    return tset;
}

std::unordered_set<dht::token> decode_tokens(const set_type_impl::native_type& tokens) {
    std::unordered_set<dht::token> tset;
    for (auto& t: tokens) {
        auto str = value_cast<sstring>(t);
        SCYLLA_ASSERT(str == dht::token::from_sstring(str).to_sstring());
        tset.insert(dht::token::from_sstring(str));
    }
    return tset;
}

static std::unordered_set<raft::server_id> decode_nodes_ids(const set_type_impl::native_type& nodes_ids) {
    std::unordered_set<raft::server_id> ids_set;
    for (auto& id: nodes_ids) {
        auto uuid = value_cast<utils::UUID>(id);
        ids_set.insert(raft::server_id{uuid});
    }
    return ids_set;
}

static cdc::generation_id_v2 decode_cdc_generation_id(const data_value& gen_id) {
    auto native = value_cast<tuple_type_impl::native_type>(gen_id);
    auto ts = value_cast<db_clock::time_point>(native[0]);
    auto id = value_cast<utils::UUID>(native[1]);
    return cdc::generation_id_v2{ts, id};
}

static std::vector<cdc::generation_id_v2> decode_cdc_generations_ids(const set_type_impl::native_type& gen_ids) {
    std::vector<cdc::generation_id_v2> gen_ids_list;
    for (auto& gen_id: gen_ids) {
        gen_ids_list.push_back(decode_cdc_generation_id(gen_id));
    }
    return gen_ids_list;
}

future<std::unordered_map<gms::inet_address, std::unordered_set<dht::token>>> system_keyspace::load_tokens() {
    co_await peers_table_read_fixup();

    const sstring req = format("SELECT peer, tokens FROM system.{}", PEERS);
    std::unordered_map<gms::inet_address, std::unordered_set<dht::token>> ret;
    const auto cql_result = co_await execute_cql(req);
    for (const auto& row : *cql_result) {
        if (row.has("tokens")) {
            ret.emplace(gms::inet_address(row.get_as<net::inet_address>("peer")),
                decode_tokens(deserialize_set_column(*peers(), row, "tokens")));
        }
    }
    co_return ret;
}

future<std::unordered_map<gms::inet_address, locator::host_id>> system_keyspace::load_host_ids() {
    co_await peers_table_read_fixup();

    const sstring req = format("SELECT peer, host_id FROM system.{}", PEERS);
    std::unordered_map<gms::inet_address, locator::host_id> ret;
    const auto cql_result = co_await execute_cql(req);
    for (const auto& row : *cql_result) {
        ret.emplace(gms::inet_address(row.get_as<net::inet_address>("peer")),
            locator::host_id(row.get_as<utils::UUID>("host_id")));
    }
    co_return ret;
}

future<std::unordered_map<locator::host_id, gms::loaded_endpoint_state>> system_keyspace::load_endpoint_state() {
    co_await peers_table_read_fixup();

    const auto msg = co_await execute_cql(format("SELECT peer, host_id, tokens, data_center, rack from system.{}", PEERS));

    std::unordered_map<locator::host_id, gms::loaded_endpoint_state> ret;
    for (const auto& row : *msg) {
        gms::loaded_endpoint_state st;
        auto ep = row.get_as<net::inet_address>("peer");
        if (!row.has("host_id")) {
            // Must never happen after `peers_table_read_fixup` call above
            on_internal_error_noexcept(slogger, format("load_endpoint_state: node {} has no host_id in system.{}", ep, PEERS));
        }
        auto host_id = locator::host_id(row.get_as<utils::UUID>("host_id"));
        if (row.has("tokens")) {
            st.tokens = decode_tokens(deserialize_set_column(*peers(), row, "tokens"));
        }
        if (row.has("data_center") && row.has("rack")) {
            st.opt_dc_rack.emplace(locator::endpoint_dc_rack {
                row.get_as<sstring>("data_center"),
                row.get_as<sstring>("rack")
            });
            if (st.opt_dc_rack->dc.empty() || st.opt_dc_rack->rack.empty()) {
                slogger.error("load_endpoint_state: node {}/{} has empty dc={} or rack={}", host_id, ep, st.opt_dc_rack->dc, st.opt_dc_rack->rack);
                continue;
            }
        } else {
            slogger.warn("Endpoint {} has no {} in system.{}", ep,
                    !row.has("data_center") && !row.has("rack") ? "data_center nor rack" : !row.has("data_center") ? "data_center" : "rack",
                    PEERS);
        }
        st.endpoint = ep;
        ret.emplace(host_id, std::move(st));
    }

    co_return ret;
}

future<std::vector<gms::inet_address>> system_keyspace::load_peers() {
    co_await peers_table_read_fixup();

    const auto res = co_await execute_cql(format("SELECT peer, rpc_address FROM system.{}", PEERS));
    SCYLLA_ASSERT(res);

    std::vector<gms::inet_address> ret;
    for (const auto& row: *res) {
        if (!row.has("rpc_address")) {
            // In the Raft-based topology, we store the Host ID -> IP mapping
            // of joining nodes in PEERS. We want to ignore such rows. To achieve
            // it, we check the presence of rpc_address, but we could choose any
            // column other than host_id and tokens (rows with no tokens can
            // correspond to zero-token nodes).
            continue;
        }
        ret.emplace_back(gms::inet_address(row.get_as<net::inet_address>("peer")));
    }
    co_return ret;
}

future<std::unordered_map<gms::inet_address, sstring>> system_keyspace::load_peer_features() {
    co_await peers_table_read_fixup();

    const sstring req = format("SELECT peer, supported_features FROM system.{}", PEERS);
    std::unordered_map<gms::inet_address, sstring> ret;
    const auto cql_result = co_await execute_cql(req);
    for (const auto& row : *cql_result) {
        if (row.has("supported_features")) {
            ret.emplace(row.get_as<net::inet_address>("peer"),
                    row.get_as<sstring>("supported_features"));
        }
    }
    co_return ret;
}

future<std::unordered_map<gms::inet_address, gms::inet_address>> system_keyspace::get_preferred_ips() {
    co_await peers_table_read_fixup();

    const sstring req = format("SELECT peer, preferred_ip FROM system.{}", PEERS);
    std::unordered_map<gms::inet_address, gms::inet_address> res;

    const auto cql_result = co_await execute_cql(req);
    for (const auto& r : *cql_result) {
        if (r.has("preferred_ip")) {
            res.emplace(gms::inet_address(r.get_as<net::inet_address>("peer")),
                        gms::inet_address(r.get_as<net::inet_address>("preferred_ip")));
        }
    }

    co_return res;
}

namespace {
template <typename T>
static data_value_or_unset make_data_value_or_unset(const std::optional<T>& opt) {
    if (opt) {
        return data_value(*opt);
    } else {
        return unset_value{};
    }
};

static data_value_or_unset make_data_value_or_unset(const std::optional<std::unordered_set<dht::token>>& opt) {
    if (opt) {
        auto set_type = set_type_impl::get_instance(utf8_type, true);
        return make_set_value(set_type, prepare_tokens(*opt));
    } else {
        return unset_value{};
    }
};
}

future<> system_keyspace::update_peer_info(gms::inet_address ep, locator::host_id hid, const peer_info& info) {
    if (ep == gms::inet_address{}) {
        on_internal_error(slogger, format("update_peer_info called with empty inet_address, host_id {}", hid));
    }
    if (!hid) {
        on_internal_error(slogger, format("update_peer_info called with empty host_id, ep {}", ep));
    }
    if (_db.get_token_metadata().get_topology().is_me(ep)) {
        on_internal_error(slogger, format("update_peer_info called for this node: {}", ep));
    }

    data_value_list values = {
        data_value_or_unset(data_value(ep.addr())),
        make_data_value_or_unset(info.data_center),
        data_value_or_unset(hid.id),
        make_data_value_or_unset(info.preferred_ip),
        make_data_value_or_unset(info.rack),
        make_data_value_or_unset(info.release_version),
        make_data_value_or_unset(info.rpc_address),
        make_data_value_or_unset(info.schema_version),
        make_data_value_or_unset(info.tokens),
        make_data_value_or_unset(info.supported_features),
    };

    auto query = fmt::format("INSERT INTO system.{} "
            "(peer,data_center,host_id,preferred_ip,rack,release_version,rpc_address,schema_version,tokens,supported_features) VALUES"
            "(?,?,?,?,?,?,?,?,?,?)", PEERS);

    slogger.debug("{}: values={}", query, values);

    co_await _qp.execute_internal(query, db::consistency_level::ONE, values, cql3::query_processor::cache_internal::yes);
}

template <typename T>
future<> system_keyspace::set_scylla_local_param_as(const sstring& key, const T& value, bool visible_before_cl_replay) {
    sstring req = format("UPDATE system.{} SET value = ? WHERE key = ?", system_keyspace::SCYLLA_LOCAL);
    auto type = data_type_for<T>();
    co_await execute_cql(req, type->to_string_impl(data_value(value)), key).discard_result();
    if (visible_before_cl_replay) {
        co_await force_blocking_flush(SCYLLA_LOCAL);
    }
}

template <typename T>
future<std::optional<T>> system_keyspace::get_scylla_local_param_as(const sstring& key) {
    sstring req = format("SELECT value FROM system.{} WHERE key = ?", system_keyspace::SCYLLA_LOCAL);
    return execute_cql(req, key).then([] (::shared_ptr<cql3::untyped_result_set> res)
            -> future<std::optional<T>> {
        if (res->empty() || !res->one().has("value")) {
            return make_ready_future<std::optional<T>>(std::optional<T>());
        }
        auto type = data_type_for<T>();
        return make_ready_future<std::optional<T>>(value_cast<T>(type->deserialize(
                    type->from_string(res->one().get_as<sstring>("value")))));
    });
}

template
future<std::optional<utils::UUID>>
system_keyspace::get_scylla_local_param_as<utils::UUID>(const sstring& key);

future<> system_keyspace::set_scylla_local_param(const sstring& key, const sstring& value, bool visible_before_cl_replay) {
    return set_scylla_local_param_as<sstring>(key, value, visible_before_cl_replay);
}

future<std::optional<sstring>> system_keyspace::get_scylla_local_param(const sstring& key){
    return get_scylla_local_param_as<sstring>(key);
}

future<> system_keyspace::update_schema_version(table_schema_version version) {
    sstring req = format("INSERT INTO system.{} (key, schema_version) VALUES (?, ?)", LOCAL);
    return execute_cql(req, sstring(LOCAL), version.uuid()).discard_result();
}

/**
 * Remove stored tokens being used by another node
 */
future<> system_keyspace::remove_endpoint(gms::inet_address ep) {
    const sstring req = format("DELETE FROM system.{} WHERE peer = ?", PEERS);
    slogger.debug("DELETE FROM system.{} WHERE peer = {}", PEERS, ep);
    co_await execute_cql(req, ep.addr()).discard_result();
}

future<> system_keyspace::update_tokens(const std::unordered_set<dht::token>& tokens) {
    sstring req = format("INSERT INTO system.{} (key, tokens) VALUES (?, ?)", LOCAL);
    auto set_type = set_type_impl::get_instance(utf8_type, true);
    co_await execute_cql(req, sstring(LOCAL), make_set_value(set_type, prepare_tokens(tokens)));
}

future<> system_keyspace::force_blocking_flush(sstring cfname) {
    return container().invoke_on_all([cfname = std::move(cfname)] (db::system_keyspace& sys_ks) {
        // if (!Boolean.getBoolean("cassandra.unsafesystem"))
        return sys_ks._db.flush(NAME, cfname);
    });
}

future<std::unordered_set<dht::token>> system_keyspace::get_saved_tokens() {
    sstring req = format("SELECT tokens FROM system.{} WHERE key = ?", LOCAL);
    return execute_cql(req, sstring(LOCAL)).then([] (auto msg) {
        if (msg->empty() || !msg->one().has("tokens")) {
            return make_ready_future<std::unordered_set<dht::token>>();
        }

        auto decoded_tokens = decode_tokens(deserialize_set_column(*local(), msg->one(), "tokens"));
        return make_ready_future<std::unordered_set<dht::token>>(std::move(decoded_tokens));
    });
}

future<std::unordered_set<dht::token>> system_keyspace::get_local_tokens() {
    return get_saved_tokens().then([] (auto&& tokens) {
        if (tokens.empty()) {
            auto err = format("get_local_tokens: tokens is empty");
            slogger.error("{}", err);
            throw std::runtime_error(err);
        }
        return std::move(tokens);
    });
}

future<> system_keyspace::update_cdc_generation_id(cdc::generation_id gen_id) {
    co_await std::visit(make_visitor(
    [this] (cdc::generation_id_v1 id) -> future<> {
        co_await execute_cql(
                format("INSERT INTO system.{} (key, streams_timestamp) VALUES (?, ?)", v3::CDC_LOCAL),
                sstring(v3::CDC_LOCAL), id.ts);
    },
    [this] (cdc::generation_id_v2 id) -> future<> {
        co_await execute_cql(
                format("INSERT INTO system.{} (key, streams_timestamp, uuid) VALUES (?, ?, ?)", v3::CDC_LOCAL),
                sstring(v3::CDC_LOCAL), id.ts, id.id);
    }
    ), gen_id);
}

future<std::optional<cdc::generation_id>> system_keyspace::get_cdc_generation_id() {
    auto msg = co_await execute_cql(
            format("SELECT streams_timestamp, uuid FROM system.{} WHERE key = ?", v3::CDC_LOCAL),
            sstring(v3::CDC_LOCAL));

    if (msg->empty()) {
        co_return std::nullopt;
    }

    auto& row = msg->one();
    if (!row.has("streams_timestamp")) {
        // should not happen but whatever
        co_return std::nullopt;
    }

    auto ts = row.get_as<db_clock::time_point>("streams_timestamp");
    if (!row.has("uuid")) {
        co_return cdc::generation_id_v1{ts};
    }

    auto id = row.get_as<utils::UUID>("uuid");
    co_return cdc::generation_id_v2{ts, id};
}

static const sstring CDC_REWRITTEN_KEY = "rewritten";

future<> system_keyspace::cdc_set_rewritten(std::optional<cdc::generation_id_v1> gen_id) {
    if (gen_id) {
        return execute_cql(
                format("INSERT INTO system.{} (key, streams_timestamp) VALUES (?, ?)", v3::CDC_LOCAL),
                CDC_REWRITTEN_KEY, gen_id->ts).discard_result();
    } else {
        // Insert just the row marker.
        return execute_cql(
                format("INSERT INTO system.{} (key) VALUES (?)", v3::CDC_LOCAL),
                CDC_REWRITTEN_KEY).discard_result();
    }
}

future<bool> system_keyspace::cdc_is_rewritten() {
    // We don't care about the actual timestamp; it's additional information for debugging purposes.
    return execute_cql(format("SELECT key FROM system.{} WHERE key = ?", v3::CDC_LOCAL), CDC_REWRITTEN_KEY)
            .then([] (::shared_ptr<cql3::untyped_result_set> msg) {
        return !msg->empty();
    });
}

bool system_keyspace::bootstrap_needed() const {
    return get_bootstrap_state() == bootstrap_state::NEEDS_BOOTSTRAP;
}

bool system_keyspace::bootstrap_complete() const {
    return get_bootstrap_state() == bootstrap_state::COMPLETED;
}

bool system_keyspace::bootstrap_in_progress() const {
    return get_bootstrap_state() == bootstrap_state::IN_PROGRESS;
}

bool system_keyspace::was_decommissioned() const {
    return get_bootstrap_state() == bootstrap_state::DECOMMISSIONED;
}

system_keyspace::bootstrap_state system_keyspace::get_bootstrap_state() const {
    return _cache->_state;
}

future<> system_keyspace::set_bootstrap_state(bootstrap_state state) {
    static std::unordered_map<bootstrap_state, sstring, enum_hash<bootstrap_state>> state_to_name({
        { bootstrap_state::NEEDS_BOOTSTRAP, "NEEDS_BOOTSTRAP" },
        { bootstrap_state::COMPLETED, "COMPLETED" },
        { bootstrap_state::IN_PROGRESS, "IN_PROGRESS" },
        { bootstrap_state::DECOMMISSIONED, "DECOMMISSIONED" }
    });

    sstring state_name = state_to_name.at(state);

    sstring req = format("INSERT INTO system.{} (key, bootstrapped) VALUES (?, ?)", LOCAL);
    co_await execute_cql(req, sstring(LOCAL), state_name).discard_result();
    co_await container().invoke_on_all([state] (auto& sys_ks) {
        sys_ks._cache->_state = state;
    });
}

std::vector<schema_ptr> system_keyspace::auth_tables() {
    return {roles(), role_members(), role_attributes(), role_permissions()};
}

std::vector<schema_ptr> system_keyspace::all_tables(const db::config& cfg) {
    std::vector<schema_ptr> r;
    auto schema_tables = db::schema_tables::all_tables(schema_features::full());
    std::copy(schema_tables.begin(), schema_tables.end(), std::back_inserter(r));
    auto auth_tables = system_keyspace::auth_tables();
    std::copy(auth_tables.begin(), auth_tables.end(), std::back_inserter(r));
    r.insert(r.end(), { built_indexes(), hints(), batchlog(), paxos(), local(),
                    peers(), peer_events(), range_xfers(),
                    compactions_in_progress(), compaction_history(),
                    sstable_activity(), size_estimates(), large_partitions(), large_rows(), large_cells(),
                    scylla_local(), db::schema_tables::scylla_table_schema_history(),
                    repair_history(),
                    v3::views_builds_in_progress(), v3::built_views(),
                    v3::scylla_views_builds_in_progress(),
                    v3::truncated(),
                    v3::commitlog_cleanups(),
                    v3::cdc_local(),
                    raft(), raft_snapshots(), raft_snapshot_config(), group0_history(), discovery(),
                    topology(), cdc_generations_v3(), topology_requests(), service_levels_v2(),
    });

    if (cfg.check_experimental(db::experimental_features_t::feature::BROADCAST_TABLES)) {
        r.insert(r.end(), {broadcast_kv_store()});
    }

    if (cfg.enable_tablets()) {
        r.insert(r.end(), {tablets()});
    }

    if (cfg.check_experimental(db::experimental_features_t::feature::KEYSPACE_STORAGE_OPTIONS)) {
        r.insert(r.end(), {sstables_registry()});
    }
    // legacy schema
    r.insert(r.end(), {
                    // TODO: once we migrate hints/batchlog and add converter
                    // legacy::hints(), legacy::batchlog(),
                    legacy::keyspaces(), legacy::column_families(),
                    legacy::columns(), legacy::triggers(), legacy::usertypes(),
                    legacy::functions(), legacy::aggregates(), });

    return r;
}

static bool maybe_write_in_user_memory(schema_ptr s) {
    return (s.get() == system_keyspace::batchlog().get()) || (s.get() == system_keyspace::paxos().get())
            || s == system_keyspace::v3::scylla_views_builds_in_progress();
}

future<> system_keyspace::make(
        locator::effective_replication_map_factory& erm_factory,
        replica::database& db) {
    for (auto&& table : system_keyspace::all_tables(db.get_config())) {
        co_await db.create_local_system_table(table, maybe_write_in_user_memory(table), erm_factory);
        co_await db.find_column_family(table).init_storage();
    }
}

void system_keyspace::mark_writable() {
    for (auto&& table : system_keyspace::all_tables(_db.get_config())) {
        _db.find_column_family(table).mark_ready_for_writes(_db.commitlog_for(table));
    }
}

future<foreign_ptr<lw_shared_ptr<reconcilable_result>>>
system_keyspace::query_mutations(distributed<replica::database>& db, schema_ptr schema) {
    return replica::query_mutations(db, schema, query::full_partition_range, schema->full_slice(), db::no_timeout);
}

future<foreign_ptr<lw_shared_ptr<reconcilable_result>>>
system_keyspace::query_mutations(distributed<replica::database>& db, const sstring& ks_name, const sstring& cf_name) {
    schema_ptr schema = db.local().find_schema(ks_name, cf_name);
    return query_mutations(db, schema);
}

future<foreign_ptr<lw_shared_ptr<reconcilable_result>>>
system_keyspace::query_mutations(distributed<replica::database>& db, const sstring& ks_name, const sstring& cf_name, const dht::partition_range& partition_range, query::clustering_range row_range) {
    auto schema = db.local().find_schema(ks_name, cf_name);
    auto slice_ptr = std::make_unique<query::partition_slice>(partition_slice_builder(*schema)
        .with_range(std::move(row_range))
        .build());
    return replica::query_mutations(db, std::move(schema), partition_range, *slice_ptr, db::no_timeout).finally([slice_ptr = std::move(slice_ptr)] { });
}

future<lw_shared_ptr<query::result_set>>
system_keyspace::query(distributed<replica::database>& db, const sstring& ks_name, const sstring& cf_name) {
    schema_ptr schema = db.local().find_schema(ks_name, cf_name);
    return replica::query_data(db, schema, query::full_partition_range, schema->full_slice(), db::no_timeout).then([schema] (auto&& qr) {
        return make_lw_shared<query::result_set>(query::result_set::from_raw_result(schema, schema->full_slice(), *qr));
    });
}

future<lw_shared_ptr<query::result_set>>
system_keyspace::query(distributed<replica::database>& db, const sstring& ks_name, const sstring& cf_name, const dht::decorated_key& key, query::clustering_range row_range)
{
    auto schema = db.local().find_schema(ks_name, cf_name);
    auto pr_ptr = std::make_unique<dht::partition_range>(dht::partition_range::make_singular(key));
    auto slice_ptr = std::make_unique<query::partition_slice>(partition_slice_builder(*schema)
        .with_range(std::move(row_range))
        .build());
    return replica::query_data(db, schema, *pr_ptr, *slice_ptr, db::no_timeout).then(
            [schema, pr_ptr = std::move(pr_ptr), slice_ptr = std::move(slice_ptr)] (auto&& qr) {
        return make_lw_shared<query::result_set>(query::result_set::from_raw_result(schema, schema->full_slice(), *qr));
    });
}

static map_type_impl::native_type prepare_rows_merged(std::unordered_map<int32_t, int64_t>& rows_merged) {
    map_type_impl::native_type tmp;
    for (auto& r: rows_merged) {
        int32_t first = r.first;
        int64_t second = r.second;
        auto map_element = std::make_pair<data_value, data_value>(data_value(first), data_value(second));
        tmp.push_back(std::move(map_element));
    }
    return tmp;
}

future<> system_keyspace::update_compaction_history(utils::UUID uuid, sstring ksname, sstring cfname, int64_t compacted_at, int64_t bytes_in, int64_t bytes_out,
                                   std::unordered_map<int32_t, int64_t> rows_merged)
{
    // don't write anything when the history table itself is compacted, since that would in turn cause new compactions
    if (ksname == "system" && cfname == COMPACTION_HISTORY) {
        return make_ready_future<>();
    }

    auto map_type = map_type_impl::get_instance(int32_type, long_type, true);

    sstring req = format("INSERT INTO system.{} (id, keyspace_name, columnfamily_name, compacted_at, bytes_in, bytes_out, rows_merged) VALUES (?, ?, ?, ?, ?, ?, ?)"
                    , COMPACTION_HISTORY);

    db_clock::time_point tp{db_clock::duration{compacted_at}};
    return execute_cql(req, uuid, ksname, cfname, tp, bytes_in, bytes_out,
                       make_map_value(map_type, prepare_rows_merged(rows_merged))).discard_result().handle_exception([] (auto ep) {
        slogger.error("update compaction history failed: {}: ignored", ep);
    });
}

future<> system_keyspace::get_compaction_history(compaction_history_consumer consumer) {
    sstring req = format("SELECT * from system.{}", COMPACTION_HISTORY);
    co_await _qp.query_internal(req, [&consumer] (const cql3::untyped_result_set::row& row) mutable -> future<stop_iteration> {
        compaction_history_entry entry;
        entry.id = row.get_as<utils::UUID>("id");
        entry.ks = row.get_as<sstring>("keyspace_name");
        entry.cf = row.get_as<sstring>("columnfamily_name");
        entry.compacted_at = row.get_as<int64_t>("compacted_at");
        entry.bytes_in = row.get_as<int64_t>("bytes_in");
        entry.bytes_out = row.get_as<int64_t>("bytes_out");
        if (row.has("rows_merged")) {
            entry.rows_merged = row.get_map<int32_t, int64_t>("rows_merged");
        }
        co_await consumer(std::move(entry));
        co_return stop_iteration::no;
    });
}

future<> system_keyspace::update_repair_history(repair_history_entry entry) {
    sstring req = format("INSERT INTO system.{} (table_uuid, repair_time, repair_uuid, keyspace_name, table_name, range_start, range_end) VALUES (?, ?, ?, ?, ?, ?, ?)", REPAIR_HISTORY);
    co_await execute_cql(req, entry.table_uuid.uuid(), entry.ts, entry.id.uuid(), entry.ks, entry.cf, entry.range_start, entry.range_end).discard_result();
}

future<> system_keyspace::get_repair_history(::table_id table_id, repair_history_consumer f) {
    sstring req = format("SELECT * from system.{} WHERE table_uuid = {}", REPAIR_HISTORY, table_id);
    co_await _qp.query_internal(req, [&f] (const cql3::untyped_result_set::row& row) mutable -> future<stop_iteration> {
        repair_history_entry ent;
        ent.id = tasks::task_id(row.get_as<utils::UUID>("repair_uuid"));
        ent.table_uuid = ::table_id(row.get_as<utils::UUID>("table_uuid"));
        ent.range_start = row.get_as<int64_t>("range_start");
        ent.range_end = row.get_as<int64_t>("range_end");
        ent.ks = row.get_as<sstring>("keyspace_name");
        ent.cf = row.get_as<sstring>("table_name");
        ent.ts = row.get_as<db_clock::time_point>("repair_time");
        co_await f(std::move(ent));
        co_return stop_iteration::no;
    });
}

future<gms::generation_type> system_keyspace::increment_and_get_generation() {
    auto req = format("SELECT gossip_generation FROM system.{} WHERE key='{}'", LOCAL, LOCAL);
    auto rs = co_await _qp.execute_internal(req, cql3::query_processor::cache_internal::yes);
    gms::generation_type generation;
    if (rs->empty() || !rs->one().has("gossip_generation")) {
        // seconds-since-epoch isn't a foolproof new generation
        // (where foolproof is "guaranteed to be larger than the last one seen at this ip address"),
        // but it's as close as sanely possible
        generation = gms::get_generation_number();
    } else {
        // Other nodes will ignore gossip messages about a node that have a lower generation than previously seen.
        auto stored_generation = gms::generation_type(rs->one().template get_as<int>("gossip_generation") + 1);
        auto now = gms::get_generation_number();
        if (stored_generation >= now) {
            slogger.warn("Using stored Gossip Generation {} as it is greater than current system time {}."
                        "See CASSANDRA-3654 if you experience problems", stored_generation, now);
            generation = stored_generation;
        } else {
            generation = now;
        }
    }
    req = format("INSERT INTO system.{} (key, gossip_generation) VALUES ('{}', ?)", LOCAL, LOCAL);
    co_await _qp.execute_internal(req, {generation.value()}, cql3::query_processor::cache_internal::yes);
    co_return generation;
}

mutation system_keyspace::make_size_estimates_mutation(const sstring& ks, std::vector<system_keyspace::range_estimates> estimates) {
    auto&& schema = db::system_keyspace::size_estimates();
    auto timestamp = api::new_timestamp();
    mutation m_to_apply{schema, partition_key::from_single_value(*schema, utf8_type->decompose(ks))};

    for (auto&& e : estimates) {
        auto ck = clustering_key_prefix(std::vector<bytes>{
                utf8_type->decompose(e.schema->cf_name()), e.range_start_token, e.range_end_token});

        m_to_apply.set_clustered_cell(ck, "mean_partition_size", e.mean_partition_size, timestamp);
        m_to_apply.set_clustered_cell(ck, "partitions_count", e.partitions_count, timestamp);
    }

    return m_to_apply;
}

future<> system_keyspace::register_view_for_building(sstring ks_name, sstring view_name, const dht::token& token) {
    sstring req = format("INSERT INTO system.{} (keyspace_name, view_name, generation_number, cpu_id, first_token) VALUES (?, ?, ?, ?, ?)",
            v3::SCYLLA_VIEWS_BUILDS_IN_PROGRESS);
    return execute_cql(
            std::move(req),
            std::move(ks_name),
            std::move(view_name),
            0,
            int32_t(this_shard_id()),
            token.to_sstring()).discard_result();
}

future<> system_keyspace::update_view_build_progress(sstring ks_name, sstring view_name, const dht::token& token) {
    sstring req = format("INSERT INTO system.{} (keyspace_name, view_name, next_token, cpu_id) VALUES (?, ?, ?, ?)",
            v3::SCYLLA_VIEWS_BUILDS_IN_PROGRESS);
    return execute_cql(
            std::move(req),
            std::move(ks_name),
            std::move(view_name),
            token.to_sstring(),
            int32_t(this_shard_id())).discard_result();
}

future<> system_keyspace::remove_view_build_progress_across_all_shards(sstring ks_name, sstring view_name) {
    return execute_cql(
            format("DELETE FROM system.{} WHERE keyspace_name = ? AND view_name = ?", v3::SCYLLA_VIEWS_BUILDS_IN_PROGRESS),
            std::move(ks_name),
            std::move(view_name)).discard_result();
}

future<> system_keyspace::remove_view_build_progress(sstring ks_name, sstring view_name) {
    return execute_cql(
            format("DELETE FROM system.{} WHERE keyspace_name = ? AND view_name = ? AND cpu_id = ?", v3::SCYLLA_VIEWS_BUILDS_IN_PROGRESS),
            std::move(ks_name),
            std::move(view_name),
            int32_t(this_shard_id())).discard_result();
}

future<> system_keyspace::mark_view_as_built(sstring ks_name, sstring view_name) {
    return execute_cql(
            format("INSERT INTO system.{} (keyspace_name, view_name) VALUES (?, ?)", v3::BUILT_VIEWS),
            std::move(ks_name),
            std::move(view_name)).discard_result();
}

future<> system_keyspace::remove_built_view(sstring ks_name, sstring view_name) {
    return execute_cql(
            format("DELETE FROM system.{} WHERE keyspace_name = ? AND view_name = ?", v3::BUILT_VIEWS),
            std::move(ks_name),
            std::move(view_name)).discard_result();
}

future<std::vector<system_keyspace::view_name>> system_keyspace::load_built_views() {
    return execute_cql(format("SELECT * FROM system.{}", v3::BUILT_VIEWS)).then([] (::shared_ptr<cql3::untyped_result_set> cql_result) {
        return boost::copy_range<std::vector<view_name>>(*cql_result
                | boost::adaptors::transformed([] (const cql3::untyped_result_set::row& row) {
            auto ks_name = row.get_as<sstring>("keyspace_name");
            auto cf_name = row.get_as<sstring>("view_name");
            return std::pair(std::move(ks_name), std::move(cf_name));
        }));
    });
}

future<std::vector<system_keyspace::view_build_progress>> system_keyspace::load_view_build_progress() {
    return execute_cql(format("SELECT keyspace_name, view_name, first_token, next_token, cpu_id FROM system.{}",
            v3::SCYLLA_VIEWS_BUILDS_IN_PROGRESS)).then([] (::shared_ptr<cql3::untyped_result_set> cql_result) {
        std::vector<view_build_progress> progress;
        for (auto& row : *cql_result) {
            auto ks_name = row.get_as<sstring>("keyspace_name");
            auto cf_name = row.get_as<sstring>("view_name");
            auto first_token = dht::token::from_sstring(row.get_as<sstring>("first_token"));
            auto next_token_sstring = row.get_opt<sstring>("next_token");
            std::optional<dht::token> next_token;
            if (next_token_sstring) {
                next_token = dht::token::from_sstring(std::move(next_token_sstring).value());
            }
            auto cpu_id = row.get_as<int32_t>("cpu_id");
            progress.emplace_back(view_build_progress{
                    view_name(std::move(ks_name), std::move(cf_name)),
                    std::move(first_token),
                    std::move(next_token),
                    static_cast<shard_id>(cpu_id)});
        }
        return progress;
    }).handle_exception([] (const std::exception_ptr& eptr) {
        slogger.warn("Failed to load view build progress: {}", eptr);
        return std::vector<view_build_progress>();
    });
}


template <typename... Args>
future<::shared_ptr<cql3::untyped_result_set>> system_keyspace::execute_cql_with_timeout(sstring req,
        db::timeout_clock::time_point timeout,
        Args&&... args) {
    const db::timeout_clock::time_point now = db::timeout_clock::now();
    const db::timeout_clock::duration d =
        now < timeout ?
            timeout - now :
            // let the `storage_proxy` time out the query down the call chain
            db::timeout_clock::duration::zero();

    struct timeout_context {
        std::unique_ptr<service::client_state> client_state;
        service::query_state query_state;
        timeout_context(db::timeout_clock::duration d)
                : client_state(std::make_unique<service::client_state>(service::client_state::internal_tag{}, timeout_config{d, d, d, d, d, d, d}))
                , query_state(*client_state, empty_service_permit())
        {}
    };
    return do_with(timeout_context(d), [this, req = std::move(req), &args...] (auto& tctx) {
        return _qp.execute_internal(req,
            cql3::query_options::DEFAULT.get_consistency(),
            tctx.query_state,
            { data_value(std::forward<Args>(args))... },
            cql3::query_processor::cache_internal::yes);
    });
}

future<service::paxos::paxos_state> system_keyspace::load_paxos_state(partition_key_view key, schema_ptr s, gc_clock::time_point now,
        db::timeout_clock::time_point timeout) {
    static auto cql = format("SELECT * FROM system.{} WHERE row_key = ? AND cf_id = ?", PAXOS);
    // FIXME: we need execute_cql_with_now()
    (void)now;
    auto f = execute_cql_with_timeout(cql, timeout, to_legacy(*key.get_compound_type(*s), key.representation()), s->id().uuid());
    return f.then([s, key = std::move(key)] (shared_ptr<cql3::untyped_result_set> results) mutable {
        if (results->empty()) {
            return service::paxos::paxos_state();
        }
        auto& row = results->one();
        auto promised = row.has("promise")
                        ? row.get_as<utils::UUID>("promise") : utils::UUID_gen::min_time_UUID();

        std::optional<service::paxos::proposal> accepted;
        if (row.has("proposal")) {
            accepted = service::paxos::proposal(row.get_as<utils::UUID>("proposal_ballot"),
                    ser::deserialize_from_buffer<>(row.get_blob("proposal"),  boost::type<frozen_mutation>(), 0));
        }

        std::optional<service::paxos::proposal> most_recent;
        if (row.has("most_recent_commit_at")) {
            // the value can be missing if it was pruned, supply empty one since
            // it will not going to be used anyway
            auto fm = row.has("most_recent_commit") ?
                     ser::deserialize_from_buffer<>(row.get_blob("most_recent_commit"), boost::type<frozen_mutation>(), 0) :
                     freeze(mutation(s, key));
            most_recent = service::paxos::proposal(row.get_as<utils::UUID>("most_recent_commit_at"),
                    std::move(fm));
        }

        return service::paxos::paxos_state(promised, std::move(accepted), std::move(most_recent));
    });
}

static int32_t paxos_ttl_sec(const schema& s) {
    // Keep paxos state around for paxos_grace_seconds. If one of the Paxos participants
    // is down for longer than paxos_grace_seconds it is considered to be dead and must rebootstrap.
    // Otherwise its Paxos table state will be repaired by nodetool repair or Paxos repair.
    return std::chrono::duration_cast<std::chrono::seconds>(s.paxos_grace_seconds()).count();
}

future<> system_keyspace::save_paxos_promise(const schema& s, const partition_key& key, const utils::UUID& ballot, db::timeout_clock::time_point timeout) {
    static auto cql = format("UPDATE system.{} USING TIMESTAMP ? AND TTL ? SET promise = ? WHERE row_key = ? AND cf_id = ?", PAXOS);
    return execute_cql_with_timeout(cql,
            timeout,
            utils::UUID_gen::micros_timestamp(ballot),
            paxos_ttl_sec(s),
            ballot,
            to_legacy(*key.get_compound_type(s), key.representation()),
            s.id().uuid()
        ).discard_result();
}

future<> system_keyspace::save_paxos_proposal(const schema& s, const service::paxos::proposal& proposal, db::timeout_clock::time_point timeout) {
    static auto cql = format("UPDATE system.{} USING TIMESTAMP ? AND TTL ? SET promise = ?, proposal_ballot = ?, proposal = ? WHERE row_key = ? AND cf_id = ?", PAXOS);
    partition_key_view key = proposal.update.key();
    return execute_cql_with_timeout(cql,
            timeout,
            utils::UUID_gen::micros_timestamp(proposal.ballot),
            paxos_ttl_sec(s),
            proposal.ballot,
            proposal.ballot,
            ser::serialize_to_buffer<bytes>(proposal.update),
            to_legacy(*key.get_compound_type(s), key.representation()),
            s.id().uuid()
        ).discard_result();
}

future<> system_keyspace::save_paxos_decision(const schema& s, const service::paxos::proposal& decision, db::timeout_clock::time_point timeout) {
    // We always erase the last proposal when we learn about a new Paxos decision. The ballot
    // timestamp of the decision is used for entire mutation, so if the "erased" proposal is more
    // recent it will naturally stay on top.
    // Erasing the last proposal is just an optimization and does not affect correctness:
    // sp::begin_and_repair_paxos will exclude an accepted proposal if it is older than the most
    // recent commit.
    static auto cql = format("UPDATE system.{} USING TIMESTAMP ? AND TTL ? SET proposal_ballot = null, proposal = null,"
            " most_recent_commit_at = ?, most_recent_commit = ? WHERE row_key = ? AND cf_id = ?", PAXOS);
    partition_key_view key = decision.update.key();
    return execute_cql_with_timeout(cql,
            timeout,
            utils::UUID_gen::micros_timestamp(decision.ballot),
            paxos_ttl_sec(s),
            decision.ballot,
            ser::serialize_to_buffer<bytes>(decision.update),
            to_legacy(*key.get_compound_type(s), key.representation()),
            s.id().uuid()
        ).discard_result();
}

future<> system_keyspace::delete_paxos_decision(const schema& s, const partition_key& key, const utils::UUID& ballot, db::timeout_clock::time_point timeout) {
    // This should be called only if a learn stage succeeded on all replicas.
    // In this case we can remove learned paxos value using ballot's timestamp which
    // guarantees that if there is more recent round it will not be affected.
    static auto cql = format("DELETE most_recent_commit FROM system.{} USING TIMESTAMP ?  WHERE row_key = ? AND cf_id = ?", PAXOS);

    return execute_cql_with_timeout(cql,
            timeout,
            utils::UUID_gen::micros_timestamp(ballot),
            to_legacy(*key.get_compound_type(s), key.representation()),
            s.id().uuid()
        ).discard_result();
}

future<std::set<sstring>> system_keyspace::load_local_enabled_features() {
    std::set<sstring> features;
    auto features_str = co_await get_scylla_local_param(gms::feature_service::ENABLED_FEATURES_KEY);
    if (features_str) {
        features = gms::feature_service::to_feature_set(*features_str);
    }
    co_return features;
}

future<> system_keyspace::save_local_enabled_features(std::set<sstring> features, bool visible_before_cl_replay) {
    auto features_str = fmt::to_string(fmt::join(features, ","));
    co_await set_scylla_local_param(gms::feature_service::ENABLED_FEATURES_KEY, features_str, visible_before_cl_replay);
}

future<utils::UUID> system_keyspace::get_raft_group0_id() {
    auto opt = co_await get_scylla_local_param_as<utils::UUID>("raft_group0_id");
    co_return opt.value_or<utils::UUID>({});
}

future<> system_keyspace::set_raft_group0_id(utils::UUID uuid) {
    return set_scylla_local_param_as<utils::UUID>("raft_group0_id", uuid, false);
}

static constexpr auto GROUP0_HISTORY_KEY = "history";

future<utils::UUID> system_keyspace::get_last_group0_state_id() {
    auto rs = co_await execute_cql(
        format(
            "SELECT state_id FROM system.{} WHERE key = '{}' LIMIT 1",
            GROUP0_HISTORY, GROUP0_HISTORY_KEY));
    SCYLLA_ASSERT(rs);
    if (rs->empty()) {
        co_return utils::UUID{};
    }
    co_return rs->one().get_as<utils::UUID>("state_id");
}

future<bool> system_keyspace::group0_history_contains(utils::UUID state_id) {
    auto rs = co_await execute_cql(
        format(
            "SELECT state_id FROM system.{} WHERE key = '{}' AND state_id = ?",
            GROUP0_HISTORY, GROUP0_HISTORY_KEY),
        state_id);
    SCYLLA_ASSERT(rs);
    co_return !rs->empty();
}

mutation system_keyspace::make_group0_history_state_id_mutation(
        utils::UUID state_id, std::optional<gc_clock::duration> gc_older_than, std::string_view description) {
    auto s = group0_history();
    mutation m(s, partition_key::from_singular(*s, GROUP0_HISTORY_KEY));
    auto& row = m.partition().clustered_row(*s, clustering_key::from_singular(*s, state_id));
    auto ts = utils::UUID_gen::micros_timestamp(state_id);
    row.apply(row_marker(ts));
    if (!description.empty()) {
        auto cdef = s->get_column_definition("description");
        SCYLLA_ASSERT(cdef);
        row.cells().apply(*cdef, atomic_cell::make_live(*cdef->type, ts, cdef->type->decompose(description)));
    }
    if (gc_older_than) {
        using namespace std::chrono;
        SCYLLA_ASSERT(*gc_older_than >= gc_clock::duration{0});

        auto ts_micros = microseconds{ts};
        auto gc_older_than_micros = duration_cast<microseconds>(*gc_older_than);
        SCYLLA_ASSERT(gc_older_than_micros < ts_micros);

        auto tomb_upper_bound = utils::UUID_gen::min_time_UUID(ts_micros - gc_older_than_micros);
        // We want to delete all entries with IDs smaller than `tomb_upper_bound`
        // but the deleted range is of the form (x, +inf) since the schema is reversed.
        auto range = query::clustering_range::make_starting_with({
                clustering_key_prefix::from_single_value(*s, timeuuid_type->decompose(tomb_upper_bound)), false});
        auto bv = bound_view::from_range(range);

        m.partition().apply_delete(*s, range_tombstone{bv.first, bv.second, tombstone{ts, gc_clock::now()}});
    }
    return m;
}

future<mutation> system_keyspace::get_group0_history(distributed<replica::database>& db) {
    auto s = group0_history();
    auto rs = co_await db::system_keyspace::query_mutations(db, db::system_keyspace::NAME, db::system_keyspace::GROUP0_HISTORY);
    SCYLLA_ASSERT(rs);
    auto& ps = rs->partitions();
    for (auto& p: ps) {
        auto mut = p.mut().unfreeze(s);
        auto partition_key = value_cast<sstring>(utf8_type->deserialize(mut.key().get_component(*s, 0)));
        if (partition_key == GROUP0_HISTORY_KEY) {
            co_return mut;
        }
        slogger.warn("get_group0_history: unexpected partition in group0 history table: {}", partition_key);
    }

    slogger.warn("get_group0_history: '{}' partition not found", GROUP0_HISTORY_KEY);
    co_return mutation(s, partition_key::from_singular(*s, GROUP0_HISTORY_KEY));
}

static future<std::optional<mutation>> get_scylla_local_mutation(replica::database& db, std::string_view key) {
    auto s = db.find_schema(db::system_keyspace::NAME, db::system_keyspace::SCYLLA_LOCAL);

    partition_key pk = partition_key::from_singular(*s, key);
    dht::partition_range pr = dht::partition_range::make_singular(dht::decorate_key(*s, pk));

    auto rs = co_await replica::query_mutations(db.container(), s, pr, s->full_slice(), db::no_timeout);
    SCYLLA_ASSERT(rs);
    auto& ps = rs->partitions();
    for (auto& p: ps) {
        auto mut = p.mut().unfreeze(s);
        co_return std::move(mut);
    }

    co_return std::nullopt;
}

future<std::optional<mutation>> system_keyspace::get_group0_schema_version() {
    return get_scylla_local_mutation(_db, "group0_schema_version");
}

static constexpr auto AUTH_VERSION_KEY = "auth_version";

future<system_keyspace::auth_version_t> system_keyspace::get_auth_version() {
    auto str_opt = co_await get_scylla_local_param(AUTH_VERSION_KEY);
    if (!str_opt) {
        co_return auth_version_t::v1;
    }
    auto& str = *str_opt;
    if (str == "" || str == "1") {
        co_return auth_version_t::v1;
    }
    if (str == "2") {
        co_return auth_version_t::v2;
    }
    on_internal_error(slogger, fmt::format("unexpected auth_version in scylla_local got {}", str));
}

future<std::optional<mutation>> system_keyspace::get_auth_version_mutation() {
    return get_scylla_local_mutation(_db, AUTH_VERSION_KEY);
}

static service::query_state& internal_system_query_state() {
    using namespace std::chrono_literals;
    const auto t = 10s;
    static timeout_config tc{ t, t, t, t, t, t, t };
    static thread_local service::client_state cs(service::client_state::internal_tag{}, tc);
    static thread_local service::query_state qs(cs, empty_service_permit());
    return qs;
};

future<mutation> system_keyspace::make_auth_version_mutation(api::timestamp_type ts, db::system_keyspace::auth_version_t version) {
    static sstring query = format("INSERT INTO {}.{} (key, value) VALUES (?, ?);", db::system_keyspace::NAME, db::system_keyspace::SCYLLA_LOCAL);
    auto muts = co_await _qp.get_mutations_internal(query, internal_system_query_state(), ts, {AUTH_VERSION_KEY, std::to_string(int64_t(version))});
    if (muts.size() != 1) {
         on_internal_error(slogger, fmt::format("expected 1 auth_version mutation got {}", muts.size()));
    }
    co_return std::move(muts[0]);
}

static constexpr auto SERVICE_LEVELS_VERSION_KEY = "service_level_version";

future<std::optional<mutation>> system_keyspace::get_service_levels_version_mutation() {
    return get_scylla_local_mutation(_db, SERVICE_LEVELS_VERSION_KEY);
}

future<mutation> system_keyspace::make_service_levels_version_mutation(int8_t version, const service::group0_guard& guard) {
    static sstring query = format("INSERT INTO {}.{} (key, value) VALUES (?, ?);", db::system_keyspace::NAME, db::system_keyspace::SCYLLA_LOCAL);
    auto timestamp = guard.write_timestamp();
    auto muts = co_await _qp.get_mutations_internal(query, internal_system_query_state(), timestamp, {SERVICE_LEVELS_VERSION_KEY, format("{}", version)});

    if (muts.size() != 1) {
        on_internal_error(slogger, format("expecting single insert mutation, got {}", muts.size()));
    }
    co_return std::move(muts[0]);
}

future<std::optional<int8_t>> system_keyspace::get_service_levels_version() {
    return get_scylla_local_param_as<int8_t>(SERVICE_LEVELS_VERSION_KEY);
}

static constexpr auto GROUP0_UPGRADE_STATE_KEY = "group0_upgrade_state";

future<std::optional<sstring>> system_keyspace::load_group0_upgrade_state() {
    return get_scylla_local_param_as<sstring>(GROUP0_UPGRADE_STATE_KEY);
}

future<> system_keyspace::save_group0_upgrade_state(sstring value) {
    return set_scylla_local_param(GROUP0_UPGRADE_STATE_KEY, value, false);
}

static constexpr auto MUST_SYNCHRONIZE_TOPOLOGY_KEY = "must_synchronize_topology";

future<bool> system_keyspace::get_must_synchronize_topology() {
    auto opt = co_await get_scylla_local_param_as<bool>(MUST_SYNCHRONIZE_TOPOLOGY_KEY);
    co_return opt.value_or(false);
}

future<> system_keyspace::set_must_synchronize_topology(bool value) {
    return set_scylla_local_param_as<bool>(MUST_SYNCHRONIZE_TOPOLOGY_KEY, value, false);
}

static std::set<sstring> decode_features(const set_type_impl::native_type& features) {
    std::set<sstring> fset;
    for (auto& f : features) {
        fset.insert(value_cast<sstring>(std::move(f)));
    }
    return fset;
}

static bool must_have_tokens(service::node_state nst) {
    switch (nst) {
    case service::node_state::none: return false;
    // Bootstrapping and replacing nodes don't have tokens at first,
    // they are inserted only at some point during bootstrap/replace
    case service::node_state::bootstrapping: return false;
    case service::node_state::replacing: return false;
    // A decommissioning node doesn't have tokens at the end, they are
    // removed during transition to the left_token_ring state.
    case service::node_state::decommissioning: return false;
    case service::node_state::removing: return true;
    case service::node_state::rebuilding: return true;
    case service::node_state::normal: return true;
    case service::node_state::left: return false;
    }
}

future<service::topology> system_keyspace::load_topology_state(const std::unordered_set<locator::host_id>& force_load_hosts) {
    auto rs = co_await execute_cql(
        format("SELECT * FROM system.{} WHERE key = '{}'", TOPOLOGY, TOPOLOGY));
    SCYLLA_ASSERT(rs);

    service::topology_state_machine::topology_type ret;

    if (rs->empty()) {
        co_return ret;
    }

    for (auto& row : *rs) {
        if (!row.has("host_id")) {
            // There are no clustering rows, only the static row.
            // Skip the whole loop, the static row is handled later.
            break;
        }

        raft::server_id host_id{row.get_as<utils::UUID>("host_id")};
        auto datacenter = row.get_as<sstring>("datacenter");
        auto rack = row.get_as<sstring>("rack");
        auto release_version = row.get_as<sstring>("release_version");
        uint32_t num_tokens = row.get_as<int32_t>("num_tokens");
        sstring tokens_string = row.get_as<sstring>("tokens_string");
        size_t shard_count = row.get_as<int32_t>("shard_count");
        uint8_t ignore_msb = row.get_as<int32_t>("ignore_msb");
        sstring cleanup_status = row.get_as<sstring>("cleanup_status");
        utils::UUID request_id = row.get_as<utils::UUID>("request_id");

        service::node_state nstate = service::node_state_from_string(row.get_as<sstring>("node_state"));

        std::optional<service::ring_slice> ring_slice;
        if (row.has("tokens")) {
            auto tokens = decode_tokens(deserialize_set_column(*topology(), row, "tokens"));

            ring_slice = service::ring_slice {
                .tokens = std::move(tokens),
            };
        } else {
            auto zero_token = num_tokens == 0 && tokens_string.empty();
            if (zero_token) {
                // We distinguish normal zero-token nodes from token-owning nodes without tokens at the moment
                // in the following way:
                // - for normal zero-token nodes, ring_slice is engaged with an empty set of tokens,
                // - for token-owning nodes without tokens at the moment, ring_slice equals std::nullopt.
                // ring_slice also equals std::nullopt for joining zero-token nodes. The reason is that the
                // topology coordinator assigns tokens in the join_group0 state handler, and we want to simulate
                // assigning zero tokens for zero-token nodes. It allows us to have the same assertions for all nodes.
                // The code below is correct because the join_group0 state is the last transition state if a joining
                // node is zero-token.
                // Note that we need this workaround because we store tokens in a non-frozen set, which doesn't
                // distinguish an empty set from no value.
                if (nstate != service::node_state::none && nstate != service::node_state::bootstrapping
                        && nstate != service::node_state::replacing) {
                    ring_slice = service::ring_slice {
                        .tokens = std::unordered_set<dht::token>(),
                    };
                }
            } else if (must_have_tokens(nstate)) {
                on_fatal_internal_error(slogger, format(
                        "load_topology_state: node {} in {} state but missing ring slice", host_id, nstate));
            }
        }

        std::optional<raft::server_id> replaced_id;
        if (row.has("replaced_id")) {
            replaced_id = raft::server_id(row.get_as<utils::UUID>("replaced_id"));
        }

        std::optional<sstring> rebuild_option;
        if (row.has("rebuild_option")) {
            rebuild_option = row.get_as<sstring>("rebuild_option");
        }

        std::set<sstring> supported_features;
        if (row.has("supported_features")) {
            supported_features = decode_features(deserialize_set_column(*topology(), row, "supported_features"));
        }

        if (row.has("topology_request")) {
            auto req = service::topology_request_from_string(row.get_as<sstring>("topology_request"));
            ret.requests.emplace(host_id, req);
            switch(req) {
            case service::topology_request::replace:
                if (!replaced_id) {
                    on_internal_error(slogger, fmt::format("replaced_id is missing for a node {}", host_id));
                }
                ret.req_param.emplace(host_id, service::replace_param{*replaced_id});
                break;
            case service::topology_request::rebuild:
                if (!rebuild_option) {
                    on_internal_error(slogger, fmt::format("rebuild_option is missing for a node {}", host_id));
                }
                ret.req_param.emplace(host_id, service::rebuild_param{*rebuild_option});
                break;
            default:
                // no parameters for other requests
                break;
            }
        } else {
            switch (nstate) {
            case service::node_state::bootstrapping:
                // The tokens aren't generated right away when we enter the `bootstrapping` node state.
                // Therefore we need to know the number of tokens when we generate them during the bootstrap process.
                ret.req_param.emplace(host_id, service::join_param{num_tokens, tokens_string});
                break;
            case service::node_state::replacing:
                // If a node is replacing we need to know which node it is replacing and which nodes are ignored
                if (!replaced_id) {
                    on_internal_error(slogger, fmt::format("replaced_id is missing for a node {}", host_id));
                }
                ret.req_param.emplace(host_id, service::replace_param{*replaced_id});
                break;
            case service::node_state::rebuilding:
                // If a node is rebuilding it needs to know the parameter for the operation
                if (!rebuild_option) {
                    on_internal_error(slogger, fmt::format("rebuild_option is missing for a node {}", host_id));
                }
                ret.req_param.emplace(host_id, service::rebuild_param{*rebuild_option});
                break;
            default:
                // no parameters for other operations
                break;
            }
        }

        std::unordered_map<raft::server_id, service::replica_state>* map = nullptr;
        if (nstate == service::node_state::normal) {
            map = &ret.normal_nodes;
        } else if (nstate == service::node_state::left) {
            ret.left_nodes.emplace(host_id);
            if (force_load_hosts.contains(locator::host_id(host_id.uuid()))) {
                map = &ret.left_nodes_rs;
            }
        } else if (nstate == service::node_state::none) {
            map = &ret.new_nodes;
        } else {
            map = &ret.transition_nodes;
            // Currently, at most one node at a time can be in transitioning state.
            if (!map->empty()) {
                const auto& [other_id, other_rs] = *map->begin();
                on_fatal_internal_error(slogger, format(
                    "load_topology_state: found two nodes in transitioning state: {} in {} state and {} in {} state",
                    other_id, other_rs.state, host_id, nstate));
            }
        }
        if (map) {
            map->emplace(host_id, service::replica_state{
                nstate, std::move(datacenter), std::move(rack), std::move(release_version),
                ring_slice, shard_count, ignore_msb, std::move(supported_features),
                service::cleanup_status_from_string(cleanup_status), request_id});
        }
    }

    {
        // Here we access static columns, any row will do.
        auto& some_row = *rs->begin();

        if (some_row.has("version")) {
            ret.version = some_row.get_as<service::topology::version_t>("version");
        }

        if (some_row.has("fence_version")) {
            ret.fence_version = some_row.get_as<service::topology::version_t>("fence_version");
        }

        if (some_row.has("transition_state")) {
            ret.tstate = service::transition_state_from_string(some_row.get_as<sstring>("transition_state"));
        } else {
            // Any remaining transition_nodes must be in rebuilding state.
            auto it = std::find_if(ret.transition_nodes.begin(), ret.transition_nodes.end(),
                    [] (auto& p) { return p.second.state != service::node_state::rebuilding; });
            if (it != ret.transition_nodes.end()) {
                on_internal_error(slogger, format(
                    "load_topology_state: topology not in transition state"
                    " but transition node {} in rebuilding state is present", it->first));
            }
        }

        if (some_row.has("new_cdc_generation_data_uuid")) {
            ret.new_cdc_generation_data_uuid = some_row.get_as<utils::UUID>("new_cdc_generation_data_uuid");
        }

        if (some_row.has("committed_cdc_generations")) {
            ret.committed_cdc_generations = decode_cdc_generations_ids(deserialize_set_column(*topology(), some_row, "committed_cdc_generations"));
        }

        if (some_row.has("new_keyspace_rf_change_data")) {
            ret.new_keyspace_rf_change_ks_name = some_row.get_as<sstring>("new_keyspace_rf_change_ks_name");
            ret.new_keyspace_rf_change_data = some_row.get_map<sstring,sstring>("new_keyspace_rf_change_data");
        }

        if (!ret.committed_cdc_generations.empty()) {
            // Sanity check for CDC generation data consistency.
            auto gen_id = ret.committed_cdc_generations.back();
            auto gen_rows = co_await execute_cql(
                format("SELECT count(range_end) as cnt FROM {}.{} WHERE key = '{}' AND id = ?",
                        NAME, CDC_GENERATIONS_V3, cdc::CDC_GENERATIONS_V3_KEY),
                gen_id.id);
            SCYLLA_ASSERT(gen_rows);
            if (gen_rows->empty()) {
                on_internal_error(slogger, format(
                    "load_topology_state: last committed CDC generation time UUID ({}) present, but data missing", gen_id.id));
            }
            auto cnt = gen_rows->one().get_as<int64_t>("cnt");
            slogger.debug("load_topology_state: last committed CDC generation time UUID ({}), loaded {} ranges", gen_id.id, cnt);
        } else {
            if (!ret.normal_nodes.empty()) {
                on_internal_error(slogger,
                    "load_topology_state: normal nodes present but no committed CDC generations");
            }
        }

        if (some_row.has("unpublished_cdc_generations")) {
            ret.unpublished_cdc_generations = decode_cdc_generations_ids(deserialize_set_column(*topology(), some_row, "unpublished_cdc_generations"));
        }

        if (some_row.has("global_topology_request")) {
            auto req = service::global_topology_request_from_string(
                    some_row.get_as<sstring>("global_topology_request"));
            ret.global_request.emplace(req);
        }

        if (some_row.has("global_topology_request_id")) {
            ret.global_request_id = some_row.get_as<utils::UUID>("global_topology_request_id");
        }

        if (some_row.has("enabled_features")) {
            ret.enabled_features = decode_features(deserialize_set_column(*topology(), some_row, "enabled_features"));
        }

        if (some_row.has("session")) {
            ret.session = service::session_id(some_row.get_as<utils::UUID>("session"));
        }

        if (some_row.has("tablet_balancing_enabled")) {
            ret.tablet_balancing_enabled = some_row.get_as<bool>("tablet_balancing_enabled");
        } else {
            ret.tablet_balancing_enabled = true;
        }

        if (some_row.has("upgrade_state")) {
            ret.upgrade_state = service::upgrade_state_from_string(some_row.get_as<sstring>("upgrade_state"));
        } else {
            ret.upgrade_state = service::topology::upgrade_state_type::not_upgraded;
        }

        if (some_row.has("ignore_nodes")) {
            ret.ignored_nodes = decode_nodes_ids(deserialize_set_column(*topology(), some_row, "ignore_nodes"));
        }
    }

    co_return ret;
}

future<std::optional<service::topology_features>> system_keyspace::load_topology_features_state() {
    auto rs = co_await execute_cql(
        format("SELECT host_id, node_state, supported_features, enabled_features FROM system.{} WHERE key = '{}'", TOPOLOGY, TOPOLOGY));
    SCYLLA_ASSERT(rs);

    co_return decode_topology_features_state(std::move(rs));
}

std::optional<service::topology_features> system_keyspace::decode_topology_features_state(::shared_ptr<cql3::untyped_result_set> rs) {
    service::topology_features ret;

    if (rs->empty()) {
        return std::nullopt;
    }

    auto& some_row = *rs->begin();
    if (!some_row.has("enabled_features")) {
        return std::nullopt;
    }

    for (auto& row : *rs) {
        if (!row.has("host_id")) {
            // There are no clustering rows, only the static row.
            // Skip the whole loop, the static row is handled later.
            break;
        }

        raft::server_id host_id{row.get_as<utils::UUID>("host_id")};
        service::node_state nstate = service::node_state_from_string(row.get_as<sstring>("node_state"));
        if (row.has("supported_features") && nstate == service::node_state::normal) {
            ret.normal_supported_features.emplace(host_id, decode_features(deserialize_set_column(*topology(), row, "supported_features")));
        }
    }

    ret.enabled_features = decode_features(deserialize_set_column(*topology(), some_row, "enabled_features"));

    return ret;
}

future<cdc::topology_description>
system_keyspace::read_cdc_generation(utils::UUID id) {
    auto gen_desc = co_await read_cdc_generation_opt(id);

    if (!gen_desc) {
        on_internal_error(slogger, format(
            "read_cdc_generation: data for CDC generation {} not present", id));
    }

    co_return std::move(*gen_desc);
}

future<std::optional<cdc::topology_description>>
system_keyspace::read_cdc_generation_opt(utils::UUID id) {
    utils::chunked_vector<cdc::token_range_description> entries;
    co_await _qp.query_internal(
            format("SELECT range_end, streams, ignore_msb FROM {}.{} WHERE key = '{}' AND id = ?",
                   NAME, CDC_GENERATIONS_V3, cdc::CDC_GENERATIONS_V3_KEY),
            db::consistency_level::ONE,
            { id },
            1000, // for ~1KB rows, ~1MB page size
            [&] (const cql3::untyped_result_set_row& row) {
        std::vector<cdc::stream_id> streams;
        row.get_list_data<bytes>("streams", std::back_inserter(streams));
        entries.push_back(cdc::token_range_description{
            dht::token::from_int64(row.get_as<int64_t>("range_end")),
            std::move(streams),
            uint8_t(row.get_as<int8_t>("ignore_msb"))});
        return make_ready_future<stop_iteration>(stop_iteration::no);
    });

    if (entries.empty()) {
        co_return std::nullopt;
    }

    co_return cdc::topology_description{std::move(entries)};
}

future<> system_keyspace::sstables_registry_create_entry(sstring location, sstring status, sstables::sstable_state state, sstables::entry_descriptor desc) {
    static const auto req = format("INSERT INTO system.{} (location, generation, status, state, version, format) VALUES (?, ?, ?, ?, ?, ?)", SSTABLES_REGISTRY);
    slogger.trace("Inserting {}.{} into {}", location, desc.generation, SSTABLES_REGISTRY);
    co_await execute_cql(req, location, desc.generation, status, sstables::state_to_dir(state), fmt::to_string(desc.version), fmt::to_string(desc.format)).discard_result();
}

future<> system_keyspace::sstables_registry_update_entry_status(sstring location, sstables::generation_type gen, sstring status) {
    static const auto req = format("UPDATE system.{} SET status = ? WHERE location = ? AND generation = ?", SSTABLES_REGISTRY);
    slogger.trace("Updating {}.{} -> status={} in {}", location, gen, status, SSTABLES_REGISTRY);
    co_await execute_cql(req, status, location, gen).discard_result();
}

future<> system_keyspace::sstables_registry_update_entry_state(sstring location, sstables::generation_type gen, sstables::sstable_state state) {
    static const auto req = format("UPDATE system.{} SET state = ? WHERE location = ? AND generation = ?", SSTABLES_REGISTRY);
    auto new_state = sstables::state_to_dir(state);
    slogger.trace("Updating {}.{} -> state={} in {}", location, gen, new_state, SSTABLES_REGISTRY);
    co_await execute_cql(req, new_state, location, gen).discard_result();
}

future<> system_keyspace::sstables_registry_delete_entry(sstring location, sstables::generation_type gen) {
    static const auto req = format("DELETE FROM system.{} WHERE location = ? AND generation = ?", SSTABLES_REGISTRY);
    slogger.trace("Removing {}.{} from {}", location, gen, SSTABLES_REGISTRY);
    co_await execute_cql(req, location, gen).discard_result();
}

future<> system_keyspace::sstables_registry_list(sstring location, sstable_registry_entry_consumer consumer) {
    static const auto req = format("SELECT status, state, generation, version, format FROM system.{} WHERE location = ?", SSTABLES_REGISTRY);
    slogger.trace("Listing {} entries from {}", location, SSTABLES_REGISTRY);

    co_await _qp.query_internal(req, db::consistency_level::ONE, { location }, 1000, [ consumer = std::move(consumer) ] (const cql3::untyped_result_set::row& row) -> future<stop_iteration> {
        auto status = row.get_as<sstring>("status");
        auto state = sstables::state_from_dir(row.get_as<sstring>("state"));
        auto gen = sstables::generation_type(row.get_as<utils::UUID>("generation"));
        auto ver = sstables::version_from_string(row.get_as<sstring>("version"));
        auto fmt = sstables::format_from_string(row.get_as<sstring>("format"));
        sstables::entry_descriptor desc(gen, ver, fmt, sstables::component_type::TOC);
        co_await consumer(std::move(status), std::move(state), std::move(desc));
        co_return stop_iteration::no;
    });
}

future<service::topology_request_state> system_keyspace::get_topology_request_state(utils::UUID id, bool require_entry) {
    auto rs = co_await execute_cql(
        format("SELECT done, error FROM system.{} WHERE id = {}", TOPOLOGY_REQUESTS, id));
    if (!rs || rs->empty()) {
        if (require_entry) {
            on_internal_error(slogger, format("no entry for request id {}", id));
        } else {
            co_return service::topology_request_state{false, ""};
        }
    }

    auto& row = rs->one();
    sstring error;

    if (row.has("error")) {
        error = row.get_as<sstring>("error");
    }

    co_return service::topology_request_state{row.get_as<bool>("done"), std::move(error)};
}

system_keyspace::topology_requests_entry system_keyspace::topology_request_row_to_entry(utils::UUID id, const cql3::untyped_result_set_row& row) {
    topology_requests_entry entry;
    entry.id = id;
    if (row.has("initiating_host")) {
        entry.initiating_host = row.get_as<utils::UUID>("initiating_host");
    }
    if (row.has("request_type")) {
        entry.request_type = service::topology_request_from_string(row.get_as<sstring>("request_type"));
    }
    if (row.has("start_time")) {
        entry.start_time = row.get_as<db_clock::time_point>("start_time");
    }
    if (row.has("done")) {
        entry.done = row.get_as<bool>("done");
    }
    if (row.has("error")) {
        entry.error = row.get_as<sstring>("error");
    }
    if (row.has("end_time")) {
        entry.end_time = row.get_as<db_clock::time_point>("end_time");
    }
    return entry;
}

future<system_keyspace::topology_requests_entry> system_keyspace::get_topology_request_entry(utils::UUID id, bool require_entry) {
    auto rs = co_await execute_cql(
        format("SELECT * FROM system.{} WHERE id = {}", TOPOLOGY_REQUESTS, id));

    if (!rs || rs->empty()) {
        if (require_entry) {
            on_internal_error(slogger, format("no entry for request id {}", id));
        } else {
            co_return topology_requests_entry{
                .id = utils::null_uuid()
            };
        }
    }

    const auto& row = rs->one();
    co_return topology_request_row_to_entry(id, row);
}

future<system_keyspace::topology_requests_entries> system_keyspace::get_topology_request_entries(db_clock::time_point end_time_limit) {
    // Running requests.
    auto rs_running = co_await execute_cql(
        format("SELECT * FROM system.{} WHERE done = false ALLOW FILTERING", TOPOLOGY_REQUESTS));


    // Requests which finished after end_time_limit.
    auto rs_done = co_await execute_cql(
        format("SELECT * FROM system.{} WHERE end_time > {} ALLOW FILTERING", TOPOLOGY_REQUESTS, end_time_limit.time_since_epoch().count()));

    topology_requests_entries m;
    for (const auto& row: *rs_done) {
        auto id = row.get_as<utils::UUID>("id");
        m.emplace(id, topology_request_row_to_entry(id, row));
    }

    for (const auto& row: *rs_running) {
        auto id = row.get_as<utils::UUID>("id");
        // If a topology request finishes between the reads, it may be contained in both row sets.
        // Keep the latest info.
        m.emplace(id, topology_request_row_to_entry(id, row));
    }

    co_return m;
}

sstring system_keyspace_name() {
    return system_keyspace::NAME;
}

system_keyspace::system_keyspace(
        cql3::query_processor& qp, replica::database& db) noexcept
    : _qp(qp)
    , _db(db)
    , _cache(std::make_unique<local_cache>())
{
    _db.plug_system_keyspace(*this);
}

system_keyspace::~system_keyspace() {
}

future<> system_keyspace::shutdown() {
    _db.unplug_system_keyspace();
    co_return;
}

future<::shared_ptr<cql3::untyped_result_set>> system_keyspace::execute_cql(const sstring& query_string, const data_value_list& values) {
    return _qp.execute_internal(query_string, values, cql3::query_processor::cache_internal::yes);
}

} // namespace db
