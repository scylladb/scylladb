/*
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include <boost/range/algorithm.hpp>
#include <boost/range/algorithm_ext/push_back.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/adaptor/filtered.hpp>
#include <boost/range/adaptor/map.hpp>

#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/core/reactor.hh>
#include <seastar/json/json_elements.hh>
#include "system_keyspace.hh"
#include "types/types.hh"
#include "service/client_state.hh"
#include "service/query_state.hh"
#include "cql3/query_options.hh"
#include "cql3/query_processor.hh"
#include "cql3/untyped_result_set.hh"
#include "utils/fb_utilities.hh"
#include "utils/hash.hh"
#include "version.hh"
#include "thrift/server.hh"
#include "exceptions/exceptions.hh"
#include "cql3/query_processor.hh"
#include "query_context.hh"
#include "partition_slice_builder.hh"
#include "db/config.hh"
#include "gms/feature_service.hh"
#include "system_keyspace_view_types.hh"
#include "schema/schema_builder.hh"
#include "utils/hashers.hh"
#include "release.hh"
#include "log.hh"
#include <seastar/core/enum.hh>
#include "gms/inet_address.hh"
#include "index/secondary_index.hh"
#include "message/messaging_service.hh"
#include "mutation_query.hh"
#include "db/size_estimates_virtual_reader.hh"
#include "db/timeout_clock.hh"
#include "sstables/sstables.hh"
#include "db/view/build_progress_virtual_reader.hh"
#include "db/schema_tables.hh"
#include "index/built_indexes_virtual_reader.hh"
#include "gms/generation-number.hh"
#include "db/virtual_table.hh"
#include "service/storage_service.hh"
#include "protocol_server.hh"
#include "gms/gossiper.hh"
#include "service/paxos/paxos_state.hh"
#include "service/raft/raft_group_registry.hh"
#include "utils/build_id.hh"
#include "query-result-set.hh"
#include "idl/frozen_mutation.dist.hh"
#include "idl/frozen_mutation.dist.impl.hh"
#include <boost/algorithm/cxx11/any_of.hpp>
#include "client_data.hh"
#include "service/topology_state_machine.hh"
#include "sstables/open_info.hh"
#include "sstables/generation_type.hh"
#include "cdc/generation.hh"
#include "replica/tablets.hh"
#include "replica/query.hh"

using days = std::chrono::duration<int, std::ratio<24 * 3600>>;

namespace db {
namespace {
    const auto set_null_sharder = schema_builder::register_static_configurator([](const sstring& ks_name, const sstring& cf_name, schema_static_props& props) {
        // tables in the "system" keyspace which need to use null sharder
        static const std::unordered_set<sstring> system_ks_null_shard_tables = {
            schema_tables::SCYLLA_TABLE_SCHEMA_HISTORY,
            system_keyspace::RAFT,
            system_keyspace::RAFT_SNAPSHOTS,
            system_keyspace::RAFT_SNAPSHOT_CONFIG,
            system_keyspace::GROUP0_HISTORY,
            system_keyspace::DISCOVERY,
            system_keyspace::BROADCAST_KV_STORE,
            system_keyspace::TOPOLOGY,
            system_keyspace::CDC_GENERATIONS_V3,
            system_keyspace::TABLETS,
        };
        if (ks_name == system_keyspace::NAME && system_ks_null_shard_tables.contains(cf_name)) {
            props.use_null_sharder = true;
        }
    });
    const auto set_wait_for_sync_to_commitlog = schema_builder::register_static_configurator([](const sstring& ks_name, const sstring& cf_name, schema_static_props& props) {
        static const std::unordered_set<sstring> extra_durable_tables = {
            system_keyspace::PAXOS,
            system_keyspace::SCYLLA_LOCAL,
            system_keyspace::RAFT,
            system_keyspace::RAFT_SNAPSHOTS,
            system_keyspace::RAFT_SNAPSHOT_CONFIG,
            system_keyspace::DISCOVERY,
            system_keyspace::BROADCAST_KV_STORE,
            system_keyspace::TOPOLOGY,
            system_keyspace::CDC_GENERATIONS_V3,
            system_keyspace::TABLETS,
        };
        if (ks_name == system_keyspace::NAME && extra_durable_tables.contains(cf_name)) {
            props.wait_for_sync_to_commitlog = true;
        }
    });
    const auto set_use_schema_commitlog = schema_builder::register_static_configurator([](const sstring& ks_name, const sstring& cf_name, schema_static_props& props) {
        static const std::unordered_set<sstring> raft_tables = {
            system_keyspace::RAFT,
            system_keyspace::RAFT_SNAPSHOTS,
            system_keyspace::RAFT_SNAPSHOT_CONFIG,
            system_keyspace::GROUP0_HISTORY,
            system_keyspace::DISCOVERY,
            system_keyspace::TABLETS,
        };
        if (ks_name == system_keyspace::NAME && raft_tables.contains(cf_name)) {
            props.use_schema_commitlog = true;
            props.load_phase = system_table_load_phase::phase2;
        }
    });
}

std::unique_ptr<query_context> qctx = {};

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
            .with_column("shard_count", int32_type)
            .with_column("ignore_msb", int32_type)
            .with_column("supported_features", set_type_impl::get_instance(utf8_type, true))
            .with_column("new_cdc_generation_data_uuid", uuid_type, column_kind::static_column)
            .with_column("version", long_type, column_kind::static_column)
            .with_column("transition_state", utf8_type, column_kind::static_column)
            .with_column("current_cdc_generation_uuid", uuid_type, column_kind::static_column)
            .with_column("current_cdc_generation_timestamp", timestamp_type, column_kind::static_column)
            .with_column("global_topology_request", utf8_type, column_kind::static_column)
            .with_column("enabled_features", set_type_impl::get_instance(utf8_type, true), column_kind::static_column)
            .set_comment("Current state of topology change machine")
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
            /* The unique identifier of this generation. */
            .with_column("id", uuid_type, column_kind::partition_key)
            /* The generation describes a mapping from all tokens in the token ring to a set of stream IDs.
             * This mapping is built from a bunch of smaller mappings, each describing how tokens in a
             * subrange of the token ring are mapped to stream IDs; these subranges together cover the entire
             * token ring.  Each such range-local mapping is represented by a row of this table. The
             * clustering key of the row is the end of the range being described by this row. The start of
             * this range is the range_end of the previous row (in the clustering order, which is the integer
             * order) or of the last row of this partition if this is the first the first row. */
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
            /* Column used for sanity checking. For a given generation it's equal to the number of ranges in
             * this generation; thus, after the generation is fully inserted, it must be equal to the number
             * of rows in the partition. */
            .with_column("num_ranges", int32_type, column_kind::static_column)
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
            {"compaction_time", timestamp_type}
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
       builder.set_gc_grace_seconds(0);
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
            .with_column("uuid", uuid_type)
            .with_column("status", utf8_type)
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

static constexpr auto schema_gc_grace = std::chrono::duration_cast<std::chrono::seconds>(days(7)).count();

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
         {"local_read_repair_chance", double_type},
         {"max_compaction_threshold", int32_type},
         {"max_index_interval", int32_type},
         {"memtable_flush_period_in_ms", int32_type},
         {"min_compaction_threshold", int32_type},
         {"min_index_interval", int32_type},
         {"read_repair_chance", double_type},
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

future<> system_keyspace::setup_version(sharded<netw::messaging_service>& ms) {
    auto& cfg = _db.get_config();
    sstring req = fmt::format("INSERT INTO system.{} (key, release_version, cql_version, thrift_version, native_protocol_version, data_center, rack, partitioner, rpc_address, broadcast_address, listen_address) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                    , db::system_keyspace::LOCAL);

    return execute_cql(req, sstring(db::system_keyspace::LOCAL),
                            version::release(),
                            cql3::query_processor::CQL_VERSION,
                            ::cassandra::thrift_version,
                            to_sstring(unsigned(cql_serialization_format::latest().protocol_version())),
                            local_dc_rack().dc,
                            local_dc_rack().rack,
                            sstring(cfg.partitioner()),
                            utils::fb_utilities::get_broadcast_rpc_address().addr(),
                            utils::fb_utilities::get_broadcast_address().addr(),
                            ms.local().listen_address().addr()
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
    locator::endpoint_dc_rack _local_dc_rack_info;
    system_keyspace::bootstrap_state _state;
};

future<std::unordered_map<gms::inet_address, locator::endpoint_dc_rack>> system_keyspace::load_dc_rack_info() {
    auto msg = co_await execute_cql(format("SELECT peer, data_center, rack from system.{}", PEERS));

    std::unordered_map<gms::inet_address, locator::endpoint_dc_rack> ret;
    for (const auto& row : *msg) {
        net::inet_address peer = row.template get_as<net::inet_address>("peer");
        if (!row.has("data_center") || !row.has("rack")) {
            continue;
        }
        gms::inet_address gms_addr(std::move(peer));
        sstring dc = row.template get_as<sstring>("data_center");
        sstring rack = row.template get_as<sstring>("rack");

        ret.emplace(gms_addr, locator::endpoint_dc_rack{ dc, rack });
    }

    co_return ret;
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

future<> system_keyspace::setup(sharded<locator::snitch_ptr>& snitch, sharded<netw::messaging_service>& ms) {
    assert(this_shard_id() == 0);

    co_await setup_version(ms);
    co_await update_schema_version(_db.get_version());
    co_await build_bootstrap_info();
    co_await check_health();
    co_await db::schema_tables::save_system_keyspace_schema(_qp);
    // #2514 - make sure "system" is written to system_schema.keyspaces.
    co_await db::schema_tables::save_system_schema(_qp, NAME);
    co_await cache_truncation_record();

    if (snitch.local()->prefer_local()) {
        auto preferred_ips = co_await get_preferred_ips();
        co_await ms.invoke_on_all([&preferred_ips] (auto& ms) {
            return ms.init_local_preferred_ip_cache(preferred_ips);
        });
    }
}

struct truncation_record {
    static constexpr uint32_t current_magic = 0x53435452; // 'S' 'C' 'T' 'R'

    uint32_t magic;
    std::vector<db::replay_position> positions;
    db_clock::time_point time_stamp;
};

}

namespace db {

future<truncation_record> system_keyspace::get_truncation_record(table_id cf_id) {
    if (_db.get_config().ignore_truncation_record.is_set()) {
        truncation_record r{truncation_record::current_magic};
        return make_ready_future<truncation_record>(std::move(r));
    }
    sstring req = format("SELECT * from system.{} WHERE table_uuid = ?", TRUNCATED);
    return execute_cql(req, {cf_id.uuid()}).then([](::shared_ptr<cql3::untyped_result_set> rs) {
        truncation_record r{truncation_record::current_magic};

        for (const cql3::untyped_result_set_row& row : *rs) {
            auto shard = row.get_as<int32_t>("shard");
            auto ts = row.get_as<db_clock::time_point>("truncated_at");
            auto pos = row.get_as<int32_t>("position");
            auto id = row.get_as<int64_t>("segment_id");

            r.time_stamp = ts;
            r.positions.emplace_back(replay_position(shard, id, pos));
        }
        return make_ready_future<truncation_record>(std::move(r));
    });
}

// Read system.truncate table and cache last truncation time in `table` object for each table on every shard
future<> system_keyspace::cache_truncation_record() {
    if (_db.get_config().ignore_truncation_record.is_set()) {
        return make_ready_future<>();
    }
    sstring req = format("SELECT DISTINCT table_uuid, truncated_at from system.{}", TRUNCATED);
    return execute_cql(req).then([this] (::shared_ptr<cql3::untyped_result_set> rs) {
        return parallel_for_each(rs->begin(), rs->end(), [this] (const cql3::untyped_result_set_row& row) {
            auto table_uuid = table_id(row.get_as<utils::UUID>("table_uuid"));
            auto ts = row.get_as<db_clock::time_point>("truncated_at");

            return _db.container().invoke_on_all([table_uuid, ts] (replica::database& db) mutable {
                try {
                    replica::table& cf = db.find_column_family(table_uuid);
                    cf.cache_truncation_record(ts);
                } catch (replica::no_such_column_family&) {
                    slogger.debug("Skip caching truncation time for {} since the table is no longer present", table_uuid);
                }
            });
        });
    });
}

future<> system_keyspace::save_truncation_record(table_id id, db_clock::time_point truncated_at, db::replay_position rp) {
    sstring req = format("INSERT INTO system.{} (table_uuid, shard, position, segment_id, truncated_at) VALUES(?,?,?,?,?)", TRUNCATED);
    return qctx->qp().execute_internal(req, {id.uuid(), int32_t(rp.shard_id()), int32_t(rp.pos), int64_t(rp.base_id()), truncated_at}, cql3::query_processor::cache_internal::yes).discard_result().then([] {
        return force_blocking_flush(TRUNCATED);
    });
}

future<> system_keyspace::save_truncation_record(const replica::column_family& cf, db_clock::time_point truncated_at, db::replay_position rp) {
    return save_truncation_record(cf.schema()->id(), truncated_at, rp);
}

future<replay_positions> system_keyspace::get_truncated_position(table_id cf_id) {
    return get_truncation_record(cf_id).then([](truncation_record e) {
        return make_ready_future<replay_positions>(e.positions);
    });
}

future<db_clock::time_point> system_keyspace::get_truncated_at(table_id cf_id) {
    return get_truncation_record(cf_id).then([](truncation_record e) {
        return make_ready_future<db_clock::time_point>(e.time_stamp);
    });
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
        assert(str == dht::token::from_sstring(str).to_sstring());
        tset.insert(dht::token::from_sstring(str));
    }
    return tset;
}

future<> system_keyspace::update_tokens(gms::inet_address ep, const std::unordered_set<dht::token>& tokens)
{
    if (ep == utils::fb_utilities::get_broadcast_address()) {
        co_return co_await remove_endpoint(ep);
    }

    sstring req = format("INSERT INTO system.{} (peer, tokens) VALUES (?, ?)", PEERS);
    slogger.debug("INSERT INTO system.{} (peer, tokens) VALUES ({}, {})", PEERS, ep, tokens);
    auto set_type = set_type_impl::get_instance(utf8_type, true);
    co_await execute_cql(req, ep.addr(), make_set_value(set_type, prepare_tokens(tokens))).discard_result();
    co_await force_blocking_flush(PEERS);
}


future<std::unordered_map<gms::inet_address, std::unordered_set<dht::token>>> system_keyspace::load_tokens() {
    sstring req = format("SELECT peer, tokens FROM system.{}", PEERS);
    return execute_cql(req).then([] (::shared_ptr<cql3::untyped_result_set> cql_result) {
        std::unordered_map<gms::inet_address, std::unordered_set<dht::token>> ret;
        for (auto& row : *cql_result) {
            auto peer = gms::inet_address(row.get_as<net::inet_address>("peer"));
            if (row.has("tokens")) {
                ret.emplace(peer, decode_tokens(deserialize_set_column(*peers(), row, "tokens")));
            }
        }
        return ret;
    });
}

future<std::unordered_map<gms::inet_address, locator::host_id>> system_keyspace::load_host_ids() {
    sstring req = format("SELECT peer, host_id FROM system.{}", PEERS);
    return execute_cql(req).then([] (::shared_ptr<cql3::untyped_result_set> cql_result) {
        std::unordered_map<gms::inet_address, locator::host_id> ret;
        for (auto& row : *cql_result) {
            auto peer = gms::inet_address(row.get_as<net::inet_address>("peer"));
            if (row.has("host_id")) {
                ret.emplace(peer, locator::host_id(row.get_as<utils::UUID>("host_id")));
            }
        }
        return ret;
    });
}

future<std::vector<gms::inet_address>> system_keyspace::load_peers() {
    auto res = co_await execute_cql(format("SELECT peer, tokens FROM system.{}", PEERS));
    assert(res);

    std::vector<gms::inet_address> ret;
    for (auto& row: *res) {
        if (!row.has("tokens")) {
            // Ignore rows that don't have tokens. Such rows may
            // be introduced by code that persists parts of peer
            // information (such as RAFT_ID) which may potentially
            // race with deleting a peer (during node removal).
            continue;
        }
        ret.emplace_back(row.get_as<net::inet_address>("peer"));
    }
    co_return ret;
}

future<std::unordered_map<gms::inet_address, sstring>> system_keyspace::load_peer_features() {
    sstring req = format("SELECT peer, supported_features FROM system.{}", PEERS);
    return execute_cql(req).then([] (::shared_ptr<cql3::untyped_result_set> cql_result) {
        std::unordered_map<gms::inet_address, sstring> ret;
        for (auto& row : *cql_result) {
            if (row.has("supported_features")) {
                ret.emplace(row.get_as<net::inet_address>("peer"),
                        row.get_as<sstring>("supported_features"));
            }
        }
        return ret;
    });
}

future<std::unordered_map<gms::inet_address, gms::inet_address>> system_keyspace::get_preferred_ips() {
    sstring req = format("SELECT peer, preferred_ip FROM system.{}", PEERS);
    return execute_cql(req).then([] (::shared_ptr<cql3::untyped_result_set> cql_res_set) {
        std::unordered_map<gms::inet_address, gms::inet_address> res;

        for (auto& r : *cql_res_set) {
            if (r.has("preferred_ip")) {
                res.emplace(gms::inet_address(r.get_as<net::inet_address>("peer")),
                            gms::inet_address(r.get_as<net::inet_address>("preferred_ip")));
            }
        }

        return res;
    });
}

template <typename Value>
future<> system_keyspace::update_cached_values(gms::inet_address ep, sstring column_name, Value value) {
    return make_ready_future<>();
}

template <typename Value>
future<> system_keyspace::update_peer_info(gms::inet_address ep, sstring column_name, Value value) {
    if (ep == utils::fb_utilities::get_broadcast_address()) {
        co_return;
    }

    co_await update_cached_values(ep, column_name, value);
    sstring req = format("INSERT INTO system.{} (peer, {}) VALUES (?, ?)", PEERS, column_name);
    slogger.debug("INSERT INTO system.{} (peer, {}) VALUES ({}, {})", PEERS, column_name, ep, value);
    co_await execute_cql(req, ep.addr(), value).discard_result();
}
// sets are not needed, since tokens are updated by another method
template future<> system_keyspace::update_peer_info<sstring>(gms::inet_address ep, sstring column_name, sstring);
template future<> system_keyspace::update_peer_info<utils::UUID>(gms::inet_address ep, sstring column_name, utils::UUID);
template future<> system_keyspace::update_peer_info<net::inet_address>(gms::inet_address ep, sstring column_name, net::inet_address);

template <typename T>
future<> set_scylla_local_param_as(const sstring& key, const T& value) {
    sstring req = format("UPDATE system.{} SET value = ? WHERE key = ?", system_keyspace::SCYLLA_LOCAL);
    auto type = data_type_for<T>();
    co_await qctx->execute_cql(req, type->to_string_impl(data_value(value)), key).discard_result();
    // Flush the table so that the value is available on boot before commitlog replay.
    // database::maybe_init_schema_commitlog() depends on it.
    co_await smp::invoke_on_all([] () -> future<> {
        co_await qctx->qp().db().real_database().flush(db::system_keyspace::NAME, system_keyspace::SCYLLA_LOCAL);
    });
}

template <typename T>
future<std::optional<T>> get_scylla_local_param_as(const sstring& key) {
    sstring req = format("SELECT value FROM system.{} WHERE key = ?", system_keyspace::SCYLLA_LOCAL);
    return qctx->execute_cql(req, key).then([] (::shared_ptr<cql3::untyped_result_set> res)
            -> future<std::optional<T>> {
        if (res->empty() || !res->one().has("value")) {
            return make_ready_future<std::optional<T>>(std::optional<T>());
        }
        auto type = data_type_for<T>();
        return make_ready_future<std::optional<T>>(value_cast<T>(type->deserialize(
                    type->from_string(res->one().get_as<sstring>("value")))));
    });
}

future<> system_keyspace::set_scylla_local_param(const sstring& key, const sstring& value) {
    return set_scylla_local_param_as<sstring>(key, value);
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
    sstring req = format("DELETE FROM system.{} WHERE peer = ?", PEERS);
    slogger.debug("DELETE FROM system.{} WHERE peer = {}", PEERS, ep);
    co_await execute_cql(req, ep.addr()).discard_result();
    co_await force_blocking_flush(PEERS);
}

future<> system_keyspace::update_tokens(const std::unordered_set<dht::token>& tokens) {
    if (tokens.empty()) {
        return make_exception_future<>(std::invalid_argument("remove_endpoint should be used instead"));
    }

    sstring req = format("INSERT INTO system.{} (key, tokens) VALUES (?, ?)", LOCAL);
    auto set_type = set_type_impl::get_instance(utf8_type, true);
    return execute_cql(req, sstring(LOCAL), make_set_value(set_type, prepare_tokens(tokens))).discard_result().then([] {
        return force_blocking_flush(LOCAL);
    });
}

future<> system_keyspace::force_blocking_flush(sstring cfname) {
    assert(qctx);
    return qctx->_qp.invoke_on_all([cfname = std::move(cfname)] (cql3::query_processor& qp) {
        // if (!Boolean.getBoolean("cassandra.unsafesystem"))
        return qp.db().real_database().flush(NAME, cfname); // FIXME: get real database in another way
    });
}

/**
 * One of three things will happen if you try to read the system keyspace:
 * 1. files are present and you can read them: great
 * 2. no files are there: great (new node is assumed)
 * 3. files are present but you can't read them: bad
 */
future<> system_keyspace::check_health() {
    using namespace cql_transport::messages;
    sstring req = format("SELECT cluster_name FROM system.{} WHERE key=?", LOCAL);
    return execute_cql(req, sstring(LOCAL)).then([this] (::shared_ptr<cql3::untyped_result_set> msg) {
        if (msg->empty() || !msg->one().has("cluster_name")) {
            // this is a brand new node
            sstring ins_req = format("INSERT INTO system.{} (key, cluster_name) VALUES (?, ?)", LOCAL);
            auto cluster_name = _db.get_config().cluster_name();
            return execute_cql(ins_req, sstring(LOCAL), cluster_name).discard_result();
        } else {
            auto cluster_name = _db.get_config().cluster_name();
            auto saved_cluster_name = msg->one().get_as<sstring>("cluster_name");

            if (cluster_name != saved_cluster_name) {
                throw exceptions::configuration_exception("Saved cluster name " + saved_cluster_name + " != configured name " + cluster_name);
            }

            return make_ready_future<>();
        }
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

    co_await force_blocking_flush(v3::CDC_LOCAL);
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
    co_await force_blocking_flush(LOCAL);
    co_await container().invoke_on_all([state] (auto& sys_ks) {
        sys_ks._cache->_state = state;
    });
}

class cluster_status_table : public memtable_filling_virtual_table {
private:
    service::storage_service& _ss;
    gms::gossiper& _gossiper;
public:
    cluster_status_table(service::storage_service& ss, gms::gossiper& g)
            : memtable_filling_virtual_table(build_schema())
            , _ss(ss), _gossiper(g) {}

    static schema_ptr build_schema() {
        auto id = generate_legacy_id(system_keyspace::NAME, "cluster_status");
        return schema_builder(system_keyspace::NAME, "cluster_status", std::make_optional(id))
            .with_column("peer", inet_addr_type, column_kind::partition_key)
            .with_column("dc", utf8_type)
            .with_column("up", boolean_type)
            .with_column("status", utf8_type)
            .with_column("load", utf8_type)
            .with_column("tokens", int32_type)
            .with_column("owns", float_type)
            .with_column("host_id", uuid_type)
            .with_version(system_keyspace::generate_schema_version(id))
            .build();
    }

    future<> execute(std::function<void(mutation)> mutation_sink) override {
        return _ss.get_ownership().then([&, mutation_sink] (std::map<gms::inet_address, float> ownership) {
            const locator::token_metadata& tm = _ss.get_token_metadata();

            for (auto&& e : _gossiper.get_endpoint_states()) {
                auto endpoint = e.first;

                mutation m(schema(), partition_key::from_single_value(*schema(), data_value(endpoint).serialize_nonnull()));
                row& cr = m.partition().clustered_row(*schema(), clustering_key::make_empty()).cells();

                set_cell(cr, "up", _gossiper.is_alive(endpoint));
                set_cell(cr, "status", _gossiper.get_gossip_status(endpoint));
                set_cell(cr, "load", _gossiper.get_application_state_value(endpoint, gms::application_state::LOAD));

                auto hostid = tm.get_host_id_if_known(endpoint);
                if (hostid) {
                    set_cell(cr, "host_id", hostid->uuid());
                }

                if (tm.is_normal_token_owner(endpoint)) {
                    sstring dc = tm.get_topology().get_location(endpoint).dc;
                    set_cell(cr, "dc", dc);
                }

                if (ownership.contains(endpoint)) {
                    set_cell(cr, "owns", ownership[endpoint]);
                }

                set_cell(cr, "tokens", int32_t(tm.get_tokens(endpoint).size()));

                mutation_sink(std::move(m));
            }
        });
    }
};

class token_ring_table : public streaming_virtual_table {
private:
    replica::database& _db;
    service::storage_service& _ss;
public:
    token_ring_table(replica::database& db, service::storage_service& ss)
            : streaming_virtual_table(build_schema())
            , _db(db)
            , _ss(ss)
    {
        _shard_aware = true;
    }

    static schema_ptr build_schema() {
        auto id = generate_legacy_id(system_keyspace::NAME, "token_ring");
        return schema_builder(system_keyspace::NAME, "token_ring", std::make_optional(id))
            .with_column("keyspace_name", utf8_type, column_kind::partition_key)
            .with_column("start_token", utf8_type, column_kind::clustering_key)
            .with_column("endpoint", inet_addr_type, column_kind::clustering_key)
            .with_column("end_token", utf8_type)
            .with_column("dc", utf8_type)
            .with_column("rack", utf8_type)
            .with_version(system_keyspace::generate_schema_version(id))
            .build();
    }

    dht::decorated_key make_partition_key(const sstring& name) {
        return dht::decorate_key(*_s, partition_key::from_single_value(*_s, data_value(name).serialize_nonnull()));
    }

    clustering_key make_clustering_key(sstring start_token, gms::inet_address host) {
        return clustering_key::from_exploded(*_s, {
            data_value(start_token).serialize_nonnull(),
            data_value(host).serialize_nonnull()
        });
    }

    struct endpoint_details_cmp {
        bool operator()(const dht::endpoint_details& l, const dht::endpoint_details& r) const {
            return inet_addr_type->less(
                data_value(l._host).serialize_nonnull(),
                data_value(r._host).serialize_nonnull());
        }
    };

    future<> execute(reader_permit permit, result_collector& result, const query_restrictions& qr) override {
        struct decorated_keyspace_name {
            sstring name;
            dht::decorated_key key;
        };

        auto keyspace_names = boost::copy_range<std::vector<decorated_keyspace_name>>(
            _db.get_keyspaces()
                | boost::adaptors::filtered([] (auto&& e) {
                      auto&& rs = e.second.get_replication_strategy();
                      return rs.is_vnode_based();
                  })
                | boost::adaptors::transformed([this] (auto&& e) {
                    return decorated_keyspace_name{e.first, make_partition_key(e.first)};
        }));

        boost::sort(keyspace_names, [less = dht::ring_position_less_comparator(*_s)]
                (const decorated_keyspace_name& l, const decorated_keyspace_name& r) {
            return less(l.key, r.key);
        });

        for (const decorated_keyspace_name& e : keyspace_names) {
            auto&& dk = e.key;
            if (!this_shard_owns(dk) || !contains_key(qr.partition_range(), dk) || !_db.has_keyspace(e.name)) {
                continue;
            }

            std::vector<dht::token_range_endpoints> ranges = co_await _ss.describe_ring(e.name);

            co_await result.emit_partition_start(dk);
            boost::sort(ranges, [] (const dht::token_range_endpoints& l, const dht::token_range_endpoints& r) {
                return l._start_token < r._start_token;
            });

            for (dht::token_range_endpoints& range : ranges) {
                boost::sort(range._endpoint_details, endpoint_details_cmp());

                for (const dht::endpoint_details& detail : range._endpoint_details) {
                    clustering_row cr(make_clustering_key(range._start_token, detail._host));
                    set_cell(cr.cells(), "end_token", sstring(range._end_token));
                    set_cell(cr.cells(), "dc", sstring(detail._datacenter));
                    set_cell(cr.cells(), "rack", sstring(detail._rack));
                    co_await result.emit_row(std::move(cr));
                }
            }

            co_await result.emit_partition_end();
        }
    }
};

class snapshots_table : public streaming_virtual_table {
    distributed<replica::database>& _db;
public:
    explicit snapshots_table(distributed<replica::database>& db)
            : streaming_virtual_table(build_schema())
            , _db(db)
    {
        _shard_aware = true;
    }

    static schema_ptr build_schema() {
        auto id = generate_legacy_id(system_keyspace::NAME, "snapshots");
        return schema_builder(system_keyspace::NAME, "snapshots", std::make_optional(id))
            .with_column("keyspace_name", utf8_type, column_kind::partition_key)
            .with_column("table_name", utf8_type, column_kind::clustering_key)
            .with_column("snapshot_name", utf8_type, column_kind::clustering_key)
            .with_column("live", long_type)
            .with_column("total", long_type)
            .set_comment("Lists all the snapshots along with their size, dropped tables are not part of the listing.")
            .with_version(system_keyspace::generate_schema_version(id))
            .build();
    }

    dht::decorated_key make_partition_key(const sstring& name) {
        return dht::decorate_key(*_s, partition_key::from_single_value(*_s, data_value(name).serialize_nonnull()));
    }

    clustering_key make_clustering_key(sstring table_name, sstring snapshot_name) {
        return clustering_key::from_exploded(*_s, {
            data_value(std::move(table_name)).serialize_nonnull(),
            data_value(std::move(snapshot_name)).serialize_nonnull()
        });
    }

    future<> execute(reader_permit permit, result_collector& result, const query_restrictions& qr) override {
        struct decorated_keyspace_name {
            sstring name;
            dht::decorated_key key;
        };
        std::vector<decorated_keyspace_name> keyspace_names;

        for (const auto& [name, _] : _db.local().get_keyspaces()) {
            auto dk = make_partition_key(name);
            if (!this_shard_owns(dk) || !contains_key(qr.partition_range(), dk)) {
                continue;
            }
            keyspace_names.push_back({std::move(name), std::move(dk)});
        }

        boost::sort(keyspace_names, [less = dht::ring_position_less_comparator(*_s)]
                (const decorated_keyspace_name& l, const decorated_keyspace_name& r) {
            return less(l.key, r.key);
        });

        using snapshots_by_tables_map = std::map<sstring, std::map<sstring, replica::table::snapshot_details>>;

        class snapshot_reducer {
        private:
            snapshots_by_tables_map _result;
        public:
            future<> operator()(const snapshots_by_tables_map& value) {
                for (auto& [table_name, snapshots] : value) {
                    if (auto [_, added] = _result.try_emplace(table_name, std::move(snapshots)); added) {
                        continue;
                    }
                    auto& rp = _result.at(table_name);
                    for (auto&& [snapshot_name, snapshot_detail]: snapshots) {
                        if (auto [_, added] = rp.try_emplace(snapshot_name, std::move(snapshot_detail)); added) {
                            continue;
                        }
                        auto& detail = rp.at(snapshot_name);
                        detail.live += snapshot_detail.live;
                        detail.total += snapshot_detail.total;
                    }
                }
                return make_ready_future<>();
            }
            snapshots_by_tables_map get() && {
                return std::move(_result);
            }
        };

        for (auto& ks_data : keyspace_names) {
            co_await result.emit_partition_start(ks_data.key);

            const auto snapshots_by_tables = co_await _db.map_reduce(snapshot_reducer(), [ks_name_ = ks_data.name] (replica::database& db) mutable -> future<snapshots_by_tables_map> {
                auto ks_name = std::move(ks_name_);
                snapshots_by_tables_map snapshots_by_tables;
                for (auto& [_, table] : db.get_column_families()) {
                    if (table->schema()->ks_name() != ks_name) {
                        continue;
                    }
                    const auto unordered_snapshots = co_await table->get_snapshot_details();
                    snapshots_by_tables.emplace(table->schema()->cf_name(), std::map<sstring, replica::table::snapshot_details>(unordered_snapshots.begin(), unordered_snapshots.end()));
                }
                co_return snapshots_by_tables;
            });

            for (const auto& [table_name, snapshots] : snapshots_by_tables) {
                for (auto& [snapshot_name, details] : snapshots) {
                    clustering_row cr(make_clustering_key(table_name, snapshot_name));
                    set_cell(cr.cells(), "live", details.live);
                    set_cell(cr.cells(), "total", details.total);
                    co_await result.emit_row(std::move(cr));
                }

            }

            co_await result.emit_partition_end();
        }
    }
};

class protocol_servers_table : public memtable_filling_virtual_table {
private:
    service::storage_service& _ss;

    struct protocol_server_info {
        sstring name;
        sstring protocol;
        sstring protocol_version;
        std::vector<sstring> listen_addresses;

        explicit protocol_server_info(protocol_server& s)
            : name(s.name())
            , protocol(s.protocol())
            , protocol_version(s.protocol_version()) {
            for (const auto& addr : s.listen_addresses()) {
                listen_addresses.push_back(format("{}:{}", addr.addr(), addr.port()));
            }
        }
    };
public:
    explicit protocol_servers_table(service::storage_service& ss)
        : memtable_filling_virtual_table(build_schema())
        , _ss(ss) {
        _shard_aware = true;
    }

    static schema_ptr build_schema() {
        auto id = generate_legacy_id(system_keyspace::NAME, "protocol_servers");
        return schema_builder(system_keyspace::NAME, "protocol_servers", std::make_optional(id))
            .with_column("name", utf8_type, column_kind::partition_key)
            .with_column("protocol", utf8_type)
            .with_column("protocol_version", utf8_type)
            .with_column("listen_addresses", list_type_impl::get_instance(utf8_type, false))
            .set_comment("Lists all client protocol servers and their status.")
            .with_version(system_keyspace::generate_schema_version(id))
            .build();
    }

    future<> execute(std::function<void(mutation)> mutation_sink) override {
        // Servers are registered on shard 0 only
        const auto server_infos = co_await smp::submit_to(0ul, [&ss = _ss.container()] {
            return boost::copy_range<std::vector<protocol_server_info>>(ss.local().protocol_servers()
                    | boost::adaptors::transformed([] (protocol_server* s) { return protocol_server_info(*s); }));
        });
        for (auto server : server_infos) {
            auto dk = dht::decorate_key(*_s, partition_key::from_single_value(*schema(), data_value(server.name).serialize_nonnull()));
            if (!this_shard_owns(dk)) {
                continue;
            }
            mutation m(schema(), std::move(dk));
            row& cr = m.partition().clustered_row(*schema(), clustering_key::make_empty()).cells();
            set_cell(cr, "protocol", server.protocol);
            set_cell(cr, "protocol_version", server.protocol_version);
            std::vector<data_value> addresses(server.listen_addresses.begin(), server.listen_addresses.end());
            set_cell(cr, "listen_addresses", make_list_value(schema()->get_column_definition("listen_addresses")->type, std::move(addresses)));
            mutation_sink(std::move(m));
        }
    }
};

class runtime_info_table : public memtable_filling_virtual_table {
private:
    distributed<replica::database>& _db;
    service::storage_service& _ss;
    std::optional<dht::decorated_key> _generic_key;

private:
    std::optional<dht::decorated_key> maybe_make_key(sstring key) {
        auto dk = dht::decorate_key(*_s, partition_key::from_single_value(*schema(), data_value(std::move(key)).serialize_nonnull()));
        if (this_shard_owns(dk)) {
            return dk;
        }
        return std::nullopt;
    }

    void do_add_partition(std::function<void(mutation)>& mutation_sink, dht::decorated_key key, std::vector<std::pair<sstring, sstring>> rows) {
        mutation m(schema(), std::move(key));
        for (auto&& [ckey, cvalue] : rows) {
            row& cr = m.partition().clustered_row(*schema(), clustering_key::from_single_value(*schema(), data_value(std::move(ckey)).serialize_nonnull())).cells();
            set_cell(cr, "value", std::move(cvalue));
        }
        mutation_sink(std::move(m));
    }

    void add_partition(std::function<void(mutation)>& mutation_sink, sstring key, sstring value) {
        if (_generic_key) {
            do_add_partition(mutation_sink, *_generic_key, {{key, std::move(value)}});
        }
    }

    void add_partition(std::function<void(mutation)>& mutation_sink, sstring key, std::initializer_list<std::pair<sstring, sstring>> rows) {
        auto dk = maybe_make_key(std::move(key));
        if (dk) {
            do_add_partition(mutation_sink, std::move(*dk), std::move(rows));
        }
    }

    future<> add_partition(std::function<void(mutation)>& mutation_sink, sstring key, std::function<future<sstring>()> value_producer) {
        if (_generic_key) {
            do_add_partition(mutation_sink, *_generic_key, {{key, co_await value_producer()}});
        }
    }

    future<> add_partition(std::function<void(mutation)>& mutation_sink, sstring key, std::function<future<std::vector<std::pair<sstring, sstring>>>()> value_producer) {
        auto dk = maybe_make_key(std::move(key));
        if (dk) {
            do_add_partition(mutation_sink, std::move(*dk), co_await value_producer());
        }
    }

    template <typename T>
    future<T> map_reduce_tables(std::function<T(replica::table&)> map, std::function<T(T, T)> reduce = std::plus<T>{}) {
        class shard_reducer {
            T _v{};
            std::function<T(T, T)> _reduce;
        public:
            shard_reducer(std::function<T(T, T)> reduce) : _reduce(std::move(reduce)) { }
            future<> operator()(T v) {
                v = _reduce(_v, v);
                return make_ready_future<>();
            }
            T get() && { return std::move(_v); }
        };
        co_return co_await _db.map_reduce(shard_reducer(reduce), [map, reduce] (replica::database& db) {
            T val = {};
            for (auto& [_, table] : db.get_column_families()) {
               val = reduce(val, map(*table));
            }
            return val;
        });
    }
    template <typename T>
    future<T> map_reduce_shards(std::function<T()> map, std::function<T(T, T)> reduce = std::plus<T>{}, T initial = {}) {
        co_return co_await map_reduce(
                boost::irange(0u, smp::count),
                [map] (shard_id shard) {
                    return smp::submit_to(shard, [map] {
                        return map();
                    });
                },
                initial,
                reduce);
    }

public:
    explicit runtime_info_table(distributed<replica::database>& db, service::storage_service& ss)
        : memtable_filling_virtual_table(build_schema())
        , _db(db)
        , _ss(ss) {
        _shard_aware = true;
        _generic_key = maybe_make_key("generic");
    }

    static schema_ptr build_schema() {
        auto id = generate_legacy_id(system_keyspace::NAME, "runtime_info");
        return schema_builder(system_keyspace::NAME, "runtime_info", std::make_optional(id))
            .with_column("group", utf8_type, column_kind::partition_key)
            .with_column("item", utf8_type, column_kind::clustering_key)
            .with_column("value", utf8_type)
            .set_comment("Runtime system information.")
            .with_version(system_keyspace::generate_schema_version(id))
            .build();
    }

    future<> execute(std::function<void(mutation)> mutation_sink) override {
        co_await add_partition(mutation_sink, "gossip_active", [this] () -> future<sstring> {
            return _ss.is_gossip_running().then([] (bool running){
                return format("{}", running);
            });
        });
        co_await add_partition(mutation_sink, "load", [this] () -> future<sstring> {
            return map_reduce_tables<int64_t>([] (replica::table& tbl) {
                return tbl.get_stats().live_disk_space_used;
            }).then([] (int64_t load) {
                return format("{}", load);
            });
        });
        add_partition(mutation_sink, "uptime", format("{} seconds", std::chrono::duration_cast<std::chrono::seconds>(engine().uptime()).count()));
        add_partition(mutation_sink, "trace_probability", format("{:.2}", tracing::tracing::get_local_tracing_instance().get_trace_probability()));
        co_await add_partition(mutation_sink, "memory", [this] () {
            struct stats {
                // take the pre-reserved memory into account, as seastar only returns
                // the stats of memory managed by the seastar allocator, but we instruct
                // it to reserve addition memory for non-seastar allocator on per-shard
                // basis.
                uint64_t total = 0;
                uint64_t free = 0;
                static stats reduce(stats a, stats b) {
                    return stats{
                        a.total + b.total + db::config::wasm_udf_reserved_memory,
                        a.free + b.free};
                    };
            };
            return map_reduce_shards<stats>([] () {
                const auto& s = memory::stats();
                return stats{s.total_memory(), s.free_memory()};
            }, stats::reduce, stats{}).then([] (stats s) {
                return std::vector<std::pair<sstring, sstring>>{
                        {"total", format("{}", s.total)},
                        {"used", format("{}", s.total - s.free)},
                        {"free", format("{}", s.free)}};
            });
        });
        co_await add_partition(mutation_sink, "memtable", [this] () {
            struct stats {
                uint64_t total = 0;
                uint64_t free = 0;
                uint64_t entries = 0;
                static stats reduce(stats a, stats b) { return stats{a.total + b.total, a.free + b.free, a.entries + b.entries}; }
            };
            return map_reduce_tables<stats>([] (replica::table& t) {
                logalloc::occupancy_stats s;
                uint64_t partition_count = 0;
                for (replica::memtable* active_memtable : t.active_memtables()) {
                    s += active_memtable->region().occupancy();
                    partition_count += active_memtable->partition_count();
                }
                return stats{s.total_space(), s.free_space(), partition_count};
            }, stats::reduce).then([] (stats s) {
                return std::vector<std::pair<sstring, sstring>>{
                        {"memory_total", format("{}", s.total)},
                        {"memory_used", format("{}", s.total - s.free)},
                        {"memory_free", format("{}", s.free)},
                        {"entries", format("{}", s.entries)}};
            });
        });
        co_await add_partition(mutation_sink, "cache", [this] () {
            struct stats {
                uint64_t total = 0;
                uint64_t free = 0;
                uint64_t entries = 0;
                uint64_t hits = 0;
                uint64_t misses = 0;
                utils::rate_moving_average hits_moving_average;
                utils::rate_moving_average requests_moving_average;
                static stats reduce(stats a, stats b) {
                    return stats{
                        a.total + b.total,
                        a.free + b.free,
                        a.entries + b.entries,
                        a.hits + b.hits,
                        a.misses + b.misses,
                        a.hits_moving_average + b.hits_moving_average,
                        a.requests_moving_average + b.requests_moving_average};
                }
            };
            return _db.map_reduce0([] (replica::database& db) {
                stats res{};
                auto occupancy = db.row_cache_tracker().region().occupancy();
                res.total = occupancy.total_space();
                res.free = occupancy.free_space();
                res.entries = db.row_cache_tracker().partitions();
                for (const auto& [_, t] : db.get_column_families()) {
                    auto& cache_stats = t->get_row_cache().stats();
                    res.hits += cache_stats.hits.count();
                    res.misses += cache_stats.misses.count();
                    res.hits_moving_average += cache_stats.hits.rate();
                    res.requests_moving_average += (cache_stats.hits.rate() + cache_stats.misses.rate());
                }
                return res;
            }, stats{}, stats::reduce).then([] (stats s) {
                return std::vector<std::pair<sstring, sstring>>{
                        {"memory_total", format("{}", s.total)},
                        {"memory_used", format("{}", s.total - s.free)},
                        {"memory_free", format("{}", s.free)},
                        {"entries", format("{}", s.entries)},
                        {"hits", format("{}", s.hits)},
                        {"misses", format("{}", s.misses)},
                        {"hit_rate_total", format("{:.2}", static_cast<double>(s.hits) / static_cast<double>(s.hits + s.misses))},
                        {"hit_rate_recent", format("{:.2}", s.hits_moving_average.mean_rate)},
                        {"requests_total", format("{}", s.hits + s.misses)},
                        {"requests_recent", format("{}", static_cast<uint64_t>(s.requests_moving_average.mean_rate))}};
            });
        });
        co_await add_partition(mutation_sink, "incremental_backup_enabled", [this] () {
            return _db.map_reduce0([] (replica::database& db) {
                return boost::algorithm::any_of(db.get_keyspaces(), [] (const auto& id_and_ks) {
                    return id_and_ks.second.incremental_backups_enabled();
                });
            }, false, std::logical_or{}).then([] (bool res) -> sstring {
                return res ? "true" : "false";
            });
        });
    }
};

class versions_table : public memtable_filling_virtual_table {
public:
    explicit versions_table()
        : memtable_filling_virtual_table(build_schema()) {
        _shard_aware = false;
    }

    static schema_ptr build_schema() {
        auto id = generate_legacy_id(system_keyspace::NAME, "versions");
        return schema_builder(system_keyspace::NAME, "versions", std::make_optional(id))
            .with_column("key", utf8_type, column_kind::partition_key)
            .with_column("version", utf8_type)
            .with_column("build_mode", utf8_type)
            .with_column("build_id", utf8_type)
            .set_comment("Version information.")
            .with_version(system_keyspace::generate_schema_version(id))
            .build();
    }

    future<> execute(std::function<void(mutation)> mutation_sink) override {
        mutation m(schema(), partition_key::from_single_value(*schema(), data_value("local").serialize_nonnull()));
        row& cr = m.partition().clustered_row(*schema(), clustering_key::make_empty()).cells();
        set_cell(cr, "version", scylla_version());
        set_cell(cr, "build_mode", scylla_build_mode());
        set_cell(cr, "build_id", get_build_id());
        mutation_sink(std::move(m));
        return make_ready_future<>();
    }
};

class db_config_table final : public streaming_virtual_table {
    db::config& _cfg;

    static schema_ptr build_schema() {
        auto id = generate_legacy_id(system_keyspace::NAME, "config");
        return schema_builder(system_keyspace::NAME, "config", std::make_optional(id))
            .with_column("name", utf8_type, column_kind::partition_key)
            .with_column("type", utf8_type)
            .with_column("source", utf8_type)
            .with_column("value", utf8_type)
            .with_version(system_keyspace::generate_schema_version(id))
            .build();
    }

    future<> execute(reader_permit permit, result_collector& result, const query_restrictions& qr) override {
        struct config_entry {
            dht::decorated_key key;
            sstring_view type;
            sstring source;
            sstring value;
        };

        std::vector<config_entry> cfg;
        for (auto&& cfg_ref : _cfg.values()) {
            auto&& c = cfg_ref.get();
            dht::decorated_key dk = dht::decorate_key(*_s, partition_key::from_single_value(*_s, data_value(c.name()).serialize_nonnull()));
            if (this_shard_owns(dk)) {
                cfg.emplace_back(config_entry{ std::move(dk), c.type_name(), c.source_name(), c.value_as_json()._res });
            }
        }

        boost::sort(cfg, [less = dht::ring_position_less_comparator(*_s)]
                (const config_entry& l, const config_entry& r) {
            return less(l.key, r.key);
        });

        for (auto&& c : cfg) {
            co_await result.emit_partition_start(c.key);
            mutation m(schema(), c.key);
            clustering_row cr(clustering_key::make_empty());
            set_cell(cr.cells(), "type", c.type);
            set_cell(cr.cells(), "source", c.source);
            set_cell(cr.cells(), "value", c.value);
            co_await result.emit_row(std::move(cr));
            co_await result.emit_partition_end();
        }
    }

    virtual future<> apply(const frozen_mutation& fm) override {
        const mutation m = fm.unfreeze(_s);
        query::result_set rs(m);
        auto name = rs.row(0).get<sstring>("name");
        auto value = rs.row(0).get<sstring>("value");

        if (!_cfg.enable_cql_config_updates()) {
            return virtual_table::apply(fm); // will return back exceptional future
        }

        if (!name) {
            return make_exception_future<>(virtual_table_update_exception("option name is required"));
        }

        if (!value) {
            return make_exception_future<>(virtual_table_update_exception("option value is required"));
        }

        if (rs.row(0).cells().contains("type")) {
            return make_exception_future<>(virtual_table_update_exception("option type is immutable"));
        }

        if (rs.row(0).cells().contains("source")) {
            return make_exception_future<>(virtual_table_update_exception("option source is not updateable"));
        }

        return smp::submit_to(0, [&cfg = _cfg, name = std::move(*name), value = std::move(*value)] () mutable -> future<> {
            for (auto& c_ref : cfg.values()) {
                auto& c = c_ref.get();
                if (c.name() == name) {
                    std::exception_ptr ex;
                    try {
                        if (co_await c.set_value_on_all_shards(value, utils::config_file::config_source::CQL)) {
                            co_return;
                        } else {
                            ex = std::make_exception_ptr(virtual_table_update_exception("option is not live-updateable"));
                        }
                    } catch (boost::bad_lexical_cast&) {
                        ex = std::make_exception_ptr(virtual_table_update_exception("cannot parse option value"));
                    }
                    co_await coroutine::return_exception_ptr(std::move(ex));
                }
            }

            co_await coroutine::return_exception(virtual_table_update_exception("no such option"));
        });
    }

public:
    explicit db_config_table(db::config& cfg)
            : streaming_virtual_table(build_schema())
            , _cfg(cfg)
    {
        _shard_aware = true;
    }
};

class clients_table : public streaming_virtual_table {
    service::storage_service& _ss;

    static schema_ptr build_schema() {
        auto id = generate_legacy_id(system_keyspace::NAME, "clients");
        return schema_builder(system_keyspace::NAME, "clients", std::make_optional(id))
            .with_column("address", inet_addr_type, column_kind::partition_key)
            .with_column("port", int32_type, column_kind::clustering_key)
            .with_column("client_type", utf8_type, column_kind::clustering_key)
            .with_column("shard_id", int32_type)
            .with_column("connection_stage", utf8_type)
            .with_column("driver_name", utf8_type)
            .with_column("driver_version", utf8_type)
            .with_column("hostname", utf8_type)
            .with_column("protocol_version", int32_type)
            .with_column("ssl_cipher_suite", utf8_type)
            .with_column("ssl_enabled", boolean_type)
            .with_column("ssl_protocol", utf8_type)
            .with_column("username", utf8_type)
            .with_version(system_keyspace::generate_schema_version(id))
            .build();
    }

    dht::decorated_key make_partition_key(net::inet_address ip) {
        return dht::decorate_key(*_s, partition_key::from_single_value(*_s, data_value(ip).serialize_nonnull()));
    }

    clustering_key make_clustering_key(int32_t port, sstring clt) {
        return clustering_key::from_exploded(*_s, {
            data_value(port).serialize_nonnull(),
            data_value(clt).serialize_nonnull()
        });
    }

    future<> execute(reader_permit permit, result_collector& result, const query_restrictions& qr) override {
        // Collect
        using client_data_vec = utils::chunked_vector<client_data>;
        using shard_client_data = std::vector<client_data_vec>;
        std::vector<foreign_ptr<std::unique_ptr<shard_client_data>>> cd_vec;
        cd_vec.resize(smp::count);

        auto servers = co_await _ss.container().invoke_on(0, [] (auto& ss) { return ss.protocol_servers(); });
        co_await smp::invoke_on_all([&cd_vec_ = cd_vec, &servers_ = servers] () -> future<> {
            auto& cd_vec = cd_vec_;
            auto& servers = servers_;

            auto scd = std::make_unique<shard_client_data>();
            for (const auto& ps : servers) {
                client_data_vec cds = co_await ps->get_client_data();
                if (cds.size() != 0) {
                    scd->emplace_back(std::move(cds));
                }
            }
            cd_vec[this_shard_id()] = make_foreign(std::move(scd));
        });

        // Partition
        struct decorated_ip {
            dht::decorated_key key;
            net::inet_address ip;

            struct compare {
                dht::ring_position_less_comparator less;
                explicit compare(const class schema& s) : less(s) {}
                bool operator()(const decorated_ip& a, const decorated_ip& b) const {
                    return less(a.key, b.key);
                }
            };
        };

        decorated_ip::compare cmp(*_s);
        std::set<decorated_ip, decorated_ip::compare> ips(cmp);
        std::unordered_map<net::inet_address, client_data_vec> cd_map;
        for (int i = 0; i < smp::count; i++) {
            for (auto&& ps_cdc : *cd_vec[i]) {
                for (auto&& cd : ps_cdc) {
                    if (cd_map.contains(cd.ip)) {
                        cd_map[cd.ip].emplace_back(std::move(cd));
                    } else {
                        dht::decorated_key key = make_partition_key(cd.ip);
                        if (this_shard_owns(key) && contains_key(qr.partition_range(), key)) {
                            ips.insert(decorated_ip{std::move(key), cd.ip});
                            cd_map[cd.ip].emplace_back(std::move(cd));
                        }
                    }
                    co_await coroutine::maybe_yield();
                }
            }
        }

        // Emit
        for (const auto& dip : ips) {
            co_await result.emit_partition_start(dip.key);
            auto& clients = cd_map[dip.ip];

            boost::sort(clients, [] (const client_data& a, const client_data& b) {
                return a.port < b.port || a.client_type_str() < b.client_type_str();
            });

            for (const auto& cd : clients) {
                clustering_row cr(make_clustering_key(cd.port, cd.client_type_str()));
                set_cell(cr.cells(), "shard_id", cd.shard_id);
                set_cell(cr.cells(), "connection_stage", cd.stage_str());
                if (cd.driver_name) {
                    set_cell(cr.cells(), "driver_name", *cd.driver_name);
                }
                if (cd.driver_version) {
                    set_cell(cr.cells(), "driver_version", *cd.driver_version);
                }
                if (cd.hostname) {
                    set_cell(cr.cells(), "hostname", *cd.hostname);
                }
                if (cd.protocol_version) {
                    set_cell(cr.cells(), "protocol_version", *cd.protocol_version);
                }
                if (cd.ssl_cipher_suite) {
                    set_cell(cr.cells(), "ssl_cipher_suite", *cd.ssl_cipher_suite);
                }
                if (cd.ssl_enabled) {
                    set_cell(cr.cells(), "ssl_enabled", *cd.ssl_enabled);
                }
                if (cd.ssl_protocol) {
                    set_cell(cr.cells(), "ssl_protocol", *cd.ssl_protocol);
                }
                set_cell(cr.cells(), "username", cd.username ? *cd.username : sstring("anonymous"));
                co_await result.emit_row(std::move(cr));
            }
            co_await result.emit_partition_end();
        }
    }

public:
    clients_table(service::storage_service& ss)
            : streaming_virtual_table(build_schema())
            , _ss(ss)
    {
        _shard_aware = true;
    }
};

// Shows the current state of each Raft group.
// Currently it shows only the configuration.
// In the future we plan to add additional columns with more information.
class raft_state_table : public streaming_virtual_table {
private:
    sharded<service::raft_group_registry>& _raft_gr;

public:
    raft_state_table(sharded<service::raft_group_registry>& raft_gr)
        : streaming_virtual_table(build_schema())
        , _raft_gr(raft_gr) {
    }

    future<> execute(reader_permit permit, result_collector& result, const query_restrictions& qr) override {
        struct decorated_gid {
            raft::group_id gid;
            dht::decorated_key key;
            unsigned shard;
        };

        auto groups_and_shards = co_await _raft_gr.map([] (service::raft_group_registry& raft_gr) {
            return std::pair{raft_gr.all_groups(), this_shard_id()};
        });

        std::vector<decorated_gid> decorated_gids;
        for (auto& [groups, shard]: groups_and_shards) {
            for (auto& gid: groups) {
                decorated_gids.push_back(decorated_gid{gid, make_partition_key(gid), shard});
            }
        }

        // Must return partitions in token order.
        std::sort(decorated_gids.begin(), decorated_gids.end(), [less = dht::ring_position_less_comparator(*_s)]
                (const decorated_gid& l, const decorated_gid& r) { return less(l.key, r.key); });

        for (auto& [gid, dk, shard]: decorated_gids) {
            if (!contains_key(qr.partition_range(), dk)) {
                continue;
            }

            auto cfg_opt = co_await _raft_gr.invoke_on(shard,
                    [gid=gid] (service::raft_group_registry& raft_gr) -> std::optional<raft::configuration> {
                // Be ready for a group to disappear while we're querying.
                auto* srv = raft_gr.find_server(gid);
                if (!srv) {
                    return std::nullopt;
                }
                // FIXME: the configuration returned here is obtained from raft::fsm, it may not be
                // persisted yet, so this is not 100% correct. It may happen that we crash after
                // a config entry is appended in-memory in fsm but before it's persisted. It would be
                // incorrect to return the configuration observed during this window - after restart
                // the configuration would revert to the previous one.  Perhaps this is unlikely to
                // happen in practice, but for correctness we should add a way of querying the
                // latest persisted configuration.
                return srv->get_configuration();
            });

            if (!cfg_opt) {
                continue;
            }

            co_await result.emit_partition_start(dk);

            // List current config first, because 'C' < 'P' and the disposition
            // (ascii_type, 'CURRENT' vs 'PREVIOUS') is the first column in the clustering key.
            co_await emit_member_set(result, "CURRENT", cfg_opt->current);
            co_await emit_member_set(result, "PREVIOUS", cfg_opt->previous);

            co_await result.emit_partition_end();
        }
    }

private:
    static schema_ptr build_schema() {
        auto id = generate_legacy_id(system_keyspace::NAME, "raft_state");
        return schema_builder(system_keyspace::NAME, "raft_state", std::make_optional(id))
            .with_column("group_id", timeuuid_type, column_kind::partition_key)
            .with_column("disposition", ascii_type, column_kind::clustering_key) // can be 'CURRENT` or `PREVIOUS'
            .with_column("server_id", uuid_type, column_kind::clustering_key)
            .with_column("can_vote", boolean_type)
            .set_comment("Currently operating RAFT configuration")
            .with_version(system_keyspace::generate_schema_version(id))
            .build();
    }

    dht::decorated_key make_partition_key(raft::group_id gid) {
        // Make sure to use timeuuid_native_type so comparisons are done correctly
        // (we must emit partitions in the correct token order).
        return dht::decorate_key(*_s, partition_key::from_single_value(
                *_s, data_value(timeuuid_native_type{gid.uuid()}).serialize_nonnull()));
    }

    clustering_key make_clustering_key(std::string_view disposition, raft::server_id id) {
        return clustering_key::from_exploded(*_s, {
            data_value(disposition).serialize_nonnull(),
            data_value(id.uuid()).serialize_nonnull()
        });
    }

    future<> emit_member_set(result_collector& result, std::string_view disposition,
                             const raft::config_member_set& set) {
        // Must sort servers in clustering order (i.e. according to their IDs).
        // This is how `config_member::operator<` works so no need for custom comparator.
        std::vector<raft::config_member> members{set.begin(), set.end()};
        std::sort(members.begin(), members.end());
        for (auto& member: members) {
            clustering_row cr{make_clustering_key(disposition, member.addr.id)};
            set_cell(cr.cells(), "can_vote", member.can_vote);
            co_await result.emit_row(std::move(cr));
        }
    }
};

// Map from table's schema ID to table itself. Helps avoiding accidental duplication.
static thread_local std::map<table_id, std::unique_ptr<virtual_table>> virtual_tables;

void register_virtual_tables(distributed<replica::database>& dist_db, distributed<service::storage_service>& dist_ss, sharded<gms::gossiper>& dist_gossiper, sharded<service::raft_group_registry>& dist_raft_gr, db::config& cfg) {
    auto add_table = [] (std::unique_ptr<virtual_table>&& tbl) {
        virtual_tables[tbl->schema()->id()] = std::move(tbl);
    };

    auto& db = dist_db.local();
    auto& ss = dist_ss.local();
    auto& gossiper = dist_gossiper.local();

    // Add built-in virtual tables here.
    add_table(std::make_unique<cluster_status_table>(ss, gossiper));
    add_table(std::make_unique<token_ring_table>(db, ss));
    add_table(std::make_unique<snapshots_table>(dist_db));
    add_table(std::make_unique<protocol_servers_table>(ss));
    add_table(std::make_unique<runtime_info_table>(dist_db, ss));
    add_table(std::make_unique<versions_table>());
    add_table(std::make_unique<db_config_table>(cfg));
    add_table(std::make_unique<clients_table>(ss));
    add_table(std::make_unique<raft_state_table>(dist_raft_gr));
}

// Does not include virtual tables.
std::vector<schema_ptr> system_keyspace::all_tables(const db::config& cfg) {
    std::vector<schema_ptr> r;
    auto schema_tables = db::schema_tables::all_tables(schema_features::full());
    std::copy(schema_tables.begin(), schema_tables.end(), std::back_inserter(r));
    r.insert(r.end(), { built_indexes(), hints(), batchlog(), paxos(), local(),
                    peers(), peer_events(), range_xfers(),
                    compactions_in_progress(), compaction_history(),
                    sstable_activity(), size_estimates(), large_partitions(), large_rows(), large_cells(),
                    scylla_local(), db::schema_tables::scylla_table_schema_history(),
                    repair_history(),
                    v3::views_builds_in_progress(), v3::built_views(),
                    v3::scylla_views_builds_in_progress(),
                    v3::truncated(),
                    v3::cdc_local(),
    });
    if (cfg.consistent_cluster_management()) {
        r.insert(r.end(), {raft(), raft_snapshots(), raft_snapshot_config(), group0_history(), discovery()});

        if (cfg.check_experimental(db::experimental_features_t::feature::RAFT)) {
            r.insert(r.end(), {topology(), cdc_generations_v3()});
        }

        if (cfg.check_experimental(db::experimental_features_t::feature::BROADCAST_TABLES)) {
            r.insert(r.end(), {broadcast_kv_store()});
        }

        if (cfg.check_experimental(db::experimental_features_t::feature::TABLETS)) {
            r.insert(r.end(), {tablets()});
        }
    }
    if (cfg.check_experimental(db::experimental_features_t::feature::KEYSPACE_STORAGE_OPTIONS)) {
        r.insert(r.end(), {sstables_registry()});
    }
    // legacy schema
    r.insert(r.end(), {
                    // TODO: once we migrate hints/batchlog and add convertor
                    // legacy::hints(), legacy::batchlog(),
                    legacy::keyspaces(), legacy::column_families(),
                    legacy::columns(), legacy::triggers(), legacy::usertypes(),
                    legacy::functions(), legacy::aggregates(), });

    return r;
}

// Precondition: `register_virtual_tables` has finished.
static std::vector<schema_ptr> all_virtual_tables() {
    std::vector<schema_ptr> r;
    for (auto&& [_, vt] : virtual_tables) {
        r.push_back(vt->schema());
    }
    return r;
}

static void install_virtual_readers(db::system_keyspace& sys_ks, replica::database& db) {
    db.find_column_family(system_keyspace::size_estimates()).set_virtual_reader(mutation_source(db::size_estimates::virtual_reader(db, sys_ks)));
    db.find_column_family(system_keyspace::v3::views_builds_in_progress()).set_virtual_reader(mutation_source(db::view::build_progress_virtual_reader(db)));
    db.find_column_family(system_keyspace::built_indexes()).set_virtual_reader(mutation_source(db::index::built_indexes_virtual_reader(db)));

    for (auto&& [id, vt] : virtual_tables) {
        auto&& cf = db.find_column_family(vt->schema());
        cf.set_virtual_reader(vt->as_mutation_source());
        cf.set_virtual_writer([&vt = *vt] (const frozen_mutation& m) { return vt.apply(m); });
    }
}

static bool maybe_write_in_user_memory(schema_ptr s) {
    return (s.get() == system_keyspace::batchlog().get()) || (s.get() == system_keyspace::paxos().get())
            || s == system_keyspace::v3::scylla_views_builds_in_progress()
            || s == system_keyspace::raft();
}

future<> system_keyspace::make(
        locator::effective_replication_map_factory& erm_factory,
        replica::database& db, db::config& cfg, system_table_load_phase phase) {
    for (auto&& table : system_keyspace::all_tables(db.get_config())) {
        if (table->static_props().load_phase != phase) {
            continue;
        }

        co_await db.create_local_system_table(table, maybe_write_in_user_memory(table), erm_factory);
    }
}

future<> system_keyspace::initialize_virtual_tables(
        distributed<replica::database>& dist_db, distributed<service::storage_service>& dist_ss,
        sharded<gms::gossiper>& dist_gossiper, distributed<service::raft_group_registry>& dist_raft_gr,
        db::config& cfg) {
    register_virtual_tables(dist_db, dist_ss, dist_gossiper, dist_raft_gr, cfg);

    auto& db = dist_db.local();
    for (auto&& table: all_virtual_tables()) {
        co_await db.create_local_system_table(table, false, dist_ss.local().get_erm_factory());
    }

    install_virtual_readers(*this, db);
}

future<locator::host_id> system_keyspace::load_local_host_id() {
    sstring req = format("SELECT host_id FROM system.{} WHERE key=?", LOCAL);
    auto msg = co_await execute_cql(req, sstring(LOCAL));
    if (msg->empty() || !msg->one().has("host_id")) {
        co_return co_await set_local_random_host_id();
    } else {
        auto host_id = locator::host_id(msg->one().get_as<utils::UUID>("host_id"));
        slogger.info("Loaded local host id: {}", host_id);
        co_return host_id;
    }
}

future<locator::host_id> system_keyspace::set_local_random_host_id() {
    auto host_id = locator::host_id::create_random_id();
    slogger.info("Setting local host id to {}", host_id);

    sstring req = format("INSERT INTO system.{} (key, host_id) VALUES (?, ?)", LOCAL);
    co_await execute_cql(req, sstring(LOCAL), host_id.uuid());
    co_await force_blocking_flush(LOCAL);
    co_return host_id;
}

locator::endpoint_dc_rack system_keyspace::local_dc_rack() const {
    return _cache->_local_dc_rack_info;
}

future<foreign_ptr<lw_shared_ptr<reconcilable_result>>>
system_keyspace::query_mutations(distributed<replica::database>& db, const sstring& ks_name, const sstring& cf_name) {
    schema_ptr schema = db.local().find_schema(ks_name, cf_name);
    return replica::query_mutations(db, schema, query::full_partition_range, schema->full_slice(), db::no_timeout);
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

future<int> system_keyspace::increment_and_get_generation() {
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
    co_await force_blocking_flush(LOCAL);
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

future<service::paxos::paxos_state> system_keyspace::load_paxos_state(partition_key_view key, schema_ptr s, gc_clock::time_point now,
        db::timeout_clock::time_point timeout) {
    static auto cql = format("SELECT * FROM system.{} WHERE row_key = ? AND cf_id = ?", PAXOS);
    // FIXME: we need execute_cql_with_now()
    (void)now;
    auto f = qctx->execute_cql_with_timeout(cql, timeout, to_legacy(*key.get_compound_type(*s), key.representation()), s->id().uuid());
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
            // the value can be missing if it was pruned, suply empty one since
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
    return qctx->execute_cql_with_timeout(cql,
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
    return qctx->execute_cql_with_timeout(cql,
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
    return qctx->execute_cql_with_timeout(cql,
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

    return qctx->execute_cql_with_timeout(cql,
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

future<> system_keyspace::save_local_enabled_features(std::set<sstring> features) {
    auto features_str = fmt::to_string(fmt::join(features, ","));
    co_await set_scylla_local_param(gms::feature_service::ENABLED_FEATURES_KEY, features_str);
}

future<utils::UUID> system_keyspace::get_raft_group0_id() {
    auto opt = co_await get_scylla_local_param_as<utils::UUID>("raft_group0_id");
    co_return opt.value_or<utils::UUID>({});
}

future<> system_keyspace::set_raft_group0_id(utils::UUID uuid) {
    return set_scylla_local_param_as<utils::UUID>("raft_group0_id", uuid);
}

static constexpr auto GROUP0_HISTORY_KEY = "history";

future<utils::UUID> system_keyspace::get_last_group0_state_id() {
    auto rs = co_await qctx->execute_cql(
        format(
            "SELECT state_id FROM system.{} WHERE key = '{}' LIMIT 1",
            GROUP0_HISTORY, GROUP0_HISTORY_KEY));
    assert(rs);
    if (rs->empty()) {
        co_return utils::UUID{};
    }
    co_return rs->one().get_as<utils::UUID>("state_id");
}

future<bool> system_keyspace::group0_history_contains(utils::UUID state_id) {
    auto rs = co_await qctx->execute_cql(
        format(
            "SELECT state_id FROM system.{} WHERE key = '{}' AND state_id = ?",
            GROUP0_HISTORY, GROUP0_HISTORY_KEY),
        state_id);
    assert(rs);
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
        assert(cdef);
        row.cells().apply(*cdef, atomic_cell::make_live(*cdef->type, ts, cdef->type->decompose(description)));
    }
    if (gc_older_than) {
        using namespace std::chrono;
        assert(*gc_older_than >= gc_clock::duration{0});

        auto ts_micros = microseconds{ts};
        auto gc_older_than_micros = duration_cast<microseconds>(*gc_older_than);
        assert(gc_older_than_micros < ts_micros);

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
    assert(rs);
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

static constexpr auto GROUP0_UPGRADE_STATE_KEY = "group0_upgrade_state";

future<std::optional<sstring>> system_keyspace::load_group0_upgrade_state() {
    return get_scylla_local_param_as<sstring>(GROUP0_UPGRADE_STATE_KEY);
}

future<> system_keyspace::save_group0_upgrade_state(sstring value) {
    return set_scylla_local_param(GROUP0_UPGRADE_STATE_KEY, value);
}

static constexpr auto MUST_SYNCHRONIZE_TOPOLOGY_KEY = "must_synchronize_topology";

future<bool> system_keyspace::get_must_synchronize_topology() {
    auto opt = co_await get_scylla_local_param_as<bool>(MUST_SYNCHRONIZE_TOPOLOGY_KEY);
    co_return opt.value_or(false);
}

future<> system_keyspace::set_must_synchronize_topology(bool value) {
    return set_scylla_local_param_as<bool>(MUST_SYNCHRONIZE_TOPOLOGY_KEY, value);
}

static std::set<sstring> decode_features(const set_type_impl::native_type& features) {
    std::set<sstring> fset;
    for (auto& f : features) {
        fset.insert(value_cast<sstring>(std::move(f)));
    }
    return fset;
}

future<service::topology> system_keyspace::load_topology_state() {
    auto rs = co_await qctx->execute_cql(
        format("SELECT * FROM system.{} WHERE key = '{}'", TOPOLOGY, TOPOLOGY));
    assert(rs);

    service::topology_state_machine::topology_type ret;

    if (rs->empty()) {
        co_return ret;
    }

    for (auto& row : *rs) {
        raft::server_id host_id{row.get_as<utils::UUID>("host_id")};
        auto datacenter = row.get_as<sstring>("datacenter");
        auto rack = row.get_as<sstring>("rack");
        auto release_version = row.get_as<sstring>("release_version");
        uint32_t num_tokens = row.get_as<int32_t>("num_tokens");
        size_t shard_count = row.get_as<int32_t>("shard_count");
        uint8_t ignore_msb = row.get_as<int32_t>("ignore_msb");

        service::node_state nstate = service::node_state_from_string(row.get_as<sstring>("node_state"));

        std::optional<service::ring_slice> ring_slice;
        if (row.has("tokens")) {
            auto tokens = decode_tokens(deserialize_set_column(*topology(), row, "tokens"));

            if (tokens.empty()) {
                on_fatal_internal_error(slogger, format(
                    "load_topology_state: node {} has tokens column present but tokens are empty",
                    host_id));
            }

            ring_slice = service::ring_slice {
                .tokens = std::move(tokens),
            };
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
                ret.req_param.emplace(host_id, *replaced_id);
                break;
            case service::topology_request::rebuild:
                if (!rebuild_option) {
                    on_internal_error(slogger, fmt::format("rebuild_option is missing for a node {}", host_id));
                }
                ret.req_param.emplace(host_id, *rebuild_option);
                break;
            case service::topology_request::join:
                ret.req_param.emplace(host_id, num_tokens);
                break;
            default:
                // no parameters for other requests
                break;
            }
        } else {
            switch (nstate) {
            case service::node_state::replacing:
               // If a node is replacing abother node we need to know which node it is replacing
                if (!replaced_id) {
                    on_internal_error(slogger, fmt::format("replaced_id is missing for a node {}", host_id));
                }
                ret.req_param.emplace(host_id, *replaced_id);
                break;
            case service::node_state::rebuilding:
                // If a node is rebuilding it needs to know the parameter for the operation
                if (!rebuild_option) {
                    on_internal_error(slogger, fmt::format("rebuild_option is missing for a node {}", host_id));
                }
                ret.req_param.emplace(host_id, *rebuild_option);
                break;
            default:
                // no parameters for other operations
                break;
            }
        }

        std::unordered_map<raft::server_id, service::replica_state>* map = nullptr;
        if (nstate == service::node_state::normal) {
            map = &ret.normal_nodes;
            if (!ring_slice) {
                on_fatal_internal_error(slogger, format(
                    "load_topology_state: node {} in normal state but missing ring slice", host_id));
            }
        } else if (nstate == service::node_state::left) {
            ret.left_nodes.emplace(host_id);
        } else if (nstate == service::node_state::none) {
            map = &ret.new_nodes;
        } else {
            map = &ret.transition_nodes;
            if (nstate != service::node_state::left_token_ring && !ring_slice) {
                on_fatal_internal_error(slogger, format(
                    "load_topology_state: node {} in transitioning state but missing ring slice", host_id));
            }
        }
        if (map) {
            map->emplace(host_id, service::replica_state{
                nstate, std::move(datacenter), std::move(rack), std::move(release_version),
                ring_slice, shard_count, ignore_msb, std::move(supported_features)});
        }
    }

    {
        // Here we access static columns, any row will do.
        auto& some_row = *rs->begin();

        if (some_row.has("version")) {
            ret.version = some_row.get_as<service::topology::version_t>("version");
        }

        if (some_row.has("transition_state")) {
            ret.tstate = service::transition_state_from_string(some_row.get_as<sstring>("transition_state"));
        } else {
            // Any remaining transition_nodes must be in left_token_ring state
            auto it = std::find_if(ret.transition_nodes.begin(), ret.transition_nodes.end(),
                    [] (auto& p) { return p.second.state != service::node_state::left_token_ring; });
            if (it != ret.transition_nodes.end()) {
                on_internal_error(slogger, format(
                    "load_topology_state: topology not in transition state"
                    " but transition node {} in state {} is present", it->first, it->second.state));
            }
        }

        if (some_row.has("new_cdc_generation_data_uuid")) {
            ret.new_cdc_generation_data_uuid = some_row.get_as<utils::UUID>("new_cdc_generation_data_uuid");
        }

        if (some_row.has("current_cdc_generation_uuid")) {
            auto gen_uuid = some_row.get_as<utils::UUID>("current_cdc_generation_uuid");
            if (!some_row.has("current_cdc_generation_timestamp")) {
                on_internal_error(slogger, format(
                    "load_topology_state: current CDC generation UUID ({}) present, but timestamp missing", gen_uuid));
            }
            auto gen_ts = some_row.get_as<db_clock::time_point>("current_cdc_generation_timestamp");
            ret.current_cdc_generation_id = cdc::generation_id_v2 {
                .ts = gen_ts,
                .id = gen_uuid
            };

            // Sanity check for CDC generation data consistency.
            {
                auto gen_rows = co_await qctx->execute_cql(
                    format("SELECT count(range_end) as cnt, num_ranges FROM system.{} WHERE id = ?",
                           CDC_GENERATIONS_V3),
                    gen_uuid);
                assert(gen_rows);
                if (gen_rows->empty()) {
                    on_internal_error(slogger, format(
                        "load_topology_state: current CDC generation UUID ({}) present, but data missing", gen_uuid));
                }
                auto& row = gen_rows->one();
                auto counted_ranges = row.get_as<int64_t>("cnt");
                auto num_ranges = row.get_as<int32_t>("num_ranges");
                if (counted_ranges != num_ranges) {
                    on_internal_error(slogger, format(
                        "load_topology_state: inconsistency in CDC generation data (UUID {}):"
                        " counted {} ranges, should be {}", gen_uuid, counted_ranges, num_ranges));
                }
            }
        } else {
            if (!ret.normal_nodes.empty()) {
                on_internal_error(slogger,
                    "load_topology_state: normal nodes present but no current CDC generation ID");
            }
        }

        if (some_row.has("global_topology_request")) {
            auto req = service::global_topology_request_from_string(
                    some_row.get_as<sstring>("global_topology_request"));
            ret.global_request.emplace(req);
        }

        if (some_row.has("enabled_features")) {
            ret.enabled_features = decode_features(deserialize_set_column(*topology(), some_row, "enabled_features"));
        }
    }

    co_return ret;
}

future<int64_t> system_keyspace::get_topology_fence_version() {
    auto opt = co_await get_scylla_local_param_as<int64_t>("topology_fence_version");
    co_return opt.value_or<int64_t>(0);
}

future<> system_keyspace::update_topology_fence_version(int64_t value) {
    return set_scylla_local_param_as<int64_t>("topology_fence_version", value);
}

future<cdc::topology_description>
system_keyspace::read_cdc_generation(utils::UUID id) {
    std::vector<cdc::token_range_description> entries;
    auto num_ranges = 0;
    co_await _qp.query_internal(
            format("SELECT range_end, streams, ignore_msb, num_ranges FROM {}.{} WHERE id = ?",
                   NAME, CDC_GENERATIONS_V3),
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
        num_ranges = row.get_as<int32_t>("num_ranges");
        return make_ready_future<stop_iteration>(stop_iteration::no);
    });

    if (entries.empty()) {
        // The data must be present by precondition.
        on_internal_error(slogger, format(
            "read_cdc_generation: data for CDC generation {} not present", id));
    }

    if (entries.size() != num_ranges) {
        throw std::runtime_error(format(
            "read_cdc_generation: wrong number of rows. The `num_ranges` column claimed {} rows,"
            " but reading the partition returned {}.", num_ranges, entries.size()));
    }

    co_return cdc::topology_description{std::move(entries)};
}

future<> system_keyspace::sstables_registry_create_entry(sstring location, utils::UUID uuid, sstring status, sstables::entry_descriptor desc) {
    static const auto req = format("INSERT INTO system.{} (location, generation, uuid, status, version, format) VALUES (?, ?, ?, ?, ?, ?)", SSTABLES_REGISTRY);
    slogger.trace("Inserting {}.{}:{} into {}", location, desc.generation, uuid, SSTABLES_REGISTRY);
    co_await execute_cql(req, location, desc.generation, uuid, status, fmt::to_string(desc.version), fmt::to_string(desc.format)).discard_result();
}

future<utils::UUID> system_keyspace::sstables_registry_lookup_entry(sstring location, sstables::generation_type gen) {
    static const auto req = format("SELECT uuid FROM system.{} WHERE location = ? AND generation = ?", SSTABLES_REGISTRY);
    slogger.trace("Looking up {}.{} in {}", location, gen, SSTABLES_REGISTRY);
    auto msg = co_await execute_cql(req, location, gen);
    if (msg->empty() || !msg->one().has("uuid")) {
        slogger.trace("ERROR: Cannot find {}.{} in {}", location, gen, SSTABLES_REGISTRY);
        co_await coroutine::return_exception(std::runtime_error("No entry in sstables registry"));
    }

    auto uuid = msg->one().get_as<utils::UUID>("uuid");
    slogger.trace("Found {}.{}:{} in {}", location, gen, uuid, SSTABLES_REGISTRY);
    co_return uuid;
}

future<> system_keyspace::sstables_registry_update_entry_status(sstring location, sstables::generation_type gen, sstring status) {
    static const auto req = format("UPDATE system.{} SET status = ? WHERE location = ? AND generation = ?", SSTABLES_REGISTRY);
    slogger.trace("Updating {}.{} -> {} in {}", location, gen, status, SSTABLES_REGISTRY);
    co_await execute_cql(req, status, location, gen).discard_result();
}

future<> system_keyspace::sstables_registry_delete_entry(sstring location, sstables::generation_type gen) {
    static const auto req = format("DELETE FROM system.{} WHERE location = ? AND generation = ?", SSTABLES_REGISTRY);
    slogger.trace("Removing {}.{} from {}", location, gen, SSTABLES_REGISTRY);
    co_await execute_cql(req, location, gen).discard_result();
}

future<> system_keyspace::sstables_registry_list(sstring location, sstable_registry_entry_consumer consumer) {
    static const auto req = format("SELECT uuid, status, generation, version, format FROM system.{} WHERE location = ?", SSTABLES_REGISTRY);
    slogger.trace("Listing {} entries from {}", location, SSTABLES_REGISTRY);

    co_await _qp.query_internal(req, db::consistency_level::ONE, { location }, 1000, [ consumer = std::move(consumer) ] (const cql3::untyped_result_set::row& row) -> future<stop_iteration> {
        auto uuid = row.get_as<utils::UUID>("uuid");
        auto status = row.get_as<sstring>("status");
        auto gen = sstables::generation_type(row.get_as<utils::UUID>("generation"));
        auto ver = sstables::version_from_string(row.get_as<sstring>("version"));
        auto fmt = sstables::format_from_string(row.get_as<sstring>("format"));
        sstables::entry_descriptor desc("", "", "", gen, ver, fmt, sstables::component_type::TOC);
        co_await consumer(std::move(uuid), std::move(status), std::move(desc));
        co_return stop_iteration::no;
    });
}

sstring system_keyspace_name() {
    return system_keyspace::NAME;
}

system_keyspace::system_keyspace(
        cql3::query_processor& qp, replica::database& db, const locator::snitch_ptr& snitch) noexcept
    : _qp(qp)
    , _db(db)
    , _cache(std::make_unique<local_cache>())
{
    if (this_shard_id() == 0) {
        qctx = std::make_unique<query_context>(_qp.container());
    }

    _db.plug_system_keyspace(*this);

    // FIXME
    // This should be coupled with setup_version()'s part committing these values into
    // the system.local table. However, cql_test_env needs cached local_dc_rack strings,
    // but it doesn't call system_keyspace::setup() and thus ::setup_version() either
    _cache->_local_dc_rack_info.dc = snitch->get_datacenter();
    _cache->_local_dc_rack_info.rack = snitch->get_rack();
}

system_keyspace::~system_keyspace() {
}

future<> system_keyspace::shutdown() {
    _db.unplug_system_keyspace();
    co_return;
}

future<::shared_ptr<cql3::untyped_result_set>> system_keyspace::execute_cql(const sstring& query_string, const std::initializer_list<data_value>& values) {
    return _qp.execute_internal(query_string, values, cql3::query_processor::cache_internal::yes);
}

} // namespace db
