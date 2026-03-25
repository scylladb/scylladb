/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "utils/assert.hh"
#include "db/system_distributed_keyspace.hh"

#include "cql3/untyped_result_set.hh"
#include "replica/database.hh"
#include "db/consistency_level_type.hh"
#include "db/system_keyspace.hh"
#include "schema/schema_builder.hh"
#include "timeout_config.hh"
#include "types/types.hh"
#include "types/tuple.hh"
#include "types/set.hh"
#include "cdc/generation.hh"
#include "cql3/query_processor.hh"
#include "service/storage_proxy.hh"
#include "service/migration_manager.hh"
#include "locator/host_id.hh"

#include <seastar/core/seastar.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future-util.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/coroutine/parallel_for_each.hh>

#include <optional>
#include <vector>

static logging::logger dlogger("system_distributed_keyspace");
extern logging::logger cdc_log;

namespace db {

extern thread_local data_type cdc_streams_set_type;
thread_local data_type cdc_streams_set_type = set_type_impl::get_instance(bytes_type, false);


schema_ptr view_build_status() {
    static thread_local auto schema = [] {
        auto id = generate_legacy_id(system_distributed_keyspace::NAME, system_distributed_keyspace::VIEW_BUILD_STATUS);
        return schema_builder(this_smp_shard_count(), system_distributed_keyspace::NAME, system_distributed_keyspace::VIEW_BUILD_STATUS, std::make_optional(id))
                .with_column("keyspace_name", utf8_type, column_kind::partition_key)
                .with_column("view_name", utf8_type, column_kind::partition_key)
                .with_column("host_id", uuid_type, column_kind::clustering_key)
                .with_column("status", utf8_type)
                .with_hash_version()
                .build();
    }();
    return schema;
}


/* A user-facing table providing identifiers of the streams used in CDC generations. */
schema_ptr cdc_desc() {
    thread_local auto schema = [] {
        auto id = generate_legacy_id(system_distributed_keyspace::NAME, system_distributed_keyspace::CDC_DESC_V2);
        return schema_builder(this_smp_shard_count(), system_distributed_keyspace::NAME, system_distributed_keyspace::CDC_DESC_V2, {id})
                /* The timestamp of this CDC generation. */
                .with_column("time", timestamp_type, column_kind::partition_key)
                /* For convenience, the list of stream IDs in this generation is split into token ranges
                 * which the stream IDs were mapped to (by the partitioner) when the generation was created.  */
                .with_column("range_end", long_type, column_kind::clustering_key)
                /* The set of stream identifiers used in this CDC generation for the token range
                 * ending on `range_end`. */
                .with_column("streams", cdc_streams_set_type)
                .with_hash_version()
                .build();
    }();
    return schema;
}

/* A user-facing table providing CDC generation timestamps. */
schema_ptr cdc_timestamps() {
    thread_local auto schema = [] {
        auto id = generate_legacy_id(system_distributed_keyspace::NAME, system_distributed_keyspace::CDC_TIMESTAMPS);
        return schema_builder(this_smp_shard_count(), system_distributed_keyspace::NAME, system_distributed_keyspace::CDC_TIMESTAMPS, {id})
                /* This is a single-partition table. The partition key is always "timestamps". */
                .with_column("key", utf8_type, column_kind::partition_key)
                /* The timestamp of this CDC generation. */
                .with_column("time", reversed_type_impl::get_instance(timestamp_type), column_kind::clustering_key)
                /* Expiration time of this CDC generation (or null if not expired). */
                .with_column("expired", timestamp_type)
                .with_hash_version()
                .build();
    }();
    return schema;
}

static const sstring CDC_TIMESTAMPS_KEY = "timestamps";

schema_ptr snapshots() {
    static thread_local auto schema = [] {
        auto id = generate_legacy_id(system_distributed_keyspace::NAME, system_distributed_keyspace::SNAPSHOTS);
        return schema_builder(this_smp_shard_count(), system_distributed_keyspace::NAME, system_distributed_keyspace::SNAPSHOTS, std::make_optional(id))
                // Name of the snapshot
                .with_column("name", utf8_type, column_kind::partition_key)
                // When snapshot was created
                .with_column("created_at", timestamp_type)
                // When snapshot expires
                .with_column("expires_at", timestamp_type)
                .with_column("namespace_version", utf8_type)
                .with_column("manifest_version", utf8_type)
                .with_hash_version()
                .build();
    }();
    return schema;
}

schema_ptr snapshot_keyspaces() {
    static thread_local auto schema = [] {
        auto id = generate_legacy_id(system_distributed_keyspace::NAME, system_distributed_keyspace::SNAPSHOT_KEYSPACES);
        return schema_builder(this_smp_shard_count(), system_distributed_keyspace::NAME, system_distributed_keyspace::SNAPSHOT_KEYSPACES, std::make_optional(id))
                // Name of the snapshot
                .with_column("snapshot_name", utf8_type, column_kind::partition_key)
                // The name of keyspace
                .with_column("keyspace_name", utf8_type, column_kind::clustering_key)
                // Keyspace schema
                .with_column("keyspace_schema", utf8_type)
                .with_hash_version()
                .build();
    }();
    return schema;
}

schema_ptr snapshot_tables() {
    static thread_local auto schema = [] {
        auto id = generate_legacy_id(system_distributed_keyspace::NAME, system_distributed_keyspace::SNAPSHOT_TABLES);
        return schema_builder(this_smp_shard_count(), system_distributed_keyspace::NAME, system_distributed_keyspace::SNAPSHOT_TABLES, std::make_optional(id))
                // Name of the snapshot
                .with_column("snapshot_name", utf8_type, column_kind::partition_key)
                // The name of keyspace
                .with_column("keyspace_name", utf8_type, column_kind::clustering_key)
                // The name of table
                .with_column("table_name", utf8_type, column_kind::clustering_key)
                // Table ID
                .with_column("table_id", uuid_type)
                // Table type
                .with_column("type", int32_type)
                // Optional base table
                .with_column("base_table_id", uuid_type)
                // Table schema
                .with_column("table_schema", utf8_type)
                .with_column("tablet_layout", utf8_type)
                .with_hash_version()
                .build();
    }();
    return schema;
}


schema_ptr snapshot_tablets() {
    static thread_local auto schema = [] {
        auto id = generate_legacy_id(system_distributed_keyspace::NAME, system_distributed_keyspace::SNAPSHOT_TABLETS);
        return schema_builder(this_smp_shard_count(), system_distributed_keyspace::NAME, system_distributed_keyspace::SNAPSHOT_TABLETS, std::make_optional(id))
                // Name of the snapshot
                .with_column("snapshot_name", utf8_type, column_kind::partition_key)
                // The name of keyspace
                .with_column("keyspace_name", utf8_type, column_kind::partition_key)
                // The name of table
                .with_column("table_name", utf8_type, column_kind::partition_key)
                // The datacenter for which the tablet mapping was active
                .with_column("datacenter", utf8_type, column_kind::partition_key)
                // First token in the token range covered by this tablet
                .with_column("first_token", long_type, column_kind::clustering_key)
                // Tablet ID
                .with_column("tablet_id", long_type)
                // Last token in the token range covered by this tablet
                .with_column("last_token", long_type)
                // Repair time
                .with_column("repair_time", timestamp_type)
                // Repaired at
                .with_column("repaired_at", long_type)

                .with_hash_version()
                .build();
    }();
    return schema;
}

schema_ptr snapshot_nodes() {
    static thread_local auto schema = [] {
        auto id = generate_legacy_id(system_distributed_keyspace::NAME, system_distributed_keyspace::SNAPSHOT_NODES);
        return schema_builder(this_smp_shard_count(), system_distributed_keyspace::NAME, system_distributed_keyspace::SNAPSHOT_NODES, std::make_optional(id))
                // Name of the snapshot
                .with_column("snapshot_name", utf8_type, column_kind::partition_key)
                // Datacenter of the node (at snapshot)
                .with_column("datacenter", utf8_type, column_kind::clustering_key)
                // The rack of the node (at snapshot)
                .with_column("rack", utf8_type, column_kind::clustering_key)
                // Actual node
                .with_column("node", uuid_type, column_kind::clustering_key)
                .with_hash_version()
                .build();
    }();
    return schema;
}

schema_ptr snapshot_sstables() {
    static thread_local auto schema = [] {
        auto id = generate_legacy_id(system_distributed_keyspace::NAME, system_distributed_keyspace::SNAPSHOT_SSTABLES);
        return schema_builder(this_smp_shard_count(), system_distributed_keyspace::NAME, system_distributed_keyspace::SNAPSHOT_SSTABLES, std::make_optional(id))
                // Name of the snapshot
                .with_column("snapshot_name", utf8_type, column_kind::partition_key)
                // Keyspace where the snapshot was taken
                .with_column("keyspace", utf8_type, column_kind::partition_key)
                // Table within the keyspace
                .with_column("table", utf8_type, column_kind::partition_key)
                // Datacenter where this SSTable is located
                .with_column("datacenter", utf8_type, column_kind::partition_key)
                // Rack where this SSTable is located
                .with_column("rack", utf8_type, column_kind::partition_key)
                // First token in the token range covered by this SSTable
                .with_column("first_token", long_type, column_kind::clustering_key)
                // Unique identifier for the SSTable (UUID)
                .with_column("sstable_id", uuid_type, column_kind::clustering_key)
                // Last token in the token range covered by this SSTable
                .with_column("last_token", long_type)
                // TOC filename of the SSTable
                .with_column("toc_name", utf8_type)
                // Prefix path in object storage where the SSTable was backed up
                .with_column("prefix", utf8_type)
                // Flag if the SSTable was downloaded already
                .with_column("downloaded", boolean_type)
                // Node ID where sstable resides (optional)
                .with_column("node", uuid_type)
                // Tablet to which the sstable belonged at time of snapshot
                .with_column("tablet", long_type)
                // State - local, being_backed_up, remote_and_local, remote
                .with_column("state", int32_type)
                // Repair time
                .with_column("repaired_at", long_type)
                // Data size 
                .with_column("data_size", long_type)
                // Index size 
                .with_column("index_size", long_type)
                .with_hash_version()
                .build();
    }();
    return schema;
}

schema_ptr snapshot_remote_locations() {
    static thread_local auto schema = [] {
        auto id = generate_legacy_id(system_distributed_keyspace::NAME, system_distributed_keyspace::SNAPSHOT_REMOTE_LOCATIONS);
        return schema_builder(this_smp_shard_count(), system_distributed_keyspace::NAME, system_distributed_keyspace::SNAPSHOT_REMOTE_LOCATIONS, std::make_optional(id))
                // Name of the snapshot
                .with_column("snapshot_name", utf8_type, column_kind::partition_key)
                // The datacenter for which the location is used
                .with_column("datacenter", utf8_type, column_kind::clustering_key)
                // The endpoint of the location 
                .with_column("endpoint", utf8_type)
                // Storage bucket
                .with_column("bucket", utf8_type)
                // Storage prefix
                .with_column("prefix", utf8_type)
                // State - local, being_backed_up, remote_and_local, remote
                .with_column("state", int32_type)
                .with_hash_version()
                .build();
    }();
    return schema;
}

// This is the set of tables which this node ensures to exist in the cluster.
// It does that by announcing the creation of these schemas on initialization
// of the `system_distributed_keyspace` service (see `start()`), unless it first
// detects that the table already exists.
//
// Note: this module (system distributed keyspace) may also provide schema definitions
// and access functions for tables that are not listed here, i.e. tables which this node
// does not ensure to exist. Such definitions exist most likely for backward compatibility
// with previous versions of Scylla (needed during upgrades), but since they are not listed here,
// they won't be created in new clusters.
static std::vector<schema_ptr> ensured_tables() {
    return {
        view_build_status(),
        cdc_desc(),
        cdc_timestamps(),
        snapshots(),
        snapshot_remote_locations(),
        snapshot_keyspaces(),
        snapshot_tables(),
        snapshot_tablets(),
        snapshot_nodes(),
        snapshot_sstables(),
        snapshot_remote_locations(),
    };
}

std::vector<schema_ptr> system_distributed_keyspace::all_distributed_tables() {
    return {view_build_status(), cdc_desc(), cdc_timestamps(),
        snapshots(),
        snapshot_remote_locations(),
        snapshot_keyspaces(),
        snapshot_tables(),
        snapshot_tablets(),
        snapshot_nodes(),
        snapshot_sstables()
    };
}

system_distributed_keyspace::system_distributed_keyspace(cql3::query_processor& qp, service::migration_manager& mm, service::storage_proxy& sp)
        : _qp(qp)
        , _mm(mm)
        , _sp(sp) {
}

future<> system_distributed_keyspace::create_tables(std::vector<schema_ptr> tables) {
    if (this_shard_id() != 0) {
        _started = true;
        co_return;
    }

    auto db = _sp.data_dictionary();

    while (true) {
        // Check if there is any work to do before taking the group 0 guard.
        bool keyspaces_setup = db.has_keyspace(NAME);
        bool tables_setup = std::all_of(tables.begin(), tables.end(), [db] (schema_ptr t) { return db.has_schema(t->ks_name(), t->cf_name()); } );
        if (keyspaces_setup && tables_setup) {
            dlogger.info("system_distributed(_everywhere) keyspaces and tables are up-to-date. Not creating");
            _started = true;
            co_return;
        }

        auto group0_guard = co_await _mm.start_group0_operation();
        auto ts = group0_guard.write_timestamp();
        utils::chunked_vector<mutation> mutations;
        sstring description;

        auto ksm = keyspace_metadata::new_keyspace(
                NAME,
                "org.apache.cassandra.locator.SimpleStrategy",
                {{"replication_factor", "3"}},
                std::nullopt, std::nullopt);
        if (!db.has_keyspace(NAME)) {
            mutations = service::prepare_new_keyspace_announcement(db.real_database(), ksm, ts);
            description += format(" create {} keyspace;", NAME);
        } else {
            dlogger.info("{} keyspace is already present. Not creating", NAME);
        }

        // Get mutations for creating tables.
        auto num_keyspace_mutations = mutations.size();
        co_await coroutine::parallel_for_each(ensured_tables(),
                [this, &mutations, db, ts, ksm] (auto&& table) -> future<> {
            if (!db.has_schema(table->ks_name(), table->cf_name())) {
                co_return co_await service::prepare_new_column_family_announcement(mutations, _sp, *ksm, std::move(table), ts);
            }
        });
        if (mutations.size() > num_keyspace_mutations) {
            description += " create and update system_distributed(_everywhere) tables";
        } else {
            dlogger.info("All tables are present and up-to-date on start");
        }

        if (!mutations.empty()) {
            try {
                co_await _mm.announce(std::move(mutations), std::move(group0_guard), description);
            } catch (service::group0_concurrent_modification&) {
                dlogger.info("Concurrent operation is detected while starting, retrying.");
                continue;
            }
        }

        _started = true;
        co_return;
    }
}

future<> system_distributed_keyspace::start() {
    if (this_shard_id() != 0) {
        _started = true;
        co_return;
    }

    co_await create_tables(ensured_tables());
}

future<> system_distributed_keyspace::stop() {
    return make_ready_future<>();
}

static service::query_state& internal_distributed_query_state() {
    using namespace std::chrono_literals;
    const auto t = 10s;
    static timeout_config tc{ t, t, t, t, t, t, t };
    static thread_local service::client_state cs(service::client_state::internal_tag{}, tc);
    static thread_local service::query_state qs(cs, empty_service_permit());
    return qs;
};

/* We want to make sure that writes/reads to/from CDC management-related distributed tables
 * are consistent: a read following an acknowledged write to the same partition should contact
 * at least one of the replicas that the write contacted.
 *
 * Normally we would achieve that by always using CL = QUORUM,
 * but there's one special case when that's impossible: a single-node cluster. In that case we'll
 * use CL = ONE for writing the data, which will do the right thing -- saving the data in the only
 * possible replica. Until another node joins, reads will also use CL = ONE, retrieving the data
 * from the only existing replica.
 *
 * With system_distributed_everywhere tables things are simpler since they are using the Everywhere
 * replication strategy. We perform all writes to these tables with CL=ALL.
 * The number of replicas in the Everywhere strategy depends on the number of token owners:
 * if there's one token owner, then CL=ALL means one replica, if there's two, then two etc.
 * We don't need to modify the CL in the query parameters.
 */
static db::consistency_level quorum_if_many(size_t num_token_owners) {
    return num_token_owners > 1 ? db::consistency_level::QUORUM : db::consistency_level::ONE;
}

static future<utils::chunked_vector<mutation>> get_cdc_streams_descriptions_v2_mutation(
        const replica::database& db,
        db_clock::time_point time,
        const cdc::topology_description& desc) {
    auto s = db.find_schema(system_distributed_keyspace::NAME, system_distributed_keyspace::CDC_DESC_V2);

    auto ts = api::new_timestamp();
    utils::chunked_vector<mutation> res;
    res.emplace_back(s, partition_key::from_singular(*s, time));
    size_t size_estimate = 0;
    for (auto& e : desc.entries()) {
        // We want to keep each mutation below ~1 MB.
        if (size_estimate >= 1000 * 1000) {
            res.emplace_back(s, partition_key::from_singular(*s, time));
            size_estimate = 0;
        }

        set_type_impl::native_type streams;
        streams.reserve(e.streams.size());
        for (auto& stream : e.streams) {
            streams.push_back(data_value(stream.to_bytes()));
        }

        // We estimate 20 bytes per stream ID.
        // Stream IDs themselves weigh 16 bytes each (2 * sizeof(int64_t))
        // but there's metadata to be taken into account.
        // It has been verified experimentally that 20 bytes per stream ID is a good estimate.
        size_estimate += e.streams.size() * 20;
        res.back().set_cell(clustering_key::from_singular(*s, dht::token::to_int64(e.token_range_end)),
                to_bytes("streams"), make_set_value(cdc_streams_set_type, std::move(streams)), ts);

        co_await coroutine::maybe_yield();
    }

    co_return res;
}

future<>
system_distributed_keyspace::create_cdc_desc(
        db_clock::time_point time,
        const cdc::topology_description& desc,
        context ctx) {
    using namespace std::chrono_literals;

    auto ms = co_await get_cdc_streams_descriptions_v2_mutation(_qp.db().real_database(), time, desc);
    co_await max_concurrent_for_each(ms, 20, [&] (mutation& m) -> future<> {
        // We use the storage_proxy::mutate API since CQL is not the best for handling large batches.
        co_await _sp.mutate(
            { std::move(m) },
            quorum_if_many(ctx.num_token_owners),
            db::timeout_clock::now() + 30s,
            nullptr, // trace_state
            empty_service_permit(),
            db::allow_per_partition_rate_limit::no,
            false // raw_counters
        );
    });

    // Commit the description.
    co_await _qp.execute_internal(
            format("INSERT INTO {}.{} (key, time) VALUES (?, ?)", NAME, CDC_TIMESTAMPS),
            quorum_if_many(ctx.num_token_owners),
            internal_distributed_query_state(),
            { CDC_TIMESTAMPS_KEY, time },
            cql3::query_processor::cache_internal::no).discard_result();
}

future<bool>
system_distributed_keyspace::cdc_desc_exists(
        db_clock::time_point streams_ts,
        context ctx) {
    // Reading from this table on a freshly upgraded node that is the first to announce the CDC_TIMESTAMPS
    // schema would most likely result in replicas refusing to return data, telling the node that they can't
    // find the schema. Indeed, it takes some time for the nodes to synchronize their schema; schema is
    // only eventually consistent.
    //
    // This problem doesn't occur on writes since writes enforce schema pull if the receiving replica
    // notices that the write comes from an unknown schema, but it does occur on reads.
    //
    // Hence we work around it with a hack: we send a mutation with an empty partition to force our replicas
    // to pull the schema.
    //
    // This is not strictly necessary; the code that calls this function does it in a retry loop
    // so eventually, after the schema gets pulled, the read would succeed.
    // Still, the errors are also unnecessary and if we can get rid of them - let's do it.
    //
    // FIXME: find a more elegant way to deal with this ``problem''.
    if (!_forced_cdc_timestamps_schema_sync) {
        using namespace std::chrono_literals;
        auto s = _qp.db().find_schema(system_distributed_keyspace::NAME, system_distributed_keyspace::CDC_TIMESTAMPS);
        mutation m(s, partition_key::from_singular(*s, CDC_TIMESTAMPS_KEY));
        co_await _sp.mutate(
            { std::move(m) },
            quorum_if_many(ctx.num_token_owners),
            db::timeout_clock::now() + 10s,
            nullptr, // trace_state
            empty_service_permit(),
            db::allow_per_partition_rate_limit::no,
            false // raw_counters
        );

        _forced_cdc_timestamps_schema_sync = true;
    }

    // At this point replicas know the schema, we can perform the actual read...
    co_return co_await _qp.execute_internal(
            format("SELECT time FROM {}.{} WHERE key = ? AND time = ?", NAME, CDC_TIMESTAMPS),
            quorum_if_many(ctx.num_token_owners),
            internal_distributed_query_state(),
            { CDC_TIMESTAMPS_KEY, streams_ts },
            cql3::query_processor::cache_internal::no
    ).then([] (::shared_ptr<cql3::untyped_result_set> cql_result) -> bool {
        return !cql_result->empty() && cql_result->one().has("time");
    });
}

future<std::map<db_clock::time_point, cdc::streams_version>> 
system_distributed_keyspace::cdc_get_versioned_streams(db_clock::time_point not_older_than, context ctx) {
    auto timestamps_cql = co_await _qp.execute_internal(
            format("SELECT time FROM {}.{} WHERE key = ?", NAME, CDC_TIMESTAMPS),
            quorum_if_many(ctx.num_token_owners),
            internal_distributed_query_state(),
            { CDC_TIMESTAMPS_KEY },
            cql3::query_processor::cache_internal::no);

    std::vector<db_clock::time_point> timestamps;
    timestamps.reserve(timestamps_cql->size());
    for (auto& row : *timestamps_cql) {
        timestamps.push_back(row.get_as<db_clock::time_point>("time"));
    }

    // `time` is the table's clustering key, so the results are already sorted
    auto first = std::lower_bound(timestamps.rbegin(), timestamps.rend(), not_older_than);
    // need first gen _intersecting_ the timestamp.
    if (first != timestamps.rbegin()) {
        --first;
    }

    std::map<db_clock::time_point, cdc::streams_version> result;
    co_await max_concurrent_for_each(first, timestamps.rend(), 5, [this, &ctx, &result] (db_clock::time_point ts) -> future<> {
        auto streams_cql = co_await _qp.execute_internal(
                format("SELECT streams FROM {}.{} WHERE time = ?", NAME, CDC_DESC_V2),
                quorum_if_many(ctx.num_token_owners),
                internal_distributed_query_state(),
                { ts },
                cql3::query_processor::cache_internal::no);

        utils::chunked_vector<cdc::stream_id> ids;
        for (auto& row : *streams_cql) {
            row.get_list_data<bytes>("streams", std::back_inserter(ids));
            co_await coroutine::maybe_yield();
        }

        result.emplace(ts, cdc::streams_version{std::move(ids), ts});
    });

    co_return result;
}

future<db_clock::time_point> 
system_distributed_keyspace::cdc_current_generation_timestamp(context ctx) {
    auto timestamp_cql = co_await _qp.execute_internal(
            format("SELECT time FROM {}.{} WHERE key = ? limit 1", NAME, CDC_TIMESTAMPS),
            quorum_if_many(ctx.num_token_owners),
            internal_distributed_query_state(),
            { CDC_TIMESTAMPS_KEY },
            cql3::query_processor::cache_internal::no);

    co_return timestamp_cql->one().get_as<db_clock::time_point>("time");
}

// TODO: this is very hardcoded.
static constexpr uint64_t snapshot_table_ttl_seconds = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::days(3)).count();

snapshot_table_helper::snapshot_table_helper(cql3::query_processor& qp)
    : _qp(qp)
{}

future<> snapshot_table_helper::insert_snapshot_sstable(sstring snapshot_name, sstring ks, sstring table, sstring dc, sstring rack
    , sstables::sstable_id sstable_id, dht::token first_token, dht::token last_token, sstring toc_name, sstring prefix
    , locator::host_id node, size_t tablet_id, snapshot_state state, int64_t repaired_at
    , int64_t data_size, int64_t index_size
    , db::consistency_level cl) 
{
    co_await insert_snapshot_sstables(snapshot_name, ks, table, dc, rack, 
        std::span<const snapshot_sstable_entry>({ snapshot_sstable_entry{
            .sstable_id = sstable_id,
            .first_token = first_token,
            .last_token = last_token,
            .toc_name = std::move(toc_name),
            .prefix = std::move(prefix),
            .node = node,
            .tablet_id = tablet_id,
            .state = state,
            .repaired_at = repaired_at,
            .data_size = data_size,
            .index_size = index_size,
        }}), cl);
}

future<snapshot_sstables_progress> snapshot_table_helper::get_snapshot_sstables_progress(sstring snapshot_name, sstring ks, sstring table, sstring dc, sstring rack, db::consistency_level cl) const {
    static const sstring query = format("SELECT downloaded FROM {}.{}"
        " WHERE snapshot_name = ? AND \"keyspace\" = ? AND \"table\" = ? AND datacenter = ? AND rack = ?",
        system_distributed_keyspace::NAME, system_distributed_keyspace::SNAPSHOT_SSTABLES);
    snapshot_sstables_progress progress = {};
    co_await _qp.query_internal(query, cl, { snapshot_name, ks, table, dc, rack }, 1000, [&] (const cql3::untyped_result_set_row& row) {
        progress.nr_sstables++;
        if (row.get_or<bool>("downloaded", false)) {
            progress.nr_downloaded_sstables++;
        }
        return make_ready_future<stop_iteration>(stop_iteration::no);
    });
    co_return progress;
}

future<utils::chunked_vector<snapshot_sstable_entry>>
snapshot_table_helper::get_snapshot_sstables(sstring snapshot_name, sstring ks, sstring table, sstring dc, sstring rack, db::consistency_level cl, std::optional<dht::token> start_token, std::optional<dht::token> end_token) const {
    utils::chunked_vector<snapshot_sstable_entry> sstables;

    static const sstring base_query = format("SELECT toc_name, prefix, sstable_id, first_token, last_token, downloaded, node, tablet, state, repaired_at, data_size, index_size FROM {}.{}"
        " WHERE snapshot_name = ? AND \"keyspace\" = ? AND \"table\" = ? AND datacenter = ? AND rack = ?"
        , system_distributed_keyspace::NAME, system_distributed_keyspace::SNAPSHOT_SSTABLES
    );

    auto read_row = [&] (const cql3::untyped_result_set_row& row) {
            sstables.emplace_back(sstables::sstable_id(row.get_as<utils::UUID>("sstable_id"))
                , dht::token::from_int64(row.get_as<int64_t>("first_token"))
                , dht::token::from_int64(row.get_as<int64_t>("last_token"))
                , row.get_as<sstring>("toc_name")
                , row.get_as<sstring>("prefix")
                , is_downloaded(row.get_or<bool>("downloaded", false))
                , locator::host_id(row.get_or<utils::UUID>("node", utils::UUID{}))
                , row.get_or<int64_t>("tablet", 0)
                , snapshot_state(row.get_or<int32_t>("state", 0))
                , row.get_or<int64_t>("repaired_at", 0)
                , row.get_or<int64_t>("data_size", 0)
                , row.get_or<int64_t>("index_size", 0)
            );
            return make_ready_future<stop_iteration>(stop_iteration::no);
    };

    if (start_token && end_token) {
        co_await _qp.query_internal(
            base_query + " AND first_token >= ? AND first_token <= ?",
            cl,
            { snapshot_name, ks, table, dc, rack, dht::token::to_int64(*start_token), dht::token::to_int64(*end_token) },
            1000,
            read_row);
    } else if (start_token) {
        co_await _qp.query_internal(
            base_query + " AND first_token >= ?",
            cl,
            { snapshot_name, ks, table, dc, rack, dht::token::to_int64(*start_token) },
            1000,
            read_row);
    } else if (end_token) {
        co_await _qp.query_internal(
            base_query + " AND first_token <= ?",
            cl,
            { snapshot_name, ks, table, dc, rack, dht::token::to_int64(*end_token) },
            1000,
            read_row);
    } else {
        co_await _qp.query_internal(
            base_query,
            cl,
            { snapshot_name, ks, table, dc, rack },
            1000,
            read_row);
    }

    co_return sstables;
}

future<> snapshot_table_helper::update_sstable_download_status(sstring snapshot_name,
                                                               sstring ks,
                                                               sstring table,
                                                               sstring dc,
                                                               sstring rack,
                                                               sstables::sstable_id sstable_id,
                                                               dht::token start_token,
                                                               is_downloaded downloaded) const {
    static const sstring update_query = format("UPDATE {}.{} USING TTL {} SET downloaded = ? WHERE snapshot_name = ? AND \"keyspace\" = ? AND \"table\" = ? AND "
                                               "datacenter = ? AND rack = ? AND first_token = ? AND sstable_id = ?",
                                               system_distributed_keyspace::NAME,
                                               system_distributed_keyspace::SNAPSHOT_SSTABLES,
                                               snapshot_table_ttl_seconds);
    co_await _qp.execute_internal(update_query,
                                  consistency_level::ONE,
                                  internal_distributed_query_state(),
                                  {downloaded == is_downloaded::yes ? true : false, snapshot_name, ks, table, dc, rack, dht::token::to_int64(start_token), sstable_id.uuid()},
                                  cql3::query_processor::cache_internal::no);
}

future<> snapshot_table_helper::insert_snapshot_remote_location(sstring snapshot_name, sstring datacenter, sstring endpoint, sstring bucket, sstring prefix, snapshot_state state, db::consistency_level cl) {
    co_await insert_snapshot_remote_locations(std::span<const snapshot_remote_location_entry>({
        snapshot_remote_location_entry{
            .snapshot_name = std::move(snapshot_name),
            .datacenter = std::move(datacenter),
            .endpoint = std::move(endpoint),
            .bucket = std::move(bucket),
            .prefix = std::move(prefix),
            .state = state,
        }
    }), cl);
}

future<snapshot_remote_location_entry> snapshot_table_helper::get_snapshot_remote_location(sstring snapshot_name, sstring datacenter, db::consistency_level cl) const {
    static const sstring query = format("SELECT endpoint, bucket, prefix, state FROM {}.{} WHERE snapshot_name = ? AND datacenter = ?", system_distributed_keyspace::NAME, system_distributed_keyspace::SNAPSHOT_REMOTE_LOCATIONS);
    auto rs = co_await _qp.execute_internal(query, cl, internal_distributed_query_state(), {snapshot_name, datacenter}, cql3::query_processor::cache_internal::yes);
    if (rs->empty()) {
        throw std::runtime_error(format("No snapshot remote location found for snapshot '{}' in datacenter '{}'", snapshot_name, datacenter));
    }
    auto& row = rs->one();
    co_return snapshot_remote_location_entry{snapshot_name, datacenter
        , row.get_as<sstring>("endpoint")
        , row.get_as<sstring>("bucket")
        , row.get_as<sstring>("prefix")
        , snapshot_state(row.get_as<int32_t>("state"))
    };
}

/**
* Inserts a snapshot into system.dist table
*/
future<> snapshot_table_helper::insert_snapshot(const snapshot_entry& e, db::consistency_level cl) {
    static const sstring query = format(
        R"foo(INSERT INTO {}.{} (
            name, created_at, expires_at, namespace_version, manifest_version
        ) VALUES (
            ?, ?, ?, ?, ?
        ) USING TTL {})foo", system_distributed_keyspace::NAME, system_distributed_keyspace::SNAPSHOTS, snapshot_table_ttl_seconds
        );

    return _qp.execute_internal(
            query,
            cl,
            internal_distributed_query_state(),
            { e.name, e.created_at, e.expires_at, e.namespace_version, e.manifest_version },
            cql3::query_processor::cache_internal::yes).discard_result();
}

/**
* Find a snapshot entry. 
*/
future<std::optional<snapshot_entry>> snapshot_table_helper::get_snapshot(std::string_view snapshot_name, db::consistency_level cl) {
    std::optional<snapshot_entry> res;

    static const sstring query = format(R"foo(SELECT 
        name, created_at, expires_at, namespace_version, manifest_version
        FROM {}.{} 
        WHERE name = ?
        )foo", system_distributed_keyspace::NAME, system_distributed_keyspace::SNAPSHOTS
    );

    co_await _qp.query_internal(query, cl,
        { snapshot_name },
        10, [&](const cql3::untyped_result_set_row& row) {
            res = snapshot_entry {
                .name = row.get_as<sstring>("name"),
                .created_at = row.get_as<db_clock::time_point>("created_at"), 
                .expires_at = row.get_as<db_clock::time_point>("expires_at"), 
                .namespace_version = row.get_as<sstring>("namespace_version"), 
                .manifest_version = row.get_as<sstring>("manifest_version"), 
            };
            return make_ready_future<stop_iteration>(stop_iteration::yes);
        }
    );
    co_return res;
}

static constexpr size_t snapshot_max_parallel = 10;

/**
* Add remote locations to a snapshot
*/
future<> snapshot_table_helper::insert_snapshot_remote_locations(std::span<const snapshot_remote_location_entry> locations, db::consistency_level cl) {
    static const sstring query = format(
        R"foo(INSERT INTO {}.{} (
            snapshot_name, datacenter, endpoint, bucket, prefix, state
        ) VALUES (
            ?, ?, ?, ?, ?, ?
        ) USING TTL {})foo", system_distributed_keyspace::NAME, system_distributed_keyspace::SNAPSHOT_REMOTE_LOCATIONS, snapshot_table_ttl_seconds
        );

    for (auto chunk : locations | std::views::chunk(snapshot_max_parallel)) {
        co_await coroutine::parallel_for_each(chunk.begin(), chunk.end(), [&](const snapshot_remote_location_entry& e) {
            return _qp.execute_internal(
                        query, cl,
                        internal_distributed_query_state(),
                        { e.snapshot_name, e.datacenter, e.endpoint, e.bucket, e.prefix, int32_t(e.state) },
                        cql3::query_processor::cache_internal::yes).discard_result();
        });
    }
}

/**
* Get all remote locations in snapshot
*/
future<utils::chunked_vector<snapshot_remote_location_entry>> 
snapshot_table_helper::get_snapshot_remote_locations(std::string_view snapshot_name, db::consistency_level cl) {
    utils::chunked_vector<snapshot_remote_location_entry> entries;

    static const sstring query = format("SELECT snapshot_name, datacenter, endpoint, bucket, prefix, state FROM {}.{}"
        " WHERE snapshot_name = ?", system_distributed_keyspace::NAME, system_distributed_keyspace::SNAPSHOT_REMOTE_LOCATIONS
    );

    co_await _qp.query_internal(
        query, cl,
        { snapshot_name },
        1000, [&] (const cql3::untyped_result_set_row& row) {
            entries.emplace_back(row.get_as<sstring>("snapshot_name")
                , row.get_as<sstring>("datacenter")
                , row.get_as<sstring>("endpoint")
                , row.get_as<sstring>("bucket")
                , row.get_as<sstring>("prefix")
                , snapshot_state(row.get_as<int32_t>("state")) 
            );
            return make_ready_future<stop_iteration>(stop_iteration::no);
        }
    );

    co_return entries;
}

/**
* Add keyspaces to a snapshot
*/
future<> snapshot_table_helper::insert_snapshot_keyspaces(std::span<const snapshot_keyspace_entry> keyspaces, db::consistency_level cl) {
    static const sstring query = format(
        R"foo(INSERT INTO {}.{} (
            snapshot_name, keyspace_name, keyspace_schema
        ) VALUES (
            ?, ?, ?
        ) USING TTL {})foo", system_distributed_keyspace::NAME, system_distributed_keyspace::SNAPSHOT_KEYSPACES, snapshot_table_ttl_seconds
        );

    for (auto chunk : keyspaces | std::views::chunk(snapshot_max_parallel)) {
        co_await coroutine::parallel_for_each(chunk.begin(), chunk.end(), [&](const snapshot_keyspace_entry& e) {
            return _qp.execute_internal(
                        query, cl,
                        internal_distributed_query_state(),
                        { e.snapshot_name, e.keyspace_name, e.keyspace_schema },
                        cql3::query_processor::cache_internal::yes).discard_result();
        });
    }
}

/**
* Get all keyspaces in snapshot
*/
future<utils::chunked_vector<snapshot_keyspace_entry>> 
snapshot_table_helper::get_snapshot_keyspaces(std::string_view snapshot_name, db::consistency_level cl) {
    utils::chunked_vector<snapshot_keyspace_entry> entries;

    static const sstring query = format("SELECT snapshot_name, keyspace_name, keyspace_schema FROM {}.{}"
        " WHERE snapshot_name = ?", system_distributed_keyspace::NAME, system_distributed_keyspace::SNAPSHOT_KEYSPACES
    );

    co_await _qp.query_internal(
        query, cl,
        { snapshot_name },
        1000, [&] (const cql3::untyped_result_set_row& row) {
            entries.emplace_back(row.get_as<sstring>("snapshot_name")
                , row.get_as<sstring>("keyspace_name")
                , row.get_as<sstring>("keyspace_schema")
            );
            return make_ready_future<stop_iteration>(stop_iteration::no);
        }
    );

    co_return entries;
}

/**
* Add tables to a snapshot
*/
future<> snapshot_table_helper::insert_snapshot_tables(std::span<const snapshot_table_entry> tables, db::consistency_level cl) {
    static const sstring query = format(
        R"foo(INSERT INTO {}.{} (
            snapshot_name, keyspace_name, table_name, table_id, type, base_table_id, table_schema, tablet_layout
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?
        ) USING TTL {})foo", system_distributed_keyspace::NAME, system_distributed_keyspace::SNAPSHOT_TABLES, snapshot_table_ttl_seconds
        );

    for (auto chunk : tables | std::views::chunk(snapshot_max_parallel)) {
        co_await coroutine::parallel_for_each(chunk.begin(), chunk.end(), [&](const snapshot_table_entry& e) {
            return _qp.execute_internal(
                        query, cl,
                        internal_distributed_query_state(),
                        { 
                            e.snapshot_name, e.keyspace_name, e.table_name, e.table_id.uuid(),
                            int32_t(e.type), e.base_table_id.uuid(), e.table_schema, e.tablet_layout
                        },
                        cql3::query_processor::cache_internal::yes).discard_result();
        });
    }
}


/**
* Get all tables in snapshot, optionally restricted by keyspace
*/
future<utils::chunked_vector<snapshot_table_entry>> 
snapshot_table_helper::get_snapshot_tables(std::string_view snapshot_name, std::string_view keyspace, std::string_view table, db::consistency_level cl) {
    utils::chunked_vector<snapshot_table_entry> entries;

    static const sstring base_query = format(R"foo(SELECT 
        snapshot_name, keyspace_name, table_name, table_id, type, base_table_id, table_schema, tablet_layout
        FROM {}.{} WHERE snapshot_name = ?)foo", system_distributed_keyspace::NAME, system_distributed_keyspace::SNAPSHOT_TABLES
    );

    auto read_row = [&] (const cql3::untyped_result_set_row& row) {
        entries.emplace_back(row.get_as<sstring>("snapshot_name")
            , row.get_as<sstring>("keyspace_name")
            , row.get_as<sstring>("table_name")
            , table_id(row.get_as<utils::UUID>("table_id"))
            , snapshot_table_type(row.get_as<int32_t>("type"))
            , table_id(row.get_as<utils::UUID>("base_table_id"))
            , row.get_as<sstring>("table_schema")
            , row.get_as<sstring>("tablet_layout")
        );
        return make_ready_future<stop_iteration>(stop_iteration::no);
    };

    auto query = base_query;
    query_data_vector params = {
        sstring(snapshot_name)
    };

    if (!keyspace.empty()) {
        query += " AND keyspace_name = ?";
        params.emplace_back(keyspace);
    }
    if (!table.empty()) {
        query += " AND table_name = ?";
        params.emplace_back(table);
    }
    co_await _qp.query_internal(query, cl, params, 1000, read_row);
    co_return entries;
}

/**
 * Add tablets to a snapshot
 */
future<> snapshot_table_helper::insert_snapshot_tablets(std::string_view snapshot_name
        , std::string_view keyspace, std::string_view table, std::string_view datacenter
        , std::span<const snapshot_tablet_entry> tablets, db::consistency_level cl
    ) 
{
    static const sstring query = format(
        R"foo(INSERT INTO {}.{} (
            snapshot_name, keyspace_name, table_name, datacenter, first_token, tablet_id, last_token, repair_time, repaired_at
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?
        ) USING TTL {})foo", system_distributed_keyspace::NAME, system_distributed_keyspace::SNAPSHOT_TABLETS, snapshot_table_ttl_seconds
        );

    for (auto chunk : tablets | std::views::chunk(snapshot_max_parallel)) {
        co_await coroutine::parallel_for_each(chunk.begin(), chunk.end(), [&](const snapshot_tablet_entry& e) {
            return _qp.execute_internal(
                        query, cl,
                        internal_distributed_query_state(),
                        { 
                            sstring(snapshot_name), sstring(keyspace), sstring(table), sstring(datacenter), 
                            dht::token::to_int64(e.first_token), int64_t(e.tablet_id), dht::token::to_int64(e.last_token),
                            e.repair_time, e.repaired_at
                        },
                        cql3::query_processor::cache_internal::yes).discard_result();
        });
    }
}


/**
* Get all tables in snapshot, optionally restricted by keyspace
*/
future<utils::chunked_vector<snapshot_tablet_entry>> snapshot_table_helper::get_snapshot_tablets(std::string_view snapshot_name
        , std::string_view keyspace, std::string_view table, std::string_view datacenter
        , db::consistency_level cl
    ) 
{
    utils::chunked_vector<snapshot_tablet_entry> entries;

    static const sstring query = format(R"foo(SELECT 
        first_token, tablet_id, last_token, repair_time, repaired_at 
        FROM {}.{} WHERE snapshot_name = ? AND keyspace_name = ? AND table_name = ? AND datacenter = ?)foo", 
        system_distributed_keyspace::NAME, system_distributed_keyspace::SNAPSHOT_TABLETS
    );

    co_await _qp.query_internal(
        query, cl,
        { sstring(snapshot_name), sstring(keyspace), sstring(table), sstring(datacenter) },
        1000, [&] (const cql3::untyped_result_set_row& row) {
            entries.emplace_back(row.get_as<int64_t>("tablet_id")
                , dht::token::from_int64(row.get_as<int64_t>("first_token"))
                , dht::token::from_int64(row.get_as<int64_t>("last_token"))
                , row.get_as<db_clock::time_point>("repair_time")
                , row.get_as<int64_t>("repaired_at")
            );
            return make_ready_future<stop_iteration>(stop_iteration::no);
        }
    );

    co_return entries;
}

/**
 * Add nodes to a snapshot
 */
future<> snapshot_table_helper::insert_snapshot_nodes(std::string_view snapshot_name
    , std::span<const snapshot_node_entry> nodes
    , db::consistency_level cl
)
{
    static const sstring query = format(
        R"foo(INSERT INTO {}.{} (
            snapshot_name, datacenter, rack, node
        ) VALUES (
            ?, ?, ?, ?
        ) USING TTL {})foo", system_distributed_keyspace::NAME, system_distributed_keyspace::SNAPSHOT_NODES, snapshot_table_ttl_seconds
        );

    for (auto chunk : nodes | std::views::chunk(snapshot_max_parallel)) {
        co_await coroutine::parallel_for_each(chunk.begin(), chunk.end(), [&](const snapshot_node_entry& e) {
            return _qp.execute_internal(
                        query, cl,
                        internal_distributed_query_state(),
                        { 
                            sstring(snapshot_name), sstring(e.datacenter), sstring(e.rack), e.node.uuid()
                        },
                        cql3::query_processor::cache_internal::yes).discard_result();
        });
    }
}

/**
 * Get all nodes in snapshot for a given datacenter and optionally rack
 */
future<utils::chunked_vector<snapshot_node_entry>> snapshot_table_helper::get_snapshot_nodes(std::string_view snapshot_name
    , std::string_view datacenter
    , std::string_view rack
    , db::consistency_level cl
)
{
    utils::chunked_vector<snapshot_node_entry> entries;

    static const sstring base_query = format(R"foo(SELECT 
        datacenter, rack, node 
        FROM {}.{} WHERE snapshot_name = ?)foo", 
        system_distributed_keyspace::NAME, system_distributed_keyspace::SNAPSHOT_NODES
    );

    auto query = base_query;
    query_data_vector params = {
        sstring(snapshot_name)
    };

    if (!datacenter.empty()) {
        query += " AND datacenter = ?";
        params.emplace_back(sstring(datacenter));
    }

    if (!rack.empty()) {
        query += " AND rack = ?";
        params.emplace_back(sstring(rack));
    }

    co_await _qp.query_internal(
        query, cl,
        params,
        1000, [&] (const cql3::untyped_result_set_row& row) {
            entries.emplace_back(row.get_as<sstring>("datacenter"), row.get_as<sstring>("rack")
                , locator::host_id(row.get_as<utils::UUID>("node"))
            );
            return make_ready_future<stop_iteration>(stop_iteration::no);
        }
    );

    co_return entries;

}

template<typename Range>
requires std::ranges::range<Range>
static future<> do_insert_snapshot_sstables(cql3::query_processor& qp, std::string_view snapshot_name, std::string_view ks, std::string_view table, std::string_view dc, std::string_view rack
    , const Range& sstables
    , db::consistency_level cl
)
{
    static const sstring query = format("INSERT INTO {}.{} (snapshot_name, \"keyspace\", \"table\", datacenter, rack, first_token, sstable_id, last_token, toc_name, prefix, node, tablet, state, repaired_at, data_size, index_size) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) USING TTL {}"
        , system_distributed_keyspace::NAME, system_distributed_keyspace::SNAPSHOT_SSTABLES
        , snapshot_table_ttl_seconds
    );

    for (auto chunk : sstables | std::views::chunk(snapshot_max_parallel)) {
        co_await coroutine::parallel_for_each(chunk.begin(), chunk.end(), [&](const snapshot_sstable_entry& e) {
            return qp.execute_internal(
                    query,
                    cl,
                    internal_distributed_query_state(),
                    {
                      std::move(snapshot_name), std::move(ks), std::move(table), std::move(dc), std::move(rack),
                      dht::token::to_int64(e.first_token), e.sstable_id.uuid(), dht::token::to_int64(e.last_token), std::move(e.toc_name), std::move(e.prefix),
                      e.node.uuid(), int64_t(e.tablet_id), int32_t(e.state), e.repaired_at,
                      e.data_size, e.index_size
                    },
                    cql3::query_processor::cache_internal::yes).discard_result();
        });
    }
}

/* Inserts multiple SSTable entries for a given snapshot, keyspace, table, datacenter,
 * and rack. 
 */
future<> snapshot_table_helper::insert_snapshot_sstables(std::string_view snapshot_name, std::string_view ks, std::string_view table, std::string_view dc, std::string_view rack
    , std::span<const snapshot_sstable_entry> sstables
    , db::consistency_level cl
)
{
    co_await do_insert_snapshot_sstables(_qp, snapshot_name, ks, table, dc, rack, sstables, cl);
}

future<> snapshot_table_helper::insert_snapshot_sstables(std::string_view snapshot_name, std::string_view ks, std::string_view table, std::string_view dc, std::string_view rack
    , const utils::chunked_vector<snapshot_sstable_entry>& sstables
    , db::consistency_level cl
)
{
    co_await do_insert_snapshot_sstables(_qp, snapshot_name, ks, table, dc, rack, sstables, cl);
}


} // namespace db
