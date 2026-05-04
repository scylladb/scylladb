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
#include "sstables/sstables.hh"
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
        return schema_builder(system_distributed_keyspace::NAME, system_distributed_keyspace::VIEW_BUILD_STATUS, std::make_optional(id))
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
        return schema_builder(system_distributed_keyspace::NAME, system_distributed_keyspace::CDC_DESC_V2, {id})
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
        return schema_builder(system_distributed_keyspace::NAME, system_distributed_keyspace::CDC_TIMESTAMPS, {id})
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

schema_ptr snapshot_sstables() {
    static thread_local auto schema = [] {
        auto id = generate_legacy_id(system_distributed_keyspace::NAME, system_distributed_keyspace::SNAPSHOT_SSTABLES);
        return schema_builder(system_distributed_keyspace::NAME, system_distributed_keyspace::SNAPSHOT_SSTABLES, std::make_optional(id))
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
                .with_hash_version()
                .build();
    }();
    return schema;
}

schema_ptr sstables_registry() {
    static thread_local auto schema = [] {
        auto id = generate_legacy_id(system_distributed_keyspace::NAME, system_distributed_keyspace::SSTABLES_REGISTRY);
        return schema_builder(system_distributed_keyspace::NAME, system_distributed_keyspace::SSTABLES_REGISTRY, std::make_optional(id))
                .with_column("table_id", uuid_type, column_kind::partition_key)
                .with_column("node_owner", uuid_type, column_kind::partition_key)
                .with_column("generation", timeuuid_type, column_kind::clustering_key)
                .with_column("status", utf8_type)
                .with_column("state", utf8_type)
                .with_column("version", utf8_type)
                .with_column("format", utf8_type)
                .set_comment("SSTables ownership table")
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
static std::vector<schema_ptr> ensured_tables(bool with_sstables_registry) {
    auto r = std::vector<schema_ptr>{
        view_build_status(),
        cdc_desc(),
        cdc_timestamps(),
        snapshot_sstables(),
    };
    if (with_sstables_registry) {
        r.push_back(sstables_registry());
    }
    return r;
}

std::vector<schema_ptr> system_distributed_keyspace::all_distributed_tables(bool with_sstables_registry) {
    auto r = std::vector<schema_ptr>{view_build_status(), cdc_desc(), cdc_timestamps(), snapshot_sstables()};
    if (with_sstables_registry) {
        r.push_back(sstables_registry());
    }
    return r;
}

system_distributed_keyspace::system_distributed_keyspace(cql3::query_processor& qp, service::migration_manager& mm, service::storage_proxy& sp, bool with_sstables_registry)
        : _qp(qp)
        , _mm(mm)
        , _sp(sp)
        , _with_sstables_registry(with_sstables_registry) {
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
        co_await coroutine::parallel_for_each(ensured_tables(_with_sstables_registry),
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

    co_await create_tables(ensured_tables(_with_sstables_registry));
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

future<> system_distributed_keyspace::insert_snapshot_sstable(sstring snapshot_name, sstring ks, sstring table, sstring dc, sstring rack, sstables::sstable_id sstable_id, dht::token first_token, dht::token last_token, sstring toc_name, sstring prefix, db::consistency_level cl) {
    // Not inserting the downloaded column so that re-populating on restore
    // retry doesn't overwrite downloaded=true set by a previous attempt
    static const sstring query = format("INSERT INTO {}.{} (snapshot_name, \"keyspace\", \"table\", datacenter, rack, first_token, sstable_id, last_token, toc_name, prefix) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) USING TTL {}", NAME, SNAPSHOT_SSTABLES, SNAPSHOT_SSTABLES_TTL_SECONDS);

    return _qp.execute_internal(
            query,
            cl,
            internal_distributed_query_state(),
            { std::move(snapshot_name), std::move(ks), std::move(table), std::move(dc), std::move(rack),
              dht::token::to_int64(first_token), sstable_id.uuid(), dht::token::to_int64(last_token), std::move(toc_name), std::move(prefix) },
            cql3::query_processor::cache_internal::yes).discard_result();
}

future<utils::chunked_vector<snapshot_sstable_entry>>
system_distributed_keyspace::get_snapshot_sstables(sstring snapshot_name, sstring ks, sstring table, sstring dc, sstring rack, db::consistency_level cl, std::optional<dht::token> start_token, std::optional<dht::token> end_token) const {
    utils::chunked_vector<snapshot_sstable_entry> sstables;

    static const sstring base_query = format("SELECT toc_name, prefix, sstable_id, first_token, last_token, downloaded FROM {}.{}"
        " WHERE snapshot_name = ? AND \"keyspace\" = ? AND \"table\" = ? AND datacenter = ? AND rack = ?", NAME, SNAPSHOT_SSTABLES);

    auto read_row = [&] (const cql3::untyped_result_set_row& row) {
            sstables.emplace_back(sstables::sstable_id(row.get_as<utils::UUID>("sstable_id")), dht::token::from_int64(row.get_as<int64_t>("first_token")), dht::token::from_int64(row.get_as<int64_t>("last_token")), row.get_as<sstring>("toc_name"), row.get_as<sstring>("prefix"), is_downloaded(row.get_or<bool>("downloaded", false)));
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

future<> system_distributed_keyspace::update_sstable_download_status(sstring snapshot_name,
                                                                     sstring ks,
                                                                     sstring table,
                                                                     sstring dc,
                                                                     sstring rack,
                                                                     sstables::sstable_id sstable_id,
                                                                     dht::token start_token,
                                                                     is_downloaded downloaded) const {
    static const sstring update_query = format("UPDATE {}.{} USING TTL {} SET downloaded = ? WHERE snapshot_name = ? AND \"keyspace\" = ? AND \"table\" = ? AND "
                                               "datacenter = ? AND rack = ? AND first_token = ? AND sstable_id = ?",
                                               NAME,
                                               SNAPSHOT_SSTABLES,
                                               SNAPSHOT_SSTABLES_TTL_SECONDS);
    co_await _qp.execute_internal(update_query,
                                  consistency_level::ONE,
                                  internal_distributed_query_state(),
                                  {downloaded == is_downloaded::yes ? true : false, snapshot_name, ks, table, dc, rack, dht::token::to_int64(start_token), sstable_id.uuid()},
                                  cql3::query_processor::cache_internal::no);
}


db::consistency_level system_distributed_keyspace::sstables_registry_write_cl() const {
    auto n = _sp.get_token_metadata_ptr()->count_normal_token_owners();
    return n > 1 ? db::consistency_level::EACH_QUORUM : db::consistency_level::ONE;
}

future<> system_distributed_keyspace::sstables_registry_create_entry(table_id tid, locator::host_id node_owner, sstring status, sstables::sstable_state state, sstables::entry_descriptor desc) {
    static const auto req = format("INSERT INTO {}.{} (table_id, node_owner, generation, status, state, version, format) VALUES (?, ?, ?, ?, ?, ?, ?)", NAME, SSTABLES_REGISTRY);
    dlogger.trace("Inserting {}.{}.{} into {}", tid, node_owner, desc.generation, SSTABLES_REGISTRY);
    co_await _qp.execute_internal(req, sstables_registry_write_cl(), internal_distributed_query_state(),
            { tid.id, node_owner.uuid(), data_value(desc.generation), status, sstring(sstables::state_to_dir(state)), fmt::to_string(desc.version), fmt::to_string(desc.format) },
            cql3::query_processor::cache_internal::yes).discard_result();
}

future<> system_distributed_keyspace::sstables_registry_update_entry_status(table_id tid, locator::host_id node_owner, sstables::generation_type gen, sstring status) {
    static const auto req = format("UPDATE {}.{} SET status = ? WHERE table_id = ? AND node_owner = ? AND generation = ?", NAME, SSTABLES_REGISTRY);
    dlogger.trace("Updating {}.{}.{} -> status={} in {}", tid, node_owner, gen, status, SSTABLES_REGISTRY);
    co_await _qp.execute_internal(req, sstables_registry_write_cl(), internal_distributed_query_state(),
            { status, tid.id, node_owner.uuid(), data_value(gen) },
            cql3::query_processor::cache_internal::yes).discard_result();
}

future<> system_distributed_keyspace::sstables_registry_update_entry_state(table_id tid, locator::host_id node_owner, sstables::generation_type gen, sstables::sstable_state state) {
    static const auto req = format("UPDATE {}.{} SET state = ? WHERE table_id = ? AND node_owner = ? AND generation = ?", NAME, SSTABLES_REGISTRY);
    dlogger.trace("Updating {}.{}.{} -> state={} in {}", tid, node_owner, gen, sstables::state_to_dir(state), SSTABLES_REGISTRY);
    co_await _qp.execute_internal(req, sstables_registry_write_cl(), internal_distributed_query_state(),
            { sstring(sstables::state_to_dir(state)), tid.id, node_owner.uuid(), data_value(gen) },
            cql3::query_processor::cache_internal::yes).discard_result();
}

future<> system_distributed_keyspace::sstables_registry_delete_entry(table_id tid, locator::host_id node_owner, sstables::generation_type gen) {
    static const auto req = format("DELETE FROM {}.{} WHERE table_id = ? AND node_owner = ? AND generation = ?", NAME, SSTABLES_REGISTRY);
    dlogger.trace("Removing {}.{}.{} from {}", tid, node_owner, gen, SSTABLES_REGISTRY);
    co_await _qp.execute_internal(req, sstables_registry_write_cl(), internal_distributed_query_state(),
            { tid.id, node_owner.uuid(), data_value(gen) },
            cql3::query_processor::cache_internal::yes).discard_result();
}

future<> system_distributed_keyspace::sstables_registry_list(table_id tid, locator::host_id node_owner, sstable_registry_entry_consumer consumer) {
    static const auto req = format("SELECT status, state, generation, version, format FROM {}.{} WHERE table_id = ? AND node_owner = ?", NAME, SSTABLES_REGISTRY);
    dlogger.trace("Listing {}.{} entries from {}", tid, node_owner, SSTABLES_REGISTRY);
    auto read_cl = _sp.get_token_metadata_ptr()->count_normal_token_owners() > 1 ? db::consistency_level::LOCAL_QUORUM : db::consistency_level::ONE;
    co_await _qp.query_internal(req, read_cl, { tid.id, node_owner.uuid() }, 1000,
            [ consumer = std::move(consumer) ] (const cql3::untyped_result_set::row& row) -> future<stop_iteration> {
        auto status = row.get_as<sstring>("status");
        auto state = row.get_as<sstring>("state");
        auto gen = row.get_as<utils::UUID>("generation");
        auto version = sstables::version_from_string(row.get_as<sstring>("version"));
        auto format = sstables::format_from_string(row.get_as<sstring>("format"));
        co_await consumer(std::move(status), sstables::state_from_dir(state), sstables::entry_descriptor(sstables::generation_type(gen), version, format, sstables::component_type::TOC));
        co_return stop_iteration::no;
    }, internal_distributed_query_state());
}

}
