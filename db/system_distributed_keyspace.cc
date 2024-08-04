/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
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

#include <boost/range/adaptor/transformed.hpp>

#include <optional>
#include <vector>

static logging::logger dlogger("system_distributed_keyspace");
extern logging::logger cdc_log;

namespace db {
namespace {
    const auto set_wait_for_sync_to_commitlog = schema_builder::register_static_configurator([](const sstring& ks_name, const sstring& cf_name, schema_static_props& props) {
        if ((ks_name == system_distributed_keyspace::NAME_EVERYWHERE && cf_name == system_distributed_keyspace::CDC_GENERATIONS_V2) ||
            (ks_name == system_distributed_keyspace::NAME && cf_name == system_distributed_keyspace::CDC_TOPOLOGY_DESCRIPTION))
        {
            props.wait_for_sync_to_commitlog = true;
        }
    });
}

extern thread_local data_type cdc_streams_set_type;
thread_local data_type cdc_streams_set_type = set_type_impl::get_instance(bytes_type, false);

/* See `token_range_description` struct */
thread_local data_type cdc_streams_list_type = list_type_impl::get_instance(bytes_type, false);
thread_local data_type cdc_token_range_description_type = tuple_type_impl::get_instance(
        { long_type             // dht::token token_range_end;
        , cdc_streams_list_type // std::vector<stream_id> streams;
        , byte_type             // uint8_t sharding_ignore_msb;
        });
thread_local data_type cdc_generation_description_type = list_type_impl::get_instance(cdc_token_range_description_type, false);

schema_ptr view_build_status() {
    static thread_local auto schema = [] {
        auto id = generate_legacy_id(system_distributed_keyspace::NAME, system_distributed_keyspace::VIEW_BUILD_STATUS);
        return schema_builder(system_distributed_keyspace::NAME, system_distributed_keyspace::VIEW_BUILD_STATUS, std::make_optional(id))
                .with_column("keyspace_name", utf8_type, column_kind::partition_key)
                .with_column("view_name", utf8_type, column_kind::partition_key)
                .with_column("host_id", uuid_type, column_kind::clustering_key)
                .with_column("status", utf8_type)
                .with_version(system_keyspace::generate_schema_version(id))
                .build();
    }();
    return schema;
}

/* An internal table used by nodes to exchange CDC generation data. */
schema_ptr cdc_generations_v2() {
    thread_local auto schema = [] {
        auto id = generate_legacy_id(system_distributed_keyspace::NAME_EVERYWHERE, system_distributed_keyspace::CDC_GENERATIONS_V2);
        return schema_builder(system_distributed_keyspace::NAME_EVERYWHERE, system_distributed_keyspace::CDC_GENERATIONS_V2, {id})
                /* The unique identifier of this generation. */
                .with_column("id", uuid_type, column_kind::partition_key)
                /* The generation describes a mapping from all tokens in the token ring to a set of stream IDs.
                 * This mapping is built from a bunch of smaller mappings, each describing how tokens in a subrange
                 * of the token ring are mapped to stream IDs; these subranges together cover the entire token ring.
                 * Each such range-local mapping is represented by a row of this table.
                 * The clustering key of the row is the end of the range being described by this row.
                 * The start of this range is the range_end of the previous row (in the clustering order, which is the integer order)
                 * or of the last row of this partition if this is the first the first row. */
                .with_column("range_end", long_type, column_kind::clustering_key)
                /* The set of streams mapped to in this range.
                 * The number of streams mapped to a single range in a CDC generation is bounded from above by the number
                 * of shards on the owner of that range in the token ring.
                 * In other words, the number of elements of this set is bounded by the maximum of the number of shards
                 * over all nodes. The serialized size is obtained by counting about 20B for each stream.
                 * For example, if all nodes in the cluster have at most 128 shards,
                 * the serialized size of this set will be bounded by ~2.5 KB. */
                .with_column("streams", cdc_streams_set_type)
                /* The value of the `ignore_msb` sharding parameter of the node which was the owner of this token range
                 * when the generation was first created. Together with the set of streams above it fully describes
                 * the mapping for this particular range. */
                .with_column("ignore_msb", byte_type)
                /* Column used for sanity checking.
                 * For a given generation it's equal to the number of ranges in this generation;
                 * thus, after the generation is fully inserted, it must be equal to the number of rows in the partition. */
                .with_column("num_ranges", int32_type, column_kind::static_column)
                .with_version(system_keyspace::generate_schema_version(id))
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
                .with_version(system_keyspace::generate_schema_version(id))
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
                .with_version(system_keyspace::generate_schema_version(id))
                .build();
    }();
    return schema;
}

static const sstring CDC_TIMESTAMPS_KEY = "timestamps";

schema_ptr service_levels() {
    static thread_local auto schema = [] {
        auto id = generate_legacy_id(system_distributed_keyspace::NAME, system_distributed_keyspace::SERVICE_LEVELS);
        return schema_builder(system_distributed_keyspace::NAME, system_distributed_keyspace::SERVICE_LEVELS, std::make_optional(id))
                .with_column("service_level", utf8_type, column_kind::partition_key)
                .with_version(db::system_keyspace::generate_schema_version(id))
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
        cdc_generations_v2(),
        cdc_desc(),
        cdc_timestamps(),
        service_levels(),
    };
}

// Precondition: `ks_name` is either "system_distributed" or "system_distributed_everywhere".
static void check_exists(std::string_view ks_name, std::string_view cf_name, const replica::database& db) {
    if (!db.has_schema(ks_name, cf_name)) {
        // Throw `std::runtime_error` instead of calling `on_internal_error` due to some dtests which
        // 'upgrade' Scylla from Cassandra work directories (which is an unsupported upgrade path)
        // on which this check does not pass. We don't want the node to crash in these dtests,
        // but throw an error instead. In production clusters we don't crash on `on_internal_error` anyway.
        auto err = format("expected {}.{} to exist but it doesn't", ks_name, cf_name);
        dlogger.error("{}", err);
        throw std::runtime_error{std::move(err)};
    }
}

std::vector<schema_ptr> system_distributed_keyspace::all_distributed_tables() {
    return {view_build_status(), cdc_desc(), cdc_timestamps(), service_levels()};
}

std::vector<schema_ptr> system_distributed_keyspace::all_everywhere_tables() {
    return {cdc_generations_v2()};
}

system_distributed_keyspace::system_distributed_keyspace(cql3::query_processor& qp, service::migration_manager& mm, service::storage_proxy& sp)
        : _qp(qp)
        , _mm(mm)
        , _sp(sp) {
}

static thread_local std::pair<std::string_view, data_type> new_columns[] {
    {"timeout", duration_type},
    {"workload_type", utf8_type}
};

static schema_ptr get_current_service_levels(data_dictionary::database db) {
    return db.has_schema(system_distributed_keyspace::NAME, system_distributed_keyspace::SERVICE_LEVELS)
            ? db.find_schema(system_distributed_keyspace::NAME, system_distributed_keyspace::SERVICE_LEVELS)
            : service_levels();
}

static schema_ptr get_updated_service_levels(data_dictionary::database db) {
    SCYLLA_ASSERT(this_shard_id() == 0);
    auto schema = get_current_service_levels(db);
    schema_builder b(schema);
    for (const auto& col : new_columns) {
        auto& [col_name, col_type] = col;
        bytes options_name = to_bytes(col_name.data());
        if (schema->get_column_definition(options_name)) {
            continue;
        }
        b.with_column(options_name, col_type, column_kind::regular_column);
    }
    return b.build();
}

future<> system_distributed_keyspace::start() {
    if (this_shard_id() != 0) {
        _started = true;
        co_return;
    }

    auto db = _sp.data_dictionary();
    auto tables = ensured_tables();

    while (true) {
        // Check if there is any work to do before taking the group 0 guard.
        bool keyspaces_setup = db.has_keyspace(NAME) && db.has_keyspace(NAME_EVERYWHERE);
        bool tables_setup = std::all_of(tables.begin(), tables.end(), [db] (schema_ptr t) { return db.has_schema(t->ks_name(), t->cf_name()); } );
        bool service_levels_up_to_date = get_current_service_levels(db)->equal_columns(*get_updated_service_levels(db));
        if (keyspaces_setup && tables_setup && service_levels_up_to_date) {
            dlogger.info("system_distributed(_everywhere) keyspaces and tables are up-to-date. Not creating");
            _started = true;
            co_return;
        }

        auto group0_guard = co_await _mm.start_group0_operation();
        auto ts = group0_guard.write_timestamp();
        std::vector<mutation> mutations;
        sstring description;

        auto sd_ksm = keyspace_metadata::new_keyspace(
                NAME,
                "org.apache.cassandra.locator.SimpleStrategy",
                {{"replication_factor", "3"}},
                std::nullopt);
        if (!db.has_keyspace(NAME)) {
            mutations = service::prepare_new_keyspace_announcement(db.real_database(), sd_ksm, ts);
            description += format(" create {} keyspace;", NAME);
        } else {
            dlogger.info("{} keyspace is already present. Not creating", NAME);
        }

        auto sde_ksm = keyspace_metadata::new_keyspace(
                NAME_EVERYWHERE,
                "org.apache.cassandra.locator.EverywhereStrategy",
                {},
                std::nullopt);
        if (!db.has_keyspace(NAME_EVERYWHERE)) {
            auto sde_mutations = service::prepare_new_keyspace_announcement(db.real_database(), sde_ksm, ts);
            std::move(sde_mutations.begin(), sde_mutations.end(), std::back_inserter(mutations));
            description += format(" create {} keyspace;", NAME_EVERYWHERE);
        } else {
            dlogger.info("{} keyspace is already present. Not creating", NAME_EVERYWHERE);
        }

        // Get mutations for creating and updating tables.
        auto num_keyspace_mutations = mutations.size();
        co_await coroutine::parallel_for_each(ensured_tables(),
                [this, &mutations, db, ts, sd_ksm, sde_ksm] (auto&& table) -> future<> {
            auto ksm = table->ks_name() == NAME ? sd_ksm : sde_ksm;

            // Ensure that the service_levels table contains new columns.
            if (table->cf_name() == SERVICE_LEVELS) {
                table = get_updated_service_levels(db);
            }

            if (!db.has_schema(table->ks_name(), table->cf_name())) {
                co_return co_await service::prepare_new_column_family_announcement(mutations, _sp, *ksm, std::move(table), ts);
            }

            // The service_levels table exists. Update it if it lacks new columns.
            if (table->cf_name() == SERVICE_LEVELS && !get_current_service_levels(db)->equal_columns(*table)) {
                auto update_mutations = co_await service::prepare_column_family_update_announcement(_sp, table, std::vector<view_ptr>(), ts);
                std::move(update_mutations.begin(), update_mutations.end(), std::back_inserter(mutations));
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

future<std::unordered_map<locator::host_id, sstring>> system_distributed_keyspace::view_status(sstring ks_name, sstring view_name) const {
    return _qp.execute_internal(
            format("SELECT host_id, status FROM {}.{} WHERE keyspace_name = ? AND view_name = ?", NAME, VIEW_BUILD_STATUS),
            db::consistency_level::ONE,
            internal_distributed_query_state(),
            { std::move(ks_name), std::move(view_name) },
            cql3::query_processor::cache_internal::no).then([] (::shared_ptr<cql3::untyped_result_set> cql_result) {
        return boost::copy_range<std::unordered_map<locator::host_id, sstring>>(*cql_result
                | boost::adaptors::transformed([] (const cql3::untyped_result_set::row& row) {
                    auto host_id = locator::host_id(row.get_as<utils::UUID>("host_id"));
                    auto status = row.get_as<sstring>("status");
                    return std::pair(std::move(host_id), std::move(status));
                }));
    });
}

future<> system_distributed_keyspace::start_view_build(sstring ks_name, sstring view_name) const {
    auto host_id = _sp.local_db().get_token_metadata().get_my_id();
    return _qp.execute_internal(
            format("INSERT INTO {}.{} (keyspace_name, view_name, host_id, status) VALUES (?, ?, ?, ?)", NAME, VIEW_BUILD_STATUS),
            db::consistency_level::ONE,
            internal_distributed_query_state(),
            { std::move(ks_name), std::move(view_name), host_id.uuid(), "STARTED" },
            cql3::query_processor::cache_internal::no).discard_result();
}

future<> system_distributed_keyspace::finish_view_build(sstring ks_name, sstring view_name) const {
    auto host_id = _sp.local_db().get_token_metadata().get_my_id();
    return _qp.execute_internal(
            format("UPDATE {}.{} SET status = ? WHERE keyspace_name = ? AND view_name = ? AND host_id = ?", NAME, VIEW_BUILD_STATUS),
            db::consistency_level::ONE,
            internal_distributed_query_state(),
            { "SUCCESS", std::move(ks_name), std::move(view_name), host_id.uuid() },
            cql3::query_processor::cache_internal::no).discard_result();
}

future<> system_distributed_keyspace::remove_view(sstring ks_name, sstring view_name) const {
    return _qp.execute_internal(
            format("DELETE FROM {}.{} WHERE keyspace_name = ? AND view_name = ?", NAME, VIEW_BUILD_STATUS),
            db::consistency_level::ONE,
            internal_distributed_query_state(),
            { std::move(ks_name), std::move(view_name) },
            cql3::query_processor::cache_internal::no).discard_result();
}

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

static list_type_impl::native_type prepare_cdc_generation_description(const cdc::topology_description& description) {
    list_type_impl::native_type ret;
    for (auto& e: description.entries()) {
        list_type_impl::native_type streams;
        for (auto& s: e.streams) {
            streams.push_back(data_value(s.to_bytes()));
        }

        ret.push_back(make_tuple_value(cdc_token_range_description_type,
                { data_value(dht::token::to_int64(e.token_range_end))
                , make_list_value(cdc_streams_list_type, std::move(streams))
                , data_value(int8_t(e.sharding_ignore_msb))
                }));
    }
    return ret;
}

static std::vector<cdc::stream_id> get_streams_from_list_value(const data_value& v) {
    std::vector<cdc::stream_id> ret;
    auto& list_val = value_cast<list_type_impl::native_type>(v);
    for (auto& s_val: list_val) {
        ret.push_back(value_cast<bytes>(s_val));
    }
    return ret;
}

static cdc::token_range_description get_token_range_description_from_value(const data_value& v) {
    auto& tup = value_cast<tuple_type_impl::native_type>(v);
    if (tup.size() != 3) {
        on_internal_error(cdc_log, "get_token_range_description_from_value: stream tuple type size != 3");
    }

    auto token = dht::token::from_int64(value_cast<int64_t>(tup[0]));
    auto streams = get_streams_from_list_value(tup[1]);
    auto sharding_ignore_msb = uint8_t(value_cast<int8_t>(tup[2]));

    return {std::move(token), std::move(streams), sharding_ignore_msb};
}

future<>
system_distributed_keyspace::insert_cdc_topology_description(
        cdc::generation_id_v1 gen_id,
        const cdc::topology_description& description,
        context ctx) {
    check_exists(NAME, CDC_TOPOLOGY_DESCRIPTION, _qp.db().real_database());
    return _qp.execute_internal(
            format("INSERT INTO {}.{} (time, description) VALUES (?,?)", NAME, CDC_TOPOLOGY_DESCRIPTION),
            quorum_if_many(ctx.num_token_owners),
            internal_distributed_query_state(),
            { gen_id.ts, make_list_value(cdc_generation_description_type, prepare_cdc_generation_description(description)) },
            cql3::query_processor::cache_internal::no).discard_result();
}

future<std::optional<cdc::topology_description>>
system_distributed_keyspace::read_cdc_topology_description(
        cdc::generation_id_v1 gen_id,
        context ctx) {
    check_exists(NAME, CDC_TOPOLOGY_DESCRIPTION, _qp.db().real_database());
    return _qp.execute_internal(
            format("SELECT description FROM {}.{} WHERE time = ?", NAME, CDC_TOPOLOGY_DESCRIPTION),
            quorum_if_many(ctx.num_token_owners),
            internal_distributed_query_state(),
            { gen_id.ts },
            cql3::query_processor::cache_internal::no
    ).then([] (::shared_ptr<cql3::untyped_result_set> cql_result) -> std::optional<cdc::topology_description> {
        if (cql_result->empty() || !cql_result->one().has("description")) {
            return {};
        }

        utils::chunked_vector<cdc::token_range_description> entries;

        auto entries_val = value_cast<list_type_impl::native_type>(
                cdc_generation_description_type->deserialize(cql_result->one().get_view("description")));
        for (const auto& e_val: entries_val) {
            entries.push_back(get_token_range_description_from_value(e_val));
        }

        return { std::move(entries) };
    });
}

future<>
system_distributed_keyspace::insert_cdc_generation(
        utils::UUID id,
        const cdc::topology_description& desc,
        context ctx) {
    using namespace std::chrono_literals;

    const size_t concurrency = 10;
    const size_t num_replicas = ctx.num_token_owners;

    // To insert the data quickly and efficiently we send it in batches of multiple rows
    // (each batch represented by a single mutation). We also send multiple such batches concurrently.
    // However, we need to limit the memory consumption of the operation.
    // I assume that the memory consumption grows linearly with the number of replicas
    // (we send to all replicas ``at the same time''), with the batch size (the data must
    // be copied for each replica?) and with concurrency. These assumptions may be too conservative
    // but that won't hurt in a significant way (it may hurt the efficiency of the operation a little).
    // Thus, if we want to limit the memory consumption to L, it should be true that
    // mutation_size * num_replicas * concurrency <= L, hence
    // mutation_size <= L / (num_replicas * concurrency).
    // For example, say L = 10MB, concurrency = 10, num_replicas = 100; we get
    // mutation_size <= 10MB / 1000 = 10KB.
    // On the other hand we must have mutation_size >= size of a single row,
    // so we will use mutation_size <= max(size of single row, L/(num_replicas*concurrency)).

    // It has been tested that sending 1MB batches to 3 replicas with concurrency 20 works OK,
    // which would correspond to L ~= 60MB. Hence that's the limit we use here.
    const size_t L = 60'000'000;
    const auto mutation_size_threshold = std::max(size_t(1), L / (num_replicas * concurrency));

    auto s = _qp.db().real_database().find_schema(
        system_distributed_keyspace::NAME_EVERYWHERE, system_distributed_keyspace::CDC_GENERATIONS_V2);
    auto ms = co_await cdc::get_cdc_generation_mutations_v2(s, id, desc, mutation_size_threshold, api::new_timestamp());
    co_await max_concurrent_for_each(ms, concurrency, [&] (mutation& m) -> future<> {
        co_await _sp.mutate(
            { std::move(m) },
            db::consistency_level::ALL,
            db::timeout_clock::now() + 60s,
            nullptr, // trace_state
            empty_service_permit(),
            db::allow_per_partition_rate_limit::no,
            false // raw_counters
        );
    });
}

future<std::optional<cdc::topology_description>>
system_distributed_keyspace::read_cdc_generation(utils::UUID id) {
    utils::chunked_vector<cdc::token_range_description> entries;
    size_t num_ranges = 0;
    co_await _qp.query_internal(
            // This should be a local read so 20s should be more than enough
            format("SELECT range_end, streams, ignore_msb, num_ranges FROM {}.{} WHERE id = ? USING TIMEOUT 20s", NAME_EVERYWHERE, CDC_GENERATIONS_V2),
            db::consistency_level::ONE, // we wrote the generation with ALL so ONE must see it (or there's something really wrong)
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
        co_return std::nullopt;
    }

    // Paranoic sanity check. Partial reads should not happen since generations should be retrieved only after they
    // were written successfully with CL=ALL. But nobody uses EverywhereStrategy tables so they weren't ever properly
    // tested, so just in case...
    if (entries.size() != num_ranges) {
        throw std::runtime_error(format(
                "read_cdc_generation: wrong number of rows. The `num_ranges` column claimed {} rows,"
                " but reading the partition returned {}.", num_ranges, entries.size()));
    }

    co_return std::optional{cdc::topology_description(std::move(entries))};
}

static future<std::vector<mutation>> get_cdc_streams_descriptions_v2_mutation(
        const replica::database& db,
        db_clock::time_point time,
        const cdc::topology_description& desc) {
    auto s = db.find_schema(system_distributed_keyspace::NAME, system_distributed_keyspace::CDC_DESC_V2);

    auto ts = api::new_timestamp();
    std::vector<mutation> res;
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

future<std::vector<db_clock::time_point>>
system_distributed_keyspace::get_cdc_desc_v1_timestamps(context ctx) {
    std::vector<db_clock::time_point> res;
    co_await _qp.query_internal(
            // This is a long and expensive scan (mostly due to #8061).
            // Give it a bit more time than usual.
            format("SELECT time FROM {}.{} USING TIMEOUT 60s", NAME, CDC_DESC_V1),
            quorum_if_many(ctx.num_token_owners),
            {},
            1000,
            [&] (const cql3::untyped_result_set_row& r) {
        res.push_back(r.get_as<db_clock::time_point>("time"));
        return make_ready_future<stop_iteration>(stop_iteration::no);
    });
    co_return res;
}

future<qos::service_levels_info> system_distributed_keyspace::get_service_levels() const {
    return qos::get_service_levels(_qp, NAME, SERVICE_LEVELS, db::consistency_level::ONE);
}

future<qos::service_levels_info> system_distributed_keyspace::get_service_level(sstring service_level_name) const {
    return qos::get_service_level(_qp, NAME, SERVICE_LEVELS, service_level_name, db::consistency_level::ONE);
}

future<> system_distributed_keyspace::set_service_level(sstring service_level_name, qos::service_level_options slo) const {
    static sstring prepared_query = format("INSERT INTO {}.{} (service_level) VALUES (?);", NAME, SERVICE_LEVELS);
    co_await _qp.execute_internal(prepared_query, db::consistency_level::ONE, internal_distributed_query_state(), {service_level_name}, cql3::query_processor::cache_internal::no);
    auto to_data_value = [&] (const qos::service_level_options::timeout_type& tv) {
        return std::visit(overloaded_functor {
            [&] (const qos::service_level_options::unset_marker&) {
                return data_value::make_null(duration_type);
            },
            [&] (const qos::service_level_options::delete_marker&) {
                return data_value::make_null(duration_type);
            },
            [&] (const lowres_clock::duration& d) {
                return data_value(cql_duration(months_counter{0},
                        days_counter{0},
                        nanoseconds_counter{std::chrono::duration_cast<std::chrono::nanoseconds>(d).count()}));
            },
        }, tv);
    };
    data_value workload = slo.workload == qos::service_level_options::workload_type::unspecified
            ? data_value::make_null(utf8_type)
            : data_value(qos::service_level_options::to_string(slo.workload));
    co_await _qp.execute_internal(format("UPDATE {}.{} SET timeout = ?, workload_type = ? WHERE service_level = ?;", NAME, SERVICE_LEVELS),
                db::consistency_level::ONE,
                internal_distributed_query_state(),
                {to_data_value(slo.timeout),
                    workload,
                    service_level_name},
                cql3::query_processor::cache_internal::no);
}

future<> system_distributed_keyspace::drop_service_level(sstring service_level_name) const {
    static sstring prepared_query = format("DELETE FROM {}.{} WHERE service_level= ?;", NAME, SERVICE_LEVELS);
    return _qp.execute_internal(prepared_query, db::consistency_level::ONE, internal_distributed_query_state(), {service_level_name}, cql3::query_processor::cache_internal::no).discard_result();
}

}
