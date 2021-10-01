/*
 * Copyright (C) 2018-present ScyllaDB
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

#include "db/system_distributed_keyspace.hh"

#include "cql3/untyped_result_set.hh"
#include "database.hh"
#include "db/consistency_level_type.hh"
#include "db/system_keyspace.hh"
#include "schema_builder.hh"
#include "schema_registry.hh"
#include "timeout_config.hh"
#include "types.hh"
#include "types/tuple.hh"
#include "types/set.hh"
#include "cdc/generation.hh"
#include "cql3/query_processor.hh"
#include "service/storage_proxy.hh"
#include "service/migration_manager.hh"

#include <seastar/core/seastar.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future-util.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include <boost/range/adaptor/transformed.hpp>

#include <optional>
#include <vector>
#include <set>

static logging::logger dlogger("system_distributed_keyspace");
extern logging::logger cdc_log;

namespace db {

thread_local data_type cdc_streams_set_type = set_type_impl::get_instance(bytes_type, false);

/* See `token_range_description` struct */
thread_local data_type cdc_streams_list_type = list_type_impl::get_instance(bytes_type, false);
thread_local data_type cdc_token_range_description_type = tuple_type_impl::get_instance(
        { long_type             // dht::token token_range_end;
        , cdc_streams_list_type // std::vector<stream_id> streams;
        , byte_type             // uint8_t sharding_ignore_msb;
        });
thread_local data_type cdc_generation_description_type = list_type_impl::get_instance(cdc_token_range_description_type, false);

schema_ptr view_build_status(schema_registry& registry) {
    auto id = generate_legacy_id(system_distributed_keyspace::NAME, system_distributed_keyspace::VIEW_BUILD_STATUS);
    return registry.get_or_load(system_keyspace::generate_schema_version(id), [&, id] (table_schema_version) {
        return schema_builder(registry, system_distributed_keyspace::NAME, system_distributed_keyspace::VIEW_BUILD_STATUS, std::make_optional(id))
                .with_column("keyspace_name", utf8_type, column_kind::partition_key)
                .with_column("view_name", utf8_type, column_kind::partition_key)
                .with_column("host_id", uuid_type, column_kind::clustering_key)
                .with_column("status", utf8_type)
                .with_version(system_keyspace::generate_schema_version(id))
                .build();
    });
}

/* An internal table used by nodes to exchange CDC generation data. */
schema_ptr cdc_generations_v2(schema_registry& registry) {
    auto id = generate_legacy_id(system_distributed_keyspace::NAME_EVERYWHERE, system_distributed_keyspace::CDC_GENERATIONS_V2);
    return registry.get_or_load(system_keyspace::generate_schema_version(id), [&, id] (table_schema_version) {
        return schema_builder(registry, system_distributed_keyspace::NAME_EVERYWHERE, system_distributed_keyspace::CDC_GENERATIONS_V2, {id})
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
    });
}

/* A user-facing table providing identifiers of the streams used in CDC generations. */
schema_ptr cdc_desc(schema_registry& registry) {
    auto id = generate_legacy_id(system_distributed_keyspace::NAME, system_distributed_keyspace::CDC_DESC_V2);
    return registry.get_or_load(system_keyspace::generate_schema_version(id), [&, id] (table_schema_version) {
        return schema_builder(registry, system_distributed_keyspace::NAME, system_distributed_keyspace::CDC_DESC_V2, {id})
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
    });
}

/* A user-facing table providing CDC generation timestamps. */
schema_ptr cdc_timestamps(schema_registry& registry) {
    auto id = generate_legacy_id(system_distributed_keyspace::NAME, system_distributed_keyspace::CDC_TIMESTAMPS);
    return registry.get_or_load(system_keyspace::generate_schema_version(id), [&, id] (table_schema_version) {
        return schema_builder(registry, system_distributed_keyspace::NAME, system_distributed_keyspace::CDC_TIMESTAMPS, {id})
                /* This is a single-partition table. The partition key is always "timestamps". */
                .with_column("key", utf8_type, column_kind::partition_key)
                /* The timestamp of this CDC generation. */
                .with_column("time", reversed_type_impl::get_instance(timestamp_type), column_kind::clustering_key)
                /* Expiration time of this CDC generation (or null if not expired). */
                .with_column("expired", timestamp_type)
                .with_version(system_keyspace::generate_schema_version(id))
                .build();
    });
}

static const sstring CDC_TIMESTAMPS_KEY = "timestamps";

schema_ptr service_levels(schema_registry& registry) {
    auto id = generate_legacy_id(system_distributed_keyspace::NAME, system_distributed_keyspace::SERVICE_LEVELS);
    return registry.get_or_load(system_keyspace::generate_schema_version(id), [&, id] (table_schema_version) {
        return schema_builder(registry, system_distributed_keyspace::NAME, system_distributed_keyspace::SERVICE_LEVELS, std::make_optional(id))
                .with_column("service_level", utf8_type, column_kind::partition_key)
                .with_version(db::system_keyspace::generate_schema_version(id))
                .build();
    });
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
static std::vector<schema_ptr> ensured_tables(schema_registry& registry) {
    return {
        view_build_status(registry),
        cdc_generations_v2(registry),
        cdc_desc(registry),
        cdc_timestamps(registry),
        service_levels(registry),
    };
}

// Precondition: `ks_name` is either "system_distributed" or "system_distributed_everywhere".
static void check_exists(std::string_view ks_name, std::string_view cf_name, const database& db) {
    if (!db.has_schema(ks_name, cf_name)) {
        on_internal_error(dlogger, format("expected {}.{} to exist but it doesn't", ks_name, cf_name));
    }
}

bool system_distributed_keyspace::is_extra_durable(const sstring& cf_name) {
    return cf_name == CDC_TOPOLOGY_DESCRIPTION || cf_name == CDC_GENERATIONS_V2;
}

system_distributed_keyspace::system_distributed_keyspace(cql3::query_processor& qp, service::migration_manager& mm, service::storage_proxy& sp)
        : _qp(qp)
        , _mm(mm)
        , _sp(sp) {
}

static future<> add_new_columns_if_missing(database& db, ::service::migration_manager& mm) noexcept {
    static thread_local std::pair<std::string_view, data_type> new_columns[] {
        {"timeout", duration_type},
        {"workload_type", utf8_type}
    };
    try {
        auto schema = db.find_schema(system_distributed_keyspace::NAME, system_distributed_keyspace::SERVICE_LEVELS);
        schema_builder b(schema);
        bool updated = false;
        for (const auto& col : new_columns) {
            auto& [col_name, col_type] = col;
            bytes options_name = to_bytes(col_name.data());
            if (schema->get_column_definition(options_name)) {
                continue;
            }
            updated = true;
            b.with_column(options_name, col_type, column_kind::regular_column);
        }
        if (!updated) {
            return make_ready_future<>();
        }
        schema_ptr table = b.build();
        return mm.announce_column_family_update(table, false, {}, api::timestamp_type(1)).handle_exception([] (const std::exception_ptr&) {});
    } catch (...) {
        dlogger.warn("Failed to update options column in the role attributes table: {}", std::current_exception());
        return make_ready_future<>();
    }
}

future<> system_distributed_keyspace::start() {
    if (this_shard_id() != 0) {
        _started = true;
        co_return;
    }

    static auto ignore_existing = [] (seastar::noncopyable_function<future<>()> func) {
        return futurize_invoke(std::move(func)).handle_exception_type([] (exceptions::already_exists_exception& ignored) { });
    };

    // We use min_timestamp so that the default keyspace metadata will lose with any manual adjustments.
    // See issue #2129.
    co_await ignore_existing([this] {
        auto ksm = keyspace_metadata::new_keyspace(
                NAME,
                "org.apache.cassandra.locator.SimpleStrategy",
                {{"replication_factor", "3"}},
                true /* durable_writes */);
        return _mm.announce_new_keyspace(ksm, api::min_timestamp);
    });

    co_await ignore_existing([this] {
        auto ksm = keyspace_metadata::new_keyspace(
                NAME_EVERYWHERE,
                "org.apache.cassandra.locator.EverywhereStrategy",
                {},
                true /* durable_writes */);
        return _mm.announce_new_keyspace(ksm, api::min_timestamp);
    });

    auto& registry = _qp.db().get_schema_registry();

    for (auto&& table : ensured_tables(registry)) {
        co_await ignore_existing([this, table = std::move(table)] {
            return _mm.announce_new_column_family(std::move(table), api::min_timestamp);
        });
    }

    _started = true;
    co_await add_new_columns_if_missing(_qp.db(), _mm);
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

future<std::unordered_map<utils::UUID, sstring>> system_distributed_keyspace::view_status(sstring ks_name, sstring view_name) const {
    return _qp.execute_internal(
            format("SELECT host_id, status FROM {}.{} WHERE keyspace_name = ? AND view_name = ?", NAME, VIEW_BUILD_STATUS),
            db::consistency_level::ONE,
            internal_distributed_query_state(),
            { std::move(ks_name), std::move(view_name) },
            false).then([this] (::shared_ptr<cql3::untyped_result_set> cql_result) {
        return boost::copy_range<std::unordered_map<utils::UUID, sstring>>(*cql_result
                | boost::adaptors::transformed([] (const cql3::untyped_result_set::row& row) {
                    auto host_id = row.get_as<utils::UUID>("host_id");
                    auto status = row.get_as<sstring>("status");
                    return std::pair(std::move(host_id), std::move(status));
                }));
    });
}

future<> system_distributed_keyspace::start_view_build(sstring ks_name, sstring view_name) const {
    return db::system_keyspace::load_local_host_id().then([this, ks_name = std::move(ks_name), view_name = std::move(view_name)] (utils::UUID host_id) {
        return _qp.execute_internal(
                format("INSERT INTO {}.{} (keyspace_name, view_name, host_id, status) VALUES (?, ?, ?, ?)", NAME, VIEW_BUILD_STATUS),
                db::consistency_level::ONE,
                internal_distributed_query_state(),
                { std::move(ks_name), std::move(view_name), std::move(host_id), "STARTED" },
                false).discard_result();
    });
}

future<> system_distributed_keyspace::finish_view_build(sstring ks_name, sstring view_name) const {
    return db::system_keyspace::load_local_host_id().then([this, ks_name = std::move(ks_name), view_name = std::move(view_name)] (utils::UUID host_id) {
        return _qp.execute_internal(
                format("UPDATE {}.{} SET status = ? WHERE keyspace_name = ? AND view_name = ? AND host_id = ?", NAME, VIEW_BUILD_STATUS),
                db::consistency_level::ONE,
                internal_distributed_query_state(),
                { "SUCCESS", std::move(ks_name), std::move(view_name), std::move(host_id) },
                false).discard_result();
    });
}

future<> system_distributed_keyspace::remove_view(sstring ks_name, sstring view_name) const {
    return _qp.execute_internal(
            format("DELETE FROM {}.{} WHERE keyspace_name = ? AND view_name = ?", NAME, VIEW_BUILD_STATUS),
            db::consistency_level::ONE,
            internal_distributed_query_state(),
            { std::move(ks_name), std::move(view_name) },
            false).discard_result();
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
    check_exists(NAME, CDC_TOPOLOGY_DESCRIPTION, _qp.db());
    return _qp.execute_internal(
            format("INSERT INTO {}.{} (time, description) VALUES (?,?)", NAME, CDC_TOPOLOGY_DESCRIPTION),
            quorum_if_many(ctx.num_token_owners),
            internal_distributed_query_state(),
            { gen_id.ts, make_list_value(cdc_generation_description_type, prepare_cdc_generation_description(description)) },
            false).discard_result();
}

future<std::optional<cdc::topology_description>>
system_distributed_keyspace::read_cdc_topology_description(
        cdc::generation_id_v1 gen_id,
        context ctx) {
    check_exists(NAME, CDC_TOPOLOGY_DESCRIPTION, _qp.db());
    return _qp.execute_internal(
            format("SELECT description FROM {}.{} WHERE time = ?", NAME, CDC_TOPOLOGY_DESCRIPTION),
            quorum_if_many(ctx.num_token_owners),
            internal_distributed_query_state(),
            { gen_id.ts },
            false
    ).then([] (::shared_ptr<cql3::untyped_result_set> cql_result) -> std::optional<cdc::topology_description> {
        if (cql_result->empty() || !cql_result->one().has("description")) {
            return {};
        }

        std::vector<cdc::token_range_description> entries;

        auto entries_val = value_cast<list_type_impl::native_type>(
                cdc_generation_description_type->deserialize(cql_result->one().get_view("description")));
        for (const auto& e_val: entries_val) {
            entries.push_back(get_token_range_description_from_value(e_val));
        }

        return { std::move(entries) };
    });
}

static future<utils::chunked_vector<mutation>> get_cdc_generation_mutations(
        const database& db,
        utils::UUID id,
        size_t num_replicas,
        size_t concurrency,
        const cdc::topology_description& desc) {
    assert(num_replicas);
    auto s = db.find_schema(system_distributed_keyspace::NAME_EVERYWHERE, system_distributed_keyspace::CDC_GENERATIONS_V2);

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
    const auto new_mutation_threshold = std::max(size_t(1), L / (num_replicas * concurrency));

    auto ts = api::new_timestamp();
    utils::chunked_vector<mutation> res;
    res.emplace_back(s, partition_key::from_singular(*s, id));
    res.back().set_static_cell(to_bytes("num_ranges"), int32_t(desc.entries().size()), ts);
    size_t size_estimate = 0;
    for (auto& e : desc.entries()) {
        if (size_estimate >= new_mutation_threshold) {
            res.emplace_back(s, partition_key::from_singular(*s, id));
            size_estimate = 0;
        }

        set_type_impl::native_type streams;
        streams.reserve(e.streams.size());
        for (auto& stream: e.streams) {
            streams.push_back(data_value(stream.to_bytes()));
        }

        size_estimate += e.streams.size() * 20;
        auto ckey = clustering_key::from_singular(*s, dht::token::to_int64(e.token_range_end));
        res.back().set_cell(ckey, to_bytes("streams"), make_set_value(cdc_streams_set_type, std::move(streams)), ts);
        res.back().set_cell(ckey, to_bytes("ignore_msb"), int8_t(e.sharding_ignore_msb), ts);

        co_await coroutine::maybe_yield();
    }

    co_return res;
}

future<>
system_distributed_keyspace::insert_cdc_generation(
        utils::UUID id,
        const cdc::topology_description& desc,
        context ctx) {
    using namespace std::chrono_literals;

    const size_t concurrency = 10;

    auto ms = co_await get_cdc_generation_mutations(_qp.db(), id, ctx.num_token_owners, concurrency, desc);
    co_await max_concurrent_for_each(ms, concurrency, [&] (mutation& m) -> future<> {
        co_await _sp.mutate(
            { std::move(m) },
            db::consistency_level::ALL,
            db::timeout_clock::now() + 60s,
            nullptr, // trace_state
            empty_service_permit(),
            false // raw_counters
        );
    });
}

future<std::optional<cdc::topology_description>>
system_distributed_keyspace::read_cdc_generation(utils::UUID id) {
    std::vector<cdc::token_range_description> entries;
    auto num_ranges = 0;
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
        const database& db,
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

    auto ms = co_await get_cdc_streams_descriptions_v2_mutation(_qp.db(), time, desc);
    co_await max_concurrent_for_each(ms, 20, [&] (mutation& m) -> future<> {
        // We use the storage_proxy::mutate API since CQL is not the best for handling large batches.
        co_await _sp.mutate(
            { std::move(m) },
            quorum_if_many(ctx.num_token_owners),
            db::timeout_clock::now() + 30s,
            nullptr, // trace_state
            empty_service_permit(),
            false // raw_counters
        );
    });

    // Commit the description.
    co_await _qp.execute_internal(
            format("INSERT INTO {}.{} (key, time) VALUES (?, ?)", NAME, CDC_TIMESTAMPS),
            quorum_if_many(ctx.num_token_owners),
            internal_distributed_query_state(),
            { CDC_TIMESTAMPS_KEY, time },
            false).discard_result();
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
            false
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
            false);

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
                false);

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
            false);

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

static qos::service_level_options::timeout_type get_duration(const cql3::untyped_result_set_row&row, std::string_view col_name) {
    auto dur_opt = row.get_opt<cql_duration>(col_name);
    if (!dur_opt) {
        return qos::service_level_options::unset_marker{};
    }
    return std::chrono::duration_cast<lowres_clock::duration>(std::chrono::nanoseconds(dur_opt->nanoseconds));
};

future<qos::service_levels_info> system_distributed_keyspace::get_service_levels() const {
    static sstring prepared_query = format("SELECT * FROM {}.{};", NAME, SERVICE_LEVELS);

    return _qp.execute_internal(prepared_query, db::consistency_level::ONE, internal_distributed_query_state(), {}).then([] (shared_ptr<cql3::untyped_result_set> result_set) {
        qos::service_levels_info service_levels;
        for (auto &&row : *result_set) {
            try {
                auto service_level_name = row.get_as<sstring>("service_level");
                auto workload = qos::service_level_options::parse_workload_type(row.get_opt<sstring>("workload_type").value_or(""));
                qos::service_level_options slo{
                    .timeout = get_duration(row, "timeout"),
                    .workload = workload.value_or(qos::service_level_options::workload_type::unspecified),
                };
                service_levels.emplace(service_level_name, slo);
            } catch (...) {
                dlogger.warn("Failed to fetch data for service levels: {}", std::current_exception());
            }
        }
        return service_levels;
    });
}

future<qos::service_levels_info> system_distributed_keyspace::get_service_level(sstring service_level_name) const {
    static sstring prepared_query = format("SELECT * FROM {}.{} WHERE service_level = ?;", NAME, SERVICE_LEVELS);
    return _qp.execute_internal(prepared_query, db::consistency_level::ONE, internal_distributed_query_state(), {service_level_name}).then(
                [service_level_name = std::move(service_level_name)] (shared_ptr<cql3::untyped_result_set> result_set) {
        qos::service_levels_info service_levels;
        if (!result_set->empty()) {
            try {
                auto &&row = result_set->one();
                auto workload = qos::service_level_options::parse_workload_type(row.get_opt<sstring>("workload_type").value_or(""));
                qos::service_level_options slo{
                    .timeout = get_duration(row, "timeout"),
                    .workload = workload.value_or(qos::service_level_options::workload_type::unspecified),
                };
                service_levels.emplace(service_level_name, slo);
            } catch (...) {
                dlogger.warn("Failed to fetch data for service level {}: {}", service_level_name, std::current_exception());
            }
        }
        return service_levels;
    });
}

future<> system_distributed_keyspace::set_service_level(sstring service_level_name, qos::service_level_options slo) const {
    static sstring prepared_query = format("INSERT INTO {}.{} (service_level) VALUES (?);", NAME, SERVICE_LEVELS);
    co_await _qp.execute_internal(prepared_query, db::consistency_level::ONE, internal_distributed_query_state(), {service_level_name});
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
                    service_level_name});
}

future<> system_distributed_keyspace::drop_service_level(sstring service_level_name) const {
    static sstring prepared_query = format("DELETE FROM {}.{} WHERE service_level= ?;", NAME, SERVICE_LEVELS);
    return _qp.execute_internal(prepared_query, db::consistency_level::ONE, internal_distributed_query_state(), {service_level_name}).discard_result();
}

}
