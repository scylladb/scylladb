/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <boost/algorithm/cxx11/any_of.hpp>
#include <boost/range/algorithm.hpp>
#include <boost/algorithm/string.hpp>
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/json/json_elements.hh>
#include <seastar/core/reactor.hh>

#include "db/config.hh"
#include "db/system_keyspace.hh"
#include "db/virtual_table.hh"
#include "db/virtual_tables.hh"
#include "db/size_estimates_virtual_reader.hh"
#include "db/view/build_progress_virtual_reader.hh"
#include "index/built_indexes_virtual_reader.hh"
#include "gms/gossiper.hh"
#include "mutation/frozen_mutation.hh"
#include "protocol_server.hh"
#include "release.hh"
#include "replica/database.hh"
#include "schema/schema_builder.hh"
#include "service/raft/raft_group_registry.hh"
#include "service/storage_service.hh"
#include "types/list.hh"
#include "types/types.hh"
#include "utils/build_id.hh"
#include "log.hh"

namespace db {

namespace {

logging::logger vtlog("virtual_tables");

class cluster_status_table : public memtable_filling_virtual_table {
private:
    distributed<service::storage_service>& _dist_ss;
    distributed<gms::gossiper>& _dist_gossiper;

public:
    cluster_status_table(distributed<service::storage_service>& ss, distributed<gms::gossiper>& g)
            : memtable_filling_virtual_table(build_schema())
            , _dist_ss(ss), _dist_gossiper(g) {}

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
        auto muts = co_await _dist_ss.invoke_on(0, [this] (service::storage_service& ss) -> future<std::vector<frozen_mutation>> {
            auto& gossiper = _dist_gossiper.local();
            auto ownership = co_await ss.get_ownership();
            const locator::token_metadata& tm = ss.get_token_metadata();

            std::vector<frozen_mutation> muts;
            muts.reserve(gossiper.num_endpoints());

            gossiper.for_each_endpoint_state([&] (const gms::inet_address& endpoint, const gms::endpoint_state&) {
                static thread_local auto s = build_schema();
                mutation m(s, partition_key::from_single_value(*s, data_value(endpoint).serialize_nonnull()));
                row& cr = m.partition().clustered_row(*schema(), clustering_key::make_empty()).cells();

                set_cell(cr, "up", gossiper.is_alive(endpoint));
                if (!ss.raft_topology_change_enabled() || gossiper.is_shutdown(endpoint)) {
                    set_cell(cr, "status", gossiper.get_gossip_status(endpoint));
                }
                set_cell(cr, "load", gossiper.get_application_state_value(endpoint, gms::application_state::LOAD));

                auto hostid = tm.get_host_id_if_known(endpoint);
                if (hostid) {
                    if (ss.raft_topology_change_enabled() && !gossiper.is_shutdown(endpoint)) {
                        set_cell(cr, "status", boost::to_upper_copy<std::string>(fmt::format("{}", ss.get_node_state(*hostid))));
                    }
                    set_cell(cr, "host_id", hostid->uuid());
                }

                if (hostid) {
                    sstring dc = tm.get_topology().get_location(endpoint).dc;
                    set_cell(cr, "dc", dc);
                }

                if (ownership.contains(endpoint)) {
                    set_cell(cr, "owns", ownership[endpoint]);
                }

                set_cell(cr, "tokens", int32_t(hostid ? tm.get_tokens(*hostid).size() : 0));

                muts.push_back(freeze(std::move(m)));
            });

            co_return muts;
        });

        for (auto& m : muts) {
            mutation_sink(m.unfreeze(schema()));
        }
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
            .with_column("table_name", utf8_type, column_kind::clustering_key)
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

    clustering_key make_clustering_key(const sstring& table_name, sstring start_token, gms::inet_address host) {
        return clustering_key::from_exploded(*_s, {
            data_value(table_name).serialize_nonnull(),
            data_value(start_token).serialize_nonnull(),
            data_value(host).serialize_nonnull()
        });
    }

    future<> emit_ring(result_collector& result, const dht::decorated_key& dk, const sstring& table_name, std::vector<dht::token_range_endpoints> ranges) {

        co_await result.emit_partition_start(dk);
        boost::sort(ranges, [] (const dht::token_range_endpoints& l, const dht::token_range_endpoints& r) {
            return l._start_token < r._start_token;
        });

        for (dht::token_range_endpoints& range : ranges) {
            boost::sort(range._endpoint_details, endpoint_details_cmp());

            for (const dht::endpoint_details& detail : range._endpoint_details) {
                clustering_row cr(make_clustering_key(table_name, range._start_token, detail._host));
                set_cell(cr.cells(), "end_token", sstring(range._end_token));
                set_cell(cr.cells(), "dc", sstring(detail._datacenter));
                set_cell(cr.cells(), "rack", sstring(detail._rack));
                co_await result.emit_row(std::move(cr));
            }
        }

        co_await result.emit_partition_end();
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
            _db.get_non_local_strategy_keyspaces()
                | boost::adaptors::transformed([this] (auto&& ks) {
                    return decorated_keyspace_name{ks, make_partition_key(ks)};
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

            if (_db.find_keyspace(e.name).get_replication_strategy().uses_tablets()) {
                co_await _db.get_tables_metadata().for_each_table_gently([&, this] (table_id, lw_shared_ptr<replica::table> table) -> future<> {
                    if (table->schema()->ks_name() != e.name) {
                        co_return;
                    }
                    const auto& table_name = table->schema()->cf_name();
                    std::vector<dht::token_range_endpoints> ranges = co_await _ss.describe_ring_for_table(e.name, table_name);
                    co_await emit_ring(result, e.key, table_name, std::move(ranges));
                });
            } else {
                std::vector<dht::token_range_endpoints> ranges = co_await _ss.describe_ring(e.name);
                co_await emit_ring(result, e.key, "<ALL>", std::move(ranges));
            }
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
                co_await db.get_tables_metadata().for_each_table_gently(coroutine::lambda([&] (table_id, lw_shared_ptr<replica::table> table) -> future<> {
                    if (table->schema()->ks_name() != ks_name) {
                        co_return;
                    }
                    const auto unordered_snapshots = co_await table->get_snapshot_details();
                    snapshots_by_tables.emplace(table->schema()->cf_name(), std::map<sstring, replica::table::snapshot_details>(unordered_snapshots.begin(), unordered_snapshots.end()));
                }));
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
            db.get_tables_metadata().for_each_table([&] (table_id, lw_shared_ptr<replica::table> table) {
               val = reduce(val, map(*table));
            });
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
                t.for_each_active_memtable([&] (replica::memtable& active_memtable) {
                    s += active_memtable.region().occupancy();
                    partition_count += active_memtable.partition_count();
                });
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
                db.get_tables_metadata().for_each_table([&] (table_id id, lw_shared_ptr<replica::table> t) {
                    auto& cache_stats = t->get_row_cache().stats();
                    res.hits += cache_stats.hits.count();
                    res.misses += cache_stats.misses.count();
                    res.hits_moving_average += cache_stats.hits.rate();
                    res.requests_moving_average += (cache_stats.hits.rate() + cache_stats.misses.rate());
                });
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
        for (unsigned i = 0; i < smp::count; i++) {
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

}

future<> initialize_virtual_tables(
        distributed<replica::database>& dist_db, distributed<service::storage_service>& dist_ss,
        sharded<gms::gossiper>& dist_gossiper, distributed<service::raft_group_registry>& dist_raft_gr,
        sharded<db::system_keyspace>& sys_ks,
        db::config& cfg) {
    auto& virtual_tables_registry = sys_ks.local().get_virtual_tables_registry();
    auto& virtual_tables = *virtual_tables_registry;
    auto& db = dist_db.local();
    auto& ss = dist_ss.local();

    auto add_table = [&] (std::unique_ptr<virtual_table>&& tbl) -> future<> {
        auto schema = tbl->schema();
        virtual_tables[schema->id()] = std::move(tbl);
        co_await db.create_local_system_table(schema, false, ss.get_erm_factory());
        auto& cf = db.find_column_family(schema);
        cf.mark_ready_for_writes(nullptr);
        auto& vt = virtual_tables[schema->id()];
        cf.set_virtual_reader(vt->as_mutation_source());
        cf.set_virtual_writer([&vt = *vt] (const frozen_mutation& m) { return vt.apply(m); });
    };

    // Add built-in virtual tables here.
    co_await add_table(std::make_unique<cluster_status_table>(dist_ss, dist_gossiper));
    co_await add_table(std::make_unique<token_ring_table>(db, ss));
    co_await add_table(std::make_unique<snapshots_table>(dist_db));
    co_await add_table(std::make_unique<protocol_servers_table>(ss));
    co_await add_table(std::make_unique<runtime_info_table>(dist_db, ss));
    co_await add_table(std::make_unique<versions_table>());
    co_await add_table(std::make_unique<db_config_table>(cfg));
    co_await add_table(std::make_unique<clients_table>(ss));
    co_await add_table(std::make_unique<raft_state_table>(dist_raft_gr));

    db.find_column_family(system_keyspace::size_estimates()).set_virtual_reader(mutation_source(db::size_estimates::virtual_reader(db, sys_ks.local())));
    db.find_column_family(system_keyspace::v3::views_builds_in_progress()).set_virtual_reader(mutation_source(db::view::build_progress_virtual_reader(db)));
    db.find_column_family(system_keyspace::built_indexes()).set_virtual_reader(mutation_source(db::index::built_indexes_virtual_reader(db)));
}

virtual_tables_registry::virtual_tables_registry() : unique_ptr(std::make_unique<virtual_tables_registry_impl>()) {
}

virtual_tables_registry::~virtual_tables_registry() = default;

} // namespace db
