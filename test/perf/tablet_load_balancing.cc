/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <fmt/ranges.h>
#include <bit>

#include <seastar/core/distributed.hh>
#include <seastar/core/app-template.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/reactor.hh>

#include "locator/tablets.hh"
#include "service/tablet_allocator.hh"
#include "locator/tablet_replication_strategy.hh"
#include "locator/network_topology_strategy.hh"
#include "locator/load_sketch.hh"
#include "replica/tablets.hh"
#include "locator/tablet_replication_strategy.hh"
#include "db/config.hh"
#include "schema/schema_builder.hh"
#include "service/storage_proxy.hh"
#include "db/system_keyspace.hh"

#include "test/perf/perf.hh"
#include "test/lib/log.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/random_utils.hh"

using namespace locator;
using namespace replica;
using namespace service;

static seastar::abort_source aborted;

static const sstring dc = "dc1";

static
cql_test_config tablet_cql_test_config() {
    cql_test_config c;
    return c;
}

static
future<table_id> add_table(cql_test_env& e) {
    auto id = table_id(utils::UUID_gen::get_time_UUID());
    co_await e.create_table([id] (std::string_view ks_name) {
        return *schema_builder(ks_name, id.to_sstring(), id)
                .with_column("p1", utf8_type, column_kind::partition_key)
                .with_column("r1", int32_type)
                .build();
    });
    co_return id;
}

static
size_t get_tablet_count(const tablet_metadata& tm) {
    size_t count = 0;
    for (auto& [table, tmap] : tm.all_tables()) {
        count += std::accumulate(tmap->tablets().begin(), tmap->tablets().end(), size_t(0),
                                 [] (size_t accumulator, const locator::tablet_info& info) {
                                     return accumulator + info.replicas.size();
                                 });
    }
    return count;
}

static
void apply_resize_plan(token_metadata& tm, const migration_plan& plan) {
    for (auto [table_id, resize_decision] : plan.resize_plan().resize) {
        tm.tablets().mutate_tablet_map(table_id, [&resize_decision] (tablet_map& tmap) {
            resize_decision.sequence_number = tmap.resize_decision().sequence_number + 1;
            tmap.set_resize_decision(resize_decision);
        });
    }
    for (auto table_id : plan.resize_plan().finalize_resize) {
        auto& old_tmap = tm.tablets().get_tablet_map(table_id);
        testlog.info("Setting new tablet map of size {}", old_tmap.tablet_count() * 2);
        tablet_map tmap(old_tmap.tablet_count() * 2);
        tm.tablets().set_tablet_map(table_id, std::move(tmap));
    }
}

// Reflects the plan in a given token metadata as if the migrations were fully executed.
static
void apply_plan(token_metadata& tm, const migration_plan& plan) {
    for (auto&& mig : plan.migrations()) {
        tm.tablets().mutate_tablet_map(mig.tablet.table, [&mig] (tablet_map& tmap) {
            auto tinfo = tmap.get_tablet_info(mig.tablet.tablet);
            tinfo.replicas = replace_replica(tinfo.replicas, mig.src, mig.dst);
            tmap.set_tablet(mig.tablet.tablet, tinfo);
        });
    }
    apply_resize_plan(tm, plan);
}

using seconds_double = std::chrono::duration<double>;

struct rebalance_stats {
    seconds_double elapsed_time = seconds_double(0);
    seconds_double max_rebalance_time = seconds_double(0);
    uint64_t rebalance_count = 0;

    rebalance_stats& operator+=(const rebalance_stats& other) {
        elapsed_time += other.elapsed_time;
        max_rebalance_time = std::max(max_rebalance_time, other.max_rebalance_time);
        rebalance_count += other.rebalance_count;
        return *this;
    }
};

static
rebalance_stats rebalance_tablets(tablet_allocator& talloc, shared_token_metadata& stm, locator::load_stats_ptr load_stats = {}, std::unordered_set<host_id> skiplist = {}) {
    rebalance_stats stats;

    // Sanity limit to avoid infinite loops.
    // The x10 factor is arbitrary, it's there to account for more complex schedules than direct migration.
    auto max_iterations = 1 + get_tablet_count(stm.get()->tablets()) * 10;

    for (size_t i = 0; i < max_iterations; ++i) {
        auto prev_lb_stats = talloc.stats().for_dc(dc);
        auto start_time = std::chrono::steady_clock::now();
        auto plan = talloc.balance_tablets(stm.get(), load_stats, skiplist).get();
        auto end_time = std::chrono::steady_clock::now();
        auto lb_stats = talloc.stats().for_dc(dc) - prev_lb_stats;

        auto elapsed = std::chrono::duration_cast<seconds_double>(end_time - start_time);
        rebalance_stats iteration_stats = {
            .elapsed_time = elapsed,
            .max_rebalance_time = elapsed,
            .rebalance_count = 1,
        };
        stats += iteration_stats;
        testlog.debug("Rebalance iteration {} took {:.3f} [s]: mig={}, bad={}, first_bad={}, eval={}, skiplist={}, skip: (load={}, rack={}, node={})",
                      i + 1, elapsed.count(),
                      lb_stats.migrations_produced,
                      lb_stats.bad_migrations,
                      lb_stats.bad_first_candidates,
                      lb_stats.candidates_evaluated,
                      lb_stats.migrations_from_skiplist,
                      lb_stats.migrations_skipped,
                      lb_stats.tablets_skipped_rack,
                      lb_stats.tablets_skipped_node);

        if (plan.empty()) {
            testlog.info("Rebalance took {:.3f} [s] after {} iteration(s)", stats.elapsed_time.count(), i + 1);
            return stats;
        }
        stm.mutate_token_metadata([&] (token_metadata& tm) {
            apply_plan(tm, plan);
            return make_ready_future<>();
        }).get();
    }
    throw std::runtime_error("rebalance_tablets(): convergence not reached within limit");
}

struct params {
    int iterations;
    int nodes;
    std::optional<int> tablets1;
    std::optional<int> tablets2;
    int rf1;
    int rf2;
    int shards;
    int scale1 = 1;
    int scale2 = 1;
};

struct table_balance {
    double shard_overcommit;
    double best_shard_overcommit;
    double node_overcommit;
};

constexpr auto nr_tables = 2;

struct cluster_balance {
    table_balance tables[nr_tables];
};

struct results {
    cluster_balance init;
    cluster_balance worst;
    cluster_balance last;
    rebalance_stats stats;
};

template<>
struct fmt::formatter<table_balance> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const table_balance& b, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "{{shard={:.2f} (best={:.2f}), node={:.2f}}}",
                              b.shard_overcommit, b.best_shard_overcommit, b.node_overcommit);
    }
};

template<>
struct fmt::formatter<cluster_balance> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const cluster_balance& r, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "{{table1={}, table2={}}}", r.tables[0], r.tables[1]);
    }
};

template<>
struct fmt::formatter<params> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const params& p, FormatContext& ctx) const {
        auto tablets1_per_shard = double(p.tablets1.value_or(0)) * p.rf1 / (p.nodes * p.shards);
        auto tablets2_per_shard = double(p.tablets2.value_or(0)) * p.rf2 / (p.nodes * p.shards);
        return fmt::format_to(ctx.out(), "{{iterations={}, nodes={}, tablets1={} ({:0.1f}/sh), tablets2={} ({:0.1f}/sh), rf1={}, rf2={}, shards={}}}",
                         p.iterations, p.nodes,
                         p.tablets1.value_or(0), tablets1_per_shard,
                         p.tablets2.value_or(0), tablets2_per_shard,
                         p.rf1, p.rf2, p.shards);
    }
};

future<results> test_load_balancing_with_many_tables(params p, bool tablet_aware) {
    auto cfg = tablet_cql_test_config();
    results global_res;
    co_await do_with_cql_env_thread([&] (auto& e) {
        const int n_hosts = p.nodes;
        const shard_id shard_count = p.shards;
        const int cycles = p.iterations;

        auto rack1 = endpoint_dc_rack{ dc, "rack-1" };

        std::vector<host_id> hosts;
        std::vector<inet_address> ips;

        int host_seq = 1;
        auto add_host = [&] {
            hosts.push_back(host_id(utils::make_random_uuid()));
            ips.push_back(inet_address(format("192.168.0.{}", host_seq++)));
            testlog.info("Added new node: {} ({})", hosts.back(), ips.back());
        };

        auto add_host_to_topology = [&] (token_metadata& tm, int i) {
            tm.update_host_id(hosts[i], ips[i]);
            tm.update_topology(hosts[i], rack1, node::state::normal, shard_count);
        };

        for (int i = 0; i < n_hosts; ++i) {
            add_host();
        }

        semaphore sem(1);
        auto stm = shared_token_metadata([&sem]() noexcept { return get_units(sem, 1); }, locator::token_metadata::config {
                locator::topology::config {
                        .this_endpoint = ips[0],
                        .this_host_id = hosts[0],
                        .local_dc_rack = rack1
                }
        });

        auto bootstrap = [&] {
            stm.mutate_token_metadata([&] (token_metadata& tm) {
                add_host();
                add_host_to_topology(tm, hosts.size() - 1);
                return make_ready_future<>();
            }).get();
            global_res.stats += rebalance_tablets(e.get_tablet_allocator().local(), stm);
        };

        auto decommission = [&] (host_id host) {
            auto i = std::distance(hosts.begin(), std::find(hosts.begin(), hosts.end(), host));
            if ((size_t)i == hosts.size()) {
                throw std::runtime_error(format("No such host: {}", host));
            }
            stm.mutate_token_metadata([&] (token_metadata& tm) {
                tm.update_topology(hosts[i], rack1, locator::node::state::being_decommissioned, shard_count);
                return make_ready_future<>();
            }).get();

            global_res.stats += rebalance_tablets(e.get_tablet_allocator().local(), stm);

            stm.mutate_token_metadata([&] (token_metadata& tm) {
                tm.remove_endpoint(host);
                return make_ready_future<>();
            }).get();
            testlog.info("Node decommissioned: {} ({})", hosts[i], ips[i]);
            hosts.erase(hosts.begin() + i);
            ips.erase(ips.begin() + i);
        };

        stm.mutate_token_metadata([&] (token_metadata& tm) {
            for (int i = 0; i < n_hosts; ++i) {
                add_host_to_topology(tm, i);
            }
            return make_ready_future<>();
        }).get();

        auto allocate = [&] (schema_ptr s, int rf, std::optional<int> initial_tablets) {
            replication_strategy_config_options opts;
            opts[rack1.dc] = format("{}", rf);
            network_topology_strategy tablet_rs(replication_strategy_params(opts, initial_tablets.value_or(0)));
            stm.mutate_token_metadata([&] (token_metadata& tm) -> future<> {
                auto map = co_await tablet_rs.allocate_tablets_for_new_table(s, stm.get(), 1);
                tm.tablets().set_tablet_map(s->id(), std::move(map));
            }).get();
        };

        auto id1 = add_table(e).get();
        auto id2 = add_table(e).get();
        schema_ptr s1 = e.local_db().find_schema(id1);
        schema_ptr s2 = e.local_db().find_schema(id2);
        allocate(s1, p.rf1, p.tablets1);
        allocate(s2, p.rf2, p.tablets2);

        auto check_balance = [&] () -> cluster_balance {
            cluster_balance res;

            testlog.debug("tablet metadata: {}", stm.get()->tablets());

            int table_index = 0;
            for (auto s : {s1, s2}) {
                load_sketch load(stm.get());
                load.populate(std::nullopt, s->id()).get();

                min_max_tracker<uint64_t> shard_load_minmax;
                min_max_tracker<uint64_t> node_load_minmax;
                uint64_t sum_node_load = 0;
                uint64_t shard_count = 0;
                for (auto h: hosts) {
                    auto minmax = load.get_shard_minmax(h);
                    auto node_load = load.get_load(h);
                    auto avg_shard_load = load.get_real_avg_shard_load(h);
                    auto overcommit = double(minmax.max()) / avg_shard_load;
                    shard_load_minmax.update(minmax.max());
                    shard_count += load.get_shard_count(h);
                    testlog.info("Load on host {} for table {}: total={}, min={}, max={}, spread={}, avg={:.2f}, overcommit={:.2f}",
                                 h, s->cf_name(), node_load, minmax.min(), minmax.max(), minmax.max() - minmax.min(), avg_shard_load, overcommit);
                    node_load_minmax.update(node_load);
                    sum_node_load += node_load;
                }

                auto avg_shard_load = double(sum_node_load) / shard_count;
                auto shard_overcommit = shard_load_minmax.max() / avg_shard_load;
                // Overcommit given the best distribution of tablets given current number of tablets.
                auto best_shard_overcommit = div_ceil(sum_node_load, shard_count) / avg_shard_load;
                testlog.info("Shard overcommit: {:.2f}, best={:.2f}", shard_overcommit, best_shard_overcommit);

                auto node_imbalance = node_load_minmax.max() - node_load_minmax.min();
                auto avg_node_load = double(sum_node_load) / hosts.size();
                auto node_overcommit = node_load_minmax.max() / avg_node_load;
                testlog.info("Node imbalance: min={}, max={}, spread={}, avg={:.2f}, overcommit={:.2f}",
                              node_load_minmax.min(), node_load_minmax.max(), node_imbalance, avg_node_load, node_overcommit);

                res.tables[table_index++] = {
                    .shard_overcommit = shard_overcommit,
                    .best_shard_overcommit = best_shard_overcommit,
                    .node_overcommit = node_overcommit
                };
            }

            for (int i = 0; i < nr_tables; i++) {
                auto t = res.tables[i];
                global_res.worst.tables[i].shard_overcommit = std::max(global_res.worst.tables[i].shard_overcommit, t.shard_overcommit);
                global_res.worst.tables[i].node_overcommit = std::max(global_res.worst.tables[i].node_overcommit, t.node_overcommit);
            }

            testlog.info("Overcommit: {}", res);
            return res;
        };

        testlog.debug("tablet metadata: {}", stm.get()->tablets());

        e.get_tablet_allocator().local().set_use_table_aware_balancing(tablet_aware);

        check_balance();

        rebalance_tablets(e.get_tablet_allocator().local(), stm);

        global_res.init = global_res.worst = check_balance();

        for (int i = 0; i < cycles; i++) {
            bootstrap();
            check_balance();

            decommission(hosts[0]);
            global_res.last = check_balance();
        }
    }, cfg);
    co_return global_res;
}

future<> run_simulation(const params& p, const sstring& name = "") {
    testlog.info("[run {}] params: {}", name, p);

    auto total_tablet_count = p.tablets1.value_or(0) * p.rf1 + p.tablets2.value_or(0) * p.rf2;
    testlog.info("[run {}] tablet count: {}", name, total_tablet_count);
    testlog.info("[run {}] tablet count / shard: {:.3f}", name, double(total_tablet_count) / (p.nodes * p.shards));

    auto res = co_await test_load_balancing_with_many_tables(p, true);
    testlog.info("[run {}] Overcommit       : init : {}", name, res.init);
    testlog.info("[run {}] Overcommit       : worst: {}", name, res.worst);
    testlog.info("[run {}] Overcommit       : last : {}", name, res.last);
    testlog.info("[run {}] Overcommit       : time : {:.3f} [s], max={:.3f} [s], count={}", name,
                 res.stats.elapsed_time.count(), res.stats.max_rebalance_time.count(), res.stats.rebalance_count);

    if (res.stats.elapsed_time > seconds_double(1)) {
        testlog.warn("[run {}] Scheduling took longer than 1s!", name);
    }

    auto old_res = co_await test_load_balancing_with_many_tables(p, false);
    testlog.info("[run {}] Overcommit (old) : init : {}", name, old_res.init);
    testlog.info("[run {}] Overcommit (old) : worst: {}", name, old_res.worst);
    testlog.info("[run {}] Overcommit (old) : last : {}", name, old_res.last);
    testlog.info("[run {}] Overcommit       : time : {:.3f} [s], max={:.3f} [s], count={}", name,
                 old_res.stats.elapsed_time.count(), old_res.stats.max_rebalance_time.count(), old_res.stats.rebalance_count);

    for (int i = 0; i < nr_tables; ++i) {
        if (res.worst.tables[i].shard_overcommit > old_res.worst.tables[i].shard_overcommit) {
            testlog.warn("[run {}] table{} shard overcommit worse!", name, i + 1);
        }
        auto overcommit = res.worst.tables[i].shard_overcommit;
        if (overcommit > 1.2) {
            testlog.warn("[run {}] table{} shard overcommit {:.2f} > 1.2!", name, i + 1, overcommit);
        }
    }
}

future<> run_simulations(const boost::program_options::variables_map& app_cfg) {
    for (auto i = 0; i < app_cfg["runs"].as<int>(); i++) {
        auto shards = 1 << tests::random::get_int(0, 8);
        auto rf1 = tests::random::get_int(1, 3);
        auto rf2 = tests::random::get_int(1, 3);
        auto scale1 = 1 << tests::random::get_int(0, 5);
        auto scale2 = 1 << tests::random::get_int(0, 5);
        auto nodes = tests::random::get_int(3, 6);
        params p {
            .iterations = app_cfg["iterations"].as<int>(),
            .nodes = nodes,
            .tablets1 = std::bit_ceil<size_t>(div_ceil(shards * nodes, rf1) * scale1),
            .tablets2 = std::bit_ceil<size_t>(div_ceil(shards * nodes, rf2) * scale2),
            .rf1 = rf1,
            .rf2 = rf2,
            .shards = shards,
            .scale1 = scale1,
            .scale2 = scale2,
        };

        auto name = format("#{}", i);
        co_await run_simulation(p, name);
    }
}

namespace perf {

int scylla_tablet_load_balancing_main(int argc, char** argv) {
    namespace bpo = boost::program_options;
    app_template app;
    app.add_options()
            ("runs", bpo::value<int>(), "Number of simulation runs.")
            ("iterations", bpo::value<int>()->default_value(8), "Number of topology-changing cycles in each run.")
            ("nodes", bpo::value<int>(), "Number of nodes in the cluster.")
            ("tablets1", bpo::value<int>(), "Number of tablets for the first table.")
            ("tablets2", bpo::value<int>(), "Number of tablets for the second table.")
            ("rf1", bpo::value<int>(), "Replication factor for the first table.")
            ("rf2", bpo::value<int>(), "Replication factor for the second table.")
            ("shards", bpo::value<int>(), "Number of shards per node.")
            ("verbose", "Enables standard logging")
            ;
    return app.run(argc, argv, [&] {
        return seastar::async([&] {
            if (!app.configuration().contains("verbose")) {
                auto testlog_level = logging::logger_registry().get_logger_level("testlog");
                logging::logger_registry().set_all_loggers_level(seastar::log_level::warn);
                logging::logger_registry().set_logger_level("testlog", testlog_level);
            }
            engine().at_exit([] {
                aborted.request_abort();
                return make_ready_future();
            });
            logalloc::prime_segment_pool(memory::stats().total_memory(), memory::min_free_memory()).get();
            try {
                if (app.configuration().contains("runs")) {
                    run_simulations(app.configuration()).get();
                } else {
                    params p {
                        .iterations = app.configuration()["iterations"].as<int>(),
                        .nodes = app.configuration()["nodes"].as<int>(),
                        .tablets1 = app.configuration()["tablets1"].as<int>(),
                        .tablets2 = app.configuration()["tablets2"].as<int>(),
                        .rf1 = app.configuration()["rf1"].as<int>(),
                        .rf2 = app.configuration()["rf2"].as<int>(),
                        .shards = app.configuration()["shards"].as<int>(),
                    };
                    run_simulation(p).get();
                }
            } catch (seastar::abort_requested_exception&) {
                // Ignore
            }
        });
    });
}

} // namespace perf
