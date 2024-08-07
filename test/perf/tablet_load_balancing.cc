/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <fmt/ranges.h>
#include <fmt/color.h>
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
#include "test/lib/tablets.hh"
#include "test/lib/random_utils.hh"

using namespace locator;
using namespace replica;
using namespace service;
using namespace load_balancer_test;

static seastar::abort_source aborted;

future<> run_simulation(const params& p, bool check_table_unaware, const sstring& name = "") {
    testlog.info("[run {}] params: {}", name, p);

    auto total_tablet_count = p.tablets1.value_or(0) * p.rf1 + p.tablets2.value_or(0) * p.rf2;
    testlog.info("[run {}] tablet count: {}", name, total_tablet_count);
    testlog.info("[run {}] tablet count / shard: {:.3f}", name, double(total_tablet_count) / (p.nodes * p.shards));

    auto check_results = [&] (const results& res, const sstring& tag) {
        testlog.info("[run {}] Overcommit {:<5} : init : {}", name, tag, res.init);
        testlog.info("[run {}] Overcommit {:<5} : worst: {}", name, tag, res.worst);
        testlog.info("[run {}] Overcommit {:<5} : last : {}", name, tag, res.last);
        testlog.info("[run {}] Overcommit {:<5} : time : {:.3f} [s], max={:.3f} [s], count={}", name, tag,
                     res.stats.elapsed_time.count(), res.stats.max_rebalance_time.count(), res.stats.rebalance_count);

        if (res.stats.elapsed_time > seconds_double(1)) {
            testlog.warn("[run {}] Scheduling took longer than 1s!", name);
        }

        for (int i = 0; i < nr_tables; ++i) {
            auto overcommit = res.worst.tables[i].shard_overcommit;
            if (overcommit > 1.2) {
                testlog.warn("[run {}] table{} shard overcommit {:.2f} > 1.2!", name, i + 1, overcommit);
            }
        }
    };

    auto res = co_await test_load_balancing_with_many_tables(p, true);
    check_results(res, "");

    if (check_table_unaware) {
        auto old_res = co_await test_load_balancing_with_many_tables(p, false);
        check_results(old_res, "(old)");

        for (int i = 0; i < nr_tables; ++i) {
            if (res.worst.tables[i].shard_overcommit > old_res.worst.tables[i].shard_overcommit) {
                testlog.warn("[run {}] table{} shard overcommit worse!", name, i + 1);
            }
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
        co_await run_simulation(p, app_cfg["check-table-unaware"].as<bool>(), name);
    }
}

namespace perf {

int scylla_tablet_load_balancing_main(int argc, char** argv) {
    namespace bpo = boost::program_options;
    app_template app;
    app.add_options()
            ("runs", bpo::value<int>(), "Number of simulation runs.")
            ("check-table-unaware", bpo::value<bool>()->default_value(false), "Compare with table-unaware load balancer.")
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
                    run_simulation(p, app.configuration()["check-table-unaware"].as<bool>()).get();
                }
            } catch (seastar::abort_requested_exception&) {
                // Ignore
            }
        });
    });
}

} // namespace perf
