/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <chrono>
#include <stdexcept>

#include <fmt/format.h>
#include <seastar/core/app-template.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/thread.hh>

#include "locator/tablets.hh"
#include "replica/database.hh"
#include "locator/token_metadata.hh"
#include "test/lib/topology_builder.hh"
#include "test/lib/tablet_test_utils.hh"
#include "db/config.hh"
#include "service/storage_service.hh"
#include "utils/logalloc.hh"

#include "test/perf/perf.hh"
#include "test/lib/log.hh"
#include "test/lib/cql_test_env.hh"

using namespace locator;
using namespace service;
using tablet_test_utils::add_table;
using tablet_test_utils::add_keyspace;

namespace bpo = boost::program_options;

// Per-shard state: the old token_metadata and table ERMs are held across the timed update, so the
// timed region never pays to destroy replaced tablet_maps, and the counters at the start of it.
static thread_local locator::token_metadata_ptr held_tm;
static thread_local std::vector<locator::effective_replication_map_ptr> held_erms;
static thread_local linux_perf_event shard_insns = linux_perf_event::user_instructions_retired();
static thread_local uint64_t start_insns, start_allocs;

// Times a tablet metadata update reloading only `touched`. With `replace_maps` every table's
// tablet_map is replaced beforehand (outside the timed region), so the gate can skip no table.
// Time is wall-clock on shard 0; instructions and allocations are summed over all shards.
static
perf_sample run_arm(cql_test_env& e, table_id touched, const std::vector<table_id>& table_ids, bool replace_maps) {
    if (replace_maps) {
        e.shared_token_metadata().local().mutate_token_metadata([&] (token_metadata& tm) -> future<> {
            for (auto id : table_ids) {
                co_await tm.tablets().mutate_tablet_map_async(id, [] (tablet_map&) { return make_ready_future<>(); });
            }
        }).get();
    }
    locator::tablet_metadata_change_hint hint;
    hint.tables.emplace(touched, tablet_metadata_change_hint::table_hint{.table_id = touched});

    e.db().invoke_on_all([&table_ids] (replica::database& db) {
        held_tm = db.get_token_metadata_ptr();
        held_erms.clear();
        for (auto id : table_ids) {
            held_erms.push_back(db.find_column_family(id).get_effective_replication_map());
        }
        shard_insns.enable();
        start_insns = shard_insns.read();
        start_allocs = memory::stats().mallocs();
    }).get();

    auto start = std::chrono::steady_clock::now();
    e.get_storage_service().local().update_tablet_metadata(hint).get();
    auto elapsed = std::chrono::steady_clock::now() - start;

    perf_sample total;
    total.time = elapsed;
    // Per-shard deltas, summed; the reads run before the held maps are released.
    auto per_shard = e.db().map_reduce0([] (replica::database&) {
        return std::pair<uint64_t, uint64_t>(shard_insns.read() - start_insns, memory::stats().mallocs() - start_allocs);
    }, std::pair<uint64_t, uint64_t>{}, [] (auto a, auto b) {
        return std::pair<uint64_t, uint64_t>(a.first + b.first, a.second + b.second);
    }).get();
    total.instructions = per_shard.first;
    total.allocations = per_shard.second;

    e.db().invoke_on_all([] (replica::database&) {
        held_erms.clear();
        held_tm = {};
    }).get();
    return total;
}

static
void run_measurement(const bpo::variables_map& opts) {
    auto table_count = opts["tables"].as<int>();
    auto iterations = opts["iterations"].as<int>();
    if (table_count <= 0 || iterations <= 0) {
        throw std::invalid_argument("tables and iterations must be positive");
    }

    do_with_cql_env_thread([&] (auto& e) {
        // Keeps the load balancer from moving tablets mid-measurement.
        topology_builder topo(e);

        // Replicas on this node, so the per-storage-group sweep is included.
        auto local_dc = e.local_db().get_token_metadata().get_topology().get_location().dc;
        auto ks_name = add_keyspace(e, {{local_dc, 1}}, 8);
        std::vector<table_id> table_ids;
        table_ids.reserve(table_count);
        for (int i = 0; i < table_count; ++i) {
            table_ids.push_back(add_table(e, ks_name).get());
        }

        // Blocks group0 applies and topology-coordinator commands, so no background topology
        // load commits token metadata mid-measurement.
        abort_source as;
        auto guard = e.get_raft_group0_client().start_operation(as).get();

        perf_sample shared_total, replaced_total;
        for (int i = 0; i < iterations; ++i) {
            // Alternate which arm runs first so ordering effects cancel out.
            perf_sample shared, replaced;
            if (i % 2 == 0) {
                shared = run_arm(e, table_ids[0], table_ids, false);
                replaced = run_arm(e, table_ids[0], table_ids, true);
            } else {
                replaced = run_arm(e, table_ids[0], table_ids, true);
                shared = run_arm(e, table_ids[0], table_ids, false);
            }
            shared_total += shared;
            replaced_total += replaced;
        }
        auto report = [&] (const char* name, const perf_sample& t) {
            testlog.info("{} tablet_maps, tables={}, per update over {} iterations: {:.4f}s, {:.0f} insns, {:.1f} allocs",
                         name, table_count, iterations, t.time.count() / iterations,
                         double(t.instructions) / iterations, double(t.allocations) / iterations);
        };
        report("shared", shared_total);
        report("replaced", replaced_total);
        testlog.info("ratio replaced/shared: time {:.2f}, insns {:.2f}, allocs {:.2f}",
                     replaced_total.time / shared_total.time,
                     double(replaced_total.instructions) / shared_total.instructions,
                     double(replaced_total.allocations) / shared_total.allocations);
    }).get();
}

namespace perf {

int scylla_perf_tablet_hint_propagation_main(int argc, char** argv) {
    app_template app;
    app.add_options()
            ("iterations", bpo::value<int>()->default_value(50), "Number of measurement iterations.")
            ("tables", bpo::value<int>()->default_value(50), "Total number of tables to create.");
    return app.run(argc, argv, [&] {
        return seastar::async([&] {
            logging::logger_registry().set_all_loggers_level(seastar::log_level::warn);
            logging::logger_registry().set_logger_level("testlog", seastar::log_level::info);
            logalloc::prime_segment_pool(memory::stats().total_memory(), memory::min_free_memory()).get();
            run_measurement(app.configuration());
        });
    });
}

} // namespace perf
