/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <fmt/ranges.h>
#include <seastar/core/sharded.hh>
#include <seastar/core/app-template.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/reactor.hh>

#include "locator/tablets.hh"
#include "service/tablet_allocator.hh"
#include "locator/tablet_replication_strategy.hh"
#include "locator/network_topology_strategy.hh"
#include "test/lib/topology_builder.hh"
#include "replica/tablets.hh"
#include "db/config.hh"
#include "schema/schema_builder.hh"
#include "service/storage_proxy.hh"
#include "db/system_keyspace.hh"
#include "tools/utils.hh"

#include "test/perf/perf.hh"
#include "test/lib/log.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/random_utils.hh"

using namespace locator;
using namespace replica;
using namespace service;
using namespace tools::utils;

namespace bpo = boost::program_options;

static const sstring dc = "dc1";

static
cql_test_config tablet_cql_test_config() {
    cql_test_config c;
    return c;
}

static
sstring add_keyspace(cql_test_env& e, std::unordered_map<sstring, int> dc_rf, int initial_tablets = 0) {
    static std::atomic<int> ks_id = 0;
    auto ks_name = fmt::format("keyspace{}", ks_id.fetch_add(1));
    sstring rf_options;
    for (auto& [dc, rf] : dc_rf) {
        rf_options += format(", '{}': {}", dc, rf);
    }
    e.execute_cql(fmt::format("create keyspace {} with replication = {{'class': 'NetworkTopologyStrategy'{}}}"
                              " and tablets = {{'enabled': true, 'initial': {}}}",
                              ks_name, rf_options, initial_tablets)).get();
    return ks_name;
}

static
future<table_id> add_table(cql_test_env& e, sstring ks_name) {
    auto id = table_id(utils::UUID_gen::get_time_UUID());
    co_await e.create_table([&] (std::string_view) {
        return *schema_builder(this_smp_shard_count(), ks_name, id.to_sstring(), id)
                .with_column("p1", utf8_type, column_kind::partition_key)
                .with_column("r1", int32_type)
                .build();
    });
    co_return id;
}

using seconds_double = std::chrono::duration<double>;

struct measurement {
    seconds_double elapsed;
    size_t table_count;
    size_t tables_changed;
    bool with_hint;
};

static
void run_hint_propagation_test(cql_test_env& e, const std::vector<table_id>& table_ids, size_t tables_to_change, bool with_hint) {
    auto& stm = e.shared_token_metadata().local();

    locator::tablet_metadata_change_hint hint;
    for (size_t i = 0; i < tables_to_change && i < table_ids.size(); ++i) {
        auto tid = table_ids[i];
        stm.mutate_token_metadata([&] (token_metadata& tm) -> future<> {
            co_await tm.tablets().mutate_tablet_map_async(tid, [] (tablet_map& tmap) {
                auto rd = tmap.resize_decision();
                rd.sequence_number++;
                tmap.set_resize_decision(rd);
                return make_ready_future();
            });
        }).get();
        hint.tables.emplace(tid, tablet_metadata_change_hint::table_hint{});
    }

    auto start = std::chrono::steady_clock::now();
    if (with_hint) {
        e.get_storage_service().local().update_tablet_metadata(hint).get();
    } else {
        e.get_storage_service().local().update_tablet_metadata({}).get();
    }
    auto end = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<seconds_double>(end - start);

    testlog.info("tablets={} changed={} hint={} time={:.4f}s",
                 table_ids.size(), tables_to_change, with_hint ? "yes" : "no", elapsed.count());
}

static
void run_measurement(const bpo::variables_map& opts) {
    auto table_count = opts["tables"].as<int>();
    auto tables_to_change = opts["tables-to-change"].as<int>();
    auto iterations = opts["iterations"].as<int>();

    auto cfg = tablet_cql_test_config();

    do_with_cql_env_thread([&] (auto& e) {
        topology_builder topo(e);
        topo.add_node(service::node_state::normal, this_smp_shard_count());

        // Create the tables once and reuse them for every call below, so
        // each measurement runs against the same, fixed table_count
        // instead of accumulating tables across calls and iterations.
        auto ks_name = add_keyspace(e, {{dc, 1}}, 8);
        std::vector<table_id> table_ids;
        table_ids.reserve(table_count);
        for (int i = 0; i < table_count; ++i) {
            table_ids.push_back(add_table(e, ks_name).get());
        }
        e.get_storage_service().local().update_tablet_metadata({}).get();

        for (int i = 0; i < iterations; ++i) {
            testlog.info("=== Iteration {}/{} ===", i + 1, iterations);

            run_hint_propagation_test(e, table_ids, tables_to_change, false);
            run_hint_propagation_test(e, table_ids, tables_to_change, true);
        }
    }, cfg).get();
}

using operation_func = std::function<void(const bpo::variables_map&)>;

const std::vector<operation_option> global_options {};
const std::vector<operation_option> global_positional_options{};

const std::map<operation, operation_func> operations_with_func{
    {
        {"measure-hint",
         "Measures update_tablet_metadata time with and without hint propagation",
         "",
         {
            typed_option<int>("iterations", 5, "Number of measurement iterations."),
            typed_option<int>("tables", 50, "Total number of tables to create."),
            typed_option<int>("tables-to-change", 2, "Number of tables to modify in each iteration."),
          }
        }, &run_measurement
    }
};

namespace perf {

int scylla_perf_tablet_hint_propagation_main(int argc, char** argv) {
    const auto operations = operations_with_func | std::views::keys | std::ranges::to<std::vector>();
    tool_app_template::config app_cfg{
            .name = "perf-tablet-hint-propagation",
            .description = "Measures the performance benefit of tablet hint propagation",
            .logger_name = testlog.name(),
            .lsa_segment_pool_backend_size_mb = 100,
            .operations = std::move(operations),
            .global_options = &global_options,
            .global_positional_options = &global_positional_options,
            .db_cfg_ext = db_config_and_extensions()
    };
    tool_app_template app(std::move(app_cfg));

    return app.run_async(argc, argv, [] (const operation& operation, const bpo::variables_map& app_config) {
        try {
            operations_with_func.at(operation)(app_config);
            return 0;
        } catch (seastar::abort_requested_exception&) {
            // Ignore
        }
        return 1;
    });
}

} // namespace perf
