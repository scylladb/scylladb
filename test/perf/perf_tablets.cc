/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "utils/assert.hh"
#include <fmt/ranges.h>

#include <seastar/core/distributed.hh>
#include <seastar/core/app-template.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/reactor.hh>

#include "locator/tablets.hh"
#include "replica/tablet_mutation_builder.hh"
#include "replica/tablets.hh"
#include "locator/tablet_replication_strategy.hh"
#include "db/config.hh"
#include "schema/schema_builder.hh"
#include "service/storage_proxy.hh"
#include "db/system_keyspace.hh"

#include "test/perf/perf.hh"
#include "test/lib/log.hh"
#include "test/lib/cql_test_env.hh"

using namespace locator;
using namespace replica;

seastar::abort_source aborted;

static const size_t MiB = 1 << 20;

static
cql_test_config tablet_cql_test_config() {
    cql_test_config c;
    c.db_config->enable_tablets.set(true);
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

static future<> test_basic_operations(app_template& app) {
    return do_with_cql_env_thread([&] (cql_test_env& e) {
        tablet_metadata tm;

        auto h1 = host_id(utils::UUID_gen::get_time_UUID());
        auto h2 = host_id(utils::UUID_gen::get_time_UUID());

        int nr_tables = app.configuration()["tables"].as<int>();
        int tablets_per_table = app.configuration()["tablets-per-table"].as<int>();
        int rf = app.configuration()["rf"].as<int>();

        size_t total_tablets = 0;

        std::vector<table_id> ids;
        ids.resize(nr_tables);
        for (int i = 0; i < nr_tables; ++i) {
            ids[i] = add_table(e).get();
        }

        testlog.info("Generating tablet metadata");

        for (int i = 0; i < nr_tables; ++i) {
            tablet_map tmap(tablets_per_table);

            for (tablet_id j : tmap.tablet_ids()) {
                aborted.check();
                thread::maybe_yield();
                tablet_replica_set replicas;
                for (int k = 0; k < rf; ++k) {
                    replicas.push_back({h1, 0});
                }
                SCYLLA_ASSERT(std::cmp_equal(replicas.size(), rf));
                tmap.set_tablet(j, tablet_info{std::move(replicas)});
                ++total_tablets;
            }

            tm.set_tablet_map(ids[i], std::move(tmap));
        }

        testlog.info("Total tablet count: {}", total_tablets);

        testlog.info("Size of tablet_metadata in memory: {} KiB",
                     (tm.external_memory_usage() + sizeof(tablet_metadata)) / 1024);

        tablet_metadata tm2;
        auto time_to_copy = duration_in_seconds([&] {
            tm2 = tm.copy().get();
        });

        testlog.info("Copied in {:.6f} [ms]", time_to_copy.count() * 1000);

        auto time_to_clear = duration_in_seconds([&] {
            tm2.clear_gently().get();
        });

        testlog.info("Cleared in {:.6f} [ms]", time_to_clear.count() * 1000);

        auto time_to_save = duration_in_seconds([&] {
            save_tablet_metadata(e.local_db(), tm, api::new_timestamp()).get();
        });

        testlog.info("Saved in {:.6f} [ms]", time_to_save.count() * 1000);

        auto time_to_read = duration_in_seconds([&] {
            tm2 = read_tablet_metadata(e.local_qp()).get();
        });
        SCYLLA_ASSERT(tm == tm2);

        testlog.info("Read in {:.6f} [ms]", time_to_read.count() * 1000);

        std::vector<canonical_mutation> muts;
        auto time_to_read_muts = duration_in_seconds([&] {
            muts = replica::read_tablet_mutations(e.local_qp().proxy().get_db()).get();
        });

        testlog.info("Read mutations in {:.6f} [ms]", time_to_read_muts.count() * 1000);

        auto time_to_read_hosts = duration_in_seconds([&] {
            replica::read_required_hosts(e.local_qp()).get();
        });

        testlog.info("Read required hosts in {:.6f} [ms]", time_to_read_hosts.count() * 1000);

        auto cm_size = 0;
        for (auto&& cm : muts) {
            cm_size += cm.representation().size();
        }

        testlog.info("Size of canonical mutations: {:.6f} [MiB]", double(cm_size) / MiB);

        auto&& tablets_table = e.local_db().find_column_family(db::system_keyspace::tablets());
        testlog.info("Disk space used by system.tablets: {:.6f} [MiB]", double(tablets_table.get_stats().live_disk_space_used) / MiB);

        locator::tablet_metadata_change_hint hint;

        // Migrate one tablet to h2
        {
            const auto last_table_id = ids.back();
            const auto& tmap = tm.get_tablet_map(last_table_id);

            auto ts = utils::UUID_gen::micros_timestamp(e.get_system_keyspace().local().get_last_group0_state_id().get()) + 1;

            const auto tb = tmap.first_tablet();
            replica::tablet_mutation_builder builder(ts++, last_table_id);
            const auto token = tmap.get_last_token(tb);

            builder.set_new_replicas(token,
                tablet_replica_set {
                    tablet_replica {h2, 0},
                }
            );
            builder.set_stage(token, tablet_transition_stage::streaming);
            builder.set_transition(token, tablet_transition_kind::migration);

            std::vector<mutation> muts;
            muts.push_back(builder.build());
            e.local_db().apply(freeze(muts), db::no_timeout).get();
            replica::update_tablet_metadata_change_hint(hint, muts.front());
        }

        using clk = std::chrono::high_resolution_clock;

        const auto start_full_reload = clk::now();
        const auto tm_full_reload = read_tablet_metadata(e.local_qp()).get();
        const auto end_full_reload = clk::now();
        const auto full_reload_duration = std::chrono::duration<double, std::milli>(end_full_reload - start_full_reload);

        const auto start_partial_reload = clk::now();
        update_tablet_metadata(e.local_qp(), tm, hint).get();
        const auto end_partial_reload = clk::now();
        const auto partial_reload_duration = std::chrono::duration<double, std::milli>(end_partial_reload - start_partial_reload);

        assert(tm == tm_full_reload);

        testlog.info("Tablet metadata reload:\nfull    {:>8.2f}ms\npartial {:>8.2f}ms", full_reload_duration.count(), partial_reload_duration.count());
    }, tablet_cql_test_config());
}

namespace perf {

int scylla_tablets_main(int argc, char** argv) {
    namespace bpo = boost::program_options;
    app_template app;
    app.add_options()
            ("tables", bpo::value<int>()->default_value(100), "Number of tables to create.")
            ("tablets-per-table", bpo::value<int>()->default_value(2048), "Number of tablets per table.")
            ("rf", bpo::value<int>()->default_value(3), "Number of replicas per tablet.")
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
                test_basic_operations(app).get();
            } catch (seastar::abort_requested_exception&) {
                // Ignore
            }
        });
    });
}

} // namespace perf
