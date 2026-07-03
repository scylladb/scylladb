/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "utils/assert.hh"
#include <fmt/ranges.h>
#include <limits>

#include <seastar/core/sharded.hh>
#include <seastar/core/app-template.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/reactor.hh>
#include <seastar/util/defer.hh>

#include "locator/tablets.hh"
#include "replica/tablet_mutation_builder.hh"
#include "replica/tablets.hh"
#include "locator/tablet_replication_strategy.hh"
#include "db/config.hh"
#include "schema/schema_builder.hh"
#include "service/storage_proxy.hh"
#include "service/raft/group0_state_machine.hh"
#include "service/raft/raft_group0_client.hh"
#include "raft/raft.hh"
#include "db/system_keyspace.hh"
#include "mutation/async_utils.hh"

#include "test/perf/perf.hh"
#include "test/lib/log.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/topology_builder.hh"

using namespace locator;
using namespace replica;

seastar::abort_source aborted;

static const size_t MiB = 1 << 20;

static
cql_test_config tablet_cql_test_config(unsigned schema_commitlog_segment_size_mb) {
    cql_test_config c;
    c.db_config->tablets_mode_for_new_keyspaces.set(db::tablets_mode_t::mode::enabled);
    // The tablet allocator scales a table's initial tablet count down when the
    // average number of tablet replicas per shard would exceed this goal (see
    // load_balancer::make_sizing_plan). The test pins each table to an exact
    // tablet count via tablet options, so raise the goal out of the way to keep
    // the count from being scaled down at creation time.
    c.db_config->tablets_per_shard_goal.set(std::numeric_limits<unsigned>::max());
    // The group0 raft command size limit is derived from the schema commitlog
    // segment size (roughly a quarter of it). Allow it to be raised so we can
    // apply tablet metadata commands several times larger than the ~32 MiB
    // default cap as a single command.
    if (schema_commitlog_segment_size_mb) {
        c.db_config->schema_commitlog_segment_size_in_mb.set(uint32_t(schema_commitlog_segment_size_mb));
    }
    return c;
}

static
future<table_id> add_table(cql_test_env& e, int tablet_count) {
    auto id = table_id(utils::UUID_gen::get_time_UUID());
    co_await e.create_table([id, tablet_count] (std::string_view ks_name) {
        return *schema_builder(this_smp_shard_count(), ks_name, id.to_sstring(), id)
                .with_column("p1", utf8_type, column_kind::partition_key)
                .with_column("r1", int32_type)
                // Create the table with exactly `tablet_count` tablets so that the
                // generated tablet metadata matches the table's tablet map and
                // applying it doesn't look like a tablet split/merge. pow2_count=false
                // lets us pin an arbitrary count without power-of-2 rounding.
                .set_tablet_options({
                    {"min_tablet_count", format("{}", tablet_count)},
                    {"max_tablet_count", format("{}", tablet_count)},
                    {"pow2_count", "false"},
                })
                .build();
    });
    co_return id;
}

static future<> test_basic_operations(app_template& app) {
    return do_with_cql_env_thread([&] (cql_test_env& e) {
        tablet_metadata tm;

        // Drive tablet metadata explicitly: the test pins each table's tablet
        // count via tablet options and spreads replicas across the fabricated
        // nodes built below. Disable the tablet load balancer so it doesn't
        // migrate or rebalance the tablets underneath the test once the metadata
        // is applied via group0.
        e.get_storage_service().local().set_tablet_balancing_enabled(false).get();

        int nr_tables = app.configuration()["tables"].as<int>();
        int tablets_per_table = app.configuration()["tablets-per-table"].as<int>();
        int rf = app.configuration()["rf"].as<int>();
        int nr_nodes = app.configuration()["nodes"].as<int>();
        if (nr_nodes == 0) {
            nr_nodes = rf;
        }
        if (nr_nodes < rf) {
            throw std::runtime_error(format("nodes ({}) must be >= rf ({})", nr_nodes, rf));
        }

        const auto shard_count = this_smp_shard_count();
        topology_builder topo(e);
        std::vector<host_id> nodes;
        nodes.reserve(nr_nodes);
        for (int i = 0; i < nr_nodes; ++i) {
            nodes.push_back(topo.add_node(service::node_state::normal, shard_count));
        }
        testlog.info("Added {} nodes to the topology", nodes.size());

        size_t total_tablets = 0;

        std::vector<table_id> ids;
        ids.resize(nr_tables);
        for (int i = 0; i < nr_tables; ++i) {
            ids[i] = add_table(e, tablets_per_table).get();
        }

        testlog.info("Generating tablet metadata");

        for (int i = 0; i < nr_tables; ++i) {
            tablet_map tmap(tablets_per_table);

            for (tablet_id j : tmap.tablet_ids()) {
                aborted.check();
                thread::maybe_yield();
                tablet_replica_set replicas;
                for (int k = 0; k < rf; ++k) {
                    const auto host = nodes[(total_tablets + k) % nodes.size()];
                    replicas.push_back({host, 0});
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

        utils::chunked_vector<canonical_mutation> muts;
        auto time_to_read_muts = duration_in_seconds([&] {
            replica::read_tablet_mutations(e.local_qp().proxy().get_db(), [&] (canonical_mutation m) {
                muts.emplace_back(m);
            }).get();
        });

        testlog.info("Read mutations in {:.6f} [ms] {} mutations", time_to_read_muts.count() * 1000, muts.size());

        auto time_to_read_hosts = duration_in_seconds([&] {
            replica::read_required_hosts(e.local_qp()).get();
        });

        testlog.info("Read required hosts in {:.6f} [ms]", time_to_read_hosts.count() * 1000);

        auto cm_size = 0;
        for (auto&& cm : muts) {
            cm_size += cm.representation().size();
        }

        testlog.info("Size of canonical mutations: {:.6f} [MiB]", double(cm_size) / MiB);

        // Measure the group0 command size and the time to apply it when tablet
        // mutations are routed through group0_update_collector, the way the write
        // path builds and commits the command.
        {
            const size_t max_mutation_size = app.configuration()["command-max-mutation-size-mb"].as<size_t>() * MiB;
            const auto tablets_schema = db::system_keyspace::tablets();

            utils::chunked_vector<mutation> unfrozen_muts;
            unfrozen_muts.reserve(muts.size());
            for (const auto& cm : muts) {
                unfrozen_muts.push_back(to_mutation_gently(cm, tablets_schema).get());
            }

            service::group0_update_collector collector(max_mutation_size);
            utils::chunked_vector<canonical_mutation> collected;
            auto time_to_collect = duration_in_seconds([&] {
                for (auto& m : unfrozen_muts) {
                    collector.add(std::move(m)).get();
                }
                collected = collector.collect().get();
            });

            size_t collected_size = 0;
            size_t max_piece_size = 0;
            for (const auto& cm : collected) {
                collected_size += cm.representation().size();
                max_piece_size = std::max<size_t>(max_piece_size, cm.representation().size());
            }

            testlog.info("Size of group0 command via group0_update_collector (max_mutation_size={} MiB): "
                         "{:.6f} [MiB] ({} serialized mutations, max {:.6f} MiB), collected in {:.6f} [ms]",
                         max_mutation_size / MiB, double(collected_size) / MiB, collected.size(),
                         double(max_piece_size) / MiB, time_to_collect.count() * 1000);

            auto& group0_client = e.get_raft_group0_client();
            seastar::abort_source as;
            try {
                auto time_to_apply = duration_in_seconds([&] {
                    auto guard = group0_client.start_operation(as).get();
                    auto cmd = group0_client.prepare_command(
                            service::topology_change{.mutations{std::move(collected)}}, guard, "Apply whole tablet metadata");
                    group0_client.add_entry(std::move(cmd), std::move(guard), as).get();
                });
                testlog.info("Applied whole tablet metadata to group0 in {:.6f} [ms]", time_to_apply.count() * 1000);
            } catch (const raft::command_is_too_big_error& ex) {
                testlog.error("Could not apply tablet metadata to group0 as a single command: {}", ex);
            }
        }

        auto&& tablets_table = e.local_db().find_column_family(db::system_keyspace::tablets());
        testlog.info("Disk space used by system.tablets: {:.6f} [MiB]", double(tablets_table.get_stats().live_disk_space_used.on_disk) / MiB);

        locator::tablet_metadata_change_hint hint;

        // Migrate one tablet to a different shard on its first replica's node.
        // This is a valid intra-node migration (the destination host still
        // exists in the topology) and exercises the tablet_metadata_change_hint
        // / partial reload path below.
        {
            const auto last_table_id = ids.back();
            const auto& tmap = tm.get_tablet_map(last_table_id);

            auto ts = utils::UUID_gen::micros_timestamp(e.get_system_keyspace().local().get_last_group0_state_id().get()) + 1;

            const auto tb = tmap.first_tablet();
            replica::tablet_mutation_builder builder(ts++, last_table_id);
            const auto token = tmap.get_last_token(tb);

            // Move the first replica to a different shard on the same node,
            // leaving the other replicas in place.
            auto new_replicas = tmap.get_tablet_info(tb).replicas;
            const auto dst_shard = shard_id(shard_count > 1 ? (new_replicas.front().shard ^ 1) : new_replicas.front().shard);
            new_replicas.front() = tablet_replica {new_replicas.front().host, dst_shard};
            builder.set_new_replicas(token, std::move(new_replicas));
            builder.set_stage(token, tablet_transition_stage::streaming);
            builder.set_transition(token, tablet_transition_kind::migration);

            utils::chunked_vector<mutation> muts;
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
        update_tablet_metadata(e.local_db(), e.local_qp(), tm, hint).get();
        const auto end_partial_reload = clk::now();
        const auto partial_reload_duration = std::chrono::duration<double, std::milli>(end_partial_reload - start_partial_reload);

        assert(tm == tm_full_reload);

        testlog.info("Tablet metadata reload:\nfull    {:>8.2f}ms\npartial {:>8.2f}ms", full_reload_duration.count(), partial_reload_duration.count());
    }, tablet_cql_test_config(app.configuration()["schema-commitlog-segment-size-mb"].as<unsigned>()));
}

namespace perf {

int scylla_tablets_main(int argc, char** argv) {
    namespace bpo = boost::program_options;
    app_template app;
    app.add_options()
            ("tables", bpo::value<int>()->default_value(8), "Number of tables to create.")
            ("tablets-per-table", bpo::value<int>()->default_value(16384), "Number of tablets per table.")
            ("rf", bpo::value<int>()->default_value(3), "Number of replicas per tablet.")
            ("nodes", bpo::value<int>()->default_value(0),
                    "Number of nodes to add to the topology and spread tablet replicas over "
                    "(0 = use rf). Must be >= rf.")
            ("command-max-mutation-size-mb", bpo::value<size_t>()->default_value(service::group0_update_collector::default_max_mutation_size / MiB),
                    "Max in-memory size of a single mutation in the group0 command built via group0_update_collector (controls splitting).")
            ("schema-commitlog-segment-size-mb", bpo::value<unsigned>()->default_value(0),
                    "Override schema_commitlog_segment_size_in_mb (0 = leave the default). The group0 raft "
                    "command size limit is roughly a quarter of this, so raising it allows applying larger "
                    "tablet metadata commands as a single command (see SCYLLADB-2856).")
            ("verbose", "Enables standard logging")
            ;
    return app.run(argc, argv, [&] {
        return seastar::async([&] {
            if (!app.configuration().contains("verbose")) {
                auto testlog_level = logging::logger_registry().get_logger_level("testlog");
                logging::logger_registry().set_all_loggers_level(seastar::log_level::warn);
                logging::logger_registry().set_logger_level("testlog", testlog_level);
            }
            auto stop_test = defer([] noexcept {
                aborted.request_abort();
            });
            logalloc::prime_segment_pool(memory::stats().total_memory(), memory::min_free_memory()).get();
            try {
                test_basic_operations(app).get();
            } catch (seastar::abort_requested_exception&) {
                // Ignore
            } catch (...) {
                on_fatal_internal_error(testlog, format("Aborting on unhandled exception: {}", std::current_exception()));
            }
        });
    });
}

} // namespace perf
