/*
 * Copyright (C) 2023-present-2020 ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */



#include <seastar/core/shard_id.hh>
#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>
#include "test/lib/random_utils.hh"
#include "service/topology_mutation.hh"
#include "service/storage_service.hh"
#include <fmt/ranges.h>
#include <seastar/testing/thread_test_case.hh>
#include "test/lib/cql_test_env.hh"
#include "test/lib/log.hh"
#include "test/lib/simple_schema.hh"
#include "test/lib/key_utils.hh"
#include "test/lib/test_utils.hh"
#include "test/lib/topology_builder.hh"
#include "db/config.hh"
#include "db/schema_tables.hh"
#include "schema/schema_builder.hh"

#include "replica/tablets.hh"
#include "replica/tablet_mutation_builder.hh"
#include "locator/tablets.hh"
#include "service/tablet_allocator.hh"
#include "locator/tablet_replication_strategy.hh"
#include "locator/tablet_sharder.hh"
#include "locator/load_sketch.hh"
#include "locator/snitch_base.hh"
#include "utils/UUID_gen.hh"
#include "utils/error_injection.hh"
#include "utils/to_string.hh"
#include "service/topology_coordinator.hh"
#include "service/topology_state_machine.hh"

#include <boost/regex.hpp>
#include <atomic>

BOOST_AUTO_TEST_SUITE(tablets_test)

using namespace locator;
using namespace replica;
using namespace service;

struct shared_load_stats {
    locator::load_stats stats;

    locator::load_stats_ptr get() {
        return make_lw_shared(stats);
    }

    void set_size(table_id table, size_t size_in_bytes) {
        stats.tables[table].size_in_bytes = size_in_bytes;
    }

    void set_split_ready_seq_number(table_id table, size_t seq_number) {
        stats.tables[table].split_ready_seq_number = seq_number;
    }
};

static api::timestamp_type current_timestamp(cql_test_env& e) {
    // Mutations in system.tablets got there via group0, so in order for new
    // mutations to take effect, their timestamp should be "later" than that
    return utils::UUID_gen::micros_timestamp(e.get_system_keyspace().local().get_last_group0_state_id().get()) + 1;
}

static
void verify_tablet_metadata_persistence(cql_test_env& env, const tablet_metadata& tm, api::timestamp_type& ts) {
    save_tablet_metadata(env.local_db(), tm, ts++).get();
    auto tm2 = read_tablet_metadata(env.local_qp()).get();
    BOOST_REQUIRE_EQUAL(tm, tm2);
}

static
void verify_tablet_metadata_update(cql_test_env& env, tablet_metadata& tm, std::vector<mutation> muts) {
    testlog.trace("verify_tablet_metadata_update(): {}", muts);

    auto& db = env.local_db();

    db.apply(freeze(muts), db::no_timeout).get();

    locator::tablet_metadata_change_hint hint;
    for (const auto& mut : muts) {
        update_tablet_metadata_change_hint(hint, mut);
    }

    update_tablet_metadata(db, env.local_qp(), tm, hint).get();

    auto tm_reload = read_tablet_metadata(env.local_qp()).get();
    BOOST_REQUIRE_EQUAL(tm, tm_reload);
}

static
cql_test_config tablet_cql_test_config(bool enable_tablets = true) {
    cql_test_config c;
    c.db_config->enable_tablets(enable_tablets);
    if (enable_tablets) {
        c.initial_tablets = 2;
    }
    return c;
}

static
future<table_id> add_table(cql_test_env& e, sstring test_ks_name = "") {
    auto id = table_id(utils::UUID_gen::get_time_UUID());
    co_await e.create_table([&] (std::string_view ks_name) {
        if (!test_ks_name.empty()) {
            ks_name = test_ks_name;
        }
        return *schema_builder(ks_name, id.to_sstring(), id)
                .with_column("p1", utf8_type, column_kind::partition_key)
                .with_column("r1", int32_type)
                .build();
    });
    co_return id;
}

// Run in a seastar thread
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

// Run in a seastar thread
void mutate_tablets(cql_test_env& e, const group0_guard& guard, seastar::noncopyable_function<future<>(tablet_metadata&)> mutator) {
    auto& stm = e.shared_token_metadata().local();
    stm.mutate_token_metadata([&] (token_metadata& tm) -> future<> {
        return mutator(tm.tablets());
    }).get();
    save_tablet_metadata(e.local_db(), stm.get()->tablets(), guard.write_timestamp()).get();
}

// Run in a seastar thread
void mutate_tablets(cql_test_env& e, seastar::noncopyable_function<future<>(tablet_metadata&)> mutator) {
    abort_source as;
    auto guard = e.get_raft_group0_client().start_operation(as).get();
    mutate_tablets(e, guard, std::move(mutator));
}

SEASTAR_TEST_CASE(test_tablet_metadata_persistence) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        auto h1 = host_id(utils::UUID_gen::get_time_UUID());
        auto h2 = host_id(utils::UUID_gen::get_time_UUID());
        auto h3 = host_id(utils::UUID_gen::get_time_UUID());

        auto table1 = add_table(e).get();
        auto table2 = add_table(e).get();
        auto ts = current_timestamp(e);

        {
            tablet_metadata tm = read_tablet_metadata(e.local_qp()).get();

            // Add table1
            {
                tablet_map tmap(1);
                tmap.set_tablet(tmap.first_tablet(), tablet_info {
                    tablet_replica_set {
                        tablet_replica {h1, 0},
                        tablet_replica {h2, 3},
                        tablet_replica {h3, 1},
                    },
                    db_clock::now(),
                    locator::tablet_task_info::make_auto_repair_request({}, {"dc1", "dc2"}),
                    locator::tablet_task_info::make_intranode_migration_request()
                });
                tm.set_tablet_map(table1, std::move(tmap));
            }

            verify_tablet_metadata_persistence(e, tm, ts);

            // Add table2
            {
                tablet_map tmap(4);
                auto tb = tmap.first_tablet();
                tmap.set_tablet(tb, tablet_info {
                    tablet_replica_set {
                        tablet_replica {h1, 0},
                    },
                    {},
                    {},
                    locator::tablet_task_info::make_migration_request()
                });
                tb = *tmap.next_tablet(tb);
                tmap.set_tablet(tb, tablet_info {
                    tablet_replica_set {
                        tablet_replica {h3, 3},
                    }
                });
                tb = *tmap.next_tablet(tb);
                tmap.set_tablet(tb, tablet_info {
                    tablet_replica_set {
                        tablet_replica {h2, 2},
                    },
                    {},
                    {},
                    locator::tablet_task_info::make_migration_request()
                });
                tb = *tmap.next_tablet(tb);
                tmap.set_tablet(tb, tablet_info {
                    tablet_replica_set {
                        tablet_replica {h1, 1},
                    }
                });
                tm.set_tablet_map(table2, std::move(tmap));
            }

            verify_tablet_metadata_persistence(e, tm, ts);

            // Increase RF of table2
            tm.mutate_tablet_map(table2, [&] (tablet_map& tmap) {
                auto tb = tmap.first_tablet();
                tb = *tmap.next_tablet(tb);

                tmap.set_tablet_transition_info(tb, tablet_transition_info{
                    tablet_transition_stage::allow_write_both_read_old,
                    tablet_transition_kind::migration,
                    tablet_replica_set {
                        tablet_replica {h3, 3},
                        tablet_replica {h1, 7},
                    },
                    tablet_replica {h1, 7}
                });

                tb = *tmap.next_tablet(tb);
                tmap.set_tablet_transition_info(tb, tablet_transition_info{
                    tablet_transition_stage::use_new,
                    tablet_transition_kind::migration,
                    tablet_replica_set {
                        tablet_replica {h1, 4},
                        tablet_replica {h2, 2},
                    },
                    tablet_replica {h1, 4},
                    session_id(utils::UUID_gen::get_time_UUID())
                });
            });

            verify_tablet_metadata_persistence(e, tm, ts);

            // Reduce tablet count in table2
            {
                tablet_map tmap(2);
                auto tb = tmap.first_tablet();
                tmap.set_tablet(tb, tablet_info {
                    tablet_replica_set {
                        tablet_replica {h1, 0},
                    }
                });
                tb = *tmap.next_tablet(tb);
                tmap.set_tablet(tb, tablet_info {
                    tablet_replica_set {
                        tablet_replica {h3, 3},
                    }
                });
                tm.set_tablet_map(table2, std::move(tmap));
            }

            verify_tablet_metadata_persistence(e, tm, ts);

            // Reduce RF for table1, increasing tablet count
            {
                tablet_map tmap(2);
                auto tb = tmap.first_tablet();
                tmap.set_tablet(tb, tablet_info {
                    tablet_replica_set {
                        tablet_replica {h3, 7},
                    }
                });
                tb = *tmap.next_tablet(tb);
                tmap.set_tablet(tb, tablet_info {
                    tablet_replica_set {
                        tablet_replica {h1, 3},
                    }
                });
                tm.set_tablet_map(table1, std::move(tmap));
            }

            verify_tablet_metadata_persistence(e, tm, ts);

            // Reduce tablet count for table1
            {
                tablet_map tmap(1);
                auto tb = tmap.first_tablet();
                tmap.set_tablet(tb, tablet_info {
                    tablet_replica_set {
                        tablet_replica {h1, 3},
                    }
                });
                tm.set_tablet_map(table1, std::move(tmap));
            }

            verify_tablet_metadata_persistence(e, tm, ts);

            // Change replica of table1
            {
                tablet_map tmap(1);
                auto tb = tmap.first_tablet();
                tmap.set_tablet(tb, tablet_info {
                    tablet_replica_set {
                        tablet_replica {h3, 7},
                    }
                });
                tm.set_tablet_map(table1, std::move(tmap));
            }

            verify_tablet_metadata_persistence(e, tm, ts);

            // Change resize decision of table1
            {
                tablet_map tmap(1);
                locator::resize_decision decision;
                decision.way = locator::resize_decision::split{},
                decision.sequence_number = 1;
                tmap.set_resize_decision(decision);
                tmap.set_resize_task_info(locator::tablet_task_info::make_split_request());
                tm.set_tablet_map(table1, std::move(tmap));
            }

            verify_tablet_metadata_persistence(e, tm, ts);
        }
    }, tablet_cql_test_config());
}

SEASTAR_TEST_CASE(test_read_required_hosts) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        auto h1 = host_id(utils::UUID_gen::get_time_UUID());
        auto h2 = host_id(utils::UUID_gen::get_time_UUID());
        auto h3 = host_id(utils::UUID_gen::get_time_UUID());

        tablet_metadata tm = read_tablet_metadata(e.local_qp()).get();

        auto ts = current_timestamp(e);
        verify_tablet_metadata_persistence(e, tm, ts);
        BOOST_REQUIRE_EQUAL(std::unordered_set<locator::host_id>({}),
                            read_required_hosts(e.local_qp()).get());

        // Add table1
        auto table1 = add_table(e).get();
        {
            tablet_map tmap(1);
            tmap.set_tablet(tmap.first_tablet(), tablet_info {
                tablet_replica_set {
                    tablet_replica {h1, 0},
                    tablet_replica {h2, 3},
                }
            });
            tm.set_tablet_map(table1, std::move(tmap));
        }

        ts = current_timestamp(e);
        verify_tablet_metadata_persistence(e, tm, ts);
        BOOST_REQUIRE_EQUAL(std::unordered_set<locator::host_id>({h1, h2}),
                            read_required_hosts(e.local_qp()).get());

        // Add table2
        auto table2 = add_table(e).get();
        {
            tablet_map tmap(2);
            auto tb = tmap.first_tablet();
            tmap.set_tablet(tb, tablet_info {
                tablet_replica_set {
                    tablet_replica {h1, 0},
                }
            });
            tb = *tmap.next_tablet(tb);
            tmap.set_tablet(tb, tablet_info {
                tablet_replica_set {
                    tablet_replica {h2, 0},
                }
            });
            tmap.set_tablet_transition_info(tb, tablet_transition_info{
                tablet_transition_stage::allow_write_both_read_old,
                tablet_transition_kind::migration,
                tablet_replica_set {
                    tablet_replica {h3, 0},
                },
                tablet_replica {h3, 0}
            });
            tm.set_tablet_map(table2, std::move(tmap));
        }

        ts = current_timestamp(e);
        verify_tablet_metadata_persistence(e, tm, ts);
        BOOST_REQUIRE_EQUAL(std::unordered_set<locator::host_id>({h1, h2, h3}),
                            read_required_hosts(e.local_qp()).get());
    }, tablet_cql_test_config());
}

// Check that updating tablet-metadata and reloading only modified parts from
// disk yields the correct metadata.
SEASTAR_TEST_CASE(test_tablet_metadata_update) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        auto h1 = host_id(utils::UUID_gen::get_time_UUID());
        auto h2 = host_id(utils::UUID_gen::get_time_UUID());
        auto h3 = host_id(utils::UUID_gen::get_time_UUID());

        auto& db = e.local_db();

        auto table1 = add_table(e).get();
        auto table1_schema = db.find_schema(table1);
        auto table2 = add_table(e).get();
        auto table2_schema = db.find_schema(table2);

        testlog.trace("table1: {}", table1);
        testlog.trace("table2: {}", table2);

        tablet_metadata tm = read_tablet_metadata(e.local_qp()).get();
        auto ts = current_timestamp(e);

        // Add table1
        {
            testlog.trace("add table1");

            tablet_map tmap(1);
            tmap.set_tablet(tmap.first_tablet(), tablet_info {
                tablet_replica_set {
                    tablet_replica {h1, 0},
                    tablet_replica {h2, 3},
                    tablet_replica {h3, 1},
                }
            });

            verify_tablet_metadata_update(e, tm, {
                    tablet_map_to_mutation(tmap, table1, table1_schema->ks_name(), table1_schema->cf_name(), ++ts, db.features()).get(),
            });
        }

        // Add table2
        {
            testlog.trace("add table2");

            tablet_map tmap(4);
            auto tb = tmap.first_tablet();
            tmap.set_tablet(tb, tablet_info {
                tablet_replica_set {
                    tablet_replica {h1, 0},
                }
            });
            tb = *tmap.next_tablet(tb);
            tmap.set_tablet(tb, tablet_info {
                tablet_replica_set {
                    tablet_replica {h3, 3},
                }
            });
            tb = *tmap.next_tablet(tb);
            tmap.set_tablet(tb, tablet_info {
                tablet_replica_set {
                    tablet_replica {h2, 2},
                }
            });
            tb = *tmap.next_tablet(tb);
            tmap.set_tablet(tb, tablet_info {
                tablet_replica_set {
                    tablet_replica {h1, 1},
                }
            });

            verify_tablet_metadata_update(e, tm, {
                    tablet_map_to_mutation(tmap, table2, table2_schema->ks_name(), table2_schema->cf_name(), ++ts, db.features()).get(),
            });
        }

        // Increase RF of table2
        {
            testlog.trace("increates RF of table2");

            const auto& tmap = tm.get_tablet_map(table2);
            auto tb = tmap.first_tablet();

            replica::tablet_mutation_builder builder(ts++, table2);

            tb = *tmap.next_tablet(tb);
            builder.set_new_replicas(tmap.get_last_token(tb),
                tablet_replica_set {
                    tablet_replica {h1, 7},
                }
            );
            builder.set_stage(tmap.get_last_token(tb), tablet_transition_stage::allow_write_both_read_old);
            builder.set_transition(tmap.get_last_token(tb), tablet_transition_kind::migration);

            tb = *tmap.next_tablet(tb);
            builder.set_new_replicas(tmap.get_last_token(tb),
                tablet_replica_set {
                    tablet_replica {h1, 4},
                }
            );
            builder.set_stage(tmap.get_last_token(tb), tablet_transition_stage::use_new);
            builder.set_transition(tmap.get_last_token(tb), tablet_transition_kind::migration);

            verify_tablet_metadata_update(e, tm, {
                    builder.build(),
            });
        }

        // Reduce RF for table1, increasing tablet count
        {
            testlog.trace("reduce RF for table1, increasing tablet count");

            tablet_map tmap(2);
            auto tb = tmap.first_tablet();
            tmap.set_tablet(tb, tablet_info {
                tablet_replica_set {
                    tablet_replica {h3, 7},
                }
            });
            tb = *tmap.next_tablet(tb);
            tmap.set_tablet(tb, tablet_info {
                tablet_replica_set {
                    tablet_replica {h1, 3},
                }
            });

            verify_tablet_metadata_update(e, tm, {
                    tablet_map_to_mutation(tmap, table1, table1_schema->ks_name(), table1_schema->cf_name(), ++ts, db.features()).get(),
            });
        }


        // Reduce tablet count for table1
        {
            testlog.trace("reduce tablet count for table1");

            tablet_map tmap(1);
            auto tb = tmap.first_tablet();
            tmap.set_tablet(tb, tablet_info {
                tablet_replica_set {
                    tablet_replica {h1, 3},
                }
            });

            verify_tablet_metadata_update(e, tm, {
                    tablet_map_to_mutation(tmap, table1, table1_schema->ks_name(), table1_schema->cf_name(), ++ts, db.features()).get(),
            });
        }

        // Change replica of table1
        {
            testlog.trace("change replica of table1");

            replica::tablet_mutation_builder builder(ts++, table1);

            const auto& tmap = tm.get_tablet_map(table1);
            auto tb = tmap.first_tablet();
            builder.set_replicas(tmap.get_last_token(tb),
                tablet_replica_set {
                    tablet_replica {h3, 7},
                }
            );

            verify_tablet_metadata_update(e, tm, {
                    builder.build(),
            });
        }

        // Migrate all tablets of table2
        {
            testlog.trace("stream all tablets of table2");

            const auto& tmap = tm.get_tablet_map(table2);

            std::vector<mutation> muts;
            for (std::optional<tablet_id> tb = tmap.first_tablet(); tb; tb = tmap.next_tablet(*tb)) {
                replica::tablet_mutation_builder builder(ts++, table2);

                const auto token = tmap.get_last_token(*tb);

                builder.set_new_replicas(token,
                    tablet_replica_set {
                        tablet_replica {h2, 7},
                    }
                );
                builder.set_stage(token, tablet_transition_stage::streaming);
                builder.set_transition(token, tablet_transition_kind::rebuild);

                muts.emplace_back(builder.build());
            }

            verify_tablet_metadata_update(e, tm, std::move(muts));
        }

        // Remove transitions from tablets of table2
        {
            testlog.trace("stream all tablets of table2");

            const auto& tmap = tm.get_tablet_map(table2);

            std::vector<mutation> muts;
            for (std::optional<tablet_id> tb = tmap.first_tablet(); tb; tb = tmap.next_tablet(*tb)) {
                replica::tablet_mutation_builder builder(ts++, table2);

                const auto token = tmap.get_last_token(*tb);

                builder.set_replicas(token,
                    tablet_replica_set {
                        tablet_replica {h2, 7},
                    }
                );
                builder.del_transition(token);

                muts.emplace_back(builder.build());
            }

            verify_tablet_metadata_update(e, tm, std::move(muts));
        }

        // Drop table2
        {
            testlog.trace("drop table2");

            verify_tablet_metadata_update(e, tm, {
                    make_drop_tablet_map_mutation(table2, ts++)
            });
        }
    }, tablet_cql_test_config());
}

SEASTAR_TEST_CASE(test_tablet_metadata_hint) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        auto h2 = host_id(utils::UUID_gen::get_time_UUID());
        auto h3 = host_id(utils::UUID_gen::get_time_UUID());

        auto table1 = add_table(e).get();
        auto table2 = add_table(e).get();

        testlog.trace("table1: {}", table1);
        testlog.trace("table2: {}", table2);

        tablet_metadata tm = read_tablet_metadata(e.local_qp()).get();
        auto ts = current_timestamp(e);

        auto check_hint = [&] (locator::tablet_metadata_change_hint& incremental_hint, std::vector<canonical_mutation>& muts, mutation new_mut,
                const locator::tablet_metadata_change_hint& expected_hint, std::source_location sl = std::source_location::current()) {
            testlog.info("check_hint() called from {}:{}", sl.file_name(), sl.line());

            replica::update_tablet_metadata_change_hint(incremental_hint, new_mut);

            muts.emplace_back(new_mut);
            auto full_hint_opt = replica::get_tablet_metadata_change_hint(muts);

            if (expected_hint) {
                BOOST_REQUIRE(full_hint_opt);
                BOOST_REQUIRE_EQUAL(*full_hint_opt, incremental_hint);
            } else {
                BOOST_REQUIRE(!full_hint_opt);
            }
            BOOST_REQUIRE_EQUAL(incremental_hint, expected_hint);
        };

        auto make_hint = [&] (std::initializer_list<std::pair<table_id, std::vector<token>>> tablets) {
            locator::tablet_metadata_change_hint hint;
            for (const auto& [tid, tokens] : tablets) {
                hint.tables.emplace(tid, locator::tablet_metadata_change_hint::table_hint{.table_id = tid, .tokens = tokens});
            }
            return hint;
        };

        // Unrelated mutation generates no hint
        {
            std::vector<canonical_mutation> muts;
            locator::tablet_metadata_change_hint hint;

            simple_schema s;
            auto mut = s.new_mutation("pk1");
            s.add_row(mut, s.make_ckey(1), "v");

            check_hint(hint, muts, std::move(mut), {});
        }

        // Incremental update of hint
        {
            std::vector<canonical_mutation> muts;
            locator::tablet_metadata_change_hint hint;

            const auto& tmap = tm.get_tablet_map(table1);
            std::vector<token> tokens;

            for (std::optional<tablet_id> tid = tmap.first_tablet(); tid; tid = tmap.next_tablet(*tid)) {
                const auto token = tmap.get_last_token(*tid);

                tokens.push_back(token);

                replica::tablet_mutation_builder builder(ts++, table1);
                builder.set_replicas(token,
                    tablet_replica_set {
                        tablet_replica {h2, 7},
                    }
                );

                check_hint(hint, muts, builder.build(), make_hint({{table1, tokens}}));
            }
        }
        tm = read_tablet_metadata(e.local_qp()).get();

        // Deletions (and static rows) should generate a partition hint.
        // Furthermore, if the partition had any row hints before, those should
        // be cleared, to force a full partition reload.
        auto check_delete_scenario = [&] (const char* scenario, std::function<void(table_id, mutation&, api::timestamp_type)> apply_delete) {
            testlog.info("check_delete_scenario({})", scenario);

            std::vector<canonical_mutation> muts;
            locator::tablet_metadata_change_hint hint;

            // Check that a deletion generates only a partiton hint
            {
                const auto delete_ts = ts++;
                replica::tablet_mutation_builder builder(delete_ts, table1);
                auto mut = builder.build();
                apply_delete(table1, mut, delete_ts);

                check_hint(hint, muts, std::move(mut), make_hint({{table1, {}}}));
            }

            // First add a row, to check that the deletion will clear the tokens
            // vector -- convert the row hints to a partition hint
            {
                // Add a row which will add a row hint
                {
                    const auto tokens = tm.get_tablet_map(table2).get_sorted_tokens().get();

                    replica::tablet_mutation_builder builder(ts++, table2);
                    builder.set_replicas(tokens.front(),
                        tablet_replica_set {
                            tablet_replica {h3, 7},
                        }
                    );

                    check_hint(hint, muts, builder.build(), make_hint({{table1, {}}, {table2, {tokens.front()}}}));
                }

                // Apply the deletion which should clear the row hint, but leave the partition hint
                {
                    const auto delete_ts = ts++;
                    replica::tablet_mutation_builder builder(delete_ts, table2);
                    auto mut = builder.build();
                    apply_delete(table2, mut, delete_ts);

                    check_hint(hint, muts, std::move(mut), make_hint({{table1, {}}, {table2, {}}}));
                }
            }

            tm = read_tablet_metadata(e.local_qp()).get();
        };

        // Not a real deletion, but it should act the same way as a delete.
        check_delete_scenario("static row", [&e] (table_id tbl, mutation& mut, api::timestamp_type delete_ts) {
            auto tbl_s = e.local_db().find_column_family(tbl).schema();
            mut.set_static_cell("keyspace_name", data_value(tbl_s->ks_name()), delete_ts);
        });

        check_delete_scenario("range tombstone", [&tm] (table_id tbl, mutation& mut, api::timestamp_type delete_ts) {
            auto s = db::system_keyspace::tablets();

            const auto tokens = tm.get_tablet_map(tbl).get_sorted_tokens().get();
            BOOST_REQUIRE_GE(tokens.size(), 2);

            const auto ck1 = clustering_key::from_single_value(*s, data_value(dht::token::to_int64(tokens[0])).serialize_nonnull());
            const auto ck2 = clustering_key::from_single_value(*s, data_value(dht::token::to_int64(tokens[1])).serialize_nonnull());

            mut.partition().apply_delete(*s, range_tombstone(ck1, bound_kind::excl_start, ck2, bound_kind::excl_end, tombstone(delete_ts, gc_clock::now())));
        });

        check_delete_scenario("row tombstone", [&tm] (table_id tbl, mutation& mut, api::timestamp_type delete_ts) {
            auto s = db::system_keyspace::tablets();

            const auto tokens = tm.get_tablet_map(tbl).get_sorted_tokens().get();
            const auto ck = clustering_key::from_single_value(*s, data_value(dht::token::to_int64(tokens[0])).serialize_nonnull());

            mut.partition().apply_delete(*s, ck, tombstone(delete_ts, gc_clock::now()));
        });

        // This will effectively drop both tables
        check_delete_scenario("partition tombstone", [] (table_id tbl, mutation& mut, api::timestamp_type delete_ts) {
            auto s = db::system_keyspace::tablets();

            mut.partition().apply(tombstone(delete_ts, gc_clock::now()));
        });
    }, tablet_cql_test_config());
}

SEASTAR_TEST_CASE(test_get_shard) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        auto h1 = host_id(utils::UUID_gen::get_time_UUID());
        auto h2 = host_id(utils::UUID_gen::get_time_UUID());
        auto h3 = host_id(utils::UUID_gen::get_time_UUID());

        inet_address ip1("192.168.0.1");
        inet_address ip2("192.168.0.2");
        inet_address ip3("192.168.0.3");

        auto table1 = table_id(utils::UUID_gen::get_time_UUID());
        const auto shard_count = 2;

        semaphore sem(1);
        shared_token_metadata stm([&sem] () noexcept { return get_units(sem, 1); }, locator::token_metadata::config{
                locator::topology::config{
                        .this_endpoint = ip1,
                        .this_host_id = h1,
                        .local_dc_rack = locator::endpoint_dc_rack::default_location
                }
        });

        tablet_id tid(0);
        tablet_id tid1(0);

        stm.mutate_token_metadata([&] (token_metadata& tm) {
            tm.update_topology(h1, locator::endpoint_dc_rack::default_location, node::state::normal, shard_count);
            tm.update_topology(h2, locator::endpoint_dc_rack::default_location, node::state::normal, shard_count);
            tm.update_topology(h3, locator::endpoint_dc_rack::default_location, node::state::normal, shard_count);

            tablet_metadata tmeta;
            tablet_map tmap(2);
            tid = tmap.first_tablet();
            tmap.set_tablet(tid, tablet_info {
                tablet_replica_set {
                    tablet_replica {h1, 0},
                    tablet_replica {h3, 5},
                }
            });
            tid1 = *tmap.next_tablet(tid);
            tmap.set_tablet(tid1, tablet_info {
                tablet_replica_set {
                    tablet_replica {h1, 2},
                    tablet_replica {h3, 1},
                }
            });
            tmap.set_tablet_transition_info(tid, tablet_transition_info {
                tablet_transition_stage::allow_write_both_read_old,
                tablet_transition_kind::migration,
                tablet_replica_set {
                    tablet_replica {h1, 0},
                    tablet_replica {h2, 3},
                },
                tablet_replica {h2, 3}
            });
            tmeta.set_tablet_map(table1, std::move(tmap));
            tm.set_tablets(std::move(tmeta));
            return make_ready_future<>();
        }).get();

        auto&& tmap = stm.get()->tablets().get_tablet_map(table1);

        auto get_shard = [&] (tablet_id tid, host_id host) {
            tablet_sharder sharder(*stm.get(), table1, host);
            return sharder.shard_for_reads(tmap.get_last_token(tid));
        };

        BOOST_REQUIRE_EQUAL(get_shard(tid1, h1), std::make_optional(shard_id(2)));
        BOOST_REQUIRE(!get_shard(tid1, h2));
        BOOST_REQUIRE_EQUAL(get_shard(tid1, h3), std::make_optional(shard_id(1)));

        BOOST_REQUIRE_EQUAL(get_shard(tid, h1), std::make_optional(shard_id(0)));
        BOOST_REQUIRE_EQUAL(get_shard(tid, h2), std::make_optional(shard_id(3)));
        BOOST_REQUIRE_EQUAL(get_shard(tid, h3), std::make_optional(shard_id(5)));
    }, tablet_cql_test_config());
}

SEASTAR_TEST_CASE(test_mutation_builder) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        auto h1 = host_id(utils::UUID_gen::get_time_UUID());
        auto h2 = host_id(utils::UUID_gen::get_time_UUID());
        auto h3 = host_id(utils::UUID_gen::get_time_UUID());

        auto table1 = add_table(e).get();
        auto ts = current_timestamp(e);

        tablet_metadata tm;
        tablet_id tid(0);
        tablet_id tid1(0);

        {
            tablet_map tmap(2);
            tid = tmap.first_tablet();
            tmap.set_tablet(tid, tablet_info {
                tablet_replica_set {
                    tablet_replica {h1, 0},
                    tablet_replica {h3, 5},
                }
            });
            tid1 = *tmap.next_tablet(tid);
            tmap.set_tablet(tid1, tablet_info {
                tablet_replica_set {
                    tablet_replica {h1, 2},
                    tablet_replica {h3, 1},
                }
            });
            tm.set_tablet_map(table1, std::move(tmap));
        }

        save_tablet_metadata(e.local_db(), tm, ts++).get();

        {
            tablet_mutation_builder b(ts++, table1);
            auto last_token = tm.get_tablet_map(table1).get_last_token(tid1);
            b.set_new_replicas(last_token, tablet_replica_set {
                    tablet_replica {h1, 2},
                    tablet_replica {h2, 3},
            });
            b.set_stage(last_token, tablet_transition_stage::write_both_read_new);
            b.set_transition(last_token, tablet_transition_kind::migration);
            e.local_db().apply({freeze(b.build())}, db::no_timeout).get();
        }

        {
            tablet_map expected_tmap(2);
            tid = expected_tmap.first_tablet();
            expected_tmap.set_tablet(tid, tablet_info {
                    tablet_replica_set {
                            tablet_replica {h1, 0},
                            tablet_replica {h3, 5},
                    }
            });
            tid1 = *expected_tmap.next_tablet(tid);
            expected_tmap.set_tablet(tid1, tablet_info {
                    tablet_replica_set {
                            tablet_replica {h1, 2},
                            tablet_replica {h3, 1},
                    }
            });
            expected_tmap.set_tablet_transition_info(tid1, tablet_transition_info {
                    tablet_transition_stage::write_both_read_new,
                    tablet_transition_kind::migration,
                    tablet_replica_set {
                            tablet_replica {h1, 2},
                            tablet_replica {h2, 3},
                    },
                    tablet_replica {h2, 3}
            });

            auto tm_from_disk = read_tablet_metadata(e.local_qp()).get();
            BOOST_REQUIRE_EQUAL(expected_tmap, tm_from_disk.get_tablet_map(table1));
        }

        {
            tablet_mutation_builder b(ts++, table1);
            auto last_token = tm.get_tablet_map(table1).get_last_token(tid1);
            b.set_stage(last_token, tablet_transition_stage::use_new);
            b.set_transition(last_token, tablet_transition_kind::migration);
            e.local_db().apply({freeze(b.build())}, db::no_timeout).get();
        }

        {
            tablet_map expected_tmap(2);
            tid = expected_tmap.first_tablet();
            expected_tmap.set_tablet(tid, tablet_info {
                    tablet_replica_set {
                            tablet_replica {h1, 0},
                            tablet_replica {h3, 5},
                    }
            });
            tid1 = *expected_tmap.next_tablet(tid);
            expected_tmap.set_tablet(tid1, tablet_info {
                    tablet_replica_set {
                            tablet_replica {h1, 2},
                            tablet_replica {h3, 1},
                    }
            });
            expected_tmap.set_tablet_transition_info(tid1, tablet_transition_info {
                    tablet_transition_stage::use_new,
                    tablet_transition_kind::migration,
                    tablet_replica_set {
                            tablet_replica {h1, 2},
                            tablet_replica {h2, 3},
                    },
                    tablet_replica {h2, 3}
            });

            auto tm_from_disk = read_tablet_metadata(e.local_qp()).get();
            BOOST_REQUIRE_EQUAL(expected_tmap, tm_from_disk.get_tablet_map(table1));
        }

        {
            tablet_mutation_builder b(ts++, table1);
            auto last_token = tm.get_tablet_map(table1).get_last_token(tid1);
            b.set_replicas(last_token, tablet_replica_set {
                tablet_replica {h1, 2},
                tablet_replica {h2, 3},
            });
            b.del_transition(last_token);
            e.local_db().apply({freeze(b.build())}, db::no_timeout).get();
        }

        {
            tablet_map expected_tmap(2);
            tid = expected_tmap.first_tablet();
            expected_tmap.set_tablet(tid, tablet_info {
                    tablet_replica_set {
                            tablet_replica {h1, 0},
                            tablet_replica {h3, 5},
                    }
            });
            tid1 = *expected_tmap.next_tablet(tid);
            expected_tmap.set_tablet(tid1, tablet_info {
                    tablet_replica_set {
                            tablet_replica {h1, 2},
                            tablet_replica {h2, 3},
                    }
            });

            auto tm_from_disk = read_tablet_metadata(e.local_qp()).get();
            BOOST_REQUIRE_EQUAL(expected_tmap, tm_from_disk.get_tablet_map(table1));
        }

        static const auto resize_decision = locator::resize_decision("split", 1);

        {
            tablet_mutation_builder b(ts++, table1);
            auto last_token = tm.get_tablet_map(table1).get_last_token(tid1);
            b.set_replicas(last_token, tablet_replica_set {
                    tablet_replica {h1, 2},
                    tablet_replica {h2, 3},
            });
            b.del_transition(last_token);
            b.set_resize_decision(resize_decision, e.local_db().features());
            e.local_db().apply({freeze(b.build())}, db::no_timeout).get();
        }

        {
            tablet_map expected_tmap(2);
            tid = expected_tmap.first_tablet();
            expected_tmap.set_tablet(tid, tablet_info {
                    tablet_replica_set {
                            tablet_replica {h1, 0},
                            tablet_replica {h3, 5},
                    }
            });
            tid1 = *expected_tmap.next_tablet(tid);
            expected_tmap.set_tablet(tid1, tablet_info {
                    tablet_replica_set {
                            tablet_replica {h1, 2},
                            tablet_replica {h2, 3},
                    }
            });
            expected_tmap.set_resize_decision(resize_decision);

            auto tm_from_disk = read_tablet_metadata(e.local_qp()).get();
            expected_tmap.set_resize_task_info(tm_from_disk.get_tablet_map(table1).resize_task_info());
            BOOST_REQUIRE_EQUAL(expected_tmap, tm_from_disk.get_tablet_map(table1));
        }
    }, tablet_cql_test_config());
}

SEASTAR_TEST_CASE(test_sharder) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        auto h1 = host_id(utils::UUID_gen::get_time_UUID());
        auto h2 = host_id(utils::UUID_gen::get_time_UUID());
        auto h3 = host_id(utils::UUID_gen::get_time_UUID());

        auto table1 = table_id(utils::UUID_gen::get_time_UUID());

        token_metadata tokm(token_metadata::config{ .topo_cfg{ .this_host_id = h1, .local_dc_rack = locator::endpoint_dc_rack::default_location } });
        tokm.get_topology().add_or_update_endpoint(h1);

        std::vector<tablet_id> tablet_ids;
        {
            tablet_map tmap(8);
            auto tid = tmap.first_tablet();

            tablet_ids.push_back(tid);
            tmap.set_tablet(tid, tablet_info {
                tablet_replica_set {
                    tablet_replica {h1, 3},
                    tablet_replica {h3, 5},
                }
            });

            tid = *tmap.next_tablet(tid);
            tablet_ids.push_back(tid);
            tmap.set_tablet(tid, tablet_info {
                tablet_replica_set {
                    tablet_replica {h2, 3},
                    tablet_replica {h3, 1},
                }
            });

            tid = *tmap.next_tablet(tid);
            tablet_ids.push_back(tid);
            tmap.set_tablet(tid, tablet_info {
                tablet_replica_set {
                    tablet_replica {h3, 2},
                    tablet_replica {h1, 1},
                }
            });
            tmap.set_tablet_transition_info(tid, tablet_transition_info {
                tablet_transition_stage::use_new,
                tablet_transition_kind::migration,
                tablet_replica_set {
                    tablet_replica {h1, 1},
                    tablet_replica {h2, 3},
                },
                tablet_replica {h2, 3}
            });

            tid = *tmap.next_tablet(tid);
            tablet_ids.push_back(tid);
            tmap.set_tablet(tid, tablet_info {
                tablet_replica_set {
                    tablet_replica {h3, 7},
                    tablet_replica {h2, 3},
                }
            });

            // tablet_ids[4]
            // h1 is leaving, h3 is pending
            tid = *tmap.next_tablet(tid);
            tablet_ids.push_back(tid);
            tmap.set_tablet(tid, tablet_info {
                tablet_replica_set {
                    tablet_replica {h1, 5},
                    tablet_replica {h2, 1},
                }
            });
            tmap.set_tablet_transition_info(tid, tablet_transition_info {
                    tablet_transition_stage::allow_write_both_read_old,
                    tablet_transition_kind::migration,
                    tablet_replica_set {
                            tablet_replica {h3, 7},
                            tablet_replica {h2, 1},
                    },
                    tablet_replica {h3, 7}
            });

            // tablet_ids[5]
            // h1 is leaving, h3 is pending
            tid = *tmap.next_tablet(tid);
            tablet_ids.push_back(tid);
            tmap.set_tablet(tid, tablet_info {
                tablet_replica_set {
                    tablet_replica {h1, 5},
                    tablet_replica {h2, 1},
                }
            });
            tmap.set_tablet_transition_info(tid, tablet_transition_info {
                    tablet_transition_stage::write_both_read_old,
                    tablet_transition_kind::migration,
                    tablet_replica_set {
                            tablet_replica {h3, 7},
                            tablet_replica {h2, 1},
                    },
                    tablet_replica {h3, 7}
            });

            // tablet_ids[6]
            // h1 is leaving, h3 is pending
            tid = *tmap.next_tablet(tid);
            tablet_ids.push_back(tid);
            tmap.set_tablet(tid, tablet_info {
                tablet_replica_set {
                    tablet_replica {h1, 5},
                    tablet_replica {h2, 1},
                }
            });
            tmap.set_tablet_transition_info(tid, tablet_transition_info {
                    tablet_transition_stage::write_both_read_new,
                    tablet_transition_kind::migration,
                    tablet_replica_set {
                            tablet_replica {h3, 7},
                            tablet_replica {h2, 1},
                    },
                    tablet_replica {h3, 7}
            });

            // tablet_ids[7]
            // h1 is leaving, h3 is pending
            tid = *tmap.next_tablet(tid);
            tablet_ids.push_back(tid);
            tmap.set_tablet(tid, tablet_info {
                tablet_replica_set {
                    tablet_replica {h1, 5},
                    tablet_replica {h2, 1},
                }
            });
            tmap.set_tablet_transition_info(tid, tablet_transition_info {
                    tablet_transition_stage::use_new,
                    tablet_transition_kind::migration,
                    tablet_replica_set {
                            tablet_replica {h3, 7},
                            tablet_replica {h2, 1},
                    },
                    tablet_replica {h3, 7}
            });

            tablet_metadata tm;
            tm.set_tablet_map(table1, std::move(tmap));
            tokm.set_tablets(std::move(tm));
        }

        auto& tm = tokm.tablets().get_tablet_map(table1);
        tablet_sharder sharder(tokm, table1); // for h1
        tablet_sharder sharder_h3(tokm, table1, h3);

        BOOST_REQUIRE_EQUAL(sharder.shard_for_reads(tm.get_last_token(tablet_ids[0])), 3);
        BOOST_REQUIRE_EQUAL(sharder.shard_for_reads(tm.get_last_token(tablet_ids[1])), 0); // missing
        BOOST_REQUIRE_EQUAL(sharder.shard_for_reads(tm.get_last_token(tablet_ids[2])), 1);
        BOOST_REQUIRE_EQUAL(sharder.shard_for_reads(tm.get_last_token(tablet_ids[3])), 0); // missing

        BOOST_REQUIRE_EQUAL(sharder.shard_for_writes(tm.get_last_token(tablet_ids[0])), dht::shard_replica_set{3});
        BOOST_REQUIRE_EQUAL(sharder.shard_for_writes(tm.get_last_token(tablet_ids[1])), dht::shard_replica_set{});
        BOOST_REQUIRE_EQUAL(sharder.shard_for_writes(tm.get_last_token(tablet_ids[2])), dht::shard_replica_set{1});
        BOOST_REQUIRE_EQUAL(sharder.shard_for_writes(tm.get_last_token(tablet_ids[3])), dht::shard_replica_set{});

        // Shard for read should be stable across stages of migration. The coordinator may route
        // requests to the leaving replica even if the stage on the replica side is use_new.
        BOOST_REQUIRE_EQUAL(sharder.shard_for_reads(tm.get_last_token(tablet_ids[4])), 5);
        BOOST_REQUIRE_EQUAL(sharder.shard_for_reads(tm.get_last_token(tablet_ids[5])), 5);
        BOOST_REQUIRE_EQUAL(sharder.shard_for_reads(tm.get_last_token(tablet_ids[6])), 5);
        BOOST_REQUIRE_EQUAL(sharder.shard_for_reads(tm.get_last_token(tablet_ids[7])), 5);
        BOOST_REQUIRE_EQUAL(sharder.shard_for_writes(tm.get_last_token(tablet_ids[4])), dht::shard_replica_set{5});
        BOOST_REQUIRE_EQUAL(sharder.shard_for_writes(tm.get_last_token(tablet_ids[5])), dht::shard_replica_set{5});
        BOOST_REQUIRE_EQUAL(sharder.shard_for_writes(tm.get_last_token(tablet_ids[6])), dht::shard_replica_set{5});
        BOOST_REQUIRE_EQUAL(sharder.shard_for_writes(tm.get_last_token(tablet_ids[7])), dht::shard_replica_set{5});

        // On pending host
        BOOST_REQUIRE_EQUAL(sharder_h3.shard_for_reads(tm.get_last_token(tablet_ids[4])), 7);
        BOOST_REQUIRE_EQUAL(sharder_h3.shard_for_reads(tm.get_last_token(tablet_ids[5])), 7);
        BOOST_REQUIRE_EQUAL(sharder_h3.shard_for_reads(tm.get_last_token(tablet_ids[6])), 7);
        BOOST_REQUIRE_EQUAL(sharder_h3.shard_for_reads(tm.get_last_token(tablet_ids[7])), 7);
        BOOST_REQUIRE_EQUAL(sharder_h3.shard_for_writes(tm.get_last_token(tablet_ids[4])), dht::shard_replica_set{7});
        BOOST_REQUIRE_EQUAL(sharder_h3.shard_for_writes(tm.get_last_token(tablet_ids[5])), dht::shard_replica_set{7});
        BOOST_REQUIRE_EQUAL(sharder_h3.shard_for_writes(tm.get_last_token(tablet_ids[6])), dht::shard_replica_set{7});
        BOOST_REQUIRE_EQUAL(sharder_h3.shard_for_writes(tm.get_last_token(tablet_ids[7])), dht::shard_replica_set{7});

        BOOST_REQUIRE_EQUAL(sharder.token_for_next_shard_for_reads(tm.get_last_token(tablet_ids[1]), 0), tm.get_first_token(tablet_ids[3]));
        BOOST_REQUIRE_EQUAL(sharder.token_for_next_shard_for_reads(tm.get_last_token(tablet_ids[1]), 1), tm.get_first_token(tablet_ids[2]));
        BOOST_REQUIRE_EQUAL(sharder.token_for_next_shard_for_reads(tm.get_last_token(tablet_ids[1]), 3), dht::maximum_token());

        BOOST_REQUIRE_EQUAL(sharder.token_for_next_shard_for_reads(tm.get_first_token(tablet_ids[1]), 0), tm.get_first_token(tablet_ids[3]));
        BOOST_REQUIRE_EQUAL(sharder.token_for_next_shard_for_reads(tm.get_first_token(tablet_ids[1]), 1), tm.get_first_token(tablet_ids[2]));
        BOOST_REQUIRE_EQUAL(sharder.token_for_next_shard_for_reads(tm.get_first_token(tablet_ids[1]), 3), dht::maximum_token());

        {
            auto shard_opt = sharder.next_shard_for_reads(tm.get_last_token(tablet_ids[0]));
            BOOST_REQUIRE(shard_opt);
            BOOST_REQUIRE_EQUAL(shard_opt->shard, 0);
            BOOST_REQUIRE_EQUAL(shard_opt->token, tm.get_first_token(tablet_ids[1]));
        }

        {
            auto shard_opt = sharder.next_shard_for_reads(tm.get_last_token(tablet_ids[1]));
            BOOST_REQUIRE(shard_opt);
            BOOST_REQUIRE_EQUAL(shard_opt->shard, 1);
            BOOST_REQUIRE_EQUAL(shard_opt->token, tm.get_first_token(tablet_ids[2]));
        }

        {
            auto shard_opt = sharder.next_shard_for_reads(tm.get_last_token(tablet_ids[2]));
            BOOST_REQUIRE(shard_opt);
            BOOST_REQUIRE_EQUAL(shard_opt->shard, 0);
            BOOST_REQUIRE_EQUAL(shard_opt->token, tm.get_first_token(tablet_ids[3]));
        }

        {
            auto shard_opt = sharder.next_shard_for_reads(tm.get_last_token(tablet_ids[tablet_ids.size() - 1]));
            BOOST_REQUIRE(!shard_opt);
        }
    }, tablet_cql_test_config());
}

SEASTAR_TEST_CASE(test_intranode_sharding) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        auto h1 = host_id(utils::UUID_gen::get_time_UUID());
        auto h2 = host_id(utils::UUID_gen::get_time_UUID());

        auto table1 = table_id(utils::UUID_gen::get_time_UUID());

        token_metadata tokm(token_metadata::config{ .topo_cfg{ .this_host_id = h1, .local_dc_rack = locator::endpoint_dc_rack::default_location } });
        tokm.get_topology().add_or_update_endpoint(h1);

        auto leaving_replica = tablet_replica{h1, 5};
        auto pending_replica = tablet_replica{h1, 7};
        auto const_replica = tablet_replica{h2, 1};

        // Prepare a tablet map with different tablets being in intra-node migration at different stages.
        std::vector<tablet_id> tablet_ids;
        {
            tablet_map tmap(4);
            auto tid = tmap.first_tablet();

            auto set_tablet = [&] (tablet_id tid, tablet_transition_stage stage) {
                tablet_ids.push_back(tid);
                tmap.set_tablet(tid, tablet_info{
                    tablet_replica_set{leaving_replica, const_replica}
                });
                tmap.set_tablet_transition_info(tid, tablet_transition_info {
                    stage,
                    tablet_transition_kind::intranode_migration,
                    tablet_replica_set{pending_replica, const_replica},
                    pending_replica
                });
            };

            // tablet_ids[0]
            set_tablet(tid, tablet_transition_stage::allow_write_both_read_old);

            // tablet_ids[1]
            tid = *tmap.next_tablet(tid);
            set_tablet(tid, tablet_transition_stage::write_both_read_old);

            // tablet_ids[2]
            tid = *tmap.next_tablet(tid);
            set_tablet(tid, tablet_transition_stage::write_both_read_new);

            // tablet_ids[3]
            tid = *tmap.next_tablet(tid);
            set_tablet(tid, tablet_transition_stage::use_new);

            tablet_metadata tm;
            tm.set_tablet_map(table1, std::move(tmap));
            tokm.set_tablets(std::move(tm));
        }

        auto& tm = tokm.tablets().get_tablet_map(table1);
        tablet_sharder sharder(tokm, table1); // for h1

        BOOST_REQUIRE_EQUAL(sharder.shard_for_reads(tm.get_last_token(tablet_ids[0])), 5);
        BOOST_REQUIRE_EQUAL(sharder.shard_for_reads(tm.get_last_token(tablet_ids[1])), 5);
        BOOST_REQUIRE_EQUAL(sharder.shard_for_reads(tm.get_last_token(tablet_ids[2])), 7);
        BOOST_REQUIRE_EQUAL(sharder.shard_for_reads(tm.get_last_token(tablet_ids[3])), 7);

        BOOST_REQUIRE_EQUAL(sharder.shard_for_writes(tm.get_last_token(tablet_ids[0])), dht::shard_replica_set{5});
        BOOST_REQUIRE_EQUAL(sharder.shard_for_writes(tm.get_last_token(tablet_ids[1])), dht::shard_replica_set({7, 5}));
        BOOST_REQUIRE_EQUAL(sharder.shard_for_writes(tm.get_last_token(tablet_ids[2])), dht::shard_replica_set({7, 5}));
        BOOST_REQUIRE_EQUAL(sharder.shard_for_writes(tm.get_last_token(tablet_ids[3])), dht::shard_replica_set{7});

        // On const replica
        tablet_sharder sharder_h2(tokm, table1, const_replica.host);
        for (auto id : tablet_ids) {
            BOOST_REQUIRE_EQUAL(sharder_h2.shard_for_reads(tm.get_last_token(id)), const_replica.shard);
            BOOST_REQUIRE_EQUAL(sharder_h2.shard_for_writes(tm.get_last_token(id)), dht::shard_replica_set{const_replica.shard});
        }
    }, tablet_cql_test_config());
}

SEASTAR_TEST_CASE(test_large_tablet_metadata) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        tablet_metadata tm;

        auto h1 = host_id(utils::UUID_gen::get_time_UUID());
        auto h2 = host_id(utils::UUID_gen::get_time_UUID());
        auto h3 = host_id(utils::UUID_gen::get_time_UUID());

        const int nr_tables = 1'00;
        const int tablets_per_table = 1024;

        for (int i = 0; i < nr_tables; ++i) {
            tablet_map tmap(tablets_per_table);

            for (tablet_id j : tmap.tablet_ids()) {
                tmap.set_tablet(j, tablet_info {
                    tablet_replica_set {{h1, 0}, {h2, 1}, {h3, 2},}
                });
            }

            auto id = add_table(e).get();
            tm.set_tablet_map(id, std::move(tmap));
        }

        auto ts = current_timestamp(e);
        verify_tablet_metadata_persistence(e, tm, ts);
    }, tablet_cql_test_config());
}

SEASTAR_THREAD_TEST_CASE(test_token_ownership_splitting) {
    const auto real_min_token = dht::token::first();
    const auto real_max_token = dht::token::last();

    for (auto&& tmap : {
        tablet_map(1),
        tablet_map(2),
        tablet_map(4),
        tablet_map(16),
        tablet_map(1024),
    }) {
        testlog.debug("tmap: {}", tmap);

        BOOST_REQUIRE_EQUAL(real_min_token, tmap.get_first_token(tmap.first_tablet()));
        BOOST_REQUIRE_EQUAL(real_max_token, tmap.get_last_token(tmap.last_tablet()));

        std::optional<tablet_id> prev_tb;
        for (tablet_id tb : tmap.tablet_ids()) {
            testlog.debug("first: {}, last: {}", tmap.get_first_token(tb), tmap.get_last_token(tb));
            BOOST_REQUIRE_EQUAL(tb, tmap.get_tablet_id(tmap.get_first_token(tb)));
            BOOST_REQUIRE_EQUAL(tb, tmap.get_tablet_id(tmap.get_last_token(tb)));
            if (prev_tb) {
                BOOST_REQUIRE_EQUAL(dht::next_token(tmap.get_last_token(*prev_tb)), tmap.get_first_token(tb));
            }
            prev_tb = tb;
        }
    }
}

static
void apply_resize_plan(token_metadata& tm, const migration_plan& plan) {
    for (auto [table_id, resize_decision] : plan.resize_plan().resize) {
        tm.tablets().mutate_tablet_map(table_id, [&] (tablet_map& tmap) {
            resize_decision.sequence_number = tmap.resize_decision().sequence_number + 1;
            tmap.set_resize_decision(resize_decision);
        });
    }
}

static
future<> handle_resize_finalize(cql_test_env& e, group0_guard& guard, const migration_plan& plan) {
    auto& talloc = e.get_tablet_allocator().local();
    auto& stm = e.shared_token_metadata().local();
    bool changed = false;

    for (auto table_id : plan.resize_plan().finalize_resize) {
        auto tm = stm.get();
        const auto& old_tmap = tm->tablets().get_tablet_map(table_id);

        auto new_tmap = co_await talloc.resize_tablets(tm, table_id);
        auto new_resize_decision = locator::resize_decision{};
        new_resize_decision.sequence_number = old_tmap.resize_decision().next_sequence_number();
        new_tmap.set_resize_decision(std::move(new_resize_decision));

        co_await stm.mutate_token_metadata([table_id, &new_tmap, &changed] (token_metadata& tm) {
            changed = true;
            tm.tablets().set_tablet_map(table_id, std::move(new_tmap));
            return make_ready_future<>();
        });
    }

    if (changed) {
        // Need to reload on each resize because table object expects tablet count to change by a factor of 2.
        co_await save_tablet_metadata(e.local_db(), stm.get()->tablets(), guard.write_timestamp());
        co_await e.get_storage_service().local().load_tablet_metadata({});

        // Need a new guard to make sure later changes use later timestamp.
        release_guard(std::move(guard));
        abort_source as;
        guard = co_await e.get_raft_group0_client().start_operation(as);
    }
}

// Reflects the plan in a given token metadata as if the migrations were fully executed.
static
void apply_plan(token_metadata& tm, const migration_plan& plan) {
    for (auto&& mig : plan.migrations()) {
        tm.tablets().mutate_tablet_map(mig.tablet.table, [&] (tablet_map& tmap) {
            auto tinfo = tmap.get_tablet_info(mig.tablet.tablet);
            testlog.trace("Replacing tablet {} replica from {} to {}", mig.tablet.tablet, mig.src, mig.dst);
            tinfo.replicas = replace_replica(tinfo.replicas, mig.src, mig.dst);
            tmap.set_tablet(mig.tablet.tablet, tinfo);
        });
    }
    apply_resize_plan(tm, plan);
}

// Reflects the plan in a given token metadata as if the migrations were started but not yet executed.
static
void apply_plan_as_in_progress(token_metadata& tm, const migration_plan& plan) {
    for (auto&& mig : plan.migrations()) {
        tm.tablets().mutate_tablet_map(mig.tablet.table, [&] (tablet_map& tmap) {
            auto tinfo = tmap.get_tablet_info(mig.tablet.tablet);
            tmap.set_tablet_transition_info(mig.tablet.tablet, migration_to_transition_info(tinfo, mig));
        });
    }
    apply_resize_plan(tm, plan);
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
void check_tablet_invariants(const tablet_metadata& tmeta);

static
void do_rebalance_tablets(cql_test_env& e,
                          group0_guard& guard,
                          shared_load_stats* load_stats = nullptr,
                          std::unordered_set<host_id> skiplist = {},
                          std::function<bool(const migration_plan&)> stop = nullptr,
                          bool auto_split = false)
{
    auto& talloc = e.get_tablet_allocator().local();
    auto& stm = e.shared_token_metadata().local();

    // Sanity limit to avoid infinite loops.
    // The x10 factor is arbitrary, it's there to account for more complex schedules than direct migration.
    auto max_iterations = 1 + get_tablet_count(stm.get()->tablets()) * 10;

    for (size_t i = 0; i < max_iterations; ++i) {
        auto plan = talloc.balance_tablets(stm.get(), load_stats ? load_stats->get() : nullptr, skiplist).get();
        if (plan.empty()) {
            return;
        }
        if (stop && stop(plan)) {
            return;
        }
        stm.mutate_token_metadata([&] (token_metadata& tm) {
            apply_plan(tm, plan);
            return make_ready_future<>();
        }).get();

        if (auto_split && load_stats) {
            auto& tm = *stm.get();
            for (auto& [table, tmap]: tm.tablets().all_tables()) {
                if (std::holds_alternative<resize_decision::split>(tmap->resize_decision().way)) {
                    testlog.debug("set_split_ready_seq_number({}, {})", table, tmap->resize_decision().sequence_number);
                    load_stats->set_split_ready_seq_number(table, tmap->resize_decision().sequence_number);
                }
            }
        }

        handle_resize_finalize(e, guard, plan).get();
    }
    throw std::runtime_error("rebalance_tablets(): convergence not reached within limit");
}

// Invokes the tablet scheduler and executes its plan, continuously until it emits an empty plan.
// Simulates topology coordinator but doesn't perform actual migration,
// only reflects it in the metadata.
// Run in a seastar thread.
void rebalance_tablets(cql_test_env& e,
                       shared_load_stats* load_stats = nullptr,
                       std::unordered_set<host_id> skiplist = {},
                       std::function<bool(const migration_plan&)> stop = nullptr,
                       bool auto_split = true) {
    abort_source as;
    testlog.debug("rebalance_tablets(): start");

    auto guard = e.get_raft_group0_client().start_operation(as).get();
    testlog.debug("rebalance_tablets(): took group0 guard");

    do_rebalance_tablets(e, guard, load_stats, std::move(skiplist), std::move(stop), auto_split);
    testlog.debug("rebalance_tablets(): rebalanced");

    // We should not introduce inconsistency between on-disk state and in-memory state
    // as that may violate invariants and cause failures in later operations
    // causing test flakiness.
    auto& stm = e.shared_token_metadata().local();
    save_tablet_metadata(e.local_db(), stm.get()->tablets(), guard.write_timestamp()).get();
    e.get_storage_service().local().load_tablet_metadata({}).get();

    testlog.debug("rebalance_tablets(): done");
}

static
void rebalance_tablets_as_in_progress(tablet_allocator& talloc, shared_token_metadata& stm) {
    while (true) {
        auto plan = talloc.balance_tablets(stm.get()).get();
        if (plan.empty()) {
            break;
        }
        stm.mutate_token_metadata([&] (token_metadata& tm) {
            apply_plan_as_in_progress(tm, plan);
            return make_ready_future<>();
        }).get();
    }
}

// Completes any in progress tablet migrations.
static
void execute_transitions(shared_token_metadata& stm) {
    stm.mutate_token_metadata([&] (token_metadata& tm) {
        for (auto&& [tablet, tmap_] : tm.tablets().all_tables()) {
            tm.tablets().mutate_tablet_map(tablet, [&] (tablet_map& tmap) {
                for (auto&& [tablet, trinfo]: tmap.transitions()) {
                    auto ti = tmap.get_tablet_info(tablet);
                    ti.replicas = trinfo.next;
                    tmap.set_tablet(tablet, ti);
                }
                tmap.clear_transitions();
            });
        }
        return make_ready_future<>();
    }).get();
}

SEASTAR_THREAD_TEST_CASE(test_load_balancing_with_empty_node) {
  do_with_cql_env_thread([] (auto& e) {
    // Tests the scenario of bootstrapping a single node
    // Verifies that load balancer sees it and moves tablets to that node.
    topology_builder topo(e);

    unsigned shard_count = 2;
    auto host1 = topo.add_node(node_state::normal, shard_count);
    auto host2 = topo.add_node(node_state::normal, shard_count);
    auto host3 = topo.add_node(node_state::normal, shard_count);

    auto ks_name = add_keyspace(e, {{topo.dc(), 1}}, 4);
    auto table1 = add_table(e, ks_name).get();

    mutate_tablets(e, [&] (tablet_metadata& tmeta) -> future<> {
        tablet_map tmap(4);
        auto tid = tmap.first_tablet();
        tmap.set_tablet(tid, tablet_info {
                tablet_replica_set {
                        tablet_replica {host1, 0},
                        tablet_replica {host2, 1},
                }
        });
        tid = *tmap.next_tablet(tid);
        tmap.set_tablet(tid, tablet_info {
                tablet_replica_set {
                        tablet_replica {host1, 0},
                        tablet_replica {host2, 1},
                }
        });
        tid = *tmap.next_tablet(tid);
        tmap.set_tablet(tid, tablet_info {
                tablet_replica_set {
                        tablet_replica {host1, 0},
                        tablet_replica {host2, 0},
                }
        });
        tid = *tmap.next_tablet(tid);
        tmap.set_tablet(tid, tablet_info {
                tablet_replica_set {
                        tablet_replica {host1, 1},
                        tablet_replica {host2, 0},
                }
        });
        tmeta.set_tablet_map(table1, std::move(tmap));
        co_return;
    });

    auto& stm = e.shared_token_metadata().local();

    // Sanity check
    {
        load_sketch load(stm.get());
        load.populate().get();
        BOOST_REQUIRE_EQUAL(load.get_load(host1), 4);
        BOOST_REQUIRE_EQUAL(load.get_avg_shard_load(host1), 2);
        BOOST_REQUIRE_EQUAL(load.get_load(host2), 4);
        BOOST_REQUIRE_EQUAL(load.get_avg_shard_load(host2), 2);
        BOOST_REQUIRE_EQUAL(load.get_load(host3), 0);
        BOOST_REQUIRE_EQUAL(load.get_avg_shard_load(host3), 0);
    }

    rebalance_tablets(e);

    {
        load_sketch load(stm.get());
        load.populate().get();

        for (auto h : {host1, host2, host3}) {
            testlog.debug("Checking host {}", h);
            BOOST_REQUIRE_LE(load.get_load(h), 3);
            BOOST_REQUIRE_GT(load.get_load(h), 1);
            BOOST_REQUIRE_LE(load.get_avg_shard_load(h), 2);
            BOOST_REQUIRE_GT(load.get_avg_shard_load(h), 0);
        }
    }
  }).get();
}

SEASTAR_THREAD_TEST_CASE(test_load_balancing_with_skiplist) {
  do_with_cql_env_thread([] (auto& e) {
    // Tests the scenario of balacning cluster with DOWN node
    // Verifies that load balancer doesn't moves tablets to that node.

    unsigned shard_count = 2;

    topology_builder topo(e);
    auto host1 = topo.add_node(node_state::normal, shard_count);
    auto host2 = topo.add_node(node_state::normal, shard_count);
    auto host3 = topo.add_node(node_state::normal, shard_count);

    auto ks_name = add_keyspace(e, {{topo.dc(), 1}}, 4);
    auto table1 = add_table(e, ks_name).get();

    mutate_tablets(e, [&] (tablet_metadata& tmeta) {
        tablet_map tmap(4);
        auto tid = tmap.first_tablet();
        tmap.set_tablet(tid, tablet_info {
                tablet_replica_set {
                        tablet_replica {host1, 0},
                        tablet_replica {host2, 1},
                }
        });
        tid = *tmap.next_tablet(tid);
        tmap.set_tablet(tid, tablet_info {
                tablet_replica_set {
                        tablet_replica {host1, 0},
                        tablet_replica {host2, 1},
                }
        });
        tid = *tmap.next_tablet(tid);
        tmap.set_tablet(tid, tablet_info {
                tablet_replica_set {
                        tablet_replica {host1, 0},
                        tablet_replica {host2, 0},
                }
        });
        tid = *tmap.next_tablet(tid);
        tmap.set_tablet(tid, tablet_info {
                tablet_replica_set {
                        tablet_replica {host1, 1},
                        tablet_replica {host2, 0},
                }
        });
        tmeta.set_tablet_map(table1, std::move(tmap));
        return make_ready_future<>();
    });

    auto& stm = e.shared_token_metadata().local();

    // Sanity check
    {
        load_sketch load(stm.get());
        load.populate().get();
        BOOST_REQUIRE_EQUAL(load.get_load(host1), 4);
        BOOST_REQUIRE_EQUAL(load.get_avg_shard_load(host1), 2);
        BOOST_REQUIRE_EQUAL(load.get_load(host2), 4);
        BOOST_REQUIRE_EQUAL(load.get_avg_shard_load(host2), 2);
        BOOST_REQUIRE_EQUAL(load.get_load(host3), 0);
        BOOST_REQUIRE_EQUAL(load.get_avg_shard_load(host3), 0);
    }

    rebalance_tablets(e, {}, {host3});

    {
        load_sketch load(stm.get());
        load.populate().get();
        BOOST_REQUIRE_EQUAL(load.get_load(host3), 0);
        BOOST_REQUIRE_EQUAL(load.get_avg_shard_load(host3), 0);
    }
  }).get();
}

SEASTAR_THREAD_TEST_CASE(test_decommission_rf_met) {
    // Verifies that load balancer moves tablets out of the decommissioned node.
    // The scenario is such that replication factor of tablets can be satisfied after decommission.
    do_with_cql_env_thread([](auto& e) {
        unsigned shard_count = 2;

        topology_builder topo(e);
        auto host1 = topo.add_node(node_state::normal, shard_count);
        auto host2 = topo.add_node(node_state::normal, shard_count);
        auto host3 = topo.add_node(node_state::decommissioning, shard_count);

        auto ks_name = add_keyspace(e, {{topo.dc(), 1}}, 4);
        auto table1 = add_table(e, ks_name).get();

        mutate_tablets(e, [&] (tablet_metadata& tmeta) -> future<> {
            tablet_map tmap(4);
            auto tid = tmap.first_tablet();
            tmap.set_tablet(tid, tablet_info {
                    tablet_replica_set {
                            tablet_replica {host1, 0},
                            tablet_replica {host2, 1},
                    }
            });
            tid = *tmap.next_tablet(tid);
            tmap.set_tablet(tid, tablet_info {
                    tablet_replica_set {
                            tablet_replica {host1, 0},
                            tablet_replica {host2, 1},
                    }
            });
            tid = *tmap.next_tablet(tid);
            tmap.set_tablet(tid, tablet_info {
                    tablet_replica_set {
                            tablet_replica {host1, 0},
                            tablet_replica {host3, 0},
                    }
            });
            tid = *tmap.next_tablet(tid);
            tmap.set_tablet(tid, tablet_info {
                    tablet_replica_set {
                            tablet_replica {host2, 1},
                            tablet_replica {host3, 1},
                    }
            });
            tmeta.set_tablet_map(table1, std::move(tmap));
            co_return;
        });

        rebalance_tablets(e);

        auto& stm = e.shared_token_metadata().local();

        {
            load_sketch load(stm.get());
            load.populate().get();
            BOOST_REQUIRE_EQUAL(load.get_avg_shard_load(host1), 2);
            BOOST_REQUIRE_EQUAL(load.get_avg_shard_load(host2), 2);
            BOOST_REQUIRE_EQUAL(load.get_avg_shard_load(host3), 0);
        }

        topo.set_node_state(host3, node_state::left);

        rebalance_tablets(e);

        {
            load_sketch load(stm.get());
            load.populate().get();
            BOOST_REQUIRE_EQUAL(load.get_avg_shard_load(host1), 2);
            BOOST_REQUIRE_EQUAL(load.get_avg_shard_load(host2), 2);
            BOOST_REQUIRE_EQUAL(load.get_avg_shard_load(host3), 0);
        }
    }).get();
}

SEASTAR_THREAD_TEST_CASE(test_table_creation_during_decommission) {
    // Verifies that new table doesn't get tablets allocated on a node being decommissioned
    // which may leave them on replicas absent in topology post decommission.
    // Also verifies that the allocated tablet count doesn't take into account nodes being decommissioned
    // to achieve the desired tablet count per shard in a DC.
    auto cfg = tablet_cql_test_config();
    cfg.db_config->tablets_initial_scale_factor(1);
    do_with_cql_env_thread([](auto& e) {
        topology_builder topo(e);
        topo.add_node(node_state::normal);
        topo.add_node(node_state::normal);
        auto host3 = topo.add_node(node_state::decommissioning);
        auto host4 = topo.add_node(node_state::left);

        auto ks_name = add_keyspace(e, {{topo.dc(), 1}});
        auto table1 = add_table(e, ks_name).get();
        auto s = e.local_db().find_schema(table1);

        auto& stm = e.shared_token_metadata().local();
        auto& tmap = stm.get()->tablets().get_tablet_map(table1);

        // Verify we do not treat leaving nodes as having capacity.
        BOOST_REQUIRE_EQUAL(tmap.tablet_count(), 2);

        tmap.for_each_tablet([&](auto tid, auto& tinfo) {
            for (auto& replica : tinfo.replicas) {
                BOOST_REQUIRE_NE(replica.host, host3);
                BOOST_REQUIRE_NE(replica.host, host4);
            }
            return make_ready_future<>();
        }).get();
    }, cfg).get();
}

SEASTAR_THREAD_TEST_CASE(test_table_creation_during_rack_decommission) {
    // Reproduces #22625
    // The problematic scenario happens when allocating tablets for a new table
    // when there is a rack with only non-normal nodes.
    do_with_cql_env_thread([](auto& e) {
        topology_builder topo(e);
        topo.add_node();
        topo.add_node();
        topo.start_new_rack();
        auto host3 = topo.add_node(node_state::decommissioning);
        auto host4 = topo.add_node(node_state::left);

        auto ks_name = add_keyspace(e, {{topo.dc(), 1}}, 8);
        auto table1 = add_table(e, ks_name).get();

        rebalance_tablets(e);

        auto& stm = e.shared_token_metadata().local();
        auto& tmap = stm.get()->tablets().get_tablet_map(table1);

        tmap.for_each_tablet([&](auto tid, auto& tinfo) {
            for (auto& replica : tinfo.replicas) {
                BOOST_REQUIRE_NE(replica.host, host3);
                BOOST_REQUIRE_NE(replica.host, host4);
            }
            return make_ready_future<>();
        }).get();
    }, tablet_cql_test_config()).get();
}

SEASTAR_THREAD_TEST_CASE(test_decommission_two_racks) {
    // Verifies that load balancer moves tablets out of the decommissioned node.
    // The scenario is such that replication constraints of tablets can be satisfied after decommission.
    do_with_cql_env_thread([](auto& e) {
        std::vector<endpoint_dc_rack> racks;

        topology_builder topo(e);
        racks.push_back(topo.rack());
        auto host1 = topo.add_node(node_state::normal);
        auto host3 = topo.add_node(node_state::normal);
        topo.start_new_rack();
        racks.push_back(topo.rack());
        auto host2 = topo.add_node(node_state::normal);
        auto host4 = topo.add_node(node_state::decommissioning);

        auto ks_name = add_keyspace(e, {{topo.dc(), 1}}, 4);
        auto table1 = add_table(e, ks_name).get();

        mutate_tablets(e, [&] (tablet_metadata& tmeta) -> future<> {
            tablet_map tmap(4);
            auto tid = tmap.first_tablet();
            tmap.set_tablet(tid, tablet_info {
                    tablet_replica_set {
                            tablet_replica {host1, 0},
                            tablet_replica {host2, 0},
                    }
            });
            tid = *tmap.next_tablet(tid);
            tmap.set_tablet(tid, tablet_info {
                    tablet_replica_set {
                            tablet_replica {host2, 0},
                            tablet_replica {host3, 0},
                    }
            });
            tid = *tmap.next_tablet(tid);
            tmap.set_tablet(tid, tablet_info {
                    tablet_replica_set {
                            tablet_replica {host3, 0},
                            tablet_replica {host4, 0},
                    }
            });
            tid = *tmap.next_tablet(tid);
            tmap.set_tablet(tid, tablet_info {
                    tablet_replica_set {
                            tablet_replica {host1, 0},
                            tablet_replica {host2, 0},
                    }
            });
            tmeta.set_tablet_map(table1, std::move(tmap));
            co_return;
        });

        rebalance_tablets(e);

        auto& stm = e.shared_token_metadata().local();

        {
            load_sketch load(stm.get());
            load.populate().get();
            BOOST_REQUIRE_GE(load.get_avg_shard_load(host1), 2);
            BOOST_REQUIRE_GE(load.get_avg_shard_load(host2), 2);
            BOOST_REQUIRE_GE(load.get_avg_shard_load(host3), 2);
            BOOST_REQUIRE_EQUAL(load.get_avg_shard_load(host4), 0);
        }

        // Verify replicas are not collocated on racks
        {
            auto tm = stm.get();
            auto& tmap = tm->tablets().get_tablet_map(table1);
            tmap.for_each_tablet([&](auto tid, auto& tinfo) -> future<> {
                auto rack1 = tm->get_topology().get_rack(tinfo.replicas[0].host);
                auto rack2 = tm->get_topology().get_rack(tinfo.replicas[1].host);
                BOOST_REQUIRE_NE(rack1, rack2);
                return make_ready_future<>();
            }).get();
        }
    }).get();
}

SEASTAR_THREAD_TEST_CASE(test_decommission_rack_load_failure) {
    // Verifies that load balancer moves tablets out of the decommissioned node.
    // The scenario is such that it is impossible to distribute replicas without violating rack uniqueness.
    do_with_cql_env_thread([](auto& e) {
        std::vector<endpoint_dc_rack> racks;

        topology_builder topo(e);
        racks.push_back(topo.rack());
        auto host1 = topo.add_node(node_state::normal);
        auto host2 = topo.add_node(node_state::normal);
        auto host3 = topo.add_node(node_state::normal);
        topo.start_new_rack();
        racks.push_back(topo.rack());
        auto host4 = topo.add_node(node_state::decommissioning);

        auto ks_name = add_keyspace(e, {{topo.dc(), 1}}, 4);
        auto table1 = add_table(e, ks_name).get();

        mutate_tablets(e, [&] (tablet_metadata& tmeta) -> future<> {
            tablet_map tmap(4);
            auto tid = tmap.first_tablet();
            tmap.set_tablet(tid, tablet_info {
                    tablet_replica_set {
                            tablet_replica {host1, 0},
                            tablet_replica {host4, 0},
                    }
            });
            tid = *tmap.next_tablet(tid);
            tmap.set_tablet(tid, tablet_info {
                    tablet_replica_set {
                            tablet_replica {host2, 0},
                            tablet_replica {host4, 0},
                    }
            });
            tid = *tmap.next_tablet(tid);
            tmap.set_tablet(tid, tablet_info {
                    tablet_replica_set {
                            tablet_replica {host3, 0},
                            tablet_replica {host4, 0},
                    }
            });
            tid = *tmap.next_tablet(tid);
            tmap.set_tablet(tid, tablet_info {
                    tablet_replica_set {
                            tablet_replica {host1, 0},
                            tablet_replica {host4, 0},
                    }
            });
            tmeta.set_tablet_map(table1, std::move(tmap));
            co_return;
        });

        BOOST_REQUIRE_THROW(rebalance_tablets(e), std::runtime_error);
    }).get();
}

SEASTAR_THREAD_TEST_CASE(test_decommission_rf_not_met) {
    // Verifies that load balancer moves tablets out of the decommissioned node.
    // The scenario is such that replication factor of tablets can be satisfied after decommission.
    do_with_cql_env_thread([](auto& e) {
        topology_builder topo(e);
        auto host1 = topo.add_node(node_state::normal, 2);
        auto host2 = topo.add_node(node_state::normal, 2);
        auto host3 = topo.add_node(node_state::decommissioning, 2);

        auto ks_name = add_keyspace(e, {{topo.dc(), 1}}, 1);
        auto table1 = add_table(e, ks_name).get();

        mutate_tablets(e, [&] (tablet_metadata& tmeta) -> future<> {
            tablet_map tmap(1);
            auto tid = tmap.first_tablet();
            tmap.set_tablet(tid, tablet_info {
                    tablet_replica_set {
                            tablet_replica {host1, 0},
                            tablet_replica {host2, 0},
                            tablet_replica {host3, 0},
                    }
            });
            tmeta.set_tablet_map(table1, std::move(tmap));
            co_return;
        });

        BOOST_REQUIRE_THROW(rebalance_tablets(e), std::runtime_error);
    }).get();
}

SEASTAR_THREAD_TEST_CASE(test_load_balancing_works_with_in_progress_transitions) {
  do_with_cql_env_thread([] (auto& e) {
    // Tests the scenario of bootstrapping a single node.
    // Verifies that the load balancer balances tablets on that node
    // even though there is already an active migration.
    // The test verifies that the load balancer creates a plan
    // which when executed will achieve perfect balance,
    // which is a proof that it doesn't stop due to active migrations.

    topology_builder topo(e);
    auto host1 = topo.add_node(node_state::normal, 1);
    auto host2 = topo.add_node(node_state::normal, 1);
    auto host3 = topo.add_node(node_state::normal, 2);

    auto ks_name = add_keyspace(e, {{topo.dc(), 2}}, 4);
    auto table1 = add_table(e, ks_name).get();

    mutate_tablets(e, [&] (tablet_metadata& tmeta) -> future<> {
        tablet_map tmap(4);
        std::optional<tablet_id> tid = tmap.first_tablet();
        for (int i = 0; i < 4; ++i) {
            tmap.set_tablet(*tid, tablet_info {
                    tablet_replica_set {
                            tablet_replica {host1, 0},
                            tablet_replica {host2, 0},
                    }
            });
            tid = tmap.next_tablet(*tid);
        }
        tmap.set_tablet_transition_info(tmap.first_tablet(), tablet_transition_info {
                tablet_transition_stage::allow_write_both_read_old,
                tablet_transition_kind::migration,
                tablet_replica_set {
                        tablet_replica {host3, 0},
                        tablet_replica {host2, 0},
                },
                tablet_replica {host3, 0}
        });
        tmeta.set_tablet_map(table1, std::move(tmap));
        co_return;
    });

    abort_source as;
    auto guard = e.get_raft_group0_client().start_operation(as).get();
    auto& stm = e.shared_token_metadata().local();

    rebalance_tablets_as_in_progress(e.get_tablet_allocator().local(), stm);
    execute_transitions(stm);

    {
        load_sketch load(stm.get());
        load.populate().get();

        for (auto h : {host1, host2, host3}) {
            testlog.debug("Checking host {}", h);
            BOOST_REQUIRE_EQUAL(load.get_avg_shard_load(h), 2);
        }
    }

    // Restore consistency between stm and system tables before releasing group0 guard.
    save_tablet_metadata(e.local_db(), stm.get()->tablets(), guard.write_timestamp()).get();
  }).get();
}

#ifdef SCYLLA_ENABLE_ERROR_INJECTION
SEASTAR_THREAD_TEST_CASE(test_load_balancer_shuffle_mode) {
  do_with_cql_env_thread([] (auto& e) {
    topology_builder topo(e);

    auto host1 = topo.add_node(node_state::normal, 1);
    auto host2 = topo.add_node(node_state::normal, 1);
    topo.add_node(node_state::normal, 2);

    auto ks_name = add_keyspace(e, {{topo.dc(), 2}}, 4);
    auto table1 = add_table(e, ks_name).get();

    mutate_tablets(e, [&] (tablet_metadata& tmeta) -> future<> {
        tablet_map tmap(4);
        std::optional<tablet_id> tid = tmap.first_tablet();
        for (int i = 0; i < 4; ++i) {
            tmap.set_tablet(*tid, tablet_info {
                    tablet_replica_set {
                            tablet_replica {host1, 0},
                            tablet_replica {host2, 0},
                    }
            });
            tid = tmap.next_tablet(*tid);
        }
        tmeta.set_tablet_map(table1, std::move(tmap));
        co_return;
    });

    rebalance_tablets(e);

    auto& stm = e.shared_token_metadata().local();
    BOOST_REQUIRE(e.get_tablet_allocator().local().balance_tablets(stm.get()).get().empty());

    utils::get_local_injector().enable("tablet_allocator_shuffle");
    auto disable_injection = seastar::defer([&] {
        utils::get_local_injector().disable("tablet_allocator_shuffle");
    });

    BOOST_REQUIRE(!e.get_tablet_allocator().local().balance_tablets(stm.get()).get().empty());
  }).get();
}
#endif

SEASTAR_THREAD_TEST_CASE(test_load_balancing_with_two_empty_nodes) {
  do_with_cql_env_thread([] (auto& e) {
    topology_builder topo(e);

    const auto shard_count = 2;
    auto host1 = topo.add_node(node_state::normal, shard_count);
    auto host2 = topo.add_node(node_state::normal, shard_count);
    auto host3 = topo.add_node(node_state::normal, shard_count);
    auto host4 = topo.add_node(node_state::normal, shard_count);

    auto ks_name = add_keyspace(e, {{topo.dc(), 2}}, 16);
    auto table1 = add_table(e, ks_name).get();

    mutate_tablets(e, [&] (tablet_metadata& tmeta) -> future<> {
        tablet_map tmap(16);
        for (auto tid : tmap.tablet_ids()) {
            tmap.set_tablet(tid, tablet_info {
                tablet_replica_set {
                    tablet_replica {host1, tests::random::get_int<shard_id>(0, shard_count - 1)},
                    tablet_replica {host2, tests::random::get_int<shard_id>(0, shard_count - 1)},
                }
            });
        }
        tmeta.set_tablet_map(table1, std::move(tmap));
        co_return;
    });

    rebalance_tablets(e);

    auto& stm = e.shared_token_metadata().local();

    {
        load_sketch load(stm.get());
        load.populate().get();

        for (auto h : {host1, host2, host3, host4}) {
            testlog.debug("Checking host {}", h);
            BOOST_REQUIRE_EQUAL(load.get_avg_shard_load(h), 4);
            BOOST_REQUIRE_LE(load.get_shard_imbalance(h), 1);
        }
    }
  }).get();
}

SEASTAR_THREAD_TEST_CASE(test_load_balancing_with_asymmetric_node_capacity) {
    do_with_cql_env_thread([](auto& e) {
        topology_builder topo(e);

        auto host1 = topo.add_node(node_state::decommissioning, 8);
        auto host2 = topo.add_node(node_state::normal, 1);
        auto host3 = topo.add_node(node_state::normal, 7);

        auto ks_name = add_keyspace(e, {{topo.dc(), 1}}, 16);
        auto table1 = add_table(e, ks_name).get();

        mutate_tablets(e, [&] (tablet_metadata& tmeta) -> future<> {
            tablet_map tmap(16);
            for (auto tid: tmap.tablet_ids()) {
                tmap.set_tablet(tid, tablet_info {
                    tablet_replica_set {
                        tablet_replica {host1, 0},
                    }
              });
            }
            tmeta.set_tablet_map(table1, std::move(tmap));
            co_return;
        });

        auto until_nodes_drained = [] (const migration_plan& plan) {
            return !plan.has_nodes_to_drain();
        };

        rebalance_tablets(e, {}, {}, until_nodes_drained);

        auto& stm = e.shared_token_metadata().local();

        {
          load_sketch load(stm.get());
          load.populate().get();

          for (auto h: {host2, host3}) {
              testlog.debug("Checking host {}", h);
              BOOST_REQUIRE_EQUAL(load.get_avg_shard_load(h), 2); // 16 tablets / 8 shards = 2 tablets / shard
              BOOST_REQUIRE_EQUAL(load.get_shard_imbalance(h), 0);
          }
        }
    }).get();
}

SEASTAR_THREAD_TEST_CASE(test_load_balancer_disabling) {
    do_with_cql_env_thread([] (auto& e) {
        topology_builder topo(e);
        auto host1 = topo.add_node(node_state::normal, 2);
        topo.add_node(node_state::normal, 2);

        auto ks_name = add_keyspace(e, {{topo.dc(), 1}}, 16);
        auto table1 = add_table(e, ks_name).get();

        abort_source as;
        auto guard = e.get_raft_group0_client().start_operation(as).get();
        auto& stm = e.shared_token_metadata().local();

        // host1 is loaded and host2 is empty, resulting in an imbalance.
        // host1's shard 0 is loaded and shard 1 is empty, resulting in intra-node imbalance.
        mutate_tablets(e, guard, [&] (tablet_metadata& tmeta) -> future<> {
            tablet_map tmap(16);
            for (auto tid : tmap.tablet_ids()) {
                tmap.set_tablet(tid, tablet_info {
                    tablet_replica_set {
                        tablet_replica {host1, 0},
                    }
                });
            }
            tmeta.set_tablet_map(table1, std::move(tmap));
            co_return;
        });

        {
            auto plan = e.get_tablet_allocator().local().balance_tablets(stm.get()).get();
            BOOST_REQUIRE(!plan.empty());
        }

        // Disable load balancing
        stm.mutate_token_metadata([&] (token_metadata& tm) {
            tm.tablets().set_balancing_enabled(false);
            return make_ready_future<>();
        }).get();

        {
            auto plan = e.get_tablet_allocator().local().balance_tablets(stm.get()).get();
            BOOST_REQUIRE(plan.empty());
        }

        // Check that cloning preserves the setting
        stm.mutate_token_metadata([&] (token_metadata& tm) {
            return make_ready_future<>();
        }).get();

        {
            auto plan = e.get_tablet_allocator().local().balance_tablets(stm.get()).get();
            BOOST_REQUIRE(plan.empty());
        }

        // Enable load balancing back
        stm.mutate_token_metadata([&] (token_metadata& tm) {
            tm.tablets().set_balancing_enabled(true);
            return make_ready_future<>();
        }).get();

        {
            auto plan = e.get_tablet_allocator().local().balance_tablets(stm.get()).get();
            BOOST_REQUIRE(!plan.empty());
        }

        // Check that cloning preserves the setting
        stm.mutate_token_metadata([&] (token_metadata& tm) {
            return make_ready_future<>();
        }).get();

        {
            auto plan = e.get_tablet_allocator().local().balance_tablets(stm.get()).get();
            BOOST_REQUIRE(!plan.empty());
        }
  }).get();
}

SEASTAR_THREAD_TEST_CASE(test_drained_node_is_not_balanced_internally) {
    do_with_cql_env_thread([] (auto& e) {
        topology_builder topo(e);
        auto host1 = topo.add_node(node_state::removing, 2);
        topo.add_node(node_state::normal, 2);

        auto ks_name = add_keyspace(e, {{topo.dc(), 1}}, 16);
        auto table1 = add_table(e, ks_name).get();

        abort_source as;
        auto guard = e.get_raft_group0_client().start_operation(as).get();
        auto& stm = e.shared_token_metadata().local();

        mutate_tablets(e, guard, [&] (tablet_metadata& tmeta) -> future<> {
            tablet_map tmap(16);
            for (auto tid : tmap.tablet_ids()) {
                tmap.set_tablet(tid, tablet_info {
                    tablet_replica_set {
                        tablet_replica {host1, 0},
                    }
                });
            }
            tmeta.set_tablet_map(table1, std::move(tmap));
            co_return;
        });

        migration_plan plan = e.get_tablet_allocator().local().balance_tablets(stm.get()).get();
        BOOST_REQUIRE(plan.has_nodes_to_drain());
        for (auto&& mig : plan.migrations()) {
            BOOST_REQUIRE(mig.kind != tablet_transition_kind::intranode_migration);
        }
  }).get();
}

SEASTAR_THREAD_TEST_CASE(test_plan_fails_when_removing_last_replica) {
    do_with_cql_env_thread([] (auto& e) {
        topology_builder topo(e);
        auto host1 = topo.add_node();

        auto ks_name = add_keyspace(e, {{topo.dc(), 1}}, 1);
        auto table1 = add_table(e, ks_name).get();

        topo.set_node_state(host1, node_state::removing);

        mutate_tablets(e, [&] (tablet_metadata& tmeta) -> future<> {
            tablet_map tmap(1);
            for (auto tid : tmap.tablet_ids()) {
                tmap.set_tablet(tid, tablet_info {
                    tablet_replica_set{tablet_replica{host1, 0}}
                });
            }
            tmeta.set_tablet_map(table1, std::move(tmap));
            co_return;
        });

        std::unordered_set<host_id> skiplist = {host1};
        BOOST_REQUIRE_THROW(rebalance_tablets(e, {}, skiplist), std::runtime_error);
    }).get();
}

SEASTAR_THREAD_TEST_CASE(test_skiplist_is_ignored_when_draining) {
    // When doing normal load balancing, we can ignore DOWN nodes in the node set
    // and just balance the UP nodes among themselves because it's ok to equalize
    // load in that set.
    // It's dangerous to do that when draining because that can lead to overloading of the UP nodes.
    // In the worst case, we can have only one non-drained node in the UP set, which would receive
    // all the tablets of the drained node, doubling its load.
    // It's safer to let the drain fail/stall.
    do_with_cql_env_thread([] (auto& e) {
        topology_builder topo(e);

        auto host1 = topo.add_node(node_state::removing);
        auto host2 = topo.add_node(node_state::normal);
        auto host3 = topo.add_node(node_state::normal);

        auto ks_name = add_keyspace(e, {{topo.dc(), 1}}, 2);
        auto table1 = add_table(e, ks_name).get();

        mutate_tablets(e, [&] (tablet_metadata& tmeta) -> future<> {
            tablet_map tmap(2);
            auto tid = tmap.first_tablet();
            tmap.set_tablet(tid, tablet_info {
                tablet_replica_set{tablet_replica{host1, 0}}
            });
            tid = *tmap.next_tablet(tid);
            tmap.set_tablet(tid, tablet_info {
                tablet_replica_set{tablet_replica{host1, 0}}
            });
            tmeta.set_tablet_map(table1, std::move(tmap));
            co_return;
        });

        auto& stm = e.shared_token_metadata().local();
        std::unordered_set<host_id> skiplist = {host2};
        rebalance_tablets(e, {}, skiplist);

        {
            load_sketch load(stm.get());
            load.populate().get();

            for (auto h : {host2, host3}) {
                testlog.debug("Checking host {}", h);
                BOOST_REQUIRE_EQUAL(load.get_avg_shard_load(h), 1);
            }
        }
    }).get();
}

static
void check_tablet_invariants(const tablet_metadata& tmeta) {
    for (auto&& [table, tmap] : tmeta.all_tables()) {
        tmap->for_each_tablet([&](auto tid, const tablet_info& tinfo) -> future<> {
            std::unordered_set<host_id> hosts;
            // Uniqueness of hosts
            for (const auto& replica: tinfo.replicas) {
                auto ret = hosts.insert(replica.host).second;
                if (!ret) {
                    testlog.error("Failed tablet invariant check for tablet {}: {}", tid, tinfo.replicas);
                }
                BOOST_REQUIRE(ret);
            }
            return make_ready_future<>();
        }).get();
    }
}

static
std::vector<host_id>
allocate_replicas_in_racks(const std::vector<endpoint_dc_rack>& racks, int rf,
                           const std::unordered_map<sstring, std::vector<host_id>>& hosts_by_rack) {
    // Choose replicas randomly while loading racks evenly.
    std::vector<host_id> replica_hosts;
    for (int i = 0; i < rf; ++i) {
        auto rack = racks[i % racks.size()];
        auto& rack_hosts = hosts_by_rack.at(rack.rack);
        while (true) {
            auto candidate_host = rack_hosts[tests::random::get_int<shard_id>(0, rack_hosts.size() - 1)];
            if (std::find(replica_hosts.begin(), replica_hosts.end(), candidate_host) == replica_hosts.end()) {
                replica_hosts.push_back(candidate_host);
                break;
            }
        }
    }
    return replica_hosts;
}

SEASTAR_THREAD_TEST_CASE(test_load_balancing_with_random_load) {
  do_with_cql_env_thread([] (auto& e) {
    topology_builder topo(e);
    const int n_hosts = 6;
    auto shard_count = 2;

    std::vector<host_id> hosts;
    std::unordered_map<sstring, std::vector<host_id>> hosts_by_rack;

    std::vector<endpoint_dc_rack> racks {
        topo.rack(),
        topo.start_new_rack(),
    };

    for (int i = 0; i < n_hosts; ++i) {
        auto rack = racks[(i + 1) % racks.size()];
        auto h = topo.add_node(node_state::normal, shard_count, rack);
        if (i) {
            // Leave the first host empty by making it invisible to allocation algorithm.
            hosts_by_rack[rack.rack].push_back(h);
        }
    }

    auto& stm = e.shared_token_metadata().local();

    for (int i = 0; i < 13; ++i) {
        size_t total_tablet_count = 0;
        std::vector<sstring> keyspaces;
        size_t tablet_count_bits = 8;
        int rf = tests::random::get_int<shard_id>(2, 4);
        for (size_t log2_tablets = 0; log2_tablets < tablet_count_bits; ++log2_tablets) {
            if (tests::random::get_bool()) {
                continue;
            }
            auto initial_tablets = 1 << log2_tablets;
            keyspaces.push_back(add_keyspace(e, {{topo.dc(), rf}}, initial_tablets));
            auto table = add_table(e, keyspaces.back()).get();
            mutate_tablets(e, [&] (tablet_metadata& tmeta) -> future<> {
                tablet_map tmap(initial_tablets);
                for (auto tid : tmap.tablet_ids()) {
                    // Choose replicas randomly while loading racks evenly.
                    std::vector<host_id> replica_hosts = allocate_replicas_in_racks(racks, rf, hosts_by_rack);
                    tablet_replica_set replicas;
                    for (auto h : replica_hosts) {
                        auto shard = tests::random::get_int<shard_id>(0, shard_count - 1);
                        replicas.push_back(tablet_replica {h, shard});
                    }
                    tmap.set_tablet(tid, tablet_info {std::move(replicas)});
                }
                total_tablet_count += tmap.tablet_count();
                tmeta.set_tablet_map(table, std::move(tmap));
                return make_ready_future<>();
            });
        }

        testlog.debug("tablet metadata: {}", stm.get()->tablets());
        testlog.info("Total tablet count: {}, hosts: {}", total_tablet_count, hosts.size());

        check_tablet_invariants(stm.get()->tablets());

        rebalance_tablets(e);

        check_tablet_invariants(stm.get()->tablets());

        {
            load_sketch load(stm.get());
            load.populate().get();

            min_max_tracker<unsigned> min_max_load;
            for (auto h: hosts) {
                auto l = load.get_avg_shard_load(h);
                testlog.info("Load on host {}: {}", h, l);
                min_max_load.update(l);
                BOOST_REQUIRE_LE(load.get_shard_imbalance(h), 1);
            }

            testlog.debug("tablet metadata: {}", stm.get()->tablets());
            testlog.debug("Min load: {}, max load: {}", min_max_load.min(), min_max_load.max());

//          FIXME: The algorithm cannot achieve balance in all cases yet, so we only check that it stops.
//          For example, if we have an overloaded node in one rack and target underloaded node in a different rack,
//          we won't be able to reduce the load gap by moving tablets between the two. We have to balance the overloaded
//          rack first, which is unconstrained.
//          Uncomment the following line when the algorithm is improved.
//          BOOST_REQUIRE(min_max_load.max() - min_max_load.min() <= 1);
        }

        seastar::parallel_for_each(keyspaces, [&] (const sstring& ks) {
            return e.execute_cql(fmt::format("DROP KEYSPACE {}", ks)).discard_result();
        }).get();
    }
  }).get();
}

SEASTAR_THREAD_TEST_CASE(test_per_shard_goal_mixed_dc_rf) {
    do_with_cql_env_thread([] (auto& e) {
        auto per_shard_goal = e.local_db().get_config().tablets_per_shard_goal();

        topology_builder topo(e);

        std::vector<endpoint_dc_rack> racks = {
            topo.rack(),
            topo.start_new_dc(),
        };

        std::vector<host_id> hosts;
        hosts.push_back(topo.add_node(node_state::normal, 2, racks[0]));
        hosts.push_back(topo.add_node(node_state::normal, 2, racks[0]));
        hosts.push_back(topo.add_node(node_state::normal, 2, racks[0]));

        hosts.push_back(topo.add_node(node_state::normal, 1, racks[1]));
        hosts.push_back(topo.add_node(node_state::normal, 1, racks[1]));

        auto ks_name1 = add_keyspace(e, {{racks[0].dc, 3}});
        auto ks_name2 = add_keyspace(e, {{racks[1].dc, 2}});

        // table1 overflows per-shard goal in dc1, should be scaled down.
        // wants 400 tablets (3 nodes * 2 shards * 200 tablets/shard / rf=3 = 400 tablets)
        // which will be scaled down by a factor of 0.5 to achieve 100 tablets/shard, giving
        // 200 tablets, scaled up to the nearest power of 2, which is 256.
        e.execute_cql(fmt::format("CREATE TABLE {}.table1 (p1 text, r1 int, PRIMARY KEY (p1)) "
                                  "WITH tablets = {{'min_per_shard_tablet_count': 200}}", ks_name1)).get();
        auto table1 = e.local_db().find_schema(ks_name1, "table1")->id();

        // table2 has 64 tablets/shard in dc2, should not be scaled down.
        e.execute_cql(fmt::format("CREATE TABLE {}.table2 (p1 text, r1 int, PRIMARY KEY (p1)) "
                                  "WITH tablets = {{'min_per_shard_tablet_count': 64}}", ks_name2)).get();
        auto table2 = e.local_db().find_schema(ks_name2, "table2")->id();

        rebalance_tablets(e);

        {
            auto& stm = e.shared_token_metadata().local();
            auto tm = stm.get();
            BOOST_REQUIRE_EQUAL(tm->tablets().get_tablet_map(table1).tablet_count(), 256);
            BOOST_REQUIRE_EQUAL(tm->tablets().get_tablet_map(table2).tablet_count(), 64);

            load_sketch load(tm);
            load.populate().get();

            for (auto h: hosts) {
                auto l = load.get_shard_minmax(h);
                testlog.info("Load on host {}: min={}, max={}", h, l.min(), l.max());
                BOOST_REQUIRE_LE(l.max(), 2 * per_shard_goal);
            }
        }
    }, tablet_cql_test_config()).get();
}

// This test verifies that per-table tablet count is adjusted
// in reaction to changes of relevant config and schema options.
SEASTAR_THREAD_TEST_CASE(test_tablet_option_and_config_changes) {
    auto cfg = tablet_cql_test_config();
    cfg.db_config->tablets_initial_scale_factor(10.0);

    do_with_cql_env_thread([] (auto& e) {
        topology_builder topo(e);
        auto dc = topo.dc();

        // 3 shards. default initial scale wants 30 (32) tablets.
        // keyspace 'initial' wants 2 tablets.
        topo.add_node(node_state::normal, 3);

        auto ks_name1 = add_keyspace(e, {{dc, 1}}, 2);
        e.execute_cql(fmt::format("CREATE TABLE {}.table1 (p1 text, r1 int, PRIMARY KEY (p1))", ks_name1)).get();
        auto table1 = e.local_db().find_schema(ks_name1, "table1")->id();

        auto& stm = e.shared_token_metadata().local();
        auto get_tablet_count = [&] {
            auto tm = stm.get();
            return tm->tablets().get_tablet_map(table1).tablet_count();
        };

        shared_load_stats load_stats;
        load_stats.set_size(table1, 0);

        rebalance_tablets(e, &load_stats);
        BOOST_REQUIRE_EQUAL(get_tablet_count(), 2);

        // min_per_shard_tablet_count wants 5 * 3 = 15 (16) tablets
        e.execute_cql(fmt::format("ALTER TABLE {}.table1 "
                                  "WITH tablets = {{'min_per_shard_tablet_count': 5}}", ks_name1)).get();
        rebalance_tablets(e, &load_stats);
        BOOST_REQUIRE_EQUAL(get_tablet_count(), 16);

        // Check that hint can be dropped.
        e.execute_cql(fmt::format("ALTER TABLE {}.table1 WITH tablets = {{}}", ks_name1)).get();
        rebalance_tablets(e, &load_stats);
        BOOST_REQUIRE_EQUAL(get_tablet_count(), 2);

        // Default kicks in if keyspace setting and hint are missing.
        e.execute_cql(format("ALTER KEYSPACE {} with tablets = {{'enabled': true}}", ks_name1, dc)).get();
        rebalance_tablets(e, &load_stats);
        BOOST_REQUIRE_EQUAL(get_tablet_count(), 32);

        // initial scale can be live-updated.
        auto& cfg = e.db_config();
        cfg.tablets_initial_scale_factor(5);
        rebalance_tablets(e, &load_stats);
        BOOST_REQUIRE_EQUAL(get_tablet_count(), 16);

        // per-shard goal can be live-updated.

        // merge
        cfg.tablets_per_shard_goal(1);
        rebalance_tablets(e, &load_stats);
        BOOST_REQUIRE_EQUAL(get_tablet_count(), 4);

        // split
        cfg.tablets_per_shard_goal(100);
        rebalance_tablets(e, &load_stats);
        BOOST_REQUIRE_EQUAL(get_tablet_count(), 16);

        // initial scale can be smaller than 1.
        // 0.5 tablet/shard * 3 shards = 1.5 tablets =~ 2 tablets.
        cfg.tablets_initial_scale_factor(0.5);
        rebalance_tablets(e, &load_stats);
        BOOST_REQUIRE_EQUAL(get_tablet_count(), 2);
    }, cfg).get();
}

SEASTAR_THREAD_TEST_CASE(test_creating_lots_of_tables_doesnt_overflow_metadata) {
    auto cfg = tablet_cql_test_config();
    cfg.db_config->tablets_initial_scale_factor(10.0);
    cfg.db_config->tablets_per_shard_goal(100);

    do_with_cql_env_thread([] (auto& e) {
        topology_builder topo(e);
        auto dc = topo.dc();

        // 10 tablets/shard (initial_scale) * 16 shards = 160 tablets, rounded up to 256.
        // That's 16 tablet replicas per shard per table.
        // Creating 100 tables without scaling would give 1'600 tablets per shard,
        // which would overshoot the per-shard limit significantly.
        // This test verifies that scaling kicks in sooner as more tables are created,
        // and we end up with fewer tablets even before tablet merging is executed.

        auto host1 = topo.add_node(node_state::normal, 16);

        auto ks_name1 = add_keyspace(e, {{dc, 1}});
        std::vector<table_id> tables;
        shared_load_stats load_stats;

        const auto nr_tables = 100u;
        parallel_for_each(std::views::iota(0u, nr_tables), [&] (auto i) -> future<> {
            auto table_name = fmt::format("table_{}", i);
            co_await e.execute_cql(fmt::format("CREATE TABLE {}.{} (p1 text, r1 int, PRIMARY KEY (p1))",
                                               ks_name1, table_name));
            table_id table = e.local_db().find_schema(ks_name1, table_name)->id();
            tables.push_back(table);
            load_stats.set_size(table, 0);
        }).get();

        auto& stm = e.shared_token_metadata().local();

        {
            load_sketch load(stm.get());
            load.populate().get();
            testlog.info("max load: {}", load.get_shard_minmax(host1).max());
            // The value 415 was determined empirically. If there was lack of scaling, it would be 1'600.
            BOOST_REQUIRE(load.get_shard_minmax(host1).max() <= 415);
        }

        rebalance_tablets(e, &load_stats);

        {
            load_sketch load(stm.get());
            load.populate().get();
            testlog.info("max load: {}", load.get_shard_minmax(host1).max());
            BOOST_REQUIRE(load.get_shard_minmax(host1).max() <= 200);
        }
    }, cfg).get();
}

SEASTAR_TEST_CASE(test_tablet_id_and_range_side) {
    static constexpr size_t tablet_count = 128;
    locator::tablet_map tmap(tablet_count);
    locator::tablet_map tmap_after_splitting(tablet_count * 2);

    for (size_t id = 0; id < tablet_count; id++) {
        auto left_id = tablet_id(id << 1);
        auto right_id = tablet_id(left_id.value() + 1);
        auto left_tr = tmap_after_splitting.get_token_range(left_id);
        auto right_tr = tmap_after_splitting.get_token_range(right_id);
        testlog.debug("id {}, left tr {}, right tr {}", id, left_tr, right_tr);

        auto test = [&tmap, id] (dht::token token, tablet_range_side expected_side) {
            auto [tid, side] = tmap.get_tablet_id_and_range_side(token);
            BOOST_REQUIRE_EQUAL(tid.value(), id);
            BOOST_REQUIRE_EQUAL(side, expected_side);
        };

        auto test_range = [&] (dht::token_range& tr, tablet_range_side expected_side) {
            auto lower_token = tr.start()->value() == dht::minimum_token() ? dht::first_token() : tr.start()->value();
            auto upper_token = tr.end()->value();
            test(next_token(lower_token), expected_side);
            test(upper_token, expected_side);
        };

        // Test the lower and upper bound of tablet's left and right ranges ("compaction groups").
        test_range(left_tr, tablet_range_side::left);
        test_range(right_tr, tablet_range_side::right);
    }

    return make_ready_future<>();
}

SEASTAR_THREAD_TEST_CASE(basic_tablet_storage_splitting_test) {
    auto cfg = tablet_cql_test_config();
    cfg.initial_tablets = std::bit_floor(smp::count);
    do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql(
                "CREATE TABLE cf (pk int, ck int, v int, PRIMARY KEY (pk, ck))").get();

        for (unsigned i = 0; i < smp::count * 20; i++) {
            e.execute_cql(format("INSERT INTO cf (pk, ck, v) VALUES ({}, 0, 0)", i)).get();
        }

        e.db().invoke_on_all([] (replica::database& db) {
            auto& table = db.find_column_family("ks", "cf");
            return table.flush();
        }).get();

        testlog.info("Splitting sstables...");
        e.db().invoke_on_all([] (replica::database& db) {
            auto& table = db.find_column_family("ks", "cf");
            testlog.info("sstable count: {}", table.sstables_count());
            return table.split_all_storage_groups(tasks::task_info{});
        }).get();

        testlog.info("Verifying sstables are split...");
        BOOST_REQUIRE_EQUAL(e.db().map_reduce0([] (replica::database& db) {
            auto& table = db.find_column_family("ks", "cf");
            return make_ready_future<bool>(table.all_storage_groups_split());
        }, bool(false), std::logical_or<bool>()).get(), true);
    }, std::move(cfg)).get();
}

using rack_vector = std::vector<endpoint_dc_rack>;
using hosts_by_rack_map = std::unordered_map<sstring, std::vector<host_id>>;

// runs in seastar thread.
static void do_test_load_balancing_merge_colocation(cql_test_env& e, const int n_racks, const int rf, const int n_hosts,
                                                    const unsigned shard_count, const unsigned initial_tablets,
                                                    std::function<void(token_metadata&, tablet_map&, const rack_vector&, const hosts_by_rack_map&)> set_tablets) {
    topology_builder topo(e);

    rack_vector racks;
    for (int i = 0; i < n_racks; i++) {
        racks.push_back(topo.rack());
        topo.start_new_rack();
    }

    testlog.info("merge colocation test - hosts={}, racks={}, rf={}, shard_count={}, initial_tablets={}", n_hosts, racks.size(), rf, shard_count, initial_tablets);

    hosts_by_rack_map hosts_by_rack;

    for (int i = 0; i < n_hosts; ++i) {
        auto rack = racks[i % racks.size()];
        auto h = topo.add_node(node_state::normal, shard_count, rack);
        hosts_by_rack[rack.rack].push_back(h);
    }

    auto ks_name = add_keyspace(e, {{topo.dc(), rf}}, initial_tablets);
    auto table1 = add_table(e, ks_name).get();
    auto& stm = e.shared_token_metadata().local();

    {
        abort_source as;
        auto guard = e.get_raft_group0_client().start_operation(as).get();
        stm.mutate_token_metadata([&](token_metadata& tm) -> future<> {
            tablet_metadata& tmeta = tm.tablets();
            tablet_map tmap(initial_tablets);
            locator::resize_decision decision;
            // leaves growing mode, allowing for merge decision.
            decision.sequence_number = decision.next_sequence_number();
            tmap.set_resize_decision(std::move(decision));
            set_tablets(tm, tmap, racks, hosts_by_rack);
            tmeta.set_tablet_map(table1, std::move(tmap));
            tm.set_tablets(std::move(tmeta));
            return make_ready_future < > ();
        }).get();
        save_tablet_metadata(e.local_db(), stm.get()->tablets(), guard.write_timestamp()).get();
    }

    // Lower "initial" tablets option, allowing for merge decision.
    e.execute_cql(fmt::format("alter keyspace {} with tablets = {{'enabled': true, 'initial': 1}}", ks_name)).get();

    auto tablet_count = [&] {
        return stm.get()->tablets().get_tablet_map(table1).tablet_count();
    };
    shared_load_stats load_stats;
    auto do_rebalance_tablets = [&] () {
        rebalance_tablets(e, &load_stats);
    };

    const uint64_t target_tablet_size = service::default_target_tablet_size;
    auto merge_threshold = [&] () -> uint64_t {
        return (target_tablet_size * 0.5f) * tablet_count();
    };

    while (tablet_count() > 1) {
        load_stats.set_size(table1, merge_threshold() - 1);

        auto old_tablet_count = tablet_count();
        check_tablet_invariants(stm.get()->tablets());
        do_rebalance_tablets();
        check_tablet_invariants(stm.get()->tablets());
        BOOST_REQUIRE_LT(tablet_count(), old_tablet_count);
    }

    e.execute_cql(fmt::format("drop keyspace {}", ks_name)).get();
}

SEASTAR_THREAD_TEST_CASE(test_load_balancing_merge_colocation_with_random_load) {
    do_with_cql_env_thread([] (auto& e) {
        auto seed = tests::random::get_int<int32_t>();
        std::mt19937 random_engine{seed};

        testlog.info("test_load_balancing_merge_colocation - seed {}", seed);

        for (auto i = 0; i < 10; i++) {
            const int rf = tests::random::get_int<int>(3, 3);
            const int n_racks = rf;
            const int n_hosts = tests::random::get_int<unsigned>(n_racks * rf, n_racks * rf * 2);
            const unsigned shard_count = tests::random::get_int<unsigned>(2, 12);
            const unsigned total_shard_count = n_hosts * shard_count;
            const unsigned initial_tablets = std::bit_ceil<unsigned>(tests::random::get_int<unsigned>(total_shard_count, total_shard_count * 10));

            auto set_tablets = [rf, shard_count] (token_metadata&, tablet_map& tmap, const rack_vector& racks, const hosts_by_rack_map& hosts_by_rack) {
                for (auto tid : tmap.tablet_ids()) {
                    testlog.debug("allocating replica in racks with rf {}", rf);
                    std::vector<host_id> replica_hosts = allocate_replicas_in_racks(racks, rf, hosts_by_rack);
                    tablet_replica_set replicas;
                    replicas.reserve(replica_hosts.size());
                    for (auto h : replica_hosts) {
                        replicas.push_back(tablet_replica {h, tests::random::get_int<shard_id>(0, shard_count - 1)});
                    }
                    testlog.debug("allocating replicas for tablet {}: {}", tid, replicas);
                    tmap.set_tablet(tid, tablet_info {std::move(replicas)});
                }
            };

            do_test_load_balancing_merge_colocation(e, n_racks, rf, n_hosts, shard_count, initial_tablets, set_tablets);
        }
    }).get();
}

SEASTAR_THREAD_TEST_CASE(test_load_balancing_merge_colocation_with_single_rack) {
    do_with_cql_env_thread([] (auto& e) {
        const int rf = 2;
        const int n_racks = 1;
        const int n_hosts = 2;
        const unsigned shard_count = 2;
        const unsigned initial_tablets = 2;

        auto set_tablets = [] (token_metadata&, tablet_map& tmap, const rack_vector& racks, const hosts_by_rack_map& hosts_by_rack) {
            auto& hosts = hosts_by_rack.at(racks.front().rack);
            auto host1 = hosts[0];
            auto host2 = hosts[1];
            tmap.set_tablet(tablet_id(0), tablet_info {
                tablet_replica_set {
                    tablet_replica {host1, shard_id(0)},
                    tablet_replica {host2, shard_id(0)},
                }
            });
            tmap.set_tablet(tablet_id(1), tablet_info {
                tablet_replica_set {
                    tablet_replica {host2, shard_id(0)},
                    tablet_replica {host1, shard_id(0)},
                }
            });
        };

        do_test_load_balancing_merge_colocation(e, n_racks, rf, n_hosts, shard_count, initial_tablets, set_tablets);
    }).get();
}

SEASTAR_THREAD_TEST_CASE(test_load_balancing_merge_colocation_with_decomission) {
    do_with_cql_env_thread([] (auto& e) {
        const int rf = 3;
        const int n_racks = 1;
        const int n_hosts = 4;
        const unsigned shard_count = 2;
        const unsigned initial_tablets = 2;

        auto set_tablets = [&] (token_metadata& tm, tablet_map& tmap, const rack_vector& racks, const hosts_by_rack_map& hosts_by_rack) {
            auto& rack = racks.front();
            auto& hosts = hosts_by_rack.at(rack.rack);
            BOOST_REQUIRE(hosts.size() == 4);
            auto a = hosts[0];
            auto b = hosts[1];
            auto c = hosts[2];
            auto d = hosts[3];

            // nodes   = {A, B, C, D}
            // tablet1 = {A, B, C}
            // tablet2 = {A, B, D}
            // viable target for {tablet1, B} is D.
            // viable target for {tablet2, B} is C.
            //
            // Decomission should succeed by migrating away even co-located replicas of sibling tablets that don't share viable targets.
            // That should produce:
            // tablet1 = {A, D, C}
            // tablet2 = {A, C, D}

            auto decision = tmap.resize_decision();
            decision.way = locator::resize_decision::merge{};
            tmap.set_resize_decision(std::move(decision));
            tm.update_topology(b, rack, node::state::being_decommissioned, shard_count);

            tmap.set_tablet(tablet_id(0), tablet_info {
                tablet_replica_set {
                    tablet_replica {a, shard_id(0)},
                    tablet_replica {b, shard_id(0)},
                    tablet_replica {c, shard_id(0)},
                }
            });
            tmap.set_tablet(tablet_id(1), tablet_info {
                tablet_replica_set {
                    tablet_replica {a, shard_id(0)},
                    tablet_replica {b, shard_id(0)},
                    tablet_replica {d, shard_id(0)},
                }
            });
        };

        do_test_load_balancing_merge_colocation(e, n_racks, rf, n_hosts, shard_count, initial_tablets, set_tablets);
    }).get();
}

SEASTAR_THREAD_TEST_CASE(test_load_balancing_resize_requests) {
    do_with_cql_env_thread([] (auto& e) {
        topology_builder topo(e);

        topo.add_node(node_state::normal, 2);
        topo.add_node(node_state::normal, 2);

        const size_t initial_tablets = 2;
        auto ks_name = add_keyspace(e, {{topo.dc(), 2}}, initial_tablets);
        auto table1 = add_table(e, ks_name).get();

        auto& stm = e.shared_token_metadata().local();

        auto tablet_count = [&] {
            return stm.get()->tablets().get_tablet_map(table1).tablet_count();
        };

        auto resize_decision = [&] {
            return stm.get()->tablets().get_tablet_map(table1).resize_decision();
        };

        shared_load_stats load_stats;
        auto do_rebalance_tablets = [&] () {
            rebalance_tablets(e, &load_stats, {}, nullptr, false); // no auto-split
        };

        const uint64_t max_tablet_size = service::default_target_tablet_size * 2;
        auto to_size_in_bytes = [&] (double max_tablet_size_pctg) -> uint64_t {
            return (max_tablet_size * max_tablet_size_pctg) * tablet_count();
        };


        const auto initial_ready_seq_number = std::numeric_limits<locator::resize_decision::seq_number_t>::min();

        load_stats.set_split_ready_seq_number(table1, initial_ready_seq_number);

        // avg size moved above target size, so merge is cancelled
        {
            load_stats.set_size(table1, to_size_in_bytes(0.75));

            do_rebalance_tablets();
            BOOST_REQUIRE_EQUAL(tablet_count(), initial_tablets);
            BOOST_REQUIRE(std::holds_alternative<locator::resize_decision::none>(resize_decision().way));
        }

        // Drop initial tablet count to 1 so merge can happen.
        e.execute_cql(fmt::format("alter keyspace {} with tablets = {{'enabled': true, 'initial': 1}}", ks_name)).get();

        // avg size hits split threshold, and balancer emits split request
        {
            load_stats.set_size(table1, to_size_in_bytes(1.1));

            do_rebalance_tablets();
            BOOST_REQUIRE_EQUAL(tablet_count(), initial_tablets);
            BOOST_REQUIRE(std::holds_alternative<locator::resize_decision::split>(resize_decision().way));
            BOOST_REQUIRE_GT(resize_decision().sequence_number, 0);
        }

        // replicas set their split status as ready, and load balancer finalizes split generating a new
        // tablet map, twice as large as the previous one.
        {
            load_stats.set_split_ready_seq_number(table1, resize_decision().sequence_number);

            do_rebalance_tablets();

            BOOST_REQUIRE_EQUAL(tablet_count(), initial_tablets * 2);
            BOOST_REQUIRE(std::holds_alternative<locator::resize_decision::none>(resize_decision().way));
        }

        // Check that balancer detects table size dropped to 0 and reduces tablet count down to 1 through merges.
        {
            load_stats.set_size(table1, to_size_in_bytes(0.0));
            load_stats.set_split_ready_seq_number(table1, initial_ready_seq_number);

            do_rebalance_tablets();
            BOOST_REQUIRE_EQUAL(tablet_count(), 1);
        }

    }).get();
}

SEASTAR_THREAD_TEST_CASE(test_tablet_range_splitter) {
    simple_schema ss;

    const auto dks = ss.make_pkeys(4);

    auto h1 = host_id(utils::UUID_gen::get_time_UUID());
    auto h2 = host_id(utils::UUID_gen::get_time_UUID());
    auto h3 = host_id(utils::UUID_gen::get_time_UUID());

    tablet_map tmap(4);
    auto tb = tmap.first_tablet();
    tmap.set_tablet(tb, tablet_info {
        tablet_replica_set {
            tablet_replica {h2, 0},
            tablet_replica {h3, 0},
        }
    });
    tb = *tmap.next_tablet(tb);
    tmap.set_tablet(tb, tablet_info {
        tablet_replica_set {
            tablet_replica {h1, 3},
        }
    });
    tb = *tmap.next_tablet(tb);
    tmap.set_tablet(tb, tablet_info {
        tablet_replica_set {
            tablet_replica {h2, 2},
        }
    });
    tb = *tmap.next_tablet(tb);
    tmap.set_tablet(tb, tablet_info {
        tablet_replica_set {
            tablet_replica {h1, 1},
            tablet_replica {h2, 1},
        }
    });

    using result = tablet_range_splitter::range_split_result;
    using bound = dht::partition_range::bound;

    std::vector<result> included_ranges;
    std::vector<dht::partition_range> excluded_ranges;
    for (auto tid = std::optional(tmap.first_tablet()); tid; tid = tmap.next_tablet(*tid)) {
        const auto& tablet_info = tmap.get_tablet_info(*tid);
        auto replica_it = std::ranges::find_if(tablet_info.replicas, [&] (auto&& r) { return r.host == h1; });
        auto token_range = tmap.get_token_range(*tid);
        auto range = dht::to_partition_range(token_range);
        if (replica_it == tablet_info.replicas.end()) {
            testlog.info("tablet#{}: {} (no replica on h1)", *tid, token_range);
            excluded_ranges.emplace_back(std::move(range));
        } else {
            testlog.info("tablet#{}: {} (shard {})", *tid, token_range, replica_it->shard);
            included_ranges.emplace_back(result{replica_it->shard, std::move(range)});
        }
    }

    dht::ring_position_comparator cmp(*ss.schema());

    auto check = [&] (const dht::partition_range_vector& ranges, std::vector<result> expected_result,
            std::source_location sl = std::source_location::current()) {
        testlog.info("check() @ {}:{} ranges={}", sl.file_name(), sl.line(), ranges);
        locator::tablet_range_splitter range_splitter{ss.schema(), tmap, h1, ranges};
        auto it = expected_result.begin();
        while (auto range_opt = range_splitter()) {
            testlog.debug("result: shard={} range={}", range_opt->shard, range_opt->range);
            BOOST_REQUIRE(it != expected_result.end());
            testlog.debug("expected: shard={} range={}", it->shard, it->range);
            BOOST_REQUIRE_EQUAL(it->shard, range_opt->shard);
            BOOST_REQUIRE(it->range.equal(range_opt->range, cmp));
            ++it;
        }
        if (it != expected_result.end()) {
            while (it != expected_result.end()) {
                testlog.error("missing expected result: shard={} range={}", it->shard, it->range);
                ++it;
            }
            BOOST_FAIL("splitter didn't provide all expected ranges");
        }
    };
    auto check_single = [&] (const dht::partition_range& range, std::vector<result> expected_result,
            std::source_location sl = std::source_location::current()) {
        dht::partition_range_vector ranges;
        ranges.reserve(1);
        ranges.push_back(std::move(range));
        check(ranges, std::move(expected_result), sl);
    };
    auto intersect = [&] (const dht::partition_range& range) {
        std::vector<result> intersecting_ranges;
        for (const auto& included_range : included_ranges) {
            if (auto intersection = included_range.range.intersection(range, cmp)) {
                intersecting_ranges.push_back({included_range.shard, std::move(*intersection)});
            }
        }
        return intersecting_ranges;
    };
    auto check_intersection_single = [&] (const dht::partition_range& range,
            std::source_location sl = std::source_location::current()) {
        check_single(range, intersect(range), sl);
    };
    auto check_intersection = [&] (const dht::partition_range_vector& ranges,
            std::source_location sl = std::source_location::current()) {
        std::vector<result> expected_ranges;
        for (const auto& range : ranges) {
            auto res = intersect(range);
            std::move(res.begin(), res.end(), std::back_inserter(expected_ranges));
        }
        std::sort(expected_ranges.begin(), expected_ranges.end(), [&] (const auto& a, const auto& b) {
            return !a.range.start() || b.range.before(a.range.start()->value(), cmp);
        });
        check(ranges, expected_ranges, sl);
    };

    check_single(dht::partition_range::make_open_ended_both_sides(), included_ranges);
    check(included_ranges | std::views::transform([&] (auto& r) { return r.range; }) | std::ranges::to<dht::partition_range_vector>(), included_ranges);
    check(excluded_ranges, {});

    check_intersection_single({bound{dks[0], true}, bound{dks[1], false}});
    check_intersection_single({bound{dks[0], false}, bound{dks[2], true}});
    check_intersection_single({bound{dks[2], true}, bound{dks[3], false}});
    check_intersection_single({bound{dks[0], false}, bound{dks[3], false}});
    check_intersection_single(dht::partition_range::make_starting_with(bound(dks[2], true)));
    check_intersection_single(dht::partition_range::make_ending_with(bound(dks[1], false)));
    check_intersection_single(dht::partition_range::make_singular(dks[3]));

    check_intersection({
            dht::partition_range::make_ending_with(bound(dks[0], false)),
            {bound{dks[1], true}, bound{dks[2], false}},
            dht::partition_range::make_starting_with(bound(dks[3], true))});

    check_intersection({
            {bound{dks[0], true}, bound{dks[1], false}},
            {bound{dks[1], true}, bound{dks[2], false}},
            {bound{dks[2], true}, bound{dks[3], false}}});

}

static locator::endpoint_dc_rack make_endpoint_dc_rack(gms::inet_address endpoint) {
    // This resembles rack_inferring_snitch dc/rack generation which is
    // still in use by this test via token_metadata internals
    auto dc = std::to_string(uint8_t(endpoint.bytes()[1]));
    auto rack = std::to_string(uint8_t(endpoint.bytes()[2]));
    return locator::endpoint_dc_rack{dc, rack};
}

struct calculate_tablet_replicas_for_new_rf_config
{
    struct ring_point {
        double point;
        inet_address host;
        host_id id = host_id::create_random_id();
    };
    std::vector<ring_point> ring_points;
    std::map<sstring, sstring> options;
    std::map<sstring, sstring> new_dc_rep_factor;
    std::map<sstring, size_t> expected_rep_factor;
};

static void execute_tablet_for_new_rf_test(calculate_tablet_replicas_for_new_rf_config const& test_config)
{
    auto my_address = gms::inet_address("localhost");
    // Create the RackInferringSnitch
    snitch_config cfg;
    cfg.listen_address = my_address;
    cfg.broadcast_address = my_address;
    cfg.name = "RackInferringSnitch";
    sharded<snitch_ptr> snitch;
    snitch.start(cfg).get();
    auto stop_snitch = defer([&snitch] { snitch.stop().get(); });
    snitch.invoke_on_all(&snitch_ptr::start).get();

    static constexpr size_t tablet_count = 8;

    std::vector<unsigned> nodes_shard_count(test_config.ring_points.size(), 3);

    locator::token_metadata::config tm_cfg;
    tm_cfg.topo_cfg.this_endpoint = test_config.ring_points[0].host;
    tm_cfg.topo_cfg.local_dc_rack = { snitch.local()->get_datacenter(), snitch.local()->get_rack() };
    tm_cfg.topo_cfg.this_host_id = test_config.ring_points[0].id;
    locator::shared_token_metadata stm([] () noexcept { return db::schema_tables::hold_merge_lock(); }, tm_cfg);

    // Initialize the token_metadata
    stm.mutate_token_metadata([&] (token_metadata& tm) -> future<> {
        auto& topo = tm.get_topology();
        for (const auto& [ring_point, endpoint, id] : test_config.ring_points) {
            std::unordered_set<token> tokens;
            tokens.insert(dht::token{tests::d2t(ring_point / test_config.ring_points.size())});
            topo.add_or_update_endpoint(id, make_endpoint_dc_rack(endpoint), locator::node::state::normal, 1);
            co_await tm.update_normal_tokens(std::move(tokens), id);
        }
    }).get();

    locator::replication_strategy_params params(test_config.options, tablet_count);

    auto ars_ptr = abstract_replication_strategy::create_replication_strategy(
        "NetworkTopologyStrategy", params);

    auto tablet_aware_ptr = ars_ptr->maybe_as_tablet_aware();
    BOOST_REQUIRE(tablet_aware_ptr);

    auto s = schema_builder("ks", "tb")
        .with_column("pk", utf8_type, column_kind::partition_key)
        .with_column("v", utf8_type)
        .build();

    stm.mutate_token_metadata([&] (token_metadata& tm) {
        for (size_t i = 0; i < test_config.ring_points.size(); ++i) {
            auto& [ring_point, endpoint, id] = test_config.ring_points[i];
            tm.update_topology(id, make_endpoint_dc_rack(endpoint), node::state::normal, nodes_shard_count[i]);
        }
        return make_ready_future<>();
    }).get();

    auto allocated_map = tablet_aware_ptr->allocate_tablets_for_new_table(s, stm.get(), tablet_count).get();

    BOOST_REQUIRE_EQUAL(allocated_map.tablet_count(), tablet_count);

    auto host_id_to_dc = [&stm](const locator::host_id& ep) -> std::optional<sstring> {
        auto node = stm.get()->get_topology().find_node(ep);
        if (node == nullptr) {
            return std::nullopt;
        }
        return node->dc_rack().dc;
    };

    stm.mutate_token_metadata([&] (token_metadata& tm) {
        tablet_metadata tab_meta;
        auto table = s->id();
        tab_meta.set_tablet_map(table, allocated_map);
        tm.set_tablets(std::move(tab_meta));
        return make_ready_future<>();
    }).get();

    std::map<sstring, size_t> initial_rep_factor;
    for (auto const& [dc, shard_count] : test_config.options) {
        initial_rep_factor[dc] = std::stoul(shard_count);
    }

    auto tablets = stm.get()->tablets().get_tablet_map(s->id());
    BOOST_REQUIRE_EQUAL(tablets.tablet_count(), tablet_count);
    for (auto tb : tablets.tablet_ids()) {
        const locator::tablet_info& ti = tablets.get_tablet_info(tb);

        std::map<sstring, size_t> dc_replicas_count;
        for (const auto& r : ti.replicas) {
            auto dc = host_id_to_dc(r.host);
            if (dc) {
                dc_replicas_count[*dc]++;
            }
        }

        BOOST_REQUIRE_EQUAL(dc_replicas_count, initial_rep_factor);
    }

    try {
        tablet_map old_tablets = stm.get()->tablets().get_tablet_map(s->id());
        locator::replication_strategy_params params{test_config.new_dc_rep_factor, old_tablets.tablet_count()};
        auto new_strategy = abstract_replication_strategy::create_replication_strategy("NetworkTopologyStrategy", params);
        auto tmap = new_strategy->maybe_as_tablet_aware()->reallocate_tablets(s, stm.get(), old_tablets).get();

        auto const& ts = tmap.tablets();
        BOOST_REQUIRE_EQUAL(ts.size(), tablet_count);

        for (auto tb : tmap.tablet_ids()) {
            const locator::tablet_info& ti = tmap.get_tablet_info(tb);

            std::map<sstring, size_t> dc_replicas_count;
            for (const auto& r : ti.replicas) {
                auto dc = host_id_to_dc(r.host);
                if (dc) {
                    dc_replicas_count[*dc]++;
                }
            }

            BOOST_REQUIRE_EQUAL(dc_replicas_count, test_config.expected_rep_factor);
        }

    } catch (exceptions::configuration_exception const& e) {
        thread_local boost::regex re(
                "Datacenter [0-9]+ doesn't have enough token-owning nodes for replication_factor=[0-9]+");
        boost::cmatch what;
        if (!boost::regex_search(e.what(), what, re)) {
            BOOST_FAIL("Unexpected exception: " + std::string(e.what()));
        }
    } catch (std::exception const& e) {
        BOOST_FAIL("Unexpected exception: " + std::string(e.what()));
    } catch (...) {
        BOOST_FAIL("Unexpected exception");
    }
}

SEASTAR_THREAD_TEST_CASE(test_calculate_tablet_replicas_for_new_rf_upsize_one_dc) {
    calculate_tablet_replicas_for_new_rf_config config;
    config.ring_points = {
            { 1.0,  inet_address("192.100.10.1") },
            { 4.0,  inet_address("192.100.20.1") },
            { 7.0,  inet_address("192.100.30.1") },
    };
    config.options = {{"100", "2"}};
    config.new_dc_rep_factor = {{"100", "3"}};
    config.expected_rep_factor = {{"100", 3}};
    execute_tablet_for_new_rf_test(config);
}

SEASTAR_THREAD_TEST_CASE(test_calculate_tablet_replicas_for_new_rf_downsize_one_dc) {
    calculate_tablet_replicas_for_new_rf_config config;
    config.ring_points = {
            { 1.0,  inet_address("192.100.10.1") },
            { 4.0,  inet_address("192.100.20.1") },
            { 7.0,  inet_address("192.100.30.1") },
    };
    config.options = {{"100", "3"}};
    config.new_dc_rep_factor = {{"100", "2"}};
    config.expected_rep_factor = {{"100", 2}};
    execute_tablet_for_new_rf_test(config);
}

SEASTAR_THREAD_TEST_CASE(test_calculate_tablet_replicas_for_new_rf_no_change_one_dc) {
    calculate_tablet_replicas_for_new_rf_config config;
    config.ring_points = {
            { 1.0,  inet_address("192.100.10.1") },
            { 4.0,  inet_address("192.100.20.1") },
            { 7.0,  inet_address("192.100.30.1") },
    };
    config.options = {{"100", "3"}};
    config.new_dc_rep_factor = {{"100", "3"}};
    config.expected_rep_factor = {{"100", 3}};
    execute_tablet_for_new_rf_test(config);
}

SEASTAR_THREAD_TEST_CASE(test_calculate_tablet_replicas_for_new_rf) {
    calculate_tablet_replicas_for_new_rf_config config;
    config.ring_points = {
            { 1.0,  inet_address("192.100.10.1") },
            { 2.0,  inet_address("192.101.10.1") },
            { 3.0,  inet_address("192.102.10.1") },
            { 4.0,  inet_address("192.100.20.1") },
            { 5.0,  inet_address("192.101.20.1") },
            { 6.0,  inet_address("192.102.20.1") },
            { 7.0,  inet_address("192.100.30.1") },
            { 8.0,  inet_address("192.101.30.1") },
            { 9.0,  inet_address("192.102.30.1") },
            { 10.0, inet_address("192.101.40.1") },
            { 11.0, inet_address("192.102.40.1") },
            { 12.0, inet_address("192.102.40.2") }
    };
    config.options = {
        {"100", "3"},
        {"101", "2"},
        {"102", "3"}
    };
    config.new_dc_rep_factor = {
        {"100", "3"},
        {"101", "4"},
        {"102", "2"}
    };
    config.expected_rep_factor = {
        {"100", 3},
        {"101", 4},
        {"102", 2}
    };

    execute_tablet_for_new_rf_test(config);
}

SEASTAR_THREAD_TEST_CASE(test_calculate_tablet_replicas_for_new_rf_not_enough_nodes) {

    calculate_tablet_replicas_for_new_rf_config config;
    config.ring_points = {
            { 1.0,  inet_address("192.100.10.1") },
            { 4.0,  inet_address("192.100.20.1") },
            { 7.0,  inet_address("192.100.30.1") },
    };
    config.options = {{"100", "3"}};
    config.new_dc_rep_factor = {{"100", "5"}};
    config.expected_rep_factor = {{"100", 3}};
    execute_tablet_for_new_rf_test(config);
}

SEASTAR_THREAD_TEST_CASE(test_calculate_tablet_replicas_for_new_rf_one_dc) {
    calculate_tablet_replicas_for_new_rf_config config;
    config.ring_points = {
            { 1.0,  inet_address("192.100.10.1") },
            { 4.0,  inet_address("192.100.20.1") },
            { 7.0,  inet_address("192.100.30.1") },
    };
    config.options = {{"100", "2"}};
    config.new_dc_rep_factor = {{"100", "3"}};
    config.expected_rep_factor = {{"100", 3}};
    execute_tablet_for_new_rf_test(config);
}

SEASTAR_THREAD_TEST_CASE(test_calculate_tablet_replicas_for_new_rf_one_dc_1_to_2) {
    calculate_tablet_replicas_for_new_rf_config config;
    config.ring_points = {
            { 1.0,  inet_address("192.100.10.1") },
            { 4.0,  inet_address("192.100.20.1") },
    };
    config.options = {{"100", "1"}};
    config.new_dc_rep_factor = {{"100", "2"}};
    config.expected_rep_factor = {{"100", 2}};
    execute_tablet_for_new_rf_test(config);
}

SEASTAR_THREAD_TEST_CASE(test_calculate_tablet_replicas_for_new_rf_one_dc_not_enough_nodes) {

    calculate_tablet_replicas_for_new_rf_config config;
    config.ring_points = {
            { 1.0,  inet_address("192.100.10.1") },
            { 4.0,  inet_address("192.100.10.2") },
            { 7.0,  inet_address("192.100.10.3") },
    };
    config.options = {{"100", "3"}};
    config.new_dc_rep_factor = {{"100", "5"}};
    config.expected_rep_factor = {{"100", 3}};
    execute_tablet_for_new_rf_test(config);
}

SEASTAR_THREAD_TEST_CASE(test_calculate_tablet_replicas_for_new_rf_default_rf) {
    calculate_tablet_replicas_for_new_rf_config config;
    config.ring_points = {
            { 1.0,  inet_address("192.100.10.1") },
            { 2.0,  inet_address("192.101.10.1") },
            { 3.0,  inet_address("192.102.10.1") },
            { 4.0,  inet_address("192.100.20.1") },
            { 5.0,  inet_address("192.101.20.1") },
            { 6.0,  inet_address("192.102.20.1") },
            { 7.0,  inet_address("192.100.30.1") },
            { 8.0,  inet_address("192.101.30.1") },
            { 9.0,  inet_address("192.102.30.1") },
            { 10.0, inet_address("192.100.40.1") },
            { 11.0, inet_address("192.101.40.1") },
            { 12.0, inet_address("192.102.40.1") },
            { 13.0, inet_address("192.102.40.2") }
    };
    config.options = {
        {"100", "3"},
        {"101", "2"},
        {"102", "2"}
    };
    config.new_dc_rep_factor = {
        {"100", "4"},
        {"101", "3"},
        {"102", "3"},
    };
    config.expected_rep_factor = {
        {"100", 4},
        {"101", 3},
        {"102", 3},
    };

    execute_tablet_for_new_rf_test(config);
}

SEASTAR_THREAD_TEST_CASE(test_calculate_tablet_replicas_for_new_rf_default_rf_upsize_by_two) {
    calculate_tablet_replicas_for_new_rf_config config;
    config.ring_points = {
            { 1.0,  inet_address("192.100.10.1") },
            { 2.0,  inet_address("192.101.10.1") },
            { 3.0,  inet_address("192.102.10.1") },
            { 4.0,  inet_address("192.100.20.1") },
            { 5.0,  inet_address("192.101.20.1") },
            { 6.0,  inet_address("192.102.20.1") },
            { 7.0,  inet_address("192.100.30.1") },
            { 8.0,  inet_address("192.101.30.1") },
            { 9.0,  inet_address("192.102.30.1") },
            { 10.0, inet_address("192.100.40.1") },
            { 11.0, inet_address("192.101.40.1") },
            { 12.0, inet_address("192.102.40.1") },
            { 13.0, inet_address("192.102.40.2") }
    };
    config.options = {
        {"100", "3"},
        {"101", "2"},
        {"102", "1"}
    };
    config.new_dc_rep_factor = {
        {"100", "4"},
        {"101", "3"},
        {"102", "3"},
    };
    config.expected_rep_factor = {
        {"100", 4},
        {"101", 3},
        {"102", 3},
    };

    execute_tablet_for_new_rf_test(config);
}

SEASTAR_TEST_CASE(test_tablet_count_metric) {
    auto cfg = tablet_cql_test_config();
    for (unsigned n = 1; n <= smp::count; n *= 2) {
        cfg.initial_tablets = n;
    }
    return do_with_cql_env_thread([cfg] (cql_test_env& e) {
        auto tid = add_table(e).get();
        auto total = e.db().map_reduce0([&] (replica::database& db) {
            auto count = db.find_column_family(tid).get_stats().tablet_count;
            testlog.debug("shard table_count={}", count);
            return count;
        }, int64_t(0), std::plus<int64_t>()).get();
        BOOST_REQUIRE_EQUAL(total, cfg.initial_tablets);
    }, cfg);
}

SEASTAR_TEST_CASE(test_cleanup_of_deallocated_tablet) {
    auto cfg = tablet_cql_test_config();
    cfg.initial_tablets = 1;

    return do_with_cql_env_thread([](cql_test_env& e) {
        // Create a table.
        e.execute_cql("create table ks.cf (pk int, ck int, primary key (pk, ck))").get();

        size_t all_tablets = 0;
        // Double cleanup the tablet.
        e.db().invoke_on_all([&] (replica::database& db) -> future<> {
            auto& cf = db.find_column_family("ks", "cf");
            auto& sys_ks = e.get_system_keyspace().local();
            auto tablet_count = cf.get_stats().tablet_count;
            all_tablets += tablet_count;
            if (tablet_count > 0) {
                co_await cf.cleanup_tablet(db, sys_ks, locator::tablet_id(0));
                co_await cf.cleanup_tablet(db, sys_ks, locator::tablet_id(0));
            }
        }).get();
        assert(all_tablets);
    }, cfg);
}

namespace {

future<> test_create_keyspace(sstring ks_name, std::optional<bool> tablets_opt, const cql_test_config& cfg, uint64_t initial_tablets = 0) {
    co_await do_with_cql_env_thread([&] (cql_test_env& e) {
        sstring extra;
        if (tablets_opt) {
            if (*tablets_opt) {
                if (initial_tablets) {
                    extra = format(" and tablets = {{ 'initial' : {} }}", initial_tablets);
                } else {
                    extra = " and tablets = { 'enabled' : true }";
                }
            } else {
                extra = " and tablets = { 'enabled' : false }";
            }
        }
        auto q = format("create keyspace {} with replication = {{ 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1 }}{};", ks_name, extra);
        testlog.debug("{}", q);
        e.execute_cql(q).get();
        BOOST_REQUIRE(e.local_db().has_keyspace(ks_name));

        auto tid = add_table(e, ks_name).get();
        auto total = e.db().map_reduce0([&] (replica::database& db) {
            auto count = db.find_column_family(tid).get_stats().tablet_count;
            testlog.debug("shard table_count={}", count);
            return count;
        }, int64_t(0), std::plus<int64_t>()).get();
        if (tablets_opt.value_or(cfg.db_config->enable_tablets())) {
            if (initial_tablets) {
                BOOST_REQUIRE_EQUAL(total, initial_tablets);
            } else {
                BOOST_REQUIRE_GT(total, 0);
            }
        } else {
            BOOST_REQUIRE_EQUAL(total, 0);
        }
    }, cfg);
}

}

// Test that tablets can be explicitly enabled
// when creating a keyspace when the `enable_tablets`
// configuration option is set to `false`.
SEASTAR_TEST_CASE(test_explicit_tablets_enable) {
    auto cfg = tablet_cql_test_config(false);

    // By default tablets are disabled
    co_await test_create_keyspace("test_default_settings", std::nullopt, cfg);

    // Tablets can be explicitly enabled for a new keyspace
    co_await test_create_keyspace("test_explictly_enabled_0", true, cfg, 0);
    co_await test_create_keyspace("test_explictly_enabled_128", true, cfg, 128);

    // Tablets can also be explicitly disabled for a new keyspace
    co_await test_create_keyspace("test_explictly_disabled", false, cfg);
}

// Test that tablets can be explicitly disabled
// when creating a keyspace when the `enable_tablets`
// configuration option is set to `true`.
SEASTAR_TEST_CASE(test_explicit_tablets_disable) {
    auto cfg = tablet_cql_test_config(true);

    // By default tablets are enabled
    co_await test_create_keyspace("test_default_settings", std::nullopt, cfg);

    // Tablets can be explicitly disabled for a new keyspace
    co_await test_create_keyspace("test_explictly_disabled", false, cfg);

    // Tablets can also be explicitly enabled for a new keyspace
    co_await test_create_keyspace("test_explictly_enabled_0", true, cfg, 0);
    co_await test_create_keyspace("test_explictly_enabled_128", true, cfg, 128);
}

SEASTAR_TEST_CASE(test_recognition_of_deprecated_name_for_resize_transition) {
    using transition_state = service::topology::transition_state;
    BOOST_REQUIRE_EQUAL(service::transition_state_from_string("tablet split finalization"), transition_state::tablet_split_finalization);
    BOOST_REQUIRE_EQUAL(service::transition_state_from_string("tablet resize finalization"), transition_state::tablet_resize_finalization);
    return make_ready_future<>();
}
BOOST_AUTO_TEST_SUITE_END()
