/*
 * Copyright (C) 2023-present-2020 ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */



#include "test/lib/scylla_test_case.hh"
#include "test/lib/random_utils.hh"
#include <seastar/testing/thread_test_case.hh>
#include "test/lib/cql_test_env.hh"
#include "test/lib/log.hh"
#include "db/config.hh"
#include "schema/schema_builder.hh"

#include "replica/tablets.hh"
#include "replica/tablet_mutation_builder.hh"
#include "locator/tablets.hh"
#include "service/tablet_allocator.hh"
#include "locator/tablet_sharder.hh"
#include "locator/load_sketch.hh"
#include "locator/tablet_replication_strategy.hh"
#include "utils/UUID_gen.hh"
#include "utils/error_injection.hh"

using namespace locator;
using namespace replica;
using namespace service;

static api::timestamp_type current_timestamp(cql_test_env& e) {
    // Mutations in system.tablets got there via group0, so in order for new
    // mutations to take effect, their timestamp should be "later" than that
    return utils::UUID_gen::micros_timestamp(e.get_system_keyspace().local().get_last_group0_state_id().get0()) + 1;
}

static utils::UUID next_uuid() {
    static uint64_t counter = 1;
    return utils::UUID_gen::get_time_UUID(std::chrono::system_clock::time_point(
            std::chrono::duration_cast<std::chrono::system_clock::duration>(
                    std::chrono::seconds(counter++))));
}

static
void verify_tablet_metadata_persistence(cql_test_env& env, const tablet_metadata& tm, api::timestamp_type& ts) {
    save_tablet_metadata(env.local_db(), tm, ts++).get();
    auto tm2 = read_tablet_metadata(env.local_qp()).get0();
    BOOST_REQUIRE_EQUAL(tm, tm2);
}

static
cql_test_config tablet_cql_test_config() {
    cql_test_config c;
    c.db_config->experimental_features({
            db::experimental_features_t::feature::TABLETS,
        }, db::config::config_source::CommandLine);
    c.initial_tablets = 2;
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

SEASTAR_TEST_CASE(test_tablet_metadata_persistence) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        auto h1 = host_id(utils::UUID_gen::get_time_UUID());
        auto h2 = host_id(utils::UUID_gen::get_time_UUID());
        auto h3 = host_id(utils::UUID_gen::get_time_UUID());

        auto table1 = add_table(e).get0();
        auto table2 = add_table(e).get0();
        auto ts = current_timestamp(e);

        {
            tablet_metadata tm = read_tablet_metadata(e.local_qp()).get0();

            // Add table1
            {
                tablet_map tmap(1);
                tmap.set_tablet(tmap.first_tablet(), tablet_info {
                    tablet_replica_set {
                        tablet_replica {h1, 0},
                        tablet_replica {h2, 3},
                        tablet_replica {h3, 1},
                    }
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
                tm.set_tablet_map(table2, std::move(tmap));
            }

            verify_tablet_metadata_persistence(e, tm, ts);

            // Increase RF of table2
            {
                auto&& tmap = tm.get_tablet_map(table2);
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
            }

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
                tm.set_tablet_map(table1, std::move(tmap));
            }

            verify_tablet_metadata_persistence(e, tm, ts);
        }
    }, tablet_cql_test_config());
}

SEASTAR_TEST_CASE(test_get_shard) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        auto h1 = host_id(utils::UUID_gen::get_time_UUID());
        auto h2 = host_id(utils::UUID_gen::get_time_UUID());
        auto h3 = host_id(utils::UUID_gen::get_time_UUID());

        auto table1 = table_id(utils::UUID_gen::get_time_UUID());

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
            tmap.set_tablet_transition_info(tid, tablet_transition_info {
                tablet_transition_stage::allow_write_both_read_old,
                tablet_transition_kind::migration,
                tablet_replica_set {
                    tablet_replica {h1, 0},
                    tablet_replica {h2, 3},
                },
                tablet_replica {h2, 3}
            });
            tm.set_tablet_map(table1, std::move(tmap));
        }

        auto&& tmap = tm.get_tablet_map(table1);

        BOOST_REQUIRE_EQUAL(tmap.get_shard(tid1, h1), std::make_optional(shard_id(2)));
        BOOST_REQUIRE(!tmap.get_shard(tid1, h2));
        BOOST_REQUIRE_EQUAL(tmap.get_shard(tid1, h3), std::make_optional(shard_id(1)));

        BOOST_REQUIRE_EQUAL(tmap.get_shard(tid, h1), std::make_optional(shard_id(0)));
        BOOST_REQUIRE_EQUAL(tmap.get_shard(tid, h2), std::make_optional(shard_id(3)));
        BOOST_REQUIRE_EQUAL(tmap.get_shard(tid, h3), std::make_optional(shard_id(5)));

    }, tablet_cql_test_config());
}

SEASTAR_TEST_CASE(test_mutation_builder) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        auto h1 = host_id(utils::UUID_gen::get_time_UUID());
        auto h2 = host_id(utils::UUID_gen::get_time_UUID());
        auto h3 = host_id(utils::UUID_gen::get_time_UUID());

        auto table1 = add_table(e).get0();
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

            auto tm_from_disk = read_tablet_metadata(e.local_qp()).get0();
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

            auto tm_from_disk = read_tablet_metadata(e.local_qp()).get0();
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

            auto tm_from_disk = read_tablet_metadata(e.local_qp()).get0();
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
            b.set_resize_decision(resize_decision);
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

            auto tm_from_disk = read_tablet_metadata(e.local_qp()).get0();
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

        token_metadata tokm(token_metadata::config{ .topo_cfg{ .this_host_id = h1 } });
        tokm.get_topology().add_or_update_endpoint(h1, tokm.get_topology().my_address());

        std::vector<tablet_id> tablet_ids;
        {
            tablet_map tmap(4);
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

            tablet_metadata tm;
            tm.set_tablet_map(table1, std::move(tmap));
            tokm.set_tablets(std::move(tm));
        }

        auto& tm = tokm.tablets().get_tablet_map(table1);
        tablet_sharder sharder(tokm, table1);
        BOOST_REQUIRE_EQUAL(sharder.shard_of(tm.get_last_token(tablet_ids[0])), 3);
        BOOST_REQUIRE_EQUAL(sharder.shard_of(tm.get_last_token(tablet_ids[1])), 0); // missing
        BOOST_REQUIRE_EQUAL(sharder.shard_of(tm.get_last_token(tablet_ids[2])), 1);
        BOOST_REQUIRE_EQUAL(sharder.shard_of(tm.get_last_token(tablet_ids[3])), 0); // missing

        BOOST_REQUIRE_EQUAL(sharder.token_for_next_shard(tm.get_last_token(tablet_ids[1]), 0), tm.get_first_token(tablet_ids[3]));
        BOOST_REQUIRE_EQUAL(sharder.token_for_next_shard(tm.get_last_token(tablet_ids[1]), 1), tm.get_first_token(tablet_ids[2]));
        BOOST_REQUIRE_EQUAL(sharder.token_for_next_shard(tm.get_last_token(tablet_ids[1]), 3), dht::maximum_token());

        BOOST_REQUIRE_EQUAL(sharder.token_for_next_shard(tm.get_first_token(tablet_ids[1]), 0), tm.get_first_token(tablet_ids[3]));
        BOOST_REQUIRE_EQUAL(sharder.token_for_next_shard(tm.get_first_token(tablet_ids[1]), 1), tm.get_first_token(tablet_ids[2]));
        BOOST_REQUIRE_EQUAL(sharder.token_for_next_shard(tm.get_first_token(tablet_ids[1]), 3), dht::maximum_token());

        {
            auto shard_opt = sharder.next_shard(tm.get_last_token(tablet_ids[0]));
            BOOST_REQUIRE(shard_opt);
            BOOST_REQUIRE_EQUAL(shard_opt->shard, 0);
            BOOST_REQUIRE_EQUAL(shard_opt->token, tm.get_first_token(tablet_ids[1]));
        }

        {
            auto shard_opt = sharder.next_shard(tm.get_last_token(tablet_ids[1]));
            BOOST_REQUIRE(shard_opt);
            BOOST_REQUIRE_EQUAL(shard_opt->shard, 1);
            BOOST_REQUIRE_EQUAL(shard_opt->token, tm.get_first_token(tablet_ids[2]));
        }

        {
            auto shard_opt = sharder.next_shard(tm.get_last_token(tablet_ids[2]));
            BOOST_REQUIRE(shard_opt);
            BOOST_REQUIRE_EQUAL(shard_opt->shard, 0);
            BOOST_REQUIRE_EQUAL(shard_opt->token, tm.get_first_token(tablet_ids[3]));
        }

        {
            auto shard_opt = sharder.next_shard(tm.get_last_token(tablet_ids[3]));
            BOOST_REQUIRE(!shard_opt);
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

            auto id = add_table(e).get0();
            tm.set_tablet_map(id, std::move(tmap));
        }

        auto ts = current_timestamp(e);
        verify_tablet_metadata_persistence(e, tm, ts);
    }, tablet_cql_test_config());
}

SEASTAR_THREAD_TEST_CASE(test_token_ownership_splitting) {
    const auto real_min_token = dht::token(dht::token_kind::key, std::numeric_limits<int64_t>::min() + 1);
    const auto real_max_token = dht::token(dht::token_kind::key, std::numeric_limits<int64_t>::max());

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
        tablet_map& tmap = tm.tablets().get_tablet_map(table_id);
        resize_decision.sequence_number = tmap.resize_decision().sequence_number + 1;
        tmap.set_resize_decision(resize_decision);
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
        tablet_map& tmap = tm.tablets().get_tablet_map(mig.tablet.table);
        auto tinfo = tmap.get_tablet_info(mig.tablet.tablet);
        tinfo.replicas = replace_replica(tinfo.replicas, mig.src, mig.dst);
        tmap.set_tablet(mig.tablet.tablet, tinfo);
    }
    apply_resize_plan(tm, plan);
}

// Reflects the plan in a given token metadata as if the migrations were started but not yet executed.
static
void apply_plan_as_in_progress(token_metadata& tm, const migration_plan& plan) {
    for (auto&& mig : plan.migrations()) {
        tablet_map& tmap = tm.tablets().get_tablet_map(mig.tablet.table);
        auto tinfo = tmap.get_tablet_info(mig.tablet.tablet);
        tmap.set_tablet_transition_info(mig.tablet.tablet, migration_to_transition_info(tinfo, mig));
    }
    apply_resize_plan(tm, plan);
}

static
void rebalance_tablets(tablet_allocator& talloc, shared_token_metadata& stm, locator::load_stats_ptr load_stats = {}) {
    while (true) {
        auto plan = talloc.balance_tablets(stm.get(), load_stats).get0();
        if (plan.empty()) {
            break;
        }
        stm.mutate_token_metadata([&] (token_metadata& tm) {
            apply_plan(tm, plan);
            return make_ready_future<>();
        }).get();
    }
}

static
void rebalance_tablets_as_in_progress(tablet_allocator& talloc, shared_token_metadata& stm) {
    while (true) {
        auto plan = talloc.balance_tablets(stm.get()).get0();
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
            auto& tmap = tmap_;
            for (auto&& [tablet, trinfo]: tmap.transitions()) {
                auto ti = tmap.get_tablet_info(tablet);
                ti.replicas = trinfo.next;
                tmap.set_tablet(tablet, ti);
            }
            tmap.clear_transitions();
        }
        return make_ready_future<>();
    }).get();
}

SEASTAR_THREAD_TEST_CASE(test_load_balancing_with_empty_node) {
  do_with_cql_env_thread([] (auto& e) {
    // Tests the scenario of bootstrapping a single node
    // Verifies that load balancer sees it and moves tablets to that node.

    inet_address ip1("192.168.0.1");
    inet_address ip2("192.168.0.2");
    inet_address ip3("192.168.0.3");

    auto host1 = host_id(next_uuid());
    auto host2 = host_id(next_uuid());
    auto host3 = host_id(next_uuid());

    auto table1 = table_id(next_uuid());

    unsigned shard_count = 2;

    semaphore sem(1);
    shared_token_metadata stm([&sem] () noexcept { return get_units(sem, 1); }, locator::token_metadata::config{
        locator::topology::config{
            .this_endpoint = ip1,
            .local_dc_rack = locator::endpoint_dc_rack::default_location
        }
    });

    stm.mutate_token_metadata([&] (token_metadata& tm) {
        tm.update_host_id(host1, ip1);
        tm.update_host_id(host2, ip2);
        tm.update_host_id(host3, ip3);
        tm.update_topology(host1, locator::endpoint_dc_rack::default_location, std::nullopt, shard_count);
        tm.update_topology(host2, locator::endpoint_dc_rack::default_location, std::nullopt, shard_count);
        tm.update_topology(host3, locator::endpoint_dc_rack::default_location, std::nullopt, shard_count);

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
        tablet_metadata tmeta;
        tmeta.set_tablet_map(table1, std::move(tmap));
        tm.set_tablets(std::move(tmeta));
        return make_ready_future<>();
    }).get();

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

    rebalance_tablets(e.get_tablet_allocator().local(), stm);

    {
        load_sketch load(stm.get());
        load.populate().get();

        for (auto h : {host1, host2, host3}) {
            testlog.debug("Checking host {}", h);
            BOOST_REQUIRE(load.get_load(h) <= 3);
            BOOST_REQUIRE(load.get_load(h) > 1);
            BOOST_REQUIRE(load.get_avg_shard_load(h) <= 2);
            BOOST_REQUIRE(load.get_avg_shard_load(h) > 0);
        }
    }
  }).get();
}

SEASTAR_THREAD_TEST_CASE(test_decommission_rf_met) {
    // Verifies that load balancer moves tablets out of the decommissioned node.
    // The scenario is such that replication factor of tablets can be satisfied after decommission.
    do_with_cql_env_thread([](auto& e) {
        inet_address ip1("192.168.0.1");
        inet_address ip2("192.168.0.2");
        inet_address ip3("192.168.0.3");

        auto host1 = host_id(next_uuid());
        auto host2 = host_id(next_uuid());
        auto host3 = host_id(next_uuid());

        auto table1 = table_id(next_uuid());

        semaphore sem(1);
        shared_token_metadata stm([&sem]() noexcept { return get_units(sem, 1); }, locator::token_metadata::config {
                locator::topology::config {
                        .this_endpoint = ip1,
                        .local_dc_rack = locator::endpoint_dc_rack::default_location
                }
        });

        stm.mutate_token_metadata([&](token_metadata& tm) {
            const unsigned shard_count = 2;

            tm.update_host_id(host1, ip1);
            tm.update_host_id(host2, ip2);
            tm.update_host_id(host3, ip3);
            tm.update_topology(host1, locator::endpoint_dc_rack::default_location, std::nullopt, shard_count);
            tm.update_topology(host2, locator::endpoint_dc_rack::default_location, std::nullopt, shard_count);
            tm.update_topology(host3, locator::endpoint_dc_rack::default_location, node::state::being_decommissioned,
                               shard_count);

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
            tablet_metadata tmeta;
            tmeta.set_tablet_map(table1, std::move(tmap));
            tm.set_tablets(std::move(tmeta));
            return make_ready_future<>();
        }).get();

        rebalance_tablets(e.get_tablet_allocator().local(), stm);

        {
            load_sketch load(stm.get());
            load.populate().get();
            BOOST_REQUIRE(load.get_avg_shard_load(host1) == 2);
            BOOST_REQUIRE(load.get_avg_shard_load(host2) == 2);
            BOOST_REQUIRE(load.get_avg_shard_load(host3) == 0);
        }

        stm.mutate_token_metadata([&](token_metadata& tm) {
            tm.update_topology(host3, locator::endpoint_dc_rack::default_location, node::state::left);
            return make_ready_future<>();
        }).get();

        rebalance_tablets(e.get_tablet_allocator().local(), stm);

        {
            load_sketch load(stm.get());
            load.populate().get();
            BOOST_REQUIRE(load.get_avg_shard_load(host1) == 2);
            BOOST_REQUIRE(load.get_avg_shard_load(host2) == 2);
            BOOST_REQUIRE(load.get_avg_shard_load(host3) == 0);
        }
    }).get();
}

SEASTAR_THREAD_TEST_CASE(test_decommission_two_racks) {
    // Verifies that load balancer moves tablets out of the decommissioned node.
    // The scenario is such that replication constraints of tablets can be satisfied after decommission.
    do_with_cql_env_thread([](auto& e) {
        inet_address ip1("192.168.0.1");
        inet_address ip2("192.168.0.2");
        inet_address ip3("192.168.0.3");
        inet_address ip4("192.168.0.4");

        auto host1 = host_id(next_uuid());
        auto host2 = host_id(next_uuid());
        auto host3 = host_id(next_uuid());
        auto host4 = host_id(next_uuid());

        std::vector<endpoint_dc_rack> racks = {
                endpoint_dc_rack{ "dc1", "rack-1" },
                endpoint_dc_rack{ "dc1", "rack-2" }
        };

        auto table1 = table_id(next_uuid());

        semaphore sem(1);
        shared_token_metadata stm([&sem]() noexcept { return get_units(sem, 1); }, locator::token_metadata::config {
                locator::topology::config {
                        .this_endpoint = ip1,
                        .local_dc_rack = racks[0]
                }
        });

        stm.mutate_token_metadata([&](token_metadata& tm) {
            const unsigned shard_count = 1;

            tm.update_host_id(host1, ip1);
            tm.update_host_id(host2, ip2);
            tm.update_host_id(host3, ip3);
            tm.update_host_id(host4, ip4);
            tm.update_topology(host1, racks[0], std::nullopt, shard_count);
            tm.update_topology(host2, racks[1], std::nullopt, shard_count);
            tm.update_topology(host3, racks[0], std::nullopt, shard_count);
            tm.update_topology(host4, racks[1], node::state::being_decommissioned,
                               shard_count);

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
            tablet_metadata tmeta;
            tmeta.set_tablet_map(table1, std::move(tmap));
            tm.set_tablets(std::move(tmeta));
            return make_ready_future<>();
        }).get();

        rebalance_tablets(e.get_tablet_allocator().local(), stm);

        {
            load_sketch load(stm.get());
            load.populate().get();
            BOOST_REQUIRE(load.get_avg_shard_load(host1) >= 2);
            BOOST_REQUIRE(load.get_avg_shard_load(host2) >= 2);
            BOOST_REQUIRE(load.get_avg_shard_load(host3) >= 2);
            BOOST_REQUIRE(load.get_avg_shard_load(host4) == 0);
        }

        // Verify replicas are not collocated on racks
        {
            auto tm = stm.get();
            auto& tmap = tm->tablets().get_tablet_map(table1);
            tmap.for_each_tablet([&](auto tid, auto& tinfo) -> future<> {
                auto rack1 = tm->get_topology().get_rack(tinfo.replicas[0].host);
                auto rack2 = tm->get_topology().get_rack(tinfo.replicas[1].host);
                BOOST_REQUIRE(rack1 != rack2);
                co_return;
            }).get();
        }
    }).get();
}

SEASTAR_THREAD_TEST_CASE(test_decommission_rack_load_failure) {
    // Verifies that load balancer moves tablets out of the decommissioned node.
    // The scenario is such that it is impossible to distribute replicas without violating rack uniqueness.
    do_with_cql_env_thread([](auto& e) {
        inet_address ip1("192.168.0.1");
        inet_address ip2("192.168.0.2");
        inet_address ip3("192.168.0.3");
        inet_address ip4("192.168.0.4");

        auto host1 = host_id(next_uuid());
        auto host2 = host_id(next_uuid());
        auto host3 = host_id(next_uuid());
        auto host4 = host_id(next_uuid());

        std::vector<endpoint_dc_rack> racks = {
                endpoint_dc_rack{ "dc1", "rack-1" },
                endpoint_dc_rack{ "dc1", "rack-2" }
        };

        auto table1 = table_id(next_uuid());

        semaphore sem(1);
        shared_token_metadata stm([&sem]() noexcept { return get_units(sem, 1); }, locator::token_metadata::config {
                locator::topology::config {
                        .this_endpoint = ip1,
                        .local_dc_rack = racks[0]
                }
        });

        stm.mutate_token_metadata([&](token_metadata& tm) {
            const unsigned shard_count = 1;

            tm.update_host_id(host1, ip1);
            tm.update_host_id(host2, ip2);
            tm.update_host_id(host3, ip3);
            tm.update_host_id(host4, ip4);
            tm.update_topology(host1, racks[0], std::nullopt, shard_count);
            tm.update_topology(host2, racks[0], std::nullopt, shard_count);
            tm.update_topology(host3, racks[0], std::nullopt, shard_count);
            tm.update_topology(host4, racks[1], node::state::being_decommissioned,
                               shard_count);

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
            tablet_metadata tmeta;
            tmeta.set_tablet_map(table1, std::move(tmap));
            tm.set_tablets(std::move(tmeta));
            return make_ready_future<>();
        }).get();

        BOOST_REQUIRE_THROW(rebalance_tablets(e.get_tablet_allocator().local(), stm), std::runtime_error);
    }).get();
}

SEASTAR_THREAD_TEST_CASE(test_decommission_rf_not_met) {
    // Verifies that load balancer moves tablets out of the decommissioned node.
    // The scenario is such that replication factor of tablets can be satisfied after decommission.
    do_with_cql_env_thread([](auto& e) {
        inet_address ip1("192.168.0.1");
        inet_address ip2("192.168.0.2");
        inet_address ip3("192.168.0.3");

        auto host1 = host_id(next_uuid());
        auto host2 = host_id(next_uuid());
        auto host3 = host_id(next_uuid());

        auto table1 = table_id(next_uuid());

        semaphore sem(1);
        shared_token_metadata stm([&sem]() noexcept { return get_units(sem, 1); }, locator::token_metadata::config {
                locator::topology::config {
                        .this_endpoint = ip1,
                        .local_dc_rack = locator::endpoint_dc_rack::default_location
                }
        });

        stm.mutate_token_metadata([&](token_metadata& tm) {
            const unsigned shard_count = 2;

            tm.update_host_id(host1, ip1);
            tm.update_host_id(host2, ip2);
            tm.update_host_id(host3, ip3);
            tm.update_topology(host1, locator::endpoint_dc_rack::default_location, std::nullopt, shard_count);
            tm.update_topology(host2, locator::endpoint_dc_rack::default_location, std::nullopt, shard_count);
            tm.update_topology(host3, locator::endpoint_dc_rack::default_location, node::state::being_decommissioned,
                               shard_count);

            tablet_map tmap(1);
            auto tid = tmap.first_tablet();
            tmap.set_tablet(tid, tablet_info {
                    tablet_replica_set {
                            tablet_replica {host1, 0},
                            tablet_replica {host2, 0},
                            tablet_replica {host3, 0},
                    }
            });
            tablet_metadata tmeta;
            tmeta.set_tablet_map(table1, std::move(tmap));
            tm.set_tablets(std::move(tmeta));
            return make_ready_future<>();
        }).get();

        BOOST_REQUIRE_THROW(rebalance_tablets(e.get_tablet_allocator().local(), stm), std::runtime_error);
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

    inet_address ip1("192.168.0.1");
    inet_address ip2("192.168.0.2");
    inet_address ip3("192.168.0.3");

    auto host1 = host_id(next_uuid());
    auto host2 = host_id(next_uuid());
    auto host3 = host_id(next_uuid());

    auto table1 = table_id(next_uuid());

    semaphore sem(1);
    shared_token_metadata stm([&sem] () noexcept { return get_units(sem, 1); }, locator::token_metadata::config{
        locator::topology::config{
            .this_endpoint = ip1,
            .local_dc_rack = locator::endpoint_dc_rack::default_location
        }
    });

    stm.mutate_token_metadata([&] (token_metadata& tm) {
        tm.update_host_id(host1, ip1);
        tm.update_host_id(host2, ip2);
        tm.update_host_id(host3, ip3);
        tm.update_topology(host1, locator::endpoint_dc_rack::default_location, std::nullopt, 1);
        tm.update_topology(host2, locator::endpoint_dc_rack::default_location, std::nullopt, 1);
        tm.update_topology(host3, locator::endpoint_dc_rack::default_location, std::nullopt, 2);

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
        tablet_metadata tmeta;
        tmeta.set_tablet_map(table1, std::move(tmap));
        tm.set_tablets(std::move(tmeta));
        return make_ready_future<>();
    }).get();

    rebalance_tablets_as_in_progress(e.get_tablet_allocator().local(), stm);
    execute_transitions(stm);

    {
        load_sketch load(stm.get());
        load.populate().get();

        for (auto h : {host1, host2, host3}) {
            testlog.debug("Checking host {}", h);
            BOOST_REQUIRE(load.get_avg_shard_load(h) == 2);
        }
    }
  }).get();
}

#ifdef SCYLLA_ENABLE_ERROR_INJECTION
SEASTAR_THREAD_TEST_CASE(test_load_balancer_shuffle_mode) {
  do_with_cql_env_thread([] (auto& e) {
    inet_address ip1("192.168.0.1");
    inet_address ip2("192.168.0.2");
    inet_address ip3("192.168.0.3");

    auto host1 = host_id(next_uuid());
    auto host2 = host_id(next_uuid());
    auto host3 = host_id(next_uuid());

    auto table1 = table_id(next_uuid());

    semaphore sem(1);
    shared_token_metadata stm([&sem] () noexcept { return get_units(sem, 1); }, locator::token_metadata::config{
        locator::topology::config{
            .this_endpoint = ip1,
            .local_dc_rack = locator::endpoint_dc_rack::default_location
        }
    });

    stm.mutate_token_metadata([&] (token_metadata& tm) {
        tm.update_host_id(host1, ip1);
        tm.update_host_id(host2, ip2);
        tm.update_host_id(host3, ip3);
        tm.update_topology(host1, locator::endpoint_dc_rack::default_location, std::nullopt, 1);
        tm.update_topology(host2, locator::endpoint_dc_rack::default_location, std::nullopt, 1);
        tm.update_topology(host3, locator::endpoint_dc_rack::default_location, std::nullopt, 2);

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
        tablet_metadata tmeta;
        tmeta.set_tablet_map(table1, std::move(tmap));
        tm.set_tablets(std::move(tmeta));
        return make_ready_future<>();
    }).get();

    rebalance_tablets(e.get_tablet_allocator().local(), stm);

    BOOST_REQUIRE(e.get_tablet_allocator().local().balance_tablets(stm.get()).get0().empty());

    utils::get_local_injector().enable("tablet_allocator_shuffle");
    auto disable_injection = seastar::defer([&] {
        utils::get_local_injector().disable("tablet_allocator_shuffle");
    });

    BOOST_REQUIRE(!e.get_tablet_allocator().local().balance_tablets(stm.get()).get0().empty());
  }).get();
}
#endif

SEASTAR_THREAD_TEST_CASE(test_load_balancing_with_two_empty_nodes) {
  do_with_cql_env_thread([] (auto& e) {
    inet_address ip1("192.168.0.1");
    inet_address ip2("192.168.0.2");
    inet_address ip3("192.168.0.3");
    inet_address ip4("192.168.0.4");

    auto host1 = host_id(next_uuid());
    auto host2 = host_id(next_uuid());
    auto host3 = host_id(next_uuid());
    auto host4 = host_id(next_uuid());

    auto table1 = table_id(next_uuid());

    unsigned shard_count = 2;

    semaphore sem(1);
    shared_token_metadata stm([&sem] () noexcept { return get_units(sem, 1); }, locator::token_metadata::config{
        locator::topology::config{
            .this_endpoint = ip1,
            .local_dc_rack = locator::endpoint_dc_rack::default_location
        }
    });

    stm.mutate_token_metadata([&] (token_metadata& tm) {
        tm.update_host_id(host1, ip1);
        tm.update_host_id(host2, ip2);
        tm.update_host_id(host3, ip3);
        tm.update_host_id(host4, ip4);
        tm.update_topology(host1, locator::endpoint_dc_rack::default_location, std::nullopt, shard_count);
        tm.update_topology(host2, locator::endpoint_dc_rack::default_location, std::nullopt, shard_count);
        tm.update_topology(host3, locator::endpoint_dc_rack::default_location, std::nullopt, shard_count);
        tm.update_topology(host4, locator::endpoint_dc_rack::default_location, std::nullopt, shard_count);

        tablet_map tmap(16);
        for (auto tid : tmap.tablet_ids()) {
            tmap.set_tablet(tid, tablet_info {
                tablet_replica_set {
                    tablet_replica {host1, tests::random::get_int<shard_id>(0, shard_count - 1)},
                    tablet_replica {host2, tests::random::get_int<shard_id>(0, shard_count - 1)},
                }
            });
        }
        tablet_metadata tmeta;
        tmeta.set_tablet_map(table1, std::move(tmap));
        tm.set_tablets(std::move(tmeta));
        return make_ready_future<>();
    }).get();

    rebalance_tablets(e.get_tablet_allocator().local(), stm);

    {
        load_sketch load(stm.get());
        load.populate().get();

        for (auto h : {host1, host2, host3, host4}) {
            testlog.debug("Checking host {}", h);
            BOOST_REQUIRE(load.get_avg_shard_load(h) == 4);
        }
    }
  }).get();
}

SEASTAR_THREAD_TEST_CASE(test_load_balancer_disabling) {
    do_with_cql_env_thread([] (auto& e) {
        inet_address ip1("192.168.0.1");
        inet_address ip2("192.168.0.2");

        auto host1 = host_id(next_uuid());
        auto host2 = host_id(next_uuid());

        auto table1 = table_id(next_uuid());

        unsigned shard_count = 1;

        semaphore sem(1);
        shared_token_metadata stm([&sem] () noexcept { return get_units(sem, 1); }, locator::token_metadata::config{
            locator::topology::config{
                .this_endpoint = ip1,
                .local_dc_rack = locator::endpoint_dc_rack::default_location
            }
        });

        // host1 is loaded and host2 is empty, resulting in an imbalance.
        stm.mutate_token_metadata([&] (auto& tm) {
            tm.update_host_id(host1, ip1);
            tm.update_host_id(host2, ip2);
            tm.update_topology(host1, locator::endpoint_dc_rack::default_location, std::nullopt, shard_count);
            tm.update_topology(host2, locator::endpoint_dc_rack::default_location, std::nullopt, shard_count);

            tablet_map tmap(16);
            for (auto tid : tmap.tablet_ids()) {
                tmap.set_tablet(tid, tablet_info {
                    tablet_replica_set {
                        tablet_replica {host1, 0},
                    }
                });
            }
            tablet_metadata tmeta;
            tmeta.set_tablet_map(table1, std::move(tmap));
            tm.set_tablets(std::move(tmeta));
            return make_ready_future<>();
        }).get();

        {
            auto plan = e.get_tablet_allocator().local().balance_tablets(stm.get()).get0();
            BOOST_REQUIRE(!plan.empty());
        }

        // Disable load balancing
        stm.mutate_token_metadata([&] (token_metadata& tm) {
            tm.tablets().set_balancing_enabled(false);
            return make_ready_future<>();
        }).get();

        {
            auto plan = e.get_tablet_allocator().local().balance_tablets(stm.get()).get0();
            BOOST_REQUIRE(plan.empty());
        }

        // Check that cloning preserves the setting
        stm.mutate_token_metadata([&] (token_metadata& tm) {
            return make_ready_future<>();
        }).get();

        {
            auto plan = e.get_tablet_allocator().local().balance_tablets(stm.get()).get0();
            BOOST_REQUIRE(plan.empty());
        }

        // Enable load balancing back
        stm.mutate_token_metadata([&] (token_metadata& tm) {
            tm.tablets().set_balancing_enabled(true);
            return make_ready_future<>();
        }).get();

        {
            auto plan = e.get_tablet_allocator().local().balance_tablets(stm.get()).get0();
            BOOST_REQUIRE(!plan.empty());
        }

        // Check that cloning preserves the setting
        stm.mutate_token_metadata([&] (token_metadata& tm) {
            return make_ready_future<>();
        }).get();

        {
            auto plan = e.get_tablet_allocator().local().balance_tablets(stm.get()).get0();
            BOOST_REQUIRE(!plan.empty());
        }
  }).get();
}

SEASTAR_THREAD_TEST_CASE(test_load_balancing_with_random_load) {
  do_with_cql_env_thread([] (auto& e) {
    const int n_hosts = 6;

    std::vector<host_id> hosts;
    for (int i = 0; i < n_hosts; ++i) {
        hosts.push_back(host_id(next_uuid()));
    }

    std::vector<endpoint_dc_rack> racks = {
        endpoint_dc_rack{ "dc1", "rack-1" },
        endpoint_dc_rack{ "dc1", "rack-2" }
    };

    for (int i = 0; i < 13; ++i) {
        std::unordered_map<sstring, std::vector<host_id>> hosts_by_rack;

        semaphore sem(1);
        shared_token_metadata stm([&sem]() noexcept { return get_units(sem, 1); }, locator::token_metadata::config {
                locator::topology::config {
                        .this_endpoint = inet_address("192.168.0.1"),
                        .this_host_id = hosts[0],
                        .local_dc_rack = racks[1]
                }
        });

        size_t total_tablet_count = 0;
        stm.mutate_token_metadata([&](token_metadata& tm) {
            tablet_metadata tmeta;

            int i = 0;
            for (auto h : hosts) {
                auto ip = inet_address(format("192.168.0.{}", ++i));
                auto shard_count = 2;
                tm.update_host_id(h, ip);
                auto rack = racks[i % racks.size()];
                tm.update_topology(h, rack, std::nullopt, shard_count);
                if (h != hosts[0]) {
                    // Leave the first host empty by making it invisible to allocation algorithm.
                    hosts_by_rack[rack.rack].push_back(h);
                }
            }

            size_t tablet_count_bits = 8;
            int rf = tests::random::get_int<shard_id>(2, 4);
            for (size_t log2_tablets = 0; log2_tablets < tablet_count_bits; ++log2_tablets) {
                if (tests::random::get_bool()) {
                    continue;
                }
                auto table = table_id(next_uuid());
                tablet_map tmap(1 << log2_tablets);
                for (auto tid : tmap.tablet_ids()) {
                    // Choose replicas randomly while loading racks evenly.
                    std::vector<host_id> replica_hosts;
                    for (int i = 0; i < rf; ++i) {
                        auto rack = racks[i % racks.size()];
                        auto& rack_hosts = hosts_by_rack[rack.rack];
                        while (true) {
                            auto candidate_host = rack_hosts[tests::random::get_int<shard_id>(0, rack_hosts.size() - 1)];
                            if (std::find(replica_hosts.begin(), replica_hosts.end(), candidate_host) == replica_hosts.end()) {
                                replica_hosts.push_back(candidate_host);
                                break;
                            }
                        }
                    }
                    tablet_replica_set replicas;
                    for (auto h : replica_hosts) {
                        auto shard_count = tm.get_topology().find_node(h)->get_shard_count();
                        auto shard = tests::random::get_int<shard_id>(0, shard_count - 1);
                        replicas.push_back(tablet_replica {h, shard});
                    }
                    tmap.set_tablet(tid, tablet_info {std::move(replicas)});
                }
                total_tablet_count += tmap.tablet_count();
                tmeta.set_tablet_map(table, std::move(tmap));
            }
            tm.set_tablets(std::move(tmeta));
            return make_ready_future<>();
        }).get();

        testlog.debug("tablet metadata: {}", stm.get()->tablets());
        testlog.info("Total tablet count: {}, hosts: {}", total_tablet_count, hosts.size());

        rebalance_tablets(e.get_tablet_allocator().local(), stm);

        {
            load_sketch load(stm.get());
            load.populate().get();

            min_max_tracker<unsigned> min_max_load;
            for (auto h: hosts) {
                auto l = load.get_avg_shard_load(h);
                testlog.info("Load on host {}: {}", h, l);
                min_max_load.update(l);
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
    }
  }).get();
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

        for (int i = 0; i < smp::count * 20; i++) {
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
            return table.split_all_storage_groups();
        }).get();

        testlog.info("Verifying sstables are split...");
        BOOST_REQUIRE_EQUAL(e.db().map_reduce0([] (replica::database& db) {
            auto& table = db.find_column_family("ks", "cf");
            return make_ready_future<bool>(table.all_storage_groups_split());
        }, bool(false), std::logical_or<bool>()).get0(), true);
    }, std::move(cfg)).get();
}

SEASTAR_THREAD_TEST_CASE(test_load_balancing_resize_requests) {
    do_with_cql_env_thread([] (auto& e) {
        inet_address ip1("192.168.0.1");
        inet_address ip2("192.168.0.2");

        auto host1 = host_id(next_uuid());
        auto host2 = host_id(next_uuid());

        auto table1 = table_id(next_uuid());

        unsigned shard_count = 2;

        semaphore sem(1);
        shared_token_metadata stm([&sem] () noexcept { return get_units(sem, 1); }, locator::token_metadata::config{
                locator::topology::config{
                        .this_endpoint = ip1,
                        .local_dc_rack = locator::endpoint_dc_rack::default_location
                }
        });

        stm.mutate_token_metadata([&] (token_metadata& tm) {
            tm.update_host_id(host1, ip1);
            tm.update_host_id(host2, ip2);
            tm.update_topology(host1, locator::endpoint_dc_rack::default_location, std::nullopt, shard_count);
            tm.update_topology(host2, locator::endpoint_dc_rack::default_location, std::nullopt, shard_count);

            tablet_map tmap(2);
            for (auto tid : tmap.tablet_ids()) {
                tmap.set_tablet(tid, tablet_info {
                        tablet_replica_set {
                                tablet_replica {host1, tests::random::get_int<shard_id>(0, shard_count - 1)},
                                tablet_replica {host2, tests::random::get_int<shard_id>(0, shard_count - 1)},
                        }
                });
            }
            tablet_metadata tmeta;
            tmeta.set_tablet_map(table1, std::move(tmap));
            tm.set_tablets(std::move(tmeta));
            return make_ready_future<>();
        }).get();

        auto tablet_count = [&] {
            return stm.get()->tablets().get_tablet_map(table1).tablet_count();
        };
        auto resize_decision = [&] {
            return stm.get()->tablets().get_tablet_map(table1).resize_decision();
        };

        auto do_rebalance_tablets = [&] (locator::load_stats load_stats) {
            rebalance_tablets(e.get_tablet_allocator().local(), stm, make_lw_shared(std::move(load_stats)));
        };

        const size_t initial_tablets = tablet_count();
        const uint64_t max_tablet_size = service::default_target_tablet_size * 2;
        auto to_size_in_bytes = [&] (double max_tablet_size_pctg) -> uint64_t {
            return (max_tablet_size * max_tablet_size_pctg) * tablet_count();
        };


        const auto initial_ready_seq_number = std::numeric_limits<locator::resize_decision::seq_number_t>::min();

        // there are 2 tablets, each with avg size hitting merge threshold, so merge request is emitted
        {
            locator::load_stats load_stats = {
                .tables = {
                    { table1, table_load_stats{ .size_in_bytes = to_size_in_bytes(0.0), .split_ready_seq_number = initial_ready_seq_number }},
                }
            };

            do_rebalance_tablets(std::move(load_stats));
            BOOST_REQUIRE(tablet_count() == initial_tablets);
            BOOST_REQUIRE(std::holds_alternative<locator::resize_decision::merge>(resize_decision().way));
        }

        // avg size moved above target size, so merge is cancelled
        {
            locator::load_stats load_stats = {
                .tables = {
                    { table1, table_load_stats{ .size_in_bytes = to_size_in_bytes(0.75), .split_ready_seq_number = initial_ready_seq_number }},
                }
            };

            do_rebalance_tablets(std::move(load_stats));
            BOOST_REQUIRE(tablet_count() == initial_tablets);
            BOOST_REQUIRE(std::holds_alternative<locator::resize_decision::none>(resize_decision().way));
        }

        // avg size hits split threshold, and balancer emits split request
        {
            locator::load_stats load_stats = {
                .tables = {
                    { table1, table_load_stats{ .size_in_bytes = to_size_in_bytes(1.1), .split_ready_seq_number = initial_ready_seq_number }},
                }
            };

            do_rebalance_tablets(std::move(load_stats));
            BOOST_REQUIRE(tablet_count() == initial_tablets);
            BOOST_REQUIRE(std::holds_alternative<locator::resize_decision::split>(resize_decision().way));
            BOOST_REQUIRE(resize_decision().sequence_number > 0);
        }

        // replicas set their split status as ready, and load balancer finalizes split generating a new
        // tablet map, twice as large as the previous one.
        {
            locator::load_stats load_stats = {
                .tables = {
                    { table1, table_load_stats{ .size_in_bytes = to_size_in_bytes(1.1), .split_ready_seq_number = resize_decision().sequence_number }},
                }
            };

            do_rebalance_tablets(std::move(load_stats));

            BOOST_REQUIRE(tablet_count() == initial_tablets * 2);
            BOOST_REQUIRE(std::holds_alternative<locator::resize_decision::none>(resize_decision().way));
        }
    }).get();
}
