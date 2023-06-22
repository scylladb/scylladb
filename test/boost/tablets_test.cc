/*
 * Copyright (C) 2023-present-2020 ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */



#include "test/lib/scylla_test_case.hh"
#include <seastar/testing/thread_test_case.hh>
#include "test/lib/cql_test_env.hh"
#include "test/lib/log.hh"
#include "db/config.hh"
#include "schema/schema_builder.hh"

#include "replica/tablets.hh"
#include "locator/tablets.hh"
#include "locator/tablet_sharder.hh"
#include "locator/tablet_replication_strategy.hh"
#include "utils/fb_utilities.hh"

using namespace locator;
using namespace replica;

static api::timestamp_type next_timestamp = api::new_timestamp();

static
void verify_tablet_metadata_persistence(cql_test_env& env, const tablet_metadata& tm) {
    save_tablet_metadata(env.local_db(), tm, next_timestamp++).get();
    auto tm2 = read_tablet_metadata(env.local_qp()).get0();
    BOOST_REQUIRE_EQUAL(tm, tm2);
}

static
cql_test_config tablet_cql_test_config() {
    cql_test_config c;
    c.db_config->experimental_features({
            db::experimental_features_t::feature::TABLETS,
        }, db::config::config_source::CommandLine);
    c.db_config->consistent_cluster_management(true);
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

        {
            tablet_metadata tm;

            // Empty
            verify_tablet_metadata_persistence(e, tm);

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

            verify_tablet_metadata_persistence(e, tm);

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

            verify_tablet_metadata_persistence(e, tm);

            // Increase RF of table2
            {
                auto&& tmap = tm.get_tablet_map(table2);
                auto tb = tmap.first_tablet();
                tb = *tmap.next_tablet(tb);

                tmap.set_tablet_transition_info(tb, tablet_transition_info{
                    tablet_replica_set {
                        tablet_replica {h3, 3},
                        tablet_replica {h1, 7},
                    },
                    tablet_replica {h1, 7}
                });

                tb = *tmap.next_tablet(tb);
                tmap.set_tablet_transition_info(tb, tablet_transition_info{
                    tablet_replica_set {
                        tablet_replica {h1, 4},
                        tablet_replica {h2, 2},
                    },
                    tablet_replica {h1, 4}
                });
            }

            verify_tablet_metadata_persistence(e, tm);

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

            verify_tablet_metadata_persistence(e, tm);

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

            verify_tablet_metadata_persistence(e, tm);

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

            verify_tablet_metadata_persistence(e, tm);

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

            verify_tablet_metadata_persistence(e, tm);
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
        tablet_id tid;
        tablet_id tid1;

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

SEASTAR_TEST_CASE(test_sharder) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        auto h1 = host_id(utils::UUID_gen::get_time_UUID());
        auto h2 = host_id(utils::UUID_gen::get_time_UUID());
        auto h3 = host_id(utils::UUID_gen::get_time_UUID());

        auto table1 = table_id(utils::UUID_gen::get_time_UUID());

        token_metadata tokm(token_metadata::config{});
        tokm.get_topology().add_or_update_endpoint(utils::fb_utilities::get_broadcast_address(), h1);

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

        verify_tablet_metadata_persistence(e, tm);
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
