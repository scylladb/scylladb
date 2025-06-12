/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */


#include <boost/test/unit_test.hpp>
#include <stdint.h>

#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>
#include "test/lib/cql_test_env.hh"
#include "test/lib/log.hh"

#include <seastar/core/future-util.hh>
#include <seastar/core/shared_ptr.hh>
#include "cql3/query_processor.hh"
#include "cql3/untyped_result_set.hh"
#include "db/batchlog_manager.hh"
#include "db/commitlog/commitlog.hh"
#include "db/config.hh"
#include "message/messaging_service.hh"
#include "service/storage_proxy.hh"

BOOST_AUTO_TEST_SUITE(batchlog_manager_test)

static atomic_cell make_atomic_cell(data_type dt, bytes value) {
    return atomic_cell::make_live(*dt, 0, std::move(value));
};

SEASTAR_TEST_CASE(test_execute_batch) {
    return do_with_cql_env([] (auto& e) {
        auto& qp = e.local_qp();
        auto& bp =  e.batchlog_manager().local();

        return e.execute_cql("create table cf (p1 varchar, c1 int, r1 int, PRIMARY KEY (p1, c1));").discard_result().then([&qp, &e, &bp] () mutable {
            auto& db = e.local_db();
            auto s = db.find_schema("ks", "cf");

            const column_definition& r1_col = *s->get_column_definition("r1");
            auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
            auto c_key = clustering_key::from_exploded(*s, {int32_type->decompose(1)});

            mutation m(s, key);
            m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type, int32_type->decompose(100)));

            using namespace std::chrono_literals;

            auto version = netw::messaging_service::current_version;
            auto bm = qp.proxy().get_batchlog_mutation_for({ m }, s->id().uuid(), version, db_clock::now() - db_clock::duration(3h));

            return qp.proxy().mutate_locally(bm, tracing::trace_state_ptr(), db::commitlog::force_sync::no).then([&bp] () mutable {
                return bp.count_all_batches().then([](auto n) {
                    BOOST_CHECK_EQUAL(n, 1);
                }).then([&bp] () mutable {
                    return bp.do_batch_log_replay(db::batchlog_manager::post_replay_cleanup::yes);
                });
            });
        }).then([&qp] {
            return qp.execute_internal("select * from ks.cf where p1 = ? and c1 = ?;", { sstring("key1"), 1 }, cql3::query_processor::cache_internal::yes).then([](auto rs) {
                BOOST_REQUIRE(!rs->empty());
                auto i = rs->one().template get_as<int32_t>("r1");
                BOOST_CHECK_EQUAL(i, int32_t(100));
            });
        });
    });
}

SEASTAR_TEST_CASE(test_batchlog_cleanup) {
    cql_test_config cfg;
    cfg.db_config->batchlog_replay_cleanup_after_replays.set_value("9999999", utils::config_file::config_source::Internal);
    cfg.db_config->batchlog_cleanup_with_memtable_rollover.set_value("false", utils::config_file::config_source::CommandLine);

    return do_with_cql_env_thread([] (auto& e) {
        auto& bm = e.batchlog_manager().local();

        e.execute_cql("CREATE TABLE tbl (pk bigint PRIMARY KEY, v text)").get();

        {
            const uint64_t batch_count = 100000;
            const auto before = lowres_clock::now();
            testlog.info("inserting batches");

            for (uint64_t i = 0; i != batch_count; ++i) {
                auto q = format("BEGIN BATCH\n"
                        "INSERT INTO tbl (pk, v) VALUES ({}, 'value');\n"
                        "INSERT INTO tbl (pk, v) VALUES ({}, 'value');\n"
                        "INSERT INTO tbl (pk, v) VALUES ({}, 'value');\n"
                        "INSERT INTO tbl (pk, v) VALUES ({}, 'value');\n"
                        "APPLY BATCH", i, i * 2, i * 3, i * 4);
                e.execute_cql(q).get();
            }

            testlog.info("done inserting {} batches in {} milliseconds", batch_count,
                    std::chrono::duration_cast<std::chrono::milliseconds>(lowres_clock::now() - before).count());
        }

        // Make all tombstones purgeable.
        sleep(std::chrono::seconds(1)).get();

        {
            const auto before = lowres_clock::now();
            testlog.info("starting batchlog cleanup");
            bm.do_batch_log_replay(db::batchlog_manager::post_replay_cleanup::yes).get();
            testlog.info("done batchlog cleanup in {} milliseconds", std::chrono::duration_cast<std::chrono::milliseconds>(lowres_clock::now() - before).count());
        }
    }, cfg);
}

BOOST_AUTO_TEST_SUITE_END()
