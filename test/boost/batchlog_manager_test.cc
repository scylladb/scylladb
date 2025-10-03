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
#include "test/lib/cql_assertions.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/log.hh"

#include <seastar/core/future-util.hh>
#include <seastar/core/shared_ptr.hh>
#include "cql3/statements/batch_statement.hh"
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
                    return bp.do_batch_log_replay(db::batchlog_manager::post_replay_cleanup::yes).discard_result();
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

    return do_with_cql_env_thread([] (cql_test_env& env) -> void {
        auto& bm = env.batchlog_manager().local();

        env.execute_cql("CREATE TABLE tbl (pk bigint PRIMARY KEY, v text)").get();

        const uint64_t batch_count = 8;

        for (uint64_t i = 0; i != batch_count; ++i) {
            std::vector<sstring> queries;
            std::vector<std::string_view> query_views;
            for (uint64_t j = 0; j != i+2; ++j) {
                queries.emplace_back(format("INSERT INTO tbl (pk, v) VALUES ({}, 'value');", j));
                query_views.emplace_back(queries.back());
            }
            env.execute_batch(
                    query_views,
                    cql3::statements::batch_statement::type::LOGGED,
                    std::make_unique<cql3::query_options>(db::consistency_level::ONE, std::vector<cql3::raw_value>())).get();
        }

        const auto fragments_query = "SELECT mutation_source FROM MUTATION_FRAGMENTS(system.batchlog) WHERE partition_region = 0 ALLOW FILTERING";

        assert_that(env.execute_cql("SELECT id FROM system.batchlog").get())
            .is_rows()
            .is_empty();
        assert_that(env.execute_cql(fragments_query).get())
            .is_rows()
            .with_size(batch_count)
            .assert_for_columns_of_each_row([] (columns_assertions& columns) {
                columns.with_typed_column<sstring>("mutation_source", "memtable:0");
            });

        bm.do_batch_log_replay(db::batchlog_manager::post_replay_cleanup::no).get();

        assert_that(env.execute_cql(fragments_query).get())
            .is_rows()
            .with_size(batch_count)
            .assert_for_columns_of_each_row([] (columns_assertions& columns) {
                columns.with_typed_column<sstring>("mutation_source", "memtable:0");
            });

        // Make all tombstones purgeable.
        // Batchlog table tombston GC settings are hardcoded and the table is a local one too,
        // so we cannot work around sleeps with using repair-mode here.
        sleep(std::chrono::seconds(1)).get();

        bm.do_batch_log_replay(db::batchlog_manager::post_replay_cleanup::yes).get();

        assert_that(env.execute_cql(fragments_query).get())
            .is_rows()
            .is_empty();

    }, cfg);
}

future<> run_batchlog_cleanup_with_failed_batches_test(bool replay_fails, db::batchlog_manager::post_replay_cleanup cleanup) {
#ifndef SCYLLA_ENABLE_ERROR_INJECTION
    return make_ready_future<>();
#endif

    cql_test_config cfg;
    cfg.db_config->batchlog_replay_cleanup_after_replays.set_value("9999999", utils::config_file::config_source::Internal);

    return do_with_cql_env_thread([=] (cql_test_env& env) -> void {
        auto& bm = env.batchlog_manager().local();

        env.execute_cql("CREATE TABLE tbl (pk bigint PRIMARY KEY, v text)").get();

        const uint64_t batch_count = 8;
        uint64_t failed_batches = 0;

        for (uint64_t i = 0; i != batch_count; ++i) {
            std::vector<sstring> queries;
            std::vector<std::string_view> query_views;
            for (uint64_t j = 0; j != i+2; ++j) {
                queries.emplace_back(format("INSERT INTO tbl (pk, v) VALUES ({}, 'value');", j));
                query_views.emplace_back(queries.back());
            }
            const bool fail = i % 2;
            bool injected_exception_thrown = false;

            if (fail) {
                ++failed_batches;
                utils::get_local_injector().enable("storage_proxy_fail_send_batch");
            }
            try {
                env.execute_batch(
                        query_views,
                        cql3::statements::batch_statement::type::LOGGED,
                        std::make_unique<cql3::query_options>(db::consistency_level::ONE, std::vector<cql3::raw_value>())).get();
            } catch (std::runtime_error& ex) {
                if (fail) {
                    BOOST_REQUIRE_EQUAL(std::string(ex.what()), "Error injection: failing to send batch");
                    injected_exception_thrown = true;
                } else {
                    throw;
                }
            }
            BOOST_REQUIRE_EQUAL(injected_exception_thrown, fail);

            utils::get_local_injector().disable("storage_proxy_fail_send_batch");
        }

        const auto fragments_query = "SELECT mutation_source FROM MUTATION_FRAGMENTS(system.batchlog) WHERE partition_region = 0 ALLOW FILTERING";

        assert_that(env.execute_cql("SELECT id FROM system.batchlog").get())
            .is_rows()
            .with_size(failed_batches);

        assert_that(env.execute_cql(fragments_query).get())
            .is_rows()
            .with_size(batch_count)
            .assert_for_columns_of_each_row([&] (columns_assertions& columns) {
                columns.with_typed_column<sstring>("mutation_source", "memtable:0");
            });

        if (replay_fails) {
            smp::invoke_on_all([] {
                utils::get_local_injector().enable("storage_proxy_fail_replay_batch");
            }).get();
        }

        bm.do_batch_log_replay(cleanup).get();

        assert_that(env.execute_cql("SELECT id FROM system.batchlog").get())
            .is_rows()
            .with_size(replay_fails ? failed_batches : 0);

        assert_that(env.execute_cql(fragments_query).get())
            .is_rows()
            .with_size(cleanup
                    ? (replay_fails ? failed_batches : 0)
                    : batch_count)
            .assert_for_columns_of_each_row([&] (columns_assertions& columns) {
                columns.with_typed_column<sstring>("mutation_source", [&] (const sstring& source) {
                    return cleanup || source == "memtable:0";
                });
            });

        smp::invoke_on_all([] {
            utils::get_local_injector().disable("storage_proxy_fail_replay_batch");
        }).get();
    }, cfg);
}

SEASTAR_TEST_CASE(test_batchlog_replay_fails_no_cleanup) {
    return run_batchlog_cleanup_with_failed_batches_test(true, db::batchlog_manager::post_replay_cleanup::no);
}

SEASTAR_TEST_CASE(test_batchlog_replay_fails_with_cleanup) {
    return run_batchlog_cleanup_with_failed_batches_test(true, db::batchlog_manager::post_replay_cleanup::yes);
}

SEASTAR_TEST_CASE(test_batchlog_replay_no_cleanup) {
    return run_batchlog_cleanup_with_failed_batches_test(false, db::batchlog_manager::post_replay_cleanup::no);
}

SEASTAR_TEST_CASE(test_batchlog_replay_with_cleanup) {
    return run_batchlog_cleanup_with_failed_batches_test(false, db::batchlog_manager::post_replay_cleanup::yes);
}

BOOST_AUTO_TEST_SUITE_END()
