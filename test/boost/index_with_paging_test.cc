/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "test/lib/scylla_test_case.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/cql_assertions.hh"
#include "test/lib/eventually.hh"
#include "cql3/untyped_result_set.hh"
#include "cql3/query_processor.hh"
#include "transport/messages/result_message.hh"

SEASTAR_TEST_CASE(test_index_with_paging) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("CREATE TABLE tab (pk int, ck text, v int, v2 int, v3 text, PRIMARY KEY (pk, ck))").get();
        e.execute_cql("CREATE INDEX ON tab (v)").get();

        sstring big_string(100, 'j');
        // There should be enough rows to use multiple pages
        auto prepared_id = e.prepare("INSERT INTO tab (pk, ck, v, v2, v3) VALUES (?, ?, 1, ?, ?)").get();
        auto big_string_v = cql3::raw_value::make_value(serialized(big_string));
        max_concurrent_for_each(boost::irange(0, 64 * 1024), 2, [&] (auto i) {
            return e.execute_prepared(prepared_id, {
                cql3::raw_value::make_value(serialized(i % 3)),                     // pk
                cql3::raw_value::make_value(serialized(format("hello{}", i))),      // ck
                cql3::raw_value::make_value(int32_type->decompose(data_value(i))),  // v2
                big_string_v,                                                       // v3

            }).discard_result();
        }).get();

        e.db().invoke_on_all([] (replica::database& db) {
            // The semaphore's queue has to able to absorb one read / row in this test.
            db.get_reader_concurrency_semaphore().set_max_queue_length(64 * 1024);
        }).get();

        eventually([&] {
            auto qo = std::make_unique<cql3::query_options>(db::consistency_level::LOCAL_ONE, std::vector<cql3::raw_value>{},
                    cql3::query_options::specific_options{4321, nullptr, {}, api::new_timestamp()});
            auto res = e.execute_cql("SELECT * FROM tab WHERE v = 1", std::move(qo)).get();
            auto rows = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(res);
            BOOST_REQUIRE_NE(rows, nullptr);
            // It's fine to get less rows than requested due to paging limit, but never more than that
            BOOST_REQUIRE_LE(rows->rs().get_metadata().column_count(), 4321);
        });

        eventually([&] {
            auto res = e.execute_cql("SELECT * FROM tab WHERE v = 1").get();
            assert_that(res).is_rows().with_size(64 * 1024);
        });
    });
}

SEASTAR_TEST_CASE(test_index_with_paging_with_base_short_read) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("CREATE TABLE tab (pk int, ck text, v int, v2 int, v3 text, PRIMARY KEY (pk, ck))").get();
        e.execute_cql("CREATE INDEX ON tab (v)").get();

        // Enough to trigger a short read on the base table during scan
        sstring big_string(2 * query::result_memory_limiter::maximum_result_size, 'j');

        const int row_count = 67;
        for (int i = 0; i < row_count; ++i) {
            e.execute_cql(format("INSERT INTO tab (pk, ck, v, v2, v3) VALUES ({}, 'hello{}', 1, {}, '{}')", i % 3, i, i, big_string)).get();
        }

        eventually([&] {
            uint64_t count = 0;
            e.qp().local().query_internal("SELECT * FROM ks.tab WHERE v = 1", [&] (const cql3::untyped_result_set_row&) {
                ++count;
                return make_ready_future<stop_iteration>(stop_iteration::no);
            }).get();
            BOOST_REQUIRE_EQUAL(count, row_count);
        });
    });
}

SEASTAR_TEST_CASE(test_index_with_paging_with_base_short_read_no_ck) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("CREATE TABLE tab (pk int, v int, v2 int, v3 text, PRIMARY KEY (pk))").get();
        e.execute_cql("CREATE INDEX ON tab (v)").get();

        // Enough to trigger a short read on the base table during scan
        sstring big_string(2 * query::result_memory_limiter::maximum_result_size, 'j');

        const int row_count = 67;
        for (int i = 0; i < row_count; ++i) {
            e.execute_cql(format("INSERT INTO tab (pk, v, v2, v3) VALUES ({}, 1, {}, '{}')", i, i, big_string)).get();
        }

        eventually([&] {
            uint64_t count = 0;
            e.qp().local().query_internal("SELECT * FROM ks.tab WHERE v = 1", [&] (const cql3::untyped_result_set_row&) {
                ++count;
                return make_ready_future<stop_iteration>(stop_iteration::no);
            }).get();
            BOOST_REQUIRE_EQUAL(count, row_count);
        });
    });
}
