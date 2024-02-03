/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */


#include <boost/range/irange.hpp>
#include <boost/range/adaptors.hpp>
#include <boost/range/algorithm.hpp>
#include <boost/test/unit_test.hpp>
#include <stdint.h>

#include "test/lib/scylla_test_case.hh"
#include "test/lib/cql_test_env.hh"

#include <seastar/core/future-util.hh>
#include <seastar/core/shared_ptr.hh>
#include "cql3/query_processor.hh"
#include "cql3/untyped_result_set.hh"
#include "db/batchlog_manager.hh"
#include "db/commitlog/commitlog.hh"
#include "message/messaging_service.hh"
#include "service/storage_proxy.hh"

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
                    return bp.do_batch_log_replay();
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

