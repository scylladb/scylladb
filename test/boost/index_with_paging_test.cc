/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <seastar/testing/test_case.hh>
#include "test/lib/cql_test_env.hh"
#include "test/lib/cql_assertions.hh"
#include "transport/messages/result_message.hh"

SEASTAR_TEST_CASE(test_index_with_paging) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("CREATE TABLE tab (pk int, ck text, v int, v2 int, v3 text, PRIMARY KEY (pk, ck))").get();
        e.execute_cql("CREATE INDEX ON tab (v)").get();

        sstring big_string(4096, 'j');
        // There should be enough rows to use multiple pages
        for (int i = 0; i < 64 * 1024; ++i) {
            e.execute_cql(format("INSERT INTO tab (pk, ck, v, v2, v3) VALUES ({}, 'hello{}', 1, {}, '{}')", i % 3, i, i, big_string)).get();
        }

        e.db().invoke_on_all([] (database& db) {
            // The semaphore's queue has to able to absorb one read / row in this test.
            db.get_reader_concurrency_semaphore().set_max_queue_length(64 * 1024);
        }).get();

        eventually([&] {
            auto qo = std::make_unique<cql3::query_options>(db::consistency_level::LOCAL_ONE, std::vector<cql3::raw_value>{},
                    cql3::query_options::specific_options{4321, nullptr, {}, api::new_timestamp()});
            auto res = e.execute_cql("SELECT * FROM tab WHERE v = 1", std::move(qo)).get0();
            auto rows = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(res);
            BOOST_REQUIRE_NE(rows, nullptr);
            // It's fine to get less rows than requested due to paging limit, but never more than that
            BOOST_REQUIRE_LE(rows->rs().get_metadata().column_count(), 4321);
        });

        eventually([&] {
            auto res = e.execute_cql("SELECT * FROM tab WHERE v = 1").get0();
            assert_that(res).is_rows().with_size(64 * 1024);
        });
    });
}
