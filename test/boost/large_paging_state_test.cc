/*
 * Copyright (C) 2020-present ScyllaDB
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


#include <boost/test/unit_test.hpp>
#include <seastar/testing/test_case.hh>

#include "test/lib/cql_test_env.hh"
#include "transport/messages/result_message.hh"
#include "types.hh"

static lw_shared_ptr<service::pager::paging_state> extract_paging_state(::shared_ptr<cql_transport::messages::result_message> res) {
    auto rows = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(res);
    auto paging_state = rows->rs().get_metadata().paging_state();
    if (!paging_state) {
        return nullptr;
    }
    return make_lw_shared<service::pager::paging_state>(*paging_state);
};

static size_t count_rows_fetched(::shared_ptr<cql_transport::messages::result_message> res) {
    auto rows = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(res);
    return rows->rs().result_set().size();
};

static bool has_more_pages(::shared_ptr<cql_transport::messages::result_message> res) {
    auto rows = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(res);
    return rows->rs().get_metadata().flags().contains(cql3::metadata::flag::HAS_MORE_PAGES);
};

// While executing a paged select statement, if the bottom 32 bits of the remaining rows number hold a high enough value,
// the returned rows number will be subtracted from just them and the top bits will remain unchanged, so we can't tell
// if they're even being read. We're checking whether the top 32 bits are actually used by setting the bottom bits to 0
// and assuring that the rows are still returned and the top bits are number of returned rows is correctly subtracted
// in the response. 
SEASTAR_TEST_CASE(test_use_high_bits_of_remaining_rows_in_paging_state) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE test (pk int, ck int, PRIMARY KEY (pk, ck));").get();
        auto id = e.prepare("INSERT INTO test (pk, ck) VALUES (?, ?);").get0();
        const auto raw_pk = int32_type->decompose(data_value(0));
        const auto cql3_pk = cql3::raw_value::make_value(raw_pk);

        for (int i = 0; i < 50; i++) {
            const auto cql3_ck = cql3::raw_value::make_value(int32_type->decompose(data_value(i)));
            e.execute_prepared(id, {cql3_pk, cql3_ck}).get();
        }

        auto qo = std::make_unique<cql3::query_options>(db::consistency_level::LOCAL_ONE, std::vector<cql3::raw_value>{},
                    cql3::query_options::specific_options{5, nullptr, {}, api::new_timestamp()});
        auto msg = e.execute_cql("select * from test;", std::move(qo)).get0();
        auto paging_state = extract_paging_state(msg);
        uint64_t rows_fetched = count_rows_fetched(msg);
        BOOST_REQUIRE_EQUAL(paging_state->get_remaining() + rows_fetched, query::max_rows);

        uint64_t test_remaining = paging_state->get_remaining() & ((uint64_t(1) << 32) - 1) << 32;
        BOOST_REQUIRE(test_remaining > std::numeric_limits<uint32_t>::max());
        paging_state->set_remaining(test_remaining);

        while (has_more_pages(msg)) {
            qo = std::make_unique<cql3::query_options>(db::consistency_level::LOCAL_ONE, std::vector<cql3::raw_value>{},
                    cql3::query_options::specific_options{5, paging_state, {}, api::new_timestamp()});
            msg = e.execute_cql("SELECT * FROM test;", std::move(qo)).get0();
            rows_fetched = count_rows_fetched(msg);
            test_remaining = test_remaining - rows_fetched;
            if (has_more_pages(msg)) {
                paging_state = extract_paging_state(msg);
                assert(paging_state);
                BOOST_REQUIRE_EQUAL(test_remaining, paging_state->get_remaining());
            }
        }
    });
}

SEASTAR_TEST_CASE(test_use_high_bits_of_remaining_rows_in_paging_state_filtering) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE test (pk int, ck int, PRIMARY KEY (pk, ck));").get();
        auto id = e.prepare("INSERT INTO test (pk, ck) VALUES (?, ?);").get0();
        const auto raw_pk = int32_type->decompose(data_value(0));
        const auto cql3_pk = cql3::raw_value::make_value(raw_pk);

        for (int i = 0; i < 50; i++) {
            const auto cql3_ck = cql3::raw_value::make_value(int32_type->decompose(data_value(i)));
            e.execute_prepared(id, {cql3_pk, cql3_ck}).get();
        }

        auto qo = std::make_unique<cql3::query_options>(db::consistency_level::LOCAL_ONE, std::vector<cql3::raw_value>{},
                    cql3::query_options::specific_options{5, nullptr, {}, api::new_timestamp()});
        auto msg = e.execute_cql("select * from test where ck > 10;", std::move(qo)).get0();
        auto paging_state = extract_paging_state(msg);
        uint64_t rows_fetched = count_rows_fetched(msg);
        BOOST_REQUIRE_EQUAL(paging_state->get_remaining() + rows_fetched, query::max_rows);

        uint64_t test_remaining = paging_state->get_remaining() & ((uint64_t(1) << 32) - 1) << 32;
        BOOST_REQUIRE(test_remaining > std::numeric_limits<uint32_t>::max());
        paging_state->set_remaining(test_remaining);

        while (has_more_pages(msg)) {
            qo = std::make_unique<cql3::query_options>(db::consistency_level::LOCAL_ONE, std::vector<cql3::raw_value>{},
                    cql3::query_options::specific_options{5, paging_state, {}, api::new_timestamp()});
            msg = e.execute_cql("SELECT * FROM test where ck > 10;", std::move(qo)).get0();
            rows_fetched = count_rows_fetched(msg);
            test_remaining = test_remaining - rows_fetched;
            if (has_more_pages(msg)) {
                paging_state = extract_paging_state(msg);
                assert(paging_state);
                BOOST_REQUIRE_EQUAL(test_remaining, paging_state->get_remaining());
            }
        }
    });
}
