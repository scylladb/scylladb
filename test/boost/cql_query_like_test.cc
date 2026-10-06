/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */


#include <boost/test/unit_test.hpp>
#include <boost/multiprecision/cpp_int.hpp>

#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include "test/lib/cql_test_env.hh"
#include "test/lib/cql_assertions.hh"

#include <seastar/core/future-util.hh>
#include "test/lib/exception_utils.hh"

BOOST_AUTO_TEST_SUITE(cql_query_like_test)

using namespace std::literals::chrono_literals;

SEASTAR_TEST_CASE(test_like_operator_on_token) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        cquery_nofail(e, "create table t (s text primary key)");
        BOOST_REQUIRE_EXCEPTION(
                e.execute_cql("select * from t where token(s) like 'abc' allow filtering").get(),
                exceptions::invalid_request_exception,
                exception_predicate::message_contains("token function"));
    });
}

BOOST_AUTO_TEST_SUITE_END()
