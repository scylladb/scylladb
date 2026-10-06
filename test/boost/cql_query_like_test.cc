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

namespace {

auto T(const char* t) { return utf8_type->decompose(t); }

} // anonymous namespace

SEASTAR_TEST_CASE(test_like_operator_bind_marker) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        cquery_nofail(e, "create table t (s text primary key, )");
        cquery_nofail(e, "insert into t (s) values ('abc')");
        auto stmt = e.prepare("select s from t where s like ? allow filtering").get();
        require_rows(e, stmt, {cql3::raw_value::make_value(T("_b_"))}, {{T("abc")}});
        require_rows(e, stmt, {cql3::raw_value::make_value(T("%g"))}, {});
        require_rows(e, stmt, {cql3::raw_value::make_value(T("%c"))}, {{T("abc")}});
    });
}

SEASTAR_TEST_CASE(test_like_operator_blank_pattern) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        cquery_nofail(e, "create table t (p int primary key, s text)");
        cquery_nofail(e, "insert into t (p, s) values (1, 'abc')");
        require_rows(e, "select s from t where s like '' allow filtering", {});
        cquery_nofail(e, "insert into t (p, s) values (2, '')");
        require_rows(e, "select s from t where s like '' allow filtering", {{T("")}});
    });
}

SEASTAR_TEST_CASE(test_like_operator_ascii) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        cquery_nofail(e, "create table t (s ascii primary key, )");
        cquery_nofail(e, "insert into t (s) values ('abc')");
        require_rows(e, "select s from t where s like '%c' allow filtering", {{T("abc")}});
    });
}

SEASTAR_TEST_CASE(test_like_operator_varchar) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        cquery_nofail(e, "create table t (s varchar primary key, )");
        cquery_nofail(e, "insert into t (s) values ('abc')");
        require_rows(e, "select s from t where s like '%c' allow filtering", {{T("abc")}});
    });
}

namespace {

/// Asserts that a column of type \p type cannot be LHS of the LIKE operator.
auto assert_like_doesnt_accept(const char* type) {
    return do_with_cql_env_thread([type] (cql_test_env& e) {
        cquery_nofail(e, format("create table t (k {}, p int primary key)", type).c_str());
        BOOST_REQUIRE_EXCEPTION(
                e.execute_cql("select * from t where k like 123 allow filtering").get(),
                exceptions::invalid_request_exception,
                exception_predicate::message_contains("only on string types"));
    });
}

} // anonymous namespace

SEASTAR_TEST_CASE(test_like_operator_fails_on_bigint)    { return assert_like_doesnt_accept("bigint");    }
SEASTAR_TEST_CASE(test_like_operator_fails_on_blob)      { return assert_like_doesnt_accept("blob");      }
SEASTAR_TEST_CASE(test_like_operator_fails_on_boolean)   { return assert_like_doesnt_accept("boolean");   }
SEASTAR_TEST_CASE(test_like_operator_fails_on_counter)   { return assert_like_doesnt_accept("counter");   }
SEASTAR_TEST_CASE(test_like_operator_fails_on_decimal)   { return assert_like_doesnt_accept("decimal");   }
SEASTAR_TEST_CASE(test_like_operator_fails_on_double)    { return assert_like_doesnt_accept("double");    }
SEASTAR_TEST_CASE(test_like_operator_fails_on_duration)  { return assert_like_doesnt_accept("duration");  }
SEASTAR_TEST_CASE(test_like_operator_fails_on_float)     { return assert_like_doesnt_accept("float");     }
SEASTAR_TEST_CASE(test_like_operator_fails_on_inet)      { return assert_like_doesnt_accept("inet");      }
SEASTAR_TEST_CASE(test_like_operator_fails_on_int)       { return assert_like_doesnt_accept("int");       }
SEASTAR_TEST_CASE(test_like_operator_fails_on_smallint)  { return assert_like_doesnt_accept("smallint");  }
SEASTAR_TEST_CASE(test_like_operator_fails_on_timestamp) { return assert_like_doesnt_accept("timestamp"); }
SEASTAR_TEST_CASE(test_like_operator_fails_on_tinyint)   { return assert_like_doesnt_accept("tinyint");   }
SEASTAR_TEST_CASE(test_like_operator_fails_on_uuid)      { return assert_like_doesnt_accept("uuid");      }
SEASTAR_TEST_CASE(test_like_operator_fails_on_varint)    { return assert_like_doesnt_accept("varint");    }
SEASTAR_TEST_CASE(test_like_operator_fails_on_timeuuid)  { return assert_like_doesnt_accept("timeuuid");  }
SEASTAR_TEST_CASE(test_like_operator_fails_on_date)      { return assert_like_doesnt_accept("date");      }
SEASTAR_TEST_CASE(test_like_operator_fails_on_time)      { return assert_like_doesnt_accept("time");      }

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
