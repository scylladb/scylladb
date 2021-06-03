/*
 * Copyright (C) 2015-present ScyllaDB
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


#include <boost/range/irange.hpp>
#include <boost/range/adaptors.hpp>
#include <boost/range/algorithm.hpp>
#include <boost/test/unit_test.hpp>
#include <boost/multiprecision/cpp_int.hpp>
#include <experimental/source_location>

#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include "test/lib/cql_test_env.hh"
#include "test/lib/cql_assertions.hh"

#include <seastar/core/future-util.hh>
#include "types/user.hh"
#include "types/map.hh"
#include "types/list.hh"
#include "types/set.hh"
#include "test/lib/exception_utils.hh"
#include "schema_builder.hh"

using namespace std::literals::chrono_literals;

namespace {

auto T(const char* t) { return utf8_type->decompose(t); }

} // anonymous namespace

SEASTAR_TEST_CASE(test_like_operator) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        cquery_nofail(e, "create table t (p int primary key, s text)");
        require_rows(e, "select s from t where s like 'abc' allow filtering", {});
        cquery_nofail(e, "insert into t (p, s) values (1, 'abc')");
        require_rows(e, "select s from t where s like 'abc' allow filtering", {{T("abc")}});
        require_rows(e, "select s from t where s like 'ab_' allow filtering", {{T("abc")}});
        cquery_nofail(e, "insert into t (p, s) values (2, 'abb')");
        require_rows(e, "select s from t where s like 'ab_' allow filtering", {{T("abc")}, {T("abb")}});
        require_rows(e, "select s from t where s like '%c' allow filtering", {{T("abc")}});
        require_rows(e, "select s from t where s like 'aaa' allow filtering", {});
    });
}

SEASTAR_TEST_CASE(test_like_operator_on_partition_key) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        // Fully constrained:
        cquery_nofail(e, "create table t (s text primary key)");
        cquery_nofail(e, "insert into t (s) values ('abc')");
        require_rows(e, "select s from t where s like 'a__' allow filtering", {{T("abc")}});
        cquery_nofail(e, "insert into t (s) values ('acc')");
        require_rows(e, "select s from t where s like 'a__' allow filtering", {{T("abc")}, {T("acc")}});

        // Partially constrained:
        cquery_nofail(e, "create table t2 (s1 text, s2 text, primary key((s1, s2)))");
        cquery_nofail(e, "insert into t2 (s1, s2) values ('abc', 'abc')");
        require_rows(e, "select s2 from t2 where s2 like 'a%' allow filtering", {{T("abc")}});
        cquery_nofail(e, "insert into t2 (s1, s2) values ('aba', 'aba')");
        require_rows(e, "select s2 from t2 where s2 like 'a%' allow filtering", {{T("abc")}, {T("aba")}});
    });
}

SEASTAR_TEST_CASE(test_like_operator_on_clustering_key) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        cquery_nofail(e, "create table t (p int, s text, primary key(p, s))");
        cquery_nofail(e, "insert into t (p, s) values (1, 'abc')");
        require_rows(e, "select s from t where s like '%c' allow filtering", {{T("abc")}});
        cquery_nofail(e, "insert into t (p, s) values (2, 'acc')");
        require_rows(e, "select s from t where s like '%c' allow filtering", {{T("abc")}, {T("acc")}});
        cquery_nofail(e, "insert into t (p, s) values (2, 'acd')");
        require_rows(e, "select s from t where p = 2 and s like '%c' allow filtering", {{T("acc")}});
    });
}

SEASTAR_TEST_CASE(test_like_operator_conjunction) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        cquery_nofail(e, "create table t (s1 text primary key, s2 text)");
        cquery_nofail(e, "insert into t (s1, s2) values ('abc', 'ABC')");
        cquery_nofail(e, "insert into t (s1, s2) values ('a', 'A')");
        require_rows(e, "select * from t where s1 like 'a%' and s2 like '__C' allow filtering",
                     {{T("abc"), T("ABC")}});
        require_rows(e, "select * from t where s1 like 'a%' and s1 like '__C' allow filtering", {});
        require_rows(e, "select s1 from t where s1 like 'a%' and s1 like '_' allow filtering", {{T("a")}});
        require_rows(e, "select s1 from t where s1 like 'a%' and s1 like '%' allow filtering", {{T("a")}, {T("abc")}});
        require_rows(e, "select s1 from t where s1 like 'a%' and s1 like '_b_' and s1 like '%c' allow filtering",
                     {{T("abc")}});
        require_rows(e, "select s1 from t where s1 like 'a%' and s1 = 'abc' allow filtering", {{T("abc")}});
    });
}

SEASTAR_TEST_CASE(test_like_operator_static_column) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        cquery_nofail(e, "create table t (p int, c text, s text static, primary key(p, c))");
        require_rows(e, "select s from t where s like '%c' allow filtering", {});
        cquery_nofail(e, "insert into t (p, s) values (1, 'abc')");
        require_rows(e, "select s from t where s like '%c' allow filtering", {{T("abc")}});
        require_rows(e, "select * from t where c like '%' allow filtering", {});
    });
}

SEASTAR_TEST_CASE(test_like_operator_bind_marker) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        cquery_nofail(e, "create table t (s text primary key, )");
        cquery_nofail(e, "insert into t (s) values ('abc')");
        auto stmt = e.prepare("select s from t where s like ? allow filtering").get0();
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
