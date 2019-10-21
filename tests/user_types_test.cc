/*
 * Copyright (C) 2019 ScyllaDB
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
#include <seastar/testing/thread_test_case.hh>
#include "tests/cql_test_env.hh"
#include "tests/cql_assertions.hh"

#include "types/user.hh"
#include "types/list.hh"
#include "exception_utils.hh"

SEASTAR_TEST_CASE(test_user_type_nested) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("create type ut1 (f1 int);").get();
        e.execute_cql("create type ut2 (f2 frozen<ut1>);").get();
    });
}

SEASTAR_TEST_CASE(test_user_type_reversed) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        e.execute_cql("create type my_type (a int);").get();
        e.execute_cql("create table tbl (a int, b frozen<my_type>, primary key ((a), b)) with clustering order by (b desc);").get();
        e.execute_cql("insert into tbl (a, b) values (1, (2));").get();
        assert_that(e.execute_cql("select a,b.a from tbl;").get0())
                .is_rows()
                .with_size(1)
                .with_row({int32_type->decompose(1), int32_type->decompose(2)});
    });
}

SEASTAR_TEST_CASE(test_user_type) {
    return do_with_cql_env([] (cql_test_env& e) {
        return e.execute_cql("create type ut1 (my_int int, my_bigint bigint, my_text text);").discard_result().then([&e] {
            return e.execute_cql("create table cf (id int primary key, t frozen <ut1>);").discard_result();
        }).then([&e] {
            return e.execute_cql("insert into cf (id, t) values (1, (1001, 2001, 'abc1'));").discard_result();
        }).then([&e] {
            return e.execute_cql("select t.my_int, t.my_bigint, t.my_text from cf where id = 1;");
        }).then([&e] (shared_ptr<cql_transport::messages::result_message> msg) {
            assert_that(msg).is_rows()
                .with_rows({
                     {int32_type->decompose(int32_t(1001)), long_type->decompose(int64_t(2001)), utf8_type->decompose(sstring("abc1"))},
                });
        }).then([&e] {
            return e.execute_cql("update cf set t = { my_int: 1002, my_bigint: 2002, my_text: 'abc2' } where id = 1;").discard_result();
        }).then([&e] {
            return e.execute_cql("select t.my_int, t.my_bigint, t.my_text from cf where id = 1;");
        }).then([&e] (shared_ptr<cql_transport::messages::result_message> msg) {
            assert_that(msg).is_rows()
                .with_rows({
                     {int32_type->decompose(int32_t(1002)), long_type->decompose(int64_t(2002)), utf8_type->decompose(sstring("abc2"))},
                });
        }).then([&e] {
            return e.execute_cql("insert into cf (id, t) values (2, (frozen<ut1>)(2001, 3001, 'abc4'));").discard_result();
        }).then([&e] {
            return e.execute_cql("select t from cf where id = 2;");
        }).then([&e] (shared_ptr<cql_transport::messages::result_message> msg) {
            auto ut = user_type_impl::get_instance("ks", to_bytes("ut1"),
                        {to_bytes("my_int"), to_bytes("my_bigint"), to_bytes("my_text")},
                        {int32_type, long_type, utf8_type}, false);
            auto ut_val = make_user_value(ut,
                          user_type_impl::native_type({int32_t(2001),
                                                       int64_t(3001),
                                                       sstring("abc4")}));
            assert_that(msg).is_rows()
                .with_rows({
                     {ut->decompose(ut_val)},
                });
        });
    });
}
