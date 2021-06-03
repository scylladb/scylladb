/*
 * Copyright (C) 2019-present ScyllaDB
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
#include "test/lib/cql_test_env.hh"
#include "test/lib/cql_assertions.hh"

#include "types/user.hh"
#include "types/list.hh"
#include "test/lib/exception_utils.hh"

#include <boost/algorithm/string/join.hpp>

// Specifies that the given 'cql' query fails with the 'msg' message.
// Requires a cql_test_env. The caller must be inside thread.
#define REQUIRE_INVALID(e, cql, msg) \
    BOOST_REQUIRE_EXCEPTION( \
        e.execute_cql(cql).get(), \
        exceptions::invalid_request_exception, \
        exception_predicate::message_equals(msg))

static void flush(cql_test_env& e) {
    e.db().invoke_on_all([](database& dbi) {
        return dbi.flush_all_memtables();
    }).get();
}

template <typename F>
static void before_and_after_flush(cql_test_env& e, F f) {
    f();
    flush(e);
    f();
}

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

SEASTAR_TEST_CASE(test_invalid_user_type_statements) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("create type ut1 (a int)").discard_result().get();

        // non-frozen UDTs can't be part of primary key
        REQUIRE_INVALID(e, "create table bad (a ut1 primary key, b int)",
                "Invalid non-frozen user-defined type for PRIMARY KEY component a");
        REQUIRE_INVALID(e, "create table bad (a int, b ut1, c int, primary key (a, b))",
                "Invalid non-frozen user-defined type for PRIMARY KEY component b");

        // non-frozen UDTs can't be inside collections, in create table statements...
        REQUIRE_INVALID(e, "create table bad (a int primary key, b list<ut1>)",
                "Non-frozen user types or collections are not allowed inside collections: list<ut1>");
        REQUIRE_INVALID(e, "create table bad (a int primary key, b set<ut1>)",
                "Non-frozen user types or collections are not allowed inside collections: set<ut1>");
        REQUIRE_INVALID(e, "create table bad (a int primary key, b map<int, ut1>)",
                "Non-frozen user types or collections are not allowed inside collections: map<int, ut1>");
        REQUIRE_INVALID(e, "create table bad (a int primary key, b map<ut1, int>)",
                "Non-frozen user types or collections are not allowed inside collections: map<ut1, int>");
        // ... and in user type definitions
        REQUIRE_INVALID(e, "create type ut2 (a int, b list<ut1>)",
                "Non-frozen user types or collections are not allowed inside collections: list<ut1>");
        //
        // non-frozen UDTs can't be inside UDTs
        REQUIRE_INVALID(e, "create type ut2 (a int, b ut1)",
                "A user type cannot contain non-frozen user type fields");

        // table cannot refer to UDT in another keyspace
        e.execute_cql("create keyspace ks2 with replication={'class':'SimpleStrategy','replication_factor':1}").discard_result().get();
        e.execute_cql("create type ks2.ut2 (a int)").discard_result().get();
        REQUIRE_INVALID(e, "create table bad (a int primary key, b ks2.ut2)",
                "Statement on keyspace ks cannot refer to a user type in keyspace ks2; "
                "user types can only be used in the keyspace they are defined in");
        REQUIRE_INVALID(e, "create table bad (a int primary key, b frozen<ks2.ut2>)",
                "Statement on keyspace ks cannot refer to a user type in keyspace ks2; "
                "user types can only be used in the keyspace they are defined in");

        // can't reference non-existing UDT
        REQUIRE_INVALID(e, "create table bad (a int primary key, b ut2)",
                "Unknown type ks.ut2");

        // can't delete fields of frozen UDT or non-UDT columns
        e.execute_cql("create table cf1 (a int primary key, b frozen<ut1>, c int)").discard_result().get();
        REQUIRE_INVALID(e, "delete b.a from cf1 where a = 0",
                "Frozen UDT column b does not support field deletions");
        REQUIRE_INVALID(e, "delete c.a from cf1 where a = 0",
                "Invalid deletion operation for non-UDT column c");

        // can't update fields of frozen UDT or non-UDT columns
        REQUIRE_INVALID(e, "update cf1 set b.a = 0 where a = 0",
                "Invalid operation (b.a = 0) for frozen UDT column b");
        REQUIRE_INVALID(e, "update cf1 set c.a = 0 where a = 0",
                "Invalid operation (c.a = 0) for non-UDT column c");

        // can't delete non-existing fields of UDT columns
        e.execute_cql("create table cf2 (a int primary key, b ut1, c int)").discard_result().get();
        REQUIRE_INVALID(e, "delete b.foo from cf2 where a = 0",
                "UDT column b does not have a field named foo");

        // can't update non-existing fields of UDT columns
        REQUIRE_INVALID(e, "update cf2 set b.foo = 0 where a = 0",
                "UDT column b does not have a field named foo");

        // can't insert UDT with non-existing fields
        REQUIRE_INVALID(e, "insert into cf2 (a, b, c) VALUES (0, {a:0,foo:0}, 0)",
                "Unknown field 'foo' in value of user defined type ut1");
        REQUIRE_INVALID(e, "insert into cf2 (a, b, c) VALUES (0, (0, 0), 0)",
                "Invalid tuple literal for b: too many elements. Type ut1 expects 1 but got 2");

        // non-frozen UDTs can't contain non-frozen collections
        e.execute_cql("create type ut3 (a int, b list<int>)").discard_result().get();
        REQUIRE_INVALID(e, "create table bad (a int primary key, b ut3)",
                "Non-frozen UDTs with nested non-frozen collections are not supported");

        // cannot have too many fields inside UDTs
        REQUIRE_INVALID(e, format("create type ut4 ({})", boost::algorithm::join(
                boost::irange(0, 1 << 15) | boost::adaptors::transformed([] (int i) { return format("a{} int", i); }), ", ")),
                format("A user type cannot have more than {} fields", (1 << 15) - 1));

        e.execute_cql(format("create type ut4 ({})", boost::algorithm::join(
                boost::irange(1, 1 << 15) | boost::adaptors::transformed([] (int i) { return format("a{} int", i); }), ", "))).discard_result().get();
        REQUIRE_INVALID(e, "alter type ut4 add b int",
                "Cannot add new field to type ks.ut4: maximum number of fields reached");
    });
}

static future<> test_alter_user_type(bool frozen) {
    return do_with_cql_env_thread([frozen] (cql_test_env& e) {
        const sstring val1 = "1";
        const sstring val2 = "22";
        const sstring val3 = "333";
        const sstring val4 = "4444";

        e.execute_cql("create type ut (b text)").discard_result().get();
        e.execute_cql(format("create table cf (a int primary key, b {})", frozen ? "frozen<ut>" : "ut")).discard_result().get();

        e.execute_cql("insert into cf (a, b) values (1, {b:'1'})").discard_result().get();

        assert_that(e.execute_cql("select b.b from cf").get0()).is_rows().with_rows_ignore_order({
                {{utf8_type->decompose(val1)}},
        });

        e.execute_cql("alter type ut add a int").discard_result().get();
        e.execute_cql("insert into cf (a, b) values (2, {a:2,b:'22'})").discard_result().get();

        auto ut = user_type_impl::get_instance("ks", to_bytes("ut"),
                    {to_bytes("b"), to_bytes("a")}, {utf8_type, int32_type}, !frozen);

        auto mk_ut = [&] (const std::vector<data_value>& vs) {
            return ut->decompose(make_user_value(ut, user_type_impl::native_type(vs)));
        };

        auto mk_int = [] (int x) { return int32_type->decompose(x); };

        auto int_null = data_value::make_null(int32_type);
        auto text_null = data_value::make_null(utf8_type);

        assert_that(e.execute_cql("select * from cf").get0()).is_rows().with_rows_ignore_order({
            {mk_int(1), (frozen ? mk_ut({val1}) : mk_ut({val1, int_null}))},
            {mk_int(2), mk_ut({val2, 2})},
        });

        assert_that(e.execute_cql("select * from cf where b={b:'1'} allow filtering").get0()).is_rows().with_rows_ignore_order({
            {mk_int(1), (frozen ? mk_ut({val1}) : mk_ut({val1, int_null}))},
        });

        assert_that(e.execute_cql("select b.a from cf").get0()).is_rows().with_rows_ignore_order({
            {{}},
            {mk_int(2)},
        });

        flush(e);

        e.execute_cql("alter type ut add c int").discard_result().get();

        ut = user_type_impl::get_instance("ks", to_bytes("ut"),
                    {to_bytes("b"), to_bytes("a"), to_bytes("c")}, {utf8_type, int32_type, int32_type}, !frozen);

        assert_that(e.execute_cql("select * from cf").get0()).is_rows().with_rows_ignore_order({
            {mk_int(1), (frozen ? mk_ut({val1}) : mk_ut({val1, int_null, int_null}))},
            {mk_int(2), (frozen ? mk_ut({val2, 2}) : mk_ut({val2, 2, int_null}))},
        });

        e.execute_cql("alter type ut rename b to foo").discard_result().get();

        ut = user_type_impl::get_instance("ks", to_bytes("ut"),
                    {to_bytes("foo"), to_bytes("a"), to_bytes("c")}, {utf8_type, int32_type, int32_type}, !frozen);

        e.execute_cql("insert into cf (a, b) values (3, ('333', 3, 3))").discard_result().get();
        e.execute_cql("insert into cf (a, b) values (4, {foo:'4444',c:4})").discard_result().get();

        before_and_after_flush(e, [&] {
            assert_that(e.execute_cql("select * from cf").get0()).is_rows().with_rows_ignore_order({
                {mk_int(1), (frozen ? mk_ut({val1}) : mk_ut({val1, int_null, int_null}))},
                {mk_int(2), (frozen ? mk_ut({val2, 2}) : mk_ut({val2, 2, int_null}))},
                {mk_int(3), (frozen ? mk_ut({val3, 3, 3}) : mk_ut({val3, 3, 3}))},
                {mk_int(4), mk_ut({val4, int_null, 4})},
            });

            assert_that(e.execute_cql("select b.foo from cf").get0()).is_rows().with_rows_ignore_order({
                {utf8_type->decompose(val1)},
                {utf8_type->decompose(val2)},
                {utf8_type->decompose(val3)},
                {utf8_type->decompose(val4)},
            });
        });
    });
}

SEASTAR_TEST_CASE(test_alter_frozen_user_type) {
    return test_alter_user_type(true);
}

SEASTAR_TEST_CASE(test_alter_nonfrozen_user_type) {
    return test_alter_user_type(false);
}

future<> test_user_type_insert_delete(bool frozen) {
    return do_with_cql_env_thread([frozen] (cql_test_env& e) {
        auto ut = user_type_impl::get_instance("ks", to_bytes("ut"),
                    {to_bytes("a"), to_bytes("b"), to_bytes("c")},
                    {int32_type, utf8_type, long_type}, !frozen);

        auto int_null = data_value::make_null(int32_type);
        auto text_null = data_value::make_null(utf8_type);
        auto long_null = data_value::make_null(long_type);

        e.execute_cql("create type ut (a int, b text, c bigint)").discard_result().get();
        e.execute_cql(format("create table cf (a int primary key, b {})", frozen ? "frozen<ut>" : "ut")).discard_result().get();

        e.execute_cql("insert into cf (a, b) values (1, {a:1, b:'text1', c:1})").discard_result().get();
        e.execute_cql("insert into cf (a, b) values (2, {a:2, c:2})").discard_result().get();
        e.execute_cql("insert into cf (a, b) values (3, {b:'text3', c:3})").discard_result().get();
        e.execute_cql("insert into cf (a, b) values (4, {a:4, b:'text4'})").discard_result().get();
        e.execute_cql("insert into cf (a, b) values (5, null)").discard_result().get();
        e.execute_cql("insert into cf (a, b) values (6, {a:null})").discard_result().get();
        e.execute_cql("insert into cf (a, b) values (7, (7))").discard_result().get();
        e.execute_cql("insert into cf (a, b) values (8, (8, null, 8))").discard_result().get();
        e.execute_cql("insert into cf (a, b) values (9, (9, 'text9'))").discard_result().get();

        auto msg = e.execute_cql("select * from cf").get0();

        {
        auto mk_row = [&] (int k, const std::vector<data_value>& vs) -> std::vector<bytes_opt> {
            return {int32_type->decompose(k), ut->decompose(make_user_value(ut, user_type_impl::native_type(vs)))};
        };

        auto mk_null_row = [&] (int k) -> std::vector<bytes_opt> {
            return {int32_type->decompose(k), {}};
        };

        before_and_after_flush(e, [&] {
            assert_that(msg).is_rows().with_rows_ignore_order({
                mk_row(1, {1, "text1", int64_t(1)}),
                mk_row(2, {2, text_null, int64_t(2)}),
                mk_row(3, {int_null, "text3", int64_t(3)}),
                mk_row(4, {4, "text4", long_null}),
                mk_null_row(5),
                (frozen ? mk_row(6, {int_null, text_null, long_null}) : mk_null_row(6)),
                (frozen ? mk_row(7, {7}) : mk_row(7, {7, text_null, long_null})),
                mk_row(8, {8, text_null,  int64_t(8)}),
                (frozen ? mk_row(9, {9, "text9"}) : mk_row(9, {9, "text9", long_null})),
            });
        });
        }

        msg = e.execute_cql("select b.b from cf").get0();

        {
        auto mk_row = [&] (const data_value& v) -> std::vector<bytes_opt> {
            return {utf8_type->decompose(v)};
        };

        before_and_after_flush(e, [&] {
            assert_that(msg).is_rows().with_rows_ignore_order({
                mk_row("text1"),
                {{}},
                mk_row("text3"),
                mk_row("text4"),
                {{}},
                {{}},
                {{}},
                {{}},
                mk_row("text9"),
            });
        });
        }

        e.execute_cql("delete b from cf where a in (1,2,3,4,5,6,7,8,9)").discard_result().get();

        msg = e.execute_cql("select b.b from cf").get0();
        before_and_after_flush(e, [&] {
            assert_that(msg).is_rows().with_rows_ignore_order({
                {{}}, {{}}, {{}}, {{}}, {{}}, {{}}, {{}}, {{}}, {{}},
            });
        });
    });
}

SEASTAR_TEST_CASE(test_frozen_user_type_insert_delete) {
    return test_user_type_insert_delete(true);
}

SEASTAR_TEST_CASE(test_nonfrozen_user_type_insert_delete) {
    return test_user_type_insert_delete(false);
}

SEASTAR_TEST_CASE(test_nonfrozen_user_type_set_field) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        auto ut = user_type_impl::get_instance("ks", to_bytes("ut"),
                    {to_bytes("a"), to_bytes("b"), to_bytes("c")},
                    {int32_type, utf8_type, long_type}, true);

        auto int_null = data_value::make_null(int32_type);
        auto text_null = data_value::make_null(utf8_type);
        auto long_null = data_value::make_null(long_type);

        auto mk_row = [&] (int k, const std::vector<data_value>& vs) -> std::vector<bytes_opt> {
            return {int32_type->decompose(k), ut->decompose(make_user_value(ut, user_type_impl::native_type(vs)))};
        };

        auto mk_null_row = [&] (int k) -> std::vector<bytes_opt> {
            return {int32_type->decompose(k), {}};
        };

        e.execute_cql("create type ut (a int, b text, c bigint)").discard_result().get();
        e.execute_cql("create table cf (a int primary key, b ut)").discard_result().get();

        e.execute_cql("insert into cf (a, b) values (1, {a:1, b:'text1', c:1})").discard_result().get();
        e.execute_cql("insert into cf (a, b) values (2, null)").discard_result().get();
        e.execute_cql("insert into cf (a, b) values (3, {a:null})").discard_result().get();

        before_and_after_flush(e, [&] {
            assert_that(e.execute_cql("select * from cf where a = 2").get0()).is_rows().with_rows_ignore_order({
                mk_null_row(2),
            });
        });

        e.execute_cql("update cf set b.b = null where a in (2,3)").discard_result().get();

        before_and_after_flush(e, [&] {
            assert_that(e.execute_cql("select * from cf where a in (2, 3)").get0()).is_rows().with_rows_ignore_order({
                mk_null_row(2),
                mk_null_row(3),
            });
        });

        e.execute_cql("update cf set b.b = 'text' where a = 1").discard_result().get();
        e.execute_cql("update cf set b.a = 2 where a = 2").discard_result().get();
        e.execute_cql("update cf set b.c = 2 where a = 2").discard_result().get();
        e.execute_cql("update cf set b.c = 3 where a = 3").discard_result().get();

        before_and_after_flush(e, [&] {
            assert_that(e.execute_cql("select * from cf").get0()).is_rows().with_rows_ignore_order({
                mk_row(1, {1, "text", int64_t(1)}),
                mk_row(2, {2, text_null, int64_t(2)}),
                mk_row(3, {int_null, text_null, int64_t(3)}),
            });
        });

        e.execute_cql("update cf set b.a = null where a = 1").discard_result().get();

        before_and_after_flush(e, [&] {
            assert_that(e.execute_cql("select * from cf where a = 1").get0()).is_rows().with_rows_ignore_order({
                mk_row(1, {int_null, "text", int64_t(1)}),
            });
        });

        e.execute_cql("delete b.c from cf where a in (1,2,3)").discard_result().get();

        before_and_after_flush(e, [&] {
            assert_that(e.execute_cql("select * from cf").get0()).is_rows().with_rows_ignore_order({
                mk_row(1, {int_null, "text", long_null}),
                mk_row(2, {2, text_null, long_null}),
                mk_null_row(3),
            });
        });

        e.execute_cql("delete b.b, b.a from cf where a in (1,2)").discard_result().get();

        before_and_after_flush(e, [&] {
            assert_that(e.execute_cql("select * from cf").get0()).is_rows().with_rows_ignore_order({
                mk_null_row(1),
                mk_null_row(2),
                mk_null_row(3),
            });
        });

        auto ut_inner = user_type_impl::get_instance("ks", to_bytes("ut_inner"),
                    {to_bytes("a"), to_bytes("b")},
                    {int32_type, int32_type}, false);

        auto ut_inner_null = data_value::make_null(ut_inner);

        auto mk_ut_inner_val = [&] (const std::vector<data_value>& vs) -> data_value {
            return make_user_value(ut_inner, user_type_impl::native_type(vs));
        };

        e.execute_cql("create type ut_inner (a int, b int)").discard_result().get();

        ut = user_type_impl::get_instance("ks", to_bytes("ut"),
                    {to_bytes("foo"), to_bytes("b"), to_bytes("c"), to_bytes("d")},
                    {int32_type, utf8_type, long_type, ut_inner}, true);

        e.execute_cql("alter type ut rename a to foo").discard_result().get();
        e.execute_cql("alter type ut add d frozen<ut_inner>").discard_result().get();

        assert_that(e.execute_cql("select * from cf").get0()).is_rows().with_rows_ignore_order({
            mk_null_row(1),
            mk_null_row(2),
            mk_null_row(3),
        });

        e.execute_cql("update cf set b.foo = 1 where a = 1").discard_result().get();
        e.execute_cql("update cf set b.d = {a:1, b:2} where a = 2").discard_result().get();

        before_and_after_flush(e, [&] {
            assert_that(e.execute_cql("select * from cf").get0()).is_rows().with_rows_ignore_order({
                mk_row(1, {1, text_null, long_null, ut_inner_null}),
                mk_row(2, {int_null, text_null, long_null, mk_ut_inner_val({1, 2})}),
                mk_null_row(3),
            });
        });
    });
}

SEASTAR_TEST_CASE(test_nonfrozen_user_types_prepared) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        auto ut = user_type_impl::get_instance("ks", to_bytes("ut"),
                    {to_bytes("a"), to_bytes("b"), to_bytes("c")},
                    {int32_type, utf8_type, long_type}, true);

        auto execute_prepared = [&] (const sstring& cql, const std::vector<cql3::raw_value>& vs) {
            auto id = e.prepare(cql).get0();
            e.execute_prepared(id, vs).discard_result().get();
        };

        auto mk_int = [] (int x) {
            return cql3::raw_value::make_value(int32_type->decompose(x));
        };

        auto mk_ut = [&] (const std::vector<data_value>& vs) {
            return cql3::raw_value::make_value(ut->decompose(make_user_value(ut, vs)));
        };

        auto mk_tuple = [&] (const std::vector<data_value>& vs) {
            auto type = static_pointer_cast<const tuple_type_impl>(ut);
            return cql3::raw_value::make_value(type->decompose(make_tuple_value(type, vs)));
        };

        auto text_null = data_value::make_null(utf8_type);
        auto long_null = data_value::make_null(long_type);

        auto mk_row = [&] (int k, const std::vector<data_value>& vs) -> std::vector<bytes_opt> {
            return {int32_type->decompose(k), ut->decompose(make_user_value(ut, user_type_impl::native_type(vs)))};
        };

        auto mk_null_row = [] (int k) -> std::vector<bytes_opt> {
            return {int32_type->decompose(k), {}};
        };

        e.execute_cql("create type ut (a int, b text, c bigint)").discard_result().get();
        e.execute_cql("create table cf (a int primary key, b ut)").discard_result().get();

        execute_prepared("insert into cf (a, b) values (?, ?)", {mk_int(1), mk_ut({1, "text1", long_null})});
        execute_prepared("insert into cf (a, b) values (?, ?)", {mk_int(2), mk_ut({2, text_null, int64_t(2)})});
        execute_prepared("insert into cf (a, b) values (?, ?)", {mk_int(3), mk_ut({})});

        assert_that(e.execute_cql("select * from cf").get0()).is_rows().with_rows_ignore_order({
            mk_row(1, {1, "text1", long_null}),
            mk_row(2, {2, text_null, int64_t(2)}),
            mk_null_row(3),
        });

        auto query_prepared = [&] (const sstring& cql, const std::vector<cql3::raw_value>& vs) {
            auto id = e.prepare(cql).get0();
            return e.execute_prepared(id, vs).get0();
        };

        auto mk_ut_list = [&] (const std::vector<std::vector<data_value>>& vss) {
            std::vector<data_value> ut_vs;
            for (const auto& vs: vss) {
                ut_vs.push_back(make_user_value(ut, vs));
            }

            const auto& ut_list_type = list_type_impl::get_instance(ut, true);
            return cql3::raw_value::make_value(
                    ut_list_type->decompose(make_list_value(ut_list_type, list_type_impl::native_type(ut_vs))));
        };

        assert_that(query_prepared("select * from cf where b in ? allow filtering", {mk_ut_list({{1, "text1", long_null}, {}})}))
                .is_rows().with_rows_ignore_order({
            mk_row(1, {1, "text1", long_null}),
        });

        execute_prepared("insert into cf (a, b) values (?, ?)", {mk_int(4), mk_tuple({4, "text4", int64_t(4)})});
        assert_that(e.execute_cql("select * from cf where a = 4").get0()).is_rows().with_rows_ignore_order({
            mk_row(4, {4, "text4", int64_t(4)}),
        });

        auto mk_longer_tuple = [&] (const std::vector<data_value>& vs) {
            auto type = tuple_type_impl::get_instance({int32_type, utf8_type, long_type, int32_type});
            return cql3::raw_value::make_value(type->decompose(make_tuple_value(type, vs)));
        };

        BOOST_REQUIRE_EXCEPTION(
            execute_prepared("insert into cf (a, b) values (?, ?)", {mk_int(4), mk_longer_tuple({4, "text4", int64_t(4), 5})}),
            exceptions::invalid_request_exception,
            exception_predicate::message_equals("User Defined Type value contained too many fields (expected 3, got 4)"));
    });
}

// This test reproduces issue #5544: user-defined types with case-sensitive
// (quoted) names were unusable:
// As with other identifiers in CQL, user type names also have their case
// folded to lowercase - unless quoted. In this test we create a type called
// "PHone" (like this, with the quotes), and then try to use it in a
// CREATE TABLE command. The bug was that CREATE TABLE failed - with an
// exception "Unknown type ks.phone".
SEASTAR_TEST_CASE(test_user_type_quoted) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("CREATE TYPE \"PHone\" (country_code int, number text)").get();
        e.execute_cql("CREATE TABLE cf (pk blob, pn \"PHone\", PRIMARY KEY (pk))").get();
        e.execute_cql("CREATE TABLE cf2 (pk blob, pn frozen<\"PHone\">, PRIMARY KEY (pk))").get();
        e.execute_cql("CREATE TABLE cf3 (pk blob, pn frozen<list<\"PHone\">>, PRIMARY KEY (pk))").get();
        // Pass if the above CREATE TABLE completes without an exception.
    });
}
