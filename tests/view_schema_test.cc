/*
 * Copyright (C) 2016 ScyllaDB
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
#include <boost/range/adaptor/map.hpp>

#include "database.hh"
#include "db/view/node_view_update_backlog.hh"
#include "db/view/view_builder.hh"

#include "tests/test-utils.hh"
#include "tests/cql_test_env.hh"
#include "tests/cql_assertions.hh"

#include "db/config.hh"

using namespace std::literals::chrono_literals;

// CQL usually folds identifier names - keyspace, table and column names -
// to lowercase. That is, unless the identifier is enclosed in double
// quotation marks (") then the identifier becomes case sensitive.
// Let's test that case-sensitive (quoted) column names can be used for
// materialized views. Test that data can be inserted and queried, and
// that case sensitive columns in views can be renamed.
// This test reproduces issues #3388 and #3391.
SEASTAR_TEST_CASE(test_case_sensitivity) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (\"theKey\" int, \"theClustering\" int, \"theValue\" int, primary key (\"theKey\", \"theClustering\"));").get();
        e.execute_cql("create materialized view mv_test as select * from cf "
                       "where \"theKey\" is not null and \"theClustering\" is not null and \"theValue\" is not null "
                       "primary key (\"theKey\",\"theClustering\")").get();
        e.execute_cql("create materialized view mv_test2 as select \"theKey\", \"theClustering\", \"theValue\" from cf "
                       "where \"theKey\" is not null and \"theClustering\" is not null and \"theValue\" is not null "
                       "primary key (\"theKey\",\"theClustering\")").get();
        e.execute_cql("insert into cf (\"theKey\", \"theClustering\", \"theValue\") values (0 ,0, 0);").get();

        for (auto view : {"mv_test", "mv_test2"}) {
            eventually([&] {
            auto msg = e.execute_cql(sprint("select \"theKey\", \"theClustering\", \"theValue\" from %s ", view)).get0();
            assert_that(msg).is_rows()
                .with_size(1)
                .with_row({
                    {int32_type->decompose(0)},
                    {int32_type->decompose(0)},
                    {int32_type->decompose(0)},
                });
            });
        }

        e.execute_cql("alter table cf rename \"theClustering\" to \"Col\";").get();

        for (auto view : {"mv_test", "mv_test2"}) {
            eventually([&] {
            auto msg = e.execute_cql(sprint("select \"theKey\", \"Col\", \"theValue\" from %s ", view)).get0();
            assert_that(msg).is_rows()
                .with_size(1)
                .with_row({
                    {int32_type->decompose(0)},
                    {int32_type->decompose(0)},
                    {int32_type->decompose(0)},
                });
            });
        }
    });
}

SEASTAR_TEST_CASE(test_access_and_schema) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (p int, c ascii, v bigint, primary key (p, c));").get();
        e.execute_cql("create materialized view vcf as select * from cf "
                      "where v is not null and p is not null and c is not null "
                      "primary key (v, p, c)").get();
        e.execute_cql("insert into cf (p, c, v) values (0, 'foo', 1);").get();
        assert_that_failed(e.execute_cql("insert into vcf (p, c, v) values (1, 'foo', 1);"));
        assert_that_failed(e.execute_cql("alter table vcf add foo text;"));
        assert_that_failed(e.execute_cql("alter table vcf with compaction = { 'class' : 'LeveledCompactionStrategy' };"));
        e.execute_cql("alter materialized view vcf with compaction = { 'class' : 'LeveledCompactionStrategy' };").get();
        e.execute_cql("alter table cf add foo text;").get();
        e.execute_cql("insert into cf (p, c, v, foo) values (0, 'foo', 1, 'bar');").get();
        eventually([&] {
        auto msg = e.execute_cql("select foo from vcf").get0();
        assert_that(msg).is_rows()
            .with_size(1)
            .with_row({
                {utf8_type->decompose(sstring("bar"))},
            });
        });
        e.execute_cql("alter table cf rename c to bar;").get();
        auto msg = e.execute_cql("select bar from vcf").get0();
        assert_that(msg).is_rows()
            .with_size(1)
            .with_row({
                {utf8_type->decompose(sstring("foo"))},
            });
    });
}

SEASTAR_TEST_CASE(test_updates) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table base (k int, v int, primary key (k));").get();
        e.execute_cql("create materialized view mv as select * from base "
                       "where k is not null and v is not null primary key (v, k)").get();

        e.execute_cql("insert into base (k, v) values (0, 0);").get();
        auto msg = e.execute_cql("select k, v from base where k = 0").get0();
        assert_that(msg).is_rows()
            .with_size(1)
            .with_row({ {int32_type->decompose(0)}, {int32_type->decompose(0)} });
        eventually([&] {
        auto msg = e.execute_cql("select k, v from mv where v = 0").get0();
        assert_that(msg).is_rows()
            .with_size(1)
            .with_row({ {int32_type->decompose(0)}, {int32_type->decompose(0)} });
        });

        e.execute_cql("insert into base (k, v) values (0, 1);").get();
        msg = e.execute_cql("select k, v from base where k = 0").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({ {int32_type->decompose(0)}, {int32_type->decompose(1)} });
        eventually([&] {
        auto msg = e.execute_cql("select k, v from mv where v = 0").get0();
        assert_that(msg).is_rows().with_size(0);
        msg = e.execute_cql("select k, v from mv where v = 1").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({ {int32_type->decompose(0)}, {int32_type->decompose(1)} });
        });
    });
}

SEASTAR_TEST_CASE(test_updates_no_read_before_update) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table base (k int, c int, v int, primary key (k));").get();
        e.execute_cql("create materialized view mv as select * from base "
                              "where k is not null and c is not null primary key (k, c)").get();

        e.execute_cql("insert into base (k, c, v) values (0, 0, 0);").get();
        eventually([&] {
        auto msg = e.execute_cql("select k, v from base where k = 0").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({ {int32_type->decompose(0)}, {int32_type->decompose(0)} });
        });
        auto msg = e.execute_cql("select k, v from mv where k = 0").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({ {int32_type->decompose(0)}, {int32_type->decompose(0)} });

        e.execute_cql("insert into base (k, c, v) values (0, 0, 1);").get();
        eventually([&] {
        auto msg = e.execute_cql("select k, v from base where k = 0").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({ {int32_type->decompose(0)}, {int32_type->decompose(1)} });
        });
        msg = e.execute_cql("select k, v from mv where k = 0").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({ {int32_type->decompose(0)}, {int32_type->decompose(1)} });
    });
}

SEASTAR_TEST_CASE(test_reuse_name) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (p int primary key, v int);").get();
        e.execute_cql("create materialized view vcf as select * from cf "
                      "where v is not null and p is not null primary key (v, p)").get();
        e.execute_cql("drop materialized view vcf").get();
        e.execute_cql("create materialized view vcf as select * from cf "
                      "where v is not null and p is not null "
                      "primary key (v, p)").get();
    });
}

SEASTAR_TEST_CASE(test_all_types) {
    logging::logger_registry().set_all_loggers_level(logging::log_level::error);
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("CREATE TYPE myType (a int, b uuid, c set<text>)").get();
        e.execute_cql("CREATE TABLE cf ("
                    "k int PRIMARY KEY, "
                    "asciival ascii, "
                    "bigintval bigint, "
                    "blobval blob, "
                    "booleanval boolean, "
                    "dateval date, "
                    "decimalval decimal, "
                    "doubleval double, "
                    "floatval float, "
                    "inetval inet, "
                    "intval int, "
                    "textval text, "
                    "timeval time, "
                    "timestampval timestamp, "
                    "timeuuidval timeuuid, "
                    "uuidval uuid,"
                    "varcharval varchar, "
                    "varintval varint, "
                    "listval list<int>, "
                    "frozenlistval frozen<list<int>>, "
                    "setval set<uuid>, "
                    "frozensetval frozen<set<uuid>>, "
                    "mapval map<ascii, int>,"
                    "frozenmapval frozen<map<ascii, int>>,"
                    "tupleval frozen<tuple<int, ascii, uuid>>,"
                    "udtval frozen<myType>)").get();
        auto s = e.local_db().find_schema(sstring("ks"), sstring("cf"));
        BOOST_REQUIRE(s);
        for (auto& col : s->all_columns()) {
            auto f = e.execute_cql(sprint("create materialized view mv_%s as select * from cf "
                                          "where %s is not null and k is not null primary key (%s, k)",
                                          col.name_as_text(), col.name_as_text(), col.name_as_text()));
            if (col.type->is_multi_cell() || col.is_partition_key()) {
                assert_that_failed(f);
            } else {
                f.get();
            }
        }

        // ================ ascii ================
        e.execute_cql("insert into cf (k, asciival) values (0, 'ascii text');").get();
        eventually([&] {
        auto msg = e.execute_cql("select k, asciival, udtval from mv_asciival where asciival = 'ascii text'").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({ {int32_type->decompose(0)}, {ascii_type->decompose("ascii text")}, { } });
        });

        // ================ bigint ================
        e.execute_cql("insert into cf (k, bigintval) values (0, 12121212);").get();
        eventually([&] {
        auto msg = e.execute_cql("select k, bigintval, asciival from mv_bigintval where bigintval = 12121212").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({ {int32_type->decompose(0)}, {long_type->decompose(12121212L)}, {ascii_type->decompose("ascii text")} });
        });

        // ================ blob ================
        e.execute_cql("insert into cf (k, blobval) values (0, 0x000001);").get();
        eventually([&] {
        auto msg = e.execute_cql("select k, blobval, asciival from mv_blobval where blobval = 0x000001").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({ {int32_type->decompose(0)}, {bytes_type->from_string("000001")}, {ascii_type->decompose("ascii text")} });
        });

        // ================ boolean ================
        e.execute_cql("insert into cf (k, booleanval) values (0, true);").get();
        eventually([&] {
        auto msg = e.execute_cql("select k, booleanval, asciival from mv_booleanval where booleanval = true").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({ {int32_type->decompose(0)}, {boolean_type->decompose(true)}, {ascii_type->decompose("ascii text")} });
        });
        e.execute_cql("insert into cf (k, booleanval) values (0, false);").get();
        eventually([&] {
        auto msg = e.execute_cql("select k, booleanval, asciival from mv_booleanval where booleanval = true").get0();
        assert_that(msg).is_rows()
                .with_size(0);
        msg = e.execute_cql("select k, booleanval, asciival from mv_booleanval where booleanval = false").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({ {int32_type->decompose(0)}, {boolean_type->decompose(false)}, {ascii_type->decompose("ascii text")} });
        });

        // ================ date ================
        e.execute_cql("insert into cf (k, dateval) values (0, '1986-01-19');").get();
        eventually([&] {
        auto msg = e.execute_cql("select k, dateval, asciival from mv_dateval where dateval = '1986-01-19'").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({ {int32_type->decompose(0)}, {simple_date_type->from_string("1986-01-19")}, {ascii_type->decompose("ascii text")} });
        });

        // ================ decimal ================
        e.execute_cql("insert into cf (k, decimalval) values (0, 123123.123123);").get();
        eventually([&] {
        auto msg = e.execute_cql("select k, decimalval, asciival from mv_decimalval where decimalval = 123123.123123").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({ {int32_type->decompose(0)}, {decimal_type->from_string("123123.123123")}, {ascii_type->decompose("ascii text")} });
        });

        // ================ double ================
        e.execute_cql("insert into cf (k, doubleval) values (0, 123123.123123);").get();
        eventually([&] {
        auto msg = e.execute_cql("select k, doubleval, asciival from mv_doubleval where doubleval = 123123.123123").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({ {int32_type->decompose(0)}, {double_type->from_string("123123.123123")}, {ascii_type->decompose("ascii text")} });
        });

        // ================ float ================
        e.execute_cql("insert into cf (k, floatval) values (0, 123123.123123);").get();
        eventually([&] {
        auto msg = e.execute_cql("select k, floatval, asciival from mv_floatval where floatval = 123123.123123").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({ {int32_type->decompose(0)}, {float_type->from_string("123123.123123")}, {ascii_type->decompose("ascii text")} });
        });

        // ================ inet ================
        e.execute_cql("insert into cf (k, inetval) values (0, '127.0.0.1');").get();
        eventually([&] {
        auto msg = e.execute_cql("select k, inetval, asciival from mv_inetval where inetval = '127.0.0.1'").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({ {int32_type->decompose(0)}, {inet_addr_type->from_string("127.0.0.1")}, {ascii_type->decompose("ascii text")} });
        });

        // ================ int ================
        e.execute_cql("insert into cf (k, intval) values (0, 456);").get();
        eventually([&] {
        auto msg = e.execute_cql("select k, intval, asciival from mv_intval where intval = 456").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({ {int32_type->decompose(0)}, {int32_type->decompose(456)}, {ascii_type->decompose("ascii text")} });
        });

        // ================ utf8 ================
        e.execute_cql("insert into cf (k, textval) values (0, '\"some \" text');").get();
        eventually([&] {
        auto msg = e.execute_cql("select k, textval, asciival from mv_textval where textval = '\"some \" text'").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({ {int32_type->decompose(0)}, {utf8_type->from_string("\"some \" text")}, {ascii_type->decompose("ascii text")} });
        });

        // ================ time ================
        e.execute_cql("insert into cf (k, timeval) values (0, '07:35:07.000111222');").get();
        eventually([&] {
        auto msg = e.execute_cql("select k, timeval, asciival from mv_timeval where timeval = '07:35:07.000111222'").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({ {int32_type->decompose(0)}, {time_type->from_string("07:35:07.000111222")}, {ascii_type->decompose("ascii text")} });
        });

        // ================ timestamp ================
        e.execute_cql("insert into cf (k, timestampval) values (0, '123123123123');").get();
        eventually([&] {
        auto msg = e.execute_cql("select k, timestampval, asciival from mv_timestampval where timestampval = '123123123123'").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({ {int32_type->decompose(0)}, {timestamp_type->from_string("123123123123")}, {ascii_type->decompose("ascii text")} });
        });

        // ================ timeuuid ================
        e.execute_cql("insert into cf (k, timeuuidval) values (0, D2177dD0-EAa2-11de-a572-001B779C76e3);").get();
        eventually([&] {
        auto msg = e.execute_cql("select k, timeuuidval, asciival from mv_timeuuidval where timeuuidval = D2177dD0-EAa2-11de-a572-001B779C76e3").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({ {int32_type->decompose(0)}, {timeuuid_type->from_string("D2177dD0-EAa2-11de-a572-001B779C76e3")}, {ascii_type->decompose("ascii text")} });
        });

        // ================ uuid ================
        e.execute_cql("insert into cf (k, uuidval) values (0, 6bddc89a-5644-11e4-97fc-56847afe9799);").get();
        eventually([&] {
        auto msg = e.execute_cql("select k, uuidval, asciival from mv_uuidval where uuidval = 6bddc89a-5644-11e4-97fc-56847afe9799").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({ {int32_type->decompose(0)}, {uuid_type->from_string("6bddc89a-5644-11e4-97fc-56847afe9799")}, {ascii_type->decompose("ascii text")} });
        });

        // ================ varint ================
        e.execute_cql("insert into cf (k, varintval) values (0, 1234567890123456789012345678901234567890);").get();
        eventually([&] {
        auto msg = e.execute_cql("select k, varintval, asciival from mv_varintval where varintval = 1234567890123456789012345678901234567890").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({ {int32_type->decompose(0)}, {varint_type->from_string("1234567890123456789012345678901234567890")}, {ascii_type->decompose("ascii text")} });
        });

        // ================ lists ================
        auto list_type = s->get_column_definition(bytes("listval"))->type;
        e.execute_cql("insert into cf (k, listval) values (0, [1, 2, 3]);").get();
        eventually([&] {
        auto msg = e.execute_cql("select k, listval from mv_intval where intval = 456").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({ {int32_type->decompose(0)}, {make_list_value(list_type, list_type_impl::native_type({1, 2, 3})).serialize()} });
        });

        e.execute_cql("insert into cf (k, listval) values (0, [1]);").get();
        eventually([&] {
        auto msg = e.execute_cql("select k, listval from mv_intval where intval = 456").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({ {int32_type->decompose(0)}, {make_list_value(list_type, list_type_impl::native_type({data_value(1)})).serialize()} });
        });

        e.execute_cql("update cf set listval = listval + [2] where k = 0;").get();
        eventually([&] {
        auto msg = e.execute_cql("select k, listval from mv_intval where intval = 456").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({ {int32_type->decompose(0)}, {make_list_value(list_type, list_type_impl::native_type({1, 2})).serialize()} });
        });

        e.execute_cql("update cf set listval = [0] + listval where k = 0;").get();
        eventually([&] {
        auto msg = e.execute_cql("select k, listval from mv_intval where intval = 456").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({ {int32_type->decompose(0)}, {make_list_value(list_type, list_type_impl::native_type({0, 1, 2})).serialize()} });
        });

        e.execute_cql("update cf set listval[1] = 10 where k = 0;").get();
        eventually([&] {
        auto msg = e.execute_cql("select k, listval from mv_intval where intval = 456").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({ {int32_type->decompose(0)}, {make_list_value(list_type, list_type_impl::native_type({0, 10, 2})).serialize()} });
        });

        e.execute_cql("delete listval[1] from cf where k = 0;").get();
        eventually([&] {
        auto msg = e.execute_cql("select k, listval from mv_intval where intval = 456").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({ {int32_type->decompose(0)}, {make_list_value(list_type, list_type_impl::native_type({0, 2})).serialize()} });
        });

        e.execute_cql("insert into cf (k, listval) values (0, []);").get();
        eventually([&] {
        auto msg = e.execute_cql("select k, listval from cf where k = 0").get0();
        assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(0)}, {} }});
        });

        auto msg = e.execute_cql("select k, listval from mv_intval where intval = 456").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_rows({{ {int32_type->decompose(0)}, {} }});

        // frozen
        e.execute_cql("insert into cf (k, frozenlistval) values (0, [1, 2, 3]);").get();
        eventually([&] {
        auto msg = e.execute_cql("select k, frozenlistval, asciival from mv_frozenlistval where frozenlistval = [1, 2, 3]").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({ {int32_type->decompose(0)}, {make_list_value(list_type, list_type_impl::native_type({1, 2, 3})).serialize()}, {ascii_type->decompose("ascii text")} });
        });

        e.execute_cql("insert into cf (k, frozenlistval) values (0, [3, 2, 1]);").get();
        eventually([&] {
        auto msg = e.execute_cql("select k, frozenlistval, asciival from mv_frozenlistval where frozenlistval = [3, 2, 1]").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({ {int32_type->decompose(0)}, {make_list_value(list_type, list_type_impl::native_type({3, 2, 1})).serialize()}, {ascii_type->decompose("ascii text")} });
        });

        e.execute_cql("insert into cf (k, frozenlistval) values (0, []);").get();
        eventually([&] {
        auto msg = e.execute_cql("select k, frozenlistval, asciival from mv_frozenlistval where frozenlistval = []").get0();
        assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(0)}, {make_list_value(list_type, list_type_impl::native_type({})).serialize()} , {ascii_type->decompose("ascii text")} }});
        });

        // ================ sets ================
        auto set_type = s->get_column_definition(bytes("setval"))->type;
        e.execute_cql("insert into cf (k, setval) values (0, {6bddc89a-5644-11e4-97fc-56847afe9798, 6bddc89a-5644-11e4-97fc-56847afe9799});").get();
        eventually([&] {
        auto msg = e.execute_cql("select k, setval from mv_intval where intval = 456").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({ {int32_type->decompose(0)}, {make_set_value(set_type, set_type_impl::native_type({
                    utils::UUID("6bddc89a-5644-11e4-97fc-56847afe9798"),
                    utils::UUID("6bddc89a-5644-11e4-97fc-56847afe9799")})).serialize()} });
        });

        e.execute_cql("insert into cf (k, setval) values (0, {6bddc89a-5644-11e4-97fc-56847afe9798, 6bddc89a-5644-11e4-97fc-56847afe9798, 6bddc89a-5644-11e4-97fc-56847afe9799});").get();
        eventually([&] {
        auto msg = e.execute_cql("select k, setval from mv_intval where intval = 456").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({ {int32_type->decompose(0)}, {make_set_value(set_type, set_type_impl::native_type({
                    utils::UUID("6bddc89a-5644-11e4-97fc-56847afe9798"),
                    utils::UUID("6bddc89a-5644-11e4-97fc-56847afe9799")})).serialize()} });
        });

        e.execute_cql("update cf set setval = setval + {6bddc89a-5644-0000-97fc-56847afe9799} where k = 0;").get();
        eventually([&] {
        auto msg = e.execute_cql("select k, setval from mv_intval where intval = 456").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({ {int32_type->decompose(0)}, {make_set_value(set_type, set_type_impl::native_type({
                    utils::UUID("6bddc89a-5644-0000-97fc-56847afe9799"),
                    utils::UUID("6bddc89a-5644-11e4-97fc-56847afe9798"),
                    utils::UUID("6bddc89a-5644-11e4-97fc-56847afe9799")})).serialize()} });
        });

        e.execute_cql("update cf set setval = setval - {6bddc89a-5644-0000-97fc-56847afe9799} where k = 0;").get();
        eventually([&] {
        auto msg = e.execute_cql("select k, setval from mv_intval where intval = 456").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({ {int32_type->decompose(0)}, {make_set_value(set_type, set_type_impl::native_type({
                    utils::UUID("6bddc89a-5644-11e4-97fc-56847afe9798"),
                    utils::UUID("6bddc89a-5644-11e4-97fc-56847afe9799")})).serialize()} });
        });

        e.execute_cql("insert into cf (k, setval) values (0, {});").get();
        eventually([&] {
        auto msg = e.execute_cql("select k, setval from mv_intval where intval = 456").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({ {int32_type->decompose(0)}, {} });
        });

        // frozen
        e.execute_cql("insert into cf (k, frozensetval) values (0, {});").get();
        eventually([&] {
        auto msg = e.execute_cql("select k, frozensetval from mv_frozensetval  where frozensetval = {}").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({ {int32_type->decompose(0)}, {make_set_value(set_type, set_type_impl::native_type({})).serialize()} });
        });

        e.execute_cql("insert into cf (k, frozensetval) values (0, {6bddc89a-5644-11e4-97fc-56847afe9798, 6bddc89a-5644-11e4-97fc-56847afe9799});").get();
        eventually([&] {
        auto msg = e.execute_cql("select k, frozensetval, asciival from mv_frozensetval where frozensetval = {6bddc89a-5644-11e4-97fc-56847afe9798, 6bddc89a-5644-11e4-97fc-56847afe9799}").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({ {int32_type->decompose(0)}, {make_set_value(set_type, set_type_impl::native_type({
                    utils::UUID("6bddc89a-5644-11e4-97fc-56847afe9798"),
                    utils::UUID("6bddc89a-5644-11e4-97fc-56847afe9799")})).serialize()}, {ascii_type->decompose("ascii text")} });
        });

        e.execute_cql("insert into cf (k, frozensetval) values (0, {6bddc89a-0000-11e4-97fc-56847afe9799, 6bddc89a-5644-11e4-97fc-56847afe9798});").get();
        eventually([&] {
        auto msg = e.execute_cql("select k, frozensetval, asciival from mv_frozensetval where frozensetval = {6bddc89a-0000-11e4-97fc-56847afe9799, 6bddc89a-5644-11e4-97fc-56847afe9798}").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({ {int32_type->decompose(0)}, {make_set_value(set_type, set_type_impl::native_type({
                    utils::UUID("6bddc89a-0000-11e4-97fc-56847afe9799"),
                    utils::UUID("6bddc89a-5644-11e4-97fc-56847afe9798")})).serialize()}, {ascii_type->decompose("ascii text")} });
        });

        // ================ maps ================
        auto map_type = s->get_column_definition(bytes("mapval"))->type;
        e.execute_cql("insert into cf (k, mapval) values (0, {'a': 1, 'b': 2});").get();
        eventually([&] {
        auto msg = e.execute_cql("select k, mapval from mv_intval where intval = 456").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({ {int32_type->decompose(0)}, {make_map_value(map_type, map_type_impl::native_type({
                    {sstring("a"), 1}, {sstring("b"), 2}})).serialize()} });
        });

        e.execute_cql("update cf set mapval['c'] = 3 where k = 0;").get();
        eventually([&] {
        auto msg = e.execute_cql("select k, mapval from mv_intval where intval = 456").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({ {int32_type->decompose(0)}, {make_map_value(map_type, map_type_impl::native_type({
                    {sstring("a"), 1}, {sstring("b"), 2}, {sstring("c"), 3}})).serialize()} });
        });

        e.execute_cql("update cf set mapval['b'] = 10 where k = 0;").get();
        eventually([&] {
        auto msg = e.execute_cql("select k, mapval from mv_intval where intval = 456").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({ {int32_type->decompose(0)}, {make_map_value(map_type, map_type_impl::native_type({
                    {sstring("a"), 1}, {sstring("b"), 10}, {sstring("c"), 3}})).serialize()} });
        });

        e.execute_cql("delete mapval['b'] from cf where k = 0;").get();
        eventually([&] {
        auto msg = e.execute_cql("select k, mapval from mv_intval where intval = 456").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({ {int32_type->decompose(0)}, {make_map_value(map_type, map_type_impl::native_type({
                    {sstring("a"), 1}, {sstring("c"), 3}})).serialize()} });
        });

        e.execute_cql("insert into cf (k, mapval) values (0, {});").get();
        eventually([&] {
        auto msg = e.execute_cql("select k, mapval from mv_intval where intval = 456").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({ {int32_type->decompose(0)}, {} });
        });

        // frozen
        e.execute_cql("insert into cf (k, frozenmapval) values (0, {'a': 1, 'b': 2});").get();
        eventually([&] {
        auto msg = e.execute_cql("select k, frozenmapval, asciival from mv_frozenmapval where frozenmapval = {'a': 1, 'b': 2}").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({ {int32_type->decompose(0)}, {make_map_value(map_type, map_type_impl::native_type({
                    {sstring("a"), 1}, {sstring("b"), 2}})).serialize()}, {ascii_type->decompose("ascii text")} });
        });

        e.execute_cql("insert into cf (k, frozenmapval) values (0, {'a': 1, 'b': 2, 'c': 3});").get();
        eventually([&] {
        auto msg = e.execute_cql("select k, frozenmapval, asciival from mv_frozenmapval where frozenmapval = {'a': 1, 'b': 2, 'c': 3}").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({ {int32_type->decompose(0)}, {make_map_value(map_type, map_type_impl::native_type({
                    {sstring("a"), 1}, {sstring("b"), 2}, {sstring("c"), 3}})).serialize()}, {ascii_type->decompose("ascii text")} });
        });

        // ================ tuples ================
        auto tuple_type = s->get_column_definition(bytes("tupleval"))->type;
        e.execute_cql("insert into cf (k, tupleval) values (0, (1, 'foobar', 6bddc89a-5644-11e4-97fc-56847afe9799));").get();
        eventually([&] {
        auto msg = e.execute_cql("select k, tupleval, asciival from mv_tupleval where tupleval = (1, 'foobar', 6bddc89a-5644-11e4-97fc-56847afe9799)").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({ {int32_type->decompose(0)}, {make_tuple_value(tuple_type, tuple_type_impl::native_type({
                    1, data_value::make(ascii_type, std::make_unique<sstring>("foobar")),
                    utils::UUID("6bddc89a-5644-11e4-97fc-56847afe9799")})).serialize()}, {ascii_type->decompose("ascii text")} });
        });

        e.execute_cql("insert into cf (k, tupleval) values (0, (1, null, 6bddc89a-5644-11e4-97fc-56847afe9799));").get();
        eventually([&] {
        auto msg = e.execute_cql("select k, tupleval, asciival from mv_tupleval where tupleval = (1, 'foobar', 6bddc89a-5644-11e4-97fc-56847afe9799)").get0();
        assert_that(msg).is_rows().with_size(0);
        msg = e.execute_cql("select k, tupleval, asciival from mv_tupleval where tupleval = (1, null, 6bddc89a-5644-11e4-97fc-56847afe9799)").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({ {int32_type->decompose(0)}, {make_tuple_value(tuple_type, tuple_type_impl::native_type({
                    1, data_value::make_null(ascii_type),
                    utils::UUID("6bddc89a-5644-11e4-97fc-56847afe9799")})).serialize()}, {ascii_type->decompose("ascii text")} });
        });

        // ================ UDTs ================
        auto udt_type = s->get_column_definition(bytes("udtval"))->type;
        auto udt_set_type = static_pointer_cast<const user_type_impl>(udt_type)->field_type(2);
        e.execute_cql("insert into cf (k, udtval) values (0, (1, 6bddc89a-5644-11e4-97fc-56847afe9799, {'foo', 'bar'}));").get();
        eventually([&] {
        auto msg = e.execute_cql("select k, udtval.a, udtval.b, udtval.c, asciival from mv_udtval where udtval = (1, 6bddc89a-5644-11e4-97fc-56847afe9799, {'foo', 'bar'})").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_rows({{ {int32_type->decompose(0)}, {int32_type->decompose(1)}, {uuid_type->from_string("6bddc89a-5644-11e4-97fc-56847afe9799")},
                            {make_set_value(udt_set_type, set_type_impl::native_type({sstring("bar"), sstring("foo")})).serialize()},
                            {ascii_type->decompose("ascii text")} }});
        });

        e.execute_cql("insert into cf (k, udtval) values (0, {b: 6bddc89a-5644-11e4-97fc-56847afe9799, a: 1, c: {'foo', 'bar'}});").get();
        eventually([&] {
        auto msg = e.execute_cql("select k, udtval.a, udtval.b, udtval.c, asciival from mv_udtval where udtval = {b: 6bddc89a-5644-11e4-97fc-56847afe9799, a: 1, c: {'foo', 'bar'}}").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_rows({{ {int32_type->decompose(0)}, {int32_type->decompose(1)}, {uuid_type->from_string("6bddc89a-5644-11e4-97fc-56847afe9799")},
                            {make_set_value(udt_set_type, set_type_impl::native_type({sstring("bar"), sstring("foo")})).serialize()},
                            {ascii_type->decompose("ascii text")} }});
        });

        e.execute_cql("insert into cf (k, udtval) values (0, {a: null, b: 6bddc89a-5644-11e4-97fc-56847afe9799, c: {'foo', 'bar'}});").get();
        eventually([&] {
        auto msg = e.execute_cql("select k, udtval.a, udtval.b, udtval.c, asciival from mv_udtval where udtval = {a: 1, b: 6bddc89a-5644-11e4-97fc-56847afe9799, c: {'foo', 'bar'}}").get0();
        assert_that(msg).is_rows().with_size(0);
        msg = e.execute_cql("select k, udtval.a, udtval.b, udtval.c, asciival from mv_udtval where udtval = {a: null, b: 6bddc89a-5644-11e4-97fc-56847afe9799, c: {'foo', 'bar'}}").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_rows({{ {int32_type->decompose(0)}, {}, {uuid_type->from_string("6bddc89a-5644-11e4-97fc-56847afe9799")},
                            {make_set_value(udt_set_type, set_type_impl::native_type({sstring("bar"), sstring("foo")})).serialize()},
                            {ascii_type->decompose("ascii text")} }});
        });

        e.execute_cql("insert into cf (k, udtval) values (0, {a: 1, b: 6bddc89a-5644-11e4-97fc-56847afe9799});").get();
        eventually([&] {
        auto msg = e.execute_cql("select k, udtval.a, udtval.b, udtval.c, asciival from mv_udtval where udtval = {a: 1, b: 6bddc89a-5644-11e4-97fc-56847afe9799, c: {'foo', 'bar'}}").get0();
        assert_that(msg).is_rows().with_size(0);
        msg = e.execute_cql("select k, udtval.a, udtval.b, udtval.c, asciival from mv_udtval where udtval = {a: 1, b: 6bddc89a-5644-11e4-97fc-56847afe9799}").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_rows({{ {int32_type->decompose(0)}, {int32_type->decompose(1)}, {uuid_type->from_string("6bddc89a-5644-11e4-97fc-56847afe9799")},
                              {}, {ascii_type->decompose("ascii text")} }});
        });
    });
}

SEASTAR_TEST_CASE(test_drop_table_with_mv) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (p int PRIMARY KEY, v int);").get();
        e.execute_cql("create materialized view vcf as select * from cf "
                      "where v is not null and p is not null "
                      "primary key (v, p)").get();
        assert_that_failed(e.execute_cql("drop table vcf"));
    });
}

SEASTAR_TEST_CASE(test_drop_table_with_active_mv) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (p int primary key, v int);").get();
        e.execute_cql("create materialized view vcf as select * from cf "
                      "where v is not null and p is not null "
                      "primary key (v, p)").get();
        assert_that_failed(e.execute_cql("drop table cf"));
        e.execute_cql("drop materialized view vcf").get();
        e.execute_cql("drop table cf").get();
    });
}

SEASTAR_TEST_CASE(test_alter_table) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (p int, c text, primary key (p, c));").get();
        e.execute_cql("create materialized view vcf as select * from cf "
                      "where p is not null and c is not null "
                      "primary key (p, c)").get();
        e.execute_cql("alter table cf alter c type blob").get();
    });
}

SEASTAR_TEST_CASE(test_alter_reversed_type_base_table) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (p int, c text, primary key (p, c)) with clustering order by (c desc);").get();
        e.execute_cql("create materialized view vcf as select * from cf "
                      "where p is not null and c is not null "
                      "primary key (p, c) with clustering order by (c asc)").get();
        e.execute_cql("alter table cf alter c type blob").get();
    });
}

SEASTAR_TEST_CASE(test_alter_reversed_type_view_table) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (p int, c text, primary key (p, c));").get();
        e.execute_cql("create materialized view vcf as select * from cf "
                      "where p is not null and c is not null "
                      "primary key (p, c) with clustering order by (c desc)").get();
        e.execute_cql("alter table cf alter c type blob").get();
    });
}

SEASTAR_TEST_CASE(test_alter_compatible_type) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (p int, c text, primary key (p));").get();
        e.execute_cql("create materialized view vcf as select * from cf "
                      "where p is not null and c is not null "
                      "primary key (p, c) with clustering order by (c desc)").get();
        e.execute_cql("alter table cf alter c type blob").get();
    });
}

SEASTAR_TEST_CASE(test_alter_incompatible_type) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (p int, c int, primary key (p));").get();
        e.execute_cql("create materialized view vcf as select * from cf "
                      "where p is not null and c is not null "
                      "primary key (p, c) with clustering order by (c desc)").get();
        assert_that_failed(e.execute_cql("alter table cf alter c type blob"));
    });
}

SEASTAR_TEST_CASE(test_drop_non_existing) {
    return do_with_cql_env_thread([] (auto& e) {
        assert_that_failed(e.execute_cql("drop materialized view view_doees_not_exist;"));
        assert_that_failed(e.execute_cql("drop materialized view keyspace_does_not_exist.view_doees_not_exist;"));
        e.execute_cql("drop materialized view if exists view_doees_not_exist;").get();
        e.execute_cql("drop materialized view if exists keyspace_does_not_exist.view_doees_not_exist;").get();
    });
}

SEASTAR_TEST_CASE(test_create_mv_with_unrestricted_pk_parts) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (p int, c ascii, v bigint, primary key (p, c));").get();
        e.execute_cql("create materialized view vcf as select p from cf "
                       "where v is not null and p is not null and c is not null "
                       "primary key (v, p, c)").get();
        e.execute_cql("insert into cf (p, c, v) values (0, 'foo', 1);").get();
        eventually([&] {
        auto msg = e.execute_cql("select * from vcf").get0();
        assert_that(msg).is_rows()
            .with_size(1)
            .with_row({ {long_type->decompose(1L)}, {int32_type->decompose(0)}, {utf8_type->decompose(sstring("foo"))} });
        });
    });
}

SEASTAR_TEST_CASE(test_partition_tombstone) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (p int, c int, v int, primary key (p, c));").get();
        e.execute_cql("create materialized view vcf as select p from cf "
                      "where p is not null and c is not null and v is not null "
                      "primary key (p, c, v)").get();
        e.execute_cql("insert into cf (p, c, v) values (1, 2, 200);").get();
        e.execute_cql("insert into cf (p, c, v) values (1, 3, 300);").get();
        eventually([&] {
        auto msg = e.execute_cql("select * from vcf").get0();
        assert_that(msg).is_rows().with_size(2);
        });
        e.execute_cql("delete from cf where p = 1;").get();
        eventually([&] {
        auto msg = e.execute_cql("select * from vcf").get0();
        assert_that(msg).is_rows().with_size(0);
        });
    });
}

SEASTAR_TEST_CASE(test_ck_tombstone) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (p int, c int, v int, primary key (p, c));").get();
        e.execute_cql("create materialized view vcf as select p from cf "
                              "where p is not null and c is not null and v is not null "
                              "primary key (p, c, v)").get();
        e.execute_cql("insert into cf (p, c, v) values (1, 2, 200);").get();
        e.execute_cql("insert into cf (p, c, v) values (1, 3, 300);").get();
        eventually([&] {
        auto msg = e.execute_cql("select * from vcf").get0();
        assert_that(msg).is_rows().with_size(2);
        });
        e.execute_cql("delete from cf where p = 1 and c = 3;").get();
        eventually([&] {
        auto msg = e.execute_cql("select * from vcf").get0();
        assert_that(msg).is_rows().with_size(1);
        });
    });
}

SEASTAR_TEST_CASE(test_static_table) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (p int, c int, sv int static, v int, primary key (p, c))").get();
        assert_that_failed(e.execute_cql(
                        "create materialized view vcf_static as select * from cf "
                        "where p is not null and c is not null and sv is not null "
                        "primary key (sv, p, c)"));
        assert_that_failed(e.execute_cql(
                        "create materialized view vcf_static as select v, sv from cf "
                        "where p is not null and c is not null and v is not null "
                        "primary key (v, p, c)"));
        assert_that_failed(e.execute_cql(
                        "create materialized view vcf_static as select * from cf "
                        "where p is not null and c is not null and v is not null "
                        "primary key (v, p, c)"));

        e.execute_cql("create materialized view vcf as select v, p, c from cf "
                      "where p is not null and c is not null and v is not null "
                      "primary key (v, p, c)").get();

        for (auto i = 0; i < 100; ++i) {
            e.execute_cql(sprint("insert into cf (p, c, sv, v) values (0, %d, %d, %d)", i % 2, i * 100, i)).get();
        }

        eventually([&] {
        auto msg = e.execute_cql("select * from vcf").get0();
        assert_that(msg).is_rows().with_size(2);
        });
        try {
            e.execute_cql("select sv from vcf").get();
            BOOST_ASSERT(false);
        } catch (...) { }
    });
}


SEASTAR_TEST_CASE(test_static_data) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table  ab ( a int, b int , c int static , primary key(a,b)) with clustering order by (b asc);").get();
        e.execute_cql("create materialized view ba as select a ,b from ab "
                       "where a is not null and b is not null primary key (b,a) with clustering order by (b asc);").get();

        e.execute_cql("insert into ab (a, b) values (1, 2);").get();
        auto msg = e.execute_cql("select a, b from ab where a = 1;").get0();
        assert_that(msg).is_rows()
            .with_size(1)
            .with_row({ {int32_type->decompose(1)}, {int32_type->decompose(2)} });
        eventually([&] {
        auto msg = e.execute_cql("select a, b from ba where b = 2;").get0();
        assert_that(msg).is_rows()
            .with_size(1)
            .with_row({ {int32_type->decompose(1)}, {int32_type->decompose(2)} });
        });

        e.execute_cql("insert into ab (a , b , c) values (3, 4, 5);").get();
        auto msg2 = e.execute_cql("select a, b from ab where a = 3;").get0();
        assert_that(msg2).is_rows()
                .with_size(1)
                .with_row({ {int32_type->decompose(3)}, {int32_type->decompose(4)} });
        eventually([&] {
        auto msg = e.execute_cql("select a, b from ba where b = 4;").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({ {int32_type->decompose(3)}, {int32_type->decompose(4)} });
        });
    });
}



SEASTAR_TEST_CASE(test_old_timestamps) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (p int, c int, v int, primary key (p, c))").get();
        e.execute_cql("create materialized view vcf as select * from cf "
                      "where p is not null and c is not null and v is not null "
                      "primary key (v, p, c)").get();

        for (auto i = 0; i < 100; ++i) {
            e.execute_cql(sprint("insert into cf (p, c, v) values (0, %d, 1)", i % 2)).get();
        }

        eventually([&] {
        auto msg = e.execute_cql("select * from vcf").get0();
        assert_that(msg).is_rows().with_size(2);
        msg = e.execute_cql("select c from vcf where p = 0 and v = 1").get0();
        assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(0)} }, { {int32_type->decompose(1)} }});
        });

        //Make sure an old TS does nothing
        e.execute_cql("update cf using timestamp 100 set v = 5 where p = 0 and c = 0").get();
        eventually([&] {
        auto msg = e.execute_cql("select c from vcf where p = 0 and v = 1").get0();
        assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(0)} }, { {int32_type->decompose(1)} }});
        msg = e.execute_cql("select c from vcf where p = 0 and v = 5").get0();
        assert_that(msg).is_rows().with_size(0);
        });

        //Latest TS
        e.execute_cql("update cf set v = 5 where p = 0 and c = 0").get();
        eventually([&] {
        auto msg = e.execute_cql("select c from vcf where p = 0 and v = 5").get0();
        assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(0)} }});
        msg = e.execute_cql("select c from vcf where p = 0 and v = 1").get0();
        assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(1)} }});
        });
    });
}

SEASTAR_TEST_CASE(test_regular_column_timestamp_updates) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (p int primary key, v1 int, v2 int)").get();
        e.execute_cql("create materialized view vcf as select * from cf "
                      "where p is not null and v1 is not null "
                      "primary key (p, v1)").get();

        e.execute_cql("update cf using timestamp 1 set v1 = 0, v2 = 0 where p = 0").get();
        e.execute_cql("update cf using timestamp 1 set v2 = 1 where p = 0").get();
        e.execute_cql("update cf using timestamp 1 set v1 = 1 where p = 0").get();
        eventually([&] {
        auto msg = e.execute_cql("select * from vcf").get0();
        assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
        });

        e.execute_cql("delete from cf using timestamp 2 where p = 0").get();

        e.execute_cql("update cf using timestamp 3 set v1 = 0, v2 = 0 where p = 0").get();
        e.execute_cql("update cf using timestamp 4 set v1 = 1 where p = 0").get();
        e.execute_cql("update cf using timestamp 5 set v2 = 1 where p = 0").get();
        e.execute_cql("update cf using timestamp 6 set v1 = 2 where p = 0").get();
        e.execute_cql("update cf using timestamp 7 set v2 = 2 where p = 0").get();
        eventually([&] {
        auto msg = e.execute_cql("select * from vcf").get0();
        assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(0)}, {int32_type->decompose(2)}, {int32_type->decompose(2)} }});
        });
    });
}

SEASTAR_TEST_CASE(test_counters_table) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (p int primary key, count counter)").get();
        assert_that_failed(e.execute_cql(
                "create materialized view vcf_static as select * from cf "
                "where p is not null and count is not null "
                "primary key (count, p)"));
    });
}

void do_test_complex_timestamp_updates(cql_test_env& e, std::function<void()>&& maybe_flush) {
    e.execute_cql("create table cf (p int, c int, v1 int, v2 int, v3 int, primary key (p, c))").get();
    e.execute_cql("create materialized view vcf as select * from cf "
                  "where p is not null and c is not null and v1 is not null "
                  "primary key (v1, p, c)").get();

    // Set initial values TS=0, leaving v3 null and verify view
    e.execute_cql("insert into cf (p, c, v1, v2) values (0, 0, 1, 0) using timestamp 0").get();
    eventually([&] {
    auto msg = e.execute_cql("select * from vcf").get0();
    assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(0)}, {} }});
    });

    // Update v1's timestamp TS=2
    e.execute_cql("update cf using timestamp 2 set v1 = 1 where p = 0 and c = 0").get();
    maybe_flush();
    eventually([&] {
    auto msg = e.execute_cql("select v2 from vcf where v1 = 1 and p = 0 and c = 0").get0();
    assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(0)} }});
    });

    // Update v1 @ TS=3, tombstones v1=1 and adds v1=0 partition
    e.execute_cql("update cf using timestamp 3 set v1 = 0 where p = 0 and c = 0").get();
    maybe_flush();
    eventually([&] {
    auto msg = e.execute_cql("select v2 from vcf where v1 = 1 and p = 0 and c = 0").get0();
    assert_that(msg).is_rows().with_size(0);
    });

    // Update v1 back to 1 with TS=4
    e.execute_cql("update cf using timestamp 4 set v1 = 1 where p = 0 and c = 0").get();
    maybe_flush();
    eventually([&] {
    auto msg = e.execute_cql("select v2, v3 from vcf where v1 = 1 and p = 0 and c = 0").get0();
    assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(0)}, {} }});
    });

    // Add v3 @ TS=1
    e.execute_cql("update cf using timestamp 1 set v3 = 1 where p = 0 and c = 0").get();
    maybe_flush();
    eventually([&] {
    auto msg = e.execute_cql("select v2, v3 from vcf where v1 = 1 and p = 0 and c = 0").get0();
    assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(0)}, {int32_type->decompose(1)} }});
    });

    // Update v2 @ TS=2
    e.execute_cql("update cf using timestamp 2 set v2 = 2 where p = 0 and c = 0").get();
    maybe_flush();
    eventually([&] {
    auto msg = e.execute_cql("select v2 from vcf where v1 = 1 and p = 0 and c = 0").get0();
    assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(2)} }});
    });

    // Update v2 @ TS=3
    e.execute_cql("update cf using timestamp 3 set v2 = 4 where p = 0 and c = 0").get();
    maybe_flush();
    eventually([&] {
    auto msg = e.execute_cql("select v2 from vcf where v1 = 1 and p = 0 and c = 0").get0();
    assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(4)} }});
    });

    // Tombstone v1
    e.execute_cql("delete from cf using timestamp 5 where p = 0 and c = 0").get();
    eventually([&] {
    auto msg = e.execute_cql("select v2 from vcf").get0();
    assert_that(msg).is_rows().with_size(0);
    });

    // Add the row back without v2
    e.execute_cql("insert into cf (p, c, v1) values (0, 0, 1) using timestamp 6").get();
    // Make sure v2 doesn't pop back in.
    eventually([&] {
    auto msg = e.execute_cql("select v2 from vcf where v1 = 1 and p = 0 and c = 0").get0();
    assert_that(msg).is_rows().with_rows({{ {} }});
    });

    // New partition
    // Insert a row @ TS=0
    e.execute_cql("insert into cf (p, c, v1, v2, v3) values (1, 0, 0, 0, 0) using timestamp 0").get();

    // Overwrite PK, v1 and v3 @ TS=1, but don't overwrite v2
    e.execute_cql("insert into cf (p, c, v1, v3) values (1, 0, 0, 0) using timestamp 1").get();

    // Delete @ TS=0 (which should only delete v2)
    e.execute_cql("delete from cf using timestamp 0 where p = 1 and c = 0").get();
    eventually([&] {
    auto msg = e.execute_cql("select * from vcf where v1 = 0 and p = 1 and c = 0").get0();
    assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {}, {int32_type->decompose(0)} }});
    });

    e.execute_cql("update cf using timestamp 2 set v1 = 1 where p = 1 and c = 0").get();
    maybe_flush();
    e.execute_cql("update cf using timestamp 3 set v1 = 0 where p = 1 and c = 0").get();
    maybe_flush();
    eventually([&] {
    auto msg = e.execute_cql("select * from vcf where v1 = 0 and p = 1 and c = 0").get0();
    assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {}, {int32_type->decompose(0)} }});
    });

    e.execute_cql("update cf using timestamp 3 set v2 = 0 where p = 1 and c = 0").get();
    maybe_flush();
    eventually([&] {
    auto msg = e.execute_cql("select * from vcf where v1 = 0 and p = 1 and c = 0").get0();
    assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} }});
    });
}

SEASTAR_TEST_CASE(test_complex_timestamp_updates) {
    return do_with_cql_env_thread([] (auto& e) {
        do_test_complex_timestamp_updates(e, [] { });
    });
}

SEASTAR_TEST_CASE(test_complex_timestamp_updates_with_flush) {
    db::config cfg;
    cfg.enable_cache(false);
    return do_with_cql_env_thread([] (auto& e) {
        do_test_complex_timestamp_updates(e, [&] {
            e.local_db().flush_all_memtables().get();
        });
    }, cfg);
}

SEASTAR_TEST_CASE(test_range_tombstone) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (p int, c1 int, c2 int, v int, primary key (p, c1, c2))").get();
        e.execute_cql("create materialized view vcf as select * from cf "
                      "where p is not null and c1 is not null and c2 is not null and v is not null "
                      "primary key ((v, p), c1, c2)").get();

        for (auto i = 0; i < 100; ++i) {
            e.execute_cql(sprint("insert into cf (p, c1, c2, v) values (0, %d, %d, 1)", i % 2, i)).get();
        }

        eventually([&] {
        auto msg = e.execute_cql("select * from vcf").get0();
        assert_that(msg).is_rows().with_size(100);
        });

        e.execute_cql("delete from cf where p = 0 and c1 = 0").get();
        eventually([&] {
        auto msg = e.execute_cql("select * from vcf").get0();
        assert_that(msg).is_rows().with_size(50);
        });

        e.execute_cql("delete from cf where p = 0 and c1 = 1 and c2 >= 50 and c2 < 101").get();
        eventually([&] {
        auto msg = e.execute_cql("select * from vcf").get0();
        assert_that(msg).is_rows().with_size(25);
        });
    });
}

SEASTAR_TEST_CASE(test_compound_partition_key) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (p1 int, p2 int, v int, primary key ((p1, p2)))").get();
        auto s = e.local_db().find_schema(sstring("ks"), sstring("cf"));
        auto& p1 = *s->get_column_definition(to_bytes(sstring("p1")));
        auto& p2 = *s->get_column_definition(to_bytes(sstring("p2")));

        for (auto&& cdef : s->all_columns_in_select_order()) {
            auto maybe_failed = [&cdef] (auto&& f) {
                try {
                    f.get();
                } catch (...) {
                    if (!cdef.is_partition_key()) {
                        BOOST_FAIL("MV creation should have failed");
                    }
                }
            };
            maybe_failed(e.execute_cql(sprint(
                        "create materialized view mv1_%s as select * from cf "
                        "where %s is not null and p1 is not null %s "
                        "primary key (%s, p1 %s)",
                        cdef.name_as_text(), cdef.name_as_text(), cdef == p2 ? "" : "and p2 is not null",
                        cdef.name_as_text(), cdef == p2 ? "" : ", p2")));
            maybe_failed(e.execute_cql(sprint(
                        "create materialized view mv2_%s as select * from cf "
                        "where %s is not null and p1 is not null %s "
                        "primary key (%s, p2 %s)",
                        cdef.name_as_text(), cdef.name_as_text(), cdef == p2 ? "" : "and p2 is not null",
                        cdef.name_as_text(), cdef == p1 ? "" : ", p1")));
            maybe_failed(e.execute_cql(sprint(
                        "create materialized view mv3_%s as select * from cf "
                        "where %s is not null and p1 is not null %s "
                        "primary key ((%s, p1), p2)",
                        cdef.name_as_text(), cdef.name_as_text(), cdef == p2 ? "" : "and p2 is not null", cdef.name_as_text())));
        }

        e.execute_cql("insert into cf (p1, p2, v) values (0, 2, 5)").get();

        eventually([&] {
        auto msg = e.execute_cql("select p1, v from mv1_p2 where p2 = 2").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({
                    {int32_type->decompose(0)},
                    {int32_type->decompose(5)},
                });
        });

        eventually([&] {
        auto msg = e.execute_cql("select p1, v from mv2_p1 where p2 = 2 and p1 = 0").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({
                    {int32_type->decompose(0)},
                    {int32_type->decompose(5)},
                });
        });

        eventually([&] {
        auto msg = e.execute_cql("select p1 from mv1_v where v = 5").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({
                    {int32_type->decompose(0)},
                });
        });

        eventually([&] {
        auto msg = e.execute_cql("select p2 from mv3_v where v = 5 and p1 = 0").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({
                    {int32_type->decompose(2)},
                });
        });

        e.execute_cql("insert into cf (p1, p2, v) values (0, 2, 8)").get();

        eventually([&] {
        auto msg = e.execute_cql("select p1, v from mv1_p2 where p2 = 2").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({
                    {int32_type->decompose(0)},
                    {int32_type->decompose(8)},
                });
        });

        eventually([&] {
        auto msg = e.execute_cql("select p1, v from mv2_p1 where p2 = 2 and p1 = 0").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({
                    {int32_type->decompose(0)},
                    {int32_type->decompose(8)},
                });
        });

        eventually([&] {
        auto msg = e.execute_cql("select p1 from mv1_v where v = 5").get0();
        assert_that(msg).is_rows()
                .with_size(0);
        });

        eventually([&] {
        auto msg = e.execute_cql("select p2 from mv3_v where v = 5 and p1 = 0").get0();
        assert_that(msg).is_rows()
                .with_size(0);
        });
        eventually([&] {
        auto msg = e.execute_cql("select p2 from mv3_v where v = 8 and p1 = 0").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({
                    {int32_type->decompose(2)},
                });
        });
    });
}

SEASTAR_TEST_CASE(test_collections) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (p int, v int, lv list<int>, primary key (p));").get();
        e.execute_cql("create materialized view mv as select * from cf "
                      "where p is not null and v is not null primary key (v, p)").get();

        e.execute_cql("insert into cf (p, v, lv) values (0, 0, [1, 2, 3])").get();

        auto s = e.local_db().find_schema(sstring("ks"), sstring("cf"));
        auto list_type = s->get_column_definition(bytes("lv"))->type;
        eventually([&] {
        auto msg = e.execute_cql("select p, lv from mv where v = 0").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({ {int32_type->decompose(0)}, {make_list_value(list_type, list_type_impl::native_type({1, 2, 3})).serialize()} });
        });

        e.execute_cql("insert into cf (p, v) values (1, 1)").get();
        e.execute_cql("insert into cf (p, lv) values (1, [1, 2, 3])").get();
        eventually([&] {
        auto msg = e.execute_cql("select p, lv from mv where v = 1").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({ {int32_type->decompose(1)}, {make_list_value(list_type, list_type_impl::native_type({1, 2, 3})).serialize()} });
        });
    });
}

SEASTAR_TEST_CASE(test_update) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (p int, v int, primary key (p));").get();
        e.execute_cql("create materialized view mv as select * from cf "
                      "where p is not null and v is not null primary key (v, p)").get();

        e.execute_cql("insert into cf (p, v) values (0, 0)").get();
        eventually([&] {
        auto msg = e.execute_cql("select * from mv where v = 0").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({ {int32_type->decompose(0)}, {int32_type->decompose(0)} });
        });

        e.execute_cql("insert into cf (p, v) values (0, 1)").get();
        eventually([&] {
        auto msg = e.execute_cql("select * from mv where v = 1").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({ {int32_type->decompose(1)}, {int32_type->decompose(0)} });
        });
    });
}

SEASTAR_TEST_CASE(test_ttl) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (p int, c int, v1 int, v2 int, v3 int, primary key (p, c));").get();
        e.execute_cql("create materialized view mv as select p, c, v1, v2 from cf "
                      "where p is not null and c is not null and v1 is not null primary key (v1, c, p)").get();

        e.execute_cql("insert into cf (p, c, v1, v2, v3) values (0, 0, 0, 0, 0) using ttl 3").get();
        eventually([&] {
        auto msg = e.execute_cql("select * from mv").get0();
        assert_that(msg).is_rows().with_size(1);
        forward_jump_clocks(4s);
        msg = e.execute_cql("select * from mv").get0();
        assert_that(msg).is_rows().with_size(0);
        });

        e.execute_cql("insert into cf (p, c, v1, v2, v3) values (1, 1, 1, 1, 1) using ttl 3").get();
        forward_jump_clocks(1s);
        eventually([&] {
        auto msg = e.execute_cql("select v2 from mv").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({ {int32_type->decompose(1)} });
        });

        e.execute_cql("insert into cf (p, c, v1) values (1, 1, 1)").get();
        forward_jump_clocks(4s);
        eventually([&] {
        auto msg = e.execute_cql("select v2 from mv").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({ { } });
        });

        e.execute_cql("insert into cf (p, c, v1, v2, v3) values (2, 2, 2, 2, 2) using ttl 3").get();
        eventually([&] {
        auto msg = e.execute_cql("select * from mv where v1 = 2").get0();
        assert_that(msg).is_rows().with_size(1);
        });
        forward_jump_clocks(2s);
        e.execute_cql("update cf using ttl 8 set v3 = 4 where p = 2 and c = 2").get();
        forward_jump_clocks(2s);
        eventually([&] {
        auto msg = e.execute_cql("select * from mv where v1 = 2").get0();
        assert_that(msg).is_rows().with_size(0);
        msg = e.execute_cql("select * from cf where p = 2 and c = 2").get0();
        assert_that(msg).is_rows()
            .with_size(1)
            .with_row({ {int32_type->decompose(2)}, {int32_type->decompose(2)}, { }, { }, {int32_type->decompose(4)} });
        });
    });
}

SEASTAR_TEST_CASE(test_row_deletion) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (p int, c int, v1 int, v2 int, primary key (p, c));").get();
        e.execute_cql("create materialized view mv as select * from cf "
                      "where p is not null and c is not null and v1 is not null primary key (v1, c, p)").get();

        e.execute_cql("delete from cf using timestamp 6 where p = 1 and c = 1;").get();
        e.execute_cql("insert into cf (p, c, v1, v2) values (1, 1, 1, 1) using timestamp 3").get();
        eventually([&] {
        auto msg = e.execute_cql("select * from mv").get0();
        assert_that(msg).is_rows().with_size(0);
        });
    });
}

SEASTAR_TEST_CASE(test_conflicting_timestamp) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (p int, c int, v int, primary key (p, c));").get();
        e.execute_cql("create materialized view mv as select * from cf "
                      "where p is not null and c is not null and v is not null primary key (v, c, p)").get();

        for (auto i = 0; i < 50; ++i) {
            e.execute_cql(sprint("insert into cf (p, c, v) values (1, 1, %d)", i)).get();
        }
        eventually([&] {
        auto msg = e.execute_cql("select * from mv").get0();
        assert_that(msg).is_rows()
            .with_size(1)
            .with_row({ {int32_type->decompose(49)}, {int32_type->decompose(1)}, {int32_type->decompose(1)} });
        });
    });
}

SEASTAR_TEST_CASE(test_clustering_order) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (a int, b int, c int, d int, primary key (a, b, c)) with clustering order by (b asc, c desc)").get();
        e.execute_cql("create materialized view mv1 as select * from cf "
                      "where b is not null and c is not null primary key (a, b, c) with clustering order by (b desc)").get();
        e.execute_cql("create materialized view mv2 as select * from cf "
                      "where b is not null and c is not null primary key (a, c, b) with clustering order by (c asc)").get();
        e.execute_cql("create materialized view mv3 as select * from cf "
                      "where b is not null and c is not null primary key (a, b, c)").get();
        e.execute_cql("create materialized view mv4 as select * from cf "
                      "where b is not null and c is not null primary key (a, c, b) with clustering order by (c desc)").get();

        e.execute_cql("insert into cf (a, b, c, d) values (1, 1, 1, 1)").get();
        e.execute_cql("insert into cf (a, b, c, d) values (1, 2, 2, 2)").get();

        eventually([&] {
        auto msg = e.execute_cql("select b from mv1").get0();
        assert_that(msg).is_rows()
            .with_size(2)
            .with_rows({{ {int32_type->decompose(2)} },
                        { {int32_type->decompose(1)} }});
        });

        eventually([&] {
        auto msg = e.execute_cql("select c from mv2").get0();
        assert_that(msg).is_rows()
            .with_size(2)
            .with_rows({{ {int32_type->decompose(1)} },
                        { {int32_type->decompose(2)} }});
        });

        eventually([&] {
        auto msg = e.execute_cql("select b from mv3").get0();
        assert_that(msg).is_rows()
            .with_size(2)
            .with_rows({{ {int32_type->decompose(1)} },
                        { {int32_type->decompose(2)} }});
        });

        eventually([&] {
        auto msg = e.execute_cql("select c from mv4").get0();
        assert_that(msg).is_rows()
            .with_size(2)
            .with_rows({{ {int32_type->decompose(2)} },
                        { {int32_type->decompose(1)} }});
        });
    });
}

SEASTAR_TEST_CASE(test_multiple_deletes) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (p int, c int, primary key (p, c));").get();
        e.execute_cql("create materialized view mv as select * from cf "
                      "where c is not null primary key (c, p)").get();

        e.execute_cql("insert into cf (p, c) values (1, 1)").get();
        e.execute_cql("insert into cf (p, c) values (1, 2)").get();
        e.execute_cql("insert into cf (p, c) values (1, 3)").get();

        eventually([&] {
        auto msg = e.execute_cql("select p, c from mv").get0();
        assert_that(msg).is_rows()
            .with_size(3)
            .with_rows({ { {int32_type->decompose(1)}, {int32_type->decompose(1)} },
                         { {int32_type->decompose(1)}, {int32_type->decompose(2)} },
                         { {int32_type->decompose(1)}, {int32_type->decompose(3)} }});
        });

        e.execute_cql("delete from cf where p = 1 and c > 1 and c < 3").get();
        eventually([&] {
        auto msg = e.execute_cql("select p, c from mv").get0();
        assert_that(msg).is_rows()
            .with_size(2)
            .with_rows({ { {int32_type->decompose(1)}, {int32_type->decompose(1)} },
                         { {int32_type->decompose(1)}, {int32_type->decompose(3)} }});
        });

        e.execute_cql("delete from cf where p = 1").get();
        eventually([&] {
        auto msg = e.execute_cql("select p, c from mv").get0();
        assert_that(msg).is_rows().with_size(0);
        });
    });
}

SEASTAR_TEST_CASE(test_partition_key_only_table) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (p1 int, p2 int, primary key ((p1, p2)));").get();
        e.execute_cql("create materialized view mv as select * from cf "
                      "where p1 is not null and p2 is not null primary key (p2, p1)").get();

        e.execute_cql("insert into cf (p1, p2) values (1, 1)").get();
        eventually([&] {
        auto msg = e.execute_cql("select * from mv").get0();
        assert_that(msg).is_rows()
            .with_size(1)
            .with_row({ {int32_type->decompose(1)}, {int32_type->decompose(1)} });
        });
    });
}

SEASTAR_TEST_CASE(test_delete_single_column_in_view_clustering_key) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (a int, b int, c int, d int, primary key (a, b))").get();
        e.execute_cql("create materialized view mv as select * from cf "
                      "where a is not null and b is not null and d is not null "
                      "primary key (a, d, b)").get();

        e.execute_cql("insert into cf (a, b, c, d) values (0, 0, 0, 0)").get();
        eventually([&] {
        auto msg = e.execute_cql("select a, d, b, c from mv").get0();
        assert_that(msg).is_rows()
            .with_size(1)
            .with_row({ {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} });
        });

        e.execute_cql("delete c from cf where a = 0 and b = 0").get();
        eventually([&] {
        auto msg = e.execute_cql("select a, d, b, c from mv").get0();
        assert_that(msg).is_rows()
            .with_size(1)
            .with_row({ {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(0)}, { } });
        });

        e.execute_cql("delete d from cf where a = 0 and b = 0").get();
        eventually([&] {
        auto msg = e.execute_cql("select a, d, b from mv").get0();
        assert_that(msg).is_rows()
            .with_size(0);
        });
    });
}

SEASTAR_TEST_CASE(test_delete_single_column_in_view_partition_key) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (a int, b int, c int, d int, primary key (a, b))").get();
        e.execute_cql("create materialized view mv as select * from cf "
                              "where a is not null and b is not null and d is not null "
                              "primary key (d, a, b)").get();

        e.execute_cql("insert into cf (a, b, c, d) values (0, 0, 0, 0)").get();
        eventually([&] {
        auto msg = e.execute_cql("select a, d, b, c from mv").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({ {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} });
        });

        e.execute_cql("delete c from cf where a = 0 and b = 0").get();
        eventually([&] {
        auto msg = e.execute_cql("select a, d, b, c from mv").get0();
        assert_that(msg).is_rows()
                .with_size(1)
                .with_row({ {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(0)}, { } });
        });


        e.execute_cql("delete d from cf where a = 0 and b = 0").get();
        eventually([&] {
        auto msg = e.execute_cql("select a, d, b from mv").get0();
        assert_that(msg).is_rows()
                .with_size(0);
        });

    });
}

SEASTAR_TEST_CASE(test_multiple_non_primary_keys_in_view) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (a int, b int, c int, d int, e int, primary key ((a, b), c))").get();
        assert_that_failed(
                e.execute_cql("create materialized view mv as select * from cf "
                              "where a is not null and b is not null and c is not null and d is not null and e is not null "
                              "primary key ((d, a), b, e, c)"));
        assert_that_failed(
                e.execute_cql("create materialized view mv as select * from cf "
                              "where a is not null and b is not null and c is not null and d is not null and e is not null "
                              "primary key ((a, b), c, d, e)"));
    });
}

SEASTAR_TEST_CASE(test_null_in_clustering_columns) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (p int, c int, v1 int, v2 int, primary key (p, c))").get();
        e.execute_cql("create materialized view vcf as select * from cf "
                      "where p is not null and c is not null and v1 is not null "
                      "primary key (p, v1, c)").get();

        e.execute_cql("insert into cf (p, c, v1, v2) values (0, 1, 2, 3)").get();
        eventually([&] {
        auto msg = e.execute_cql("select p, c, v1, v2 from vcf").get0();
        assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(3)} }});
        });

        e.execute_cql("update cf set v1 = null where p = 0 and c = 1").get();
        eventually([&] {
        auto msg = e.execute_cql("select p, c, v1, v2 from vcf").get0();
        assert_that(msg).is_rows().with_size(0);
        });

        e.execute_cql("update cf set v2 = 9 where p = 0 and c = 1").get();
        eventually([&] {
        auto msg = e.execute_cql("select p, c, v1, v2 from vcf").get0();
        assert_that(msg).is_rows().with_size(0);
        });
    });
}

SEASTAR_TEST_CASE(test_create_and_alter_mv_with_ttl) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (p int primary key, v int) with default_time_to_live = 60").get();
        assert_that_failed(
                e.execute_cql("create materialized view mv as select * from cf "
                              "where p is not null and v is not null "
                              "primary key (v, p) with default_time_to_live = 30"));
        e.execute_cql("create materialized view vcf as select * from cf "
                      "where p is not null and v is not null "
                      "primary key (v, p)").get();
        assert_that_failed(e.execute_cql("alter materialized view mv with default_time_to_live = 30"));
    });
}

SEASTAR_TEST_CASE(test_create_with_select_restrictions) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (a int, b int, c int, d int, e int, primary key ((a, b), c, d))").get();
        assert_that_failed(e.execute_cql(
                "create materialized view mv as select * from cf where b is not null and c is not null and d is not null primary key ((a, b), c, d)"));
        assert_that_failed(e.execute_cql(
                "create materialized view mv as select * from cf where a is not null and c is not null and d is not null primary key ((a, b), c, d)"));
        assert_that_failed(e.execute_cql(
                "create materialized view mv as select * from cf where a is not null and b is not null and d is not null primary key ((a, b), c, d)"));
        assert_that_failed(e.execute_cql(
                "create materialized view mv as select * from cf where a is not null and b is not null and c is not null primary key ((a, b), c, d)"));
        assert_that_failed(e.execute_cql(
                "create materialized view mv as select * from cf primary key (a, b, c, d)"));

        e.execute_cql("create materialized view mv1 as select * from cf where a = 1 and b = 1 and c is not null and d is not null primary key ((a, b), c, d)").get();
        e.execute_cql("create materialized view mv2 as select * from cf where a is not null and b is not null and c = 1 and d is not null primary key ((a, b), c, d)").get();
        e.execute_cql("create materialized view mv3 as select * from cf where a is not null and b is not null and c = 1 and d = 1 primary key ((a, b), c, d)").get();
        e.execute_cql("create materialized view mv4 as select * from cf where a = 1 and b = 1 and c = 1 and d = 1 primary key ((a, b), c, d)").get();
        e.execute_cql("create materialized view mv5 as select * from cf where a = 1 and b = 1 and c > 1 and d is not null primary key ((a, b), c, d)").get();
        e.execute_cql("create materialized view mv6 as select * from cf where a = 1 and b = 1 and c = 1 and d in (1, 2, 3) primary key ((a, b), c, d)").get();
        e.execute_cql("create materialized view mv7 as select * from cf where a = 1 and b = 1 and (c, d) = (1, 1) primary key ((a, b), c, d)").get();
        e.execute_cql("create materialized view mv8 as select * from cf where a = 1 and b = 1 and (c, d) > (1, 1) primary key ((a, b), c, d)").get();
        e.execute_cql("create materialized view mv9 as select * from cf where a = 1 and b = 1 and (c, d) in ((1, 1), (2, 2)) primary key ((a, b), c, d)").get();
        e.execute_cql("create materialized view mv10 as select * from cf where a = (int) 1 and b = 1 and c = 1 and d = 1 primary key ((a, b), c, d)").get();
        e.execute_cql("create materialized view mv11 as select * from cf where a = blobasint(intasblob(1)) and b = 1 and c = 1 and d = 1 primary key ((a, b), c, d)").get();
    });
}

SEASTAR_TEST_CASE(test_filter_with_function) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (p int, c int, v int, primary key (p, c))").get();
        e.execute_cql("create materialized view vcf as select * from cf "
                      "where p = blobAsInt(intAsBlob(1)) and c is not null "
                      "primary key (p, c)").get();

        e.execute_cql("insert into cf (p, c, v) values (0, 0, 0)").get();
        e.execute_cql("insert into cf (p, c, v) values (0, 1, 1)").get();
        e.execute_cql("insert into cf (p, c, v) values (1, 0, 2)").get();
        e.execute_cql("insert into cf (p, c, v) values (1, 1, 3)").get();

        eventually([&] {
        auto msg = e.execute_cql("select p, c, v from vcf").get0();
        assert_that(msg).is_rows()
                .with_rows({{ {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(2)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(3)} }});
        });

        e.execute_cql("alter table cf rename p to foo").get();
        auto msg = e.execute_cql("select foo, c, v from vcf").get0();
        assert_that(msg).is_rows()
                .with_rows({{ {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(2)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(3)} }});
    });
}

SEASTAR_TEST_CASE(test_filter_with_type_cast) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (p int, c int, v int, primary key (p, c))").get();
        e.execute_cql("create materialized view vcf as select * from cf "
                      "where p = (int) 1 and c is not null "
                      "primary key (p, c)").get();

        e.execute_cql("insert into cf (p, c, v) values (0, 0, 0)").get();
        e.execute_cql("insert into cf (p, c, v) values (0, 1, 1)").get();
        e.execute_cql("insert into cf (p, c, v) values (1, 0, 2)").get();
        e.execute_cql("insert into cf (p, c, v) values (1, 1, 3)").get();

        eventually([&] {
        auto msg = e.execute_cql("select p, c, v from vcf").get0();
        assert_that(msg).is_rows()
                .with_rows({{ {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(2)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(3)} }});
        });

        e.execute_cql("alter table cf rename p to foo").get();
        auto msg = e.execute_cql("select foo, c, v from vcf").get0();
        assert_that(msg).is_rows()
                .with_rows({{ {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(2)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(3)} }});
    });
}

SEASTAR_TEST_CASE(test_partition_key_filtering_unrestricted_part) {
    return do_with_cql_env_thread([] (auto& e) {
        for (auto&& pk : {"((a, b), c)", "((b, a), c)", "(a, b, c)", "(c, b, a)", "((c, a), b)"}) {
            e.execute_cql("create table cf (a int, b int, c int, d int, primary key ((a, b), c))").get();
            e.execute_cql(sprint("create materialized view vcf as select * from cf "
                                 "where a = 1 and b is not null and c is not null "
                                 "primary key %s", pk)).get();

            e.execute_cql("insert into cf (a, b, c, d) values (0, 0, 0, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (0, 1, 0, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 0, 0, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 0, 1, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 1, 0, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 1, 1, 0)").get();

            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                                { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                                { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                                { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                                { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("update cf set d = 1 where a = 0 and b = 0 and c = 0").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                                { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                                { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                                { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                                { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("update cf set d = 1 where a = 1 and b = 1 and c = 0").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                                { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                                { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                                { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)} },
                                { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("delete from cf where a = 0 and b = 0 and c = 0").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                                { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                                { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                                { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)} },
                                { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("delete from cf where a = 1 and b = 1 and c = 0").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                                { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                                { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                                { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("delete from cf where a = 1 and b = 0").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                                { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("delete from cf where a = 1 and b = 1").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_size(0);
            });

            e.execute_cql("drop materialized view vcf").get();
            e.execute_cql("drop table cf").get();
        }
    });
}

// Test that although normal SELECT queries limit the ability to query
// for slices (ranges) of partition keys, because there is no way to implement
// such queries efficiently, the same slices *do* work for the SELECT statement
// defining a materialized view - there the condition is tested for each
// partition separately, so the performance concerns do not hold.
// This verifies part of issue #2367. See also test_clustering_key_in_restrictions.
SEASTAR_TEST_CASE(test_partition_key_filtering_with_slice) {
    return do_with_cql_env_thread([] (auto& e) {
        // Although the test below tests that slices are allowed in MV's
        // SELECT, let's first verify that these slices are still *not*
        // allowed in ordinary SELECT queries:

        e.execute_cql("create table cf (a int, b int, primary key (a))").get();
        try {
            e.execute_cql("select * from cf where a > 0").get();
            BOOST_FAIL("slice query of partition key");
        } catch (exceptions::invalid_request_exception&) {
            // Expecting "Only EQ and IN relation are supported on the
            // partition key (unless you use the token() function)"
        }
        e.execute_cql("drop table cf").get();

        e.execute_cql("create table cf (a int, b int, c int, primary key (a, b, c))").get();
        try {
            e.execute_cql("select * from cf where a = 1 and b > 0 and c > 0").get();
            BOOST_FAIL("slice of clustering key with non-contiguous range");
        } catch (exceptions::invalid_request_exception&) {
            // Expecting "Clustering column "c" cannot be restricted
            // (preceding column "b" is restricted by a non-EQ relation)"
        }
        e.execute_cql("drop table cf").get();

        e.execute_cql("create table cf (a int, b int, c int, primary key (a, b, c))").get();
        try {
            e.execute_cql("select * from cf where a = 1 and c = 1 and b > 0").get();
            BOOST_FAIL("slice of clustering key with non-contiguous range");
        } catch (exceptions::invalid_request_exception&) {
            // Expecting "PRIMARY KEY column "c" cannot be restricted
            // (preceding column "b" is restricted by a non-EQ relation)
        }
        e.execute_cql("drop table cf").get();


        // And now for the actual MV tests, where the slices should be allowed:
        for (auto&& pk : {"((a, b), c)", "((b, a), c)", "(a, b, c)", "(c, b, a)", "((c, a), b)"}) {
            e.execute_cql("create table cf (a int, b int, c int, d int, primary key ((a, b), c))").get();
            e.execute_cql(sprint("create materialized view vcf as select * from cf "
                                 "where a > 0 and b > 5 and c is not null "
                                 "primary key %s", pk)).get();

            e.execute_cql("insert into cf (a, b, c, d) values (0, 0, 1, 1)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (0, 10, 1, 2)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 0, 2, 1)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 10, 2, 2)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (2, 1, 3, 1)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (2, 10, 3, 2)").get();

            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                                { {int32_type->decompose(1)}, {int32_type->decompose(10)}, {int32_type->decompose(2)}, {int32_type->decompose(2)} },
                                { {int32_type->decompose(2)}, {int32_type->decompose(10)}, {int32_type->decompose(3)}, {int32_type->decompose(2)} }});

            e.execute_cql("insert into cf (a, b, c, d) values (3, 10, 4, 2)").get();
            msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                                { {int32_type->decompose(1)}, {int32_type->decompose(10)}, {int32_type->decompose(2)}, {int32_type->decompose(2)} },
                                { {int32_type->decompose(2)}, {int32_type->decompose(10)}, {int32_type->decompose(3)}, {int32_type->decompose(2)} },
                                { {int32_type->decompose(3)}, {int32_type->decompose(10)}, {int32_type->decompose(4)}, {int32_type->decompose(2)} }});


            e.execute_cql("update cf set d = 1 where a = 0 and b = 0 and c = 0").get();
            msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                                { {int32_type->decompose(1)}, {int32_type->decompose(10)}, {int32_type->decompose(2)}, {int32_type->decompose(2)} },
                                { {int32_type->decompose(2)}, {int32_type->decompose(10)}, {int32_type->decompose(3)}, {int32_type->decompose(2)} },
                                { {int32_type->decompose(3)}, {int32_type->decompose(10)}, {int32_type->decompose(4)}, {int32_type->decompose(2)} }});

            e.execute_cql("update cf set d = 100 where a = 3 and b = 10 and c = 4").get();
            msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                                { {int32_type->decompose(1)}, {int32_type->decompose(10)}, {int32_type->decompose(2)}, {int32_type->decompose(2)} },
                                { {int32_type->decompose(2)}, {int32_type->decompose(10)}, {int32_type->decompose(3)}, {int32_type->decompose(2)} },
                                { {int32_type->decompose(3)}, {int32_type->decompose(10)}, {int32_type->decompose(4)}, {int32_type->decompose(100)} }});

            e.execute_cql("delete from cf where a = 0 and b = 0 and c = 0").get();
            msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                                { {int32_type->decompose(1)}, {int32_type->decompose(10)}, {int32_type->decompose(2)}, {int32_type->decompose(2)} },
                                { {int32_type->decompose(2)}, {int32_type->decompose(10)}, {int32_type->decompose(3)}, {int32_type->decompose(2)} },
                                { {int32_type->decompose(3)}, {int32_type->decompose(10)}, {int32_type->decompose(4)}, {int32_type->decompose(100)} }});

            e.execute_cql("delete from cf where a = 3 and b = 10 and c = 4").get();
            msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                                { {int32_type->decompose(1)}, {int32_type->decompose(10)}, {int32_type->decompose(2)}, {int32_type->decompose(2)} },
                                { {int32_type->decompose(2)}, {int32_type->decompose(10)}, {int32_type->decompose(3)}, {int32_type->decompose(2)} }});

            e.execute_cql("drop materialized view vcf").get();
            e.execute_cql("drop table cf").get();
        }
    });
}

SEASTAR_TEST_CASE(test_partition_key_restrictions) {
    return do_with_cql_env_thread([] (auto& e) {
        for (auto&& pk : {"((a, b), c)", "((b, a), c)", "(a, b, c)", "(c, b, a)", "((c, a), b)"}) {
            e.execute_cql("create table cf (a int, b int, c int, d int, primary key (a, b, c))").get();
            e.execute_cql(sprint("create materialized view vcf as select * from cf "
                                 "where a = 1 and b is not null and c is not null "
                                 "primary key %s", pk)).get();

            e.execute_cql("insert into cf (a, b, c, d) values (0, 0, 0, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (0, 1, 0, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 0, 0, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 0, 1, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 1, 0, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 1, 1, 0)").get();

            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                            { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("update cf set d = 1 where a = 0 and b = 0 and c = 0").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                            { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("update cf set d = 1 where a = 1 and b = 1 and c = 0").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                            { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("delete from cf where a = 0 and b = 0 and c = 0").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                            { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("delete from cf where a = 1 and b = 1 and c = 0").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                            { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("delete from cf where a = 1 and b = 0").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("delete from cf where a = 1 and b = 1").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_size(0);
            });

            e.execute_cql("drop materialized view vcf").get();
            e.execute_cql("drop table cf").get();
        }
    });
}

SEASTAR_TEST_CASE(test_partition_key_compound_restrictions) {
    return do_with_cql_env_thread([] (auto& e) {
        for (auto&& pk : {"((a, b), c)", "((b, a), c)", "(a, b, c)", "(c, b, a)", "((c, a), b)"}) {
            e.execute_cql("create table cf (a int, b int, c int, d int, primary key ((a, b), c))").get();
            e.execute_cql(sprint("create materialized view vcf as select * from cf "
                                 "where a = 1 and b = 1 and c is not null "
                                 "primary key %s", pk)).get();

            e.execute_cql("insert into cf (a, b, c, d) values (0, 0, 0, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (0, 0, 1, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (0, 1, 0, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (0, 1, 1, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 0, 0, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 0, 1, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 1, 0, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 1, 1, 0)").get();

            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                                { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                                { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("update cf set d = 1 where a = 1 and b = 0 and c = 0").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                                { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                                { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("update cf set d = 1 where a = 1 and b = 1 and c = 0").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                                { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)} },
                                { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("delete from cf where a = 1 and b = 0 and c = 0").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                                { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)} },
                                { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("delete from cf where a = 1 and b = 1 and c = 0").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                                { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("delete from cf where a = 1 and b = 1").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_size(0);
            });

            e.execute_cql("drop materialized view vcf").get();
            e.execute_cql("drop table cf").get();
        }
    });
}

SEASTAR_TEST_CASE(test_partition_key_restrictions_not_include_all) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (a int, b int, c int, d int, primary key ((a, b), c))").get();
        e.execute_cql("create materialized view vcf as select a, b, c from cf "
                      "where a = 1 and b = 1 and c is not null "
                      "primary key ((a, b), c)").get();

        e.execute_cql("insert into cf (a, b, c, d) values (0, 0, 0, 0)").get();
        e.execute_cql("insert into cf (a, b, c, d) values (0, 0, 1, 0)").get();
        e.execute_cql("insert into cf (a, b, c, d) values (0, 1, 0, 0)").get();
        e.execute_cql("insert into cf (a, b, c, d) values (0, 1, 1, 0)").get();
        e.execute_cql("insert into cf (a, b, c, d) values (1, 0, 0, 0)").get();
        e.execute_cql("insert into cf (a, b, c, d) values (1, 0, 1, 0)").get();
        e.execute_cql("insert into cf (a, b, c, d) values (1, 1, 0, 0)").get();
        e.execute_cql("insert into cf (a, b, c, d) values (1, 1, 1, 0)").get();

        eventually([&] {
        auto msg = e.execute_cql("select a, b, c from vcf").get0();
        assert_that(msg).is_rows().with_rows_ignore_order({
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
        });

        eventually([&] {
        e.execute_cql("update cf set d = 1 where a = 1 and b = 0 and c = 0").get();
        auto msg = e.execute_cql("select a, b, c from vcf").get0();
        assert_that(msg).is_rows().with_rows_ignore_order({
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
        });

        eventually([&] {
        e.execute_cql("update cf set d = 1 where a = 1 and b = 1 and c = 0").get();
        auto msg = e.execute_cql("select a, b, c from vcf").get0();
        assert_that(msg).is_rows().with_rows_ignore_order({
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
        });

        eventually([&] {
        e.execute_cql("delete from cf where a = 1 and b = 0 and c = 0").get();
        auto msg = e.execute_cql("select a, b, c from vcf").get0();
        assert_that(msg).is_rows().with_rows_ignore_order({
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
        });

        eventually([&] {
        e.execute_cql("delete from cf where a = 1 and b = 1 and c = 0").get();
        auto msg = e.execute_cql("select a, b, c from vcf").get0();
        assert_that(msg).is_rows().with_rows_ignore_order({
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
        });

        eventually([&] {
        e.execute_cql("delete from cf where a = 1 and b = 1").get();
        auto msg = e.execute_cql("select a, b, c from vcf").get0();
        assert_that(msg).is_rows().with_size(0);
        });
    });
}

SEASTAR_TEST_CASE(test_clustering_key_eq_restrictions) {
    return do_with_cql_env_thread([] (auto& e) {
        for (auto&& pk : {"((a, b), c)", "((b, a), c)", "(a, b, c)", "(c, b, a)", "((c, a), b)"}) {
            e.execute_cql("create table cf (a int, b int, c int, d int, primary key (a, b, c))").get();
            e.execute_cql(sprint("create materialized view vcf as select * from cf "
                                 "where a is not null and b = 1 and c is not null "
                                 "primary key %s", pk)).get();

            e.execute_cql("insert into cf (a, b, c, d) values (0, 0, 0, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (0, 0, 1, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (0, 1, 0, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (0, 1, 1, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 0, 0, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 0, 1, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 1, 0, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 1, 1, 0)").get();

            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("update cf set d = 1 where a = 1 and b = 0 and c = 0").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("update cf set d = 1 where a = 0 and b = 1 and c = 0").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)} },
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("delete from cf where a = 0 and b = 0 and c = 0").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)} },
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("delete from cf where a = 1 and b = 1 and c = 0").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)} },
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("delete from cf where a in (0, 1) and b = 1").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_size(0);
            });

            e.execute_cql("drop materialized view vcf").get();
            e.execute_cql("drop table cf").get();
        }
    });
}

SEASTAR_TEST_CASE(test_clustering_key_slice_restrictions) {
    return do_with_cql_env_thread([] (auto& e) {
        for (auto&& pk : {"((a, b), c)", "((b, a), c)", "(a, b, c)", "(c, b, a)", "((c, a), b)"}) {
            e.execute_cql("create table cf (a int, b int, c int, d int, primary key (a, b, c))").get();
            e.execute_cql(sprint("create materialized view vcf as select * from cf "
                                 "where a is not null and b >= 1 and c is not null "
                                 "primary key %s", pk)).get();

            e.execute_cql("insert into cf (a, b, c, d) values (0, 0, 0, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (0, 0, 1, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (0, 1, 0, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (0, 1, 1, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 0, 0, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 0, 1, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 1, 0, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 2, 1, 0)").get();

            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("update cf set d = 1 where a = 1 and b = 0 and c = 0").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("update cf set d = 1 where a = 0 and b = 1 and c = 0").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)} },
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("delete from cf where a = 0 and b = 0 and c = 0").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)} },
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("delete from cf where a = 1 and b = 1 and c = 0").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)} },
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("delete from cf where a in (0, 1) and b >= 1 and b <= 4").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_size(0);
            });

            e.execute_cql("drop materialized view vcf").get();
            e.execute_cql("drop table cf").get();
        }
    });
}

// Another test for issue #2367.
SEASTAR_TEST_CASE(test_clustering_key_in_restrictions) {
    return do_with_cql_env_thread([] (auto& e) {
        for (auto&& pk : {"((a, b), c)", "((b, a), c)", "(a, b, c)", "(c, b, a)", "((c, a), b)"}) {
            e.execute_cql("create table cf (a int, b int, c int, d int, primary key (a, b, c))").get();
            e.execute_cql(sprint("create materialized view vcf as select * from cf "
                                 "where a is not null and b IN (1, 2) and c is not null "
                                 "primary key %s", pk)).get();

            e.execute_cql("insert into cf (a, b, c, d) values (0, 0, 0, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (0, 0, 1, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (0, 1, 0, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (0, 1, 1, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 0, 0, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 0, 1, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 1, 0, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 2, 1, 0)").get();

            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});

            e.execute_cql("update cf set d = 1 where a = 1 and b = 0 and c = 0").get();
            msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});

            e.execute_cql("update cf set d = 1 where a = 0 and b = 1 and c = 0").get();
            msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)} },
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});

            e.execute_cql("delete from cf where a = 0 and b = 0 and c = 0").get();
            msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)} },
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});

            e.execute_cql("delete from cf where a = 1 and b = 1 and c = 0").get();
            msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)} },
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});

            e.execute_cql("delete from cf where a in (0, 1) and b >= 1 and b <= 4").get();
            msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_size(0);

            e.execute_cql("drop materialized view vcf").get();
            e.execute_cql("drop table cf").get();
        }
    });
}

SEASTAR_TEST_CASE(test_clustering_key_multi_column_restrictions) {
    return do_with_cql_env_thread([] (auto& e) {
        for (auto&& pk : {"((a, b), c)", "((b, a), c)", "(a, b, c)", "(c, b, a)", "((c, a), b)"}) {
            e.execute_cql("create table cf (a int, b int, c int, d int, primary key (a, b, c))").get();
            e.execute_cql(sprint("create materialized view vcf as select * from cf "
                                 "where a is not null and (b, c) >= (1, 0) "
                                 "primary key %s", pk)).get();

            e.execute_cql("insert into cf (a, b, c, d) values (0, 0, 0, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (0, 0, 1, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (0, 1, 0, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (0, 1, 1, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 0, 0, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 0, 1, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 1, -1, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 1, 0, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 1, 1, 0)").get();

            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("update cf set d = 1 where a = 1 and b = 0 and c = 0").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("update cf set d = 1 where a = 0 and b = 1 and c = 0").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)} },
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("delete from cf where a = 0 and b = 0 and c = 0").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)} },
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("delete from cf where a = 1 and b = 1 and c = 0").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)} },
                            { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                            { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("delete from cf where a in (0, 1)").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_size(0);
            });

            e.execute_cql("drop materialized view vcf").get();
            e.execute_cql("drop table cf").get();
        }
    });
}

SEASTAR_TEST_CASE(test_clustering_key_filtering_restrictions) {
    return do_with_cql_env_thread([] (auto& e) {
        for (auto&& pk : {"((a, b), c)", "((b, a), c)", "(a, b, c)", "(c, b, a)", "((c, a), b)"}) {
            e.execute_cql("create table cf (a int, b int, c int, d int, primary key (a, b, c))").get();
            e.execute_cql(sprint("create materialized view vcf as select * from cf "
                                 "where a is not null and b is not null and c = 1 "
                                 "primary key %s", pk)).get();

            e.execute_cql("insert into cf (a, b, c, d) values (0, 0, 0, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (0, 0, 1, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (0, 1, 0, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (0, 1, 1, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 0, 0, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 0, 1, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 1, -1, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 1, 0, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 1, 1, 0)").get();

            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                        { {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                        { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                        { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                        { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("update cf set d = 1 where a = 1 and b = 0 and c = 0").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                        { {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                        { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                        { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                        { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("update cf set d = 1 where a = 0 and b = 1 and c = 1").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                        { {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                        { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)} },
                        { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                        { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("delete from cf where a = 0 and b = 0 and c = 0").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                        { {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                        { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)} },
                        { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                        { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("delete from cf where a = 1 and b = 1 and c = 1").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                        { {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                        { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)} },
                        { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("delete from cf where a in (0, 1)").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_size(0);
            });

            e.execute_cql("drop materialized view vcf").get();
            e.execute_cql("drop table cf").get();
        }
    });
}

SEASTAR_TEST_CASE(test_partition_key_and_clustering_key_filtering_restrictions) {
    return do_with_cql_env_thread([] (auto& e) {
        for (auto&& pk : {"((a, b), c)", "((b, a), c)", "(a, b, c)", "(c, b, a)", "((c, a), b)"}) {
            e.execute_cql("create table cf (a int, b int, c int, d int, primary key (a, b, c))").get();
            e.execute_cql(sprint("create materialized view vcf as select * from cf "
                                 "where a = 1 and b is not null and c = 1 "
                                 "primary key %s", pk)).get();

            e.execute_cql("insert into cf (a, b, c, d) values (0, 0, 0, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (0, 0, 1, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (0, 1, 0, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (0, 1, 1, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 0, 0, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 0, 1, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 1, -1, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 1, 0, 0)").get();
            e.execute_cql("insert into cf (a, b, c, d) values (1, 1, 1, 0)").get();

            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                        { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                        { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("update cf set d = 1 where a = 0 and b = 0 and c = 0").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                        { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                        { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("update cf set d = 1 where a = 1 and b = 1 and c = 1").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                        { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                        { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
            });

            e.execute_cql("delete from cf where a = 0 and b = 0 and c = 0").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                        { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                        { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
            });

            e.execute_cql("delete from cf where a = 1 and b = 1 and c = 1").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                        { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
            });

            e.execute_cql("delete from cf where a = 1").get();
            eventually([&] {
            auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
            assert_that(msg).is_rows().with_size(0);
            });

            e.execute_cql("drop materialized view vcf").get();
            e.execute_cql("drop table cf").get();
        }
    });
}

SEASTAR_TEST_CASE(test_restrictions_on_all_types) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create type myType (a int, b uuid, c set<text>)").get();
        auto column_names = ::join(", ", std::vector<sstring>({
            "asciival",
            "bigintval",
            "blobval",
            "booleanval",
            "dateval",
            "decimalval",
            "doubleval",
            "floatval",
            "inetval",
            "intval",
            "textval",
            "timeval",
            "timestampval",
            "timeuuidval",
            "uuidval",
            "varcharval",
            "varintval",
            "frozenlistval",
            "frozensetval",
            "frozenmapval",
            "tupleval",
            "udtval"}));
        e.execute_cql(sprint("create table cf ("
                    "asciival ascii, "
                    "bigintval bigint, "
                    "blobval blob, "
                    "booleanval boolean, "
                    "dateval date, "
                    "decimalval decimal, "
                    "doubleval double, "
                    "floatval float, "
                    "inetval inet, "
                    "intval int, "
                    "textval text, "
                    "timeval time, "
                    "timestampval timestamp, "
                    "timeuuidval timeuuid, "
                    "uuidval uuid,"
                    "varcharval varchar, "
                    "varintval varint, "
                    "frozenlistval frozen<list<int>>, "
                    "frozensetval frozen<set<uuid>>, "
                    "frozenmapval frozen<map<ascii, int>>,"
                    "tupleval frozen<tuple<int, ascii, uuid>>,"
                    "udtval frozen<myType>, primary key (%s))", column_names)).get();

        e.execute_cql(sprint("create materialized view vcf as select * from cf where "
                "asciival = 'abc' AND "
                "bigintval = 123 AND "
                "blobval = 0xfeed AND "
                "booleanval = true AND "
                "dateval = '1987-03-23' AND "
                "decimalval = 123.123 AND "
                "doubleval = 123.123 AND "
                "floatval = 123.123 AND "
                "inetval = '127.0.0.1' AND "
                "intval = 123 AND "
                "textval = 'abc' AND "
                "timeval = '07:35:07.000111222' AND "
                "timestampval = 123123123 AND "
                "timeuuidval = 6BDDC89A-5644-11E4-97FC-56847AFE9799 AND "
                "uuidval = 6BDDC89A-5644-11E4-97FC-56847AFE9799 AND "
                "varcharval = 'abc' AND "
                "varintval = 123123123 AND "
                "frozenlistval = [1, 2, 3] AND "
                "frozensetval = {6BDDC89A-5644-11E4-97FC-56847AFE9799} AND "
                "frozenmapval = {'a': 1, 'b': 2} AND "
                "tupleval = (1, 'foobar', 6BDDC89A-5644-11E4-97FC-56847AFE9799) AND "
                "udtval = {a: 1, b: 6BDDC89A-5644-11E4-97FC-56847AFE9799, c: {'foo', 'bar'}} "
                "PRIMARY KEY (%s)", column_names)).get();

        e.execute_cql(sprint("insert into cf (%s) values ( "
                "'abc',"
                "123,"
                "0xfeed,"
                "true,"
                "'1987-03-23',"
                "123.123,"
                "123.123,"
                "123.123,"
                "'127.0.0.1',"
                "123,"
                "'abc',"
                "'07:35:07.000111222',"
                "123123123,"
                "6BDDC89A-5644-11E4-97FC-56847AFE9799,"
                "6BDDC89A-5644-11E4-97FC-56847AFE9799,"
                "'abc',"
                "123123123,"
                "[1, 2, 3],"
                "{6BDDC89A-5644-11E4-97FC-56847AFE9799},"
                "{'a': 1, 'b': 2},"
                "(1, 'foobar', 6BDDC89A-5644-11E4-97FC-56847AFE9799),"
                "{a: 1, b: 6BDDC89A-5644-11E4-97FC-56847AFE9799, c: {'foo', 'bar'}})", column_names)).get();

        eventually([&] {
        auto msg = e.execute_cql("select * from vcf").get0();
        assert_that(msg).is_rows().with_size(1);
        });
    });
}

// Test a view defined by a SELECT which filters by a non-primary key column
// which also happens to be a new primary key column in the view.
// This used to cause problems (see issue #3430), but no longer does.
// We still have problems in issue #3430 when one non-PK column is filtered,
// and a different one is added to the view's PK (see other tests below).
SEASTAR_TEST_CASE(test_non_primary_key_restrictions) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (a int, b int, c int, d int, primary key (a, b))").get();
        e.execute_cql("create materialized view vcf as select * from cf "
                      "where a is not null and b is not null and c is not null and c = 1"
                      "primary key (a, b, c)").get();

        e.execute_cql("insert into cf (a, b, c, d) values (0, 0, 0, 0)").get();
        e.execute_cql("insert into cf (a, b, c, d) values (0, 0, 1, 0)").get();
        e.execute_cql("insert into cf (a, b, c, d) values (0, 1, 0, 0)").get();
        e.execute_cql("insert into cf (a, b, c, d) values (0, 1, 1, 0)").get();
        e.execute_cql("insert into cf (a, b, c, d) values (1, 0, 0, 0)").get();
        e.execute_cql("insert into cf (a, b, c, d) values (1, 0, 1, 0)").get();
        e.execute_cql("insert into cf (a, b, c, d) values (1, 1, 0, 0)").get();
        e.execute_cql("insert into cf (a, b, c, d) values (1, 1, 1, 0)").get();

        BOOST_TEST_PASSPOINT();
        eventually([&] {
        auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
        assert_that(msg).is_rows().with_rows_ignore_order({
                        { {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                        { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                        { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                        { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
        });

        // Insert new rows that do not match the filter c=1, so will cause no
        // change to the view table:
        BOOST_TEST_PASSPOINT();
        e.execute_cql("insert into cf (a, b, c, d) values (2, 0, 0, 0)").get();
        e.execute_cql("insert into cf (a, b, c, d) values (2, 1, 2, 0)").get();
        eventually([&] {
        auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
        assert_that(msg).is_rows().with_rows_ignore_order({
                        { {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                        { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                        { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                        { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
        });

        // Insert two new base rows that do match the filter c=1, so will
        // add new view rows as well. This test is superfluous, as above
        // we already added 4 rows in the same fashion.
        BOOST_TEST_PASSPOINT();
        e.execute_cql("insert into cf (a, b, c, d) values (1, 2, 1, 0)").get();
        e.execute_cql("insert into cf (a, b, c, d) values (1, 3, 1, 0)").get();
        eventually([&] {
        auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
        assert_that(msg).is_rows().with_rows_ignore_order({
                        { {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                        { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                        { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                        { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                        { {int32_type->decompose(1)}, {int32_type->decompose(2)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                        { {int32_type->decompose(1)}, {int32_type->decompose(3)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
        });

        // Delete one of the rows we just added which matches the filter,
        // so a view row will also be removed.
        BOOST_TEST_PASSPOINT();
        e.execute_cql("delete from cf where a = 1 and b = 2").get();
        eventually([&] {
        auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
        assert_that(msg).is_rows().with_rows_ignore_order({
                        { {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                        { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                        { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                        { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                        { {int32_type->decompose(1)}, {int32_type->decompose(3)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
        });

        // Change the c on one of the rows we just added from 1 to 0.
        // Because it previously had c=1, it had a matching view row, but
        // now that it has c=0 this view row will have to be deleted.
        // A row with a=1,b=3 will still exist in the base table, but not
        // in the view table.
        BOOST_TEST_PASSPOINT();
        e.execute_cql("update cf set c = 0 where a = 1 and b = 3").get();
        eventually([&] {
        auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
        assert_that(msg).is_rows().with_rows_ignore_order({
                        { {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                        { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                        { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                        { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
        });

        // Change the c on the row which now has c=0 back to c=1, should
        // cause the view row to be added again.
        BOOST_TEST_PASSPOINT();
        e.execute_cql("update cf set c = 1 where a = 1 and b = 3").get();
        eventually([&] {
        auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
        assert_that(msg).is_rows().with_rows_ignore_order({
                        { {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                        { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                        { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                        { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                        { {int32_type->decompose(1)}, {int32_type->decompose(3)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
        });

        // Finally delete this row that now has c=1. The view row should also
        // get deleted (as we've already tested above).
        BOOST_TEST_PASSPOINT();
        e.execute_cql("delete from cf where a = 1 and b = 3").get();
        eventually([&] {
        auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
        assert_that(msg).is_rows().with_rows_ignore_order({
                        { {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                        { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                        { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                        { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
        });

        // The following update creates a new base row, which doesn't have c=1
        // (it has an empty c) so it will not create a new view row or change
        // any existing view row.
        BOOST_TEST_PASSPOINT();
        e.execute_cql("update cf set d = 1 where a = 0 and b = 2").get();
        eventually([&] {
        auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
        assert_that(msg).is_rows().with_rows_ignore_order({
                        { {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                        { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                        { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                        { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
        });

        // This sets d=1 on a base row which already exists and has c=1,
        // matching the view's filter, so the data also appears in the view
        // row:
        e.execute_cql("update cf set d = 1 where a = 1 and b = 1").get();
        BOOST_TEST_PASSPOINT();
        eventually([&] {
        auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
        assert_that(msg).is_rows().with_rows_ignore_order({
                        { {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                        { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                        { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                        { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
        });

        // This deletes a base row we created above which didn't have c=1
        // so a view row was not created for it, c is still not 1 and now
        // now we don't need to delete any view row.
        e.execute_cql("delete from cf where a = 0 and b = 2").get();
        BOOST_TEST_PASSPOINT();
        eventually([&] {
        auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
        assert_that(msg).is_rows().with_rows_ignore_order({
                        { {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                        { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                        { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                        { {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
        });

        // This deletes a row which does have c=1, so it matches the view
        // filter and has a corresponding view row which should be deleted
        e.execute_cql("delete from cf where a = 1 and b = 1").get();
        BOOST_TEST_PASSPOINT();
        eventually([&] {
        auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
        assert_that(msg).is_rows().with_rows_ignore_order({
                        { {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                        { {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} },
                        { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
        });

        // Delete an entire partition. This partition has two rows, both match
        // the view filter c=1, and cause two view rows to also be deleted.
        e.execute_cql("delete from cf where a = 0").get();
        BOOST_TEST_PASSPOINT();
        eventually([&] {
        auto msg = e.execute_cql("select a, b, c, d from vcf").get0();
        assert_that(msg).is_rows().with_rows_ignore_order({
                        { {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(1)}, {int32_type->decompose(0)} }});
        });
    });
}

// This is an test of a view filtered by a non-key column (a column which is
// neither in the base's primary key, nor the view primary key).
// The unique difficulty with filtering by a non-key column is that the value
// of such column can be *updated* - and also be expired with TTL - so the
// question of whether a base row matches or doesn't match the filter can
// change. That means we may need to remove and re-insert the same view row
// when one of the columns is modified back and forth.
// The following two tests, test_non_primary_key_restrictions_update()
// and test_non_primary_key_restrictions_ttl(), reproduces issue #3430
// in two ways, and still doesn't work today so is #if'ed out, replacing
// it in a test which verifies that such filtering is forbidden
#if 1
SEASTAR_TEST_CASE(test_non_primary_key_restrictions_forbidden) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (a int, b int, c int, primary key (a))").get();
        try {
            // currently we expect this to refuse to work, with the exception:
            // "exceptions::invalid_request_exception: Non-primary key columns
            // cannot be restricted in the SELECT statement used for
            // materialized view vcf creation (got restrictions on: c)"
            // This is because of issue #3430. When this issue is solved, the
            // #if 1 above should be changed to #if 0.
            e.execute_cql("create materialized view vcf as select * from cf "
                          "where a is not null and b is not null and c = 1"
                          "primary key (a, b)").get();
            BOOST_ASSERT(false);
        } catch (exceptions::invalid_request_exception&) { }

    });
}
#else
SEASTAR_TEST_CASE(test_non_primary_key_restrictions_update) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (a int, b int, c int, primary key (a))").get();
        e.execute_cql("create materialized view vcf as select * from cf "
                      "where a is not null and b is not null and c = 1"
                      "primary key (a, b)").get();
        // Insert a base row with c=0, which does not match the filter c=1.
        // The view will have no rows. Then change c from 0 to 1 and see the
        // row appear in the view, change it back to 0 and see it disappear,
        // and change it back to 1 to see it reappear.
        // We have a bug with the last re-appearance (the tombstone continues
        // to shadow the view row we wanted to re-add).
        BOOST_TEST_PASSPOINT();
        e.execute_cql("insert into cf (a, b, c) values (1, 11, 0)").get();
        eventually([&] {
            auto msg = e.execute_cql("select a, b, c from vcf").get0();
            assert_that(msg).is_rows().is_empty();
        });
        BOOST_TEST_PASSPOINT();
        e.execute_cql("update cf set c = 1 where a = 1").get();
        eventually([&] {
            auto msg = e.execute_cql("select a, b, c from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                { {int32_type->decompose(1)}, {int32_type->decompose(11)}, {int32_type->decompose(1)} }});
        });
        BOOST_TEST_PASSPOINT();
        e.execute_cql("update cf set c = 0 where a = 1").get();
        eventually([&] {
            auto msg = e.execute_cql("select a, b, c from vcf").get0();
            assert_that(msg).is_rows().is_empty();
        });
        BOOST_TEST_PASSPOINT();
        // The bug is here - when we set c = 1 again, we expect to see the
        // view row re-added. And it isn't.
        e.execute_cql("update cf set c = 1 where a = 1").get();
        eventually([&] {
            auto msg = e.execute_cql("select a, b, c from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                { {int32_type->decompose(1)}, {int32_type->decompose(11)}, {int32_type->decompose(1)} }});
        });
    });
    // TODO: when the above tests works, write a similar one just with multiple
    // columns in the in the filter (e.g., c = 1 and d = 1). These columns could
    // be modified with different timestamps, we need to make sure the row
    // deletions and insertions are also timestamped properly.
}

// This is another reproducer for #3430. While in the above test we updated
// column "c" to remove make it match and un-match the filter, here we use
// a TTL to expire c, and have it un-match the filter.
SEASTAR_TEST_CASE(test_non_primary_key_restrictions_ttl) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (a int, b int, c int, primary key (a))").get();
        e.execute_cql("create materialized view vcf as select * from cf "
                      "where a is not null and b is not null and c = 1"
                      "primary key (a, b)").get();
        // Insert a base row without c, and set c=1 (matching the filter)
        // with a TTL. The view will then have a row, but it should disappear
        // when the TTL expires.
        // We later re-add c=1, and expect to see the view row appear again.
        BOOST_TEST_PASSPOINT();
        e.execute_cql("insert into cf (a, b, c) values (1, 11, 0)").get();
        eventually([&] {
            auto msg = e.execute_cql("select a, b, c from vcf").get0();
            assert_that(msg).is_rows().is_empty();
        });
        BOOST_TEST_PASSPOINT();
        e.execute_cql("update cf using ttl 5 set c = 1 where a = 1").get();
        eventually([&] {
            auto msg = e.execute_cql("select a, b, c from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                { {int32_type->decompose(1)}, {int32_type->decompose(11)}, {int32_type->decompose(1)} }});
        });
        BOOST_TEST_PASSPOINT();
        // The bug was here: When c expires, we expect to see the view row
        // expire. Instead, the view row remained, and just its c column
        // expired.
        forward_jump_clocks(6s);
        eventually([&] {
            auto msg = e.execute_cql("select a, b, c from vcf").get0();
            assert_that(msg).is_rows().is_empty();
        });
        BOOST_TEST_PASSPOINT();
        // After the above passes, we also expect to be able to bring the
        // view row back to life by setting c = 1.
        e.execute_cql("update cf set c = 1 where a = 1").get();
        eventually([&] {
            auto msg = e.execute_cql("select a, b, c from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                { {int32_type->decompose(1)}, {int32_type->decompose(11)}, {int32_type->decompose(1)} }});
        });
    });
}
#endif

// In the above two reproducers for #3430, the column c was not part of the
// base table's key (as we explained, this is important) but also wasn't in
// the view's key. In this test, we make c part of the view's key. This makes
// things easier for Scylla, because anyway modifying c (which is part of the
// view key) is expected to add or remove entire rows and we have mechanisms
// to deal with that (properly timestamped shadowable tombstones). The
// following two tests with the "vk" (view key) suffix worked. Let's make sure
// it continues to work.
SEASTAR_TEST_CASE(test_non_primary_key_restrictions_update_vk) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (a int, c int, primary key (a))").get();
        e.execute_cql("create materialized view vcf as select * from cf "
                      "where a is not null and c is not null and c = 1"
                      "primary key (a, c)").get();
        // Insert a base row with c=0, which does not match the filter c=1.
        // The view will have no rows. Then change c from 0 to 1 and see the
        // row appear in the view, change it back to 0 and see it disappear,
        // and change it back to 1 to see it reappear.
        // We have a bug with the last re-appearance (the tombstone continues
        // to shadow the view row we wanted to re-add).
        BOOST_TEST_PASSPOINT();
        e.execute_cql("insert into cf (a, c) values (1, 0)").get();
        eventually([&] {
            auto msg = e.execute_cql("select a, c from vcf").get0();
            assert_that(msg).is_rows().is_empty();
        });
        BOOST_TEST_PASSPOINT();
        e.execute_cql("update cf set c = 1 where a = 1").get();
        eventually([&] {
            auto msg = e.execute_cql("select a, c from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                { {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
        });
        BOOST_TEST_PASSPOINT();
        e.execute_cql("update cf set c = 0 where a = 1").get();
        eventually([&] {
            auto msg = e.execute_cql("select a, c from vcf").get0();
            assert_that(msg).is_rows().is_empty();
        });
        BOOST_TEST_PASSPOINT();
        // The bug is here - when we set c = 1 again, we expect to see the
        // view row re-added. And it isn't.
        e.execute_cql("update cf set c = 1 where a = 1").get();
        eventually([&] {
            auto msg = e.execute_cql("select a, c from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                { {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
        });
    });
}
SEASTAR_TEST_CASE(test_non_primary_key_restrictions_ttl_vk) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (a int, c int, primary key (a))").get();
        e.execute_cql("create materialized view vcf as select * from cf "
                      "where a is not null and c is not null and c = 1"
                      "primary key (a, c)").get();
        // Insert a base row without c, and set c=1 (matching the filter)
        // with a TTL. The view will then have a row, but it should disappear
        // when the TTL expires.
        // We later re-add c=1, and expect to see the view row appear again.
        BOOST_TEST_PASSPOINT();
        e.execute_cql("insert into cf (a, c) values (1, 0)").get();
        eventually([&] {
            auto msg = e.execute_cql("select a, c from vcf").get0();
            assert_that(msg).is_rows().is_empty();
        });
        BOOST_TEST_PASSPOINT();
        e.execute_cql("update cf using ttl 5 set c = 1 where a = 1").get();
        eventually([&] {
            auto msg = e.execute_cql("select a, c from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                { {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
        });
        BOOST_TEST_PASSPOINT();
        forward_jump_clocks(6s);
        eventually([&] {
            auto msg = e.execute_cql("select a, c from vcf").get0();
            assert_that(msg).is_rows().is_empty();
        });
        BOOST_TEST_PASSPOINT();
        e.execute_cql("update cf set c = 1 where a = 1").get();
        eventually([&] {
            auto msg = e.execute_cql("select a, c from vcf").get0();
            assert_that(msg).is_rows().with_rows_ignore_order({
                { {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
        });
    });
}

// Test reproducing https://issues.apache.org/jira/browse/CASSANDRA-10910
SEASTAR_TEST_CASE(test_restricted_regular_column_timestamp_updates) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (k int primary key, c int, val int)").get();
        e.execute_cql("create materialized view vcf as select * from cf "
                      "where k is not null and c is not null and c = 1"
                      "primary key (k ,c)").get();

        e.execute_cql("update cf using timestamp 1 set c = 0, val = 0 where k = 0").get();
        e.execute_cql("update cf using timestamp 3 set c = 1 where k = 0").get();
        e.execute_cql("update cf using timestamp 2 set val = 1 where k = 0").get();
        e.execute_cql("update cf using timestamp 4 set c = 1 where k = 0").get();
        e.execute_cql("update cf using timestamp 3 set val = 2 where k = 0").get();
        eventually([&] {
        auto msg = e.execute_cql("select c, k, val from vcf").get0();
        assert_that(msg).is_rows().with_rows_ignore_order({{ {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(2)} }});
        });
    });
}

SEASTAR_TEST_CASE(test_old_timestamps_with_restrictions) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (k int, c int, val text, primary key (k, c))").get();
        e.execute_cql("create materialized view vcf as select * from cf "
                      "where k is not null and c is not null and val is not null "
                      "primary key (val, k ,c)").get();

        for (auto i = 0; i < 100; ++i) {
            e.execute_cql(sprint("insert into cf (k, c, val) values (0, %d, 'baz') using timestamp 300", i % 2)).get();
        }

        eventually([&] {
        auto msg = e.execute_cql("select * from vcf").get0();
        assert_that(msg).is_rows().with_size(2);
        msg = e.execute_cql("select c from vcf where val = 'baz'").get0();
        assert_that(msg).is_rows().with_rows({ {{int32_type->decompose(0)}}, {{int32_type->decompose(1)}} });
        });

        // Make sure an old TS does nothing
        e.execute_cql("update cf using timestamp 100 set val = 'bar' where k = 0 and c = 1").get();
        eventually([&] {
        auto msg = e.execute_cql("select c from vcf where val = 'baz'").get0();
        assert_that(msg).is_rows().with_rows({ {{int32_type->decompose(0)}}, {{int32_type->decompose(1)}} });
        msg = e.execute_cql("select c from vcf where val = 'bar'").get0();
        assert_that(msg).is_rows().with_size(0);
        });

        // Latest TS
        e.execute_cql("update cf using timestamp 500 set val = 'bar' where k = 0 and c = 1").get();
        eventually([&] {
        auto msg = e.execute_cql("select c from vcf where val = 'baz'").get0();
        assert_that(msg).is_rows().with_rows({ {{int32_type->decompose(0)}} });
        msg = e.execute_cql("select c from vcf where val = 'bar'").get0();
        assert_that(msg).is_rows().with_rows({ {{int32_type->decompose(1)}} });
        });
    });
}

void do_complex_restricted_timestamp_update_test(cql_test_env& e, std::function<void()>&& maybe_flush) {
    e.execute_cql("create table cf (p int, c int, v1 int, v2 int, v3 int, primary key (p, c))").get();
    e.execute_cql("create materialized view vcf as select * from cf "
                  "where p is not null and c is not null and v1 is not null "
                  "primary key (v1, p, c)").get();

    // Set initial values TS=0, matching the restriction and verify view
    e.execute_cql("insert into cf (p, c, v1, v2) values (0, 0, 1, 0) using timestamp 0").get();
    eventually([&] {
    auto msg = e.execute_cql("select * from vcf").get0();
    assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(0)}, {} }});
    });

    // Update v1's timestamp TS=2
    e.execute_cql("update cf using timestamp 2 set v1 = 1 where p = 0 and c = 0").get();
    maybe_flush();
    eventually([&] {
    auto msg = e.execute_cql("select v2 from vcf where v1 = 1 and p = 0 and c = 0").get0();
    assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(0)} }});
    });

    // Update v1 @ TS=3, tombstones v1=1 and tries to add v1=0 partition
    e.execute_cql("update cf using timestamp 3 set v1 = 0 where p = 0 and c = 0").get();
    maybe_flush();
    eventually([&] {
    auto msg = e.execute_cql("select v2 from vcf where v1 = 0 and p = 0 and c = 0").get0();
    assert_that(msg).is_rows().with_size(1);
    });

    // Update v1 back to 1 with TS=4
    e.execute_cql("update cf using timestamp 4 set v1 = 1 where p = 0 and c = 0").get();
    maybe_flush();
    eventually([&] {
    auto msg = e.execute_cql("select v2, v3 from vcf where v1 = 1 and p = 0 and c = 0").get0();
    assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(0)}, {} }});
    });

    // Add v3 @ TS=1
    e.execute_cql("update cf using timestamp 1 set v3 = 1 where p = 0 and c = 0").get();
    maybe_flush();
    eventually([&] {
    auto msg = e.execute_cql("select v2, v3 from vcf where v1 = 1 and p = 0 and c = 0").get0();
    assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(0)}, {int32_type->decompose(1)} }});
    });

    // Update v2 @ TS=2
    e.execute_cql("update cf using timestamp 2 set v2 = 2 where p = 0 and c = 0").get();
    maybe_flush();
    eventually([&] {
    auto msg = e.execute_cql("select v2 from vcf where v1 = 1 and p = 0 and c = 0").get0();
    assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(2)} }});
    });

    // Update v2 @ TS=3
    e.execute_cql("update cf using timestamp 3 set v2 = 1 where p = 0 and c = 0").get();
    maybe_flush();
    eventually([&] {
    auto msg = e.execute_cql("select v2 from vcf where v1 = 1 and p = 0 and c = 0").get0();
    assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(1)} }});
    });

    // Tombstone v1
    e.execute_cql("delete from cf using timestamp 5 where p = 0 and c = 0").get();
    eventually([&] {
    auto msg = e.execute_cql("select v2 from vcf").get0();
    assert_that(msg).is_rows().with_size(0);
    });

    // Add the row back without v2
    e.execute_cql("insert into cf (p, c, v1) values (0, 0, 1) using timestamp 6").get();
    // Make sure v2 doesn't pop back in.
    eventually([&] {
    auto msg = e.execute_cql("select v2 from vcf where v1 = 1 and p = 0 and c = 0").get0();
    assert_that(msg).is_rows().with_rows({{ {} }});
    });

    // New partition
    // Insert a row @ TS=0
    e.execute_cql("insert into cf (p, c, v1, v2, v3) values (1, 0, 1, 0, 0) using timestamp 0").get();

    // Overwrite PK, v1 and v3 @ TS=1, but don't overwrite v2
    e.execute_cql("insert into cf (p, c, v1, v3) values (1, 0, 1, 0) using timestamp 1").get();

    // Delete @ TS=0 (which should only delete v2)
    e.execute_cql("delete from cf using timestamp 0 where p = 1 and c = 0").get();
    eventually([&] {
    auto msg = e.execute_cql("select * from vcf where v1 = 1 and p = 1 and c = 0").get0();
    assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {}, {int32_type->decompose(0)} }});
    });

    e.execute_cql("update cf using timestamp 2 set v1 = 1 where p = 1 and c = 1").get();
    maybe_flush();
    e.execute_cql("update cf using timestamp 3 set v1 = 1 where p = 1 and c = 0").get();
    maybe_flush();
    eventually([&] {
    auto msg = e.execute_cql("select * from vcf where v1 = 1 and p = 1 and c = 0").get0();
    assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {}, {int32_type->decompose(0)} }});
    });

    e.execute_cql("update cf using timestamp 3 set v2 = 0 where p = 1 and c = 0").get();
    maybe_flush();
    eventually([&] {
    auto msg = e.execute_cql("select * from vcf where v1 = 1 and p = 1 and c = 0").get0();
    assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(1)}, {int32_type->decompose(1)}, {int32_type->decompose(0)}, {int32_type->decompose(0)}, {int32_type->decompose(0)} }});
    });
}

SEASTAR_TEST_CASE(complex_restricted_timestamp_update_test) {
    return do_with_cql_env_thread([] (auto& e) {
        do_complex_restricted_timestamp_update_test(e, [] { });
    });
}

SEASTAR_TEST_CASE(complex_restricted_timestamp_update_test_with_flush) {
    db::config cfg;
    cfg.enable_cache(false);
    return do_with_cql_env_thread([] (auto& e) {
        do_complex_restricted_timestamp_update_test(e, [&] {
            e.local_db().flush_all_memtables().get();
        });
    }, cfg);
}

void complex_timestamp_with_base_pk_columns_in_view_pk_deletion_test(cql_test_env& e, std::function<void()>&& maybe_flush) {
    e.execute_cql("create table cf (p int, c int, v1 int, v2 int, primary key (p, c))").get();
    e.execute_cql("create materialized view vcf as select * from cf "
                  "where p is not null and c is not null "
                  "primary key (c, p)").get();

    // Set initial values TS=1
    e.execute_cql("insert into cf (p, c, v1, v2) values (1, 2, 3, 4) using timestamp 1").get();
    maybe_flush();
    eventually([&] {
    auto msg = e.execute_cql("select v1, v2, WRITETIME(v2) from vcf where p = 1 and c = 2").get0();
    assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(3)}, {int32_type->decompose(4)}, {long_type->decompose(1L)} }});
    });

    // Delete row TS=2
    e.execute_cql("delete from cf using timestamp 2 where p = 1 and c = 2").get();
    maybe_flush();
    eventually([&] {
    auto msg = e.execute_cql("select * from vcf").get0();
    assert_that(msg).is_rows().with_size(0);
    });

    // Add PK @ TS=3
    e.execute_cql("insert into cf (p, c) values (1, 2) using timestamp 3").get();
    maybe_flush();
    eventually([&] {
    auto msg = e.execute_cql("select * from vcf").get0();
    assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(2)}, {int32_type->decompose(1)}, {}, {} }});
    });

    // Reset values TS=10
    e.execute_cql("insert into cf (p, c, v1, v2) values (1, 2, 3, 4) using timestamp 10").get();
    maybe_flush();
    eventually([&] {
    auto msg = e.execute_cql("select v1, v2, WRITETIME(v2) from vcf where p = 1 and c = 2").get0();
    assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(3)}, {int32_type->decompose(4)}, {long_type->decompose(10L)} }});
    });

    // Update values TS=20
    e.execute_cql("update cf using timestamp 20 set v2 = 5 where p = 1 and c = 2").get();
    maybe_flush();
    eventually([&] {
    auto msg = e.execute_cql("select v1, v2, WRITETIME(v2) from vcf where p = 1 and c = 2").get0();
    assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(3)}, {int32_type->decompose(5)}, {long_type->decompose(20L)} }});
    });

    // Delete row TS=10
    e.execute_cql("delete from cf using timestamp 10 where p = 1 and c = 2").get();
    maybe_flush();
    eventually([&] {
    auto msg = e.execute_cql("select v1, v2, WRITETIME(v2) from vcf").get0();
    assert_that(msg).is_rows().with_rows({{ { }, {int32_type->decompose(5)}, {long_type->decompose(20L)} }});
    });

    e.execute_cql("drop materialized view vcf").get();
    e.execute_cql("drop table cf").get();
}

void complex_timestamp_with_base_non_pk_columns_in_view_pk_deletion_test(cql_test_env& e, std::function<void()>&& maybe_flush) {
    e.execute_cql("create table cf (p int primary key, v1 int, v2 int)").get();
    e.execute_cql("create materialized view vcf as select * from cf "
                  "where p is not null and v1 is not null "
                  "primary key (v1, p)").get();

    // Set initial values TS=1
    e.execute_cql("insert into cf (p, v1, v2) values (3, 1, 5) using timestamp 1").get();
    maybe_flush();
    eventually([&] {
    auto msg = e.execute_cql("select v2, WRITETIME(v2) from vcf where v1 = 1 and p = 3").get0();
    assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(5)}, {long_type->decompose(1L)} }});
    });

    // Delete row TS=2
    e.execute_cql("delete from cf using timestamp 2 where p = 3").get();
    maybe_flush();
    eventually([&] {
    auto msg = e.execute_cql("select * from vcf").get0();
    assert_that(msg).is_rows().with_size(0);
    });

    // Add PK @ TS=3
    e.execute_cql("insert into cf (p, v1) values (3, 1) using timestamp 3").get();
    maybe_flush();
    eventually([&] {
    auto msg = e.execute_cql("select * from vcf").get0();
    assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(1)}, {int32_type->decompose(3)}, {} }});
    });

    // Insert v2 @ TS=2
    e.execute_cql("insert into cf (p, v1, v2) values (3, 1, 4) using timestamp 2").get();
    maybe_flush();
    eventually([&] {
    auto msg = e.execute_cql("select * from vcf").get0();
    assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(1)}, {int32_type->decompose(3)}, {} }});
    });

    // Insert v2 @ TS=3
    e.execute_cql("update cf using timestamp 3 set v2 = 4 where p = 3").get();
    maybe_flush();
    eventually([&] {
    auto msg = e.execute_cql("select v1, p, v2, WRITETIME(v2) from vcf").get0();
    assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(1)}, {int32_type->decompose(3)}, {int32_type->decompose(4)}, {long_type->decompose(3L)} }});
    });

   e.execute_cql("drop materialized view vcf").get();
   e.execute_cql("drop table cf").get();
}

SEASTAR_TEST_CASE(complex_timestamp_deletion_test) {
    return do_with_cql_env_thread([] (auto& e) {
        complex_timestamp_with_base_pk_columns_in_view_pk_deletion_test(e, [] { });
        complex_timestamp_with_base_non_pk_columns_in_view_pk_deletion_test(e, [] { });
    });
}

SEASTAR_TEST_CASE(complex_timestamp_deletion_test_with_flush) {
    db::config cfg;
    cfg.enable_cache(false);
    return do_with_cql_env_thread([] (auto& e) {
        complex_timestamp_with_base_pk_columns_in_view_pk_deletion_test(e, [&] {
            e.local_db().flush_all_memtables().get();
        });
        complex_timestamp_with_base_non_pk_columns_in_view_pk_deletion_test(e, [&] {
            e.local_db().flush_all_memtables().get();
        });
    }, cfg);
}

// Test that we are not allowed to create a view without the "is not null"
// restrictions on all the view's primary key columns.
// Actually, although most of Cassandra documentation suggests that IS NOT
// NULL is needed on all of the view's primary key columns, there's actually
// one case where this is optional: It optional on a column which is the
// base's only partition key column, because the partition key (in its
// entirety) is always guaranteed to be non-null. In all other cases, the
// IS NOT NULL specification it is mandatory.
// We want to be sure that in every case, the error is caught when creating
// the view - not later when adding data to the base table, as we discovered
// was happening in some cases in issue #2628.
SEASTAR_TEST_CASE(test_is_not_null) {
    return do_with_cql_env_thread([] (auto& e) {
        // Test 1: with one partition column in the base table.
        // This should work with the "where v is not null" restriction
        // on the view's new key column, but fail without it.
        // Adding a "where p is not null" restriction is not necessary for
        // the base's partition key (because they cannot be null), but
        // also not harmful.
        e.execute_cql("create table cf (p int PRIMARY KEY, v int, w int)").get();
        e.execute_cql("create materialized view vcf1 as select * from cf "
                      "where v is not null "
                      "primary key (v, p)").get();
        e.execute_cql("create materialized view vcf2 as select * from cf "
                      "where v is not null and p is not null "
                      "primary key (v, p)").get();
        try {
            // should fail, missing restriction on v (p is also missing, but
            // as can be seen from the success above, not mandatory).
            e.execute_cql("create materialized view vcf3 as select * from cf "
                          "primary key (v, p)").get();
            BOOST_ASSERT(false);
        } catch (exceptions::invalid_request_exception&) { }
        try {
            // should fail, missing restriction on v
            e.execute_cql("create materialized view vcf4 as select * from cf "
                          "where p is not null "
                          "primary key (v, p)").get();
            BOOST_ASSERT(false);
        } catch (exceptions::invalid_request_exception&) { }

        // Test adding rows to cf and all views on it which we succeeded adding
        // above. In issue #2628, we saw that the view creation was succeeding
        // above despite the missing "is not null", and then the updates here
        // were failing. This was wrong.
        e.execute_cql("insert into cf (p, v, w) values (1, 2, 3)").get();

        // Test 2: where the base table has a composite partition key.
        // It appears (see Cassandra's CreateViewStatement.getColumnIdentifier())
        // that when the partition key is composite (composed of multiple columns)
        // individual columns may be null, so we must have an IS NOT NULL
        // restriction on those (p1 and p2 below) too, and it's no longer optional.
        e.execute_cql("create table cf2 (p1 int, p2 int, v int, primary key ((p1, p2)))").get();
        e.execute_cql("create materialized view vcf24 as select * from cf2 "
                      "where p1 is not null and p2 is not null and v is not null "
                      "primary key (v, p1, p2)").get(); // this should succeed
        try {
            // should fail, missing restriction on p1
            e.execute_cql("create materialized view vcf21 as select * from cf2 "
                          "where p2 is not null and v is not null "
                          "primary key (v, p1, p2)").get();
            BOOST_ASSERT(false);
        } catch (exceptions::invalid_request_exception&) { }
        try {
            // should fail, missing restriction on p2
            e.execute_cql("create materialized view vcf22 as select * from cf2 "
                          "where p1 is not null and v is not null "
                          "primary key (v, p1, p2)").get();
            BOOST_ASSERT(false);
        } catch (exceptions::invalid_request_exception&) { }
        try {
            // should fail, missing restriction on v
            e.execute_cql("create materialized view vcf23 as select * from cf2 "
                         "where p1 is not null and p2 is not null "
                         "primary key (v, p1, p2)").get();
            BOOST_ASSERT(false);
        } catch (exceptions::invalid_request_exception&) { }
        e.execute_cql("insert into cf2 (p1, p2, v) values (1, 2, 3)").get();

        // Test 3: this time the base has a non-composite partition key p1,
        // but also a clustering key c. The IS NOT NULL can be omitted on p1,
        // but necessary on c, and on the new view primary key column - v:
        e.execute_cql("create table cf3 (p1 int, c int, v int, primary key (p1, c))").get();
        e.execute_cql("create materialized view vcf32 as select * from cf3 "
                      "where c is not null and v is not null and p1 is not null "
                      "primary key (v, p1, c)").get();
        e.execute_cql("create materialized view vcf31 as select * from cf3 "
                      "where c is not null and v is not null "   // fine to omit p1
                      "primary key (v, p1, c)").get();
        try {
            // should fail, missing restriction on c
            e.execute_cql("create materialized view vcf33 as select * from cf3 "
                          "where p1 is not null and v is not null "
                          "primary key (v, p1, c)").get();
            BOOST_ASSERT(false);
        } catch (exceptions::invalid_request_exception&) { }
        try {
            // should fail, missing restriction on v
            e.execute_cql("create materialized view vcf34 as select * from cf3 "
                          "where p1 is not null and c is not null "
                          "primary key (v, p1, c)").get();
            BOOST_ASSERT(false);
        } catch (exceptions::invalid_request_exception&) { }
        e.execute_cql("insert into cf3 (p1, c, v) values (1, 2, 3)").get();

        // FIXME: we should also test that beyond "IS NOT NULL" being
        // verified on view creation, it also does its job when adding
        // rows - that those with NULL values are properly ignored.
    });
}

// Test that it is forbidden to add more than one new column to the
// view's primary key beyond what was in the base's primary key.
SEASTAR_TEST_CASE(test_only_one_allowed) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (p int PRIMARY KEY, v int, w int)").get();
        try {
            e.execute_cql("create materialized view vcf as select * from cf "
                          "where v is not null and w is not null "
                          "primary key (v, w, p)").get();
            BOOST_ASSERT(false);
        } catch (exceptions::invalid_request_exception&) { }
    });
}

// Test that a view cannot be created without its primary key containing all
// columns of the base's primary key. This reproduces issue #2720.
SEASTAR_TEST_CASE(test_view_key_must_include_base_key) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (a int, b int, c int, primary key (a))").get();
        // Adding a column (b) to cf's primary key (a) is fine:
        e.execute_cql("create materialized view vcf1 as select * from cf "
                      "where a is not null and b is not null "
                      "primary key (b, a)").get();
        // But missing any of cf's primary columns in the view, is not.
        // Even before the fix to #2720 this case generated an error - but not
        // the expected error because of test order.
        try {
            e.execute_cql("create materialized view vcf2 as select * from cf "
                          "where b is not null "
                          "primary key (b)").get();
            BOOST_ASSERT(false);
        } catch (exceptions::invalid_request_exception&) { }

        // A slightly more elaborate case, which actually reproduces the
        // problem we had issue #2720 - in this case we didn't detect the
        // error of the missing key column.
        e.execute_cql("create table cf2 (a int, b int, c int, primary key (a, b))").get();
        try {
            e.execute_cql("create materialized view vcf21 as select * from cf2 "
                          "where c is not null and b is not null "
                          "primary key (c, b)").get();  // error: "a" is missing in this key.
        } catch (exceptions::invalid_request_exception&) { }
    });
}

SEASTAR_TEST_CASE(test_base_non_pk_columns_in_view_partition_key_are_non_emtpy) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("create table cf (p1 int, p2 text, c text, v text, primary key ((p1, p2), c))").get();
        e.execute_cql("insert into cf (p1, p2, c, v) values (1, '', '', '')").get();

        size_t id = 0;
        auto make_view_name = [&id] { return sprint("vcf_%d", id++); };

        auto views_matching = {
            "create materialized view %s as select * from cf "
            "where p1 is not null and p2 is not null and c is not null and v is not null "
            "primary key (p1, v, c, p2)",

            "create materialized view %s as select * from cf "
            "where p1 is not null and p2 is not null and c is not null and v is not null "
            "primary key ((p2, v), c, p1)",

            "create materialized view %s as select * from cf "
            "where p1 is not null and p2 is not null and c is not null and v is not null "
            "primary key ((v, p2), c, p1)",

            "create materialized view %s as select * from cf "
            "where p1 is not null and p2 is not null and c is not null and v is not null "
            "primary key ((c, v), p1, p2)",

            "create materialized view %s as select * from cf "
            "where p1 is not null and p2 is not null and c is not null and v is not null "
            "primary key ((v, c), p1, p2)"
        };
        for (auto&& view : views_matching) {
            auto name = make_view_name();
            auto f = e.local_view_builder().wait_until_built("ks", name);
            e.execute_cql(sprint(view, name)).get();
            f.get();
            auto msg = e.execute_cql(sprint("select p1, p2, c, v from %s", name)).get0();
            assert_that(msg).is_rows()
                    .with_size(1)
                    .with_row({
                            {int32_type->decompose(1)},
                            {utf8_type->decompose(data_value(""))},
                            {utf8_type->decompose(data_value(""))},
                            {utf8_type->decompose(data_value(""))},
                    });
        }

        auto views_not_matching = {
                "create materialized view %s as select * from cf "
                "where p1 is not null and p2 is not null and c is not null and v is not null "
                "primary key (c, p1, p2, v)",

                "create materialized view %s as select * from cf "
                "where p1 is not null and p2 is not null and c is not null and v is not null "
                "primary key (p2, p1, c, v)"
        };
        for (auto&& view : views_not_matching) {
            auto name = make_view_name();
            auto f = e.local_view_builder().wait_until_built("ks", name);
            e.execute_cql(sprint(view, name)).get();
            f.get();
            auto msg = e.execute_cql(sprint("select p1, p2, c, v from %s", name)).get0();
            assert_that(msg).is_rows().is_empty();
        }
        auto name = make_view_name();
        auto f = e.local_view_builder().wait_until_built("ks", name);
        e.execute_cql(sprint("create materialized view %s as select * from cf "
                             "where p1 is not null and p2 is not null and c is not null and v is not null "
                             "primary key (v, p1, p2, c)", name)).get();
        f.get();
        auto msg = e.execute_cql(sprint("select p1, p2, c, v from %s", name)).get0();
        assert_that(msg).is_rows().is_empty();

        e.local_db().flush_all_memtables().get();
        e.execute_cql("update cf set v = 'a' where p1 = 1 and p2 = '' and c = ''").get();
        eventually([&] {
            auto msg = e.execute_cql(sprint("select p1, p2, c, v from %s", name)).get0();
            assert_that(msg).is_rows()
                    .with_size(1)
                    .with_row({
                            {int32_type->decompose(1)},
                            {utf8_type->decompose(data_value(""))},
                            {utf8_type->decompose(data_value(""))},
                            {utf8_type->decompose("a")},
                    });
        });
        e.execute_cql("update cf set v = '' where p1 = 1 and p2 = '' and c = ''").get();
        eventually([&] {
            auto msg = e.execute_cql(sprint("select p1, p2, c, v from %s", name)).get0();
            assert_that(msg).is_rows().is_empty();
        });
        e.execute_cql("delete v from cf where p1 = 1 and p2 = '' and c = ''").get();
        eventually([&] {
            auto msg = e.execute_cql(sprint("select p1, p2, c, v from %s", name)).get0();
            assert_that(msg).is_rows().is_empty();
        });
    });
}

SEASTAR_TEST_CASE(test_alter_table_with_updates) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (p int, c int, v1 int, v2 int, primary key (p, c));").get();
        e.execute_cql("create materialized view vcf as select p, c, v1, v2 from cf "
                      "where p is not null and c is not null and v1 is not null and v2 is not null "
                      "primary key (v1, p, c)").get();
        e.execute_cql("update cf set v1 = 4, v2 = 5 where p = 1 and c = 1").get();
        e.execute_cql("alter table cf add f int;").get();
        e.execute_cql("alter table cf add o int;").get();
        e.execute_cql("alter table cf add t int;").get();
        e.execute_cql("alter table cf add x int;").get();
        e.execute_cql("alter table cf add z int;").get();
        e.execute_cql("update cf set v2 = 7 where p = 1 and c = 1").get();
        eventually([&] {
            auto msg = e.execute_cql("select p, c, v1, v2 from vcf").get0();
            assert_that(msg).is_rows()
                    .with_size(1)
                    .with_row({
                        {int32_type->decompose(1)},
                        {int32_type->decompose(1)},
                        {int32_type->decompose(4)},
                        {int32_type->decompose(7)},
                    });
        });
    });
}

// Test that a regular column which we did not add to the view is really
// not in the view. Even if to fix issue #3362 we add "virtual cells"
// for the unselected columns, those should not be visible to the end-user
// of the view table.
SEASTAR_TEST_CASE(test_unselected_column) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (p int, c int, x int, y list<int>, z set<int>, w map<int,int>, primary key (p, c))").get();
        e.execute_cql("create materialized view vcf as select p, c from cf "
                      "where p is not null and c is not null "
                      "primary key (c, p)").get();
        e.execute_cql("insert into cf (p, c, x) values (1, 2, 3)").get();
        BOOST_TEST_PASSPOINT();
        auto msg = e.execute_cql("select * from cf").get0();
        assert_that(msg).is_rows().with_size(1)
                .with_row({{int32_type->decompose(1)}, {int32_type->decompose(2)}, {}, {int32_type->decompose(3)}, {}, {}});
        BOOST_TEST_PASSPOINT();
        // Check that when we ask for all of vcf's columns, we only get the
        // ones we actually selected - c and p, not x, y, z, or w:
        eventually([&] {
            auto msg = e.execute_cql("select * from vcf").get0();
            assert_that(msg).is_rows().with_size(1)
                    .with_row({{int32_type->decompose(2)}, {int32_type->decompose(1)}});
        });
        // Check that we cannot explicitly select the x, y, z or w columns in
        // vcf as they are not one of the columns we selected for the view.
        try {
            e.execute_cql("select x from vcf").get();
            BOOST_ASSERT(false);
        } catch (exceptions::invalid_request_exception&) {
            // we expect: exceptions::invalid_request_exception: Undefined name x in selection clause
        }
        try {
            e.execute_cql("select y from vcf").get();
            BOOST_ASSERT(false);
        } catch (exceptions::invalid_request_exception&) {
        }
        try {
            e.execute_cql("select z from vcf").get();
            BOOST_ASSERT(false);
        } catch (exceptions::invalid_request_exception&) {
        }
        try {
            e.execute_cql("select w from vcf").get();
            BOOST_ASSERT(false);
        } catch (exceptions::invalid_request_exception&) {
        }
    });
}

SEASTAR_THREAD_TEST_CASE(node_view_update_backlog) {
    db::view::node_update_backlog b(2, 10ms);
    auto backlog = [] (size_t size) { return db::view::update_backlog{size, 1000}; };
    b.add_fetch(0, backlog(10));
    b.add_fetch(1, backlog(50));
    BOOST_REQUIRE(b.load() == backlog(10));
    sleep(11ms).get();
    b.add_fetch(1, backlog(100));
    BOOST_REQUIRE(b.load() == backlog(100));
}