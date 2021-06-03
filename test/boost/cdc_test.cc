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

#include <seastar/testing/thread_test_case.hh>
#include <string>
#include <boost/range/adaptor/map.hpp>

#include "cdc/log.hh"
#include "cdc/cdc_extension.hh"
#include "db/config.hh"
#include "schema_builder.hh"
#include "test/lib/cql_assertions.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/exception_utils.hh"
#include "test/lib/log.hh"
#include "transport/messages/result_message.hh"

#include "types.hh"
#include "types/tuple.hh"
#include "types/map.hh"
#include "types/list.hh"
#include "types/set.hh"
#include "types/user.hh"

using namespace std::string_literals;

namespace cdc {
api::timestamp_type find_timestamp(const mutation&);
utils::UUID generate_timeuuid(api::timestamp_type);
}

SEASTAR_THREAD_TEST_CASE(test_find_mutation_timestamp) {
    do_with_cql_env_thread([] (cql_test_env& e) {
        cquery_nofail(e, "CREATE TYPE ut (a int, b int)");
        cquery_nofail(e, "CREATE TABLE ks.t (pk int, ck int, vstatic int static, vint int, "
                "vmap map<int, int>, vfmap frozen<map<int, int>>, vut ut, vfut frozen<ut>, primary key (pk, ck))");

        auto schema = schema_builder("ks", "t")
            .with_column("pk", int32_type, column_kind::partition_key)
            .with_column("vstatic", int32_type, column_kind::static_column)
            .with_column("ck", int32_type, column_kind::clustering_key)
            .with_column("vint", int32_type)
            .with_column("vmap", map_type_impl::get_instance(int32_type, int32_type, true))
            .with_column("vfmap", map_type_impl::get_instance(int32_type, int32_type, false))
            .with_column("vut", user_type_impl::get_instance("ks", "ut", {to_bytes("a"), to_bytes("b")}, {int32_type, int32_type}, true))
            .with_column("vfut", user_type_impl::get_instance("ks", "ut", {to_bytes("a"), to_bytes("b")}, {int32_type, int32_type}, false))
            .build();

        auto check_stmt = [&] (const sstring& query) {
            auto muts = e.get_modification_mutations(query).get0();
            BOOST_REQUIRE(!muts.empty());

            for (auto& m: muts) {
                /* We want to check if `find_timestamp` truly returns this mutation's timestamp.
                 * The mutation was created very recently (in the `get_modification_mutations` call),
                 * so we can do it by comparing the returned timestamp with the current time
                 * -- the difference should be small.
                 */
                auto ts = cdc::find_timestamp(m);
                BOOST_REQUIRE(
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        api::timestamp_clock::duration(api::new_timestamp() - ts))
                    < std::chrono::milliseconds(5000));
            }
        };

        check_stmt("INSERT INTO ks.t (pk, ck) VALUES (0, 0)");
        check_stmt("INSERT INTO ks.t (pk, ck, vint) VALUES (0,0,0)");
        check_stmt("INSERT INTO ks.t (pk, ck, vmap) VALUES (0,0,{0:0})");
        check_stmt("INSERT INTO ks.t (pk, ck, vfmap) VALUES (0,0,{0:0})");
        check_stmt("INSERT INTO ks.t (pk, ck, vut) VALUES (0,0,{b:0})");
        check_stmt("INSERT INTO ks.t (pk, ck, vfut) VALUES (0,0,{b:0})");
        check_stmt("INSERT INTO ks.t (pk, ck, vint) VALUES (0,0,null)");
        check_stmt("INSERT INTO ks.t (pk, ck, vmap) VALUES (0,0,null)");
        check_stmt("INSERT INTO ks.t (pk, ck, vfmap) VALUES (0,0,null)");
        check_stmt("INSERT INTO ks.t (pk, ck, vut) VALUES (0,0,null)");
        check_stmt("INSERT INTO ks.t (pk, ck, vfut) VALUES (0,0,null)");
        check_stmt("INSERT INTO ks.t (pk, vstatic) VALUES (0, 0)");
        check_stmt("UPDATE ks.t SET vint = 0 WHERE pk = 0 AND ck = 0");
        check_stmt("UPDATE ks.t SET vmap = {0:0} WHERE pk = 0 AND ck = 0");
        check_stmt("UPDATE ks.t SET vmap[0] = 0 WHERE pk = 0 AND ck = 0");
        check_stmt("UPDATE ks.t SET vfmap = {0:0} WHERE pk = 0 AND ck = 0");
        check_stmt("UPDATE ks.t SET vut = {b:0} WHERE pk = 0 AND ck = 0");
        check_stmt("UPDATE ks.t SET vut.b = 0 WHERE pk = 0 AND ck = 0");
        check_stmt("UPDATE ks.t SET vfut = {b:0} WHERE pk = 0 AND ck = 0");
        check_stmt("UPDATE ks.t SET vint = null WHERE pk = 0 AND ck = 0");
        check_stmt("UPDATE ks.t SET vmap = null WHERE pk = 0 AND ck = 0");
        check_stmt("UPDATE ks.t SET vmap[0] = null WHERE pk = 0 AND ck = 0");
        check_stmt("UPDATE ks.t SET vfmap = null WHERE pk = 0 AND ck = 0");
        check_stmt("UPDATE ks.t SET vut = null WHERE pk = 0 AND ck = 0");
        check_stmt("UPDATE ks.t SET vut.b = null WHERE pk = 0 AND ck = 0");
        check_stmt("UPDATE ks.t SET vfut = null WHERE pk = 0 AND ck = 0");
        check_stmt("UPDATE ks.t SET vstatic = 0 WHERE pk = 0");
        check_stmt("DELETE FROM ks.t WHERE pk = 0 and ck = 0");
        check_stmt("DELETE FROM ks.t WHERE pk = 0 and ck > 0");
        check_stmt("DELETE FROM ks.t WHERE pk = 0 and ck > 0 and ck <= 1");
        check_stmt("DELETE FROM ks.t WHERE pk = 0");
        check_stmt("DELETE vint FROM t WHERE pk = 0 AND ck = 0");
        check_stmt("DELETE vmap FROM t WHERE pk = 0 AND ck = 0");
        check_stmt("DELETE vmap[0] FROM t WHERE pk = 0 AND ck = 0");
        check_stmt("DELETE vfmap FROM t WHERE pk = 0 AND ck = 0");
        check_stmt("DELETE vut FROM t WHERE pk = 0 AND ck = 0");
        check_stmt("DELETE vut.b FROM t WHERE pk = 0 AND ck = 0");
        check_stmt("DELETE vfut FROM t WHERE pk = 0 AND ck = 0");
        check_stmt("DELETE vstatic FROM t WHERE pk = 0");
    }).get();
}

SEASTAR_THREAD_TEST_CASE(test_generate_timeuuid) {
    auto seed = std::random_device{}();
    testlog.info("test_generate_timeuuid seed: {}", seed);

    std::mt19937 rnd_engine(seed);
    std::uniform_int_distribution<api::timestamp_type> dist(1505959942168984, 1649959942168984);

    for (int i = 0; i < 1000; ++i) {
        auto t1 = dist(rnd_engine);
        auto t2 = dist(rnd_engine);
        auto tuuid1 = cdc::generate_timeuuid(t1);
        auto tuuid2 = cdc::generate_timeuuid(t2);

        auto cmp = timeuuid_type->compare(timeuuid_type->decompose(tuuid1), timeuuid_type->decompose(tuuid2));
        BOOST_REQUIRE((t1 == t2) || (t1 < t2 && cmp < 0) || (t1 > t2 && cmp > 0));
        BOOST_REQUIRE(utils::UUID_gen::micros_timestamp(tuuid1) == t1 && utils::UUID_gen::micros_timestamp(tuuid2) == t2);
    }
}

SEASTAR_THREAD_TEST_CASE(test_with_cdc_parameter) {
    do_with_cql_env_thread([](cql_test_env& e) {
        struct expected {
            bool enabled = false;
            bool preimage = false;
            bool postimage = false;
            int ttl = 86400;
        };

        auto assert_cdc = [&] (const expected& exp) {
            BOOST_REQUIRE_EQUAL(exp.enabled,
                    e.local_db().find_schema("ks", "tbl")->cdc_options().enabled());
            if (exp.enabled) {
                e.require_table_exists("ks", cdc::log_name("tbl")).get();
            } else {
                e.require_table_does_not_exist("ks", cdc::log_name("tbl")).get();
            }
            BOOST_REQUIRE_EQUAL(exp.preimage,
                    e.local_db().find_schema("ks", "tbl")->cdc_options().preimage());
            BOOST_REQUIRE_EQUAL(exp.postimage,
                    e.local_db().find_schema("ks", "tbl")->cdc_options().postimage());
            BOOST_REQUIRE_EQUAL(exp.ttl,
                    e.local_db().find_schema("ks", "tbl")->cdc_options().ttl());
        };

        auto test = [&] (const sstring& create_prop,
                         const sstring& alter1_prop,
                         const sstring& alter2_prop,
                         const expected& create_expected,
                         const expected& alter1_expected,
                         const expected& alter2_expected) {
            e.execute_cql(format("CREATE TABLE ks.tbl (a int PRIMARY KEY) {}", create_prop)).get();
            assert_cdc(create_expected);
            e.execute_cql(format("ALTER TABLE ks.tbl WITH cdc = {}", alter1_prop)).get();
            assert_cdc(alter1_expected);
            e.execute_cql(format("ALTER TABLE ks.tbl WITH cdc = {}", alter2_prop)).get();
            assert_cdc(alter2_expected);
            e.execute_cql("DROP TABLE ks.tbl").get();
            e.require_table_does_not_exist("ks", cdc::log_name("tbl")).get();
        };

        test("", "{'enabled':'true'}", "{'enabled':'false'}", {false}, {true}, {false});
        test("WITH cdc = {'enabled':'true'}", "{'enabled':'false'}", "{'enabled':'true'}", {true}, {false}, {true});
        test("WITH cdc = {'enabled':'false'}", "{'enabled':'true'}", "{'enabled':'false'}", {false}, {true}, {false});
        test("", "{'enabled':'true','preimage':'true','postimage':'true','ttl':'1'}", "{'enabled':'false'}", {false}, {true, true, true, 1}, {false});
        test("WITH cdc = {'enabled':'true','preimage':'true','postimage':'true','ttl':'1'}", "{'enabled':'false'}", "{'enabled':'true','preimage':'false','postimage':'true','ttl':'2'}", {true, true, true, 1}, {false}, {true, false, true, 2});
    }).get();
}

SEASTAR_THREAD_TEST_CASE(test_detecting_conflict_of_cdc_log_table_with_existing_table) {
    do_with_cql_env_thread([] (cql_test_env& e) {
        // Conflict on CREATE which enables cdc log
        e.execute_cql("CREATE TABLE ks.tbl_scylla_cdc_log (a int PRIMARY KEY)").get();
        BOOST_REQUIRE_THROW(e.execute_cql("CREATE TABLE ks.tbl (a int PRIMARY KEY) WITH cdc = {'enabled': true}").get(), exceptions::invalid_request_exception);
        e.require_table_does_not_exist("ks", "tbl").get();

        // Conflict on ALTER which enables cdc log
        e.execute_cql("CREATE TABLE ks.tbl (a int PRIMARY KEY)").get();
        e.require_table_exists("ks", "tbl").get();
        BOOST_REQUIRE_THROW(e.execute_cql("ALTER TABLE ks.tbl WITH cdc = {'enabled': true}").get(), exceptions::invalid_request_exception);
    }).get();
}

SEASTAR_THREAD_TEST_CASE(test_permissions_of_cdc_log_table) {
    do_with_cql_env_thread([] (cql_test_env& e) {
        auto assert_unauthorized = [&e] (const sstring& stmt) {
            testlog.info("Must throw unauthorized_exception: {}", stmt);
            BOOST_REQUIRE_THROW(e.execute_cql(stmt).get(), exceptions::unauthorized_exception);
        };

        e.execute_cql("CREATE TABLE ks.tbl (a int PRIMARY KEY) WITH cdc = {'enabled': true}").get();
        e.require_table_exists("ks", "tbl").get();

        // Allow MODIFY, SELECT, ALTER
        auto log_table = "ks." + cdc::log_name("tbl");
        auto stream_id = cdc::log_meta_column_name("stream_id");
        auto time = cdc::log_meta_column_name("time");
        auto batch_seq_no = cdc::log_meta_column_name("batch_seq_no");
        auto ttl = cdc::log_meta_column_name("ttl");

        e.execute_cql(format("INSERT INTO {} (\"{}\", \"{}\", \"{}\") VALUES (0x00000000000000000000000000000000, now(), 0)",
            log_table, stream_id, time, batch_seq_no
        )).get();
        e.execute_cql(format("UPDATE {} SET \"{}\"= 100 WHERE \"{}\" = 0x00000000000000000000000000000000 AND \"{}\" = now() AND \"{}\" = 0",
            log_table, ttl, stream_id, time, batch_seq_no
        )).get();
        e.execute_cql(format("DELETE FROM {} WHERE \"{}\" = 0x00000000000000000000000000000000 AND \"{}\" = now() AND \"{}\" = 0",
            log_table, stream_id, time, batch_seq_no
        )).get();
        e.execute_cql("SELECT * FROM " + log_table).get();
        e.execute_cql("ALTER TABLE " + log_table + " ALTER \"" + ttl + "\" TYPE blob").get();

        // Disallow DROP
        assert_unauthorized("DROP TABLE " + log_table);
    }).get();
}

SEASTAR_THREAD_TEST_CASE(test_disallow_cdc_on_materialized_view) {
    do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE ks.tbl (a int PRIMARY KEY)").get();
        e.require_table_exists("ks", "tbl").get();

        BOOST_REQUIRE_THROW(e.execute_cql("CREATE MATERIALIZED VIEW ks.mv AS SELECT a FROM ks.tbl PRIMARY KEY (a) WITH cdc = {'enabled': true}").get(), exceptions::invalid_request_exception);
        e.require_table_does_not_exist("ks", "mv").get();
    }).get();
}

SEASTAR_THREAD_TEST_CASE(test_permissions_of_cdc_description) {
    do_with_cql_env_thread([] (cql_test_env& e) {
        auto assert_unauthorized = [&e] (const sstring& stmt) {
            testlog.info("Must throw unauthorized_exception: {}", stmt);
            BOOST_REQUIRE_THROW(e.execute_cql(stmt).get(), exceptions::unauthorized_exception);
        };

        const std::string generations_v2 = "system_distributed_everywhere.cdc_generation_descriptions_v2";
        const std::string streams = "system_distributed.cdc_streams_descriptions_v2";
        const std::string timestamps = "system_distributed.cdc_generation_timestamps";

        for (auto& t : {generations_v2, streams, timestamps}) {
            e.require_table_exists(t).get();

            // Disallow DROP
            assert_unauthorized(format("DROP TABLE {}", t));

            // Allow SELECT
            e.execute_cql(format("SELECT * FROM {}", t)).get();
        }

        // Disallow ALTER
        for (auto& t : {streams}) {
            assert_unauthorized(format("ALTER TABLE {} ALTER time TYPE blob", t));
        }
        assert_unauthorized(format("ALTER TABLE {} ALTER id TYPE blob", generations_v2));
        assert_unauthorized(format("ALTER TABLE {} ALTER key TYPE blob", timestamps));

        // Allow DELETE
        for (auto& t : {streams}) {
            e.execute_cql(format("DELETE FROM {} WHERE time = toTimeStamp(now())", t)).get();
        }
        e.execute_cql(format("DELETE FROM {} WHERE id = uuid()", generations_v2)).get();
        e.execute_cql(format("DELETE FROM {} WHERE key = 'timestamps'", timestamps)).get();

        // Allow UPDATE, INSERT
        e.execute_cql(format("INSERT INTO {} (id, range_end) VALUES (uuid(), 0)", generations_v2)).get();
        e.execute_cql(format("INSERT INTO {} (time, range_end) VALUES (toTimeStamp(now()), 0)", streams)).get();
        e.execute_cql(format("UPDATE {} SET expired = toTimeStamp(now()) WHERE key = 'timestamps' AND time = toTimeStamp(now())", timestamps)).get();
    }).get();
}

SEASTAR_THREAD_TEST_CASE(test_cdc_log_schema) {
    do_with_cql_env_thread([] (cql_test_env& e) {
        int required_column_count = 0;

        const auto base_tbl_name = "tbl";
        e.execute_cql("CREATE TYPE typ (x int)").get();
        e.execute_cql(format("CREATE TABLE {} (pk int, ck int, s int static, c int, "
                "c_list list<int>, c_map map<int, int>, c_set set<int>, c_typ typ,"
                "PRIMARY KEY (pk, ck)) WITH cdc = {{'enabled': 'true'}}", base_tbl_name)).get();
        const auto log_schema = e.local_db().find_schema("ks", cdc::log_name(base_tbl_name));

        auto assert_has_column = [&] (sstring column_name, data_type type, column_kind kind = column_kind::regular_column) {
            BOOST_TEST_MESSAGE(format("Checking that column {} exists", column_name));
            const auto cdef = log_schema->get_column_definition(to_bytes(column_name));
            BOOST_REQUIRE_NE(cdef, nullptr);
            BOOST_TEST_MESSAGE(format("Want kind {}, has {}", (int)kind, (int)cdef->kind));
            BOOST_REQUIRE(cdef->kind == kind);
            BOOST_TEST_MESSAGE(format("Want type {}, has {}", type->name(), cdef->type->name()));
            BOOST_REQUIRE(*cdef->type == *type);
            required_column_count++;
        };

        auto assert_does_not_have_column = [&] (sstring column_name) {
            BOOST_TEST_MESSAGE(format("Checking that column {} does not exist", column_name));
            const auto cdef = log_schema->get_column_definition(to_bytes(column_name));
            BOOST_REQUIRE_EQUAL(cdef, nullptr);
        };

        BOOST_TEST_MESSAGE(format("Schema of the cdc log table is: {}", log_schema));

        // cdc log partition key
        assert_has_column(cdc::log_meta_column_name("stream_id"), bytes_type, column_kind::partition_key);
        assert_has_column(cdc::log_meta_column_name("time"), timeuuid_type, column_kind::clustering_key);
        assert_has_column(cdc::log_meta_column_name("batch_seq_no"), int32_type, column_kind::clustering_key);

        // cdc log clustering key
        assert_has_column(cdc::log_meta_column_name("operation"), byte_type);
        assert_has_column(cdc::log_meta_column_name("ttl"), long_type);
        assert_has_column(cdc::log_meta_column_name("end_of_batch"), boolean_type);

        // pk
        assert_has_column(cdc::log_data_column_name("pk"), int32_type);
        assert_does_not_have_column(cdc::log_data_column_deleted_name("pk"));
        assert_does_not_have_column(cdc::log_data_column_deleted_elements_name("pk"));

        // ck
        assert_has_column(cdc::log_data_column_name("ck"), int32_type);
        assert_does_not_have_column(cdc::log_data_column_deleted_name("ck"));
        assert_does_not_have_column(cdc::log_data_column_deleted_elements_name("ck"));

        // static row
        assert_has_column(cdc::log_data_column_name("s"), int32_type);
        assert_has_column(cdc::log_data_column_deleted_name("s"), boolean_type);
        assert_does_not_have_column(cdc::log_data_column_deleted_elements_name("s"));

        // clustering row, atomic
        assert_has_column(cdc::log_data_column_name("c"), int32_type);
        assert_has_column(cdc::log_data_column_deleted_name("c"), boolean_type);
        assert_does_not_have_column(cdc::log_data_column_deleted_elements_name("c"));

        // clustering row, list
        assert_has_column(cdc::log_data_column_name("c_list"), map_type_impl::get_instance(timeuuid_type, int32_type, false));
        assert_has_column(cdc::log_data_column_deleted_name("c_list"), boolean_type);
        assert_has_column(cdc::log_data_column_deleted_elements_name("c_list"), set_type_impl::get_instance(timeuuid_type, false));

        // clustering row, map
        assert_has_column(cdc::log_data_column_name("c_map"), map_type_impl::get_instance(int32_type, int32_type, false));
        assert_has_column(cdc::log_data_column_deleted_name("c_map"), boolean_type);
        assert_has_column(cdc::log_data_column_deleted_elements_name("c_map"), set_type_impl::get_instance(int32_type, false));

        // clustering row, set
        assert_has_column(cdc::log_data_column_name("c_set"), set_type_impl::get_instance(int32_type, false));
        assert_has_column(cdc::log_data_column_deleted_name("c_set"), boolean_type);
        assert_has_column(cdc::log_data_column_deleted_elements_name("c_set"), set_type_impl::get_instance(int32_type, false));

        // clustering row, udt
        const auto c_typ_frozen = user_type_impl::get_instance("ks", "typ", {to_bytes("x")}, {int32_type}, false);
        assert_has_column(cdc::log_data_column_name("c_typ"), c_typ_frozen);
        assert_has_column(cdc::log_data_column_deleted_name("c_typ"), boolean_type);
        assert_has_column(cdc::log_data_column_deleted_elements_name("c_typ"), set_type_impl::get_instance(short_type, false));

        // Check if we missed something
        BOOST_REQUIRE_EQUAL(required_column_count, log_schema->all_columns_count());
    }).get();
}

static std::vector<std::vector<bytes_opt>> to_bytes(const cql_transport::messages::result_message::rows& rows) {
    auto rs = rows.rs().result_set().rows();
    std::vector<std::vector<bytes_opt>> results;
    for (auto it = rs.begin(); it != rs.end(); ++it) {
        results.push_back(*it);
    }
    return results;
}

static size_t column_index(const cql_transport::messages::result_message::rows& rows, const sstring& name) {
    size_t res = 0;
    for (auto& col : rows.rs().get_metadata().get_names()) {
        if (col->name->text() == name) {
            return res;
        }
        ++res;
    }
    throw std::invalid_argument("No such column: " + name);
}

template<typename Comp = std::equal_to<bytes_opt>>
static std::vector<std::vector<bytes_opt>> filter_by_operation(const cql_transport::messages::result_message::rows& rows, std::vector<std::vector<bytes_opt>> results, cdc::operation op, const Comp& comp = {}) {
    const auto op_type = data_type_for<std::underlying_type_t<cdc::operation>>();
    auto op_index = column_index(rows, cdc::log_meta_column_name("operation"));
    auto op_bytes = op_type->decompose(std::underlying_type_t<cdc::operation>(op));

    std::erase_if(results, [&](const std::vector<bytes_opt>& bo) {
        return !comp(op_bytes, bo[op_index]);
    });

    return results;
}

template<typename Comp = std::equal_to<bytes_opt>>
static std::vector<std::vector<bytes_opt>> to_bytes_filtered(const cql_transport::messages::result_message::rows& rows, cdc::operation op, const Comp& comp = {}) {
    return filter_by_operation<Comp>(rows, to_bytes(rows), op, comp);
}

static void sort_by_time(const cql_transport::messages::result_message::rows& rows, std::vector<std::vector<bytes_opt>>& results) {
    auto time_index = column_index(rows, cdc::log_meta_column_name("time"));
    std::sort(results.begin(), results.end(),
            [time_index] (const std::vector<bytes_opt>& a, const std::vector<bytes_opt>& b) {
                return timeuuid_type->as_less_comparator()(*a[time_index], *b[time_index]);
            });
}

static auto select_log(cql_test_env& e, const sstring& table_name) {
    auto msg = e.execute_cql(format("SELECT * FROM ks.{}", cdc::log_name(table_name))).get0();
    auto rows = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(msg);
    BOOST_REQUIRE(rows);
    return rows;
};

SEASTAR_THREAD_TEST_CASE(test_primary_key_logging) {
    do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "CREATE TABLE ks.tbl (pk int, pk2 int, ck int, ck2 int, s int STATIC, val int, PRIMARY KEY((pk, pk2), ck, ck2)) WITH cdc = {'enabled':'true'}");
        cquery_nofail(e, "INSERT INTO ks.tbl(pk, pk2, ck, ck2, val) VALUES(1, 11, 111, 1111, 11111)");
        cquery_nofail(e, "INSERT INTO ks.tbl(pk, pk2, ck, ck2, val) VALUES(1, 22, 222, 2222, 22222)");
        cquery_nofail(e, "INSERT INTO ks.tbl(pk, pk2, ck, ck2, val) VALUES(1, 33, 333, 3333, 33333)");
        cquery_nofail(e, "INSERT INTO ks.tbl(pk, pk2, ck, ck2, val) VALUES(1, 44, 444, 4444, 44444)");
        cquery_nofail(e, "INSERT INTO ks.tbl(pk, pk2, ck, ck2, val) VALUES(2, 11, 111, 1111, 11111)");
        cquery_nofail(e, "INSERT INTO ks.tbl(pk, pk2, ck, ck2, val) VALUES(3, 11, 111, 1111, 11111) USING TTL 600");
        cquery_nofail(e, "INSERT INTO ks.tbl(pk, pk2, s) VALUES (4, 11, 111)");
        cquery_nofail(e, "DELETE val FROM ks.tbl WHERE pk = 1 AND pk2 = 11 AND ck = 111 AND ck2 = 1111");
        cquery_nofail(e, "DELETE FROM ks.tbl WHERE pk = 1 AND pk2 = 11 AND ck = 111 AND ck2 = 1111");
        cquery_nofail(e, "DELETE FROM ks.tbl WHERE pk = 1 AND pk2 = 11 AND ck > 222 AND ck <= 444");
        cquery_nofail(e, "UPDATE ks.tbl SET val = 555 WHERE pk = 2 AND pk2 = 11 AND ck = 111 AND ck2 = 1111");
        cquery_nofail(e, "UPDATE ks.tbl USING TTL 3600 SET val = 444 WHERE pk = 3 AND pk2 = 11 AND ck = 111 AND ck2 = 1111");
        cquery_nofail(e, "DELETE FROM ks.tbl WHERE pk = 1 AND pk2 = 11");
        auto msg = e.execute_cql(format("SELECT \"{}\", \"{}\", \"{}\", \"{}\", \"{}\", \"{}\", \"{}\" FROM ks.{}",
            cdc::log_meta_column_name("time"),
            cdc::log_data_column_name("pk"),
            cdc::log_data_column_name("pk2"),
            cdc::log_data_column_name("ck"),
            cdc::log_data_column_name("ck2"),
            cdc::log_meta_column_name("operation"),
            cdc::log_meta_column_name("ttl"),
            cdc::log_name("tbl"))).get0();
        auto rows = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(msg);
        BOOST_REQUIRE(rows);

        auto results = to_bytes_filtered(*rows, cdc::operation::pre_image, std::not_equal_to<bytes_opt>{});

        sort_by_time(*rows, results);

        auto actual_i = results.begin();
        auto actual_end = results.end();
        auto assert_row = [&] (int pk, int pk2, int ck = -1, int ck2 = -1, std::optional<int64_t> ttl = {}) {
            std::cerr << "check " << pk << " " << pk2 << " " << ck << " " << ck2 << " " << ttl << std::endl;
            BOOST_REQUIRE(actual_i != actual_end);
            auto& actual_row = *actual_i;
            BOOST_REQUIRE_EQUAL(int32_type->decompose(pk), actual_row[1]);
            BOOST_REQUIRE_EQUAL(int32_type->decompose(pk2), actual_row[2]);
            if (ck != -1) {
                BOOST_REQUIRE_EQUAL(int32_type->decompose(ck), actual_row[3]);
            } else {
                BOOST_REQUIRE(!actual_row[3]);
            }
            if (ck2 != -1) {
                BOOST_REQUIRE_EQUAL(int32_type->decompose(ck2), actual_row[4]);
            } else {
                BOOST_REQUIRE(!actual_row[4]);
            }
            if (ttl) {
                BOOST_REQUIRE_EQUAL(long_type->decompose(*ttl), actual_row[6]);
            } else {
                BOOST_REQUIRE(!actual_row[6]);
            }
            ++actual_i;
        };
        // INSERT INTO ks.tbl(pk, pk2, ck, ck2, val) VALUES(1, 11, 111, 1111, 11111)
        assert_row(1, 11, 111, 1111);
        // INSERT INTO ks.tbl(pk, pk2, ck, ck2, val) VALUES(1, 22, 222, 2222, 22222)
        assert_row(1, 22, 222, 2222);
        // INSERT INTO ks.tbl(pk, pk2, ck, ck2, val) VALUES(1, 33, 333, 3333, 33333)
        assert_row(1, 33, 333, 3333);
        // INSERT INTO ks.tbl(pk, pk2, ck, ck2, val) VALUES(1, 44, 444, 4444, 44444)
        assert_row(1, 44, 444, 4444);
        // INSERT INTO ks.tbl(pk, pk2, ck, ck2, val) VALUES(2, 11, 111, 1111, 11111)
        assert_row(2, 11, 111, 1111);
        // INSERT INTO ks.tbl(pk, pk2, ck, ck2, val) VALUES(3, 11, 111, 1111, 11111) WITH TTL 600
        assert_row(3, 11, 111, 1111, 600);
        // INSERT INTO ks.tbl(pk, pk2, s) VALUES (4, 11, 111)
        assert_row(4, 11, -1, -1);
        // DELETE val FROM ks.tbl WHERE pk = 1 AND pk2 = 11 AND ck = 111 AND ck2 = 1111
        assert_row(1, 11, 111, 1111);
        // DELETE FROM ks.tbl WHERE pk = 1 AND pk2 = 11 AND ck = 111 AND ck2 = 1111
        assert_row(1, 11, 111, 1111);
        // First row for DELETE FROM ks.tbl WHERE pk = 1 AND pk2 = 11 AND ck > 222 AND ck <= 444
        assert_row(1, 11, 222);
        // Second row for DELETE FROM ks.tbl WHERE pk = 1 AND pk2 = 11 AND ck > 222 AND ck <= 444
        assert_row(1, 11, 444);
        // UPDATE ks.tbl SET val = 555 WHERE pk = 2 AND pk2 = 11 AND ck = 111 AND ck2 = 1111
        assert_row(2, 11, 111, 1111);
        // UPDATE ks.tbl USING TTL 3600 SET val = 444 WHERE pk = 3 AND pk2 = 11 AND ck = 111 AND ck2 = 1111
        assert_row(3, 11, 111, 1111, 3600);
        // DELETE FROM ks.tbl WHERE pk = 1 AND pk2 = 11
        assert_row(1, 11);
        BOOST_REQUIRE(actual_i == actual_end);
    }).get();
}

SEASTAR_THREAD_TEST_CASE(test_pre_post_image_logging) {
    do_with_cql_env_thread([](cql_test_env& e) {
        auto test = [&e] (cdc::image_mode pre_enabled, bool post_enabled, bool with_ttl) {
            // note: 'val3' column is not used, but since not set in initial update, would provoke #6143 unless fixed.
            cquery_nofail(e, format("CREATE TABLE ks.tbl (pk int, pk2 int, ck int, ck2 int, val int, val2 int, val3 int, PRIMARY KEY((pk, pk2), ck, ck2)) "
                "WITH cdc = {{'enabled':'true', 'preimage':'{}', 'postimage':'{}'}}", pre_enabled, post_enabled));
            cquery_nofail(e, "UPDATE ks.tbl"s + (with_ttl ? " USING TTL 654" : "") + " SET val = 11111, val2 = 22222 WHERE pk=1 AND pk2=11 AND ck=111 AND ck2=1111");

            auto rows = select_log(e, "tbl");

            BOOST_REQUIRE(to_bytes_filtered(*rows, cdc::operation::pre_image).empty());
            BOOST_REQUIRE_EQUAL(!post_enabled, to_bytes_filtered(*rows, cdc::operation::post_image).empty());

            auto first = to_bytes_filtered(*rows, cdc::operation::update);

            auto ck2_index = column_index(*rows, cdc::log_data_column_name("ck2"));
            auto val_index = column_index(*rows, cdc::log_data_column_name("val"));
            auto val2_index = column_index(*rows, cdc::log_data_column_name("val2"));
            auto ttl_index = column_index(*rows, cdc::log_meta_column_name("ttl"));
            auto eor_index = column_index(*rows, cdc::log_meta_column_name("end_of_batch"));

            auto val_type = int32_type;
            auto val = *first[0][val_index];
            auto val2 = *first[0][val2_index];

            BOOST_REQUIRE_EQUAL(int32_type->decompose(1111), first[0][ck2_index]);
            BOOST_REQUIRE_EQUAL(data_value(11111), val_type->deserialize(bytes_view(val)));
            BOOST_REQUIRE_EQUAL(data_value(22222), val_type->deserialize(bytes_view(val2)));

            auto last = 11111;
            int64_t last_ttl = 654;
            for (auto i = 0u; i < 10; ++i) {
                auto nv = last + 1;
                const int64_t new_ttl = 100 * (i + 1);
                cquery_nofail(e, "UPDATE ks.tbl" + (with_ttl ? format(" USING TTL {}", new_ttl) : "") + " SET val=" + std::to_string(nv) +" where pk=1 AND pk2=11 AND ck=111 AND ck2=1111");

                rows = select_log(e, "tbl");

                auto pre_image = to_bytes_filtered(*rows, cdc::operation::pre_image);
                auto second = to_bytes_filtered(*rows, cdc::operation::update);
                auto post_image = to_bytes_filtered(*rows, cdc::operation::post_image);

                BOOST_REQUIRE_EQUAL(pre_enabled == cdc::image_mode::off, pre_image.empty());
                BOOST_REQUIRE_EQUAL(!post_enabled, post_image.empty());

                sort_by_time(*rows, second);
                sort_by_time(*rows, pre_image);
                sort_by_time(*rows, post_image);

                if (pre_enabled != cdc::image_mode::off) {
                    BOOST_REQUIRE_EQUAL(pre_image.size(), i + 1);

                    val = *pre_image.back()[val_index];
                    // note: no val2 in pre-image, because we are not modifying it.
                    BOOST_REQUIRE_EQUAL(int32_type->decompose(1111), *pre_image.back()[ck2_index]);
                    BOOST_REQUIRE_EQUAL(data_value(last), val_type->deserialize(bytes_view(val)));
                    BOOST_REQUIRE_EQUAL(bytes_opt(), pre_image.back()[ttl_index]);
                }

                if (pre_enabled == cdc::image_mode::full) {
                    BOOST_REQUIRE_EQUAL(int32_type->decompose(22222), *pre_image.back()[val2_index]);
                }
                if (pre_enabled == cdc::image_mode::on) {
                    BOOST_REQUIRE(!pre_image.back()[val2_index]);
                }

                if (post_enabled) {
                    val = *post_image.back()[val_index];
                    val2 = *post_image.back()[val2_index];
                    auto eor = *post_image.back()[eor_index];

                    BOOST_REQUIRE_EQUAL(int32_type->decompose(1111), *post_image.back()[ck2_index]);
                    BOOST_REQUIRE_EQUAL(data_value(nv), val_type->deserialize(bytes_view(val)));
                    BOOST_REQUIRE_EQUAL(data_value(22222), val_type->deserialize(bytes_view(val2)));
                    BOOST_REQUIRE_EQUAL(data_value(true), boolean_type->deserialize(bytes_view(eor)));
                }

                const auto& ttl_cell = second[second.size() - 2][ttl_index];
                if (with_ttl) {
                    BOOST_REQUIRE_EQUAL(long_type->decompose(last_ttl), ttl_cell);
                } else {
                    BOOST_REQUIRE(!ttl_cell);
                }

                last = nv;
                last_ttl = new_ttl;
            }
            e.execute_cql("DROP TABLE ks.tbl").get();
        };
        for (auto pre : { cdc::image_mode::on, cdc::image_mode::full, cdc::image_mode::off }) {
            for (auto post : { true, false }) {
                for (auto ttl : { true, false}) {
                    test(pre, post, ttl);
                }
            }
        }
    }).get();
}

SEASTAR_THREAD_TEST_CASE(test_pre_post_image_logging_static_row) {
    do_with_cql_env_thread([](cql_test_env& e) {
        auto test = [&e] (bool enabled, bool with_ttl) {
            cquery_nofail(e, format("CREATE TABLE ks.tbl (pk int, pk2 int, ck int, ck2 int, s int STATIC, s2 int STATIC, val int, PRIMARY KEY((pk, pk2), ck, ck2)) "
                "WITH cdc = {{'enabled':'true', 'preimage':'{0}', 'postimage':'{0}'}}", enabled));
            cquery_nofail(e, "INSERT INTO ks.tbl(pk, pk2, s, s2) VALUES(1, 11, 111, 1111)"s + (with_ttl ? " USING TTL 654" : ""));

            auto rows = select_log(e, "tbl");

            BOOST_REQUIRE(to_bytes_filtered(*rows, cdc::operation::pre_image).empty());
            BOOST_REQUIRE(to_bytes_filtered(*rows, cdc::operation::post_image).empty() == !enabled);

            auto first = to_bytes_filtered(*rows, cdc::operation::update);

            auto s_index = column_index(*rows, cdc::log_data_column_name("s"));
            auto s2_index = column_index(*rows, cdc::log_data_column_name("s2"));
            auto ttl_index = column_index(*rows, cdc::log_meta_column_name("ttl"));

            auto s_type = int32_type;
            auto s = *first[0][s_index];
            auto s2 = *first[0][s2_index];

            BOOST_REQUIRE_EQUAL(data_value(111), s_type->deserialize(bytes_view(s)));
            BOOST_REQUIRE_EQUAL(data_value(1111), s_type->deserialize(bytes_view(s2)));

            auto last = 111;
            int64_t last_ttl = 654;
            for (auto i = 0u; i < 10; ++i) {
                auto nv = last + 1;
                const int64_t new_ttl = 100 * (i + 1);
                cquery_nofail(e, "UPDATE ks.tbl" + (with_ttl ? format(" USING TTL {}", new_ttl) : "") + " SET s=" + std::to_string(nv) +" where pk=1 AND pk2=11");

                rows = select_log(e, "tbl");

                auto pre_image = to_bytes_filtered(*rows, cdc::operation::pre_image);
                auto second = to_bytes_filtered(*rows, cdc::operation::update);
                auto post_image = to_bytes_filtered(*rows, cdc::operation::post_image);

                if (!enabled) {
                    BOOST_REQUIRE(pre_image.empty());
                    BOOST_REQUIRE(post_image.empty());
                } else {
                    sort_by_time(*rows, second);
                    sort_by_time(*rows, pre_image);
                    sort_by_time(*rows, post_image);
                    BOOST_REQUIRE_EQUAL(pre_image.size(), i + 1);

                    s = *pre_image.back()[s_index];
                    BOOST_REQUIRE_EQUAL(data_value(last), s_type->deserialize(bytes_view(s)));
                    BOOST_REQUIRE_EQUAL(bytes_opt(), pre_image.back()[ttl_index]);

                    s2 = *post_image.back()[s2_index];
                    BOOST_REQUIRE_EQUAL(data_value(1111), s_type->deserialize(bytes_view(s2)));

                    const auto& ttl_cell = second[second.size() - 2][ttl_index];
                    if (with_ttl) {
                        BOOST_REQUIRE_EQUAL(long_type->decompose(last_ttl), ttl_cell);
                    } else {
                        BOOST_REQUIRE(!ttl_cell);
                    }
                }

                last = nv;
                last_ttl = new_ttl;
            }
            cquery_nofail(e, "DROP TABLE ks.tbl");
        };
        test(true, true);
        test(true, false);
        test(false, true);
        test(false, false);
    }).get();
}

SEASTAR_THREAD_TEST_CASE(test_range_deletion) {
    do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "CREATE TABLE ks.tbl (pk int, ck int, val int, PRIMARY KEY(pk, ck)) WITH cdc = {'enabled':'true'}");
        cquery_nofail(e, "DELETE FROM ks.tbl WHERE pk = 123 AND ck > 1 AND ck < 23");
        cquery_nofail(e, "DELETE FROM ks.tbl WHERE pk = 123 AND ck >= 4 AND ck <= 56");

        auto msg = e.execute_cql(format("SELECT \"{}\", \"{}\", \"{}\", \"{}\" FROM ks.{}",
            cdc::log_meta_column_name("time"),
            cdc::log_data_column_name("pk"),
            cdc::log_data_column_name("ck"),
            cdc::log_meta_column_name("operation"),
            cdc::log_name("tbl"))).get0();
        auto rows = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(msg);
        BOOST_REQUIRE(rows);

        auto results = to_bytes(*rows);
        sort_by_time(*rows, results);

        auto ck_index = column_index(*rows, cdc::log_data_column_name("ck"));
        auto ck_type = int32_type;
        auto op_index = column_index(*rows, cdc::log_meta_column_name("operation"));
        auto op_type = data_type_for<std::underlying_type_t<cdc::operation>>();

        size_t row_idx = 0;

        auto check_row = [&](int32_t ck, cdc::operation operation) {
            testlog.trace("{}", results[row_idx][ck_index]);
            testlog.trace("{}", bytes_opt(ck_type->decompose(ck)));
            BOOST_REQUIRE_EQUAL(results[row_idx][ck_index], bytes_opt(ck_type->decompose(ck)));
            BOOST_REQUIRE_EQUAL(results[row_idx][op_index], bytes_opt(op_type->decompose(std::underlying_type_t<cdc::operation>(operation))));
            ++row_idx;
        };

        BOOST_REQUIRE_EQUAL(results.size(), 4);

        // ck > 1 AND ck < 23
        check_row(1, cdc::operation::range_delete_start_exclusive);
        check_row(23, cdc::operation::range_delete_end_exclusive);
        // ck >= 4 AND ck <= 56
        check_row(4, cdc::operation::range_delete_start_inclusive);
        check_row(56, cdc::operation::range_delete_end_inclusive);
    }).get();
}

SEASTAR_THREAD_TEST_CASE(test_add_columns) {
    do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "CREATE TABLE ks.tbl (pk int, pk2 int, ck int, ck2 int, val int, PRIMARY KEY((pk, pk2), ck, ck2)) WITH cdc = {'enabled':'true'}");
        cquery_nofail(e, "INSERT INTO ks.tbl(pk, pk2, ck, ck2, val) VALUES(1, 11, 111, 1111, 11111)");

        auto rows = select_log(e, "tbl");

        BOOST_REQUIRE(!to_bytes_filtered(*rows, cdc::operation::insert).empty());

        cquery_nofail(e, "ALTER TABLE ks.tbl ADD kokos text");
        cquery_nofail(e, "INSERT INTO ks.tbl(pk, pk2, ck, ck2, val, kokos) VALUES(1, 11, 111, 1111, 11111, 'kaka')");

        rows = select_log(e, "tbl");
        auto inserts = to_bytes_filtered(*rows, cdc::operation::insert);
        sort_by_time(*rows, inserts);

        auto kokos_index = column_index(*rows, cdc::log_data_column_name("kokos"));
        auto kokos_type = utf8_type;
        auto kokos = *inserts.back()[kokos_index];

        BOOST_REQUIRE_EQUAL(data_value("kaka"), kokos_type->deserialize(bytes_view(kokos)));
    }).get();
}

// #5582 - just quickly test that we can create the cdc enabled table on a different shard
// and still get the logs proper.
SEASTAR_THREAD_TEST_CASE(test_cdc_across_shards) {
    do_with_cql_env_thread([](cql_test_env& e) {
        if (smp::count < 2) {
            testlog.warn("This test case requires at least 2 shards");
            return;
        }
        smp::submit_to(1, [&e] {
            // this is actually ok. there is no members of cql_test_env actually used in call
            return seastar::async([&] {
                cquery_nofail(e, "CREATE TABLE ks.tbl (pk int, pk2 int, ck int, ck2 int, val int, PRIMARY KEY((pk, pk2), ck, ck2)) WITH cdc = {'enabled':'true'}");
            });
        }).get();
        cquery_nofail(e, "INSERT INTO ks.tbl(pk, pk2, ck, ck2, val) VALUES(1, 11, 111, 1111, 11111)");

        auto rows = select_log(e, "tbl");

        BOOST_REQUIRE(!to_bytes_filtered(*rows, cdc::operation::insert).empty());
    }).get();
}

SEASTAR_THREAD_TEST_CASE(test_negative_ttl_fail) {
    do_with_cql_env_thread([](cql_test_env& e) {
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("CREATE TABLE ks.fail (a int PRIMARY KEY, b int) WITH cdc = {'enabled':true,'ttl':'-1'}").get0(),
                exceptions::configuration_exception,
                exception_predicate::message_contains("ttl"));
    }).get();
}

SEASTAR_THREAD_TEST_CASE(test_ttls) {
    do_with_cql_env_thread([](cql_test_env& e) {
        auto test_ttl = [&e] (int ttl_seconds) {
            const auto base_tbl_name = "tbl" + std::to_string(ttl_seconds);
            cquery_nofail(e, format("CREATE TABLE ks.{} (pk int, ck int, val int, PRIMARY KEY(pk, ck)) WITH cdc = {{'enabled':'true', 'ttl':{}}}", base_tbl_name, ttl_seconds));
            BOOST_REQUIRE_EQUAL(e.local_db().find_schema("ks", base_tbl_name)->cdc_options().ttl(), ttl_seconds);
            cquery_nofail(e, format("INSERT INTO ks.{} (pk, ck, val) VALUES(1, 11, 111)", base_tbl_name));

            auto log_schema = e.local_db().find_schema("ks", cdc::log_name(base_tbl_name));

            // Construct a query like "SELECT _pk, ttl(_pk), _ck, ttl(_ck), ...  FROM ks.tbl_log_tablename"
            sstring query = "SELECT";
            bool first_token = true;
            std::vector<bytes> log_column_names; // {"_pk", "_ck", "_val", ...}
            for (auto& reg_col : log_schema->regular_columns()) {
                if (!first_token) {
                    query += ",";
                }
                first_token = false;
                query += format(" \"{0}\", ttl(\"{0}\")", reg_col.name_as_text());
                log_column_names.push_back(reg_col.name_as_text().c_str());
            }
            query += format(" FROM ks.{}", cdc::log_name(base_tbl_name));

            // Execute query and get the first (and only) row of results:
            auto msg = e.execute_cql(query).get0();
            auto rows = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(msg);
            BOOST_REQUIRE(rows);
            auto results = to_bytes(*rows);
            BOOST_REQUIRE(!results.empty());
            auto& row = results.front(); // serialized {_pk, ttl(_pk), _ck, ttl(_ck), ...}
            BOOST_REQUIRE(!(row.size()%2));

            // Traverse the result - serialized pairs of (_column, ttl(_column))
            for (size_t i = 0u; i < row.size(); i += 2u) {
                auto cell_type = log_schema->column_name_type(*log_schema->get_column_definition(log_column_names[i/2]));
                if (!row[i].has_value() || cell_type->deserialize(*row[i]).is_null()) {
                    continue; // NULL cell cannot have TTL
                }
                if (!row[i+1].has_value() && !ttl_seconds) {
                    continue; // NULL TTL value is acceptable when records are kept forever (TTL==0)
                }
                data_value cell_ttl = int32_type->deserialize(*row[i+1]);
                BOOST_REQUIRE(!cell_ttl.is_null());
                auto cell_ttl_seconds = value_cast<int32_t>(cell_ttl);
                // 30% tolerance in case of slow execution (a little flaky...)
                BOOST_REQUIRE_CLOSE((float)cell_ttl_seconds, (float)ttl_seconds, 30.f);
            }
        };
        test_ttl(0);
        test_ttl(10);
    }).get();
}

// helper funcs + structs for collection testing
using translate_func = std::function<data_value(data_value)>;
struct col_test_change {
    data_value next;
    data_value deleted;
    bool is_column_delete = false;
};
struct col_test {
    sstring update;
    data_value prev;
    std::vector<col_test_change> changes;
    data_value post = data_value::make_null(int32_type); // whatever
};

// iterate a set of updates and verify pre and delta values.
static void test_collection(cql_test_env& e, data_type val_type, data_type del_type, std::vector<col_test> tests, translate_func f = [](data_value v) { return v; }) {
    auto col_type = val_type;

    for (auto& t : tests) {
        cquery_nofail(e, t.update);

        auto rows = select_log(e, "tbl");
        auto pre_image = to_bytes_filtered(*rows, cdc::operation::pre_image);
        auto updates = to_bytes_filtered(*rows, cdc::operation::update);
        auto post_image = to_bytes_filtered(*rows, cdc::operation::post_image);

        sort_by_time(*rows, updates);
        sort_by_time(*rows, pre_image);
        sort_by_time(*rows, post_image);

        auto val_index = column_index(*rows, cdc::log_data_column_name("val"));

        if (!t.prev.is_null()) {
            BOOST_REQUIRE_GE(pre_image.size(), t.changes.size());
        }

        BOOST_REQUIRE_GE(updates.size(), t.changes.size());

        auto update = updates.end()  - t.changes.size();
        auto pre = pre_image.end()  - t.changes.size();

        if (!t.prev.is_null()) {
            auto val = *(*pre)[val_index];
            BOOST_REQUIRE_EQUAL(t.prev, f(col_type->deserialize(bytes_view(val))));
        }

        for (auto& change : t.changes) {
            auto& update_row = *update++;
            if (!change.next.is_null()) {
                auto val = col_type->deserialize(bytes_view(*update_row[val_index]));
                BOOST_REQUIRE_EQUAL(change.next, f(val));
            }
            if (!change.deleted.is_null()) {
                auto val_elems_deleted_index = column_index(*rows, cdc::log_data_column_deleted_elements_name("val"));
                auto val_elem_deleted = del_type->deserialize(bytes_view(*update_row[val_elems_deleted_index]));
                BOOST_REQUIRE_EQUAL(change.deleted, val_elem_deleted);
            }
            if (change.is_column_delete) {
                auto val_deleted_index = column_index(*rows, cdc::log_data_column_deleted_name("val"));
                BOOST_REQUIRE_EQUAL(data_value(true), boolean_type->deserialize(bytes_view(*update_row[val_deleted_index])));
            }
        }

        if (!t.post.is_null()) {
            auto val = *post_image.back()[val_index];
            BOOST_REQUIRE_EQUAL(t.post, f(col_type->deserialize(bytes_view(val))));
        }
    }
}

SEASTAR_THREAD_TEST_CASE(test_map_logging) {
    do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "CREATE TABLE ks.tbl (pk int, pk2 int, ck int, val map<text, text>, PRIMARY KEY((pk, pk2), ck)) WITH cdc = {'enabled':'true', 'preimage':'true', 'postimage':'true' }"s);
        auto cleanup = defer([&] {
            e.execute_cql("DROP TABLE ks.tbl").get();
        });

        auto map_type = map_type_impl::get_instance(utf8_type, utf8_type, false);
        auto map_keys_type = set_type_impl::get_instance(utf8_type, false);

        test_collection(e, map_type, map_keys_type, {
            {
                "UPDATE ks.tbl set val = { 'apa':'ko' } where pk=1 and pk2=11 and ck=111",
                data_value::make_null(map_type), // no previous value
                {
                    {
                        ::make_map_value(map_type, { { "apa", "ko" } }), // one added cell
                        data_value::make_null(map_keys_type), // no deleted cells
                        true // setting entire column to null -> expect delete marker
                    }
                },
                ::make_map_value(map_type, { { "apa", "ko" } })
            },
            {
                "UPDATE ks.tbl set val = val + { 'ninja':'mission' } where pk=1 and pk2=11 and ck=111",
                ::make_map_value(map_type, { { "apa", "ko" } }),
                {
                    {
                        ::make_map_value(map_type, { { "ninja", "mission" } }),
                        data_value::make_null(map_keys_type),
                    }
                },
                ::make_map_value(map_type, { { "apa", "ko" }, { "ninja", "mission" } })
            },
            {
                "UPDATE ks.tbl set val['ninja'] = 'shuriken' where pk=1 and pk2=11 and ck=111",
                ::make_map_value(map_type, { { "apa", "ko" }, { "ninja", "mission" } }),
                {
                    {
                        ::make_map_value(map_type, { { "ninja", "shuriken" } }),
                        data_value::make_null(map_keys_type),
                    }
                },
                ::make_map_value(map_type, { { "apa", "ko" }, { "ninja", "shuriken" } })
            },
            {
                "UPDATE ks.tbl set val['apa'] = null where pk=1 and pk2=11 and ck=111",
                ::make_map_value(map_type, { { "apa", "ko" }, { "ninja", "shuriken" } }),
                {
                    {
                        data_value::make_null(map_type),
                        ::make_set_value(map_keys_type, { "apa" }),
                    }
                },
                ::make_map_value(map_type, { { "ninja", "shuriken" } })
            },
            {
                "UPDATE ks.tbl set val['ninja'] = null, val['ola'] = 'kokos' where pk=1 and pk2=11 and ck=111",
                ::make_map_value(map_type, { { "ninja", "shuriken" } }),
                {
                    {
                        ::make_map_value(map_type, { { "ola", "kokos" } }),
                        ::make_set_value(map_keys_type, { "ninja" }),
                    }
                },
                ::make_map_value(map_type, { { "ola", "kokos" } })
            },
            {
                "UPDATE ks.tbl set val = { 'bolla':'trolla', 'kork':'skruv' } where pk=1 and pk2=11 and ck=111",
                ::make_map_value(map_type, { { "ola", "kokos" } }),
                {
                    {
                        ::make_map_value(map_type, { { "bolla", "trolla" }, { "kork", "skruv" } }),
                        data_value::make_null(map_keys_type),
                        true // setting entire column to null -> expect delete marker
                    }
                },
                ::make_map_value(map_type, { { "bolla", "trolla" }, { "kork", "skruv" } })
            }

        });
    }).get();
}

SEASTAR_THREAD_TEST_CASE(test_set_logging) {
    do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "CREATE TABLE ks.tbl (pk int, pk2 int, ck int, val set<text>, PRIMARY KEY((pk, pk2), ck)) WITH cdc = {'enabled':'true', 'preimage':'true', 'postimage':'true' }"s);
        auto cleanup = defer([&] {
            e.execute_cql("DROP TABLE ks.tbl").get();
        });

        auto set_type = set_type_impl::get_instance(utf8_type, false);

        test_collection(e, set_type, set_type, {
            {
                "UPDATE ks.tbl set val = { 'apa', 'ko' } where pk=1 and pk2=11 and ck=111",
                data_value::make_null(set_type), // no previous value
                {
                    {
                        ::make_set_value(set_type, { "apa", "ko" }),
                        data_value::make_null(set_type),
                        true // setting entire column to null -> expect delete marker
                    }
                },
                ::make_set_value(set_type, { "apa", "ko" })
            },
            {
                "UPDATE ks.tbl set val = val + { 'ninja', 'mission' } where pk=1 and pk2=11 and ck=111",
                ::make_set_value(set_type, { "apa", "ko" }),
                {
                    {
                        ::make_set_value(set_type, { "mission", "ninja" }), // note the sorting of sets
                        data_value::make_null(set_type),
                    }
                },
                ::make_set_value(set_type, { "apa", "ko", "mission", "ninja" })
            },
            {
                "UPDATE ks.tbl set val = val - { 'apa' } where pk=1 and pk2=11 and ck=111",
                ::make_set_value(set_type, { "apa", "ko", "mission", "ninja" }),
                {
                    {
                        data_value::make_null(set_type),
                        ::make_set_value(set_type, { "apa" }),
                    }
                },
                ::make_set_value(set_type, { "ko", "mission", "ninja" })
            },
            {
                "UPDATE ks.tbl set val = val - { 'mission' }, val = val + { 'nils' } where pk=1 and pk2=11 and ck=111",
                ::make_set_value(set_type, { "ko", "mission", "ninja" }),
                {
                    {
                        ::make_set_value(set_type, { "nils" }),
                        ::make_set_value(set_type, { "mission" }),
                    }
                },
                ::make_set_value(set_type, { "ko", "nils", "ninja" })
            },
            {
                "UPDATE ks.tbl set val = { 'bolla', 'trolla' } where pk=1 and pk2=11 and ck=111",
                ::make_set_value(set_type, { "ko", "nils", "ninja" }),
                {
                    {
                        ::make_set_value(set_type, { "bolla", "trolla" }),
                        data_value::make_null(set_type),
                        true // setting entire column to null -> expect delete marker
                    }
                },
                ::make_set_value(set_type, { "bolla", "trolla" })
            }
        });
    }).get();
}

SEASTAR_THREAD_TEST_CASE(test_list_logging) {
    do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "CREATE TABLE ks.tbl (pk int, pk2 int, ck int, val list<text>, PRIMARY KEY((pk, pk2), ck)) WITH cdc = {'enabled':'true', 'preimage':'true', 'postimage':'true' }"s);
        auto cleanup = defer([&] {
            e.execute_cql("DROP TABLE ks.tbl").get();
        });

        auto list_type = list_type_impl::get_instance(utf8_type, false);
        auto uuids_type = set_type_impl::get_instance(timeuuid_type, false);
        auto val_type = map_type_impl::get_instance(list_type->name_comparator(), list_type->value_comparator(), false);

        test_collection(e, val_type, uuids_type, {
            {
                "UPDATE ks.tbl set val = [ 'apa', 'ko' ] where pk=1 and pk2=11 and ck=111",
                data_value::make_null(list_type),
                {
                    {
                        ::make_list_value(list_type, { "apa", "ko" }),
                        data_value::make_null(uuids_type),
                        true // setting entire column to null -> expect delete marker
                    }
                },
                ::make_list_value(list_type, { "apa", "ko" })
            },
            {
                "UPDATE ks.tbl set val = val + [ 'ninja', 'mission' ] where pk=1 and pk2=11 and ck=111",
                ::make_list_value(list_type, { "apa", "ko" }),
                {
                    {
                        ::make_list_value(list_type, { "ninja", "mission" }),
                        data_value::make_null(uuids_type),
                    }
                },
                ::make_list_value(list_type, { "apa", "ko", "ninja", "mission" })
            },
            {
                "UPDATE ks.tbl set val = [ 'bosse' ] + val where pk=1 and pk2=11 and ck=111",
                ::make_list_value(list_type, { "apa", "ko", "ninja", "mission" }),
                {
                    {
                        ::make_list_value(list_type, { "bosse" }),
                        data_value::make_null(uuids_type),
                    }
                },
                ::make_list_value(list_type, { "bosse", "apa", "ko", "ninja", "mission" })
            },
            {
                "DELETE val[0] from ks.tbl where pk=1 and pk2=11 and ck=111",
                ::make_list_value(list_type, { "bosse", "apa", "ko", "ninja", "mission" }),
                {
                    {
                        data_value::make_null(list_type), // the record is the timeuuid, should maybe check, but...
                        data_value::make_null(uuids_type),
                    }
                },
                ::make_list_value(list_type, { "apa", "ko", "ninja", "mission" })
            },
            {
                "UPDATE ks.tbl set val[0] = 'babar' where pk=1 and pk2=11 and ck=111",
                ::make_list_value(list_type, { "apa", "ko", "ninja", "mission" }),
                {
                    {
                        ::make_list_value(list_type, { "babar" }),
                        data_value::make_null(uuids_type),
                    }
                },
                ::make_list_value(list_type, { "babar", "ko", "ninja", "mission" })
            },
            {
                "UPDATE ks.tbl set val = ['bolla', 'trolla'] where pk=1 and pk2=11 and ck=111",
                ::make_list_value(list_type, { "babar", "ko", "ninja", "mission" }),
                {
                    {
                        ::make_list_value(list_type, { "bolla", "trolla" }),
                        data_value::make_null(uuids_type),
                        true // setting entire column to null -> expect delete marker
                    }
                },
                ::make_list_value(list_type, { "bolla", "trolla" })
            }
        }, [&](data_value v) {
            auto map = value_cast<map_type_impl::native_type>(std::move(v));
            auto cpy = boost::copy_range<std::vector<data_value>>(map | boost::adaptors::map_values);
            // verify key is timeuuid
            for (auto& key : map | boost::adaptors::map_keys) {
                value_cast<utils::UUID>(key);
            }
            return ::make_list_value(list_type, std::move(cpy));
        });
    }).get();
}

SEASTAR_THREAD_TEST_CASE(test_udt_logging) {
    do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "CREATE TYPE ks.mytype (field0 int, field1 text)"s);
        cquery_nofail(e, "CREATE TABLE ks.tbl (pk int, pk2 int, ck int, val mytype, PRIMARY KEY((pk, pk2), ck)) WITH cdc = {'enabled':'true', 'preimage':'true', 'postimage':'true' }"s);
        auto cleanup = defer([&] {
            e.execute_cql("DROP TABLE ks.tbl").get();
            e.execute_cql("DROP TYPE ks.mytype").get();
        });

        auto udt_type = user_type_impl::get_instance("ks", to_bytes("mytype"),
            { to_bytes("field0"), to_bytes("field1") },
            { int32_type, utf8_type },
            false
        );
        auto index_set_type = set_type_impl::get_instance(short_type, false);
        auto f0_type = int32_type;
        auto f1_type = utf8_type;

        auto make_tuple = [&](std::optional<std::optional<int32_t>> i, std::optional<std::optional<sstring>> s) {
            return ::make_user_value(udt_type, {
                i ? ::data_value(*i) : data_value::make_null(f0_type),
                s ? ::data_value(*s) : data_value::make_null(f1_type),
            });
        };

        test_collection(e, udt_type, index_set_type, {
            {
                "UPDATE ks.tbl set val = { field0: 12, field1: 'ko' } where pk=1 and pk2=11 and ck=111",
                data_value::make_null(udt_type),
                {
                    {
                        make_tuple(12, "ko"),
                        data_value::make_null(index_set_type), // no deleted cells
                        true // setting entire column to null -> expect delete marker
                    }
                },
                make_tuple(12, "ko")
            },
            {
                "UPDATE ks.tbl set val.field0 = 13 where pk=1 and pk2=11 and ck=111",
                make_tuple(12, "ko"),
                {
                    {
                        make_tuple(13, std::nullopt),
                        data_value::make_null(index_set_type),
                    }
                },
                make_tuple(13, "ko")
            },
            {
                "UPDATE ks.tbl set val.field1 = 'nils' where pk=1 and pk2=11 and ck=111",
                make_tuple(13, "ko"),
                {
                    {
                        make_tuple(std::nullopt, "nils"),
                        data_value::make_null(index_set_type),
                    }
                },
                make_tuple(13, "nils")
            },
            {
                "UPDATE ks.tbl set val.field1 = null where pk=1 and pk2=11 and ck=111",
                make_tuple(13, "nils"),
                {
                    {
                        make_tuple(std::nullopt, std::nullopt),
                        ::make_set_value(index_set_type, { int16_t(1) }), // delete field1 (index 1)
                    }
                },
                make_tuple(13, std::nullopt)
            },
            {
                "UPDATE ks.tbl set val = { field0: 1, field1: 'bolla' } where pk=1 and pk2=11 and ck=111",
                make_tuple(13, std::nullopt),
                {
                    {
                        make_tuple(1, "bolla"),
                        data_value::make_null(index_set_type),
                        true // setting entire column to null -> expect delete marker
                    }
                },
                make_tuple(1, "bolla")
            },
        });
    }).get();
}

SEASTAR_THREAD_TEST_CASE(test_frozen_logging) {
    do_with_cql_env_thread([] (cql_test_env& e) {
        const auto keyspace_name = "ks";
        const auto base_tbl_name = "tbl";
        const auto log_tbl_name = cdc::log_name(base_tbl_name);
        const auto column_name = "val";

        auto test_frozen = [&] (sstring type_string, sstring value_string) {
            BOOST_TEST_MESSAGE(format("Testing type {}", type_string));
            cquery_nofail(e, format("CREATE TABLE {}.{} (pk int, ck int, {} {}, PRIMARY KEY (pk, ck)) WITH cdc = {{'enabled': 'true'}}",
                    keyspace_name, base_tbl_name, column_name, type_string)).get();

            // Corresponding column in CDC log should have the same type
            const auto base_schema = e.local_db().find_schema(keyspace_name, base_tbl_name);
            const auto base_column = base_schema->get_column_definition(column_name);
            BOOST_TEST_MESSAGE(format("Column type in base table: {}", base_column->type->name()));

            const auto log_schema = e.local_db().find_schema(keyspace_name, log_tbl_name);
            const auto log_column = log_schema->get_column_definition(column_name);
            BOOST_TEST_MESSAGE(format("Column type in log table: {}", log_column->type->name()));

            BOOST_REQUIRE(base_column->type == log_column->type);

            cquery_nofail(e, format("INSERT INTO {}.{} (pk, ck, {}) VALUES (0, 0, {})",
                    keyspace_name, base_tbl_name, column_name, value_string)).get();

            // Expect only one row, with the same value as inserted
            const auto base_msg = e.execute_cql(format("SELECT {} FROM {}.{}", column_name, keyspace_name, base_tbl_name)).get0();
            const auto base_rows = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(base_msg);
            BOOST_REQUIRE(base_rows);
            const auto base_bytes = to_bytes(*base_rows);
            BOOST_REQUIRE_EQUAL(base_bytes.size(), 1);

            const auto log_msg = e.execute_cql(format("SELECT {} FROM {}.{}", column_name, keyspace_name, log_tbl_name)).get0();
            const auto log_rows = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(log_msg);
            BOOST_REQUIRE(log_rows);
            const auto log_bytes = to_bytes(*log_rows);
            BOOST_REQUIRE_EQUAL(log_bytes, base_bytes);

            cquery_nofail(e, format("DROP TABLE {}.{}", keyspace_name, base_tbl_name)).get();
        };

        cquery_nofail(e, format("CREATE TYPE {}.udt (a text, ccc text)", keyspace_name));

        test_frozen("frozen<list<text>>", "['a', 'bb', 'ccc']");
        test_frozen("frozen<set<text>>", "{'a', 'bb', 'ccc'}");
        test_frozen("frozen<map<text, text>>", "{'a': 'bb', 'ccc': 'dddd'}");
        test_frozen("frozen<udt>", "{a: 'bb', ccc: 'dddd'}");
    }).get();
}

SEASTAR_THREAD_TEST_CASE(test_update_insert_delete_distinction) {
    do_with_cql_env_thread([](cql_test_env& e) {
        const auto base_tbl_name = "tbl_rowdel";
        const int pk = 1, ck = 11;
        cquery_nofail(e, format("CREATE TABLE ks.{} (pk int, ck int, val int, PRIMARY KEY(pk, ck)) WITH cdc = {{'enabled':'true'}}", base_tbl_name));

        cquery_nofail(e, format("INSERT INTO ks.{} (pk, ck, val) VALUES ({}, {}, 222)", base_tbl_name, pk, ck)); // (0) an insert
        cquery_nofail(e, format("UPDATE ks.{} set val=111 WHERE pk={} and ck={}", base_tbl_name, pk, ck));       // (1) an update
        cquery_nofail(e, format("DELETE val FROM ks.{} WHERE pk = {} AND ck = {}", base_tbl_name, pk, ck));      // (2) also an update
        cquery_nofail(e, format("DELETE FROM ks.{} WHERE pk = {} AND ck = {}", base_tbl_name, pk, ck));          // (3) a row delete

        const sstring query = format("SELECT \"{}\" FROM ks.{}", cdc::log_meta_column_name("operation"), cdc::log_name(base_tbl_name));
        auto msg = e.execute_cql(query).get0();
        auto rows = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(msg);
        BOOST_REQUIRE(rows);
        auto results = to_bytes(*rows);
        BOOST_REQUIRE_EQUAL(results.size(), 4);  // 1 insert + 2 updates + 1 row delete == 4

        BOOST_REQUIRE_EQUAL(results[0].size(), 1);
        BOOST_REQUIRE_EQUAL(*results[0].front(), data_value(static_cast<int8_t>(cdc::operation::insert)).serialize_nonnull()); // log entry from (0)

        BOOST_REQUIRE_EQUAL(results[1].size(), 1);
        BOOST_REQUIRE_EQUAL(*results[1].front(), data_value(static_cast<int8_t>(cdc::operation::update)).serialize_nonnull()); // log entry from (1)

        BOOST_REQUIRE_EQUAL(results[2].size(), 1);
        BOOST_REQUIRE_EQUAL(*results[2].front(), data_value(static_cast<int8_t>(cdc::operation::update)).serialize_nonnull()); // log entry from (2)

        BOOST_REQUIRE_EQUAL(results[3].size(), 1);
        BOOST_REQUIRE_EQUAL(*results[3].front(), data_value(static_cast<int8_t>(cdc::operation::row_delete)).serialize_nonnull()); // log entry from (3)
    }).get();
}

static std::vector<std::vector<data_value>> get_result(cql_test_env& e,
        const std::vector<data_type>& col_types, const sstring& query) {
    auto deser = [] (const data_type& t, const bytes_opt& b) -> data_value {
        if (!b) {
            return data_value::make_null(t);
        }
        return t->deserialize(*b);
    };

    auto msg = e.execute_cql(query).get0();
    auto rows = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(msg);
    BOOST_REQUIRE(rows);

    std::vector<std::vector<data_value>> res;
    for (auto&& r: to_bytes(*rows)) {
        BOOST_REQUIRE_LE(col_types.size(), r.size());
        std::vector<data_value> res_r;
        for (size_t i = 0; i < col_types.size(); ++i) {
            res_r.push_back(deser(col_types[i], r[i]));
        }
        res.push_back(std::move(res_r));
    }
    return res;
}

SEASTAR_THREAD_TEST_CASE(test_change_splitting) {
    do_with_cql_env_thread([](cql_test_env& e) {
        using oper_ut = std::underlying_type_t<cdc::operation>;

        auto oper_type = data_type_for<oper_ut>();
        auto m_type = map_type_impl::get_instance(int32_type, int32_type, false);
        auto keys_type = set_type_impl::get_instance(int32_type, false);

        auto int_null = data_value::make_null(int32_type);
        auto map_null = data_value::make_null(m_type);
        auto keys_null = data_value::make_null(keys_type);
        auto long_null = data_value::make_null(long_type);
        auto bool_null = data_value::make_null(boolean_type);

        auto vmap = [&] (std::vector<std::pair<data_value, data_value>> m) {
            return make_map_value(m_type, std::move(m));
        };

        auto vkeys = [&] (std::vector<data_value> s) {
            return make_set_value(keys_type, std::move(s));
        };

        auto get_result = [&] (const std::vector<data_type>& col_types, const sstring& s) -> std::vector<std::vector<data_value>> {
            return ::get_result(e, col_types, s);
        };

        cquery_nofail(e, "create table ks.t (pk int, ck int, s int static, v1 int, v2 int, m map<int, int>, primary key (pk, ck)) with cdc = {'enabled':true}");

        auto now = api::new_timestamp();

        cquery_nofail(e, format(
            "begin unlogged batch"
            " update ks.t using timestamp {} set s = -1 where pk = 0;"
            " update ks.t using timestamp {} set v1 = 1 where pk = 0 and ck = 0;"
            " update ks.t using timestamp {} set v2 = 2 where pk = 0 and ck = 0;"
            " apply batch;",
            now, now + 1, now + 2));

        {
            auto result = get_result(
                {int32_type, int32_type, int32_type, int32_type},
                "select \"cdc$batch_seq_no\", s, v1, v2 from ks.t_scylla_cdc_log where pk = 0 allow filtering");
            BOOST_REQUIRE_EQUAL(result.size(), 3);

            std::vector<std::vector<data_value>> expected = {
                { int32_t(0), int32_t(-1), int_null, int_null},
                { int32_t(0), int_null, int32_t(1), int_null},
                { int32_t(0), int_null, int_null, int32_t(2)}
            };

            BOOST_REQUIRE_EQUAL(expected, result);
        }

        cquery_nofail(e, format("update ks.t using timestamp {} set m = null where pk = 0 and ck = 2;", now));
        {
            auto result = get_result(
                {m_type, boolean_type, keys_type, timeuuid_type},
                "select m, \"cdc$deleted_m\", \"cdc$deleted_elements_m\", \"cdc$time\""
                " from ks.t_scylla_cdc_log where pk = 0 and ck = 2 allow filtering");
            BOOST_REQUIRE_EQUAL(result.size(), 1);
            BOOST_REQUIRE_EQUAL(result[0].size(), 4);

            result[0][3] = utils::UUID_gen::micros_timestamp(value_cast<utils::UUID>(result[0][3]));
            std::vector<std::vector<data_value>> expected = {
                {map_null, true, keys_null, now}
            };
            BOOST_REQUIRE_EQUAL(expected, result);
        }

        cquery_nofail(e, format("update ks.t using timestamp {} set m = {{1:1}} where pk = 0 and ck = 3;", now));
        {
            auto result = get_result(
                {m_type, boolean_type, keys_type, timeuuid_type},
                "select m, \"cdc$deleted_m\", \"cdc$deleted_elements_m\", \"cdc$time\""
                " from ks.t_scylla_cdc_log where pk = 0 and ck = 3 allow filtering");
            BOOST_REQUIRE_EQUAL(result.size(), 1);
            BOOST_REQUIRE_EQUAL(result[0].size(), 4);

            result[0][3] = utils::UUID_gen::micros_timestamp(value_cast<utils::UUID>(result[0][3]));
            std::vector<std::vector<data_value>> expected = {
                {vmap({{1,1}}), true, keys_null, now}
            };
            BOOST_REQUIRE_EQUAL(expected, result);
        }

        cquery_nofail(e, format(
            "begin unlogged batch"
            " update ks.t using timestamp {} and ttl 5 set v1 = 5, v2 = null where pk = 0 and ck = 1;"
            " update ks.t using timestamp {} and ttl 6 set m = m + {{0:6, 1:6}} where pk = 0 and ck = 1;"
            " update ks.t using timestamp {} and ttl 7 set m[2] = 7, m[3] = null where pk = 0 and ck = 1;"
            " update ks.t using timestamp {} set m[4] = 0 where pk = 0 and ck = 1;"
            " apply batch;",
            now, now, now, now));

        {
            auto result = get_result(
                {int32_type, int32_type, boolean_type, m_type, keys_type, long_type},
                "select v1, v2, \"cdc$deleted_v2\", m, \"cdc$deleted_elements_m\", \"cdc$ttl\""
                " from ks.t_scylla_cdc_log where pk = 0 and ck = 1 allow filtering");
            BOOST_REQUIRE_EQUAL(result.size(), 4);

            std::vector<std::vector<data_value>> expected = {
                // The following represents the "v1 = 5" change. The "v2 = null" change gets merged with a different change, see below
                {int32_t(5), int_null, bool_null, map_null, keys_null, int64_t(5)},
                {int_null, int_null, bool_null, vmap({{0,6},{1,6}}), keys_null, int64_t(6)},
                // The following represents the "m[2] = 7" change. The "m[3] = null" change gets merged with a different change, see below
                {int_null, int_null, bool_null, vmap({{2,7}}), keys_null, int64_t(7)},
                // The "v2 = null" and "v[3] = null" changes get merged with the "m[4] = 0" change, because dead cells
                // don't have a "ttl" concept; thus we put them together with alive cells which don't have a ttl (so ttl column = null).
                {int_null, int_null, true, vmap({{4,0}}), vkeys({3}), long_null},
            };

            // These changes have the same timestamp, so their relative order in CDC log is arbitrary
            for (auto& er: expected) {
                BOOST_REQUIRE(std::find_if(result.begin(), result.end(), [&] (const std::vector<data_value>& r) {
                    return er == r;
                }) != result.end());
            }
        }

        {
            auto result = get_result({int32_type},
                "select \"cdc$batch_seq_no\" from ks.t_scylla_cdc_log where pk = 0 and ck = 1 allow filtering");
            std::vector<std::vector<data_value>> expected = {{int32_t(0)}, {int32_t(1)}, {int32_t(2)}, {int32_t(3)}};
            BOOST_REQUIRE_EQUAL(expected, result);
        }

        cquery_nofail(e, format(
            "begin unlogged batch"
            " delete from ks.t using timestamp {} where pk = 1;"
            " delete from ks.t using timestamp {} where pk = 1 and ck >= 0 and ck < 3;"
            " delete from ks.t using timestamp {} where pk = 1 and ck = 0;"
            " insert into ks.t (pk,ck,v1) values (1,0,1) using timestamp {};"
            " update ks.t using timestamp {} set v2 = 2 where pk = 1 and ck = 0;"
            " insert into ks.t (pk,ck,m) values (1,0,{{3:3}}) using timestamp {};"
            " insert into ks.t (pk,ck,m) values (1,1,{{4:4}}) using timestamp {} and ttl 5;"

            " apply batch;",
            now, now + 1, now + 2, now + 3, now + 3, now + 4, now + 5));

        {
            auto result = get_result(
                {int32_type, int32_type, int32_type, m_type, boolean_type, oper_type},
                "select \"cdc$batch_seq_no\", v1, v2, m, \"cdc$deleted_m\", \"cdc$operation\""
                " from ks.t_scylla_cdc_log where pk = 1 allow filtering");
            BOOST_REQUIRE_EQUAL(result.size(), 9);

            // TODO: It would be nice to check how these things work together with pre/post-images, but maybe in a separate test.

            std::vector<std::vector<data_value>> expected = {
                {int32_t(0), int_null, int_null, map_null, bool_null, oper_ut(cdc::operation::partition_delete)},
                {int32_t(0), int_null, int_null, map_null, bool_null, oper_ut(cdc::operation::range_delete_start_inclusive)},
                {int32_t(1), int_null, int_null, map_null, bool_null, oper_ut(cdc::operation::range_delete_end_exclusive)},
                {int32_t(0), int_null, int_null, map_null, bool_null, oper_ut(cdc::operation::row_delete)},
                {int32_t(0), int32_t(1), int32_t(2), map_null, bool_null, oper_ut(cdc::operation::update)},
                {int32_t(0), int_null, int_null, vmap({{3,3}}), true, oper_ut(cdc::operation::insert)},
                {int32_t(0), int_null, int_null, map_null, bool_null, oper_ut(cdc::operation::insert)}, // for ck == 1
                {int32_t(1), int_null, int_null, map_null, true, oper_ut(cdc::operation::update)},
                {int32_t(2), int_null, int_null, vmap({{4,4}}), bool_null, oper_ut(cdc::operation::update)},
            };

            BOOST_REQUIRE_EQUAL(expected, result);
        }

        cquery_nofail(e, "delete from ks.t where pk = 2 and ck < 1 and ck > 2;");

        {
            auto result = get_result(
                {int32_type, int32_type, m_type, boolean_type, oper_type},
                "select v1, v2, m, \"cdc$deleted_m\", \"cdc$operation\""
                " from ks.t_scylla_cdc_log where pk = 2 allow filtering");

            // A delete from a degenerate row range should produce no rows in CDC log
            BOOST_REQUIRE_EQUAL(result.size(), 0);
        }

        // Regression test for #6050
        cquery_nofail(e, "create table ks.t2 (pk int, ck int, s int static, cs set<text>, cm map<int, int>, primary key (pk, ck)) with cdc = {'enabled':true};");
        cquery_nofail(e, format("insert into ks.t2 (pk, ck, s, cs, cm) VALUES (1, 2, 3, {{'4'}}, {{5:6}}) using timestamp {};", now));

        {
            auto cs_type = set_type_impl::get_instance(ascii_type, false);
            auto cs_null = data_value::make_null(cs_type);
            auto cs_value = make_set_value(cs_type, {"4"});

            auto cm_type = map_type_impl::get_instance(int32_type, int32_type, false);
            auto cm_null = data_value::make_null(cm_type);
            auto cm_value = make_map_value(cm_type, {{3, 3}});

            auto result = get_result(
                {int32_type, int32_type, cs_type, cm_type, boolean_type, boolean_type, oper_type},
                "select \"cdc$batch_seq_no\", s, cs, cm, \"cdc$deleted_cs\", "
                "\"cdc$deleted_cm\", \"cdc$operation\" from ks.t2_scylla_cdc_log "
                "where pk = 1 allow filtering"
            );

            BOOST_REQUIRE_EQUAL(result.size(), 2);

            std::vector<std::vector<data_value>> expected = {
                {int32_t(0), int_null, cs_null, cm_null, true, true, oper_ut(cdc::operation::insert)},
                {int32_t(0), int32_t(3), cs_value, cm_value, bool_null, bool_null, oper_ut(cdc::operation::update)},
            };
        }

        // Splitting cells from INSERT with TTL and multiple collection columns
        cquery_nofail(e, "create table ks.t3 (pk int primary key, m1 map<int, int>, m2 map<int, int>) with cdc = {'enabled':true}");
        cquery_nofail(e, format(
            "insert into ks.t3 (pk, m1, m2) VALUES (0, {{1:1}}, {{2:2}}) using timestamp {} and ttl 5;", now));
        {
            auto result = get_result(
                {m_type, boolean_type, m_type, boolean_type, long_type, oper_type},
                "select m1, \"cdc$deleted_m1\", m2, \"cdc$deleted_m2\", \"cdc$ttl\", \"cdc$operation\""
                " from ks.t3_scylla_cdc_log where pk = 0 allow filtering");
            BOOST_REQUIRE_EQUAL(result.size(), 3);

            std::vector<std::vector<data_value>> expected = {
                { map_null, bool_null, map_null, bool_null, int64_t(5), oper_ut(cdc::operation::insert) }, // row marker
                { map_null, true, map_null, true, long_null, oper_ut(cdc::operation::update) }, // deletion of maps
                { vmap({{1,1}}), bool_null, vmap({{2,2}}), bool_null, int64_t(5), oper_ut(cdc::operation::update) } // addition of cells
            };
            BOOST_REQUIRE_EQUAL(expected, result);
        }
    }).get();
}

SEASTAR_THREAD_TEST_CASE(test_batch_with_row_delete) {
    do_with_cql_env_thread([](cql_test_env& e) {
        const auto base_tbl_name = "tbl_batchrowdel";
        const int pk = 0, ck = 0;

        cquery_nofail(e, "CREATE TYPE ks.mytype (a int, b int)");
        cquery_nofail(e, format("CREATE TABLE ks.{} (pk int, ck int, v1 int, v2 mytype, v3 map<int,int>, v4 set<int>, primary key (pk, ck)) WITH cdc = {{'enabled':true,'preimage':true}}", base_tbl_name));

        cquery_nofail(e, format("INSERT INTO ks.{} (pk, ck, v1, v2, v3, v4) VALUES ({}, {}, 1, (1,2), {{1:2,3:4}}, {{1,2,3}})", base_tbl_name, pk, ck));
        cquery_nofail(e, format(
                "BEGIN UNLOGGED BATCH"
                "   UPDATE ks.{tbl_name} set v1 = 666 WHERE pk = {pk} and ck = {ck};" // (1)
                "   DELETE FROM ks.{tbl_name} WHERE pk = {pk} AND ck = {ck}; "        // (2)
                "APPLY BATCH;",
                fmt::arg("tbl_name", base_tbl_name), fmt::arg("pk", pk), fmt::arg("ck", ck)));

        const sstring query = format("SELECT v1, v2, v3, v4, \"{}\" FROM ks.{}", cdc::log_meta_column_name("operation"), cdc::log_name(base_tbl_name));
        auto msg = e.execute_cql(query).get0();
        auto rows = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(msg);
        BOOST_REQUIRE(rows);
        auto results = to_bytes(*rows);

        auto udt_type = user_type_impl::get_instance("ks", "mytype", {to_bytes("a"), to_bytes("b")}, {int32_type, int32_type}, false);
        auto m_type = map_type_impl::get_instance(int32_type, int32_type, false);
        auto s_type = set_type_impl::get_instance(int32_type, false);
        using oper_ut = std::underlying_type_t<cdc::operation>;
        auto oper_type = data_type_for<oper_ut>();

        auto int_null = data_value::make_null(int32_type);
        auto udt_null = data_value::make_null(udt_type);
        auto map_null = data_value::make_null(m_type);
        auto set_null = data_value::make_null(s_type);

        const std::vector<std::vector<data_value>> expected = {
            // Update (0)
            {int32_t(1), make_user_value(udt_type, {1,2}), make_map_value(m_type, {{1,2},{3,4}}), make_set_value(s_type, {1,2,3}), oper_ut(cdc::operation::insert)},
            // Preimage for (1)
            {int32_t(1), make_user_value(udt_type, {1,2}), make_map_value(m_type, {{1,2},{3,4}}), make_set_value(s_type, {1,2,3}), oper_ut(cdc::operation::pre_image)},
            // Update (1)
            {int32_t(666), udt_null, map_null, set_null, oper_ut(cdc::operation::update)},
            // No preimage for (1) + (2), because it is in the same group
            // Row delete (2)
            {int_null, udt_null, map_null, set_null, oper_ut(cdc::operation::row_delete)},
        };

        auto deser = [] (const data_type& t, const bytes_opt& b) -> data_value {
            if (!b) {
                return data_value::make_null(t);
            }
            return t->deserialize(*b);
        };

        for (size_t idx = 0; idx < expected.size(); ++idx) {
            const auto& er = expected[idx];
            const auto& r = results[idx];
            BOOST_REQUIRE_EQUAL(deser(int32_type, r[0]), er[0]);
            BOOST_REQUIRE_EQUAL(deser(udt_type, r[1]), er[1]);
            BOOST_REQUIRE_EQUAL(deser(m_type, r[2]), er[2]);
            BOOST_REQUIRE_EQUAL(deser(s_type, r[3]), er[3]);
            BOOST_REQUIRE_EQUAL(deser(oper_type, r[4]), er[4]);
        }
    }).get();
}

struct image_set {
    using image_row = std::vector<data_value>;
    std::vector<image_row> preimage;
    std::vector<image_row> postimage;
};

struct image_persistence_test {
    std::vector<sstring> updates;
    std::vector<sstring> column_names;
    std::vector<image_set> groups;
};

static void test_pre_post_image(cql_test_env& e, const std::vector<image_persistence_test>& tests,
        bool preimage, bool postimage) {
    const auto keyspace_name = "ks"s;
    const auto base_table_name = "tbl"s;
    const auto log_table_name = cdc::log_name(base_table_name);

    const auto log_schema = e.local_db().find_schema(keyspace_name, log_table_name);

    std::unordered_set<bytes> processed_times;

    for (const auto& t : tests) {
        BOOST_TEST_MESSAGE("Starting next test case");
        for (const auto& update : t.updates) {
            BOOST_TEST_MESSAGE(format("Executing query {}", update));
            cquery_nofail(e, update);
        }

        const auto rows = select_log(e, base_table_name);
        BOOST_REQUIRE(rows);
        auto results = to_bytes(*rows);
        sort_by_time(*rows, results);

        // Indexed by serialized timeuuid of the group
        std::map<bytes, std::vector<std::vector<bytes_opt>>, serialized_compare> groups(timeuuid_type->as_less_comparator());
        const auto time_index = column_index(*rows, cdc::log_meta_column_name("time"));
        for (const auto& row : results) {
            const auto time = *row[time_index];
            if (!processed_times.contains(time)) {
                groups[time].push_back(row);
            }
        }

        // Register new encountered timestamps so that we won't repeat them in next run
        for (const auto& time : groups | boost::adaptors::map_keys) {
            processed_times.insert(time);
        }

        BOOST_TEST_MESSAGE(format("Returned rows: {}", groups));

        // Assert that there is the same number of groups differentiated by cdc$time
        BOOST_REQUIRE_EQUAL(groups.size(), t.groups.size());

        auto compare_rows = [&] (const std::vector<std::vector<bytes_opt>>& actual,
                std::vector<image_set::image_row> expected) {
            BOOST_REQUIRE_EQUAL(actual.size(), expected.size());

            for (const auto& actual_row : actual) {
                // Deserialize values in actual_row
                std::vector<data_value> actual_values;
                for (const auto& col_name : t.column_names) {
                    const auto col_def = log_schema->get_column_definition(to_bytes(col_name));
                    BOOST_REQUIRE(col_def);

                    const auto actual_type = col_def->type;
                    const auto col_idx_in_result = column_index(*rows, col_name);
                    const auto actual_data = actual_row[col_idx_in_result];
                    data_value actual_value = actual_data
                        ? actual_type->deserialize(*actual_data)
                        : data_value::make_null(actual_type);

                    actual_values.push_back(std::move(actual_value));
                }

                BOOST_TEST_MESSAGE(format("Looking up corresponding row to {}", actual_values));

                // Order in pre-postimage is unspecified
                const auto it = std::find(expected.begin(), expected.end(), actual_values);
                if (it == expected.end()) {
                    BOOST_FAIL(format("Failed to find corresponding expected row for {}", actual_values));
                }
                expected.erase(it);
            }
        };

        auto actual_it = groups.begin();
        auto expected_it = t.groups.begin();

        while (actual_it != groups.end()) {
            // Filter preimage and postimage
            // TODO: Assert that all preimages are at the beginning,
            // and that postimages are at the end
            const auto& actual_group_id = actual_it->first;
            const auto& actual_results = (actual_it++)->second;
            const auto& expected_set = *expected_it++;

            BOOST_TEST_MESSAGE(format("Checking group {}", actual_group_id));

            const auto actual_preimage = filter_by_operation(*rows, actual_results, cdc::operation::pre_image);
            if (preimage) {
                BOOST_TEST_MESSAGE("Checking preimage");
                compare_rows(actual_preimage, expected_set.preimage);
            } else {
                BOOST_TEST_MESSAGE("Preimage should be empty");
                BOOST_REQUIRE_EQUAL(actual_preimage.size(), 0);
            }

            const auto actual_postimage = filter_by_operation(*rows, actual_results, cdc::operation::post_image);
            if (postimage) {
                BOOST_TEST_MESSAGE("Checking postimage");
                compare_rows(actual_postimage, expected_set.postimage);
            } else {
                BOOST_TEST_MESSAGE("Postimage should be empty");
                BOOST_REQUIRE_EQUAL(actual_postimage.size(), 0);
            }
        }
    }
}

void test_batch_images(bool preimage, bool postimage) {
    do_with_cql_env_thread([preimage, postimage] (cql_test_env& e) {
        cquery_nofail(e, format(
                "CREATE TABLE ks.tbl (pk int, ck int, s int STATIC, v1 int, v2 int, vm map<int, int>, PRIMARY KEY(pk, ck))"
                " WITH cdc = {{'enabled':'true', 'preimage':'{}', 'postimage':'{}'}}",
                preimage ? "true" : "false", postimage ? "true" : "false"));

        const auto now = api::new_timestamp();

        const auto map_type = map_type_impl::get_instance(int32_type, int32_type, false);
        const auto map_null = data_value::make_null(map_type);
        const auto int_null = data_value::make_null(int32_type);

        test_pre_post_image(e, {
            // Insert multiple clustering rows
            {
                {
                    "BEGIN UNLOGGED BATCH"
                    "   INSERT INTO ks.tbl (pk, ck, v1) VALUES (0, 1, 10);"
                    "   INSERT INTO ks.tbl (pk, ck, v1) VALUES (0, 2, 20);"
                    "APPLY BATCH"
                },
                {"ck", "v1"},
                {
                    {
                        .postimage = {
                            {int32_t(1), int32_t(10)},
                            {int32_t(2), int32_t(20)}
                        }
                    }
                }
            },
            // Update multiple clustering rows (same pk as before)
            {
                {
                    "BEGIN UNLOGGED BATCH"
                    "   UPDATE ks.tbl SET v1 = 11 WHERE pk = 0 AND ck = 1;"
                    "   UPDATE ks.tbl SET v2 = 22 WHERE pk = 0 AND ck = 2;"
                    "APPLY BATCH"
                },
                {"ck", "v1", "v2"},
                {
                    {
                        .preimage = {
                            // Preimage only contains columns that are modified,
                            // therefore the second row does not have value for v1
                            {int32_t(1), int32_t(10), int_null},
                            {int32_t(2), int_null, int_null}
                        },
                        .postimage = {
                            {int32_t(1), int32_t(11), int_null},
                            {int32_t(2), int32_t(20), int32_t(22)}
                        }
                    }
                }
            },
            // Delete clustering rows (same pk as before)
            {
                {
                    "BEGIN UNLOGGED BATCH"
                    "   DELETE FROM ks.tbl WHERE pk = 0 AND ck = 1;"
                    "   DELETE FROM ks.tbl WHERE pk = 0 AND ck = 2;"
                    "APPLY BATCH"
                },
                {"ck", "v1", "v2"},
                {
                    {
                        .preimage = {
                            // Preimage for delete contains everything
                            {int32_t(1), int32_t(11), int_null},
                            {int32_t(2), int32_t(20), int32_t(22)}
                        },
                    }
                }
            },
            // Clustering row and static row
            {
                {
                    "BEGIN UNLOGGED BATCH"
                    "   UPDATE ks.tbl SET s = 5 WHERE pk = 1;"
                    "   UPDATE ks.tbl SET v1 = 10 WHERE pk = 1 AND ck = 1;"
                    "APPLY BATCH"
                },
                {"ck", "s", "v1"},
                {
                    {
                        .postimage = {
                            {int_null, int32_t(5), int_null},
                            {int32_t(1), int_null, int32_t(10)}
                        }
                    }
                }
            },
            // Multiple columns in one row, different ttl
            {
                {
                    "BEGIN UNLOGGED BATCH"
                    "   UPDATE ks.tbl USING TTL 100 SET v1 = 10 WHERE pk = 2 AND ck = 0;"
                    "   UPDATE ks.tbl USING TTL 200 SET v2 = 20 WHERE pk = 2 AND ck = 0;"
                    "APPLY BATCH"
                },
                {"ck", "v1", "v2"},
                {
                    {
                        .postimage = {
                            {int32_t(0), int32_t(10), int32_t(20)}
                        }
                    }
                }
            },
            // Single row and column with multiple ttls (reproduces #6597)
            {
                {
                    "BEGIN UNLOGGED BATCH"
                    "   UPDATE ks.tbl USING TTL 100 SET vm = vm + {1:2} WHERE pk = 6597 AND ck = 0;"
                    "   UPDATE ks.tbl USING TTL 200 SET vm = vm + {3:4} WHERE pk = 6597 AND ck = 0;"
                    "APPLY BATCH"
                },
                {"ck", "vm"},
                {
                    {
                        .postimage = {
                            {int32_t(0), ::make_map_value(map_type, {{1,2},{3,4}})}
                        }
                    }
                }
            },
            // Single row and column, multiple timestamps
            {
                {
                    format("BEGIN UNLOGGED BATCH"
                        "   UPDATE ks.tbl USING TIMESTAMP {} SET vm = vm + {{1:2}} WHERE pk = 3 AND ck = 0;"
                        "   UPDATE ks.tbl USING TIMESTAMP {} SET vm = vm + {{3:4}} WHERE pk = 3 AND ck = 0;"
                        "APPLY BATCH",
                        now + 1, now + 2)
                },
                {"ck", "vm"},
                {
                    // First timestamp
                    {
                        .postimage = {
                            {int32_t(0), ::make_map_value(map_type, {{1,2}})}
                        }
                    },
                    // Second timestamp
                    {
                        .preimage = {
                            {int32_t(0), ::make_map_value(map_type, {{1,2}})}
                        },
                        .postimage = {
                            {int32_t(0), ::make_map_value(map_type, {{1,2},{3,4}})}
                        }
                    }
                }
            },
            // Reproducer for #6598
            {
                {
                    // Timestamps are necessary so that the first UPDATE will appear earlier in CDC log
                    format("UPDATE ks.tbl USING TIMESTAMP {} SET vm = {{1:2}} WHERE pk = 6598 AND ck = 1;", now + 1),
                    format("BEGIN UNLOGGED BATCH"
                        "   UPDATE ks.tbl USING TIMESTAMP {} SET vm = {{}} WHERE pk = 6598 AND ck = 0;"
                        "   UPDATE ks.tbl USING TIMESTAMP {} SET vm = vm + {{3:4}} WHERE pk = 6598 AND ck = 1;"
                        "APPLY BATCH", now + 2, now + 2)
                },
                {"ck", "vm"},
                {
                    // Non-batch UPDATE
                    {
                        .postimage = {
                            {int32_t(1), ::make_map_value(map_type, {{1,2}})}
                        }
                    },
                    // Batch
                    {
                        .preimage = {
                            {int32_t(1), ::make_map_value(map_type, {{1,2}})}
                        },
                        .postimage = {
                            {int32_t(0), map_null},
                            {int32_t(1), ::make_map_value(map_type, {{1,2},{3,4}})}
                        }
                    },
                }
            }
        }, preimage, postimage);
    }).get();
}

SEASTAR_THREAD_TEST_CASE(test_batch_pre_image) {
    test_batch_images(true, false);
}

SEASTAR_THREAD_TEST_CASE(test_batch_post_image) {
    test_batch_images(false, true);
}

SEASTAR_THREAD_TEST_CASE(test_batch_pre_post_image) {
    test_batch_images(true, true);
}

// Regression test for #7716
SEASTAR_THREAD_TEST_CASE(test_postimage_with_no_regular_columns) {
    do_with_cql_env_thread([] (cql_test_env& e) {
        using oper_ut = std::underlying_type_t<cdc::operation>;

        cquery_nofail(e, "create table ks.t (pk int, ck int, primary key (pk, ck)) with cdc = {'enabled': true, 'postimage': true}");
        cquery_nofail(e, "insert into ks.t (pk, ck) values (1, 2)");

        auto result = get_result(e,
            {data_type_for<oper_ut>(), int32_type, int32_type},
            "select \"cdc$operation\", pk, ck from ks.t_scylla_cdc_log");

        std::vector<std::vector<data_value>> expected = {
            { oper_ut(cdc::operation::insert), int32_t(1), int32_t(2) },
            { oper_ut(cdc::operation::post_image), int32_t(1), int32_t(2) },
        };

        BOOST_REQUIRE_EQUAL(expected, result);
    }).get();
}

SEASTAR_THREAD_TEST_CASE(test_image_deleted_column) {
    // Test that cdc$deleted_ columns are correctly
    // filled in the pre/post-image rows.
    //
    // Pre-image rows should set cdc$deleted_ column:
    // 1. If pre-image is in 'full' mode, for
    // all NULL columns in the read pre-image.
    // 2. If pre-image is in 'true' mode, for
    // all columns that are both affected and
    // have NULL value in the read pre-image.
    //
    // Post-image rows should never set cdc$deleted_
    // column, as post-images are always in 'full'
    // mode. Filling cdc$deleted_ columns would
    // bring no value.

    do_with_cql_env_thread([] (cql_test_env& e) {
        using oper_ut = std::underlying_type_t<cdc::operation>;
        auto oper_type = data_type_for<oper_ut>();
        auto int32_set_type = set_type_impl::get_instance(int32_type, false);
        auto make_int32_set = [&](int32_t value) {
            return ::make_set_value(int32_set_type, { value }); 
        };

        auto perform_test = [&](bool full_preimage) {
            sstring preimage_mode = full_preimage ? "'full'" : "true";

            // Create a table and insert data with v = NULL
            cquery_nofail(e, "drop table if exists tbl");
            cquery_nofail(e, format("create table tbl(pk int, ck int, v int, v2 int, primary key(pk, ck)) with cdc = {{'enabled': true, 'preimage': {}, 'postimage': true}}", preimage_mode));
            cquery_nofail(e, "insert into tbl(pk, ck, v, v2) values (1, 1, null, 1)");
            cquery_nofail(e, "insert into tbl(pk, ck, v, v2) values (2, 2, null, 2)");

            // These are the queried columns:
            sstring query_string = "select \"cdc$operation\", pk, ck, v, \"cdc$deleted_v\", \"cdc$deleted_v2\" from tbl_scylla_cdc_log";

            // Perform an insert that does not affect v column.
            // Pre-image: v=NULL, cdc$deleted_v=NULL
            // Pre-image ('full' mode): v=NULL, cdc$deleted_v=true
            // Post-image should not set cdc$deleted_ columns
            cquery_nofail(e, "insert into tbl(pk, ck, v2) values (1, 1, 1)");
            data_value deleted_v = full_preimage ? true : data_value::make_null(boolean_type);
            std::vector<data_value> preimage1 = 
                { oper_ut(cdc::operation::pre_image), int32_t(1), int32_t(1), data_value::make_null(int32_type), deleted_v, data_value::make_null(boolean_type) };
            std::vector<data_value> postimage1 = 
                { oper_ut(cdc::operation::post_image), int32_t(1), int32_t(1), data_value::make_null(int32_type), data_value::make_null(boolean_type), data_value::make_null(boolean_type) };

            // Perform an insert that affects v column.
            // Pre-image: v=NULL, cdc$deleted_v=true
            // Pre-image ('full' mode): v=NULL, cdc$deleted_v=true
            // Post-image should not set cdc$deleted_ columns   
            cquery_nofail(e, "insert into tbl(pk, ck, v, v2) values (2, 2, 2, 2)");
            std::vector<data_value> preimage2 = 
                { oper_ut(cdc::operation::pre_image), int32_t(2), int32_t(2), data_value::make_null(int32_type), true, data_value::make_null(boolean_type) };
            std::vector<data_value> postimage2 = 
                { oper_ut(cdc::operation::post_image), int32_t(2), int32_t(2), int32_t(2), data_value::make_null(boolean_type), data_value::make_null(boolean_type) };

            auto result = get_result(e, {oper_type, int32_type, int32_type, int32_type, boolean_type, boolean_type}, query_string);
            BOOST_REQUIRE_EQUAL(result.size(), 10);
            BOOST_REQUIRE_EQUAL(result[2], preimage1);
            BOOST_REQUIRE_EQUAL(result[4], postimage1);
            BOOST_REQUIRE_EQUAL(result[7], preimage2);
            BOOST_REQUIRE_EQUAL(result[9], postimage2);

            auto test_table_with_collection = [&](bool frozen_collection) {
                // Create a table and insert data with v = NULL
                cquery_nofail(e, "drop table if exists tbl");
                if (frozen_collection) {
                    cquery_nofail(e, 
                        format("create table tbl(pk int, ck int, v frozen<set<int>>, v2 frozen<set<int>>, primary key(pk, ck)) with cdc = {{'enabled': true, 'preimage': {}, 'postimage': true}}", preimage_mode));
                } else {
                    cquery_nofail(e, 
                        format("create table tbl(pk int, ck int, v set<int>, v2 set<int>, primary key(pk, ck)) with cdc = {{'enabled': true, 'preimage': {}, 'postimage': true}}", preimage_mode));
                }
                cquery_nofail(e, "insert into tbl(pk, ck, v, v2) values (1, 1, null, {1})");
                cquery_nofail(e, "insert into tbl(pk, ck, v, v2) values (2, 2, null, {2})");

                // These are the queried columns:
                sstring query_string = "select \"cdc$operation\", pk, ck, v, \"cdc$deleted_v\", \"cdc$deleted_v2\" from tbl_scylla_cdc_log";

                // Perform an insert that does not affect v column.
                // Pre-image: v=NULL, cdc$deleted_v=NULL
                // Pre-image ('full' mode): v=NULL, cdc$deleted_v=true
                // Post-image should not set cdc$deleted_ columns
                cquery_nofail(e, "insert into tbl(pk, ck, v2) values (1, 1, {1})");
                std::vector<data_value> preimage1 = 
                    { oper_ut(cdc::operation::pre_image), int32_t(1), int32_t(1), data_value::make_null(int32_set_type), deleted_v, data_value::make_null(boolean_type) };
                std::vector<data_value> postimage1 = 
                    { oper_ut(cdc::operation::post_image), int32_t(1), int32_t(1), data_value::make_null(int32_set_type), data_value::make_null(boolean_type), data_value::make_null(boolean_type) };

                // Perform an insert that affects v column.
                // Pre-image: v=NULL, cdc$deleted_v=true
                // Pre-image ('full' mode): v=NULL, cdc$deleted_v=true
                // Post-image should not set cdc$deleted_ columns   
                cquery_nofail(e, "insert into tbl(pk, ck, v, v2) values (2, 2, {2}, {2})");
                std::vector<data_value> preimage2 = 
                    { oper_ut(cdc::operation::pre_image), int32_t(2), int32_t(2), data_value::make_null(int32_set_type), true, data_value::make_null(boolean_type) };
                std::vector<data_value> postimage2 = 
                    { oper_ut(cdc::operation::post_image), int32_t(2), int32_t(2), make_int32_set(2), data_value::make_null(boolean_type), data_value::make_null(boolean_type) };

                auto result = get_result(e, {oper_type, int32_type, int32_type, int32_set_type, boolean_type, boolean_type}, query_string);
                BOOST_REQUIRE_EQUAL(result.size(), 10);
                BOOST_REQUIRE_EQUAL(result[2], preimage1);
                BOOST_REQUIRE_EQUAL(result[4], postimage1);
                BOOST_REQUIRE_EQUAL(result[7], preimage2);
                BOOST_REQUIRE_EQUAL(result[9], postimage2);
            };

            test_table_with_collection(false);
            test_table_with_collection(true);
        };

        perform_test(false);
        perform_test(true);
    }).get();
}
