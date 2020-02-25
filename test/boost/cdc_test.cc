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

#include <seastar/testing/thread_test_case.hh>
#include <string>
#include <boost/range/adaptor/map.hpp>

#include "cdc/log.hh"
#include "db/config.hh"
#include "schema_builder.hh"
#include "test/lib/cql_assertions.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/exception_utils.hh"
#include "transport/messages/result_message.hh"

#include "types.hh"
#include "types/tuple.hh"
#include "types/map.hh"
#include "types/list.hh"
#include "types/set.hh"
#include "types/user.hh"

using namespace std::string_literals;

static logging::logger tlog("cdc_test");

static cql_test_config mk_cdc_test_config() {
    shared_ptr<db::config> cfg(make_shared<db::config>());
    auto features = cfg->experimental_features();
    features.emplace_back(db::experimental_features_t::CDC);
    cfg->experimental_features(features);
    return cql_test_config(std::move(cfg));
};

namespace cdc {
api::timestamp_type find_timestamp(const schema&, const mutation&);
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
                auto ts = cdc::find_timestamp(*schema, m);
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
    }, mk_cdc_test_config()).get();
}

SEASTAR_THREAD_TEST_CASE(test_generate_timeuuid) {
    auto seed = std::random_device{}();
    tlog.info("test_generate_timeuuid seed: {}", seed);

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
    }, mk_cdc_test_config()).get();
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
    }, mk_cdc_test_config()).get();
}

SEASTAR_THREAD_TEST_CASE(test_permissions_of_cdc_log_table) {
    do_with_cql_env_thread([] (cql_test_env& e) {
        auto assert_unauthorized = [&e] (const sstring& stmt) {
            BOOST_TEST_MESSAGE(format("Must throw unauthorized_exception: {}", stmt));
            BOOST_REQUIRE_THROW(e.execute_cql(stmt).get(), exceptions::unauthorized_exception);
        };

        e.execute_cql("CREATE TABLE ks.tbl (a int PRIMARY KEY) WITH cdc = {'enabled': true}").get();
        e.require_table_exists("ks", "tbl").get();

        // Allow MODIFY, SELECT, ALTER
        e.execute_cql("INSERT INTO ks.tbl_scylla_cdc_log (stream_id_1, stream_id_2, time, batch_seq_no) VALUES (0, 0, now(), 0)").get();
        e.execute_cql("UPDATE ks.tbl_scylla_cdc_log SET ttl = 100 WHERE stream_id_1 = 0 AND stream_id_2 = 0 AND time = now() AND batch_seq_no = 0").get();
        e.execute_cql("DELETE FROM ks.tbl_scylla_cdc_log WHERE stream_id_1 = 0 AND stream_id_2 = 0 AND time = now() AND batch_seq_no = 0").get();
        e.execute_cql("SELECT * FROM ks.tbl_scylla_cdc_log").get();
        e.execute_cql("ALTER TABLE ks.tbl_scylla_cdc_log ALTER ttl TYPE blob").get();

        // Disallow DROP
        assert_unauthorized("DROP TABLE ks.tbl_scylla_cdc_log");
    }, mk_cdc_test_config()).get();
}

SEASTAR_THREAD_TEST_CASE(test_disallow_cdc_on_materialized_view) {
    do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE ks.tbl (a int PRIMARY KEY)").get();
        e.require_table_exists("ks", "tbl").get();

        BOOST_REQUIRE_THROW(e.execute_cql("CREATE MATERIALIZED VIEW ks.mv AS SELECT a FROM ks.tbl PRIMARY KEY (a) WITH cdc = {'enabled': true}").get(), exceptions::invalid_request_exception);
        e.require_table_does_not_exist("ks", "mv").get();
    }, mk_cdc_test_config()).get();
}

SEASTAR_THREAD_TEST_CASE(test_permissions_of_cdc_description) {
    do_with_cql_env_thread([] (cql_test_env& e) {
        auto test_table = [&e] (const sstring& table_name) {
            auto assert_unauthorized = [&e] (const sstring& stmt) {
                BOOST_TEST_MESSAGE(format("Must throw unauthorized_exception: {}", stmt));
                BOOST_REQUIRE_THROW(e.execute_cql(stmt).get(), exceptions::unauthorized_exception);
            };

            e.require_table_exists("system_distributed", table_name).get();

            const sstring full_name = "system_distributed." + table_name;

            // Allow MODIFY, SELECT
            e.execute_cql(format("INSERT INTO {} (time) VALUES (toTimeStamp(now()))", full_name)).get();
            e.execute_cql(format("UPDATE {} SET expired = toTimeStamp(now()) WHERE time = toTimeStamp(now())", full_name)).get();
            e.execute_cql(format("DELETE FROM {} WHERE time = toTimeStamp(now())", full_name)).get();
            e.execute_cql(format("SELECT * FROM {}", full_name)).get();

            // Disallow ALTER, DROP
            assert_unauthorized(format("ALTER TABLE {} ALTER time TYPE blob", full_name));
            assert_unauthorized(format("DROP TABLE {}", full_name));
        };

        test_table("cdc_description");
        test_table("cdc_topology_description");
    }, mk_cdc_test_config()).get();
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
static std::vector<std::vector<bytes_opt>> to_bytes_filtered(const cql_transport::messages::result_message::rows& rows, cdc::operation op, const Comp& comp = {}) {
    const auto op_type = data_type_for<std::underlying_type_t<cdc::operation>>();

    auto results = to_bytes(rows);
    auto op_index = column_index(rows, "operation");
    auto op_bytes = op_type->decompose(std::underlying_type_t<cdc::operation>(op));

    results.erase(std::remove_if(results.begin(), results.end(), [&](const std::vector<bytes_opt>& bo) {
        return !comp(op_bytes, bo[op_index]);
    }), results.end());

    return results;
}

static void sort_by_time(const cql_transport::messages::result_message::rows& rows, std::vector<std::vector<bytes_opt>>& results) {
    auto time_index = column_index(rows, "time");
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
        cquery_nofail(e, "CREATE TABLE ks.tbl (pk int, pk2 int, ck int, ck2 int, val int, PRIMARY KEY((pk, pk2), ck, ck2)) WITH cdc = {'enabled':'true'}");
        cquery_nofail(e, "INSERT INTO ks.tbl(pk, pk2, ck, ck2, val) VALUES(1, 11, 111, 1111, 11111)");
        cquery_nofail(e, "INSERT INTO ks.tbl(pk, pk2, ck, ck2, val) VALUES(1, 22, 222, 2222, 22222)");
        cquery_nofail(e, "INSERT INTO ks.tbl(pk, pk2, ck, ck2, val) VALUES(1, 33, 333, 3333, 33333)");
        cquery_nofail(e, "INSERT INTO ks.tbl(pk, pk2, ck, ck2, val) VALUES(1, 44, 444, 4444, 44444)");
        cquery_nofail(e, "INSERT INTO ks.tbl(pk, pk2, ck, ck2, val) VALUES(2, 11, 111, 1111, 11111)");
        cquery_nofail(e, "INSERT INTO ks.tbl(pk, pk2, ck, ck2, val) VALUES(3, 11, 111, 1111, 11111) USING TTL 600");
        cquery_nofail(e, "DELETE val FROM ks.tbl WHERE pk = 1 AND pk2 = 11 AND ck = 111 AND ck2 = 1111");
        cquery_nofail(e, "DELETE FROM ks.tbl WHERE pk = 1 AND pk2 = 11 AND ck = 111 AND ck2 = 1111");
        cquery_nofail(e, "DELETE FROM ks.tbl WHERE pk = 1 AND pk2 = 11 AND ck > 222 AND ck <= 444");
        cquery_nofail(e, "UPDATE ks.tbl SET val = 555 WHERE pk = 2 AND pk2 = 11 AND ck = 111 AND ck2 = 1111");
        cquery_nofail(e, "UPDATE ks.tbl USING TTL 3600 SET val = 444 WHERE pk = 3 AND pk2 = 11 AND ck = 111 AND ck2 = 1111");
        cquery_nofail(e, "DELETE FROM ks.tbl WHERE pk = 1 AND pk2 = 11");
        auto msg = e.execute_cql(format("SELECT time, \"_pk\", \"_pk2\", \"_ck\", \"_ck2\", operation, ttl FROM ks.{}", cdc::log_name("tbl"))).get0();
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
    }, mk_cdc_test_config()).get();
}

SEASTAR_THREAD_TEST_CASE(test_pre_image_logging) {
    do_with_cql_env_thread([](cql_test_env& e) {
        auto test = [&e] (bool enabled, bool with_ttl) {
            cquery_nofail(e, "CREATE TABLE ks.tbl (pk int, pk2 int, ck int, ck2 int, val int, PRIMARY KEY((pk, pk2), ck, ck2)) WITH cdc = {'enabled':'true', 'preimage':'"s + (enabled ? "true" : "false") + "'}");
            cquery_nofail(e, "INSERT INTO ks.tbl(pk, pk2, ck, ck2, val) VALUES(1, 11, 111, 1111, 11111)"s + (with_ttl ? " USING TTL 654" : ""));

            auto rows = select_log(e, "tbl");

            BOOST_REQUIRE(to_bytes_filtered(*rows, cdc::operation::pre_image).empty());

            auto first = to_bytes_filtered(*rows, cdc::operation::update);

            auto ck2_index = column_index(*rows, "_ck2");
            auto val_index = column_index(*rows, "_val");
            auto ttl_index = column_index(*rows, "ttl");

            auto val_type = tuple_type_impl::get_instance({ data_type_for<std::underlying_type_t<cdc::column_op>>(), int32_type, long_type});
            auto val = *first[0][val_index];

            BOOST_REQUIRE_EQUAL(int32_type->decompose(1111), first[0][ck2_index]);
            BOOST_REQUIRE_EQUAL(data_value(11111), value_cast<tuple_type_impl::native_type>(val_type->deserialize(bytes_view(val))).at(1));

            auto last = 11111;
            int64_t last_ttl = 654;
            for (auto i = 0u; i < 10; ++i) {
                auto nv = last + 1;
                const int64_t new_ttl = 100 * (i + 1);
                cquery_nofail(e, "UPDATE ks.tbl" + (with_ttl ? format(" USING TTL {}", new_ttl) : "") + " SET val=" + std::to_string(nv) +" where pk=1 AND pk2=11 AND ck=111 AND ck2=1111");

                rows = select_log(e, "tbl");

                auto pre_image = to_bytes_filtered(*rows, cdc::operation::pre_image);
                auto second = to_bytes_filtered(*rows, cdc::operation::update);

                if (!enabled) {
                    BOOST_REQUIRE(pre_image.empty());
                } else {
                    sort_by_time(*rows, second);
                    BOOST_REQUIRE_EQUAL(pre_image.size(), i + 1);

                    val = *pre_image.back()[val_index];
                    BOOST_REQUIRE_EQUAL(int32_type->decompose(1111), pre_image[0][ck2_index]);
                    BOOST_REQUIRE_EQUAL(data_value(last), value_cast<tuple_type_impl::native_type>(val_type->deserialize(bytes_view(val))).at(1));
                    BOOST_REQUIRE_EQUAL(bytes_opt(), pre_image.back()[ttl_index]);

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
            e.execute_cql("DROP TABLE ks.tbl").get();
        };
        test(true, true);
        test(true, false);
        test(false, true);
        test(false, false);
    }, mk_cdc_test_config()).get();
}

SEASTAR_THREAD_TEST_CASE(test_range_deletion) {
    do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "CREATE TABLE ks.tbl (pk int, ck int, val int, PRIMARY KEY(pk, ck)) WITH cdc = {'enabled':'true'}");
        cquery_nofail(e, "DELETE FROM ks.tbl WHERE pk = 123 AND ck > 1 AND ck < 23");
        cquery_nofail(e, "DELETE FROM ks.tbl WHERE pk = 123 AND ck >= 4 AND ck <= 56");

        auto msg = e.execute_cql(format("SELECT time, \"_pk\", \"_ck\", operation FROM ks.{}", cdc::log_name("tbl"))).get0();
        auto rows = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(msg);
        BOOST_REQUIRE(rows);

        auto results = to_bytes(*rows);
        sort_by_time(*rows, results);

        auto ck_index = column_index(*rows, "_ck");
        auto ck_type = int32_type;
        auto op_index = column_index(*rows, "operation");
        auto op_type = data_type_for<std::underlying_type_t<cdc::operation>>();

        size_t row_idx = 0;

        auto check_row = [&](int32_t ck, cdc::operation operation) {
            BOOST_TEST_MESSAGE(format("{}", results[row_idx][ck_index]));
            BOOST_TEST_MESSAGE(format("{}", bytes_opt(ck_type->decompose(ck))));
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
    }, mk_cdc_test_config()).get();
}

SEASTAR_THREAD_TEST_CASE(test_add_columns) {
    do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "CREATE TABLE ks.tbl (pk int, pk2 int, ck int, ck2 int, val int, PRIMARY KEY((pk, pk2), ck, ck2)) WITH cdc = {'enabled':'true'}");
        cquery_nofail(e, "INSERT INTO ks.tbl(pk, pk2, ck, ck2, val) VALUES(1, 11, 111, 1111, 11111)");

        auto rows = select_log(e, "tbl");

        BOOST_REQUIRE(!to_bytes_filtered(*rows, cdc::operation::update).empty());

        cquery_nofail(e, "ALTER TABLE ks.tbl ADD kokos text");
        cquery_nofail(e, "INSERT INTO ks.tbl(pk, pk2, ck, ck2, val, kokos) VALUES(1, 11, 111, 1111, 11111, 'kaka')");

        rows = select_log(e, "tbl");
        auto updates = to_bytes_filtered(*rows, cdc::operation::update);
        sort_by_time(*rows, updates);

        auto kokos_index = column_index(*rows, "_kokos");
        auto kokos_type = tuple_type_impl::get_instance({ data_type_for<std::underlying_type_t<cdc::column_op>>(), utf8_type, long_type});
        auto kokos = *updates.back()[kokos_index];

        BOOST_REQUIRE_EQUAL(data_value("kaka"), value_cast<tuple_type_impl::native_type>(kokos_type->deserialize(bytes_view(kokos))).at(1));
    }, mk_cdc_test_config()).get();
}

// #5582 - just quickly test that we can create the cdc enabled table on a different shard 
// and still get the logs proper. 
SEASTAR_THREAD_TEST_CASE(test_cdc_across_shards) {
    do_with_cql_env_thread([](cql_test_env& e) {
        if (smp::count < 2) {
            tlog.warn("This test case requires at least 2 shards");
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

        BOOST_REQUIRE(!to_bytes_filtered(*rows, cdc::operation::update).empty());
    }, mk_cdc_test_config()).get();
}

SEASTAR_THREAD_TEST_CASE(test_negative_ttl_fail) {
    do_with_cql_env_thread([](cql_test_env& e) {
        BOOST_REQUIRE_EXCEPTION(e.execute_cql("CREATE TABLE ks.fail (a int PRIMARY KEY, b int) WITH cdc = {'enabled':true,'ttl':'-1'}").get0(),
                exceptions::configuration_exception,
                exception_predicate::message_contains("ttl"));
    }, mk_cdc_test_config()).get();
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
    }, mk_cdc_test_config()).get();
}

// helper funcs + structs for collection testing
using translate_func = std::function<data_value(data_value)>;
struct col_test {
    sstring update;
    data_value prev;
    data_value next;
    cdc::column_op op = cdc::column_op::add;
};

// iterate a set of updates and verify pre and delta values. 
static void test_collection(cql_test_env& e, data_type val_type, std::vector<col_test> tests, translate_func f = [](data_value v) { return v; }) {
    using op_ut = std::underlying_type_t<cdc::column_op>;
    auto col_type = tuple_type_impl::get_instance({ data_type_for<op_ut>(), val_type, long_type});

    for (auto& t : tests) {
        cquery_nofail(e, t.update);
        
        auto rows = select_log(e, "tbl");
        auto pre_image = to_bytes_filtered(*rows, cdc::operation::pre_image);
        auto updates = to_bytes_filtered(*rows, cdc::operation::update);

        sort_by_time(*rows, updates);
        sort_by_time(*rows, pre_image);

        auto val_index = column_index(*rows, "_val");

        if (t.prev.is_null()) {
            BOOST_REQUIRE(pre_image.empty());
        } else {
            BOOST_REQUIRE_GT(pre_image.size(), 0);
            auto val = *pre_image.back()[val_index];
            BOOST_REQUIRE_EQUAL(t.prev, f(value_cast<tuple_type_impl::native_type>(col_type->deserialize(bytes_view(val))).at(1)));
        }

        auto val = *updates.back()[val_index];
        auto tup = value_cast<tuple_type_impl::native_type>(col_type->deserialize(bytes_view(val)));

        if (!t.next.is_null()) {
            BOOST_REQUIRE_EQUAL(t.next, f(tup.at(1)));
        }

        BOOST_REQUIRE_EQUAL(data_value(op_ut(t.op)), tup.at(0));
    }
}

SEASTAR_THREAD_TEST_CASE(test_map_logging) {
    do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "CREATE TABLE ks.tbl (pk int, pk2 int, ck int, val map<text, text>, PRIMARY KEY((pk, pk2), ck)) WITH cdc = {'enabled':'true', 'preimage':'true' }"s);
        auto cleanup = defer([&] {
            e.execute_cql("DROP TABLE ks.tbl").get();
        });


        auto map_type = map_type_impl::get_instance(utf8_type, utf8_type, false);

        test_collection(e, map_type, {
            { 
                "UPDATE ks.tbl set val = { 'apa':'ko' } where pk=1 and pk2=11 and ck=111",
                data_value::make_null(map_type), // no prev value
                ::make_map_value(map_type, { { "apa", "ko" } }), // delta
                cdc::column_op::set
            },
            { 
                "UPDATE ks.tbl set val = val + { 'ninja':'mission' } where pk=1 and pk2=11 and ck=111",
                ::make_map_value(map_type, { { "apa", "ko" } }),
                ::make_map_value(map_type, { { "ninja", "mission" } })
            },
            { 
                "UPDATE ks.tbl set val['ninja'] = 'shuriken' where pk=1 and pk2=11 and ck=111",
                ::make_map_value(map_type, { { "apa", "ko" }, { "ninja", "mission" } }), 
                ::make_map_value(map_type, { { "ninja", "shuriken" } })
            },
            { 
                "UPDATE ks.tbl set val['apa'] = null where pk=1 and pk2=11 and ck=111",
                ::make_map_value(map_type, { { "apa", "ko" }, { "ninja", "shuriken" } }), 
                ::make_map_value(map_type, { { "apa", data_value::make_null(utf8_type) } }),
                cdc::column_op::del
            }
        });
    }, mk_cdc_test_config()).get();
}

SEASTAR_THREAD_TEST_CASE(test_set_logging) {
    do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "CREATE TABLE ks.tbl (pk int, pk2 int, ck int, val set<text>, PRIMARY KEY((pk, pk2), ck)) WITH cdc = {'enabled':'true', 'preimage':'true' }"s);
        auto cleanup = defer([&] {
            e.execute_cql("DROP TABLE ks.tbl").get();
        });

        auto set_type = set_type_impl::get_instance(utf8_type, false);
        
        test_collection(e, set_type, {
            {
                "UPDATE ks.tbl set val = { 'apa', 'ko' } where pk=1 and pk2=11 and ck=111",
                data_value::make_null(set_type), ::make_set_value(set_type, { "apa", "ko" }), 
                cdc::column_op::set
            },
            {
                "UPDATE ks.tbl set val = val + { 'ninja', 'mission' } where pk=1 and pk2=11 and ck=111",
                ::make_set_value(set_type, { "apa", "ko" }),
                ::make_set_value(set_type, { "mission", "ninja" }) // note the sorting of sets
            },
            {
                "UPDATE ks.tbl set val = val - { 'apa' } where pk=1 and pk2=11 and ck=111",
                ::make_set_value(set_type, { "apa", "ko", "mission", "ninja" }), 
                ::make_set_value(set_type, { "apa" }),
                cdc::column_op::del
            }
        });
    }, mk_cdc_test_config()).get();
}

SEASTAR_THREAD_TEST_CASE(test_list_logging) {
    do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "CREATE TABLE ks.tbl (pk int, pk2 int, ck int, val list<text>, PRIMARY KEY((pk, pk2), ck)) WITH cdc = {'enabled':'true', 'preimage':'true' }"s);
        auto cleanup = defer([&] {
            e.execute_cql("DROP TABLE ks.tbl").get();
        });

        auto list_type = list_type_impl::get_instance(utf8_type, false);
        auto val_type = map_type_impl::get_instance(list_type->name_comparator(), list_type->value_comparator(), false);
        
        test_collection(e, val_type, {
            {
                "UPDATE ks.tbl set val = [ 'apa', 'ko' ] where pk=1 and pk2=11 and ck=111",
                data_value::make_null(list_type), ::make_list_value(list_type, { "apa", "ko" }), 
                cdc::column_op::set
            },
            {
                "UPDATE ks.tbl set val = val + [ 'ninja', 'mission' ] where pk=1 and pk2=11 and ck=111",
                ::make_list_value(list_type, { "apa", "ko" }),
                ::make_list_value(list_type, { "ninja", "mission" })
            },
            {
                "UPDATE ks.tbl set val = [ 'bosse' ] + val where pk=1 and pk2=11 and ck=111",
                ::make_list_value(list_type, { "apa", "ko", "ninja", "mission" }),
                ::make_list_value(list_type, { "bosse" })
            },
            {
                "DELETE val[0] from ks.tbl where pk=1 and pk2=11 and ck=111",
                ::make_list_value(list_type, { "bosse", "apa", "ko", "ninja", "mission" }),
                data_value::make_null(list_type), // the record is the timeuuid, should maybe check, but...
                cdc::column_op::del
            },
            {
                "UPDATE ks.tbl set val[0] = 'babar' where pk=1 and pk2=11 and ck=111",
                ::make_list_value(list_type, { "apa", "ko", "ninja", "mission" }), 
                ::make_list_value(list_type, { "babar" }),
            }
        }, [&](data_value v) {
            auto map = value_cast<map_type_impl::native_type>(std::move(v));
            auto cpy = boost::copy_range<std::vector<data_value>>(map | boost::adaptors::map_values);
            return ::make_list_value(list_type, std::move(cpy));
        });
    }, mk_cdc_test_config()).get();
}

SEASTAR_THREAD_TEST_CASE(test_udt_logging) {
    do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "CREATE TYPE ks.mytype (field0 int, field1 text)"s);
        cquery_nofail(e, "CREATE TABLE ks.tbl (pk int, pk2 int, ck int, val mytype, PRIMARY KEY((pk, pk2), ck)) WITH cdc = {'enabled':'true', 'preimage':'true' }"s);
        auto cleanup = defer([&] {
            e.execute_cql("DROP TABLE ks.tbl").get();
            e.execute_cql("DROP TYPE ks.mytype").get();
        });

        auto udt_type = user_type_impl::get_instance("ks", to_bytes("mytype"), 
            { to_bytes("field0"), to_bytes("field1") },
            { int32_type, utf8_type },
            true
        );
        auto f0_type = tuple_type_impl::get_instance({ int32_type });
        auto f1_type = tuple_type_impl::get_instance({ utf8_type });
        auto cdc_tuple_type = tuple_type_impl::get_instance({ f0_type, f1_type });
        
        auto make_tuple = [&](std::optional<std::optional<int32_t>> i, std::optional<std::optional<sstring>> s) {
            return ::make_tuple_value(cdc_tuple_type, {
                i ? ::make_tuple_value(f0_type, { *i }) : data_value::make_null(f0_type),
                s ? ::make_tuple_value(f1_type, { *s }) : data_value::make_null(f1_type),
            });
        };
        
        test_collection(e, cdc_tuple_type, {
            {
                "UPDATE ks.tbl set val = { field0: 12, field1: 'ko' } where pk=1 and pk2=11 and ck=111",
                data_value::make_null(cdc_tuple_type), make_tuple(12, "ko"), 
                cdc::column_op::set
            },
            {
                "UPDATE ks.tbl set val.field0 = 13 where pk=1 and pk2=11 and ck=111",
                make_tuple(12, "ko"),
                make_tuple(13, std::nullopt)
            },
            {
                "UPDATE ks.tbl set val.field1 = 'nils' where pk=1 and pk2=11 and ck=111",
                make_tuple(13, "ko"),
                make_tuple(std::nullopt, "nils")
            },
            {
                "UPDATE ks.tbl set val.field1 = null where pk=1 and pk2=11 and ck=111",
                make_tuple(13, "nils"),
                make_tuple(std::nullopt, std::optional<std::optional<sstring>>{ std::in_place, std::nullopt }),
                cdc::column_op::del
            },
        });
    }, mk_cdc_test_config()).get();
}

SEASTAR_THREAD_TEST_CASE(test_row_delete) {
    do_with_cql_env_thread([](cql_test_env& e) {
        const auto base_tbl_name = "tbl_rowdel";
        const int pk = 1, ck = 11;
        cquery_nofail(e, format("CREATE TABLE ks.{} (pk int, ck int, val int, PRIMARY KEY(pk, ck)) WITH cdc = {{'enabled':'true'}}", base_tbl_name));

        cquery_nofail(e, format("UPDATE ks.{} set val=111 WHERE pk={} and ck={}", base_tbl_name, pk, ck)); // an update
        cquery_nofail(e, format("DELETE val FROM ks.{} WHERE pk = {} AND ck = {}", base_tbl_name, pk, ck)); // also an update
        cquery_nofail(e, format("DELETE FROM ks.{} WHERE pk = {} AND ck = {}", base_tbl_name, pk, ck)); // a row delete

        const sstring query = format("SELECT operation FROM ks.{}", cdc::log_name(base_tbl_name));
        auto msg = e.execute_cql(query).get0();
        auto rows = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(msg);
        BOOST_REQUIRE(rows);
        auto results = to_bytes(*rows);
        BOOST_REQUIRE_EQUAL(results.size(), 3);  // 2 rows for update + 1 for delete

        BOOST_REQUIRE_EQUAL(results[0].size(), 1);
        BOOST_REQUIRE_EQUAL(*results[0].front(), data_value(cdc::operation::update).serialize_nonnull());

        BOOST_REQUIRE_EQUAL(results[1].size(), 1);
        BOOST_REQUIRE_EQUAL(*results[1].front(), data_value(cdc::operation::update).serialize_nonnull());

        BOOST_REQUIRE_EQUAL(results[2].size(), 1);
        BOOST_REQUIRE_EQUAL(*results[2].front(), data_value(cdc::operation::row_delete).serialize_nonnull());
    }, mk_cdc_test_config()).get();
}
