/*
 * Copyright 2015 Cloudius Systems
 */

#include <boost/range/irange.hpp>
#include "cql3/query_processor.hh"
#include "cql3/query_options.hh"
#include "core/distributed.hh"
#include "tests/test-utils.hh"
#include "tests/urchin/cql_test_env.hh"
#include "to_string.hh"

struct conversation_state {
    service::storage_proxy proxy;
    cql3::query_processor qp;
    service::client_state client_state;
    service::query_state query_state;
    cql3::query_options& options;

    conversation_state(distributed<database>& db, const sstring& ks_name)
        : proxy(db)
        , qp(proxy, db)
        , client_state(service::client_state::for_internal_calls())
        , query_state(client_state)
        , options(cql3::query_options::DEFAULT)
    {
        client_state.set_keyspace(ks_name);
    }

    auto execute_cql(const sstring& text) {
        return qp.process(text, query_state, options);
    }
};

static const sstring ks_name = "ks";
static const sstring table_name = "cf";

static future<> require_column_has_value(distributed<database>& ddb, const sstring& ks_name, const sstring& table_name,
    std::vector<boost::any> pk, std::vector<boost::any> ck, const sstring& column_name, boost::any expected)
{
    auto& db = ddb.local();
    auto ks = db.find_keyspace(ks_name);
    assert(ks != nullptr);
    auto cf = ks->find_column_family(table_name);
    assert(cf != nullptr);
    auto schema = cf->_schema;
    auto pkey = partition_key::from_deeply_exploded(*schema, pk);
    auto dk = dht::global_partitioner().decorate_key(pkey);
    auto shard = db.shard_of(dk._token);
    return ddb.invoke_on(shard, [pkey = std::move(pkey),
                                 ck = std::move(ck),
                                 ks_name = std::move(ks_name),
                                 column_name = std::move(column_name),
                                 expected = std::move(expected),
                                 table_name = std::move(table_name)] (database& db) {
        auto ks = db.find_keyspace(ks_name);
        assert(ks != nullptr);
        auto cf = ks->find_column_family(table_name);
        assert(cf != nullptr);
        auto schema = cf->_schema;
        auto p = cf->find_partition(pkey);
        assert(p != nullptr);
        auto row = p->find_row(clustering_key::from_deeply_exploded(*schema, ck));
        assert(row != nullptr);
        auto col_def = schema->get_column_definition(utf8_type->decompose(column_name));
        assert(col_def != nullptr);
        auto i = row->find(col_def->id);
        if (i == row->end()) {
            assert(((void)"column not set", 0));
        }
        bytes actual;
        if (!col_def->type->is_multi_cell()) {
            auto cell = i->second.as_atomic_cell();
            assert(cell.is_live());
            actual = { cell.value().begin(), cell.value().end() };
        } else {
            auto cell = i->second.as_collection_mutation();
            auto type = dynamic_pointer_cast<collection_type_impl>(col_def->type);
            actual = type->to_value(type->deserialize_mutation_form(cell),
                    serialization_format::internal());
        }
        assert(col_def->type->equal(actual, col_def->type->decompose(expected)));
        row->find(col_def->id);
    });
}

SEASTAR_TEST_CASE(test_create_keyspace_statement) {
    auto db = make_shared<distributed<database>>();
    auto state = make_shared<conversation_state>(*db, ks_name);

    return db->start().then([state] {
        return state->execute_cql("create keyspace ks with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };").discard_result();
    }).finally([db] {
        return db->stop().finally([db] {});
    });
}

SEASTAR_TEST_CASE(test_create_table_statement) {
    auto db = make_shared<distributed<database>>();
    auto state = make_shared<conversation_state>(*db, ks_name);

    return db->start().then([state] {
        return state->execute_cql("create table users (user_name varchar PRIMARY KEY, birth_year bigint);").discard_result();
    }).finally([db] {
        return db->stop().finally([db] {});
    });
}

SEASTAR_TEST_CASE(test_insert_statement) {
    auto db = make_shared<distributed<database>>();
    auto state = make_shared<conversation_state>(*db, ks_name);

    // CQL: create table cf (p1 varchar, c1 int, r1 int, PRIMARY KEY (p1, c1));
    return db->start().then([db] {
        return db->invoke_on_all([] (database& db) {
            keyspace ks;
            auto cf_schema = make_lw_shared(schema(ks_name, table_name,
                {{"p1", utf8_type}}, {{"c1", int32_type}}, {{"r1", int32_type}}, {}, utf8_type));
            ks.column_families.emplace(table_name, column_family(cf_schema));
            db.keyspaces.emplace(ks_name, std::move(ks));
        });
    }).then([state, db] {
        return state->execute_cql("insert into cf (p1, c1, r1) values ('key1', 1, 100);").discard_result();
    }).then([state, db] {
        return require_column_has_value(*db, ks_name, table_name, {sstring("key1")}, {1}, "r1", 100);
    }).then([state, db] {
        return state->execute_cql("update cf set r1 = 66 where p1 = 'key1' and c1 = 1;").discard_result();
    }).then([state, db] {
        return require_column_has_value(*db, ks_name, table_name, {sstring("key1")}, {1}, "r1", 66);
    }).then([db] {
        return db->stop();
    }).then_wrapped([db] (future<> f) mutable {
        return make_ready_future<>();
    });
}

class rows_assertions {
    shared_ptr<transport::messages::result_message::rows> _rows;
public:
    rows_assertions(shared_ptr<transport::messages::result_message::rows> rows)
        : _rows(rows)
    { }

    rows_assertions with_size(size_t size) {
        auto row_count = _rows->rs().size();
        if (row_count != size) {
            BOOST_FAIL(sprint("Expected %d row(s) but got %d", size, row_count));
        }
        return {*this};
    }

    rows_assertions with_row(std::initializer_list<bytes_opt> values) {
        std::vector<bytes_opt> expected_row(values);
        for (auto&& row : _rows->rs().rows()) {
            if (row == expected_row) {
                return {*this};
            }
        }
        BOOST_FAIL("Expected row not found");
        return {*this};
    }

    // Verifies that the result has the following rows and only that rows, in that order.
    rows_assertions with_rows(std::initializer_list<std::initializer_list<bytes_opt>> rows) {
        auto actual_i = _rows->rs().rows().begin();
        auto actual_end = _rows->rs().rows().end();
        int row_nr = 0;
        for (auto&& row : rows) {
            if (actual_i == actual_end) {
                BOOST_FAIL(sprint("Expected more rows (%d), got %d", rows.size(), _rows->rs().size()));
            }
            auto& actual = *actual_i;
            if (!std::equal(
                    std::begin(row), std::end(row),
                    std::begin(actual), std::end(actual))) {
                BOOST_FAIL(sprint("row %d differs, expected %s got %s", row_nr, to_string(row), to_string(actual)));
            }
            ++actual_i;
            ++row_nr;
        }
        if (actual_i != actual_end) {
            BOOST_FAIL(sprint("Expected less rows (%d), got %d. Next row is: %s", rows.size(), _rows->rs().size(),
                to_string(*actual_i)));
        }
        return {*this};
    }
};

class result_msg_assertions {
    shared_ptr<transport::messages::result_message> _msg;
public:
    result_msg_assertions(shared_ptr<transport::messages::result_message> msg)
        : _msg(msg)
    { }

    rows_assertions is_rows() {
        auto rows = dynamic_pointer_cast<transport::messages::result_message::rows>(_msg);
        BOOST_REQUIRE(rows);
        return rows_assertions(rows);
    }
};

result_msg_assertions assert_that(shared_ptr<transport::messages::result_message> msg) {
    return result_msg_assertions(msg);
}

SEASTAR_TEST_CASE(test_select_statement) {
    auto db = make_shared<distributed<database>>();
    auto state = make_shared<conversation_state>(*db, ks_name);

    return db->start().then([db] {
        return db->invoke_on_all([] (database& db) {
            keyspace ks;
            // CQL: create table cf (p1 varchar, c1 int, c2 int, r1 int, PRIMARY KEY (p1, c1, c2));
            auto cf_schema = make_lw_shared(schema(ks_name, table_name,
                {{"p1", utf8_type}},
                {{"c1", int32_type}, {"c2", int32_type}},
                {{"r1", int32_type}},
                {},
                utf8_type));
            ks.column_families.emplace(table_name, column_family(cf_schema));
            db.keyspaces.emplace(ks_name, std::move(ks));
        });
    }).then([state] {
        return state->execute_cql("insert into cf (p1, c1, c2, r1) values ('key1', 1, 2, 3);").discard_result();
    }).then([state] {
        return state->execute_cql("insert into cf (p1, c1, c2, r1) values ('key2', 1, 2, 13);").discard_result();
    }).then([state] {
        return state->execute_cql("insert into cf (p1, c1, c2, r1) values ('key3', 1, 2, 23);").discard_result();
    }).then([state] {
        // Test wildcard
        return state->execute_cql("select * from cf where p1 = 'key1' and c2 = 2 and c1 = 1;").then([] (auto msg) {
            assert_that(msg).is_rows()
                .with_size(1)
                .with_row({
                     {utf8_type->decompose(sstring("key1"))},
                     {int32_type->decompose(1)},
                     {int32_type->decompose(2)},
                     {int32_type->decompose(3)}
                 });
        });
    }).then([state] {
        // Test with only regular column
        return state->execute_cql("select r1 from cf where p1 = 'key1' and c2 = 2 and c1 = 1;").then([] (auto msg) {
            assert_that(msg).is_rows()
                .with_size(1)
                .with_row({
                     {int32_type->decompose(3)}
                 });
        });
    }).then([state] {
        // Test full partition range, singular clustering range
        return state->execute_cql("select * from cf where c1 = 1 and c2 = 2 allow filtering;").then([] (auto msg) {
            assert_that(msg).is_rows()
                .with_size(3)
                .with_row({
                     {utf8_type->decompose(sstring("key1"))},
                     {int32_type->decompose(1)},
                     {int32_type->decompose(2)},
                     {int32_type->decompose(3)}})
                .with_row({
                     {utf8_type->decompose(sstring("key2"))},
                     {int32_type->decompose(1)},
                     {int32_type->decompose(2)},
                     {int32_type->decompose(13)}})
                .with_row({
                     {utf8_type->decompose(sstring("key3"))},
                     {int32_type->decompose(1)},
                     {int32_type->decompose(2)},
                     {int32_type->decompose(23)}
                 });
        });
    }).finally([db] {
        return db->stop().finally([db] {});
    });
}

SEASTAR_TEST_CASE(test_cassandra_stress_like_write_and_read) {
    auto db = make_shared<distributed<database>>();
    auto state = make_shared<conversation_state>(*db, ks_name);

    auto execute_update_for_key = [state] (sstring key) {
        return state->execute_cql(sprint("UPDATE cf SET "
            "\"C0\" = 0x8f75da6b3dcec90c8a404fb9a5f6b0621e62d39c69ba5758e5f41b78311fbb26cc7a,"
            "\"C1\" = 0xa8761a2127160003033a8f4f3d1069b7833ebe24ef56b3beee728c2b686ca516fa51,"
            "\"C2\" = 0x583449ce81bfebc2e1a695eb59aad5fcc74d6d7311fc6197b10693e1a161ca2e1c64,"
            "\"C3\" = 0x62bcb1dbc0ff953abc703bcb63ea954f437064c0c45366799658bd6b91d0f92908d7,"
            "\"C4\" = 0x222fcbe31ffa1e689540e1499b87fa3f9c781065fccd10e4772b4c7039c2efd0fb27 "
            "WHERE \"KEY\"=%s;", key)).discard_result();
    };

    auto verify_row_for_key = [state] (sstring key) {
        return state->execute_cql(sprint("select \"C0\", \"C1\", \"C2\", \"C3\", \"C4\" from cf where \"KEY\" = %s", key)).then([] (auto msg) {
            assert_that(msg).is_rows()
                .with_size(1)
                .with_row({
                    {from_hex("8f75da6b3dcec90c8a404fb9a5f6b0621e62d39c69ba5758e5f41b78311fbb26cc7a")},
                    {from_hex("a8761a2127160003033a8f4f3d1069b7833ebe24ef56b3beee728c2b686ca516fa51")},
                    {from_hex("583449ce81bfebc2e1a695eb59aad5fcc74d6d7311fc6197b10693e1a161ca2e1c64")},
                    {from_hex("62bcb1dbc0ff953abc703bcb63ea954f437064c0c45366799658bd6b91d0f92908d7")},
                    {from_hex("222fcbe31ffa1e689540e1499b87fa3f9c781065fccd10e4772b4c7039c2efd0fb27")}
                 });
        });
    };

    return db->start().then([db] {
        return db->invoke_on_all([] (database& db) {
            keyspace ks;
            auto cf_schema = make_lw_shared(schema(ks_name, table_name,
                {{"KEY", bytes_type}},
                {},
                {{"C0", bytes_type}, {"C1", bytes_type}, {"C2", bytes_type}, {"C3", bytes_type}, {"C4", bytes_type}},
                {},
                utf8_type));
            ks.column_families.emplace(table_name, column_family(cf_schema));
            db.keyspaces.emplace(ks_name, std::move(ks));
        });
    }).then([execute_update_for_key, verify_row_for_key] {
        static auto make_key = [] (int suffix) { return sprint("0xdeadbeefcafebabe%02d", suffix); };
        auto suffixes = boost::irange(0, 10);
        return parallel_for_each(suffixes.begin(), suffixes.end(), [execute_update_for_key] (int suffix) {
            return execute_update_for_key(make_key(suffix));
        }).then([suffixes, verify_row_for_key] {
            return parallel_for_each(suffixes.begin(), suffixes.end(), [verify_row_for_key] (int suffix) {
                return verify_row_for_key(make_key(suffix));
            });
        });
    }).finally([db] {
        return db->stop().finally([db] {});
    });
}

SEASTAR_TEST_CASE(test_range_queries) {
    auto db = make_shared<distributed<database>>();
    auto state = make_shared<conversation_state>(*db, ks_name);

    return db->start().then([db] {
        return db->invoke_on_all([] (database& db) {
            keyspace ks;
            auto cf_schema = make_lw_shared(schema(ks_name, table_name,
                {{"k", bytes_type}},
                {{"c0", bytes_type}, {"c1", bytes_type}},
                {{"v", bytes_type}},
                {},
                utf8_type));
            ks.column_families.emplace(table_name, column_family(cf_schema));
            db.keyspaces.emplace(ks_name, std::move(ks));
        });
    }).then([state] {
        return state->execute_cql("update cf set v = 0x01 where k = 0x00 and c0 = 0x01 and c1 = 0x01;").discard_result();
    }).then([state] {
        return state->execute_cql("update cf set v = 0x02 where k = 0x00 and c0 = 0x01 and c1 = 0x02;").discard_result();
    }).then([state] {
        return state->execute_cql("update cf set v = 0x03 where k = 0x00 and c0 = 0x01 and c1 = 0x03;").discard_result();
    }).then([state] {
        return state->execute_cql("update cf set v = 0x04 where k = 0x00 and c0 = 0x02 and c1 = 0x02;").discard_result();
    }).then([state] {
        return state->execute_cql("update cf set v = 0x05 where k = 0x00 and c0 = 0x02 and c1 = 0x03;").discard_result();
    }).then([state] {
        return state->execute_cql("update cf set v = 0x06 where k = 0x00 and c0 = 0x02 and c1 = 0x04;").discard_result();
    }).then([state] {
        return state->execute_cql("update cf set v = 0x07 where k = 0x00 and c0 = 0x03 and c1 = 0x04;").discard_result();
    }).then([state] {
        return state->execute_cql("update cf set v = 0x08 where k = 0x00 and c0 = 0x03 and c1 = 0x05;").discard_result();
    }).then([state] {
       return state->execute_cql("select v from cf where k = 0x00").then([] (auto msg) {
           assert_that(msg).is_rows()
               .with_rows({
                   {from_hex("01")},
                   {from_hex("02")},
                   {from_hex("03")},
                   {from_hex("04")},
                   {from_hex("05")},
                   {from_hex("06")},
                   {from_hex("07")},
                   {from_hex("08")}
               });
       });
    }).then([state] {
        return state->execute_cql("select v from cf where k = 0x00 and c0 = 0x02 allow filtering;").then([] (auto msg) {
            assert_that(msg).is_rows().with_rows({
                {from_hex("04")}, {from_hex("05")}, {from_hex("06")}
            });
        });
    }).then([state] {
        return state->execute_cql("select v from cf where k = 0x00 and c0 > 0x02 allow filtering;").then([] (auto msg) {
            assert_that(msg).is_rows().with_rows({
                {from_hex("07")}, {from_hex("08")}
            });
        });
    }).then([state] {
       return state->execute_cql("select v from cf where k = 0x00 and c0 >= 0x02 allow filtering;").then([] (auto msg) {
           assert_that(msg).is_rows().with_rows({
               {from_hex("04")}, {from_hex("05")}, {from_hex("06")}, {from_hex("07")}, {from_hex("08")}
           });
       });
    }).then([state] {
       return state->execute_cql("select v from cf where k = 0x00 and c0 >= 0x02 and c0 < 0x03 allow filtering;").then([] (auto msg) {
           assert_that(msg).is_rows().with_rows({
               {from_hex("04")}, {from_hex("05")}, {from_hex("06")}
           });
       });
    }).then([state] {
       return state->execute_cql("select v from cf where k = 0x00 and c0 > 0x02 and c0 <= 0x03 allow filtering;").then([] (auto msg) {
           assert_that(msg).is_rows().with_rows({
               {from_hex("07")}, {from_hex("08")}
           });
       });
    }).then([state] {
       return state->execute_cql("select v from cf where k = 0x00 and c0 >= 0x02 and c0 <= 0x02 allow filtering;").then([] (auto msg) {
           assert_that(msg).is_rows().with_rows({
               {from_hex("04")}, {from_hex("05")}, {from_hex("06")}
           });
       });
    }).then([state] {
       return state->execute_cql("select v from cf where k = 0x00 and c0 < 0x02 allow filtering;").then([] (auto msg) {
           assert_that(msg).is_rows().with_rows({
               {from_hex("01")}, {from_hex("02")}, {from_hex("03")}
           });
       });
    }).then([state] {
       return state->execute_cql("select v from cf where k = 0x00 and c0 = 0x02 and c1 > 0x02 allow filtering;").then([] (auto msg) {
           assert_that(msg).is_rows().with_rows({
               {from_hex("05")}, {from_hex("06")}
           });
       });
    }).then([state] {
       return state->execute_cql("select v from cf where k = 0x00 and c0 = 0x02 and c1 >= 0x02 and c1 <= 0x02 allow filtering;").then([] (auto msg) {
           assert_that(msg).is_rows().with_rows({
               {from_hex("04")}
           });
       });
    }).finally([db] {
        return db->stop().finally([db] {});
    });
}

SEASTAR_TEST_CASE(test_ordering_of_composites_with_variable_length_components) {
    return do_with_cql_env([] (auto& e) {
        return e.create_table([](auto ks) {
            return schema(ks, "cf",
                {{"k", bytes_type}},
                // We need more than one clustering column so that the single-element tuple format optimisation doesn't kick in
                {{"c0", bytes_type}, {"c1", bytes_type}},
                {{"v", bytes_type}},
                {},
                utf8_type);
        }).then([&e] {
            return e.execute_cql("update cf set v = 0x01 where k = 0x00 and c0 = 0x0001 and c1 = 0x00;").discard_result();
        }).then([&e] {
            return e.execute_cql("update cf set v = 0x02 where k = 0x00 and c0 = 0x03 and c1 = 0x00;").discard_result();
        }).then([&e] {
            return e.execute_cql("update cf set v = 0x03 where k = 0x00 and c0 = 0x035555 and c1 = 0x00;").discard_result();
        }).then([&e] {
            return e.execute_cql("update cf set v = 0x04 where k = 0x00 and c0 = 0x05 and c1 = 0x00;").discard_result();
        }).then([&e] {
            return e.execute_cql("select v from cf where k = 0x00 allow filtering;").then([](auto msg) {
                assert_that(msg).is_rows().with_rows({
                    {from_hex("01")}, {from_hex("02")}, {from_hex("03")}, {from_hex("04")}
                });
            });
        });
    });
}

SEASTAR_TEST_CASE(test_query_with_static_columns) {
    return do_with_cql_env([] (auto& e) {
        return e.create_table([](auto ks) {
            // CQL: create table cf (k bytes, c bytes, v bytes, s1 bytes static, primary key (k, c));
            return schema(ks, "cf",
                {{"k", bytes_type}},
                {{"c", bytes_type}},
                {{"v", bytes_type}},
                {{"s1", bytes_type}, {"s2", bytes_type}},
                utf8_type);
        }).then([&e] {
            return e.execute_cql("update cf set s1 = 0x01 where k = 0x00;").discard_result();
        }).then([&e] {
            return e.execute_cql("update cf set v = 0x02 where k = 0x00 and c = 0x01;").discard_result();
        }).then([&e] {
            return e.execute_cql("update cf set v = 0x03 where k = 0x00 and c = 0x02;").discard_result();
        }).then([&e] {
            return e.execute_cql("select s1, v from cf;").then([](auto msg) {
                assert_that(msg).is_rows().with_rows({
                    {from_hex("01"), from_hex("02")},
                    {from_hex("01"), from_hex("03")},
                });
            });
        }).then([&e] {
            return e.execute_cql("select s1 from cf;").then([](auto msg) {
                assert_that(msg).is_rows().with_rows({
                    {from_hex("01")},
                    {from_hex("01")},
                });
            });
        }).then([&e] {
            return e.execute_cql("select s1 from cf limit 1;").then([](auto msg) {
                assert_that(msg).is_rows().with_rows({
                    {from_hex("01")},
                });
            });
        }).then([&e] {
            return e.execute_cql("select s1, v from cf limit 1;").then([](auto msg) {
                assert_that(msg).is_rows().with_rows({
                    {from_hex("01"), from_hex("02")},
                });
            });
        });
    });
}

SEASTAR_TEST_CASE(test_map_insert_update) {
    auto db = make_shared<distributed<database>>();
    auto state = make_shared<conversation_state>(*db, ks_name);

    // CQL: create table cf (p1 varchar primary key, map1 map<int, int>);
    return db->start().then([db] {
        return db->invoke_on_all([] (database& db) {
            keyspace ks;
            auto my_map_type = map_type_impl::get_instance(int32_type, int32_type, true);
            auto cf_schema = make_lw_shared(schema(ks_name, table_name,
                {{"p1", utf8_type}}, {}, {{"map1", my_map_type}}, {}, utf8_type));
            ks.column_families.emplace(table_name, column_family(cf_schema));
            db.keyspaces.emplace(ks_name, std::move(ks));
        });
    }).then([state, db] {
        return state->execute_cql("insert into cf (p1, map1) values ('key1', { 1001: 2001 });").discard_result();
    }).then([state, db] {
        return require_column_has_value(*db, ks_name, table_name, {sstring("key1")}, {},
                "map1", map_type_impl::native_type({{1001, 2001}}));
    }).then([state, db] {
        return state->execute_cql("update cf set map1[1002] = 2002 where p1 = 'key1';").discard_result();
    }).then([state, db] {
        return require_column_has_value(*db, ks_name, table_name, {sstring("key1")}, {},
                "map1", map_type_impl::native_type({{1001, 2001}, {1002, 2002}}));
    }).then([state, db] {
        // overwrite an element
        return state->execute_cql("update cf set map1[1001] = 3001 where p1 = 'key1';").discard_result();
    }).then([state, db] {
        return require_column_has_value(*db, ks_name, table_name, {sstring("key1")}, {},
                "map1", map_type_impl::native_type({{1001, 3001}, {1002, 2002}}));
    }).then([state, db] {
        // overwrite whole map
        return state->execute_cql("update cf set map1 = {1003: 4003} where p1 = 'key1';").discard_result();
    }).then([state, db] {
        return require_column_has_value(*db, ks_name, table_name, {sstring("key1")}, {},
                "map1", map_type_impl::native_type({{1003, 4003}}));
    }).then([state, db] {
        // overwrite whole map, but bad syntax
        return state->execute_cql("update cf set map1 = {1003, 4003} where p1 = 'key1';");
    }).then_wrapped([state, db] (auto f) {
        BOOST_REQUIRE(f.failed());
        std::move(f).discard_result();
    }).then([state, db] {
        // overwrite whole map
        return state->execute_cql("update cf set map1 = {1001: 5001, 1002: 5002, 1003: 5003} where p1 = 'key1';").discard_result();
    }).then([state, db] {
        return require_column_has_value(*db, ks_name, table_name, {sstring("key1")}, {},
                "map1", map_type_impl::native_type({{1001, 5001}, {1002, 5002}, {1003, 5003}}));
    }).then([state, db] {
        // discard some keys
        return state->execute_cql("update cf set map1 = map1 - {1001, 1003, 1005} where p1 = 'key1';").discard_result();
    }).then([state, db] {
        return require_column_has_value(*db, ks_name, table_name, {sstring("key1")}, {},
                "map1", map_type_impl::native_type({{{1002, 5002}}}));
    }).then([db] {
        return db->stop();
    }).then_wrapped([db] (future<> f) mutable {
        return make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(test_set_insert_update) {
    auto db = make_shared<distributed<database>>();
    auto state = make_shared<conversation_state>(*db, ks_name);

    // CQL: create table cf (p1 varchar primary key, set1 set<int>);
    return db->start().then([db] {
        return db->invoke_on_all([] (database& db) {
            keyspace ks;
            auto my_set_type = set_type_impl::get_instance(int32_type, true);
            auto cf_schema = make_lw_shared(schema(ks_name, table_name,
                {{"p1", utf8_type}}, {}, {{"set1", my_set_type}}, {}, utf8_type));
            ks.column_families.emplace(table_name, column_family(cf_schema));
            db.keyspaces.emplace(ks_name, std::move(ks));
        });
    }).then([state, db] {
        return state->execute_cql("insert into cf (p1, set1) values ('key1', { 1001 });").discard_result();
    }).then([state, db] {
        return require_column_has_value(*db, ks_name, table_name, {sstring("key1")}, {},
                "set1", set_type_impl::native_type({1001}));
    }).then([state, db] {
        return state->execute_cql("update cf set set1 = set1 + { 1002 } where p1 = 'key1';").discard_result();
    }).then([state, db] {
        return require_column_has_value(*db, ks_name, table_name, {sstring("key1")}, {},
                "set1", set_type_impl::native_type({1001, 1002}));
    }).then([state, db] {
        // overwrite an element
        return state->execute_cql("update cf set set1 = set1 + { 1001 } where p1 = 'key1';").discard_result();
    }).then([state, db] {
        return require_column_has_value(*db, ks_name, table_name, {sstring("key1")}, {},
                "set1", set_type_impl::native_type({1001, 1002}));
    }).then([state, db] {
        // overwrite entire set
        return state->execute_cql("update cf set set1 = { 1007, 1019 } where p1 = 'key1';").discard_result();
    }).then([state, db] {
        return require_column_has_value(*db, ks_name, table_name, {sstring("key1")}, {},
                "set1", set_type_impl::native_type({1007, 1019}));
    }).then([state, db] {
        // discard keys
        return state->execute_cql("update cf set set1 = set1 - { 1007, 1008 } where p1 = 'key1';").discard_result();
    }).then([state, db] {
        return require_column_has_value(*db, ks_name, table_name, {sstring("key1")}, {},
                "set1", set_type_impl::native_type({1019}));
    }).then([db] {
        return db->stop();
    }).then_wrapped([db] (future<> f) mutable {
        return make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(test_list_insert_update) {
    auto db = make_shared<distributed<database>>();
    auto state = make_shared<conversation_state>(*db, ks_name);

    // CQL: create table cf (p1 varchar primary key, list1 list<int>);
    return db->start().then([db] {
        return db->invoke_on_all([] (database& db) {
            keyspace ks;
            auto my_list_type = list_type_impl::get_instance(int32_type, true);
            auto cf_schema = make_lw_shared(schema(ks_name, table_name,
                {{"p1", utf8_type}}, {}, {{"list1", my_list_type}}, {}, utf8_type));
            ks.column_families.emplace(table_name, column_family(cf_schema));
            db.keyspaces.emplace(ks_name, std::move(ks));
        });
    }).then([state, db] {
        return state->execute_cql("insert into cf (p1, list1) values ('key1', [ 1001 ]);").discard_result();
    }).then([state, db] {
        return require_column_has_value(*db, ks_name, table_name, {sstring("key1")}, {},
                "list1", list_type_impl::native_type({boost::any(1001)}));
    }).then([state, db] {
        return state->execute_cql("update cf set list1 = [ 1002, 1003 ] where p1 = 'key1';").discard_result();
    }).then([state, db] {
        return require_column_has_value(*db, ks_name, table_name, {sstring("key1")}, {},
                "list1", list_type_impl::native_type({boost::any(1002), boost::any(1003)}));
    }).then([state, db] {
        return state->execute_cql("update cf set list1[1] = 2003 where p1 = 'key1';").discard_result();
    }).then([state, db] {
        return require_column_has_value(*db, ks_name, table_name, {sstring("key1")}, {},
                "list1", list_type_impl::native_type({boost::any(1002), boost::any(2003)}));
    }).then([db] {
        return db->stop();
    }).then_wrapped([db] (future<> f) mutable {
        return make_ready_future<>();
    });
}

