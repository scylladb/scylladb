/*
 * Copyright 2015 Cloudius Systems
 */

#include "cql3/query_processor.hh"
#include "cql3/query_options.hh"
#include "core/distributed.hh"
#include "tests/test-utils.hh"

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
    auto pkey = schema->partition_key_type->serialize_value_deep(pk);
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
        auto row = p->find_row(schema->clustering_key_type->serialize_value_deep(ck));
        assert(row != nullptr);
        auto col_def = schema->get_column_definition(utf8_type->decompose(column_name));
        assert(col_def != nullptr);
        auto i = row->find(col_def->id);
        if (i == row->end()) {
            assert(((void)"column not set", 0));
        }
        auto cell = i->second.as_atomic_cell();
        assert(cell.is_live());
        assert(col_def->type->equal(cell.value(), col_def->type->decompose(expected)));
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
