/*
 * Copyright 2015 Cloudius Systems
 */

#include <boost/test/included/unit_test.hpp>
#include "cql3/query_processor.hh"
#include "cql3/query_options.hh"
#include "tests/test-utils.hh"
#include "core/future-util.hh"

struct conversation_state {
    service::storage_proxy proxy;
    cql3::query_processor qp;
    service::client_state client_state;
    service::query_state query_state;
    cql3::query_options& options;

    conversation_state(database& db, const sstring& ks_name)
        : proxy(db)
        , qp(proxy, db)
        , client_state(service::client_state::for_internal_calls())
        , query_state(client_state)
        , options(cql3::query_options::DEFAULT)
    {
        client_state.set_keyspace(ks_name);
    }

    future<> execute_cql(const sstring& text) {
        return qp.process(text, query_state, options).discard_result();
    }
};

static const sstring ks_name = "ks";
static const sstring table_name = "cf";

static void require_column_has_value(database& db, const sstring& ks_name, const sstring& table_name,
    std::vector<boost::any> pk, std::vector<boost::any> ck, const sstring& column_name, boost::any expected)
{
    auto ks = db.find_keyspace(ks_name);
    BOOST_REQUIRE(ks != nullptr);
    auto cf = ks->find_column_family(table_name);
    BOOST_REQUIRE(cf != nullptr);
    auto schema = cf->_schema;
    auto p = cf->find_partition(schema->partition_key_type->serialize_value_deep(pk));
    BOOST_REQUIRE(p != nullptr);
    auto row = p->find_row(schema->clustering_key_type->serialize_value_deep(ck));
    BOOST_REQUIRE(row != nullptr);
    auto col_def = schema->get_column_definition(utf8_type->decompose(column_name));
    BOOST_REQUIRE(col_def != nullptr);
    auto i = row->find(col_def->id);
    if (i == row->end()) {
        BOOST_FAIL("column not set");
    }
    auto& cell = boost::any_cast<const atomic_cell&>(i->second);
    BOOST_REQUIRE(cell.is_live());
    BOOST_REQUIRE(col_def->type->equal(cell.as_live().value, col_def->type->decompose(expected)));
}

SEASTAR_TEST_CASE(test_insert_statement) {
    auto db = make_shared<database>();

    // CQL: create table cf (p1 varchar, c1 int, r1 int, PRIMARY KEY (p1, c1));
    keyspace ks;
    auto cf_schema = make_lw_shared<schema>(ks_name, table_name,
        std::vector<schema::column>({{"p1", utf8_type}}),
        std::vector<schema::column>({{"c1", int32_type}}),
        std::vector<schema::column>({{"r1", int32_type}}),
        utf8_type
    );
    ks.column_families.emplace(table_name, column_family(cf_schema));
    db->keyspaces.emplace(ks_name, std::move(ks));

    auto state = make_shared<conversation_state>(*db, ks_name);

    return now().then([state, db] {
            return state->execute_cql("insert into cf (p1, c1, r1) values ('key1', 1, 100);");
        }).then([state, db] {
            require_column_has_value(*db, ks_name, table_name, {sstring("key1")}, {1}, "r1", 100);
        }).then([state, db] {
            return state->execute_cql("update cf set r1 = 66 where p1 = 'key1' and c1 = 1;");
        }).then([state, db] {
            require_column_has_value(*db, ks_name, table_name, {sstring("key1")}, {1}, "r1", 66);
        });
}
