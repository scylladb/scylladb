/*
 * Copyright 2015 Cloudius Systems
 */

#include "cql3/query_processor.hh"
#include "cql3/query_options.hh"
#include "core/distributed.hh"
#include "core/app-template.hh"

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

    future<> execute_cql(const sstring& text) {
        return qp.process(text, query_state, options).discard_result();
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
        auto& cell = boost::any_cast<const atomic_cell&>(i->second);
        assert(cell.is_live());
        assert(col_def->type->equal(cell.as_live().value, col_def->type->decompose(expected)));
    });
}

future<> test_insert_statement() {
    auto db = make_shared<distributed<database>>();
    auto state = make_shared<conversation_state>(*db, ks_name);

    // CQL: create table cf (p1 varchar, c1 int, r1 int, PRIMARY KEY (p1, c1));
    return db->start().then([db] {
        return db->invoke_on_all([] (database& db) {
            keyspace ks;
            auto cf_schema = make_lw_shared(schema(ks_name, table_name,
                {{"p1", utf8_type}}, {{"c1", int32_type}}, {{"r1", int32_type}}, utf8_type));
            ks.column_families.emplace(table_name, column_family(cf_schema));
            db.keyspaces.emplace(ks_name, std::move(ks));
        });
    }).then([state, db] {
            return state->execute_cql("insert into cf (p1, c1, r1) values ('key1', 1, 100);");
        }).then([state, db] {
            return require_column_has_value(*db, ks_name, table_name, {sstring("key1")}, {1}, "r1", 100);
        }).then([state, db] {
            return state->execute_cql("update cf set r1 = 66 where p1 = 'key1' and c1 = 1;");
        }).then([state, db] {
            return require_column_has_value(*db, ks_name, table_name, {sstring("key1")}, {1}, "r1", 66);
        }).then([db] {
        return db->stop();
    }).then([db] {
        // keep db alive until stop(), above
        engine().exit(0);
    });
}

int main(int ac, char** av) {
    return app_template().run(ac, av, test_insert_statement);
}
