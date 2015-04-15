/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#include "core/do_with.hh"
#include "cql_test_env.hh"
#include "cql3/query_processor.hh"
#include "cql3/query_options.hh"
#include "core/distributed.hh"
#include "core/shared_ptr.hh"
#include "utils/UUID_gen.hh"

class in_memory_cql_env : public cql_test_env {
public:
    static auto constexpr ks_name = "ks";
private:
    ::shared_ptr<distributed<database>> _db;
    ::shared_ptr<distributed<cql3::query_processor>> _qp;
    ::shared_ptr<service::storage_proxy> _proxy;
private:
    struct core_local_state {
        service::client_state client_state;

        core_local_state()
            : client_state(service::client_state::for_internal_calls()) {
            client_state.set_keyspace(ks_name);
        }

        future<> stop() {
            return make_ready_future<>();
        }
    };
    distributed<core_local_state> _core_local;
private:
    auto make_query_state() {
        return ::make_shared<service::query_state>(_core_local.local().client_state);
    }
public:
    in_memory_cql_env(
        ::shared_ptr<distributed<database>> db,
        ::shared_ptr<distributed<cql3::query_processor>> qp,
        ::shared_ptr<service::storage_proxy> proxy)
            : _db(db)
            , _qp(qp)
            , _proxy(proxy)
    { }

    virtual future<::shared_ptr<transport::messages::result_message>> execute_cql(const sstring& text) override {
        auto qs = make_query_state();
        return _qp->local().process(text, *qs, cql3::query_options::DEFAULT).finally([qs] {});
    }

    virtual future<::shared_ptr<transport::messages::result_message>> execute_cql(
        const sstring& text,
        std::unique_ptr<cql3::query_options> qo) override
    {
        auto qs = make_query_state();
        auto& lqo = *qo;
        return _qp->local().process(text, *qs, lqo).finally([qs, qo = std::move(qo)] {});
    }

    virtual future<bytes> prepare(sstring query) override {
        return _qp->invoke_on_all([query, this] (auto& local_qp) {
            auto qs = this->make_query_state();
            return local_qp.prepare(query, *qs).finally([qs] {}).discard_result();
        }).then([query, this] {
            return _qp->local().compute_id(query, ks_name);
        });
    }

    virtual future<::shared_ptr<transport::messages::result_message>> execute_prepared(
        bytes id,
        std::vector<bytes_opt> values) override
    {
        auto prepared = _qp->local().get_prepared(id);
        assert(bool(prepared));
        auto stmt = prepared->statement;
        assert(stmt->get_bound_terms() == values.size());

        int32_t protocol_version = 3;
        auto options = ::make_shared<cql3::query_options>(db::consistency_level::ONE, std::experimental::nullopt, std::move(values), false,
                                                          cql3::query_options::specific_options::DEFAULT, protocol_version, serialization_format::use_32_bit());
        options->prepare(prepared->bound_names);

        auto qs = make_query_state();
        return _qp->local().process_statement(stmt, *qs, *options)
            .finally([options, qs] {});
    }

    virtual future<> create_table(std::function<schema(const sstring&)> schema_maker) override {
        return _db->invoke_on_all([schema_maker, this] (database& db) {
            auto cf_schema = make_lw_shared(schema_maker(ks_name));
            auto& ks = db.find_or_create_keyspace(ks_name);
            db.add_column_family(column_family(cf_schema));
            config::ks_meta_data ksm(ks_name,
                                     "org.apache.cassandra.locator.SimpleStrategy",
                                     std::unordered_map<sstring, sstring>(),
                                     false,
                                     std::vector<schema_ptr>(),
                                     shared_ptr<config::ut_meta_data>()
            );
            ks.create_replication_strategy(ksm);
        });
    }

    virtual future<> require_column_has_value(const sstring& table_name,
                                      std::vector<boost::any> pk,
                                      std::vector<boost::any> ck,
                                      const sstring& column_name,
                                      boost::any expected) override {
        auto& db = _db->local();
        auto& cf = db.find_column_family(ks_name, table_name);
        auto schema = cf._schema;
        auto pkey = partition_key::from_deeply_exploded(*schema, pk);
        auto dk = dht::global_partitioner().decorate_key(pkey);
        auto shard = db.shard_of(dk._token);
        return _db->invoke_on(shard, [pkey = std::move(pkey),
                                      ck = std::move(ck),
                                      ks_name = std::move(ks_name),
                                      column_name = std::move(column_name),
                                      expected = std::move(expected),
                                      table_name = std::move(table_name)] (database& db) {
            auto& cf = db.find_column_family(ks_name, table_name);
            auto schema = cf._schema;
            auto p = cf.find_partition(pkey);
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
        });
    }

    future<> start() {
        return _core_local.start();
    }

    virtual future<> stop() override {
        return _core_local.stop().then([this] {
            return _qp->stop().then([this] {
                return _db->stop();
            });
        });
    }
};

future<::shared_ptr<cql_test_env>> make_env_for_test() {
    auto db = ::make_shared<distributed<database>>();
    return db->start().then([db] {
        auto proxy = ::make_shared<service::storage_proxy>(std::ref(*db));
        auto qp = ::make_shared<distributed<cql3::query_processor>>();
        return qp->start(std::ref(*proxy), std::ref(*db)).then([db, proxy, qp] {
            auto env = ::make_shared<in_memory_cql_env>(db, qp, proxy);
            return env->start().then([env] () -> ::shared_ptr<cql_test_env> {
                return env;
            });
        });
    });
}

future<> do_with_cql_env(std::function<future<>(cql_test_env&)> func) {
    return make_env_for_test().then([func = std::move(func)] (auto e) mutable {
        return do_with(std::move(func), [e] (auto& f) {
            return f(*e);
        }).finally([e] {
            return e->stop().finally([e] {});
        });
    });
}
