/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#include <seastar/core/thread.hh>
#include "core/do_with.hh"
#include "cql_test_env.hh"
#include "cql3/query_processor.hh"
#include "cql3/query_options.hh"
#include "core/distributed.hh"
#include "core/shared_ptr.hh"
#include "utils/UUID_gen.hh"
#include "service/migration_manager.hh"
#include "message/messaging_service.hh"
#include "service/storage_service.hh"
#include "db/config.hh"
#include "schema_builder.hh"
#include "init.hh"

class in_memory_cql_env : public cql_test_env {
public:
    static auto constexpr ks_name = "ks";
private:
    ::shared_ptr<distributed<database>> _db;
    ::shared_ptr<distributed<cql3::query_processor>> _qp;
private:
    struct core_local_state {
        service::client_state client_state;

        core_local_state()
            : client_state(service::client_state::for_external_calls()) {
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
        ::shared_ptr<distributed<cql3::query_processor>> qp)
            : _db(db)
            , _qp(qp)
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
        auto id = utils::UUID_gen::get_time_UUID();
        return _db->invoke_on_all([schema_maker, id, this] (database& db) {
            schema_builder builder(make_lw_shared(schema_maker(ks_name)));
            builder.set_uuid(id);
            auto cf_schema = builder.build();
            auto& ks = db.find_keyspace(ks_name);
            auto cfg = ks.make_column_family_config(*cf_schema);
            db.add_column_family(std::move(cf_schema), std::move(cfg));
        });
    }

    virtual future<> require_keyspace_exists(const sstring& ks_name) override {
        auto& db = _db->local();
        assert(db.has_keyspace(ks_name));
        return make_ready_future<>();
    }

    virtual future<> require_table_exists(const sstring& ks_name, const sstring& table_name) override {
        auto& db = _db->local();
        assert(db.has_schema(ks_name, table_name));
        return make_ready_future<>();
    }

    virtual future<> require_column_has_value(const sstring& table_name,
                                      std::vector<boost::any> pk,
                                      std::vector<boost::any> ck,
                                      const sstring& column_name,
                                      boost::any expected) override {
        auto& db = _db->local();
        auto& cf = db.find_column_family(ks_name, table_name);
        auto schema = cf.schema();
        auto pkey = partition_key::from_deeply_exploded(*schema, pk);
        auto dk = dht::global_partitioner().decorate_key(*schema, pkey);
        auto shard = db.shard_of(dk._token);
        return _db->invoke_on(shard, [pkey = std::move(pkey),
                                      ck = std::move(ck),
                                      ks_name = std::move(ks_name),
                                      column_name = std::move(column_name),
                                      expected = std::move(expected),
                                      table_name = std::move(table_name)] (database& db) mutable {
          auto& cf = db.find_column_family(ks_name, table_name);
          auto schema = cf.schema();
          return cf.find_partition_slow(pkey).then([schema, ck, column_name, expected] (column_family::const_mutation_partition_ptr p) {
            assert(p != nullptr);
            auto row = p->find_row(clustering_key::from_deeply_exploded(*schema, ck));
            assert(row != nullptr);
            auto col_def = schema->get_column_definition(utf8_type->decompose(column_name));
            assert(col_def != nullptr);
            const atomic_cell_or_collection* cell = row->find_cell(col_def->id);
            if (!cell) {
                assert(((void)"column not set", 0));
            }
            bytes actual;
            if (!col_def->type->is_multi_cell()) {
                auto c = cell->as_atomic_cell();
                assert(c.is_live());
                actual = { c.value().begin(), c.value().end() };
            } else {
                auto c = cell->as_collection_mutation();
                auto type = dynamic_pointer_cast<const collection_type_impl>(col_def->type);
                actual = type->to_value(type->deserialize_mutation_form(c),
                                        serialization_format::internal());
            }
            assert(col_def->type->equal(actual, col_def->type->decompose(expected)));
          });
        });
    }

    virtual database& local_db() override {
        return _db->local();
    }

    cql3::query_processor& local_qp() override {
        return _qp->local();
    }

    future<> start() {
        return _core_local.start().then([this] () {
            auto query = sprint("create keyspace %s with replication = { 'class' : 'org.apache.cassandra.locator.SimpleStrategy', 'replication_factor' : 1 };", sstring{ks_name});
            return execute_cql(query).discard_result().then([] {
                return make_ready_future<>();
            });
        });
    }

    virtual future<> stop() override {
        return _core_local.stop().then([this] {
            return _qp->stop().then([this] {
                return service::get_migration_manager().stop().then([this] {
                    return service::get_storage_proxy().stop().then([this] {
                        return _db->stop().then([] {
                            return locator::i_endpoint_snitch::stop_snitch();
                        });
                    });
                });
            });
        });
    }
};

future<> init_once(distributed<database>& db) {
    static bool done = false;
    if (!done) {
        done = true;
        return init_storage_service(db).then([] {
            return init_ms_fd_gossiper("127.0.0.1", db::config::seed_provider_type());
        });
    } else {
        return make_ready_future();
    }
}

future<::shared_ptr<cql_test_env>> make_env_for_test() {
    return locator::i_endpoint_snitch::create_snitch("SimpleSnitch").then([] {
        auto db = ::make_shared<distributed<database>>();
        return init_once(*db).then([db] {
            return seastar::async([db] {
                auto cfg = make_lw_shared<db::config>();
                cfg->data_file_directories() = {"."};
                db->start(std::move(*cfg)).get();

                distributed<service::storage_proxy>& proxy = service::get_storage_proxy();
                distributed<service::migration_manager>& mm = service::get_migration_manager();
                auto qp = ::make_shared<distributed<cql3::query_processor>>();
                proxy.start(std::ref(*db)).get();
                mm.start().get();
                qp->start(std::ref(proxy), std::ref(*db)).get();

                auto& ss = service::get_local_storage_service();
                ss.init_server().get();

                auto env = ::make_shared<in_memory_cql_env>(db, qp);
                env->start().get();
                return dynamic_pointer_cast<cql_test_env>(env);
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
