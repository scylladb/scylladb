/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
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
#include "db/batchlog_manager.hh"
#include "schema_builder.hh"
#include "tmpdir.hh"

// TODO: remove (#293)
#include "message/messaging_service.hh"
#include "gms/failure_detector.hh"
#include "gms/gossiper.hh"
#include "service/storage_service.hh"

// TODO : remove once shutdown is ok.
// Broke these test when doing horror patch for #293
// Simpler to copy the code from init.cc than trying to do clever parameterization
// and whatnot.
static future<> tst_init_storage_service(distributed<database>& db) {
    return service::init_storage_service(db).then([] {
        engine().at_exit([] { return service::deinit_storage_service(); });
    });
}

static future<> tst_init_ms_fd_gossiper(sstring listen_address, db::seed_provider_type seed_provider, sstring cluster_name = "Test Cluster") {
    const gms::inet_address listen(listen_address);
    // Init messaging_service
    return net::get_messaging_service().start(listen).then([]{
        engine().at_exit([] { return net::get_messaging_service().stop(); });
    }).then([] {
        // Init failure_detector
        return gms::get_failure_detector().start().then([] {
            engine().at_exit([]{ return gms::get_failure_detector().stop(); });
        });
    }).then([listen_address, seed_provider, cluster_name] {
        // Init gossiper
        std::set<gms::inet_address> seeds;
        if (seed_provider.parameters.count("seeds") > 0) {
            size_t begin = 0;
            size_t next = 0;
            sstring seeds_str = seed_provider.parameters.find("seeds")->second;
            while (begin < seeds_str.length() && begin != (next=seeds_str.find(",",begin))) {
                seeds.emplace(gms::inet_address(seeds_str.substr(begin,next-begin)));
                begin = next+1;
            }
        }
        if (seeds.empty()) {
            seeds.emplace(gms::inet_address("127.0.0.1"));
        }
        return gms::get_gossiper().start().then([seeds, cluster_name] {
            auto& gossiper = gms::get_local_gossiper();
            gossiper.set_seeds(seeds);
            gossiper.set_cluster_name(cluster_name);
            engine().at_exit([]{ return gms::get_gossiper().stop(); });
        });
    });
}
// END TODO

future<> init_once(shared_ptr<distributed<database>> db) {
    static bool done = false;
    if (!done) {
        done = true;
        // FIXME: we leak db, since we're initializing the global storage_service with it.
        new shared_ptr<distributed<database>>(db);
        return tst_init_storage_service(*db).then([] {
            return tst_init_ms_fd_gossiper("127.0.0.1", db::config::seed_provider_type());
        }).then([] {
            return db::system_keyspace::init_local_cache();
        });
    } else {
        return make_ready_future();
    }
}

class single_node_cql_env : public cql_test_env {
public:
    static auto constexpr ks_name = "ks";
private:
    ::shared_ptr<distributed<database>> _db;
    ::shared_ptr<distributed<cql3::query_processor>> _qp;
    lw_shared_ptr<tmpdir> _data_dir;
private:
    struct core_local_state {
        service::client_state client_state;

        core_local_state()
            : client_state(service::client_state::for_external_calls()) {
        }

        future<> stop() {
            return make_ready_future<>();
        }
    };
    distributed<core_local_state> _core_local;
private:
    auto make_query_state() {
        try {
            _core_local.local().client_state.set_keyspace(*_db, ks_name);
        } catch (exceptions::invalid_request_exception&) { }
        return ::make_shared<service::query_state>(_core_local.local().client_state);
    }
public:
    single_node_cql_env()
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

        auto options = ::make_shared<cql3::query_options>(std::move(values));
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
            auto cf_schema = builder.build(schema_builder::compact_storage::no);
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
        return seastar::async([this] {
            locator::i_endpoint_snitch::create_snitch("SimpleSnitch").get();
            auto db = ::make_shared<distributed<database>>();
            init_once(db).get();
            auto cfg = make_lw_shared<db::config>();
            _data_dir = make_lw_shared<tmpdir>();
            cfg->data_file_directories() = { _data_dir->path };
            boost::filesystem::create_directories((_data_dir->path + "/system").c_str());
            db->start(std::move(*cfg)).get();

            distributed<service::storage_proxy>& proxy = service::get_storage_proxy();
            distributed<service::migration_manager>& mm = service::get_migration_manager();
            distributed<db::batchlog_manager>& bm = db::get_batchlog_manager();

            auto qp = ::make_shared<distributed<cql3::query_processor>>();
            proxy.start(std::ref(*db)).get();
            mm.start().get();
            qp->start(std::ref(proxy), std::ref(*db)).get();

            auto& ss = service::get_local_storage_service();
            static bool storage_service_started = false;
            if (!storage_service_started) {
                storage_service_started = true;
                ss.init_server().get();
            }

            bm.start(std::ref(*qp)).get();

            _core_local.start().get();
            _db = std::move(db);
            _qp = std::move(qp);

            auto query = sprint("create keyspace %s with replication = { 'class' : 'org.apache.cassandra.locator.SimpleStrategy', 'replication_factor' : 1 };", sstring{ks_name});
            execute_cql(query).get();
        });
    }

    virtual future<> stop() override {
        return _core_local.stop().then([this] {
            return db::get_batchlog_manager().stop().then([this] {
                return _qp->stop().then([this] {
                    return service::get_migration_manager().stop().then([this] {
                        return service::get_storage_proxy().stop().then([this] {
                            return _db->stop().then([this] {
                                return locator::i_endpoint_snitch::stop_snitch();
                            });
                        });
                    });
                });
            });
        });
    }
};

future<::shared_ptr<cql_test_env>> make_env_for_test() {
    return seastar::async([] {
        auto env = ::make_shared<single_node_cql_env>();
        env->start().get();
        return dynamic_pointer_cast<cql_test_env>(env);
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
