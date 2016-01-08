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
#include "db/query_context.hh"

// TODO: remove (#293)
#include "message/messaging_service.hh"
#include "gms/failure_detector.hh"
#include "gms/gossiper.hh"
#include "service/storage_service.hh"
#include "service/pending_range_calculator_service.hh"

// TODO : remove once shutdown is ok.
// Broke these test when doing horror patch for #293
// Simpler to copy the code from init.cc than trying to do clever parameterization
// and whatnot.
static future<> tst_init_storage_service(distributed<database>& db) {
    return service::get_pending_range_calculator_service().start(std::ref(db)).then([] {
        engine().at_exit([] { return service::get_pending_range_calculator_service().stop(); });
    }).then([&db] {
        return service::init_storage_service(db).then([] {
            engine().at_exit([] { return service::deinit_storage_service(); });
        });
    });
}

static future<> tst_init_ms_fd_gossiper(sstring listen_address, uint16_t port, db::seed_provider_type seed_provider, sstring cluster_name = "Test Cluster") {
    const gms::inet_address listen(listen_address);
    // Init messaging_service
    return net::get_messaging_service().start(listen, std::move(port)).then([]{
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

class single_node_cql_env : public cql_test_env {
public:
    static auto constexpr ks_name = "ks";
    static std::atomic<bool> active;
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
        if (_db->local().has_keyspace(ks_name)) {
            _core_local.local().client_state.set_keyspace(*_db, ks_name);
        }
        return ::make_shared<service::query_state>(_core_local.local().client_state);
    }
public:
    single_node_cql_env()
    { }

    virtual future<::shared_ptr<transport::messages::result_message>> execute_cql(const sstring& text) override {
        auto qs = make_query_state();
        return _qp->local().process(text, *qs, cql3::query_options::DEFAULT).finally([qs, this] {
            _core_local.local().client_state.merge(qs->get_client_state());
        });
    }

    virtual future<::shared_ptr<transport::messages::result_message>> execute_cql(
        const sstring& text,
        std::unique_ptr<cql3::query_options> qo) override
    {
        auto qs = make_query_state();
        auto& lqo = *qo;
        return _qp->local().process(text, *qs, lqo).finally([qs, qo = std::move(qo), this] {
            _core_local.local().client_state.merge(qs->get_client_state());
        });
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
        if (!prepared) {
            throw not_prepared_exception(id);
        }
        auto stmt = prepared->statement;
        assert(stmt->get_bound_terms() == values.size());

        auto options = ::make_shared<cql3::query_options>(std::move(values));
        options->prepare(prepared->bound_names);

        auto qs = make_query_state();
        return _qp->local().process_statement(stmt, *qs, *options)
            .finally([options, qs, this] {
                _core_local.local().client_state.merge(qs->get_client_state());
            });
    }

    virtual future<> create_table(std::function<schema(const sstring&)> schema_maker) override {
        auto id = utils::UUID_gen::get_time_UUID();
        schema_builder builder(make_lw_shared(schema_maker(ks_name)));
        builder.set_uuid(id);
        auto s = builder.build(schema_builder::compact_storage::no);
        return service::get_local_migration_manager().announce_new_column_family(s, true);
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
                                      std::vector<data_value> pk,
                                      std::vector<data_value> ck,
                                      const sstring& column_name,
                                      data_value expected) override {
        auto& db = _db->local();
        auto& cf = db.find_column_family(ks_name, table_name);
        auto schema = cf.schema();
        auto pkey = partition_key::from_deeply_exploded(*schema, pk);
        auto ckey = clustering_key::from_deeply_exploded(*schema, ck);
        auto exp = expected.type()->decompose(expected);
        auto dk = dht::global_partitioner().decorate_key(*schema, pkey);
        auto shard = db.shard_of(dk._token);
        return _db->invoke_on(shard, [pkey = std::move(pkey),
                                      ckey = std::move(ckey),
                                      ks_name = std::move(ks_name),
                                      column_name = std::move(column_name),
                                      exp = std::move(exp),
                                      table_name = std::move(table_name)] (database& db) mutable {
          auto& cf = db.find_column_family(ks_name, table_name);
          auto schema = cf.schema();
          return cf.find_partition_slow(schema, pkey)
                  .then([schema, ckey, column_name, exp] (column_family::const_mutation_partition_ptr p) {
            assert(p != nullptr);
            auto row = p->find_row(ckey);
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
            assert(col_def->type->equal(actual, exp));
          });
        });
    }

    virtual database& local_db() override {
        return _db->local();
    }

    cql3::query_processor& local_qp() override {
        return _qp->local();
    }

    distributed<database>& db() override {
        return *_db;
    }

    distributed<cql3::query_processor>& qp() override {
        return *_qp;
    }

    future<> start() {
        bool old_active = false;
        if (!active.compare_exchange_strong(old_active, true)) {
            throw std::runtime_error("Starting more than one cql_test_env at a time not supported "
                                     "due to singletons.");
        }
        return seastar::async([this] {
            utils::fb_utilities::set_broadcast_address(gms::inet_address("localhost"));
            utils::fb_utilities::set_broadcast_rpc_address(gms::inet_address("localhost"));
            locator::i_endpoint_snitch::create_snitch("SimpleSnitch").get();
            auto db = ::make_shared<distributed<database>>();
            auto cfg = make_lw_shared<db::config>();
            _data_dir = make_lw_shared<tmpdir>();
            cfg->data_file_directories() = { _data_dir->path };
            cfg->commitlog_directory() = _data_dir->path + "/commitlog.dir";
            cfg->num_tokens() = 256;
            cfg->ring_delay_ms() = 500;
            boost::filesystem::create_directories((_data_dir->path + "/system").c_str());
            boost::filesystem::create_directories(cfg->commitlog_directory().c_str());
            tst_init_storage_service(*db).get();

            db->start(std::move(*cfg)).get();

            tst_init_ms_fd_gossiper("127.0.0.1", 7000, db::config::seed_provider_type()).get();

            distributed<service::storage_proxy>& proxy = service::get_storage_proxy();
            distributed<service::migration_manager>& mm = service::get_migration_manager();
            distributed<db::batchlog_manager>& bm = db::get_batchlog_manager();

            proxy.start(std::ref(*db)).get();
            mm.start().get();

            auto qp = ::make_shared<distributed<cql3::query_processor>>();
            qp->start(std::ref(proxy), std::ref(*db)).get();

            bm.start(std::ref(*qp)).get();

            db->invoke_on_all([this] (database& db) {
                return db.init_system_keyspace();
            }).get();

            auto& ks = db->local().find_keyspace(db::system_keyspace::NAME);
            parallel_for_each(ks.metadata()->cf_meta_data(), [&ks] (auto& pair) {
                auto cfm = pair.second;
                return ks.make_directory_for_column_family(cfm->cf_name(), cfm->id());
            }).get();

            // In main.cc we call db::system_keyspace::setup which calls
            // minimal_setup and init_local_cache
            db::system_keyspace::minimal_setup(*db, *qp);
            db::system_keyspace::init_local_cache().get();

            service::get_local_storage_service().init_server().get();

            _core_local.start().get();
            _db = std::move(db);
            _qp = std::move(qp);

            auto query = sprint("create keyspace %s with replication = { 'class' : 'org.apache.cassandra.locator.SimpleStrategy', 'replication_factor' : 1 };", sstring{ks_name});
            execute_cql(query).get();
        });
    }

    virtual future<> stop() override {
        return seastar::async([this] {
            _core_local.stop().get();
            db::system_keyspace::deinit_local_cache().get();

            db::get_batchlog_manager().stop().get();
            _qp->stop().get();
            db::qctx = {};
            service::get_migration_manager().stop().get();
            service::get_storage_proxy().stop().get();

            gms::get_gossiper().stop().get();
            gms::get_failure_detector().stop().get();
            net::get_messaging_service().stop().get();

            _db->stop().get();

            service::get_storage_service().stop().get();
            service::get_pending_range_calculator_service().stop().get();

            locator::i_endpoint_snitch::stop_snitch().get();
            bool old_active = true;
            assert(active.compare_exchange_strong(old_active, false));
        });
    }
};

std::atomic<bool> single_node_cql_env::active = { false };

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
