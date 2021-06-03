/*
 * Copyright (C) 2015-present ScyllaDB
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

#include <boost/range/algorithm/transform.hpp>
#include <iterator>
#include <seastar/core/thread.hh>
#include <seastar/util/defer.hh>
#include "sstables/sstables.hh"
#include <seastar/core/do_with.hh>
#include "test/lib/cql_test_env.hh"
#include "cdc/generation_service.hh"
#include "cql3/functions/functions.hh"
#include "cql3/query_processor.hh"
#include "cql3/query_options.hh"
#include "cql3/statements/batch_statement.hh"
#include "cql3/cql_config.hh"
#include <seastar/core/distributed.hh>
#include <seastar/core/abort_source.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/coroutine.hh>
#include "utils/UUID_gen.hh"
#include "service/migration_manager.hh"
#include "sstables/compaction_manager.hh"
#include "message/messaging_service.hh"
#include "service/storage_service.hh"
#include "service/storage_proxy.hh"
#include "auth/service.hh"
#include "auth/common.hh"
#include "db/config.hh"
#include "db/batchlog_manager.hh"
#include "schema_builder.hh"
#include "test/lib/tmpdir.hh"
#include "test/lib/reader_permit.hh"
#include "db/query_context.hh"
#include "test/lib/test_services.hh"
#include "test/lib/log.hh"
#include "unit_test_service_levels_accessor.hh"
#include "db/view/view_builder.hh"
#include "db/view/node_view_update_backlog.hh"
#include "distributed_loader.hh"
// TODO: remove (#293)
#include "message/messaging_service.hh"
#include "gms/gossiper.hh"
#include "gms/feature_service.hh"
#include "service/storage_service.hh"
#include "db/system_keyspace.hh"
#include "db/system_distributed_keyspace.hh"
#include "db/sstables-format-selector.hh"
#include "repair/row_level.hh"
#include "debug.hh"

using namespace std::chrono_literals;

namespace {


} // anonymous namespace

future<scheduling_groups> get_scheduling_groups() {
    static std::optional<scheduling_groups> _scheduling_groups;
    if (!_scheduling_groups) {
        _scheduling_groups.emplace();
        _scheduling_groups->compaction_scheduling_group = co_await create_scheduling_group("compaction", 1000);
        _scheduling_groups->memory_compaction_scheduling_group = co_await create_scheduling_group("mem_compaction", 1000);
        _scheduling_groups->streaming_scheduling_group = co_await create_scheduling_group("streaming", 200);
        _scheduling_groups->statement_scheduling_group = co_await create_scheduling_group("statement", 1000);
        _scheduling_groups->memtable_scheduling_group = co_await create_scheduling_group("memtable", 1000);
        _scheduling_groups->memtable_to_cache_scheduling_group = co_await create_scheduling_group("memtable_to_cache", 200);
        _scheduling_groups->gossip_scheduling_group = co_await create_scheduling_group("gossip", 1000);
    }
    co_return *_scheduling_groups;
}

cql_test_config::cql_test_config()
    : cql_test_config(make_shared<db::config>())
{}

cql_test_config::cql_test_config(shared_ptr<db::config> cfg)
    : db_config(cfg)
{
    // This causes huge amounts of commitlog writes to allocate space on disk,
    // which all get thrown away when the test is done. This can cause timeouts
    // if /tmp is not tmpfs.
    db_config->commitlog_use_o_dsync.set(false);

    db_config->add_cdc_extension();
}

cql_test_config::cql_test_config(const cql_test_config&) = default;
cql_test_config::~cql_test_config() = default;

static const sstring testing_superuser = "tester";

static future<> tst_init_ms_fd_gossiper(sharded<gms::feature_service>& features, sharded<locator::shared_token_metadata>& stm, sharded<netw::messaging_service>& ms, db::config& cfg, db::seed_provider_type seed_provider,
            sharded<abort_source>& abort_sources, sstring cluster_name = "Test Cluster") {
        // Init gossiper
        std::set<gms::inet_address> seeds;
        if (seed_provider.parameters.contains("seeds")) {
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
        return gms::get_gossiper().start(std::ref(abort_sources), std::ref(features), std::ref(stm), std::ref(ms), std::ref(cfg)).then([seeds, cluster_name] {
            auto& gossiper = gms::get_local_gossiper();
            gossiper.set_seeds(seeds);
            gossiper.set_cluster_name(cluster_name);
        });
}
// END TODO

class single_node_cql_env : public cql_test_env {
public:
    static constexpr std::string_view ks_name = "ks";
    static std::atomic<bool> active;
private:
    sharded<database>& _db;
    sharded<cql3::query_processor>& _qp;
    sharded<auth::service>& _auth_service;
    sharded<db::view::view_builder>& _view_builder;
    sharded<db::view::view_update_generator>& _view_update_generator;
    sharded<service::migration_notifier>& _mnotifier;
    sharded<qos::service_level_controller>& _sl_controller;
    sharded<service::migration_manager>& _mm;
private:
    struct core_local_state {
        service::client_state client_state;

        core_local_state(auth::service& auth_service, qos::service_level_controller& sl_controller)
            : client_state(service::client_state::external_tag{}, auth_service, &sl_controller, infinite_timeout_config)
        {
            client_state.set_login(auth::authenticated_user(testing_superuser));
        }

        future<> stop() {
            return make_ready_future<>();
        }
    };
    distributed<core_local_state> _core_local;
private:
    auto make_query_state() {
        if (_db.local().has_keyspace(ks_name)) {
            _core_local.local().client_state.set_keyspace(_db.local(), ks_name);
        }
        return ::make_shared<service::query_state>(_core_local.local().client_state, empty_service_permit());
    }
public:
    single_node_cql_env(
            sharded<database>& db,
            sharded<cql3::query_processor>& qp,
            sharded<auth::service>& auth_service,
            sharded<db::view::view_builder>& view_builder,
            sharded<db::view::view_update_generator>& view_update_generator,
            sharded<service::migration_notifier>& mnotifier,
            sharded<service::migration_manager>& mm,
            sharded<qos::service_level_controller> &sl_controller)
            : _db(db)
            , _qp(qp)
            , _auth_service(auth_service)
            , _view_builder(view_builder)
            , _view_update_generator(view_update_generator)
            , _mnotifier(mnotifier)
            , _sl_controller(sl_controller)
            , _mm(mm)
    { }

    virtual future<::shared_ptr<cql_transport::messages::result_message>> execute_cql(sstring_view text) override {
        testlog.trace("{}(\"{}\")", __FUNCTION__, text);
        auto qs = make_query_state();
        return local_qp().execute_direct(text, *qs, cql3::query_options::DEFAULT).finally([qs] {});
    }

    virtual future<::shared_ptr<cql_transport::messages::result_message>> execute_cql(
        sstring_view text,
        std::unique_ptr<cql3::query_options> qo) override
    {
        testlog.trace("{}(\"{}\")", __FUNCTION__, text);
        auto qs = make_query_state();
        auto& lqo = *qo;
        return local_qp().execute_direct(text, *qs, lqo).finally([qs, qo = std::move(qo)] {});
    }

    virtual future<cql3::prepared_cache_key_type> prepare(sstring query) override {
        return qp().invoke_on_all([query, this] (auto& local_qp) {
            auto qs = this->make_query_state();
            return local_qp.prepare(query, *qs).finally([qs] {}).discard_result();
        }).then([query, this] {
            return local_qp().compute_id(query, ks_name);
        });
    }

    virtual future<::shared_ptr<cql_transport::messages::result_message>> execute_prepared(
        cql3::prepared_cache_key_type id,
        std::vector<cql3::raw_value> values,
        db::consistency_level cl = db::consistency_level::ONE) override {

        const auto& so = cql3::query_options::specific_options::DEFAULT;
        auto options = std::make_unique<cql3::query_options>(cl,
                std::move(values), cql3::query_options::specific_options{
                            so.page_size,
                            so.state,
                            db::consistency_level::SERIAL,
                            so.timestamp,
                        });
        return execute_prepared_with_qo(id, std::move(options));
    }

    virtual future<::shared_ptr<cql_transport::messages::result_message>> execute_prepared_with_qo(
        cql3::prepared_cache_key_type id,
        std::unique_ptr<cql3::query_options> qo) override
    {
        auto prepared = local_qp().get_prepared(id);
        if (!prepared) {
            throw not_prepared_exception(id);
        }
        auto stmt = prepared->statement;

        assert(stmt->get_bound_terms() == qo->get_values_count());
        qo->prepare(prepared->bound_names);

        auto qs = make_query_state();
        auto& lqo = *qo;
        return local_qp().execute_prepared(std::move(prepared), std::move(id), *qs, lqo, true)
            .finally([qs, qo = std::move(qo)] {});
    }

    virtual future<std::vector<mutation>> get_modification_mutations(const sstring& text) override {
        auto qs = make_query_state();
        auto cql_stmt = local_qp().get_statement(text, qs->get_client_state())->statement;
        auto modif_stmt = dynamic_pointer_cast<cql3::statements::modification_statement>(std::move(cql_stmt));
        if (!modif_stmt) {
            throw std::runtime_error(format("get_stmt_mutations: not a modification statement: {}", text));
        }
        auto& qo = cql3::query_options::DEFAULT;
        auto timeout = db::timeout_clock::now() + qs->get_client_state().get_timeout_config().write_timeout;

        return modif_stmt->get_mutations(local_qp().proxy(), qo, timeout, false, qo.get_timestamp(*qs), *qs)
            .finally([qs, modif_stmt = std::move(modif_stmt)] {});
    }

    virtual future<> create_table(std::function<schema(std::string_view)> schema_maker) override {
        auto id = utils::UUID_gen::get_time_UUID();
        schema_builder builder(make_lw_shared<schema>(schema_maker(ks_name)));
        builder.set_uuid(id);
        auto s = builder.build(schema_builder::compact_storage::no);
        return _mm.local().announce_new_column_family(s);
    }

    virtual future<> require_keyspace_exists(const sstring& ks_name) override {
        auto& db = _db.local();
        assert(db.has_keyspace(ks_name));
        return make_ready_future<>();
    }

    virtual future<> require_table_exists(const sstring& ks_name, const sstring& table_name) override {
        auto& db = _db.local();
        assert(db.has_schema(ks_name, table_name));
        return make_ready_future<>();
    }

    virtual future<> require_table_exists(std::string_view qualified_name) override {
        auto dot_pos = qualified_name.find_first_of('.');
        assert(dot_pos != std::string_view::npos && dot_pos != 0 && dot_pos != qualified_name.size() - 1);
        assert(_db.local().has_schema(qualified_name.substr(0, dot_pos), qualified_name.substr(dot_pos + 1)));
        return make_ready_future<>();
    }

    virtual future<> require_table_does_not_exist(const sstring& ks_name, const sstring& table_name) override {
        auto& db = _db.local();
        assert(!db.has_schema(ks_name, table_name));
        return make_ready_future<>();
    }

    virtual future<> require_column_has_value(const sstring& table_name,
                                      std::vector<data_value> pk,
                                      std::vector<data_value> ck,
                                      const sstring& column_name,
                                      data_value expected) override {
        auto& db = _db.local();
        auto& cf = db.find_column_family(ks_name, table_name);
        auto schema = cf.schema();
        auto pkey = partition_key::from_deeply_exploded(*schema, pk);
        auto ckey = clustering_key::from_deeply_exploded(*schema, ck);
        auto exp = expected.type()->decompose(expected);
        auto dk = dht::decorate_key(*schema, pkey);
        auto shard = dht::shard_of(*schema, dk._token);
        return _db.invoke_on(shard, [pkey = std::move(pkey),
                                      ckey = std::move(ckey),
                                      ks_name = std::move(ks_name),
                                      column_name = std::move(column_name),
                                      exp = std::move(exp),
                                      table_name = std::move(table_name)] (database& db) mutable {
          auto& cf = db.find_column_family(ks_name, table_name);
          auto schema = cf.schema();
          return cf.find_partition_slow(schema, tests::make_permit(), pkey)
                  .then([schema, ckey, column_name, exp] (column_family::const_mutation_partition_ptr p) {
            assert(p != nullptr);
            auto row = p->find_row(*schema, ckey);
            assert(row != nullptr);
            auto col_def = schema->get_column_definition(utf8_type->decompose(column_name));
            assert(col_def != nullptr);
            const atomic_cell_or_collection* cell = row->find_cell(col_def->id);
            if (!cell) {
                assert(((void)"column not set", 0));
            }
            bytes actual;
            if (!col_def->type->is_multi_cell()) {
                auto c = cell->as_atomic_cell(*col_def);
                assert(c.is_live());
                actual = c.value().linearize();
            } else {
                actual = linearized(serialize_for_cql(*col_def->type,
                        cell->as_collection_mutation(), cql_serialization_format::internal()));
            }
            assert(col_def->type->equal(actual, exp));
          });
        });
    }

    virtual service::client_state& local_client_state() override {
        return _core_local.local().client_state;
    }

    virtual database& local_db() override {
        return _db.local();
    }

    cql3::query_processor& local_qp() override {
        return _qp.local();
    }

    sharded<database>& db() override {
        return _db;
    }

    distributed<cql3::query_processor>& qp() override {
        return _qp;
    }

    auth::service& local_auth_service() override {
        return _auth_service.local();
    }

    virtual db::view::view_builder& local_view_builder() override {
        return _view_builder.local();
    }

    virtual db::view::view_update_generator& local_view_update_generator() override {
        return _view_update_generator.local();
    }

    virtual service::migration_notifier& local_mnotifier() override {
        return _mnotifier.local();
    }

    virtual sharded<service::migration_manager>& migration_manager() override {
        return _mm;
    }

    virtual future<> refresh_client_state() override {
        return _core_local.invoke_on_all([] (core_local_state& state) {
            return state.client_state.maybe_update_per_service_level_params();
        });
    }

    future<> start() {
        return _core_local.start(std::ref(_auth_service), std::ref(_sl_controller));
    }

    future<> stop() {
        return _core_local.stop();
    }

    future<> create_keyspace(std::string_view name) {
        auto query = format("create keyspace {} with replication = {{ 'class' : 'org.apache.cassandra.locator.SimpleStrategy', 'replication_factor' : 1 }};", name);
        return execute_cql(query).discard_result();
    }

    static future<> do_with(std::function<future<>(cql_test_env&)> func, cql_test_config cfg_in) {
        using namespace std::filesystem;

        return seastar::async([cfg_in = std::move(cfg_in), func] {
            // disable reactor stall detection during startup
            auto blocked_reactor_notify_ms = engine().get_blocked_reactor_notify_ms();
            smp::invoke_on_all([] {
                engine().update_blocked_reactor_notify_ms(std::chrono::milliseconds(1000000));
            }).get();

            logalloc::prime_segment_pool(memory::stats().total_memory(), memory::min_free_memory()).get();
            bool old_active = false;
            if (!active.compare_exchange_strong(old_active, true)) {
                throw std::runtime_error("Starting more than one cql_test_env at a time not supported due to singletons.");
            }
            auto deactivate = defer([] {
                bool old_active = true;
                auto success = active.compare_exchange_strong(old_active, false);
                assert(success);
            });

            // FIXME: make the function storage non static
            auto clear_funcs = defer([] {
                smp::invoke_on_all([] () {
                    cql3::functions::functions::clear_functions();
                }).get();
            });

            utils::fb_utilities::set_broadcast_address(gms::inet_address("localhost"));
            utils::fb_utilities::set_broadcast_rpc_address(gms::inet_address("localhost"));
            locator::i_endpoint_snitch::create_snitch("SimpleSnitch").get();
            auto stop_snitch = defer([] { locator::i_endpoint_snitch::stop_snitch().get(); });

            sharded<abort_source> abort_sources;
            abort_sources.start().get();
            auto stop_abort_sources = defer([&] { abort_sources.stop().get(); });
            sharded<database> db;
            debug::db = &db;
            auto reset_db_ptr = defer([] {
                debug::db = nullptr;
            });
            auto cfg = cfg_in.db_config;
            tmpdir data_dir;
            auto data_dir_path = data_dir.path().string();
            if (!cfg->data_file_directories.is_set()) {
                cfg->data_file_directories.set({data_dir_path});
            } else {
                data_dir_path = cfg->data_file_directories()[0];
            }
            cfg->commitlog_directory.set(data_dir_path + "/commitlog.dir");
            cfg->hints_directory.set(data_dir_path + "/hints.dir");
            cfg->view_hints_directory.set(data_dir_path + "/view_hints.dir");
            cfg->num_tokens.set(256);
            cfg->ring_delay_ms.set(500);
            cfg->shutdown_announce_in_ms.set(0);
            cfg->broadcast_to_all_shards().get();
            create_directories((data_dir_path + "/system").c_str());
            create_directories(cfg->commitlog_directory().c_str());
            create_directories(cfg->hints_directory().c_str());
            create_directories(cfg->view_hints_directory().c_str());
            for (unsigned i = 0; i < smp::count; ++i) {
                create_directories((cfg->hints_directory() + "/" + std::to_string(i)).c_str());
                create_directories((cfg->view_hints_directory() + "/" + std::to_string(i)).c_str());
            }

            if (!cfg->max_memory_for_unlimited_query_soft_limit.is_set()) {
                cfg->max_memory_for_unlimited_query_soft_limit.set(uint64_t(query::result_memory_limiter::unlimited_result_size));
            }
            if (!cfg->max_memory_for_unlimited_query_hard_limit.is_set()) {
                cfg->max_memory_for_unlimited_query_hard_limit.set(uint64_t(query::result_memory_limiter::unlimited_result_size));
            }

            sharded<locator::shared_token_metadata> token_metadata;
            token_metadata.start().get();
            auto stop_token_metadata = defer([&token_metadata] { token_metadata.stop().get(); });

            sharded<service::migration_notifier> mm_notif;
            mm_notif.start().get();
            auto stop_mm_notify = defer([&mm_notif] { mm_notif.stop().get(); });

            sharded<auth::service> auth_service;

            set_abort_on_internal_error(true);
            const gms::inet_address listen("127.0.0.1");
            auto sys_dist_ks = seastar::sharded<db::system_distributed_keyspace>();
            auto sl_controller = sharded<qos::service_level_controller>();
            sl_controller.start(std::ref(auth_service), qos::service_level_options{}).get();
            auto stop_sl_controller = defer([&sl_controller] { sl_controller.stop().get(); });
            sl_controller.invoke_on_all(&qos::service_level_controller::start).get();
            sl_controller.invoke_on_all([&sys_dist_ks, &sl_controller] (qos::service_level_controller& service) {
                qos::service_level_controller::service_level_distributed_data_accessor_ptr service_level_data_accessor =
                        ::static_pointer_cast<qos::service_level_controller::service_level_distributed_data_accessor>(
                                make_shared<qos::unit_test_service_levels_accessor>(sl_controller,sys_dist_ks));
                return service.set_distributed_data_accessor(std::move(service_level_data_accessor));
            }).get();

            sharded<netw::messaging_service> ms;
            // don't start listening so tests can be run in parallel
            ms.start(listen, std::move(7000)).get();
            auto stop_ms = defer([&ms] { ms.stop().get(); });

            // Normally the auth server is already stopped in here,
            // but if there is an initialization failure we have to
            // make sure to stop it now or ~sharded will assert.
            auto stop_auth_server = defer([&auth_service] {
                auth_service.stop().get();
            });

            auto stop_sys_dist_ks = defer([&sys_dist_ks] { sys_dist_ks.stop().get(); });

            gms::feature_config fcfg = gms::feature_config_from_db_config(*cfg, cfg_in.disabled_features);
            sharded<gms::feature_service> feature_service;
            feature_service.start(fcfg).get();
            auto stop_feature_service = defer([&] { feature_service.stop().get(); });

            // FIXME: split
            tst_init_ms_fd_gossiper(feature_service, token_metadata, ms, *cfg, db::config::seed_provider_type(), abort_sources).get();

            distributed<service::storage_proxy>& proxy = service::get_storage_proxy();
            distributed<service::migration_manager> mm;
            distributed<db::batchlog_manager>& bm = db::get_batchlog_manager();
            sharded<cql3::cql_config> cql_config;
            cql_config.start().get();
            auto stop_cql_config = defer([&] { cql_config.stop().get(); });

            sharded<db::view::view_update_generator> view_update_generator;
            sharded<cdc::generation_service> cdc_generation_service;
            sharded<repair_service> repair;

            auto& ss = service::get_storage_service();
            service::storage_service_config sscfg;
            sscfg.available_memory = memory::stats().total_memory();
            ss.start(std::ref(abort_sources), std::ref(db), std::ref(gms::get_gossiper()), std::ref(sys_dist_ks), std::ref(view_update_generator), std::ref(feature_service), sscfg, std::ref(mm), std::ref(token_metadata), std::ref(ms), std::ref(cdc_generation_service), std::ref(repair), true).get();
            auto stop_storage_service = defer([&ss] { ss.stop().get(); });

            sharded<semaphore> sst_dir_semaphore;
            sst_dir_semaphore.start(cfg->initial_sstable_loading_concurrency()).get();
            auto stop_sst_dir_sem = defer([&sst_dir_semaphore] {
                sst_dir_semaphore.stop().get();
            });

            database_config dbcfg;
            if (cfg_in.dbcfg) {
                dbcfg = std::move(*cfg_in.dbcfg);
            } else {
                dbcfg.available_memory = memory::stats().total_memory();
            }

            auto scheduling_groups = get_scheduling_groups().get();
            dbcfg.compaction_scheduling_group = scheduling_groups.compaction_scheduling_group;
            dbcfg.memory_compaction_scheduling_group = scheduling_groups.memory_compaction_scheduling_group;
            dbcfg.streaming_scheduling_group = scheduling_groups.streaming_scheduling_group;
            dbcfg.statement_scheduling_group = scheduling_groups.statement_scheduling_group;
            dbcfg.memtable_scheduling_group = scheduling_groups.memtable_scheduling_group;
            dbcfg.memtable_to_cache_scheduling_group = scheduling_groups.memtable_to_cache_scheduling_group;
            dbcfg.gossip_scheduling_group = scheduling_groups.gossip_scheduling_group;

            db.start(std::ref(*cfg), dbcfg, std::ref(mm_notif), std::ref(feature_service), std::ref(token_metadata), std::ref(abort_sources), std::ref(sst_dir_semaphore)).get();
            auto stop_db = defer([&db] {
                db.stop().get();
            });

            db.invoke_on_all([] (database& db) {
                db.set_format_by_config();
            }).get();

            auto stop_ms_fd_gossiper = defer([] {
                gms::get_gossiper().stop().get();
            });

            ss.invoke_on_all([] (auto&& ss) {
                ss.enable_all_features();
            }).get();

            smp::invoke_on_all([blocked_reactor_notify_ms] {
                engine().update_blocked_reactor_notify_ms(blocked_reactor_notify_ms);
            }).get();

            service::storage_proxy::config spcfg {
                .hints_directory_initializer = db::hints::directory_initializer::make_dummy(),
            };
            spcfg.available_memory = memory::stats().total_memory();
            db::view::node_update_backlog b(smp::count, 10ms);
            scheduling_group_key_config sg_conf =
                    make_scheduling_group_key_config<service::storage_proxy_stats::stats>();
            proxy.start(std::ref(db), spcfg, std::ref(b), scheduling_group_key_create(sg_conf).get0(), std::ref(feature_service), std::ref(token_metadata), std::ref(ms)).get();
            auto stop_proxy = defer([&proxy] { proxy.stop().get(); });

            mm.start(std::ref(mm_notif), std::ref(feature_service), std::ref(ms)).get();
            auto stop_mm = defer([&mm] { mm.stop().get(); });

            sharded<cql3::query_processor> qp;
            cql3::query_processor::memory_config qp_mcfg = {memory::stats().total_memory() / 256, memory::stats().total_memory() / 2560};
            qp.start(std::ref(proxy), std::ref(db), std::ref(mm_notif), std::ref(mm), qp_mcfg, std::ref(cql_config), std::ref(sl_controller)).get();
            auto stop_qp = defer([&qp] { qp.stop().get(); });

            // In main.cc we call db::system_keyspace::setup which calls
            // minimal_setup and init_local_cache
            db::system_keyspace::minimal_setup(qp);

            db::batchlog_manager_config bmcfg;
            bmcfg.replay_rate = 100000000;
            bmcfg.write_request_timeout = 2s;
            bm.start(std::ref(qp), bmcfg).get();
            auto stop_bm = defer([&bm] { bm.stop().get(); });

            view_update_generator.start(std::ref(db)).get();
            view_update_generator.invoke_on_all(&db::view::view_update_generator::start).get();
            auto stop_view_update_generator = defer([&view_update_generator] {
                view_update_generator.stop().get();
            });

            distributed_loader::init_system_keyspace(db).get();

            auto& ks = db.local().find_keyspace(db::system_keyspace::NAME);
            parallel_for_each(ks.metadata()->cf_meta_data(), [&ks] (auto& pair) {
                auto cfm = pair.second;
                return ks.make_directory_for_column_family(cfm->cf_name(), cfm->id());
            }).get();
            distributed_loader::init_non_system_keyspaces(db, proxy, mm).get();

            db.invoke_on_all([] (database& db) {
                for (auto& x : db.get_column_families()) {
                    table& t = *(x.second);
                    t.enable_auto_compaction();
                }
            }).get();

            auto stop_system_keyspace = defer([] { db::qctx = {}; });
            start_large_data_handler(db).get();

            db.invoke_on_all([] (database& db) {
                db.get_compaction_manager().enable();
            }).get();

            auto stop_database_d = defer([&db] {
                stop_database(db).get();
            });

            db::system_keyspace::init_local_cache().get();
            auto stop_local_cache = defer([] { db::system_keyspace::deinit_local_cache().get(); });

            sys_dist_ks.start(std::ref(qp), std::ref(mm), std::ref(proxy)).get();

            cdc_generation_service.start(std::ref(*cfg), std::ref(gms::get_gossiper()), std::ref(sys_dist_ks), std::ref(abort_sources), std::ref(token_metadata), std::ref(feature_service)).get();
            auto stop_cdc_generation_service = defer([&cdc_generation_service] {
                cdc_generation_service.stop().get();
            });

            sharded<cdc::cdc_service> cdc;
            auto get_cdc_metadata = [] (cdc::generation_service& svc) { return std::ref(svc.get_cdc_metadata()); };
            cdc.start(std::ref(proxy), sharded_parameter(get_cdc_metadata, std::ref(cdc_generation_service)), std::ref(mm_notif)).get();
            auto stop_cdc_service = defer([&] {
                cdc.stop().get();
            });

            service::get_local_storage_service().init_server(service::bind_messaging_port(false)).get();
            service::get_local_storage_service().join_cluster().get();

            auth::permissions_cache_config perm_cache_config;
            perm_cache_config.max_entries = cfg->permissions_cache_max_entries();
            perm_cache_config.validity_period = std::chrono::milliseconds(cfg->permissions_validity_in_ms());
            perm_cache_config.update_period = std::chrono::milliseconds(cfg->permissions_update_interval_in_ms());

            const qualified_name qualified_authorizer_name(auth::meta::AUTH_PACKAGE_NAME, cfg->authorizer());
            const qualified_name qualified_authenticator_name(auth::meta::AUTH_PACKAGE_NAME, cfg->authenticator());
            const qualified_name qualified_role_manager_name(auth::meta::AUTH_PACKAGE_NAME, cfg->role_manager());

            auth::service_config auth_config;
            auth_config.authorizer_java_name = qualified_authorizer_name;
            auth_config.authenticator_java_name = qualified_authenticator_name;
            auth_config.role_manager_java_name = qualified_role_manager_name;

            auth_service.start(perm_cache_config, std::ref(qp), std::ref(mm_notif), std::ref(mm), auth_config).get();
            auth_service.invoke_on_all([&mm] (auth::service& auth) {
                return auth.start(mm.local());
            }).get();

            auto deinit_storage_service_server = defer([&auth_service] {
                gms::stop_gossiping().get();
                auth_service.stop().get();
            });

            sharded<db::view::view_builder> view_builder;
            view_builder.start(std::ref(db), std::ref(sys_dist_ks), std::ref(mm_notif)).get();
            view_builder.invoke_on_all([&mm] (db::view::view_builder& vb) {
                return vb.start(mm.local());
            }).get();
            auto stop_view_builder = defer([&view_builder] {
                view_builder.stop().get();
            });

            // Create the testing user.
            try {
                auth::role_config config;
                config.is_superuser = true;
                config.can_login = true;

                auth::create_role(
                        auth_service.local(),
                        testing_superuser,
                        config,
                        auth::authentication_options()).get0();
            } catch (const auth::role_already_exists&) {
                // The default user may already exist if this `cql_test_env` is starting with previously populated data.
            }

            single_node_cql_env env(db, qp, auth_service, view_builder, view_update_generator, mm_notif, mm, std::ref(sl_controller));
            env.start().get();
            auto stop_env = defer([&env] { env.stop().get(); });

            if (!env.local_db().has_keyspace(ks_name)) {
                env.create_keyspace(ks_name).get();
            }

            with_scheduling_group(dbcfg.statement_scheduling_group, [&func, &env] {
                return func(env);
            }).get();
        });
    }

    future<::shared_ptr<cql_transport::messages::result_message>> execute_batch(
        const std::vector<sstring_view>& queries, std::unique_ptr<cql3::query_options> qo) override {
        using cql3::statements::batch_statement;
        using cql3::statements::modification_statement;
        std::vector<batch_statement::single_statement> modifications;
        boost::transform(queries, back_inserter(modifications), [this](const auto& query) {
            auto stmt = local_qp().get_statement(query, _core_local.local().client_state);
            if (!dynamic_cast<modification_statement*>(stmt->statement.get())) {
                throw exceptions::invalid_request_exception(
                    "Invalid statement in batch: only UPDATE, INSERT and DELETE statements are allowed.");
            }
            return batch_statement::single_statement(static_pointer_cast<modification_statement>(stmt->statement));
        });
        auto batch = ::make_shared<batch_statement>(
            batch_statement::type::UNLOGGED,
            std::move(modifications),
            cql3::attributes::none(),
            local_qp().get_cql_stats());
        auto qs = make_query_state();
        auto& lqo = *qo;
        return local_qp().execute_batch(batch, *qs, lqo, {}).finally([qs, batch, qo = std::move(qo)] {});
    }
};

std::atomic<bool> single_node_cql_env::active = { false };

future<> do_with_cql_env(std::function<future<>(cql_test_env&)> func, cql_test_config cfg_in) {
    return single_node_cql_env::do_with(func, std::move(cfg_in));
}

future<> do_with_cql_env_thread(std::function<void(cql_test_env&)> func, cql_test_config cfg_in, thread_attributes thread_attr) {
    return single_node_cql_env::do_with([func = std::move(func), thread_attr] (auto& e) {
        return seastar::async(thread_attr, [func = std::move(func), &e] {
            return func(e);
        });
    }, std::move(cfg_in));
}

namespace debug {

seastar::sharded<database>* db;

}
