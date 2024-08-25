/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <boost/range/algorithm/transform.hpp>
#include <iterator>
#include <random>
#include <seastar/core/thread.hh>
#include <seastar/util/defer.hh>
#include "replica/database_fwd.hh"
#include "test/lib/cql_test_env.hh"
#include "cdc/generation_service.hh"
#include "cql3/functions/functions.hh"
#include "cql3/query_processor.hh"
#include "cql3/query_options.hh"
#include "cql3/statements/batch_statement.hh"
#include "cql3/statements/modification_statement.hh"
#include "cql3/cql_config.hh"
#include <fmt/ranges.h>
#include <seastar/core/distributed.hh>
#include <seastar/core/abort_source.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/coroutine.hh>
#include "service/migration_manager.hh"
#include "service/qos/raft_service_level_distributed_data_accessor.hh"
#include "service/tablet_allocator.hh"
#include "compaction/compaction_manager.hh"
#include "message/messaging_service.hh"
#include "service/raft/raft_address_map.hh"
#include "service/raft/raft_group_registry.hh"
#include "service/storage_service.hh"
#include "service/storage_proxy.hh"
#include "service/mapreduce_service.hh"
#include "service/endpoint_lifecycle_subscriber.hh"
#include "auth/service.hh"
#include "auth/common.hh"
#include "db/config.hh"
#include "db/batchlog_manager.hh"
#include "schema/schema_builder.hh"
#include "test/lib/tmpdir.hh"
#include "test/lib/log.hh"
#include "db/view/view_builder.hh"
#include "db/view/node_view_update_backlog.hh"
#include "db/view/view_update_generator.hh"
#include "replica/distributed_loader.hh"
// TODO: remove (#293)
#include "message/messaging_service.hh"
#include "gms/gossiper.hh"
#include "gms/feature_service.hh"
#include "db/system_keyspace.hh"
#include "db/system_distributed_keyspace.hh"
#include "db/sstables-format-selector.hh"
#include "repair/row_level.hh"
#include "utils/assert.hh"
#include "utils/class_registrator.hh"
#include "utils/cross-shard-barrier.hh"
#include "streaming/stream_manager.hh"
#include "debug.hh"
#include "db/schema_tables.hh"
#include "db/virtual_tables.hh"
#include "service/raft/raft_group0_client.hh"
#include "service/raft/raft_group0.hh"
#include "sstables/sstables_manager.hh"
#include "init.hh"
#include "lang/manager.hh"

#include <sys/time.h>
#include <sys/resource.h>

using namespace std::chrono_literals;

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
    db_config->add_per_partition_rate_limit_extension();
    db_config->add_tags_extension();
    db_config->add_tombstone_gc_extension();

    db_config->flush_schema_tables_after_modification.set(false);
    db_config->commitlog_use_o_dsync(false);
}

cql_test_config::cql_test_config(const cql_test_config&) = default;
cql_test_config::~cql_test_config() = default;

static const sstring testing_superuser = "tester";

// END TODO

data_dictionary::database
cql_test_env::data_dictionary() {
    return db().local().as_data_dictionary();
}

class single_node_cql_env : public cql_test_env {
public:
    static constexpr std::string_view ks_name = "ks";
    static std::atomic<bool> active;
private:
    sharded<replica::database> _db;
    sharded<gms::feature_service> _feature_service;
    sharded<sstables::storage_manager> _sstm;
    sharded<service::storage_proxy> _proxy;
    sharded<cql3::query_processor> _qp;
    sharded<auth::service> _auth_service;
    sharded<db::view::view_builder> _view_builder;
    sharded<db::view::view_update_generator> _view_update_generator;
    sharded<service::migration_notifier> _mnotifier;
    sharded<qos::service_level_controller> _sl_controller;
    sharded<service::topology_state_machine> _topology_state_machine;
    sharded<service::migration_manager> _mm;
    sharded<db::batchlog_manager> _batchlog_manager;
    sharded<gms::gossiper> _gossiper;
    sharded<service::raft_group_registry> _group0_registry;
    sharded<db::system_keyspace> _sys_ks;
    sharded<service::tablet_allocator> _tablet_allocator;
    sharded<db::system_distributed_keyspace> _sys_dist_ks;
    sharded<locator::snitch_ptr> _snitch;
    sharded<compaction_manager> _cm;
    sharded<tasks::task_manager> _task_manager;
    sharded<netw::messaging_service> _ms;
    sharded<service::storage_service> _ss;
    sharded<locator::shared_token_metadata> _token_metadata;
    sharded<locator::effective_replication_map_factory> _erm_factory;
    sharded<sstables::directory_semaphore> _sst_dir_semaphore;
    sharded<lang::manager> _lang_manager;
    sharded<cql3::cql_config> _cql_config;
    sharded<service::endpoint_lifecycle_notifier> _elc_notif;
    sharded<cdc::generation_service> _cdc_generation_service;
    sharded<repair_service> _repair;
    sharded<streaming::stream_manager> _stream_manager;
    sharded<service::mapreduce_service> _mapreduce_service;
    sharded<direct_failure_detector::failure_detector> _fd;
    sharded<service::raft_address_map> _raft_address_map;
    sharded<service::direct_fd_pinger> _fd_pinger;
    sharded<cdc::cdc_service> _cdc;

    service::raft_group0_client* _group0_client;

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
            cql_transport::cql_protocol_extension_enum_set cql_proto_exts;
            cql_proto_exts.set(cql_transport::cql_protocol_extension::TABLETS_ROUTING_V1);
            _core_local.local().client_state.set_protocol_extensions(std::move(cql_proto_exts));
        }
        return ::make_shared<service::query_state>(_core_local.local().client_state, empty_service_permit());
    }
    static void adjust_rlimit() {
        // Tests should use 1024 file descriptors, but don't punish them
        // with weird behavior if they do.
        //
        // Since this more of a courtesy, don't make the situation worse if
        // getrlimit/setrlimit fail for some reason.
        struct rlimit lim;
        int r = getrlimit(RLIMIT_NOFILE, &lim);
        if (r == -1) {
            return;
        }
        if (lim.rlim_cur < lim.rlim_max) {
            lim.rlim_cur = lim.rlim_max;
            setrlimit(RLIMIT_NOFILE, &lim);
        }
    }
public:
    single_node_cql_env()
    {
        adjust_rlimit();
    }

    virtual future<::shared_ptr<cql_transport::messages::result_message>> execute_cql(sstring_view text) override {
        testlog.trace("{}(\"{}\")", __FUNCTION__, text);
        auto qs = make_query_state();
        auto qo = make_shared<cql3::query_options>(cql3::query_options::DEFAULT);
        return local_qp().execute_direct_without_checking_exception_message(text, *qs, *qo).then([qs, qo] (auto msg) {
            return cql_transport::messages::propagate_exception_as_future(std::move(msg));
        });
    }

    virtual future<::shared_ptr<cql_transport::messages::result_message>> execute_cql(
        sstring_view text,
        std::unique_ptr<cql3::query_options> qo) override
    {
        testlog.trace("{}(\"{}\")", __FUNCTION__, text);
        auto qs = make_query_state();
        auto& lqo = *qo;
        return local_qp().execute_direct_without_checking_exception_message(text, *qs, lqo).then([qs, qo = std::move(qo)] (auto msg) {
            return cql_transport::messages::propagate_exception_as_future(std::move(msg));
        });
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
        cql3::raw_value_vector_with_unset values,
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

        SCYLLA_ASSERT(stmt->get_bound_terms() == qo->get_values_count());
        qo->prepare(prepared->bound_names);

        auto qs = make_query_state();
        auto& lqo = *qo;
        return local_qp().execute_prepared_without_checking_exception_message(*qs, std::move(stmt), lqo, std::move(prepared), std::move(id), true)
            .then([qs, qo = std::move(qo)] (auto msg) {
                return cql_transport::messages::propagate_exception_as_future(std::move(msg));
            });
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
        cql3::statements::modification_statement::json_cache_opt json_cache = modif_stmt->maybe_prepare_json_cache(qo);
        std::vector<dht::partition_range> keys = modif_stmt->build_partition_keys(qo, json_cache);

        return modif_stmt->get_mutations(local_qp(), qo, timeout, false, qo.get_timestamp(*qs), *qs, json_cache, keys)
            .finally([qs, modif_stmt = std::move(modif_stmt)] {});
    }

    virtual future<> create_table(std::function<schema(std::string_view)> schema_maker) override {
        schema_builder builder(make_lw_shared<schema>(schema_maker(ks_name)));
        auto s = builder.build(schema_builder::compact_storage::no);
        auto group0_guard = co_await _mm.local().start_group0_operation();
        auto ts = group0_guard.write_timestamp();
        co_return co_await _mm.local().announce(co_await service::prepare_new_column_family_announcement(_proxy.local(), s, ts), std::move(group0_guard), "");
    }

    virtual service::client_state& local_client_state() override {
        return _core_local.local().client_state;
    }

    virtual replica::database& local_db() override {
        return _db.local();
    }

    virtual sharded<locator::shared_token_metadata>& shared_token_metadata() override {
        return _token_metadata;
    }

    cql3::query_processor& local_qp() override {
        return _qp.local();
    }

    sharded<replica::database>& db() override {
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

    virtual sharded<db::batchlog_manager>& batchlog_manager() override {
        return _batchlog_manager;
    }

    virtual sharded<netw::messaging_service>& get_messaging_service() override {
        return _ms;
    }

    virtual sharded<gms::gossiper>& gossiper() override {
        return _gossiper;
    }

    virtual service::raft_group0_client& get_raft_group0_client() override {
        return *_group0_client;
    }

    virtual sharded<service::raft_group_registry>& get_raft_group_registry() override {
        return _group0_registry;
    }

    virtual sharded<db::system_keyspace>& get_system_keyspace() override {
        return _sys_ks;
    }

    virtual sharded<service::tablet_allocator>& get_tablet_allocator() override {
        return _tablet_allocator;
    }

    virtual sharded<service::storage_proxy>& get_storage_proxy() override {
        return _proxy;
    }

    virtual sharded<gms::feature_service>& get_feature_service() override {
        return _feature_service;
    }

    virtual sharded<sstables::storage_manager>& get_sstorage_manager() override {
        return _sstm;
    }

    virtual sharded<service::storage_service>& get_storage_service() override {
        return _ss;
    }

    virtual sharded<tasks::task_manager>& get_task_manager() override {
        return _task_manager;
    }

    virtual future<> refresh_client_state() override {
        return _core_local.invoke_on_all([] (core_local_state& state) {
            return state.client_state.maybe_update_per_service_level_params();
        });
    }

    future<> create_keyspace(const cql_test_config& cfg, std::string_view name) {
        auto query = format("create keyspace {} with replication = {{ 'class' : 'org.apache.cassandra.locator.NetworkTopologyStrategy', 'replication_factor' : 1}}{};", name,
                            cfg.initial_tablets ? format(" and tablets = {{'initial' : {}}}", *cfg.initial_tablets) : "");
        return execute_cql(query).discard_result();
    }

    static future<> do_with(std::function<future<>(cql_test_env&)> func, cql_test_config cfg_in, std::optional<cql_test_init_configurables> init_configurables) {
        return seastar::async([cfg_in = std::move(cfg_in), init_configurables = std::move(init_configurables), func] {
            logalloc::prime_segment_pool(memory::stats().total_memory(), memory::min_free_memory()).get();
            bool old_active = false;
            if (!active.compare_exchange_strong(old_active, true)) {
                throw std::runtime_error("Starting more than one cql_test_env at a time not supported due to singletons.");
            }
            auto deactivate = defer([] {
                bool old_active = true;
                auto success = active.compare_exchange_strong(old_active, false);
                SCYLLA_ASSERT(success);
            });

            // FIXME: make the function storage non static
            auto clear_funcs = defer([] {
                smp::invoke_on_all([] () {
                    cql3::functions::change_batch batch;
                    batch.clear_functions();
                    batch.commit();
                }).get();
            });

            single_node_cql_env env;
            env.run_in_thread(std::move(func), std::move(cfg_in), std::move(init_configurables));
        });
    }

private:
    void run_in_thread(std::function<future<>(cql_test_env&)> func, cql_test_config cfg_in, std::optional<cql_test_init_configurables> init_configurables) {
            using namespace std::filesystem;

            // disable reactor stall detection during startup
            auto blocked_reactor_notify_ms = engine().get_blocked_reactor_notify_ms();
            smp::invoke_on_all([] {
                engine().update_blocked_reactor_notify_ms(std::chrono::milliseconds(1000000));
            }).get();

            sharded<abort_source> abort_sources;
            abort_sources.start().get();
            // FIXME: handle signals (SIGINT, SIGTERM) - request aborts
            auto stop_abort_sources = defer([&] { abort_sources.stop().get(); });

            debug::the_database = &_db;
            auto reset_db_ptr = defer([] {
                debug::the_database = nullptr;
            });
            auto cfg = cfg_in.db_config;
            if (!cfg->reader_concurrency_semaphore_serialize_limit_multiplier.is_set()) {
                cfg->reader_concurrency_semaphore_serialize_limit_multiplier.set(std::numeric_limits<uint32_t>::max());
            }
            if (!cfg->reader_concurrency_semaphore_kill_limit_multiplier.is_set()) {
                cfg->reader_concurrency_semaphore_kill_limit_multiplier.set(std::numeric_limits<uint32_t>::max());
            }
            tmpdir data_dir;
            auto data_dir_path = data_dir.path().string();
            if (!cfg->data_file_directories.is_set()) {
                cfg->data_file_directories.set({data_dir_path});
            } else {
                data_dir_path = cfg->data_file_directories()[0];
            }
            cfg->commitlog_directory.set(data_dir_path + "/commitlog.dir");
            cfg->schema_commitlog_directory.set(cfg->commitlog_directory() + "/schema");
            cfg->hints_directory.set(data_dir_path + "/hints.dir");
            cfg->view_hints_directory.set(data_dir_path + "/view_hints.dir");
            cfg->num_tokens.set(256);
            cfg->ring_delay_ms.set(500);
            cfg->shutdown_announce_in_ms.set(0);
            cfg->broadcast_to_all_shards().get();
            create_directories((data_dir_path + "/system").c_str());
            create_directories(cfg->commitlog_directory().c_str());
            create_directories(cfg->schema_commitlog_directory().c_str());
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

            auto scheduling_groups = get_scheduling_groups().get();

            auto notify_set = init_configurables
                ? configurable::init_all(*cfg, init_configurables->extensions, service_set(
                    _db, _ss, _mm, _proxy, _feature_service, _ms, _qp, _batchlog_manager
                )).get()
                : configurable::notify_set{}
                ;

            auto stop_configurables = defer([&] { notify_set.notify_all(configurable::system_state::stopped).get(); });

            gms::feature_config fcfg = gms::feature_config_from_db_config(*cfg, cfg_in.disabled_features);
            _feature_service.start(fcfg).get();
            auto stop_feature_service = defer([this] { _feature_service.stop().get(); });

            auto my_address = cfg_in.broadcast_address;

            locator::snitch_config snitch_config;
            snitch_config.listen_address = my_address;
            snitch_config.broadcast_address = my_address;
            _snitch.start(snitch_config).get();
            auto stop_snitch = defer([this] { _snitch.stop().get(); });
            _snitch.invoke_on_all(&locator::snitch_ptr::start).get();

            locator::token_metadata::config tm_cfg;
            tm_cfg.topo_cfg.this_endpoint = my_address;
            tm_cfg.topo_cfg.this_cql_address = my_address;
            tm_cfg.topo_cfg.local_dc_rack = { _snitch.local()->get_datacenter(), _snitch.local()->get_rack() };
            _token_metadata.start([] () noexcept { return db::schema_tables::hold_merge_lock(); }, tm_cfg).get();
            auto stop_token_metadata = defer([this] { _token_metadata.stop().get(); });

            _erm_factory.start().get();
            auto stop_erm_factory = deferred_stop(_erm_factory);

            _mnotifier.start().get();
            auto stop_mm_notify = defer([this] { _mnotifier.stop().get(); });

            _sst_dir_semaphore.start(cfg->initial_sstable_loading_concurrency()).get();
            auto stop_sst_dir_sem = defer([this] {
                _sst_dir_semaphore.stop().get();
            });

            replica::database_config dbcfg;
            if (cfg_in.dbcfg) {
                dbcfg = std::move(*cfg_in.dbcfg);
            } else {
                dbcfg.available_memory = memory::stats().total_memory();
            }

            dbcfg.compaction_scheduling_group = scheduling_groups.compaction_scheduling_group;
            dbcfg.memory_compaction_scheduling_group = scheduling_groups.memory_compaction_scheduling_group;
            dbcfg.streaming_scheduling_group = scheduling_groups.streaming_scheduling_group;
            dbcfg.statement_scheduling_group = scheduling_groups.statement_scheduling_group;
            dbcfg.memtable_scheduling_group = scheduling_groups.memtable_scheduling_group;
            dbcfg.memtable_to_cache_scheduling_group = scheduling_groups.memtable_to_cache_scheduling_group;
            dbcfg.gossip_scheduling_group = scheduling_groups.gossip_scheduling_group;
            dbcfg.sstables_format = sstables::version_from_string(cfg->sstable_format());

            auto get_tm_cfg = sharded_parameter([&] {
                return tasks::task_manager::config {
                    .task_ttl = cfg->task_ttl_seconds,
                };
            });
            _task_manager.start(std::move(get_tm_cfg), std::ref(abort_sources)).get();
            auto stop_task_manager = defer([this] {
                _task_manager.stop().get();
            });

            // get_cm_cfg is called on each shard when starting a sharded<compaction_manager>
            // we need the getter since updateable_value is not shard-safe (#7316)
            auto get_cm_cfg = sharded_parameter([&] {
                return compaction_manager::config {
                    .compaction_sched_group = compaction_manager::scheduling_group{dbcfg.compaction_scheduling_group},
                    .maintenance_sched_group = compaction_manager::scheduling_group{dbcfg.streaming_scheduling_group},
                    .available_memory = dbcfg.available_memory,
                    .static_shares = cfg->compaction_static_shares,
                    .throughput_mb_per_sec = cfg->compaction_throughput_mb_per_sec,
                    .flush_all_tables_before_major = cfg->compaction_flush_all_tables_before_major_seconds() * 1s,
                };
            });
            _cm.start(std::move(get_cm_cfg), std::ref(abort_sources), std::ref(_task_manager)).get();
            auto stop_cm = deferred_stop(_cm);

            _sstm.start(std::ref(*cfg), sstables::storage_manager::config{}).get();
            auto stop_sstm = deferred_stop(_sstm);

            lang::manager::config lang_config;
            lang_config.lua.max_bytes = cfg->user_defined_function_allocation_limit_bytes();
            lang_config.lua.max_contiguous = cfg->user_defined_function_contiguous_allocation_limit_bytes();
            lang_config.lua.timeout = std::chrono::milliseconds(cfg->user_defined_function_time_limit_ms());
            if (cfg->enable_user_defined_functions() && cfg->check_experimental(db::experimental_features_t::feature::UDF)) {
                lang_config.wasm = lang::manager::wasm_config {
                    .udf_memory_limit = cfg->wasm_udf_memory_limit(),
                    .cache_size = dbcfg.available_memory * cfg->wasm_cache_memory_fraction(),
                    .cache_instance_size = cfg->wasm_cache_instance_size_limit(),
                    .cache_timer_period = std::chrono::milliseconds(cfg->wasm_cache_timeout_in_ms()),
                    .yield_fuel = cfg->wasm_udf_yield_fuel(),
                    .total_fuel = cfg->wasm_udf_total_fuel(),
                };
            }

            _lang_manager.start(lang_config).get();
            auto stop_lang_manager = defer([this] { _lang_manager.stop().get(); });
            _lang_manager.invoke_on_all(&lang::manager::start).get();


            _db.start(std::ref(*cfg), dbcfg, std::ref(_mnotifier), std::ref(_feature_service), std::ref(_token_metadata), std::ref(_cm), std::ref(_sstm), std::ref(_lang_manager), std::ref(_sst_dir_semaphore), std::ref(abort_sources), utils::cross_shard_barrier()).get();
            auto stop_db = defer([this] {
                _db.stop().get();
            });

            _db.invoke_on_all(&replica::database::start).get();

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
            _proxy.start(std::ref(_db), spcfg, std::ref(b), scheduling_group_key_create(sg_conf).get(), std::ref(_feature_service), std::ref(_token_metadata), std::ref(_erm_factory)).get();
            auto stop_proxy = defer([this] { _proxy.stop().get(); });

            _cql_config.start(cql3::cql_config::default_tag{}).get();
            auto stop_cql_config = defer([this] { _cql_config.stop().get(); });

            cql3::query_processor::memory_config qp_mcfg;
            if (cfg_in.qp_mcfg) {
                qp_mcfg = *cfg_in.qp_mcfg;
            } else {
                qp_mcfg = {memory::stats().total_memory() / 256, memory::stats().total_memory() / 2560};
            }
            auto local_data_dict = seastar::sharded_parameter([] (const replica::database& db) { return db.as_data_dictionary(); }, std::ref(_db));

            utils::loading_cache_config auth_prep_cache_config;
            auth_prep_cache_config.max_size = qp_mcfg.authorized_prepared_cache_size;
            auth_prep_cache_config.expiry = std::min(std::chrono::milliseconds(cfg->permissions_validity_in_ms()),
                                                     std::chrono::duration_cast<std::chrono::milliseconds>(cql3::prepared_statements_cache::entry_expiry));
            auth_prep_cache_config.refresh = std::chrono::milliseconds(cfg->permissions_update_interval_in_ms());

            _qp.start(std::ref(_proxy), std::move(local_data_dict), std::ref(_mnotifier), qp_mcfg, std::ref(_cql_config), auth_prep_cache_config, std::ref(_lang_manager)).get();
            auto stop_qp = defer([this] { _qp.stop().get(); });

            _elc_notif.start().get();
            auto stop_elc_notif = defer([this] { _elc_notif.stop().get(); });

            set_abort_on_internal_error(true);
            const gms::inet_address listen("127.0.0.1");
            _sl_controller.start(std::ref(_auth_service), std::ref(_token_metadata), std::ref(abort_sources), qos::service_level_options{}).get();
            auto stop_sl_controller = defer([this] { _sl_controller.stop().get(); });
            _sl_controller.invoke_on_all(&qos::service_level_controller::start).get();

            _sys_ks.start(std::ref(_qp), std::ref(_db)).get();
            auto stop_sys_kd = defer([this] {
                _sys_ks.invoke_on_all(&db::system_keyspace::shutdown).get();
                _sys_ks.stop().get();
            });

            replica::distributed_loader::init_system_keyspace(_sys_ks, _erm_factory, _db).get();
            _db.local().init_schema_commitlog();
            _sys_ks.invoke_on_all(&db::system_keyspace::mark_writable).get();

            _sys_ks.local().build_bootstrap_info().get();

            auto host_id = cfg_in.host_id;
            if (!host_id) {
                auto linfo = _sys_ks.local().load_local_info().get();
                if (!linfo.host_id) {
                    linfo.host_id = locator::host_id::create_random_id();
                }
                host_id = linfo.host_id;
                _sys_ks.local().save_local_info(std::move(linfo), _snitch.local()->get_location(), my_address, my_address).get();
            }
            locator::shared_token_metadata::mutate_on_all_shards(_token_metadata, [hostid = host_id, &cfg_in] (locator::token_metadata& tm) {
                auto& topo = tm.get_topology();
                topo.set_host_id_cfg(hostid);
                topo.add_or_update_endpoint(hostid,
                                            cfg_in.broadcast_address,
                                            std::nullopt,
                                            locator::node::state::normal,
                                            smp::count);
                return make_ready_future<>();
            }).get();

            uint16_t port = 7000;
            if (cfg_in.ms_listen) {
               std::random_device rd;
               std::mt19937 gen(rd());
               std::uniform_int_distribution<> distrib(27000, 37000);
               port = distrib(gen);
            }
            // Don't start listening so tests can be run in parallel if cfg_in.ms_listen is not set to true explicitly.
            _ms.start(host_id, listen, std::move(port)).get();
            auto stop_ms = defer([this] { _ms.stop().get(); });

            if (cfg_in.ms_listen) {
                _ms.invoke_on_all(&netw::messaging_service::start_listen, std::ref(_token_metadata)).get();
            }

            // Normally the auth server is already stopped in here,
            // but if there is an initialization failure we have to
            // make sure to stop it now or ~sharded will SCYLLA_ASSERT.
            auto stop_auth_server = defer([this] {
                _auth_service.stop().get();
            });

            auto stop_sys_dist_ks = defer([this] { _sys_dist_ks.stop().get(); });

            // Init gossiper
            std::set<gms::inet_address> seeds;
            auto seed_provider = db::config::seed_provider_type();
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

            gms::gossip_config gcfg;
            gcfg.cluster_name = "Test Cluster";
            gcfg.seeds = std::move(seeds);
            gcfg.skip_wait_for_gossip_to_settle = 0;
            gcfg.shutdown_announce_ms = 0;
            _gossiper.start(std::ref(abort_sources), std::ref(_token_metadata), std::ref(_ms), std::move(gcfg)).get();
            auto stop_ms_fd_gossiper = defer([this] {
                _gossiper.stop().get();
            });
            _gossiper.invoke_on_all(&gms::gossiper::start).get();

            _raft_address_map.start().get();
            auto stop_address_map = defer([this] {
                _raft_address_map.stop().get();
            });

            _fd_pinger.start(std::ref(_ms), std::ref(_raft_address_map)).get();
            auto stop_fd_pinger = defer([this] { _fd_pinger.stop().get(); });

            service::direct_fd_clock fd_clock;
            _fd.start(
                std::ref(_fd_pinger), std::ref(fd_clock),
                service::direct_fd_clock::base::duration{std::chrono::milliseconds{100}}.count(),
                service::direct_fd_clock::base::duration{std::chrono::milliseconds{600}}.count()).get();

            auto stop_fd = defer([this] {
                _fd.stop().get();
            });

            _group0_registry.start(
                raft::server_id{host_id.id},
                std::ref(_raft_address_map),
                std::ref(_ms), std::ref(_fd)).get();
            auto stop_raft_gr = deferred_stop(_group0_registry);

            _stream_manager.start(std::ref(*cfg), std::ref(_db), std::ref(_view_builder), std::ref(_ms), std::ref(_mm), std::ref(_gossiper), scheduling_groups.streaming_scheduling_group).get();
            auto stop_streaming = defer([this] { _stream_manager.stop().get(); });

            _feature_service.invoke_on_all([] (auto& fs) {
                return fs.enable(fs.supported_feature_set());
            }).get();

            _mapreduce_service.start(std::ref(_ms), std::ref(_proxy), std::ref(_db), std::ref(_token_metadata), std::ref(abort_sources)).get();
            auto stop_mapreduce_service =  defer([this] { _mapreduce_service.stop().get(); });

            // gropu0 client exists only on shard 0
            service::raft_group0_client group0_client(_group0_registry.local(), _sys_ks.local(), maintenance_mode_enabled::no);

            _mm.start(std::ref(_mnotifier), std::ref(_feature_service), std::ref(_ms), std::ref(_proxy), std::ref(_gossiper), std::ref(group0_client), std::ref(_sys_ks)).get();
            auto stop_mm = defer([this] { _mm.stop().get(); });

            _tablet_allocator.start(service::tablet_allocator::config{}, std::ref(_mnotifier), std::ref(_db)).get();
            auto stop_tablet_allocator = defer([this] {
                _tablet_allocator.stop().get();
            });

            _topology_state_machine.start().get();
            auto stop_topology_state_machine = defer([this] {
                _topology_state_machine.stop().get();
            });

            service::raft_group0 group0_service{
                    abort_sources.local(), _group0_registry.local(), _ms,
                    _gossiper.local(), _feature_service.local(), _sys_ks.local(), group0_client, scheduling_groups.gossip_scheduling_group};

            _ss.start(std::ref(abort_sources), std::ref(_db),
                std::ref(_gossiper),
                std::ref(_sys_ks),
                std::ref(_sys_dist_ks),
                std::ref(_feature_service), std::ref(_mm),
                std::ref(_token_metadata), std::ref(_erm_factory), std::ref(_ms),
                std::ref(_repair),
                std::ref(_stream_manager),
                std::ref(_elc_notif),
                std::ref(_batchlog_manager),
                std::ref(_snitch),
                std::ref(_tablet_allocator),
                std::ref(_cdc_generation_service),
                std::ref(_view_builder),
                std::ref(_qp),
                std::ref(_sl_controller),
                std::ref(_topology_state_machine),
                std::ref(_task_manager)).get();
            auto stop_storage_service = defer([this] { _ss.stop().get(); });

            _mnotifier.local().register_listener(&_ss.local());
            auto stop_mm_listener = defer([this] {
                _mnotifier.local().unregister_listener(&_ss.local()).get();
            });

            smp::invoke_on_all([&] {
                return db::initialize_virtual_tables(_db, _ss, _gossiper, _group0_registry, _sys_ks, *cfg);
            }).get();

            _qp.invoke_on_all([this, &group0_client] (cql3::query_processor& qp) {
                qp.start_remote(_mm.local(), _mapreduce_service.local(), _ss.local(), group0_client);
            }).get();
            auto stop_qp_remote = defer([this] {
                _qp.invoke_on_all(&cql3::query_processor::stop_remote).get();
            });

            _cm.invoke_on_all([&](compaction_manager& cm) {
                auto cl = _db.local().commitlog();
                auto scl = _db.local().schema_commitlog();
                if (cl && scl) {
                    cm.get_tombstone_gc_state().set_gc_time_min_source([cl, scl](const table_id& id) {
                        return std::min(cl->min_gc_time(id), scl->min_gc_time(id));
                    });
                } else if (cl) {
                    cm.get_tombstone_gc_state().set_gc_time_min_source([cl](const table_id& id) {
                        return cl->min_gc_time(id);
                    });
                } else if (scl) {
                    cm.get_tombstone_gc_state().set_gc_time_min_source([scl](const table_id& id) {
                        return scl->min_gc_time(id);
                    });
                }
            }).get();

            replica::distributed_loader::init_non_system_keyspaces(_db, _proxy, _sys_ks).get();

            _db.invoke_on_all([] (replica::database& db) {
                db.get_tables_metadata().for_each_table([] (table_id, lw_shared_ptr<replica::table> table) {
                    replica::table& t = *table;
                    t.enable_auto_compaction();
                });
            }).get();

            _group0_registry.invoke_on_all([] (service::raft_group_registry& raft_gr) {
                return raft_gr.start();
            }).get();

            if (cfg_in.run_with_raft_recovery) {
                _sys_ks.local().save_group0_upgrade_state("RECOVERY").get();
            }

            group0_client.init().get();

            auto shutdown_db = defer([this] {
                _db.invoke_on_all(&replica::database::shutdown).get();
            });
            // XXX: drain_on_shutdown raft before stopping the database and
            // query processor. Group registry stop raft groups
            // when stopped, and until then the groups may use
            // the database and the query processor.
            auto drain_raft = defer([this] {
                _group0_registry.invoke_on_all(&service::raft_group_registry::drain_on_shutdown).get();
            });

            _view_update_generator.start(std::ref(_db), std::ref(_proxy), std::ref(abort_sources)).get();
            _view_update_generator.invoke_on_all(&db::view::view_update_generator::start).get();
            auto stop_view_update_generator = defer([this] {
                _view_update_generator.stop().get();
            });

            _sys_dist_ks.start(std::ref(_qp), std::ref(_mm), std::ref(_proxy)).get();

            _view_builder.start(std::ref(_db), std::ref(_sys_ks), std::ref(_sys_dist_ks), std::ref(_mnotifier), std::ref(_view_update_generator)).get();
            auto stop_view_builder = defer([this] {
                _view_builder.stop().get();
            });

            if (cfg_in.need_remote_proxy) {
                _proxy.invoke_on_all(&service::storage_proxy::start_remote, std::ref(_ms), std::ref(_gossiper), std::ref(_mm), std::ref(_sys_ks)).get();
            }
            auto stop_proxy_remote = defer([this, need = cfg_in.need_remote_proxy] {
                if (need) {
                    _proxy.invoke_on_all(&service::storage_proxy::stop_remote).get();
                }
            });

            _sl_controller.invoke_on_all([this, &group0_client] (qos::service_level_controller& service) {
                qos::service_level_controller::service_level_distributed_data_accessor_ptr service_level_data_accessor =
                        ::static_pointer_cast<qos::service_level_controller::service_level_distributed_data_accessor>(
                                make_shared<qos::raft_service_level_distributed_data_accessor>(_qp.local(), group0_client));
                return service.set_distributed_data_accessor(std::move(service_level_data_accessor));
            }).get();

            cdc::generation_service::config cdc_config;
            cdc_config.ignore_msb_bits = cfg->murmur3_partitioner_ignore_msb_bits();
            /*
             * Currently used when choosing the timestamp of the first CDC stream generation:
             * normally we choose a timestamp in the future so other nodes have a chance to learn about it
             * before it starts operating, but in the single-node-cluster case this is not necessary
             * and would only slow down tests (by having them wait).
             */
            cdc_config.ring_delay = std::chrono::milliseconds(0);
            _cdc_generation_service.start(std::ref(cdc_config), std::ref(_gossiper), std::ref(_sys_dist_ks), std::ref(_sys_ks), std::ref(abort_sources), std::ref(_token_metadata), std::ref(_feature_service), std::ref(_db), [&] { return !cfg->force_gossip_topology_changes(); }).get();
            auto stop_cdc_generation_service = defer([this] {
                _cdc_generation_service.stop().get();
            });

            auto get_cdc_metadata = [] (cdc::generation_service& svc) { return std::ref(svc.get_cdc_metadata()); };
            _cdc.start(std::ref(_proxy), sharded_parameter(get_cdc_metadata, std::ref(_cdc_generation_service)), std::ref(_mnotifier)).get();
            auto stop_cdc_service = defer([this] {
                _cdc.stop().get();
            });

            group0_service.start().get();
            auto stop_group0_service = defer([&group0_service] {
                group0_service.abort().get();
            });

            _ss.local().set_group0(group0_service);

            const auto generation_number = gms::generation_type(_sys_ks.local().increment_and_get_generation().get());

            // Load address_map from system.peers and subscribe to gossiper events to keep it updated.
            _ss.local().init_address_map(_raft_address_map.local(), generation_number).get();
            auto cancel_address_map_subscription = defer([this] {
                _ss.local().uninit_address_map().get();
            });

            auto stop_group0_usage_in_storage_service = defer([this] {
                _ss.local().wait_for_group0_stop().get();
            });

            group0_service.setup_group0_if_exist(_sys_ks.local(), _ss.local(), _qp.local(), _mm.local()).get();

            try {
                _ss.local().join_cluster(_sys_dist_ks, _proxy, service::start_hint_manager::no, generation_number).get();
            } catch (std::exception& e) {
                // if any of the defers crashes too, we'll never see
                // the error
                testlog.error("Failed to join cluster: {}", e);
                throw;
            }

            utils::loading_cache_config perm_cache_config;
            perm_cache_config.max_size = cfg->permissions_cache_max_entries();
            perm_cache_config.expiry = std::chrono::milliseconds(cfg->permissions_validity_in_ms());
            perm_cache_config.refresh = std::chrono::milliseconds(cfg->permissions_update_interval_in_ms());

            const qualified_name qualified_authorizer_name(auth::meta::AUTH_PACKAGE_NAME, cfg->authorizer());
            const qualified_name qualified_authenticator_name(auth::meta::AUTH_PACKAGE_NAME, cfg->authenticator());
            const qualified_name qualified_role_manager_name(auth::meta::AUTH_PACKAGE_NAME, cfg->role_manager());

            auth::service_config auth_config;
            auth_config.authorizer_java_name = qualified_authorizer_name;
            auth_config.authenticator_java_name = qualified_authenticator_name;
            auth_config.role_manager_java_name = qualified_role_manager_name;

            _auth_service.start(perm_cache_config, std::ref(_qp), std::ref(group0_client), std::ref(_mnotifier), std::ref(_mm), auth_config, maintenance_socket_enabled::no).get();
            _auth_service.invoke_on_all([this] (auth::service& auth) {
                return auth.start(_mm.local(), _sys_ks.local());
            }).get();

            auto deinit_storage_service_server = defer([this] {
                _gossiper.invoke_on_all(&gms::gossiper::shutdown).get();
                _auth_service.stop().get();
            });

            db::batchlog_manager_config bmcfg;
            bmcfg.replay_rate = 100000000;
            bmcfg.write_request_timeout = 2s;
            bmcfg.delay = 0ms;
            _batchlog_manager.start(std::ref(_qp), std::ref(_sys_ks), bmcfg).get();
            auto stop_bm = defer([this] {
                _batchlog_manager.stop().get();
            });

            _view_builder.invoke_on_all([this] (db::view::view_builder& vb) {
                return vb.start(_mm.local());
            }).get();

            // Create the testing user.
            try {
                auth::role_config config;
                config.is_superuser = true;
                config.can_login = true;

                auto& as   = abort_sources.local();
                auto guard = group0_client.start_operation(as).get();
                service::group0_batch mc{std::move(guard)};
                auth::create_role(
                        _auth_service.local(),
                        testing_superuser,
                        config,
                        auth::authentication_options(),
                        mc).get();
                std::move(mc).commit(group0_client, as, ::service::raft_timeout{}).get();
            } catch (const auth::role_already_exists&) {
                // The default user may already exist if this `cql_test_env` is starting with previously populated data.
            }

            notify_set.notify_all(configurable::system_state::started).get();

            _group0_client = &group0_client;

            _core_local.start(std::ref(_auth_service), std::ref(_sl_controller)).get();
            auto stop_core_local = defer([this] { _core_local.stop().get(); });

            if (!local_db().has_keyspace(ks_name)) {
                create_keyspace(cfg_in, ks_name).get();
            }

            with_scheduling_group(dbcfg.statement_scheduling_group, [&func, this] {
                return func(*this);
            }).get();
    }

public:
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
        return local_qp().execute_batch_without_checking_exception_message(batch, *qs, lqo, {}).then([qs, batch, qo = std::move(qo)] (auto msg) {
            return cql_transport::messages::propagate_exception_as_future(std::move(msg));
        });
    }
};

std::atomic<bool> single_node_cql_env::active = { false };

future<> do_with_cql_env(std::function<future<>(cql_test_env&)> func, cql_test_config cfg_in, std::optional<cql_test_init_configurables> init_configurables) {
    return single_node_cql_env::do_with(func, std::move(cfg_in), std::move(init_configurables));
}

future<> do_with_cql_env_thread(std::function<void(cql_test_env&)> func, cql_test_config cfg_in, thread_attributes thread_attr, std::optional<cql_test_init_configurables> init_configurables) {
    return single_node_cql_env::do_with([func = std::move(func), thread_attr] (auto& e) {
        return seastar::async(thread_attr, [func = std::move(func), &e] {
            return func(e);
        });
    }, std::move(cfg_in), std::move(init_configurables));
}

// this function should be called in seastar thread
void do_with_mc(cql_test_env& env, std::function<void(service::group0_batch&)> func) {
    seastar::abort_source as;
    auto& g0 = env.get_raft_group0_client();
    auto guard = g0.start_operation(as).get();
    auto mc = service::group0_batch(std::move(guard));
    func(mc);
    std::move(mc).commit(g0, as, std::nullopt).get();
}

reader_permit make_reader_permit(cql_test_env& env) {
    return env.local_db().get_reader_concurrency_semaphore().make_tracking_only_permit(nullptr, "test", db::no_timeout, {});
}
