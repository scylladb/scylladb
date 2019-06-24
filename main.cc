/*
 * Copyright (C) 2014 ScyllaDB
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

#include "supervisor.hh"
#include "database.hh"
#include <seastar/core/app-template.hh>
#include <seastar/core/distributed.hh>
#include "thrift/server.hh"
#include "transport/server.hh"
#include <seastar/http/httpd.hh>
#include "api/api_init.hh"
#include "db/config.hh"
#include "db/extensions.hh"
#include "db/legacy_schema_migrator.hh"
#include "service/storage_service.hh"
#include "service/migration_manager.hh"
#include "service/load_broadcaster.hh"
#include "service/view_update_backlog_broker.hh"
#include "streaming/stream_session.hh"
#include "db/system_keyspace.hh"
#include "db/system_distributed_keyspace.hh"
#include "db/batchlog_manager.hh"
#include "db/commitlog/commitlog.hh"
#include "db/hints/manager.hh"
#include "db/commitlog/commitlog_replayer.hh"
#include "db/view/view_builder.hh"
#include "utils/runtime.hh"
#include "utils/file_lock.hh"
#include "log.hh"
#include "debug.hh"
#include "init.hh"
#include "release.hh"
#include "repair/repair.hh"
#include "repair/row_level.hh"
#include <cstdio>
#include <seastar/core/file.hh>
#include <sys/time.h>
#include <sys/resource.h>
#include "disk-error-handler.hh"
#include "tracing/tracing.hh"
#include "tracing/tracing_backend_registry.hh"
#include <seastar/core/prometheus.hh>
#include "message/messaging_service.hh"
#include <seastar/net/dns.hh>
#include <seastar/core/memory.hh>
#include <seastar/util/log-cli.hh>

#include "db/view/view_update_generator.hh"
#include "service/cache_hitrate_calculator.hh"
#include "sstables/compaction_manager.hh"
#include "sstables/sstables.hh"
#include "gms/feature_service.hh"
#include "distributed_loader.hh"

namespace fs = std::filesystem;

seastar::metrics::metric_groups app_metrics;

using namespace std::chrono_literals;

namespace bpo = boost::program_options;

namespace tracing {

void register_tracing_keyspace_backend(backend_registry& br);

}

class stop_signal {
    bool _caught = false;
    condition_variable _cond;
private:
    void signaled() {
        _caught = true;
        _cond.broadcast();
    }
public:
    stop_signal() {
        engine().handle_signal(SIGINT, [this] { signaled(); });
        engine().handle_signal(SIGTERM, [this] { signaled(); });
    }
    ~stop_signal() {
        // There's no way to unregister a handler yet, so register a no-op handler instead.
        engine().handle_signal(SIGINT, [] {});
        engine().handle_signal(SIGTERM, [] {});
    }
    future<> wait() {
        return _cond.wait([this] { return _caught; });
    }
    bool stopping() const {
        return _caught;
    }
};

template<typename K, typename V, typename... Args, typename K2, typename V2 = V>
V get_or_default(const std::unordered_map<K, V, Args...>& ss, const K2& key, const V2& def = V()) {
    const auto iter = ss.find(key);
    if (iter != ss.end()) {
        return iter->second;
    }
    return def;
}

static fs::path relative_conf_dir(fs::path path) {
    static auto conf_dir = db::config::get_conf_dir(); // this is not gonna change in our life time
    return conf_dir / path;
}

static future<>
read_config(bpo::variables_map& opts, db::config& cfg) {
    sstring file;

    if (opts.count("options-file") > 0) {
        file = opts["options-file"].as<sstring>();
    } else {
        file = relative_conf_dir("scylla.yaml").string();
    }
    return check_direct_io_support(file).then([file, &cfg] {
        return cfg.read_from_file(file, [](auto & opt, auto & msg, auto status) {
            auto level = log_level::warn;
            if (status.value_or(db::config::value_status::Invalid) != db::config::value_status::Invalid) {
                level = log_level::error;
            }
            startlog.log(level, "{} : {}", msg, opt);
        });
    }).handle_exception([file](auto ep) {
        startlog.error("Could not read configuration file {}: {}", file, ep);
        return make_exception_future<>(ep);
    });
}
static future<> disk_sanity(sstring path, bool developer_mode) {
    return check_direct_io_support(path).then([] {
        return make_ready_future<>();
    }).handle_exception([path](auto ep) {
        startlog.error("Could not access {}: {}", path, ep);
        return make_exception_future<>(ep);
    });
};

class directories {
public:
    future<> touch_and_lock(sstring path) {
        return io_check([path] { return recursive_touch_directory(path); }).then_wrapped([this, path] (future<> f) {
            try {
                f.get();
                return utils::file_lock::acquire(path + "/.lock").then([this](utils::file_lock lock) {
                   _locks.emplace_back(std::move(lock));
                }).handle_exception([path](auto ep) {
                    // only do this because "normal" unhandled exception exit in seastar
                    // _drops_ system_error message ("what()") and thus does not quite deliver
                    // the relevant info to the user
                    try {
                        std::rethrow_exception(ep);
                    } catch (std::exception& e) {
                        startlog.error("Could not initialize {}: {}", path, e.what());
                        throw;
                    } catch (...) {
                        throw;
                    }
                });
            } catch (...) {
                startlog.error("Directory '{}' cannot be initialized. Tried to do it but failed with: {}", path, std::current_exception());
                throw;
            }
        });
    }
    template<typename _Iter>
    future<> touch_and_lock(_Iter i, _Iter e) {
        return parallel_for_each(i, e, [this](sstring path) {
           return touch_and_lock(std::move(path));
        });
    }
    template<typename _Range>
    future<> touch_and_lock(_Range&& r) {
        return touch_and_lock(std::begin(r), std::end(r));
    }
private:
    std::vector<utils::file_lock>
        _locks;
};

static
void
verify_rlimit(bool developer_mode) {
    struct rlimit lim;
    int r = getrlimit(RLIMIT_NOFILE, &lim);
    if (r == -1) {
        throw std::system_error(errno, std::system_category());
    }
    auto recommended = 200'000U;
    auto min = 10'000U;
    if (lim.rlim_cur < min) {
        if (developer_mode) {
            startlog.warn("NOFILE rlimit too low (recommended setting {}, minimum setting {};"
                          " you may run out of file descriptors.", recommended, min);
        } else {
            startlog.error("NOFILE rlimit too low (recommended setting {}, minimum setting {};"
                          " refusing to start.", recommended, min);
            throw std::runtime_error("NOFILE rlimit too low");
        }
    }
}

static bool cpu_sanity() {
#if defined(__x86_64__) || defined(__i386__)
    if (!__builtin_cpu_supports("sse4.2")) {
        std::cerr << "Scylla requires a processor with SSE 4.2 support\n";
        return false;
    }
#endif
    return true;
}

static void tcp_syncookies_sanity() {
    try {
        auto f = file_desc::open("/proc/sys/net/ipv4/tcp_syncookies", O_RDONLY | O_CLOEXEC);
        char buf[128] = {};
        f.read(buf, 128);
        if (sstring(buf) == "0\n") {
            startlog.warn("sysctl entry net.ipv4.tcp_syncookies is set to 0.\n"
                          "For better performance, set following parameter on sysctl is strongly recommended:\n"
                          "net.ipv4.tcp_syncookies=1");
        }
    } catch (const std::system_error& e) {
            startlog.warn("Unable to check if net.ipv4.tcp_syncookies is set {}", e);
    }
}

static future<>
verify_seastar_io_scheduler(bool has_max_io_requests, bool has_properties, bool developer_mode) {
    auto note_bad_conf = [developer_mode] (sstring cause) {
        sstring msg = "I/O Scheduler is not properly configured! This is a non-supported setup, and performance is expected to be unpredictably bad.\n Reason found: "
                    + cause + "\n"
                    + "To properly configure the I/O Scheduler, run the scylla_io_setup utility shipped with Scylla.\n";

        sstring devmode_msg = msg + "To ignore this, see the developer-mode configuration option.";
        if (developer_mode) {
            startlog.warn(msg.c_str());
        } else {
            startlog.error(devmode_msg.c_str());
            throw std::runtime_error("Bad I/O Scheduler configuration");
        }
    };

    if (!has_max_io_requests && !has_properties) {
        note_bad_conf("none of --max-io-requests, --io-properties and --io-properties-file are set.");
    }
    return smp::invoke_on_all([developer_mode, note_bad_conf, has_max_io_requests] {
        if (has_max_io_requests) {
            auto capacity = engine().get_io_queue().capacity();
            if (capacity < 4) {
                auto cause = format("I/O Queue capacity for this shard is too low ({:d}, minimum 4 expected).", capacity);
                note_bad_conf(cause);
            }
        }
    });
}

static
void
verify_adequate_memory_per_shard(bool developer_mode) {
    auto shard_mem = memory::stats().total_memory();
    if (shard_mem >= (1 << 30)) {
        return;
    }
    if (developer_mode) {
        startlog.warn("Only {} MiB per shard; this is below the recommended minimum of 1 GiB/shard;"
                " continuing since running in developer mode", shard_mem >> 20);
    } else {
        startlog.error("Only {} MiB per shard; this is below the recommended minimum of 1 GiB/shard; terminating."
                "Configure more memory (--memory option) or decrease shard count (--smp option).", shard_mem >> 20);
        throw std::runtime_error("configuration (memory per shard too low)");
    }
}

class memory_threshold_guard {
    seastar::memory::scoped_large_allocation_warning_threshold _slawt;
public:
    explicit memory_threshold_guard(size_t threshold) : _slawt(threshold)  {}
    future<> stop() { return make_ready_future<>(); }
};

static std::optional<std::vector<sstring>> parse_hinted_handoff_enabled(sstring opt) {
    using namespace boost::algorithm;

    if (boost::iequals(opt, "false") || opt == "0") {
        return std::nullopt;
    } else if (boost::iequals(opt, "true") || opt == "1") {
        return std::vector<sstring>{};
    }

    std::vector<sstring> dcs;
    split(dcs, opt, is_any_of(","));

    std::for_each(dcs.begin(), dcs.end(), [] (sstring& dc) {
        trim(dc);
        if (dc.empty()) {
            startlog.error("hinted_handoff_enabled: DC name may not be an empty string");
            throw bad_configuration_error();
        }
    });

    return dcs;
}

int main(int ac, char** av) {
  int return_value = 0;
  try {
    // early check to avoid triggering
    if (!cpu_sanity()) {
        _exit(71);
    }
    runtime::init_uptime();
    std::setvbuf(stdout, nullptr, _IOLBF, 1000);
    app_template::config app_cfg;
    app_cfg.name = "Scylla";
    app_cfg.default_task_quota = 500us;
    app_cfg.auto_handle_sigint_sigterm = false;
    app_template app(std::move(app_cfg));

    auto ext = std::make_shared<db::extensions>();
    auto cfg = make_lw_shared<db::config>(ext);
    auto init = app.get_options_description().add_options();

    // If --version is requested, print it out and exit immediately to avoid
    // Seastar-specific warnings that may occur when running the app
    init("version", bpo::bool_switch(), "print version number and exit");
    bpo::variables_map vm;
    bpo::store(bpo::command_line_parser(ac, av).options(app.get_options_description()).allow_unregistered().run(), vm);
    if (vm["version"].as<bool>()) {
        fmt::print("{}\n", scylla_version());
        return 0;
    }

    bpo::options_description deprecated("Deprecated options - ignored");
    deprecated.add_options()
        ("background-writer-scheduling-quota", bpo::value<float>())
        ("auto-adjust-flush-quota", bpo::value<bool>());
    app.get_options_description().add(deprecated);

    // TODO : default, always read?
    init("options-file", bpo::value<sstring>(), "configuration file (i.e. <SCYLLA_HOME>/conf/scylla.yaml)");

    configurable::append_all(*cfg, init);
    cfg->add_options(init);

    distributed<database> db;
    seastar::sharded<service::cache_hitrate_calculator> cf_cache_hitrate_calculator;
    debug::db = &db;
    auto& qp = cql3::get_query_processor();
    auto& proxy = service::get_storage_proxy();
    auto& mm = service::get_migration_manager();
    api::http_context ctx(db, proxy);
    httpd::http_server_control prometheus_server;
    prometheus::config pctx;
    directories dirs;
    sharded<gms::feature_service> feature_service;

    return app.run(ac, av, [&] () -> future<int> {

        fmt::print("Scylla version {} starting ...\n", scylla_version());
        auto&& opts = app.configuration();

        namespace sm = seastar::metrics;
        app_metrics.add_group("scylladb", {
            sm::make_gauge("current_version", sm::description("Current ScyllaDB version."), { sm::label_instance("version", scylla_version()), sm::shard_label("") }, [] { return 0; })
        });

        const std::unordered_set<sstring> ignored_options = { "auto-adjust-flush-quota", "background-writer-scheduling-quota" };
        for (auto& opt: ignored_options) {
            if (opts.count(opt)) {
                fmt::print("{} option ignored (deprecated)\n", opt);
            }
        }

        // Check developer mode before even reading the config file, because we may not be
        // able to read it if we need to disable strict dma mode.
        // We'll redo this later and apply it to all reactors.
        if (opts.count("developer-mode")) {
            engine().set_strict_dma(false);
        }

        tcp_syncookies_sanity();

        return seastar::async([cfg, ext, &db, &qp, &proxy, &mm, &ctx, &opts, &dirs, &pctx, &prometheus_server, &return_value, &cf_cache_hitrate_calculator,
                               &feature_service] {
          try {
            ::stop_signal stop_signal; // we can move this earlier to support SIGINT during initialization
            read_config(opts, *cfg).get();
            configurable::init_all(opts, *cfg, *ext).get();

            logalloc::prime_segment_pool(memory::stats().total_memory(), memory::min_free_memory()).get();
            logging::apply_settings(cfg->logging_settings(opts));

            startlog.info("Scylla version {} starting.", scylla_version());

            verify_rlimit(cfg->developer_mode());
            verify_adequate_memory_per_shard(cfg->developer_mode());
            if (cfg->partitioner() != "org.apache.cassandra.dht.Murmur3Partitioner") {
                if (cfg->enable_deprecated_partitioners()) {
                    startlog.warn("The partitioner {} is deprecated and will be removed in a future version."
                            "  Contact scylladb-users@googlegroups.com if you are using it in production", cfg->partitioner());
                } else {
                    startlog.error("The partitioner {} is deprecated and will be removed in a future version."
                            "  To enable it, add \"enable_deprecated_partitioners: true\" to scylla.yaml"
                            "  Contact scylladb-users@googlegroups.com if you are using it in production", cfg->partitioner());
                    throw bad_configuration_error();
                }
            }
            feature_service.start().get();
            // FIXME: feature_service.stop(), when we fix up shutdown
            dht::set_global_partitioner(cfg->partitioner(), cfg->murmur3_partitioner_ignore_msb_bits());
            auto make_sched_group = [&] (sstring name, unsigned shares) {
                if (cfg->cpu_scheduler()) {
                    return seastar::create_scheduling_group(name, shares).get0();
                } else {
                    return seastar::scheduling_group();
                }
            };
            auto maintenance_scheduling_group = make_sched_group("streaming", 200);
            auto start_thrift = cfg->start_rpc();
            uint16_t api_port = cfg->api_port();
            ctx.api_dir = cfg->api_ui_dir();
            ctx.api_doc = cfg->api_doc_dir();
            sstring listen_address = cfg->listen_address();
            sstring rpc_address = cfg->rpc_address();
            sstring api_address = cfg->api_address() != "" ? cfg->api_address() : rpc_address;
            sstring broadcast_address = cfg->broadcast_address();
            sstring broadcast_rpc_address = cfg->broadcast_rpc_address();
            std::optional<std::vector<sstring>> hinted_handoff_enabled = parse_hinted_handoff_enabled(cfg->hinted_handoff_enabled());
            auto prom_addr = [&] {
                try {
                    return seastar::net::dns::get_host_by_name(cfg->prometheus_address()).get0();
                } catch (...) {
                    std::throw_with_nested(std::runtime_error(fmt::format("Unable to resolve prometheus_address {}", cfg->prometheus_address())));
                }
            }();
            supervisor::notify("starting prometheus API server");
            uint16_t pport = cfg->prometheus_port();
            std::any stop_prometheus;
            if (pport) {
                pctx.metric_help = "Scylla server statistics";
                pctx.prefix = cfg->prometheus_prefix();
                prometheus_server.start("prometheus").get();
                stop_prometheus = ::make_shared(defer([&prometheus_server] {
                    startlog.info("stopping prometheus API server");
                    prometheus_server.stop().get();
                }));
                prometheus::start(prometheus_server, pctx);
                with_scheduling_group(maintenance_scheduling_group, [&] {
                  return prometheus_server.listen(ipv4_addr{prom_addr.addr_list.front(), pport}).handle_exception([pport, &cfg] (auto ep) {
                    startlog.error("Could not start Prometheus API server on {}:{}: {}", cfg->prometheus_address(), pport, ep);
                    return make_exception_future<>(ep);
                  });
                }).get();
            }
            if (!broadcast_address.empty()) {
                try {
                    utils::fb_utilities::set_broadcast_address(gms::inet_address::lookup(broadcast_address).get0());
                } catch (...) {
                    startlog.error("Bad configuration: invalid 'broadcast_address': {}: {}", broadcast_address, std::current_exception());
                    throw bad_configuration_error();
                }
            } else if (!listen_address.empty()) {
                try {
                    utils::fb_utilities::set_broadcast_address(gms::inet_address::lookup(listen_address).get0());
                } catch (...) {
                    startlog.error("Bad configuration: invalid 'listen_address': {}: {}", listen_address, std::current_exception());
                    throw bad_configuration_error();
                }
            } else {
                startlog.error("Bad configuration: neither listen_address nor broadcast_address are defined\n");
                throw bad_configuration_error();
            }

            if (!broadcast_rpc_address.empty()) {
                utils::fb_utilities::set_broadcast_rpc_address(gms::inet_address::lookup(broadcast_rpc_address).get0());
            } else {
                if (rpc_address == "0.0.0.0") {
                    startlog.error("If rpc_address is set to a wildcard address {}, then you must set broadcast_rpc_address to a value other than {}", rpc_address, rpc_address);
                    throw bad_configuration_error();
                }
                utils::fb_utilities::set_broadcast_rpc_address(gms::inet_address::lookup(rpc_address).get0());
            }

            // TODO: lib.
            auto is_true = [](sstring val) {
                std::transform(val.begin(), val.end(), val.begin(), ::tolower);
                return val == "true" || val == "1";
            };

            // The start_native_transport method is invoked by API as well, and uses the config object
            // (through db) directly. Lets fixup default valued right here instead then, so it in turn can be
            // kept simple
            // TODO: make intrinsic part of config defaults instead
            auto& ceo = cfg->client_encryption_options();
            if (is_true(get_or_default(ceo, "enabled", "false"))) {
                ceo["enabled"] = "true";
                ceo["certificate"] = get_or_default(ceo, "certificate", relative_conf_dir("scylla.crt").string());
                ceo["keyfile"] = get_or_default(ceo, "keyfile", relative_conf_dir("scylla.key").string());
                ceo["require_client_auth"] = is_true(get_or_default(ceo, "require_client_auth", "false")) ? "true" : "false";
            } else {
                ceo["enabled"] = "false";
            }

            using namespace locator;
            // Re-apply strict-dma after we've read the config file, this time
            // to all reactors
            if (opts.count("developer-mode")) {
                smp::invoke_on_all([] { engine().set_strict_dma(false); }).get();
            }
            supervisor::notify("creating tracing");
            tracing::backend_registry tracing_backend_registry;
            tracing::register_tracing_keyspace_backend(tracing_backend_registry);
            tracing::tracing::create_tracing(tracing_backend_registry, "trace_keyspace_helper").get();
            supervisor::notify("creating snitch");
            i_endpoint_snitch::create_snitch(cfg->endpoint_snitch()).get();
            // #293 - do not stop anything
            // engine().at_exit([] { return i_endpoint_snitch::stop_snitch(); });
            supervisor::notify("determining DNS name");
            auto e = [&] {
                try {
                    return seastar::net::dns::get_host_by_name(api_address).get0();
                } catch (...) {
                    std::throw_with_nested(std::runtime_error(fmt::format("Unable to resolve api_address {}", api_address)));
                }
            }();
            supervisor::notify("starting API server");
            auto ip = e.addr_list.front();
            ctx.http_server.start("API").get();
            api::set_server_init(ctx).get();
            with_scheduling_group(maintenance_scheduling_group, [&] {
                return ctx.http_server.listen(ipv4_addr{ip, api_port});
            }).get();
            startlog.info("Scylla API server listening on {}:{} ...", api_address, api_port);
            static sharded<auth::service> auth_service;
            static sharded<db::system_distributed_keyspace> sys_dist_ks;
            static sharded<db::view::view_update_generator> view_update_generator;
            auto& gossiper = gms::get_gossiper();
            gossiper.start(std::ref(feature_service), std::ref(*cfg)).get();
            // #293 - do not stop anything
            //engine().at_exit([]{ return gms::get_gossiper().stop(); });
            supervisor::notify("initializing storage service");
            init_storage_service(db, gossiper, auth_service, sys_dist_ks, view_update_generator, feature_service);
            supervisor::notify("starting per-shard database core");

            // Note: changed from using a move here, because we want the config object intact.
            database_config dbcfg;
            dbcfg.compaction_scheduling_group = make_sched_group("compaction", 1000);
            dbcfg.memory_compaction_scheduling_group = make_sched_group("mem_compaction", 1000);
            dbcfg.streaming_scheduling_group = maintenance_scheduling_group;
            dbcfg.statement_scheduling_group = make_sched_group("statement", 1000);
            dbcfg.memtable_scheduling_group = make_sched_group("memtable", 1000);
            dbcfg.memtable_to_cache_scheduling_group = make_sched_group("memtable_to_cache", 200);
            dbcfg.available_memory = memory::stats().total_memory();
            db.start(std::ref(*cfg), dbcfg).get();
            auto stop_database_and_sstables = defer([&db] {
                // #293 - do not stop anything - not even db (for real)
                //return db.stop();
                // call stop on each db instance, but leave the shareded<database> pointers alive.
                startlog.info("Shutdown database started");
                stop_database(db).then([&db] {
                    return db.invoke_on_all([](auto& db) {
                        return db.stop();
                    });
                }).then([] {
                    startlog.info("Shutdown database finished");
                    return sstables::await_background_jobs_on_all_shards();
                }).get();
            });
            verify_seastar_io_scheduler(opts.count("max-io-requests"), opts.count("io-properties") || opts.count("io-properties-file"),
                                        db.local().get_config().developer_mode()).get();
            supervisor::notify("creating data directories");
            dirs.touch_and_lock(db.local().get_config().data_file_directories()).get();
            supervisor::notify("creating commitlog directory");
            dirs.touch_and_lock(db.local().get_config().commitlog_directory()).get();
            std::unordered_set<sstring> directories;
            directories.insert(db.local().get_config().data_file_directories().cbegin(),
                    db.local().get_config().data_file_directories().cend());
            directories.insert(db.local().get_config().commitlog_directory());

            supervisor::notify("creating hints directories");
            if (hinted_handoff_enabled) {
                fs::path hints_base_dir(db.local().get_config().hints_directory());
                dirs.touch_and_lock(db.local().get_config().hints_directory()).get();
                directories.insert(db.local().get_config().hints_directory());
                for (unsigned i = 0; i < smp::count; ++i) {
                    sstring shard_dir((hints_base_dir / seastar::to_sstring(i).c_str()).native());
                    dirs.touch_and_lock(shard_dir).get();
                    directories.insert(std::move(shard_dir));
                }
            }
            fs::path view_pending_updates_base_dir = fs::path(db.local().get_config().view_hints_directory());
            sstring view_pending_updates_base_dir_str = view_pending_updates_base_dir.native();
            dirs.touch_and_lock(view_pending_updates_base_dir_str).get();
            directories.insert(view_pending_updates_base_dir_str);
            for (unsigned i = 0; i < smp::count; ++i) {
                sstring shard_dir((view_pending_updates_base_dir / seastar::to_sstring(i).c_str()).native());
                dirs.touch_and_lock(shard_dir).get();
                directories.insert(std::move(shard_dir));
            }

            supervisor::notify("verifying directories");
            parallel_for_each(directories, [&db] (sstring pathname) {
                return disk_sanity(pathname, db.local().get_config().developer_mode()).then([dir = std::move(pathname)] {
                    return distributed_loader::verify_owner_and_mode(fs::path(dir)).handle_exception([](auto ep) {
                        startlog.error("Failed owner and mode verification: {}", ep);
                        return make_exception_future<>(ep);
                    });
                });
            }).get();

            // Initialization of a keyspace is done by shard 0 only. For system
            // keyspace, the procedure  will go through the hardcoded column
            // families, and in each of them, it will load the sstables for all
            // shards using distributed database object.
            // Iteration through column family directory for sstable loading is
            // done only by shard 0, so we'll no longer face race conditions as
            // described here: https://github.com/scylladb/scylla/issues/1014
            distributed_loader::init_system_keyspace(db).get();

            supervisor::notify("starting gossip");
            // Moved local parameters here, esp since with the
            // ssl stuff it gets to be a lot.
            uint16_t storage_port = cfg->storage_port();
            uint16_t ssl_storage_port = cfg->ssl_storage_port();
            double phi = cfg->phi_convict_threshold();
            auto seed_provider= cfg->seed_provider();
            sstring cluster_name = cfg->cluster_name();

            const auto& ssl_opts = cfg->server_encryption_options();
            auto tcp_nodelay_inter_dc = cfg->inter_dc_tcp_nodelay();
            auto encrypt_what = get_or_default(ssl_opts, "internode_encryption", "none");
            auto trust_store = get_or_default(ssl_opts, "truststore");
            auto cert = get_or_default(ssl_opts, "certificate", relative_conf_dir("scylla.crt").string());
            auto key = get_or_default(ssl_opts, "keyfile", relative_conf_dir("scylla.key").string());
            auto prio = get_or_default(ssl_opts, "priority_string", sstring());
            auto clauth = is_true(get_or_default(ssl_opts, "require_client_auth", "false"));
            if (cluster_name.empty()) {
                cluster_name = "Test Cluster";
                startlog.warn("Using default cluster name is not recommended. Using a unique cluster name will reduce the chance of adding nodes to the wrong cluster by mistake");
            }
            init_scheduling_config scfg;
            scfg.statement = dbcfg.statement_scheduling_group;
            scfg.streaming = dbcfg.streaming_scheduling_group;
            scfg.gossip = scheduling_group();
            init_ms_fd_gossiper(gossiper
                    , feature_service
                    , *cfg
                    , listen_address
                    , storage_port
                    , ssl_storage_port
                    , tcp_nodelay_inter_dc
                    , encrypt_what
                    , trust_store
                    , cert
                    , key
                    , prio
                    , clauth
                    , cfg->internode_compression()
                    , seed_provider
                    , memory::stats().total_memory()
                    , scfg
                    , cluster_name
                    , phi
                    , cfg->listen_on_broadcast_address());
            supervisor::notify("starting storage proxy");
            service::storage_proxy::config spcfg;
            spcfg.hinted_handoff_enabled = hinted_handoff_enabled;
            spcfg.available_memory = memory::stats().total_memory();
            smp_service_group_config storage_proxy_smp_service_group_config;
            // Assuming less than 1kB per queued request, this limits storage_proxy submit_to() queues to 5MB or less
            storage_proxy_smp_service_group_config.max_nonlocal_requests = 5000;
            spcfg.read_smp_service_group = create_smp_service_group(storage_proxy_smp_service_group_config).get0();
            spcfg.write_smp_service_group = create_smp_service_group(storage_proxy_smp_service_group_config).get0();
            spcfg.write_ack_smp_service_group = create_smp_service_group(storage_proxy_smp_service_group_config).get0();
            static db::view::node_update_backlog node_backlog(smp::count, 10ms);
            proxy.start(std::ref(db), spcfg, std::ref(node_backlog)).get();
            // #293 - do not stop anything
            // engine().at_exit([&proxy] { return proxy.stop(); });
            supervisor::notify("starting migration manager");
            mm.start().get();
            // #293 - do not stop anything
            // engine().at_exit([&mm] { return mm.stop(); });
            supervisor::notify("starting query processor");
            cql3::query_processor::memory_config qp_mcfg = {memory::stats().total_memory() / 256, memory::stats().total_memory() / 2560};
            qp.start(std::ref(proxy), std::ref(db), qp_mcfg).get();
            // #293 - do not stop anything
            // engine().at_exit([&qp] { return qp.stop(); });
            supervisor::notify("initializing batchlog manager");
            db::batchlog_manager_config bm_cfg;
            bm_cfg.write_request_timeout = cfg->write_request_timeout_in_ms() * 1ms;
            bm_cfg.replay_rate = cfg->batchlog_replay_throttle_in_kb() * 1000;

            db::get_batchlog_manager().start(std::ref(qp), bm_cfg).get();
            // #293 - do not stop anything
            // engine().at_exit([] { return db::get_batchlog_manager().stop(); });
            sstables::init_metrics().get();

            db::system_keyspace::minimal_setup(db, qp);
            service::read_sstables_format(service::get_storage_service()).get();

            // schema migration, if needed, is also done on shard 0
            db::legacy_schema_migrator::migrate(proxy, db, qp.local()).get();

            // truncation record migration
            db::system_keyspace::migrate_truncation_records().get();

            supervisor::notify("loading system sstables");

            distributed_loader::ensure_system_table_directories(db).get();

            supervisor::notify("loading non-system sstables");
            distributed_loader::init_non_system_keyspaces(db, proxy).get();

            supervisor::notify("starting view update generator");
            view_update_generator.start(std::ref(db), std::ref(proxy)).get();
            supervisor::notify("discovering staging sstables");
            db.invoke_on_all([] (database& db) {
                for (auto& x : db.get_column_families()) {
                    table& t = *(x.second);
                    for (sstables::shared_sstable sst : *t.get_sstables()) {
                        if (sst->requires_view_building()) {
                            view_update_generator.local().register_staging_sstable(std::move(sst), t.shared_from_this());
                        }
                    }
                }
            }).get();

            // register connection drop notification to update cf's cache hit rate data
            db.invoke_on_all([] (database& db) {
                db.register_connection_drop_notifier(netw::get_local_messaging_service());
            }).get();
            supervisor::notify("setting up system keyspace");
            db::system_keyspace::setup(db, qp, service::get_storage_service()).get();
            supervisor::notify("starting commit log");
            auto cl = db.local().commitlog();
            if (cl != nullptr) {
                auto paths = cl->get_segments_to_replay();
                if (!paths.empty()) {
                    supervisor::notify("replaying commit log");
                    auto rp = db::commitlog_replayer::create_replayer(db).get0();
                    rp.recover(paths, db::commitlog::descriptor::FILENAME_PREFIX).get();
                    supervisor::notify("replaying commit log - flushing memtables");
                    db.invoke_on_all([] (database& db) {
                        return db.flush_all_memtables();
                    }).get();
                    supervisor::notify("replaying commit log - removing old commitlog segments");
                    cl->delete_segments(std::move(paths));
                }
            }

            db.invoke_on_all([&proxy] (database& db) {
                db.get_compaction_manager().start();
            }).get();

            // If the same sstable is shared by several shards, it cannot be
            // deleted until all shards decide to compact it. So we want to
            // start these compactions now. Note we start compacting only after
            // all sstables in this CF were loaded on all shards - otherwise
            // we will have races between the compaction and loading processes
            // We also want to trigger regular compaction on boot.

            for (auto& x : db.local().get_column_families()) {
                column_family& cf = *(x.second);
                distributed_loader::reshard(db, cf.schema()->ks_name(), cf.schema()->cf_name());
            }
            db.invoke_on_all([&proxy] (database& db) {
                for (auto& x : db.get_column_families()) {
                    column_family& cf = *(x.second);
                    cf.trigger_compaction();
                }
            }).get();
            api::set_server_gossip(ctx).get();
            api::set_server_snitch(ctx).get();
            api::set_server_storage_proxy(ctx).get();
            api::set_server_load_sstable(ctx).get();
            static seastar::sharded<memory_threshold_guard> mtg;
            mtg.start(cfg->large_memory_allocation_warning_threshold());
            supervisor::notify("initializing migration manager RPC verbs");
            service::get_migration_manager().invoke_on_all([] (auto& mm) {
                mm.init_messaging_service();
            }).get();
            supervisor::notify("initializing storage proxy RPC verbs");
            proxy.invoke_on_all([] (service::storage_proxy& p) {
                p.init_messaging_service();
            }).get();

            supervisor::notify("starting streaming service");
            streaming::stream_session::init_streaming_service(db, sys_dist_ks, view_update_generator).get();
            api::set_server_stream_manager(ctx).get();

            supervisor::notify("starting hinted handoff manager");
            if (hinted_handoff_enabled) {
                db::hints::manager::rebalance(cfg->hints_directory()).get();
            }
            db::hints::manager::rebalance(cfg->view_hints_directory()).get();

            proxy.invoke_on_all([] (service::storage_proxy& local_proxy) {
                auto& ss = service::get_local_storage_service();
                ss.register_subscriber(&local_proxy);
                local_proxy.start_hints_manager(gms::get_local_gossiper().shared_from_this(), ss.shared_from_this());
            }).get();

            supervisor::notify("starting messaging service");
            // Start handling REPAIR_CHECKSUM_RANGE messages
            netw::get_messaging_service().invoke_on_all([&db] (auto& ms) {
                ms.register_repair_checksum_range([&db] (sstring keyspace, sstring cf, dht::token_range range, rpc::optional<repair_checksum> hash_version) {
                    auto hv = hash_version ? *hash_version : repair_checksum::legacy;
                    return do_with(std::move(keyspace), std::move(cf), std::move(range),
                            [&db, hv] (auto& keyspace, auto& cf, auto& range) {
                        return checksum_range(db, keyspace, cf, range, hv);
                    });
                });
            }).get();
            repair_service rs(gossiper);
            repair_init_messaging_service_handler(rs, sys_dist_ks, view_update_generator).get();
            supervisor::notify("starting storage service", true);
            auto& ss = service::get_local_storage_service();
            ss.init_messaging_service_part().get();
            api::set_server_messaging_service(ctx).get();
            api::set_server_storage_service(ctx).get();
            ss.init_server_without_the_messaging_service_part().get();
            supervisor::notify("starting batchlog manager");
            db::get_batchlog_manager().invoke_on_all([] (db::batchlog_manager& b) {
                return b.start();
            }).get();
            supervisor::notify("starting load broadcaster");
            // should be unique_ptr, but then lambda passed to at_exit will be non copieable and
            // casting to std::function<> will fail to compile
            auto lb = make_shared<service::load_broadcaster>(db, gms::get_local_gossiper());
            lb->start_broadcasting();
            service::get_local_storage_service().set_load_broadcaster(lb);
            auto stop_load_broadcater = defer([lb = std::move(lb)] () {
                startlog.info("stopping load broadcaster");
                lb->stop_broadcasting().get();
            });
            supervisor::notify("starting cf cache hit rate calculator");
            cf_cache_hitrate_calculator.start(std::ref(db), std::ref(cf_cache_hitrate_calculator)).get();
            auto stop_cache_hitrate_calculator = defer([&cf_cache_hitrate_calculator] {
                startlog.info("stopping cf cache hit rate calculator");
                return cf_cache_hitrate_calculator.stop().get();
            });
            cf_cache_hitrate_calculator.local().run_on(engine().cpu_id());

            supervisor::notify("starting view update backlog broker");
            static sharded<service::view_update_backlog_broker> view_backlog_broker;
            view_backlog_broker.start(std::ref(proxy), std::ref(gms::get_gossiper())).get();
            view_backlog_broker.invoke_on_all(&service::view_update_backlog_broker::start).get();
            auto stop_view_backlog_broker = defer([] {
                startlog.info("stopping view update backlog broker");
                view_backlog_broker.stop().get();
            });

            api::set_server_cache(ctx);
            startlog.info("Waiting for gossip to settle before accepting client requests...");
            gms::get_local_gossiper().wait_for_gossip_to_settle().get();
            api::set_server_gossip_settle(ctx).get();

            supervisor::notify("allow replaying hints");
            proxy.invoke_on_all([] (service::storage_proxy& local_proxy) {
                local_proxy.allow_replaying_hints();
            }).get();

            if (cfg->view_building()) {
                supervisor::notify("Launching generate_mv_updates for non system tables");
                view_update_generator.invoke_on_all(&db::view::view_update_generator::start).get();
            }

            static sharded<db::view::view_builder> view_builder;
            if (cfg->view_building()) {
                supervisor::notify("starting the view builder");
                view_builder.start(std::ref(db), std::ref(sys_dist_ks), std::ref(mm)).get();
                view_builder.invoke_on_all(&db::view::view_builder::start).get();
            }

            supervisor::notify("starting native transport");
            with_scheduling_group(dbcfg.statement_scheduling_group, [] {
                return service::get_local_storage_service().start_native_transport();
            }).get();
            if (start_thrift) {
                with_scheduling_group(dbcfg.statement_scheduling_group, [] {
                    return service::get_local_storage_service().start_rpc_server();
                }).get();
            }
            if (cfg->defragment_memory_on_idle()) {
                smp::invoke_on_all([] () {
                    engine().set_idle_cpu_handler([] (reactor::work_waiting_on_reactor check_for_work) {
                        return logalloc::shard_tracker().compact_on_idle(check_for_work);
                    });
                }).get();
            }
            smp::invoke_on_all([&cfg] () {
                return logalloc::shard_tracker().set_reclamation_step(cfg->lsa_reclamation_step());
            }).get();
            if (cfg->abort_on_lsa_bad_alloc()) {
                smp::invoke_on_all([&cfg]() {
                    return logalloc::shard_tracker().enable_abort_on_bad_alloc();
                }).get();
            }
            api::set_server_done(ctx).get();
            supervisor::notify("serving");
            // Register at_exit last, so that storage_service::drain_on_shutdown will be called first
            auto stop_repair = defer([] {
                startlog.info("stopping repair");
                repair_shutdown(service::get_local_storage_service().db()).get();
            });

            auto stop_view_update_generator = defer([] {
                startlog.info("stopping view update generator");
                view_update_generator.stop().get();
            });

            auto do_drain = defer([] {
                startlog.info("draining local storage");
                service::get_local_storage_service().drain_on_shutdown().get();
            });

            auto stop_view_builder = defer([cfg] {
                if (cfg->view_building()) {
                    startlog.info("stopping view builder");
                    view_builder.stop().get();
                }
            });

            auto stop_compaction_manager = defer([&db] {
                db.invoke_on_all([](auto& db) {
                    startlog.info("stopping compaction manager");
                    return db.get_compaction_manager().stop();
                }).get();
            });
            startlog.info("Scylla version {} initialization completed.", scylla_version());
            stop_signal.wait().get();
            startlog.info("Signal received; shutting down");
	    // At this point, all objects destructors and all shutdown hooks registered with defer() are executed
          } catch (...) {
            startlog.info("Startup failed: {}", std::current_exception());
            // We should be returning 1 here, but the system is not yet prepared for orderly rollback of main() objects
            // and thread_local variables.
            _exit(1);
            return 1;
          }
          startlog.info("Scylla version {} shutdown complete.", scylla_version());
          // We should be returning 0 here, but the system is not yet prepared for orderly rollback of main() objects
          // and thread_local variables.
          _exit(0);
          return 0;
        });
    });
  } catch (...) {
      // reactor may not have been initialized, so can't use logger
      fprint(std::cerr, "FATAL: Exception during startup, aborting: %s\n", std::current_exception());
      return 7; // 1 has a special meaning for upstart
  }
}

namespace debug {

seastar::sharded<database>* db;

}
