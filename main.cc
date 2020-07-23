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

#include "build_id.hh"
#include "supervisor.hh"
#include "database.hh"
#include <seastar/core/reactor.hh>
#include <seastar/core/app-template.hh>
#include <seastar/core/distributed.hh>
#include "transport/server.hh"
#include <seastar/http/httpd.hh>
#include "api/api_init.hh"
#include "db/config.hh"
#include "db/extensions.hh"
#include "db/legacy_schema_migrator.hh"
#include "service/storage_service.hh"
#include "service/migration_manager.hh"
#include "service/load_meter.hh"
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
#include "log.hh"
#include "utils/directories.hh"
#include "debug.hh"
#include "auth/common.hh"
#include "init.hh"
#include "release.hh"
#include "repair/repair.hh"
#include "repair/row_level.hh"
#include <cstdio>
#include <seastar/core/file.hh>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/prctl.h>
#include "tracing/tracing.hh"
#include "tracing/tracing_backend_registry.hh"
#include <seastar/core/prometheus.hh>
#include "message/messaging_service.hh"
#include "db/sstables-format-selector.hh"
#include <seastar/net/dns.hh>
#include <seastar/core/io_queue.hh>
#include <seastar/core/abort_on_ebadf.hh>

#include "db/view/view_update_generator.hh"
#include "service/cache_hitrate_calculator.hh"
#include "sstables/compaction_manager.hh"
#include "sstables/sstables.hh"
#include "gms/feature_service.hh"
#include "distributed_loader.hh"
#include "cql3/cql_config.hh"
#include "connection_notifier.hh"
#include "transport/controller.hh"
#include "thrift/controller.hh"

#include "alternator/server.hh"
#include "redis/service.hh"
#include "cdc/log.hh"
#include "cdc/cdc_extension.hh"
#include "alternator/tags_extension.hh"
#include "alternator/rmw_operation.hh"

namespace fs = std::filesystem;

seastar::metrics::metric_groups app_metrics;

using namespace std::chrono_literals;

namespace bpo = boost::program_options;

// Must live in a seastar::thread
class stop_signal {
    bool _caught = false;
    condition_variable _cond;
    sharded<abort_source> _abort_sources;
    future<> _broadcasts_to_abort_sources_done = make_ready_future<>();
private:
    void signaled() {
        if (_caught) {
            return;
        }
        _caught = true;
        _cond.broadcast();
        _broadcasts_to_abort_sources_done = _broadcasts_to_abort_sources_done.then([this] {
            return _abort_sources.invoke_on_all(&abort_source::request_abort);
        });
    }
public:
    stop_signal() {
        _abort_sources.start().get();
        engine().handle_signal(SIGINT, [this] { signaled(); });
        engine().handle_signal(SIGTERM, [this] { signaled(); });
    }
    ~stop_signal() {
        // There's no way to unregister a handler yet, so register a no-op handler instead.
        engine().handle_signal(SIGINT, [] {});
        engine().handle_signal(SIGTERM, [] {});
        _broadcasts_to_abort_sources_done.get();
        _abort_sources.stop().get();
    }
    future<> wait() {
        return _cond.wait([this] { return _caught; });
    }
    bool stopping() const {
        return _caught;
    }
    abort_source& as_local_abort_source() { return _abort_sources.local(); }
    sharded<abort_source>& as_sharded_abort_source() { return _abort_sources; }
};

template<typename K, typename V, typename... Args, typename K2, typename V2 = V>
V get_or_default(const std::unordered_map<K, V, Args...>& ss, const K2& key, const V2& def = V()) {
    const auto iter = ss.find(key);
    if (iter != ss.end()) {
        return iter->second;
    }
    return def;
}

static future<>
read_config(bpo::variables_map& opts, db::config& cfg) {
    sstring file;

    if (opts.count("options-file") > 0) {
        file = opts["options-file"].as<sstring>();
    } else {
        file = db::config::get_conf_sub("scylla.yaml").string();
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

// Handles SIGHUP, using it to trigger re-reading of the configuration file. Should
// only be constructed on shard 0.
class sighup_handler {
    bpo::variables_map& _opts;
    db::config& _cfg;
    condition_variable _cond;
    bool _pending = false; // if asked to reread while already reading
    bool _stopping = false;
    future<> _done = do_work();  // Launch main work loop, capture completion future
public:
    // Installs the signal handler. Must call stop() (and wait for it) before destruction.
    sighup_handler(bpo::variables_map& opts, db::config& cfg) : _opts(opts), _cfg(cfg) {
        startlog.info("installing SIGHUP handler");
        engine().handle_signal(SIGHUP, [this] { reread_config(); });
    }
private:
    void reread_config() {
        if (_stopping) {
            return;
        }
        _pending = true;
        _cond.broadcast();
    }
    // Main work loop. Waits for either _stopping or _pending to be raised, and
    // re-reads the configuration file if _pending. We use a repeat loop here to
    // avoid having multiple reads of the configuration file happening in parallel
    // (this can cause an older read to overwrite the results of a younger read).
    future<> do_work() {
        return repeat([this] {
            return _cond.wait([this] { return _pending || _stopping; }).then([this] {
                return async([this] {
                    if (_stopping) {
                        return stop_iteration::yes;
                    } else if (_pending) {
                        _pending = false;
                        try {
                            startlog.info("re-reading configuration file");
                            read_config(_opts, _cfg).get();
                            _cfg.broadcast_to_all_shards().get();
                            startlog.info("completed re-reading configuration file");
                        } catch (...) {
                            startlog.error("failed to re-read configuration file: {}", std::current_exception());
                        }
                    }
                    return stop_iteration::no;
                });
            });
        });
    }
public:
    // Signals the main work loop to stop, and waits for it (and any in-progress work)
    // to complete. After this is waited for, the object can be destroyed.
    future<> stop() {
        // No way to unregister yet
        engine().handle_signal(SIGHUP, [] {});
        _pending = false;
        _stopping = true;
        _cond.broadcast();
        return std::move(_done);
    }
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
    if (!__builtin_cpu_supports("sse4.2") || !__builtin_cpu_supports("pclmul")) {
        std::cerr << "Scylla requires a processor with SSE 4.2 and PCLMUL support\n";
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

// Formats parsed program options into a string as follows:
// "[key1: value1_1 value1_2 ..., key2: value2_1 value 2_2 ..., (positional) value3, ...]"
std::string format_parsed_options(const std::vector<bpo::option>& opts) {
    return fmt::format("[{}]",
        boost::algorithm::join(opts | boost::adaptors::transformed([] (const bpo::option& opt) {
            if (opt.value.empty()) {
                return opt.string_key;
            }

            return (opt.string_key.empty() ?  "(positional) " : fmt::format("{}: ", opt.string_key)) +
                        boost::algorithm::join(opt.value, " ");
        }), ", ")
    );
}

static constexpr char startup_msg[] = "Scylla version {} with build-id {} starting ...\n";

void print_starting_message(int ac, char** av, const bpo::parsed_options& opts) {
    fmt::print(startup_msg, scylla_version(), get_build_id());
    if (ac) {
        fmt::print("command used: \"{}", av[0]);
        for (int i = 1; i < ac; ++i) {
            fmt::print(" {}", av[i]);
        }
        fmt::print("\"\n");
    }

    fmt::print("parsed command line options: {}\n", format_parsed_options(opts.options));
}

// Glue logic between db::config and cql3::cql_config
class cql_config_updater {
    cql3::cql_config& _cql_config;
    const db::config& _cfg;
    std::vector<std::any> _observers;
private:
    template <typename T>
    void tie(T& dest, const db::config::named_value<T>& src) {
        dest = src();
        _observers.emplace_back(make_lw_shared<utils::observer<T>>(src.observe([&dest] (const T& value) { dest = value; })));
    }
public:
    cql_config_updater(cql3::cql_config& cql_config, const db::config& cfg)
            : _cql_config(cql_config), _cfg(cfg) {
        tie(_cql_config.restrictions.partition_key_restrictions_max_cartesian_product_size, _cfg.max_partition_key_restrictions_per_query);
        tie(_cql_config.restrictions.clustering_key_restrictions_max_cartesian_product_size, _cfg.max_clustering_key_restrictions_per_query);
    }
};

template <typename Func>
inline auto defer_verbose_shutdown(const char* what, Func&& func) {
    auto vfunc = [what, func = std::forward<Func>(func)] () mutable {
        startlog.info("Shutting down {}", what);
        try {
            func();
        } catch (...) {
            startlog.error("Unexpected error shutting down {}: {}", what, std::current_exception());
            throw;
        }
        startlog.info("Shutting down {} was successful", what);
    };

    return deferred_action(std::move(vfunc));
}

int main(int ac, char** av) {
    // Allow core dumps. The would be disabled by default if
    // CAP_SYS_NICE was added to the binary, as is suggested by the
    // epoll backend.
    int r = prctl(PR_SET_DUMPABLE, 1, 0, 0, 0);
    if (r) {
        std::cerr << "Could not make scylla dumpable\n";
        exit(1);
    }

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
    ext->add_schema_extension<alternator::tags_extension>(alternator::tags_extension::NAME);
    ext->add_schema_extension<cdc::cdc_extension>(cdc::cdc_extension::NAME);

    auto cfg = make_lw_shared<db::config>(ext);
    auto init = app.get_options_description().add_options();

    init("version", bpo::bool_switch(), "print version number and exit");

    bpo::options_description deprecated("Deprecated options - ignored");
    deprecated.add_options()
        ("background-writer-scheduling-quota", bpo::value<float>())
        ("auto-adjust-flush-quota", bpo::value<bool>());
    app.get_options_description().add(deprecated);

    // TODO : default, always read?
    init("options-file", bpo::value<sstring>(), "configuration file (i.e. <SCYLLA_HOME>/conf/scylla.yaml)");

    configurable::append_all(*cfg, init);
    cfg->add_options(init);

    // If --version is requested, print it out and exit immediately to avoid
    // Seastar-specific warnings that may occur when running the app
    bpo::variables_map vm;
    auto parsed_opts = bpo::command_line_parser(ac, av).options(app.get_options_description()).allow_unregistered().run();
    bpo::store(parsed_opts, vm);
    if (vm["version"].as<bool>()) {
        fmt::print("{}\n", scylla_version());
        return 0;
    }

    print_starting_message(ac, av, parsed_opts);

    sharded<locator::token_metadata> token_metadata;
    sharded<service::migration_notifier> mm_notifier;
    distributed<database> db;
    seastar::sharded<service::cache_hitrate_calculator> cf_cache_hitrate_calculator;
    service::load_meter load_meter;
    debug::db = &db;
    auto& qp = cql3::get_query_processor();
    auto& proxy = service::get_storage_proxy();
    auto& mm = service::get_migration_manager();
    api::http_context ctx(db, proxy, load_meter, token_metadata);
    httpd::http_server_control prometheus_server;
    utils::directories dirs;
    sharded<gms::feature_service> feature_service;

    return app.run(ac, av, [&] () -> future<int> {

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

        return seastar::async([cfg, ext, &db, &qp, &proxy, &mm, &mm_notifier, &ctx, &opts, &dirs,
                &prometheus_server, &cf_cache_hitrate_calculator, &load_meter, &feature_service,
                &token_metadata] {
          try {
            ::stop_signal stop_signal; // we can move this earlier to support SIGINT during initialization
            read_config(opts, *cfg).get();
            configurable::init_all(opts, *cfg, *ext).get();
            cfg->setup_directories();

            // We're writing to a non-atomic variable here. But bool writes are atomic
            // in all supported architectures, and the broadcast_to_all_shards().get() below
            // will apply the required memory barriers anyway.
            ser::gc_clock_using_3_1_0_serialization = cfg->enable_3_1_0_compatibility_mode();

            cfg->broadcast_to_all_shards().get();

            ::sighup_handler sighup_handler(opts, *cfg);
            auto stop_sighup_handler = defer_verbose_shutdown("sighup", [&] {
                sighup_handler.stop().get();
            });

            logalloc::prime_segment_pool(memory::stats().total_memory(), memory::min_free_memory()).get();
            logging::apply_settings(cfg->logging_settings(opts));

            startlog.info(startup_msg, scylla_version(), get_build_id());

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
            gms::feature_config fcfg = gms::feature_config_from_db_config(*cfg);

            feature_service.start(fcfg).get();
            // FIXME storage_proxy holds a reference on it and is not yet stopped.
            // also the proxy leaves range_slice_read_executor-s hanging around
            // and willing to find out if the cluster_supports_digest_multipartition_reads
            //
            //auto stop_feature_service = defer_verbose_shutdown("feature service", [&feature_service] {
            //    feature_service.stop().get();
            //});

            schema::set_default_partitioner(cfg->partitioner(), cfg->murmur3_partitioner_ignore_msb_bits());
            auto make_sched_group = [&] (sstring name, unsigned shares) {
                if (cfg->cpu_scheduler()) {
                    return seastar::create_scheduling_group(name, shares).get0();
                } else {
                    return seastar::scheduling_group();
                }
            };
            auto maintenance_scheduling_group = make_sched_group("streaming", 200);
            uint16_t api_port = cfg->api_port();
            ctx.api_dir = cfg->api_ui_dir();
            ctx.api_doc = cfg->api_doc_dir();
            auto preferred = cfg->listen_interface_prefer_ipv6() ? std::make_optional(net::inet_address::family::INET6) : std::nullopt;
            auto family = cfg->enable_ipv6_dns_lookup() || preferred ? std::nullopt : std::make_optional(net::inet_address::family::INET);
            sstring listen_address = cfg->listen_address();
            sstring rpc_address = cfg->rpc_address();
            sstring api_address = cfg->api_address() != "" ? cfg->api_address() : rpc_address;
            sstring broadcast_address = cfg->broadcast_address();
            sstring broadcast_rpc_address = cfg->broadcast_rpc_address();
            std::optional<std::vector<sstring>> hinted_handoff_enabled = parse_hinted_handoff_enabled(cfg->hinted_handoff_enabled());
            auto prom_addr = [&] {
                try {
                    return gms::inet_address::lookup(cfg->prometheus_address(), family, preferred).get0();
                } catch (...) {
                    std::throw_with_nested(std::runtime_error(fmt::format("Unable to resolve prometheus_address {}", cfg->prometheus_address())));
                }
            }();
            supervisor::notify("starting prometheus API server");
            uint16_t pport = cfg->prometheus_port();
            std::any stop_prometheus;
            if (pport) {
                prometheus_server.start("prometheus").get();
                stop_prometheus = ::make_shared(defer_verbose_shutdown("prometheus API server", [&prometheus_server, pport] {
                    prometheus_server.stop().get();
                }));

                //FIXME discarded future
                prometheus::config pctx;
                pctx.metric_help = "Scylla server statistics";
                pctx.prefix = cfg->prometheus_prefix();
                (void)prometheus::start(prometheus_server, pctx);
                with_scheduling_group(maintenance_scheduling_group, [&] {
                  return prometheus_server.listen(socket_address{prom_addr, pport}).handle_exception([pport, &cfg] (auto ep) {
                    startlog.error("Could not start Prometheus API server on {}:{}: {}", cfg->prometheus_address(), pport, ep);
                    return make_exception_future<>(ep);
                  });
                }).get();
            }
            if (!broadcast_address.empty()) {
                try {
                    utils::fb_utilities::set_broadcast_address(gms::inet_address::lookup(broadcast_address, family, preferred).get0());
                } catch (...) {
                    startlog.error("Bad configuration: invalid 'broadcast_address': {}: {}", broadcast_address, std::current_exception());
                    throw bad_configuration_error();
                }
            } else if (!listen_address.empty()) {
                try {
                    utils::fb_utilities::set_broadcast_address(gms::inet_address::lookup(listen_address, family, preferred).get0());
                } catch (...) {
                    startlog.error("Bad configuration: invalid 'listen_address': {}: {}", listen_address, std::current_exception());
                    throw bad_configuration_error();
                }
            } else {
                startlog.error("Bad configuration: neither listen_address nor broadcast_address are defined\n");
                throw bad_configuration_error();
            }

            if (!broadcast_rpc_address.empty()) {
                utils::fb_utilities::set_broadcast_rpc_address(gms::inet_address::lookup(broadcast_rpc_address, family, preferred).get0());
            } else {
                if (rpc_address == "0.0.0.0") {
                    startlog.error("If rpc_address is set to a wildcard address {}, then you must set broadcast_rpc_address to a value other than {}", rpc_address, rpc_address);
                    throw bad_configuration_error();
                }
                utils::fb_utilities::set_broadcast_rpc_address(gms::inet_address::lookup(rpc_address, family, preferred).get0());
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
            auto ceo = cfg->client_encryption_options();
            if (is_true(get_or_default(ceo, "enabled", "false"))) {
                ceo["enabled"] = "true";
                ceo["certificate"] = get_or_default(ceo, "certificate", db::config::get_conf_sub("scylla.crt").string());
                ceo["keyfile"] = get_or_default(ceo, "keyfile", db::config::get_conf_sub("scylla.key").string());
                ceo["require_client_auth"] = is_true(get_or_default(ceo, "require_client_auth", "false")) ? "true" : "false";
            } else {
                ceo["enabled"] = "false";
            }
            cfg->client_encryption_options(std::move(ceo), cfg->client_encryption_options.source());

            using namespace locator;
            // Re-apply strict-dma after we've read the config file, this time
            // to all reactors
            if (opts.count("developer-mode")) {
                smp::invoke_on_all([] { engine().set_strict_dma(false); }).get();
            }

            auto abort_on_internal_error_observer = cfg->abort_on_internal_error.observe([] (bool val) {
                set_abort_on_internal_error(val);
            });
            set_abort_on_internal_error(cfg->abort_on_internal_error());

            supervisor::notify("starting tokens manager");
            token_metadata.start().get();
            // storage_proxy holds a reference on it and is not yet stopped.
            // what's worse is that the calltrace
            //   storage_proxy::do_query 
            //                ::query_partition_key_range
            //                ::query_partition_key_range_concurrent
            // leaves unwaited futures on the reactor and once it gets there
            // the token_metadata instance is accessed and ...
            //
            //auto stop_token_metadata = defer_verbose_shutdown("token metadata", [ &token_metadata ] {
            //    token_metadata.stop().get();
            //});

            supervisor::notify("starting migration manager notifier");
            mm_notifier.start().get();
            auto stop_mm_notifier = defer_verbose_shutdown("migration manager notifier", [ &mm_notifier ] {
                mm_notifier.stop().get();
            });

            supervisor::notify("creating tracing");
            tracing::backend_registry tracing_backend_registry;
            tracing::register_tracing_keyspace_backend(tracing_backend_registry);
            tracing::tracing::create_tracing(tracing_backend_registry, "trace_keyspace_helper").get();
            supervisor::notify("creating snitch");
            i_endpoint_snitch::create_snitch(cfg->endpoint_snitch()).get();
            // #293 - do not stop anything
            // engine().at_exit([] { return i_endpoint_snitch::stop_snitch(); });
            supervisor::notify("determining DNS name");
            auto ip = [&] {
                try {
                    return gms::inet_address::lookup(api_address, family, preferred).get0();
                } catch (...) {
                    std::throw_with_nested(std::runtime_error(fmt::format("Unable to resolve api_address {}", api_address)));
                }
            }();
            supervisor::notify("starting API server");
            ctx.http_server.start("API").get();
            api::set_server_init(ctx).get();
            with_scheduling_group(maintenance_scheduling_group, [&] {
                return ctx.http_server.listen(socket_address{ip, api_port});
            }).get();
            startlog.info("Scylla API server listening on {}:{} ...", api_address, api_port);
            static sharded<auth::service> auth_service;
            static sharded<db::system_distributed_keyspace> sys_dist_ks;
            static sharded<db::view::view_update_generator> view_update_generator;
            static sharded<cql3::cql_config> cql_config;
            static sharded<::cql_config_updater> cql_config_updater;
            cql_config.start().get();
            //FIXME: discarded future
            (void)cql_config_updater.start(std::ref(cql_config), std::ref(*cfg));
            auto stop_cql_config_updater = defer([&] { cql_config_updater.stop().get(); });
            auto& gossiper = gms::get_gossiper();
            gossiper.start(std::ref(stop_signal.as_sharded_abort_source()), std::ref(feature_service), std::ref(token_metadata), std::ref(*cfg)).get();
            // #293 - do not stop anything
            //engine().at_exit([]{ return gms::get_gossiper().stop(); });
            supervisor::notify("initializing storage service");
            service::storage_service_config sscfg;
            sscfg.available_memory = memory::stats().total_memory();
            service::init_storage_service(stop_signal.as_sharded_abort_source(), db, gossiper, sys_dist_ks, view_update_generator, feature_service, sscfg, mm_notifier, token_metadata).get();
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
            db.start(std::ref(*cfg), dbcfg, std::ref(mm_notifier), std::ref(feature_service), std::ref(token_metadata), std::ref(stop_signal.as_sharded_abort_source())).get();
            start_large_data_handler(db).get();
            auto stop_database_and_sstables = defer_verbose_shutdown("database", [&db] {
                // #293 - do not stop anything - not even db (for real)
                //return db.stop();
                // call stop on each db instance, but leave the shareded<database> pointers alive.
                stop_database(db).then([&db] {
                    return db.invoke_on_all([](auto& db) {
                        return db.stop();
                    });
                }).then([] {
                    startlog.info("Shutting down database: waiting for background jobs...");
                    return sstables::await_background_jobs_on_all_shards();
                }).get();
            });
            api::set_server_config(ctx).get();
            verify_seastar_io_scheduler(opts.count("max-io-requests"), opts.count("io-properties") || opts.count("io-properties-file"),
                                        cfg->developer_mode()).get();

            dirs.init(*cfg, bool(hinted_handoff_enabled)).get();

            // We need the compaction manager ready early so we can reshard.
            db.invoke_on_all([&proxy, &stop_signal] (database& db) {
                db.get_compaction_manager().enable();
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
            auto cert = get_or_default(ssl_opts, "certificate", db::config::get_conf_sub("scylla.crt").string());
            auto key = get_or_default(ssl_opts, "keyfile", db::config::get_conf_sub("scylla.key").string());
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
            scheduling_group_key_config storage_proxy_stats_cfg =
                    make_scheduling_group_key_config<service::storage_proxy_stats::stats>();
            storage_proxy_stats_cfg.constructor = [plain_constructor = storage_proxy_stats_cfg.constructor] (void* ptr) {
                plain_constructor(ptr);
                reinterpret_cast<service::storage_proxy_stats::stats*>(ptr)->register_stats();
                reinterpret_cast<service::storage_proxy_stats::stats*>(ptr)->register_split_metrics_local();
            };
            proxy.start(std::ref(db), spcfg, std::ref(node_backlog),
                    scheduling_group_key_create(storage_proxy_stats_cfg).get0(), std::ref(feature_service), std::ref(token_metadata)).get();
            // #293 - do not stop anything
            // engine().at_exit([&proxy] { return proxy.stop(); });
            supervisor::notify("starting migration manager");
            mm.start(std::ref(mm_notifier), std::ref(feature_service)).get();
            auto stop_migration_manager = defer_verbose_shutdown("migration manager", [&mm] {
                mm.stop().get();
            });
            supervisor::notify("starting query processor");
            cql3::query_processor::memory_config qp_mcfg = {memory::stats().total_memory() / 256, memory::stats().total_memory() / 2560};
            qp.start(std::ref(proxy), std::ref(db), std::ref(mm_notifier), qp_mcfg, std::ref(cql_config)).get();
            // #293 - do not stop anything
            // engine().at_exit([&qp] { return qp.stop(); });
            supervisor::notify("initializing batchlog manager");
            db::batchlog_manager_config bm_cfg;
            bm_cfg.write_request_timeout = cfg->write_request_timeout_in_ms() * 1ms;
            bm_cfg.replay_rate = cfg->batchlog_replay_throttle_in_kb() * 1000;
            bm_cfg.delay = std::chrono::milliseconds(cfg->ring_delay_ms());

            db::get_batchlog_manager().start(std::ref(qp), bm_cfg).get();
            // #293 - do not stop anything
            // engine().at_exit([] { return db::get_batchlog_manager().stop(); });
            sstables::init_metrics().get();

            db::system_keyspace::minimal_setup(db, qp);

            db::sstables_format_selector sst_format_selector(gossiper.local(), feature_service, db);

            sst_format_selector.start().get();
            auto stop_format_selector = defer_verbose_shutdown("sstables format selector", [&sst_format_selector] {
                sst_format_selector.stop().get();
            });

            // schema migration, if needed, is also done on shard 0
            db::legacy_schema_migrator::migrate(proxy, db, qp.local()).get();

            // truncation record migration
            db::system_keyspace::migrate_truncation_records(feature_service.local().cluster_supports_truncation_table()).get();

            supervisor::notify("loading system sstables");

            distributed_loader::ensure_system_table_directories(db).get();

            static sharded<cdc::cdc_service> cdc;
            cdc.start(std::ref(proxy)).get();
            auto stop_cdc_service = defer_verbose_shutdown("cdc", [] {
                cdc.stop().get();
            });

            supervisor::notify("loading non-system sstables");
            distributed_loader::init_non_system_keyspaces(db, proxy, mm).get();

            supervisor::notify("starting view update generator");
            view_update_generator.start(std::ref(db)).get();
            supervisor::notify("discovering staging sstables");
            db.invoke_on_all([] (database& db) {
                for (auto& x : db.get_column_families()) {
                    table& t = *(x.second);
                    for (sstables::shared_sstable sst : *t.get_sstables()) {
                        if (sst->requires_view_building()) {
                            // FIXME: discarded future.
                            (void)view_update_generator.local().register_staging_sstable(std::move(sst), t.shared_from_this());
                        }
                    }
                }
            }).get();

            // register connection drop notification to update cf's cache hit rate data
            db.invoke_on_all([] (database& db) {
                db.register_connection_drop_notifier(netw::get_local_messaging_service());
            }).get();
            supervisor::notify("setting up system keyspace");
            db::system_keyspace::setup(db, qp, feature_service).get();
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
                    //FIXME: discarded future
                    (void)cl->delete_segments(std::move(paths));
                }
            }

            db.invoke_on_all([] (database& db) {
                for (auto& x : db.get_column_families()) {
                    table& t = *(x.second);
                    t.enable_auto_compaction();
                }
            }).get();

            // If the same sstable is shared by several shards, it cannot be
            // deleted until all shards decide to compact it. So we want to
            // start these compactions now. Note we start compacting only after
            // all sstables in this CF were loaded on all shards - otherwise
            // we will have races between the compaction and loading processes
            // We also want to trigger regular compaction on boot.

            // FIXME: temporary as this code is being replaced. I am keeping the scheduling
            // group that was effectively used in the bulk of it (compaction). Soon it will become
            // streaming

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
            //FIXME: discarded future
            (void)mtg.start(cfg->large_memory_allocation_warning_threshold());
            supervisor::notify("initializing migration manager RPC verbs");
            mm.invoke_on_all([] (auto& mm) {
                mm.init_messaging_service();
            }).get();
            supervisor::notify("initializing storage proxy RPC verbs");
            proxy.invoke_on_all(&service::storage_proxy::init_messaging_service).get();
            auto stop_proxy_handlers = defer_verbose_shutdown("storage proxy RPC verbs", [&proxy] {
                proxy.invoke_on_all(&service::storage_proxy::uninit_messaging_service).get();
            });

            supervisor::notify("starting streaming service");
            streaming::stream_session::init_streaming_service(db, sys_dist_ks, view_update_generator).get();
            auto stop_streaming_service = defer_verbose_shutdown("streaming service", [] {
                streaming::stream_session::uninit_streaming_service().get();
            });
            api::set_server_stream_manager(ctx).get();

            supervisor::notify("starting hinted handoff manager");
            if (hinted_handoff_enabled) {
                db::hints::manager::rebalance(cfg->hints_directory()).get();
            }
            db::hints::manager::rebalance(cfg->view_hints_directory()).get();

            proxy.invoke_on_all([] (service::storage_proxy& local_proxy) {
                auto& ss = service::get_local_storage_service();
                ss.register_subscriber(&local_proxy);
                //FIXME: discarded future
                (void)local_proxy.start_hints_manager(gms::get_local_gossiper().shared_from_this(), ss.shared_from_this());
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
            auto max_memory_repair = db.local().get_available_memory() * 0.1;
            repair_service rs(gossiper, max_memory_repair);
            auto stop_repair_service = defer_verbose_shutdown("repair service", [&rs] {
                rs.stop().get();
            });
            repair_init_messaging_service_handler(rs, sys_dist_ks, view_update_generator).get();
            auto stop_repair_messages = defer_verbose_shutdown("repair message handlers", [] {
                repair_uninit_messaging_service_handler().get();
            });
            supervisor::notify("starting storage service", true);
            auto& ss = service::get_local_storage_service();
            ss.init_messaging_service_part().get();
            api::set_server_messaging_service(ctx).get();
            api::set_server_storage_service(ctx).get();

            gossiper.local().register_(ss.shared_from_this());
            auto stop_listening = defer_verbose_shutdown("storage service notifications", [&gossiper, &ss] {
                gossiper.local().unregister_(ss.shared_from_this()).get();
            });

            /*
             * This fuse prevents gossiper from staying active in case the
             * drain_on_shutdown below is not registered. When we fix the
             * start-stop sequence it will be removed.
             */
            auto gossiping_fuse = defer_verbose_shutdown("gossiping", [] {
                gms::stop_gossiping().get();
            });

            ss.init_server().get();
            sst_format_selector.sync();
            ss.join_cluster().get();

            startlog.info("SSTable data integrity checker is {}.",
                    cfg->enable_sstable_data_integrity_check() ? "enabled" : "disabled");


            supervisor::notify("starting auth service");
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

            auth_service.start(perm_cache_config, std::ref(qp), std::ref(mm_notifier), std::ref(mm), auth_config).get();

            auth_service.invoke_on_all([&mm] (auth::service& auth) {
                return auth.start(mm.local());
            }).get();

            auto stop_auth_service = defer_verbose_shutdown("auth service", [] {
                auth_service.stop().get();
            });

            api::set_server_snapshot(ctx).get();
            auto stop_snapshots = defer_verbose_shutdown("snapshots", [] {
                service::get_storage_service().invoke_on_all(&service::storage_service::snapshots_close).get();
            });

            supervisor::notify("starting batchlog manager");
            db::get_batchlog_manager().invoke_on_all([] (db::batchlog_manager& b) {
                return b.start();
            }).get();

            supervisor::notify("starting load meter");
            load_meter.init(db, gms::get_local_gossiper()).get();
            auto stop_load_meter = defer_verbose_shutdown("load meter", [&load_meter] {
                load_meter.exit().get();
            });

            supervisor::notify("starting cf cache hit rate calculator");
            cf_cache_hitrate_calculator.start(std::ref(db)).get();
            auto stop_cache_hitrate_calculator = defer_verbose_shutdown("cf cache hit rate calculator",
                    [&cf_cache_hitrate_calculator] {
                        return cf_cache_hitrate_calculator.stop().get();
                    }
            );
            cf_cache_hitrate_calculator.local().run_on(this_shard_id());

            supervisor::notify("starting view update backlog broker");
            static sharded<service::view_update_backlog_broker> view_backlog_broker;
            view_backlog_broker.start(std::ref(proxy), std::ref(gms::get_gossiper())).get();
            view_backlog_broker.invoke_on_all(&service::view_update_backlog_broker::start).get();
            auto stop_view_backlog_broker = defer_verbose_shutdown("view update backlog broker", [] {
                view_backlog_broker.stop().get();
            });

            //FIXME: discarded future
            (void)api::set_server_cache(ctx);
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
                view_builder.start(std::ref(db), std::ref(sys_dist_ks), std::ref(mm_notifier)).get();
                view_builder.invoke_on_all([&mm] (db::view::view_builder& vb) { 
                    return vb.start(mm.local());
                }).get();
            }

            // Truncate `clients' CF - this table should not persist between server restarts.
            clear_clientlist().get();

            db.invoke_on_all([] (database& db) {
                db.revert_initial_system_read_concurrency_boost();
            }).get();

            cql_transport::controller cql_server_ctl(db, auth_service, mm_notifier, gossiper.local());

            ss.register_client_shutdown_hook("native transport", [&cql_server_ctl] {
                cql_server_ctl.stop().get();
            });

            std::any stop_cql;
            if (cfg->start_native_transport()) {
                supervisor::notify("starting native transport");
                with_scheduling_group(dbcfg.statement_scheduling_group, [&cql_server_ctl] {
                    return cql_server_ctl.start_server();
                }).get();

                // FIXME -- this should be done via client hooks instead
                stop_cql = ::make_shared(defer_verbose_shutdown("native transport", [&cql_server_ctl] {
                    cql_server_ctl.stop().get();
                }));
            }

            api::set_transport_controller(ctx, cql_server_ctl).get();
            auto stop_transport_controller = defer_verbose_shutdown("transport controller API", [&ctx] {
                api::unset_transport_controller(ctx).get();
            });

            ::thrift_controller thrift_ctl(db, auth_service);

            ss.register_client_shutdown_hook("rpc server", [&thrift_ctl] {
                thrift_ctl.stop().get();
            });

            std::any stop_rpc;
            if (cfg->start_rpc()) {
                with_scheduling_group(dbcfg.statement_scheduling_group, [&thrift_ctl] {
                    return thrift_ctl.start_server();
                }).get();

                // FIXME -- this should be done via client hooks instead
                stop_rpc = ::make_shared(defer_verbose_shutdown("rpc server", [&thrift_ctl] {
                    thrift_ctl.stop().get();
                }));
            }

            api::set_rpc_controller(ctx, thrift_ctl).get();
            auto stop_rpc_controller = defer_verbose_shutdown("rpc controller API", [&ctx] {
                api::unset_rpc_controller(ctx).get();
            });

            if (cfg->alternator_port() || cfg->alternator_https_port()) {
                alternator::rmw_operation::set_default_write_isolation(cfg->alternator_write_isolation());
                static sharded<alternator::executor> alternator_executor;
                static sharded<alternator::server> alternator_server;

                net::inet_address addr;
                try {
                    addr = net::dns::get_host_by_name(cfg->alternator_address(), family).get0().addr_list.front();
                } catch (...) {
                    std::throw_with_nested(std::runtime_error(fmt::format("Unable to resolve alternator_address {}", cfg->alternator_address())));
                }
                // Create an smp_service_group to be used for limiting the
                // concurrency when forwarding Alternator request between
                // shards - if necessary for LWT.
                smp_service_group_config c;
                c.max_nonlocal_requests = 5000;
                smp_service_group ssg = create_smp_service_group(c).get0();
                alternator_executor.start(std::ref(proxy), std::ref(mm), ssg).get();
                alternator_server.start(std::ref(alternator_executor)).get();
                std::optional<uint16_t> alternator_port;
                if (cfg->alternator_port()) {
                    alternator_port = cfg->alternator_port();
                }
                std::optional<uint16_t> alternator_https_port;
                std::optional<tls::credentials_builder> creds;
                if (cfg->alternator_https_port()) {
                    creds.emplace();
                    alternator_https_port = cfg->alternator_https_port();
                    creds->set_dh_level(tls::dh_params::level::MEDIUM);
                    creds->set_x509_key_file(cert, key, tls::x509_crt_format::PEM).get();
                    if (trust_store.empty()) {
                        creds->set_system_trust().get();
                    } else {
                        creds->set_x509_trust_file(trust_store, tls::x509_crt_format::PEM).get();
                    }
                    creds->set_priority_string(db::config::default_tls_priority);
                    if (!prio.empty()) {
                        creds->set_priority_string(prio);
                    }
                    if (clauth) {
                        creds->set_client_auth(seastar::tls::client_auth::REQUIRE);
                    }
                }
                bool alternator_enforce_authorization = cfg->alternator_enforce_authorization();
                with_scheduling_group(dbcfg.statement_scheduling_group,
                        [addr, alternator_port, alternator_https_port, creds = std::move(creds), alternator_enforce_authorization] () mutable {
                    return alternator_server.invoke_on_all(
                            [addr, alternator_port, alternator_https_port, creds = std::move(creds), alternator_enforce_authorization] (alternator::server& server) mutable {
                        auto& ss = service::get_local_storage_service();
                        return server.init(addr, alternator_port, alternator_https_port, creds, alternator_enforce_authorization, &ss.service_memory_limiter());
                    }).then([addr, alternator_port, alternator_https_port] {
                        startlog.info("Alternator server listening on {}, HTTP port {}, HTTPS port {}",
                                addr, alternator_port ? std::to_string(*alternator_port) : "OFF", alternator_https_port ? std::to_string(*alternator_https_port) : "OFF");
                    });
                }).get();
                auto stop_alternator = [ssg] {
                    alternator_server.stop().get();
                    alternator_executor.stop().get();
                    destroy_smp_service_group(ssg).get();
                };

                ss.register_client_shutdown_hook("alternator", std::move(stop_alternator));
            }

            static redis_service redis;
            if (cfg->redis_port() || cfg->redis_ssl_port()) {
                with_scheduling_group(dbcfg.statement_scheduling_group, [proxy = std::ref(proxy), db = std::ref(db), auth_service = std::ref(auth_service), cfg] {
                    return redis.init(proxy, db, auth_service, *cfg);
                }).get();
            }

            smp::invoke_on_all([&cfg] {
                logalloc::tracker::config st_cfg;
                st_cfg.defragment_on_idle = cfg->defragment_memory_on_idle();
                st_cfg.abort_on_lsa_bad_alloc = cfg->abort_on_lsa_bad_alloc();
                st_cfg.lsa_reclamation_step = cfg->lsa_reclamation_step();
                logalloc::shard_tracker().configure(st_cfg);
            }).get();

            seastar::set_abort_on_ebadf(cfg->abort_on_ebadf());
            api::set_server_done(ctx).get();
            supervisor::notify("serving");
            // Register at_exit last, so that storage_service::drain_on_shutdown will be called first

            auto stop_repair = defer_verbose_shutdown("repair", [] {
                repair_shutdown(service::get_local_storage_service().db()).get();
            });

            auto stop_view_update_generator = defer_verbose_shutdown("view update generator", [] {
                view_update_generator.stop().get();
            });

            auto do_drain = defer_verbose_shutdown("local storage", [] {
                service::get_local_storage_service().drain_on_shutdown().get();
            });

            auto stop_view_builder = defer_verbose_shutdown("view builder", [cfg] {
                if (cfg->view_building()) {
                    view_builder.stop().get();
                }
            });

            auto stop_redis_service = defer_verbose_shutdown("redis service", [&cfg] {
                if (cfg->redis_port() || cfg->redis_ssl_port()) {
                    redis.stop().get();
                }
            });

            startlog.info("Scylla version {} initialization completed.", scylla_version());
            stop_signal.wait().get();
            startlog.info("Signal received; shutting down");
	    // At this point, all objects destructors and all shutdown hooks registered with defer() are executed
          } catch (...) {
            startlog.error("Startup failed: {}", std::current_exception());
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
