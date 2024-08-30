/*
 * Copyright (C) 2014-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <algorithm>
#include <functional>
#include <yaml-cpp/yaml.h>
#include <fmt/ranges.h>

#include <seastar/util/closeable.hh>
#include <seastar/core/abort_source.hh>
#include "gms/inet_address.hh"
#include "auth/allow_all_authenticator.hh"
#include "auth/allow_all_authorizer.hh"
#include "auth/maintenance_socket_role_manager.hh"
#include <seastar/core/future.hh>
#include <seastar/core/timer.hh>
#include "service/qos/raft_service_level_distributed_data_accessor.hh"
#include "tasks/task_manager.hh"
#include "utils/assert.hh"
#include "utils/build_id.hh"
#include "supervisor.hh"
#include "replica/database.hh"
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
#include "service/tablet_allocator.hh"
#include "service/load_meter.hh"
#include "service/view_update_backlog_broker.hh"
#include "service/qos/service_level_controller.hh"
#include "streaming/stream_session.hh"
#include "db/system_keyspace.hh"
#include "db/system_distributed_keyspace.hh"
#include "db/batchlog_manager.hh"
#include "db/commitlog/commitlog.hh"
#include "db/hints/manager.hh"
#include "db/commitlog/commitlog_replayer.hh"
#include "db/view/view_builder.hh"
#include "utils/class_registrator.hh"
#include "utils/error_injection.hh"
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
#include <unistd.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/prctl.h>
#include "tracing/tracing.hh"
#include <seastar/core/prometheus.hh>
#include "message/messaging_service.hh"
#include "db/sstables-format-selector.hh"
#include "db/snapshot-ctl.hh"
#include "cql3/query_processor.hh"
#include <seastar/net/dns.hh>
#include <seastar/core/io_queue.hh>
#include <seastar/core/abort_on_ebadf.hh>

#include "db/view/view_update_generator.hh"
#include "service/cache_hitrate_calculator.hh"
#include "compaction/compaction_manager.hh"
#include "sstables/sstables.hh"
#include "gms/feature_service.hh"
#include "replica/distributed_loader.hh"
#include "sstables_loader.hh"
#include "cql3/cql_config.hh"
#include "transport/controller.hh"
#include "service/memory_limiter.hh"
#include "service/endpoint_lifecycle_subscriber.hh"
#include "db/schema_tables.hh"

#include "redis/controller.hh"
#include "cdc/log.hh"
#include "cdc/cdc_extension.hh"
#include "cdc/generation_service.hh"
#include "tombstone_gc_extension.hh"
#include "db/tags/extension.hh"
#include "db/paxos_grace_seconds_extension.hh"
#include "service/qos/standard_service_level_distributed_data_accessor.hh"
#include "service/storage_proxy.hh"
#include "service/mapreduce_service.hh"
#include "alternator/controller.hh"
#include "alternator/ttl.hh"
#include "tools/entry_point.hh"
#include "test/perf/entry_point.hh"
#include "db/per_partition_rate_limit_extension.hh"
#include "lang/manager.hh"
#include "sstables/sstables_manager.hh"
#include "db/virtual_tables.hh"

#include "service/raft/raft_address_map.hh"
#include "service/raft/raft_group_registry.hh"
#include "service/raft/raft_group0_client.hh"
#include "service/raft/raft_group0.hh"

#include <boost/algorithm/string/join.hpp>

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

struct object_storage_endpoint_param {
    sstring endpoint;
    s3::endpoint_config config;
};

namespace YAML {
template<>
struct convert<::object_storage_endpoint_param> {
    static bool decode(const Node& node, ::object_storage_endpoint_param& ep) {
        ep.endpoint = node["name"].as<std::string>();
        ep.config.port = node["port"].as<unsigned>();
        ep.config.use_https = node["https"].as<bool>(false);
        if (node["aws_region"] || std::getenv("AWS_DEFAULT_REGION")) {
            ep.config.aws.emplace();

            // https://github.com/scylladb/scylla-pkg/issues/3845
            // Allow picking up aws values via standard env vars as well.
            // Value in config has prio, but fall back to env.
            // This has the added benefit of potentially reducing the amount of
            // sensitive data in config files (i.e. credentials)
            auto get_node_value_or_env = [&](const char* key, const char* var) {
                auto child = node[key];
                if (child) {
                    return child.as<std::string>();
                }
                auto val = std::getenv(var);
                if (val) {
                    return std::string(val);
                }
                return std::string{};
            };
            ep.config.aws->region = get_node_value_or_env("aws_region", "AWS_DEFAULT_REGION");
            ep.config.aws->access_key_id = get_node_value_or_env("aws_access_key_id", "AWS_ACCESS_KEY_ID");
            ep.config.aws->secret_access_key = get_node_value_or_env("aws_secret_access_key", "AWS_SECRET_ACCESS_KEY");
            ep.config.aws->session_token = get_node_value_or_env("aws_session_token", "AWS_SESSION_TOKEN");
        }
        return true;
    }
};
}

static future<> read_object_storage_config(db::config& db_cfg) {
    sstring cfg_name;
    if (!db_cfg.object_storage_config_file().empty()) {
        cfg_name = db_cfg.object_storage_config_file();
    } else {
        cfg_name = db::config::get_conf_sub("object_storage.yaml").native();
        if (!co_await file_accessible(cfg_name, access_flags::exists)) {
            co_return;
        }
    }

    auto cfg_file = co_await open_file_dma(cfg_name, open_flags::ro);
    sstring data;
    std::exception_ptr ex;

    try {
        auto sz = co_await cfg_file.size();
        data = seastar::to_sstring(co_await cfg_file.dma_read_exactly<char>(0, sz));
    } catch (...) {
        ex = std::current_exception();
    }
    co_await cfg_file.close();
    if (ex) {
        co_await coroutine::return_exception_ptr(ex);
    }

    std::unordered_map<sstring, s3::endpoint_config> cfg;
    YAML::Node doc = YAML::Load(data.c_str());
    for (auto&& section : doc) {
        auto sec_name = section.first.as<std::string>();
        if (sec_name != "endpoints") {
            co_await coroutine::return_exception(std::runtime_error(fmt::format("While parsing object_storage config: section {} currently unsupported.", sec_name)));
        }

        auto endpoints = section.second.as<std::vector<object_storage_endpoint_param>>();
        for (auto&& ep : endpoints) {
            cfg[ep.endpoint] = std::move(ep.config);
        }
    }

    db_cfg.object_storage_config.set(std::move(cfg));
}

static future<>
read_config(bpo::variables_map& opts, db::config& cfg) {
    sstring file;

    if (opts.contains("options-file")) {
        file = opts["options-file"].as<sstring>();
    } else {
        file = db::config::get_conf_sub("scylla.yaml").string();
    }
    return check_direct_io_support(file).then([file, &cfg] {
        return cfg.read_from_file(file, [](auto & opt, auto & msg, auto status) {
            auto level = log_level::warn;
            if (auto value = status.value_or(db::config::value_status::Invalid);
                value != db::config::value_status::Invalid && value != db::config::value_status::Deprecated) {
                level = log_level::error;
            }
            startlog.log(level, "{} : {}", msg, opt);
        }).then([&cfg] {
            return read_object_storage_config(cfg);
        });
    }).handle_exception([file](auto ep) {
        startlog.error("Could not read configuration file {}: {}", file, ep);
        return make_exception_future<>(ep);
    });
}

#ifdef SCYLLA_ENABLE_ERROR_INJECTION
static future<>
enable_initial_error_injections(const db::config& cfg) {
    return smp::invoke_on_all([&cfg] {
        auto& injector = utils::get_local_injector();
        for (const auto& inj : cfg.error_injections_at_startup()) {
            injector.enable(inj.name, inj.one_shot, inj.parameters);
        }
    });
}
#endif

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
adjust_and_verify_rlimit(bool developer_mode) {
    struct rlimit lim;
    int r = getrlimit(RLIMIT_NOFILE, &lim);
    if (r == -1) {
        throw std::system_error(errno, std::system_category());
    }

    // First, try to increase the soft limit to the hard limit
    // Ref: http://0pointer.net/blog/file-descriptor-limits.html

    if (lim.rlim_cur < lim.rlim_max) {
        lim.rlim_cur = lim.rlim_max;
        r = setrlimit(RLIMIT_NOFILE, &lim);
        if (r == -1) {
            startlog.warn("adjusting RLIMIT_NOFILE failed with {}", std::system_error(errno, std::system_category()));
        }
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

static void tcp_timestamps_sanity() {
    try {
        auto f = file_desc::open("/proc/sys/net/ipv4/tcp_timestamps", O_RDONLY | O_CLOEXEC);
        char buf[128] = {};
        f.read(buf, 128);
        if (sstring(buf) == "0\n") {
            startlog.warn("sysctl entry net.ipv4.tcp_timestamps is set to 0.\n"
                          "To performance suffer less in a presence of packet loss, set following parameter on sysctl is strongly recommended:\n"
                          "net.ipv4.tcp_timestamps=1");
        }
    } catch (const std::system_error& e) {
        startlog.warn("Unable to check if net.ipv4.tcp_timestamps is set {}", e);
    }
}

static void
verify_seastar_io_scheduler(const boost::program_options::variables_map& opts, bool developer_mode) {
    auto note_bad_conf = [developer_mode] (sstring cause) {
        sstring msg = "I/O Scheduler is not properly configured! This is a non-supported setup, and performance is expected to be unpredictably bad.\n Reason found: "
                    + cause + "\n"
                    + "To properly configure the I/O Scheduler, run the scylla_io_setup utility shipped with Scylla.\n";

        sstring devmode_msg = msg + "To ignore this, see the developer-mode configuration option.";
        if (developer_mode) {
            startlog.warn("{}", msg.c_str());
        } else {
            startlog.error("{}", devmode_msg.c_str());
            throw std::runtime_error("Bad I/O Scheduler configuration");
        }
    };

    if (!(opts.contains("io-properties") || opts.contains("io-properties-file"))) {
        note_bad_conf("none of --io-properties and --io-properties-file are set.");
    }
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
    fmt::print("pid: {}\n", getpid());

    fmt::print("parsed command line options: {}\n", format_parsed_options(opts.options));
}

template <typename Func>
static auto defer_verbose_shutdown(const char* what, Func&& func) {
    auto vfunc = [what, func = std::forward<Func>(func)] () mutable {
        startlog.info("Shutting down {}", what);
        try {
            func();
            startlog.info("Shutting down {} was successful", what);
        } catch (...) {
            auto ex = std::current_exception();
            bool do_abort = true;
            try {
                std::rethrow_exception(ex);
            } catch (const std::system_error& e) {
                // System error codes we consider "environmental",
                // i.e. not scylla's fault, therefore there is no point in
                // aborting and dumping core.
                for (int i : {EIO, EACCES, EDQUOT, ENOSPC}) {
                    if (e.code() == std::error_code(i, std::system_category())) {
                        do_abort = false;
                        break;
                    }
                }
            } catch (const storage_io_error& e) {
                do_abort = false;
            } catch (...) {
            }
            auto msg = fmt::format("Unexpected error shutting down {}: {}", what, ex);
            if (do_abort) {
                startlog.error("{}: aborting", msg);
                abort();
            } else {
                startlog.error("{}: exiting, at {}", msg, current_backtrace());

                // Call _exit() rather than exit() to exit immediately
                // without calling exit handlers, avoiding
                // boost::intrusive::detail::destructor_impl SCYLLA_ASSERT failure
                // from ~segment_pool exit handler.
                _exit(255);
            }
        }
    };

    auto ret = deferred_action(std::move(vfunc));
    return ::make_shared<decltype(ret)>(std::move(ret));
}

namespace debug {
sharded<netw::messaging_service>* the_messaging_service;
sharded<cql3::query_processor>* the_query_processor;
sharded<qos::service_level_controller>* the_sl_controller;
sharded<service::migration_manager>* the_migration_manager;
sharded<service::storage_service>* the_storage_service;
sharded<streaming::stream_manager> *the_stream_manager;
sharded<gms::feature_service> *the_feature_service;
sharded<gms::gossiper> *the_gossiper;
sharded<locator::snitch_ptr> *the_snitch;
sharded<service::storage_proxy> *the_storage_proxy;
}

// This is used by perf-alternator to allow running scylla together with the tool
// in a single process. So that it's easier to measure internals. It's not added
// to main_func_type to not complicate common flow as no other tool needs such logic.
std::function<void(lw_shared_ptr<db::config>)> after_init_func;

static locator::host_id initialize_local_info_thread(sharded<db::system_keyspace>& sys_ks,
        sharded<locator::snitch_ptr>& snitch,
        const gms::inet_address& listen_address,
        const db::config& cfg,
        gms::inet_address broadcast_address,
        gms::inet_address broadcast_rpc_address)
{
    auto linfo = sys_ks.local().load_local_info().get();
    if (linfo.cluster_name.empty()) {
        linfo.cluster_name = cfg.cluster_name();
    } else if (linfo.cluster_name != cfg.cluster_name()) {
        throw exceptions::configuration_exception("Saved cluster name " + linfo.cluster_name + " != configured name " + cfg.cluster_name());
    }
    if (!linfo.host_id) {
        linfo.host_id = locator::host_id::create_random_id();
        startlog.info("Setting local host id to {}", linfo.host_id);
    }

    linfo.listen_address = listen_address;
    const auto host_id = linfo.host_id;
    sys_ks.local().save_local_info(std::move(linfo), snitch.local()->get_location(), broadcast_address, broadcast_rpc_address).get();
    return host_id;
}

extern "C" void __attribute__((weak)) __llvm_profile_dump();

[[gnu::noinline]]
void dump_performance_profiles() {
    if (__llvm_profile_dump) {
        startlog.info("Calling __llvm_profile_dump()");
        __llvm_profile_dump();
    }
}

static int scylla_main(int ac, char** av) {
    // Allow core dumps. The would be disabled by default if
    // CAP_SYS_NICE was added to the binary, as is suggested by the
    // epoll backend.
    int r = prctl(PR_SET_DUMPABLE, 1, 0, 0, 0);
    if (r) {
        std::cerr << "Could not make scylla dumpable\n";
        exit(1);
    }

  try {
    runtime::init_uptime();
    std::setvbuf(stdout, nullptr, _IOLBF, 1000);
    app_template::config app_cfg;
    app_cfg.name = "Scylla";
    app_cfg.description =
R"(scylla - NoSQL data store using the seastar framework

For more information, see https://github.com/scylladb/scylla.

The scylla executable hosts tools in addition to the main scylla server, these
can be invoked as: scylla {tool_name} [...]

For a list of available tools, run: scylla --list-tools
For more information about individual tools, run: scylla {tool_name} --help

To start the scylla server proper, simply invoke as: scylla server (or just scylla).
)";
    app_cfg.default_task_quota = 500us;
#ifdef DEBUG
    // Increase the task quota to improve work:poll ratio in slow debug mode.
    app_cfg.default_task_quota = 5ms;
#endif
    app_cfg.auto_handle_sigint_sigterm = false;
    app_cfg.max_networking_aio_io_control_blocks = 50000;
    // We need to have the entire app config to run the app, but we need to
    // run the app to read the config file with UDF specific options so that
    // we know whether we need to reserve additional memory for UDFs.
    app_cfg.reserve_additional_memory_per_shard = db::config::wasm_udf_reserved_memory;
    app_template app(std::move(app_cfg));

    auto ext = std::make_shared<db::extensions>();
    ext->add_schema_extension<db::tags_extension>(db::tags_extension::NAME);
    ext->add_schema_extension<cdc::cdc_extension>(cdc::cdc_extension::NAME);
    ext->add_schema_extension<db::paxos_grace_seconds_extension>(db::paxos_grace_seconds_extension::NAME);
    ext->add_schema_extension<tombstone_gc_extension>(tombstone_gc_extension::NAME);
    ext->add_schema_extension<db::per_partition_rate_limit_extension>(db::per_partition_rate_limit_extension::NAME);

    auto cfg = make_lw_shared<db::config>(ext);
    auto init = app.get_options_description().add_options();

    init("version", bpo::bool_switch(), "print version number and exit");
    init("build-id", bpo::bool_switch(), "print build-id and exit");
    init("build-mode", bpo::bool_switch(), "print build mode and exit");
    init("list-tools", bpo::bool_switch(), "list included tools and exit");

    bpo::options_description deprecated_options("Deprecated options - ignored");
    auto deprecated_options_easy_init = deprecated_options.add_options();
    cfg->add_deprecated_options(deprecated_options_easy_init);
    app.get_options_description().add(deprecated_options);

    // TODO : default, always read?
    init("options-file", bpo::value<sstring>(), "configuration file (i.e. <SCYLLA_HOME>/conf/scylla.yaml)");

    configurable::append_all(*cfg, init);
    cfg->add_options(init);

    // If --version is requested, print it out and exit immediately to avoid
    // Seastar-specific warnings that may occur when running the app
    if (!isatty(fileno(stdin))) {
        auto parsed_opts = bpo::command_line_parser(ac, av).options(app.get_options_description()).allow_unregistered().run();
        print_starting_message(ac, av, parsed_opts);
    }

    sharded<locator::shared_token_metadata> token_metadata;
    sharded<locator::effective_replication_map_factory> erm_factory;
    sharded<service::migration_notifier> mm_notifier;
    sharded<service::endpoint_lifecycle_notifier> lifecycle_notifier;
    sharded<compaction_manager> cm;
    sharded<sstables::storage_manager> sstm;
    distributed<replica::database> db;
    seastar::sharded<service::cache_hitrate_calculator> cf_cache_hitrate_calculator;
    service::load_meter load_meter;
    sharded<service::storage_proxy> proxy;
    sharded<service::storage_service> ss;
    sharded<service::migration_manager> mm;
    sharded<tasks::task_manager> task_manager;
    api::http_context ctx(db);
    httpd::http_server_control prometheus_server;
    std::optional<utils::directories> dirs = {};
    sharded<gms::feature_service> feature_service;
    sharded<db::snapshot_ctl> snapshot_ctl;
    sharded<netw::messaging_service> messaging;
    sharded<cql3::query_processor> qp;
    sharded<db::batchlog_manager> bm;
    sharded<sstables::directory_semaphore> sst_dir_semaphore;
    sharded<service::raft_group_registry> raft_gr;
    sharded<service::memory_limiter> service_memory_limiter;
    sharded<repair_service> repair;
    sharded<sstables_loader> sst_loader;
    sharded<streaming::stream_manager> stream_manager;
    sharded<service::mapreduce_service> mapreduce_service;
    sharded<gms::gossiper> gossiper;
    sharded<locator::snitch_ptr> snitch;

    return app.run(ac, av, [&] () -> future<int> {

        auto&& opts = app.configuration();

        namespace sm = seastar::metrics;
        app_metrics.add_group("scylladb", {
            sm::make_gauge("current_version", sm::description("Current ScyllaDB version."), { sm::label_instance("version", scylla_version()), sm::shard_label("") }, [] { return 0; })
        });

        for (auto& opt: deprecated_options.options()) {
            if (opts.contains(opt->long_name())) {
                startlog.warn("{} option ignored (deprecated)", opt->long_name());
            }
        }

        // Check developer mode before even reading the config file, because we may not be
        // able to read it if we need to disable strict dma mode.
        // We'll redo this later and apply it to all reactors.
        if (opts.contains("developer-mode")) {
            engine().set_strict_dma(false);
        }

        tcp_syncookies_sanity();
        tcp_timestamps_sanity();

        return seastar::async([&app, cfg, ext, &cm, &sstm, &db, &qp, &bm, &proxy, &mapreduce_service, &mm, &mm_notifier, &ctx, &opts, &dirs,
                &prometheus_server, &cf_cache_hitrate_calculator, &load_meter, &feature_service, &gossiper, &snitch,
                &token_metadata, &erm_factory, &snapshot_ctl, &messaging, &sst_dir_semaphore, &raft_gr, &service_memory_limiter,
                &repair, &sst_loader, &ss, &lifecycle_notifier, &stream_manager, &task_manager] {
          try {
              if (opts.contains("relabel-config-file") && !opts["relabel-config-file"].as<sstring>().empty()) {
                  // calling update_relabel_config_from_file can cause an exception that would stop startup
                  // that's on purpose, it means the configuration is broken and needs to be fixed
                  utils::update_relabel_config_from_file(opts["relabel-config-file"].as<sstring>()).get();
              }
            // disable reactor stall detection during startup
            auto blocked_reactor_notify_ms = engine().get_blocked_reactor_notify_ms();
            smp::invoke_on_all([] {
                engine().update_blocked_reactor_notify_ms(10000h);
            }).get();

            ::stop_signal stop_signal; // we can move this earlier to support SIGINT during initialization
            read_config(opts, *cfg).get();
#ifdef SCYLLA_ENABLE_ERROR_INJECTION
            enable_initial_error_injections(*cfg).get();
#endif
            auto notify_set = configurable::init_all(opts, *cfg, *ext, service_set(
                db, ss, mm, proxy, feature_service, messaging, qp, bm
            )).get();

            auto stop_configurables = defer_verbose_shutdown("configurables", [&] {
                notify_set.notify_all(configurable::system_state::stopped).get();
            });

            cfg->setup_directories();

            // We're writing to a non-atomic variable here. But bool writes are atomic
            // in all supported architectures, and the broadcast_to_all_shards().get() below
            // will apply the required memory barriers anyway.
            ser::gc_clock_using_3_1_0_serialization = cfg->enable_3_1_0_compatibility_mode();

            cfg->broadcast_to_all_shards().get();

            // We pass this piece of config through a global as a temporary hack.
            // See the comment at the definition of sstables::global_cache_index_pages.
            smp::invoke_on_all([&cfg] {
                sstables::global_cache_index_pages = cfg->cache_index_pages.operator utils::updateable_value<bool>();
            }).get();

            ::sighup_handler sighup_handler(opts, *cfg);
            auto stop_sighup_handler = defer_verbose_shutdown("sighup", [&] {
                sighup_handler.stop().get();
            });

            logalloc::prime_segment_pool(memory::stats().total_memory(), memory::min_free_memory()).get();
            logging::apply_settings(cfg->logging_settings(app.options().log_opts));

            startlog.info(startup_msg, scylla_version(), get_build_id());

            // Set the default scheduling_group, i.e., the main scheduling
            // group to a lower shares. Subsystems needs higher shares
            // should set it explicitly. This prevents code that is supposed to
            // run inside its own scheduling group leaking to main group and
            // causing latency issues.
            smp::invoke_on_all([] {
                auto default_sg = default_scheduling_group();
                default_sg.set_shares(200);
            }).get();

            adjust_and_verify_rlimit(cfg->developer_mode());
            verify_adequate_memory_per_shard(cfg->developer_mode());
            verify_seastar_io_scheduler(opts, cfg->developer_mode());
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

            auto unused_features = cfg->experimental_features() | boost::adaptors::filtered([] (auto& f) {
                return f == db::experimental_features_t::feature::UNUSED;
            });
            if (!unused_features.empty()) {
                startlog.warn("Ignoring unused features found in config: {}", unused_features);
            }

            gms::feature_config fcfg = gms::feature_config_from_db_config(*cfg);

            debug::the_feature_service = &feature_service;
            feature_service.start(fcfg).get();
            // FIXME storage_proxy holds a reference on it and is not yet stopped.
            // also the proxy leaves range_slice_read_executor-s hanging around
            // and willing to find out if the cluster_supports_digest_multipartition_reads
            //
            //auto stop_feature_service = defer_verbose_shutdown("feature service", [&feature_service] {
            //    feature_service.stop().get();
            //});

            schema::set_default_partitioner(cfg->partitioner(), cfg->murmur3_partitioner_ignore_msb_bits());
            auto make_sched_group = [&] (sstring name, sstring short_name, unsigned shares) {
                if (cfg->cpu_scheduler()) {
                    return seastar::create_scheduling_group(name, short_name, shares).get();
                } else {
                    return seastar::scheduling_group();
                }
            };
            auto background_reclaim_scheduling_group = make_sched_group("background_reclaim", "bgre", 50);
            auto maintenance_scheduling_group = make_sched_group("streaming", "strm", 200);

            smp::invoke_on_all([&cfg, background_reclaim_scheduling_group] {
                logalloc::tracker::config st_cfg;
                st_cfg.defragment_on_idle = cfg->defragment_memory_on_idle();
                st_cfg.abort_on_lsa_bad_alloc = cfg->abort_on_lsa_bad_alloc();
                st_cfg.lsa_reclamation_step = cfg->lsa_reclamation_step();
                st_cfg.background_reclaim_sched_group = background_reclaim_scheduling_group;
                st_cfg.sanitizer_report_backtrace = cfg->sanitizer_report_backtrace();
                logalloc::shard_tracker().configure(st_cfg);
            }).get();

            auto stop_lsa_background_reclaim = defer([&] () noexcept {
                smp::invoke_on_all([&] {
                    return logalloc::shard_tracker().stop();
                }).get();
            });

            if (cfg->broadcast_address().empty() && cfg->listen_address().empty()) {
                startlog.error("Bad configuration: neither listen_address nor broadcast_address are defined\n");
                throw bad_configuration_error();
            }

            if (cfg->broadcast_rpc_address().empty() && cfg->rpc_address() == "0.0.0.0") {
                startlog.error("If rpc_address is set to a wildcard address {}, then you must set broadcast_rpc_address", cfg->rpc_address());
                throw bad_configuration_error();
            }

            // We want to ensure a node is zero-token if and only if join_ring=false, so all the logic can rely on it.
            if (cfg->join_ring() && cfg->num_tokens() == 0 && cfg->initial_token().empty()) {
                startlog.error(
                        "Bad configuration: cannot join the ring with zero tokens. If you do not want the node "
                        "to join the ring, use join-ring=false");
                throw bad_configuration_error();
            }

            auto preferred = cfg->listen_interface_prefer_ipv6() ? std::make_optional(net::inet_address::family::INET6) : std::nullopt;
            auto family = cfg->enable_ipv6_dns_lookup() || preferred ? std::nullopt : std::make_optional(net::inet_address::family::INET);

            auto broadcast_addr = utils::resolve(cfg->broadcast_address || cfg->listen_address, family, preferred).get();
            auto broadcast_rpc_addr = utils::resolve(cfg->broadcast_rpc_address || cfg->rpc_address, family, preferred).get();

            ctx.api_dir = cfg->api_ui_dir();
            if (!ctx.api_dir.empty() && ctx.api_dir.back() != '/') {
                // The api_dir should end with a backslash, add it if it's missing
                ctx.api_dir.append("/", 1);
            }
            ctx.api_doc = cfg->api_doc_dir();
            if (!ctx.api_doc.empty() && ctx.api_doc.back() != '/') {
                // The api_doc should end with a backslash, add it if it's missing
                ctx.api_doc.append("/", 1);
            }
            const auto hinted_handoff_enabled = cfg->hinted_handoff_enabled();

            auto api_addr = utils::resolve(cfg->api_address || cfg->rpc_address, family, preferred).get();
            supervisor::notify("starting API server");
            ctx.http_server.start("API").get();
            auto stop_http_server = defer_verbose_shutdown("API server", [&ctx] {
                ctx.http_server.stop().get();
            });
            api::set_server_init(ctx).get();

            supervisor::notify("starting prometheus API server");
            std::any stop_prometheus;
            if (cfg->prometheus_port()) {
                prometheus_server.start("prometheus").get();
                stop_prometheus = defer_verbose_shutdown("prometheus API server", [&prometheus_server] {
                    prometheus_server.stop().get();
                });

                auto ip = utils::resolve(cfg->prometheus_address || cfg->listen_address, family, preferred).get();

                prometheus::config pctx;
                pctx.metric_help = "Scylla server statistics";
                pctx.prefix = cfg->prometheus_prefix();
                pctx.allow_protobuf = cfg->prometheus_allow_protobuf();
                prometheus::start(prometheus_server, pctx).get();
                with_scheduling_group(maintenance_scheduling_group, [&] {
                  return prometheus_server.listen(socket_address{ip, cfg->prometheus_port()}).handle_exception([&ip, &cfg] (auto ep) {
                    startlog.error("Could not start Prometheus API server on {}:{}: {}", ip, cfg->prometheus_port(), ep);
                    return make_exception_future<>(ep);
                  });
                }).get();
            }

            using namespace locator;
            // Re-apply strict-dma after we've read the config file, this time
            // to all reactors
            if (opts.contains("developer-mode")) {
                smp::invoke_on_all([] { engine().set_strict_dma(false); }).get();
            }

            auto abort_on_internal_error_observer = cfg->abort_on_internal_error.observe([] (bool val) {
                set_abort_on_internal_error(val);
            });
            set_abort_on_internal_error(cfg->abort_on_internal_error());

            supervisor::notify("creating snitch");
            debug::the_snitch = &snitch;
            snitch_config snitch_cfg;
            snitch_cfg.name = cfg->endpoint_snitch();
            snitch_cfg.listen_address = utils::resolve(cfg->listen_address, family).get();
            snitch_cfg.broadcast_address = broadcast_addr;
            snitch.start(snitch_cfg).get();
            auto stop_snitch = defer_verbose_shutdown("snitch", [&snitch] {
                snitch.stop().get();
            });
            snitch.invoke_on_all(&locator::snitch_ptr::start).get();
            // #293 - do not stop anything (unless snitch.on_all(start) fails)
            stop_snitch->cancel();

            api::set_server_snitch(ctx, snitch).get();
            auto stop_snitch_api = defer_verbose_shutdown("snitch API", [&ctx] {
                api::unset_server_snitch(ctx).get();
            });

            if (auto opt_public_address = snitch.local()->get_public_address()) {
                // Use the Public IP as broadcast_address to other nodes
                // and the broadcast_rpc_address (for client CQL connections).
                //
                // Cassandra 2.1 manual explicitly instructs to set broadcast_address
                // value to a public address in cassandra.yaml.
                //
                broadcast_addr = *opt_public_address;
                if (cfg->broadcast_rpc_address().empty()) {
                    broadcast_rpc_addr = *opt_public_address;
                }
            }

            supervisor::notify("starting tokens manager");
            locator::token_metadata::config tm_cfg;
            tm_cfg.topo_cfg.this_endpoint = broadcast_addr;
            tm_cfg.topo_cfg.this_cql_address = broadcast_rpc_addr;
            tm_cfg.topo_cfg.local_dc_rack = snitch.local()->get_location();
            if (snitch.local()->get_name() == "org.apache.cassandra.locator.SimpleSnitch") {
                //
                // Simple snitch wants sort_by_proximity() not to reorder nodes anyhow
                //
                // "Making all endpoints equal ensures we won't change the original
                // ordering." - quote from C* code.
                //
                // The snitch_base implementation should handle the above case correctly.
                // I'm leaving the this implementation anyway since it's the C*'s
                // implementation and some installations may depend on it.
                //
                tm_cfg.topo_cfg.disable_proximity_sorting = true;
            }
            token_metadata.start([] () noexcept { return db::schema_tables::hold_merge_lock(); }, tm_cfg).get();
            token_metadata.invoke_on_all([&] (auto& stm) {
                stm.set_stall_detector_threshold(
                        std::chrono::duration_cast<std::chrono::steady_clock::duration>(
                                std::chrono::duration<double>(cfg->topology_barrier_stall_detector_threshold_seconds())));
            }).get();
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

            api::set_server_token_metadata(ctx, token_metadata).get();
            auto stop_tokens_api = defer_verbose_shutdown("token metadata API", [&ctx] {
                api::unset_server_token_metadata(ctx).get();
            });


            supervisor::notify("starting effective_replication_map factory");
            erm_factory.start().get();
            auto stop_erm_factory = deferred_stop(erm_factory);

            supervisor::notify("starting migration manager notifier");
            mm_notifier.start().get();
            auto stop_mm_notifier = defer_verbose_shutdown("migration manager notifier", [ &mm_notifier ] {
                mm_notifier.stop().get();
            });

            supervisor::notify("starting per-shard database core");

            sst_dir_semaphore.start(cfg->initial_sstable_loading_concurrency()).get();
            auto stop_sst_dir_sem = defer_verbose_shutdown("sst_dir_semaphore", [&sst_dir_semaphore] {
                sst_dir_semaphore.stop().get();
            });

            service_memory_limiter.start(memory::stats().total_memory()).get();
            auto stop_mem_limiter = defer_verbose_shutdown("service_memory_limiter", [] {
                // Uncomment this once services release all the memory on stop
                // service_memory_limiter.stop().get();
            });

            supervisor::notify("creating and verifying directories");
            utils::directories::set dir_set;
            dir_set.add(cfg->commitlog_directory());
            dir_set.add(cfg->schema_commitlog_directory());
            dirs.emplace(cfg->developer_mode());
            dirs->create_and_verify(std::move(dir_set)).get();

            // The data directories are handled separately to prevent memory
            // fragmentation in the dentry/inode cache. The top-level
            // directories are verified first, and then the contents are
            // verified in such a way that the files that will be closed
            // immediately are located separately from those that will remain
            // open for a longer duration in the dentry/inode cache.
            utils::directories::set data_dir_set;
            data_dir_set.add(cfg->data_file_directories());
            dirs->create_and_verify(data_dir_set, utils::directories::recursive::no).get();
            utils::directories::verify_owner_and_mode_of_data_dir(std::move(data_dir_set)).get();

            auto hints_dir_initializer = db::hints::directory_initializer::make(*dirs, cfg->hints_directory()).get();
            auto view_hints_dir_initializer = db::hints::directory_initializer::make(*dirs, cfg->view_hints_directory()).get();
            if (!hinted_handoff_enabled.is_disabled_for_all()) {
                hints_dir_initializer.ensure_created_and_verified().get();
            }
            view_hints_dir_initializer.ensure_created_and_verified().get();

            auto get_tm_cfg = sharded_parameter([&] {
                return tasks::task_manager::config {
                    .task_ttl = cfg->task_ttl_seconds,
                    .broadcast_address = broadcast_addr
                };
            });
            task_manager.start(std::move(get_tm_cfg), std::ref(stop_signal.as_sharded_abort_source())).get();
            auto stop_task_manager = defer_verbose_shutdown("task_manager", [&task_manager] {
                task_manager.stop().get();
            });

            api::set_server_task_manager(ctx, task_manager, cfg).get();
            auto stop_tm_api = defer_verbose_shutdown("task manager API", [&ctx] {
                api::unset_server_task_manager(ctx).get();
            });
#ifndef SCYLLA_BUILD_MODE_RELEASE
            api::set_server_task_manager_test(ctx, task_manager).get();
            auto stop_tm_test_api = defer_verbose_shutdown("task manager API", [&ctx] {
                api::unset_server_task_manager_test(ctx).get();
            });
#endif

            // Note: changed from using a move here, because we want the config object intact.
            replica::database_config dbcfg;
            dbcfg.compaction_scheduling_group = make_sched_group("compaction", "comp", 1000);
            dbcfg.memory_compaction_scheduling_group = make_sched_group("mem_compaction", "mcmp", 1000);
            dbcfg.streaming_scheduling_group = maintenance_scheduling_group;
            dbcfg.statement_scheduling_group = make_sched_group("statement", "stmt", 1000);
            dbcfg.memtable_scheduling_group = make_sched_group("memtable", "mt", 1000);
            dbcfg.memtable_to_cache_scheduling_group = make_sched_group("memtable_to_cache", "mt2c", 200);
            dbcfg.gossip_scheduling_group = make_sched_group("gossip", "gms", 1000);
            dbcfg.commitlog_scheduling_group = make_sched_group("commitlog", "clog", 1000);
            dbcfg.schema_commitlog_scheduling_group = make_sched_group("schema_commitlog", "sclg", 1000);
            dbcfg.available_memory = memory::stats().total_memory();

            // Make sure to initialize the scheduling group keys at a point where we are sure
            // that nobody will be creating scheduling groups (e.g. service levels controller can do that).
            // Creating a scheduling group and scheduling group key concurrently
            // doesn't work properly. See: https://github.com/scylladb/seastar/issues/2231
            scheduling_group_key_config maintenance_cql_sg_stats_cfg =
            make_scheduling_group_key_config<cql_transport::cql_sg_stats>(maintenance_socket_enabled::yes);
            auto maintenance_cql_sg_stats_key = scheduling_group_key_create(maintenance_cql_sg_stats_cfg).get();
            scheduling_group_key_config cql_sg_stats_cfg = make_scheduling_group_key_config<cql_transport::cql_sg_stats>(maintenance_socket_enabled::no);
            auto cql_sg_stats_key = scheduling_group_key_create(cql_sg_stats_cfg).get();

            supervisor::notify("starting compaction_manager");
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
            cm.start(std::move(get_cm_cfg), std::ref(stop_signal.as_sharded_abort_source()), std::ref(task_manager)).get();
            auto stop_cm = defer_verbose_shutdown("compaction_manager", [&cm] {
               cm.stop().get();
            });

            sstables::storage_manager::config stm_cfg;
            stm_cfg.s3_clients_memory = std::clamp<size_t>(memory::stats().total_memory() * 0.01, 10 << 20, 100 << 20);
            sstm.start(std::ref(*cfg), stm_cfg).get();
            auto stop_sstm = defer_verbose_shutdown("sstables storage manager", [&sstm] {
                sstm.stop().get();
            });

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

            static sharded<lang::manager> langman;
            langman.start(lang_config).get();
            // don't stop for real until query_processor stops
            auto stop_lang_man = defer_verbose_shutdown("lang manager", [] { langman.invoke_on_all(&lang::manager::stop).get(); });
            langman.invoke_on_all(&lang::manager::start).get();

            supervisor::notify("starting database");
            debug::the_database = &db;
            db.start(std::ref(*cfg), dbcfg, std::ref(mm_notifier), std::ref(feature_service), std::ref(token_metadata),
                    std::ref(cm), std::ref(sstm), std::ref(langman), std::ref(sst_dir_semaphore), std::ref(stop_signal.as_sharded_abort_source()), utils::cross_shard_barrier()).get();
            auto stop_database_and_sstables = defer_verbose_shutdown("database", [&db] {
                // #293 - do not stop anything - not even db (for real)
                //return db.stop();
                // call stop on each db instance, but leave the shareded<database> pointers alive.
                db.invoke_on_all(&replica::database::stop).get();
            });

            // We need to init commitlog on shard0 before it is inited on other shards
            // because it obtains the list of pre-existing segments for replay, which must
            // not include reserve segments created by active commitlogs.
            db.local().init_commitlog().get();
            db.invoke_on_all(&replica::database::start).get();

// FIXME: The stall detector uses glibc backtrace function to
// collect backtraces, this causes ASAN failures on ARM.
// For now we just disable the stall detector in this configuration,
// the ticket about migrating to libunwind: scylladb/seastar#1878
#if !defined(SEASTAR_DEBUG) || !defined(__aarch64__)
            smp::invoke_on_all([blocked_reactor_notify_ms] {
                engine().update_blocked_reactor_notify_ms(blocked_reactor_notify_ms);
            }).get();
#else
            (void)blocked_reactor_notify_ms;
#endif
            debug::the_storage_proxy = &proxy;
            supervisor::notify("starting storage proxy");
            service::storage_proxy::config spcfg {
                .hints_directory_initializer = hints_dir_initializer,
            };
            spcfg.hinted_handoff_enabled = hinted_handoff_enabled;
            spcfg.available_memory = memory::stats().total_memory();
            smp_service_group_config storage_proxy_smp_service_group_config;
            // Assuming less than 1kB per queued request, this limits storage_proxy submit_to() queues to 5MB or less
            storage_proxy_smp_service_group_config.max_nonlocal_requests = 5000;
            spcfg.read_smp_service_group = create_smp_service_group(storage_proxy_smp_service_group_config).get();
            spcfg.write_smp_service_group = create_smp_service_group(storage_proxy_smp_service_group_config).get();
            spcfg.write_mv_smp_service_group = create_smp_service_group(storage_proxy_smp_service_group_config).get();
            spcfg.hints_write_smp_service_group = create_smp_service_group(storage_proxy_smp_service_group_config).get();
            spcfg.write_ack_smp_service_group = create_smp_service_group(storage_proxy_smp_service_group_config).get();
            static db::view::node_update_backlog node_backlog(smp::count, 10ms);
            scheduling_group_key_config storage_proxy_stats_cfg =
                    make_scheduling_group_key_config<service::storage_proxy_stats::stats>();
            storage_proxy_stats_cfg.constructor = [plain_constructor = storage_proxy_stats_cfg.constructor] (void* ptr) {
                plain_constructor(ptr);
                reinterpret_cast<service::storage_proxy_stats::stats*>(ptr)->register_stats();
                reinterpret_cast<service::storage_proxy_stats::stats*>(ptr)->register_split_metrics_local();
            };
            storage_proxy_stats_cfg.rename = [] (void* ptr) {
                reinterpret_cast<service::storage_proxy_stats::stats*>(ptr)->register_stats();
                reinterpret_cast<service::storage_proxy_stats::stats*>(ptr)->register_split_metrics_local();
            };
            proxy.start(std::ref(db), spcfg, std::ref(node_backlog),
                    scheduling_group_key_create(storage_proxy_stats_cfg).get(),
                    std::ref(feature_service), std::ref(token_metadata), std::ref(erm_factory)).get();

            // #293 - do not stop anything
            // engine().at_exit([&proxy] { return proxy.stop(); });
            api::set_server_storage_proxy(ctx, proxy).get();
            auto stop_sp_api = defer_verbose_shutdown("storage proxy API", [&ctx] {
                api::unset_server_storage_proxy(ctx).get();
            });

            static sharded<cql3::cql_config> cql_config;
            cql_config.start(std::ref(*cfg)).get();

            supervisor::notify("starting query processor");
            cql3::query_processor::memory_config qp_mcfg = {memory::stats().total_memory() / 256, memory::stats().total_memory() / 2560};
            debug::the_query_processor = &qp;
            auto local_data_dict = seastar::sharded_parameter([] (const replica::database& db) { return db.as_data_dictionary(); }, std::ref(db));

            utils::loading_cache_config auth_prep_cache_config;
            auth_prep_cache_config.max_size = qp_mcfg.authorized_prepared_cache_size;
            auth_prep_cache_config.expiry = std::min(std::chrono::milliseconds(cfg->permissions_validity_in_ms()),
                                                     std::chrono::duration_cast<std::chrono::milliseconds>(cql3::prepared_statements_cache::entry_expiry));
            auth_prep_cache_config.refresh = std::chrono::milliseconds(cfg->permissions_update_interval_in_ms());

            qp.start(std::ref(proxy), std::move(local_data_dict), std::ref(mm_notifier), qp_mcfg, std::ref(cql_config), std::move(auth_prep_cache_config), std::ref(langman)).get();

            supervisor::notify("starting lifecycle notifier");
            lifecycle_notifier.start().get();
            auto stop_lifecycle_notifier = defer_verbose_shutdown("lifecycle notifier", [ &lifecycle_notifier ] {
                lifecycle_notifier.stop().get();
            });

            supervisor::notify("creating tracing");
            sharded<tracing::tracing>& tracing = tracing::tracing::tracing_instance();
            tracing.start(sstring("trace_keyspace_helper")).get();
            auto destroy_tracing = defer_verbose_shutdown("tracing instance", [&tracing] {
                tracing.stop().get();
            });

            with_scheduling_group(maintenance_scheduling_group, [&] {
                return ctx.http_server.listen(socket_address{api_addr, cfg->api_port()});
            }).get();
            startlog.info("Scylla API server listening on {}:{} ...", api_addr, cfg->api_port());

            api::set_server_config(ctx, *cfg).get();
            auto stop_config_api = defer_verbose_shutdown("config API", [&ctx] {
                api::unset_server_config(ctx).get();
            });

            static sharded<auth::service> auth_service;
            static sharded<auth::service> maintenance_auth_service;
            static sharded<qos::service_level_controller> sl_controller;
            debug::the_sl_controller = &sl_controller;

            //starting service level controller
            qos::service_level_options default_service_level_configuration;
            sl_controller.start(std::ref(auth_service), std::ref(token_metadata), std::ref(stop_signal.as_sharded_abort_source()), default_service_level_configuration).get();
            sl_controller.invoke_on_all(&qos::service_level_controller::start).get();
            auto stop_sl_controller = defer_verbose_shutdown("service level controller", [] {
                sl_controller.stop().get();
            });

            static sharded<db::system_distributed_keyspace> sys_dist_ks;
            static sharded<db::system_keyspace> sys_ks;
            static sharded<db::view::view_update_generator> view_update_generator;
            static sharded<db::view::view_builder> view_builder;
            static sharded<cdc::generation_service> cdc_generation_service;

            db::sstables_format_selector sst_format_selector(db);

            supervisor::notify("starting system keyspace");
            sys_ks.start(std::ref(qp), std::ref(db)).get();
            // TODO: stop()?

            // Initialization of a keyspace is done by shard 0 only. For system
            // keyspace, the procedure  will go through the hardcoded column
            // families, and in each of them, it will load the sstables for all
            // shards using distributed database object.
            // Iteration through column family directory for sstable loading is
            // done only by shard 0, so we'll no longer face race conditions as
            // described here: https://github.com/scylladb/scylla/issues/1014
            supervisor::notify("loading system sstables");
            replica::distributed_loader::init_system_keyspace(sys_ks, erm_factory, db).get();

            // 1. Here we notify dependent services that system tables have been loaded,
            //    and they in turn can load the necessary data from them;
            // 2. This notification is important for the services that are needed
            //    during schema commitlog replay;
            // 3. The data this services load should be written with accompanying
            //    table.flush, since commitlog is not replayed yet;
            // 4. This is true for the following services:
            //   * features_service: we need to re-enable previously enabled features,
            //     this should be done before commitlog starts replaying
            //     since some features affect storage.
            //   * sstables_format_selector: we need to choose the appropriate format,
            //     since schema commitlog replay can write to sstables.
            when_all_succeed(feature_service.local().on_system_tables_loaded(sys_ks.local()),
                sst_format_selector.on_system_tables_loaded(sys_ks.local())).get();

            db.local().init_schema_commitlog();

            // Mark all the system tables writable and assign the proper commitlog to them.
            sys_ks.invoke_on_all(&db::system_keyspace::mark_writable).get();

            auto sch_cl = db.local().schema_commitlog();
            if (sch_cl != nullptr) {
              auto paths = sch_cl->get_segments_to_replay().get();
              if (!paths.empty()) {
                  supervisor::notify("replaying schema commit log");
                  auto rp = db::commitlog_replayer::create_replayer(db, sys_ks).get();
                  rp.recover(paths, db::schema_tables::COMMITLOG_FILENAME_PREFIX).get();
                  supervisor::notify("replaying schema commit log - flushing memtables");
                  // The schema commitlog lives only on the null shard.
                  // This is enforced when the table is marked to use
                  // it - schema_static_props::enable_schema_commitlog function
                  // also sets the use_null_sharder property.
                  // This means only the local memtables need to be flushed.
                  db.local().flush_all_memtables().get();
                  supervisor::notify("replaying schema commit log - removing old commitlog segments");
                  //FIXME: discarded future
                  (void)sch_cl->delete_segments(std::move(paths));
              }
            }

            sys_ks.local().build_bootstrap_info().get();

            const auto listen_address = utils::resolve(cfg->listen_address, family).get();
            const auto host_id = initialize_local_info_thread(sys_ks, snitch, listen_address, *cfg, broadcast_addr, broadcast_rpc_addr);

          shared_token_metadata::mutate_on_all_shards(token_metadata, [host_id, endpoint = broadcast_addr] (locator::token_metadata& tm) {
              // Makes local host id available in topology cfg as soon as possible.
              // Raft topology discard the endpoint-to-id map, so the local id can
              // still be found in the config.
              tm.get_topology().set_host_id_cfg(host_id);
              tm.get_topology().add_or_update_endpoint(host_id, endpoint);
              return make_ready_future<>();
          }).get();

            netw::messaging_service::config mscfg;

            mscfg.id = host_id;
            mscfg.ip = listen_address;
            mscfg.broadcast_address = broadcast_addr;
            mscfg.port = cfg->storage_port();
            mscfg.ssl_port = cfg->ssl_storage_port();
            mscfg.listen_on_broadcast_address = cfg->listen_on_broadcast_address();
            mscfg.rpc_memory_limit = std::max<size_t>(0.08 * memory::stats().total_memory(), mscfg.rpc_memory_limit);
            if (snitch.local()->prefer_local()) {
                mscfg.preferred_ips = sys_ks.local().get_preferred_ips().get();
            }
            mscfg.maintenance_mode = maintenance_mode_enabled{cfg->maintenance_mode()};

            const auto& seo = cfg->server_encryption_options();
            auto encrypt = utils::get_or_default(seo, "internode_encryption", "none");

            if (utils::is_true(utils::get_or_default(seo, "require_client_auth", "false"))) {
                if (encrypt == "dc" || encrypt == "rack") {
                    startlog.warn("Setting require_client_auth is incompatible with 'rack' and 'dc' internode_encryption values."
                        " To ensure that mutual TLS authentication is enforced, please set internode_encryption to 'all'. Continuing with"
                        " potentially insecure configuration."
                    );
                }
            }

            sstring compress_what = cfg->internode_compression();
            if (compress_what == "all") {
                mscfg.compress = netw::messaging_service::compress_what::all;
            } else if (compress_what == "dc") {
                mscfg.compress = netw::messaging_service::compress_what::dc;
            }

            if (encrypt == "all") {
                mscfg.encrypt = netw::messaging_service::encrypt_what::all;
            } else if (encrypt == "dc") {
                mscfg.encrypt = netw::messaging_service::encrypt_what::dc;
            } else if (encrypt == "rack") {
                mscfg.encrypt = netw::messaging_service::encrypt_what::rack;
            }

            if (!cfg->inter_dc_tcp_nodelay()) {
                mscfg.tcp_nodelay = netw::messaging_service::tcp_nodelay_what::local;
            }

            netw::messaging_service::scheduling_config scfg;
            scfg.statement_tenants = {
                    {dbcfg.statement_scheduling_group, "$user"},
                    {default_scheduling_group(), "$system"},
                    {dbcfg.streaming_scheduling_group, "$maintenance"}
            };
            scfg.streaming = dbcfg.streaming_scheduling_group;
            scfg.gossip = dbcfg.gossip_scheduling_group;

            supervisor::notify("starting messaging service");
            debug::the_messaging_service = &messaging;

            std::shared_ptr<seastar::tls::credentials_builder> creds;
            if (mscfg.encrypt != netw::messaging_service::encrypt_what::none) {
                creds = std::make_shared<seastar::tls::credentials_builder>();
                utils::configure_tls_creds_builder(*creds, cfg->server_encryption_options()).get();
            }

            // Delay listening messaging_service until gossip message handlers are registered
            messaging.start(mscfg, scfg, creds).get();
            auto stop_ms = defer_verbose_shutdown("messaging service", [&messaging] {
                messaging.invoke_on_all(&netw::messaging_service::stop).get();
            });

            // #14299 - do early init of messaging_service (or rather its TLS structures)
            // since other things (failure_detector) might try to send messages via it
            // before start_listen is called.
            messaging.invoke_on_all(&netw::messaging_service::start).get();

            api::set_server_messaging_service(ctx, messaging).get();
            auto stop_messaging_api = defer_verbose_shutdown("messaging service API", [&ctx] {
                api::unset_server_messaging_service(ctx).get();
            });

            // Task manager's messaging handlers need to be set like this because of dependency chain:
            // messaging -(needs)-> sys_ks -> db -> cm -> task_manager.
            task_manager.invoke_on_all([&] (auto& tm) {
                tm.init_ms_handlers(messaging.local());
            }).get();
            auto uninit_tm_ms_handlers = defer([&task_manager] () {
                task_manager.invoke_on_all([] (auto& tm) {
                    return tm.uninit_ms_handlers();
                }).get();
            });

            supervisor::notify("starting gossiper");
            auto cluster_name = cfg->cluster_name();
            if (cluster_name.empty()) {
                cluster_name = "Test Cluster";
                startlog.warn("Using default cluster name is not recommended. Using a unique cluster name will reduce the chance of adding nodes to the wrong cluster by mistake");
            }
            auto group0_id = sys_ks.local().get_raft_group0_id().get();

            // Fail on a gossiper seeds lookup error only if the node is not bootstrapped.
            const bool fail_on_lookup_error = !sys_ks.local().bootstrap_complete();

            auto gossiper_seeds = get_seeds_from_db_config(*cfg, broadcast_addr, fail_on_lookup_error);

            auto get_gossiper_cfg = sharded_parameter([&] {
                gms::gossip_config gcfg;
                gcfg.gossip_scheduling_group = dbcfg.gossip_scheduling_group;
                gcfg.seeds = gossiper_seeds;
                gcfg.cluster_name = cluster_name;
                gcfg.partitioner = cfg->partitioner();
                gcfg.ring_delay_ms = cfg->ring_delay_ms();
                gcfg.shadow_round_ms = cfg->shadow_round_ms();
                gcfg.shutdown_announce_ms = cfg->shutdown_announce_in_ms();
                gcfg.skip_wait_for_gossip_to_settle = cfg->skip_wait_for_gossip_to_settle();
                gcfg.group0_id = group0_id;
                gcfg.failure_detector_timeout_ms = cfg->failure_detector_timeout_in_ms;
                gcfg.force_gossip_generation = cfg->force_gossip_generation;
                return gcfg;
            });

            debug::the_gossiper = &gossiper;
            gossiper.start(std::ref(stop_signal.as_sharded_abort_source()), std::ref(token_metadata), std::ref(messaging), std::move(get_gossiper_cfg)).get();
            auto stop_gossiper = defer_verbose_shutdown("gossiper", [&gossiper] {
                // call stop on each instance, but leave the sharded<> pointers alive
                gossiper.invoke_on_all(&gms::gossiper::stop).get();
            });
            gossiper.invoke_on_all(&gms::gossiper::start).get();

            api::set_server_gossip(ctx, gossiper).get();
            auto stop_gossip_api = defer_verbose_shutdown("gossiper API", [&ctx] {
                api::unset_server_gossip(ctx).get();
            });

            static sharded<service::raft_address_map> raft_address_map;
            supervisor::notify("starting Raft address map");
            raft_address_map.start().get();
            auto stop_address_map = defer_verbose_shutdown("raft_address_map", [] {
                raft_address_map.stop().get();
            });

            static sharded<service::direct_fd_pinger> fd_pinger;
            supervisor::notify("starting direct failure detector pinger service");
            fd_pinger.start(std::ref(messaging), std::ref(raft_address_map)).get();

            auto stop_fd_pinger = defer_verbose_shutdown("fd_pinger", [] {
                fd_pinger.stop().get();
            });

            static service::direct_fd_clock fd_clock;
            static sharded<direct_failure_detector::failure_detector> fd;
            supervisor::notify("starting direct failure detector service");
            fd.start(
                std::ref(fd_pinger), std::ref(fd_clock),
                service::direct_fd_clock::base::duration{std::chrono::milliseconds{100}}.count(),
                service::direct_fd_clock::base::duration{std::chrono::milliseconds{cfg->direct_failure_detector_ping_timeout_in_ms()}}.count()).get();

            auto stop_fd = defer_verbose_shutdown("direct_failure_detector", [] {
                fd.stop().get();
            });

            raft_gr.start(raft::server_id{host_id.id}, std::ref(raft_address_map),
                    std::ref(messaging), std::ref(fd)).get();

            // group0 client exists only on shard 0.
            // The client has to be created before `stop_raft` since during
            // destruction it has to exist until raft_gr.stop() completes.
            service::raft_group0_client group0_client{raft_gr.local(), sys_ks.local(), maintenance_mode_enabled{cfg->maintenance_mode()}};

            service::raft_group0 group0_service{
                    stop_signal.as_local_abort_source(), raft_gr.local(), messaging,
                    gossiper.local(), feature_service.local(), sys_ks.local(), group0_client, dbcfg.gossip_scheduling_group};

            service::tablet_allocator::config tacfg;
            tacfg.initial_tablets_scale = cfg->tablets_initial_scale_factor();
            distributed<service::tablet_allocator> tablet_allocator;
            tablet_allocator.start(tacfg, std::ref(mm_notifier), std::ref(db)).get();
            auto stop_tablet_allocator = defer_verbose_shutdown("tablet allocator", [&tablet_allocator] {
                tablet_allocator.stop().get();
            });

            supervisor::notify("starting mapreduce service");
            mapreduce_service.start(std::ref(messaging), std::ref(proxy), std::ref(db), std::ref(token_metadata), std::ref(stop_signal.as_sharded_abort_source())).get();
            auto stop_mapreduce_service_handlers = defer_verbose_shutdown("mapreduce service", [&mapreduce_service] {
                mapreduce_service.stop().get();
            });

            supervisor::notify("starting migration manager");
            debug::the_migration_manager = &mm;
            mm.start(std::ref(mm_notifier), std::ref(feature_service), std::ref(messaging), std::ref(proxy), std::ref(gossiper), std::ref(group0_client), std::ref(sys_ks)).get();
            auto stop_migration_manager = defer_verbose_shutdown("migration manager", [&mm] {
                mm.stop().get();
            });

            // XXX: stop_raft has to happen before query_processor and migration_manager
            // is stopped, since some groups keep using the query
            // processor until are stopped inside stop_raft.
            auto stop_raft = defer_verbose_shutdown("Raft", [&raft_gr] {
                raft_gr.stop().get();
            });

            sharded<service::topology_state_machine> tsm;
            tsm.start().get();
            auto stop_tsm = defer_verbose_shutdown("topology_state_machine", [&tsm] {
                tsm.stop().get();
            });

            supervisor::notify("initializing storage service");
            debug::the_storage_service = &ss;
            ss.start(std::ref(stop_signal.as_sharded_abort_source()),
                std::ref(db), std::ref(gossiper), std::ref(sys_ks), std::ref(sys_dist_ks),
                std::ref(feature_service), std::ref(mm), std::ref(token_metadata), std::ref(erm_factory),
                std::ref(messaging), std::ref(repair),
                std::ref(stream_manager), std::ref(lifecycle_notifier), std::ref(bm), std::ref(snitch),
                std::ref(tablet_allocator), std::ref(cdc_generation_service), std::ref(view_builder), std::ref(qp), std::ref(sl_controller),
                std::ref(tsm), std::ref(task_manager)).get();

            auto stop_storage_service = defer_verbose_shutdown("storage_service", [&] {
                ss.stop().get();
            });

            api::set_server_storage_service(ctx, ss, group0_client).get();
            auto stop_ss_api = defer_verbose_shutdown("storage service API", [&ctx] {
                api::unset_server_storage_service(ctx).get();
            });

            supervisor::notify("initializing query processor remote part");
            // TODO: do this together with proxy.start_remote(...)
            qp.invoke_on_all(&cql3::query_processor::start_remote, std::ref(mm), std::ref(mapreduce_service),
                             std::ref(ss), std::ref(group0_client)).get();
            auto stop_qp_remote = defer_verbose_shutdown("query processor remote part", [&qp] {
                qp.invoke_on_all(&cql3::query_processor::stop_remote).get();
            });

            supervisor::notify("initializing virtual tables");
            smp::invoke_on_all([&] {
                return db::initialize_virtual_tables(db, ss, gossiper, raft_gr, sys_ks, *cfg);
            }).get();

            // #293 - do not stop anything
            // engine().at_exit([&qp] { return qp.stop(); });
            sstables::init_metrics().get();

            db::sstables_format_listener sst_format_listener(gossiper.local(), feature_service, sst_format_selector);

            sst_format_listener.start().get();
            auto stop_format_listener = defer_verbose_shutdown("sstables format listener", [&sst_format_listener] {
                sst_format_listener.stop().get();
            });

            supervisor::notify("starting Raft Group Registry service");
            raft_gr.invoke_on_all(&service::raft_group_registry::start).get();

            api::set_server_raft(ctx, raft_gr).get();
            auto stop_raft_api = defer_verbose_shutdown("Raft API", [&ctx] {
                api::unset_server_raft(ctx).get();
            });

            group0_client.init().get();

            // schema migration, if needed, is also done on shard 0
            db::legacy_schema_migrator::migrate(proxy, db, sys_ks, qp.local()).get();
            db::schema_tables::save_system_schema(qp.local()).get();
            db::schema_tables::recalculate_schema_version(sys_ks, proxy, feature_service.local()).get();

            // making compaction manager api available, after system keyspace has already been established.
            api::set_server_compaction_manager(ctx).get();

            cm.invoke_on_all([&](compaction_manager& cm) {
                auto cl = db.local().commitlog();
                auto scl = db.local().schema_commitlog();
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

            supervisor::notify("loading tablet metadata");
            try {
                ss.local().load_tablet_metadata({}).get();
            } catch (...) {
                if (!cfg->maintenance_mode()) {
                    throw;
                }
                startlog.error("Failed to load tablet metadata (ignoring due to maintenance mode): {}", std::current_exception());
            }

            // We do not support tablet re-sharding yet, see https://github.com/scylladb/scylladb/issues/16739.
            // To avoid undefined behaviour due to violated assumptions, that
            // each tablet has a valid shard replica, we check this assumption here, and refuse startup if violated.
            if (!locator::check_tablet_replica_shards(ss.local().get_token_metadata_ptr()->tablets(), host_id).get()) {
                throw std::runtime_error("Detected a tablet with invalid replica shard, reducing shard count with tablet-enabled tables is not yet supported. Replace the node instead.");
            }

            supervisor::notify("loading non-system sstables");
            replica::distributed_loader::init_non_system_keyspaces(db, proxy, sys_ks).get();

            sys_dist_ks.start(std::ref(qp), std::ref(mm), std::ref(proxy)).get();
            auto stop_sdks = defer_verbose_shutdown("system distributed keyspace", [] {
                sys_dist_ks.invoke_on_all(&db::system_distributed_keyspace::stop).get();
            });

            supervisor::notify("starting view update generator");
            view_update_generator.start(std::ref(db), std::ref(proxy), std::ref(stop_signal.as_sharded_abort_source())).get();
            auto stop_view_update_generator = defer_verbose_shutdown("view update generator", [] {
                view_update_generator.stop().get();
            });

            supervisor::notify("starting the view builder");
            view_builder.start(std::ref(db), std::ref(sys_ks), std::ref(sys_dist_ks), std::ref(mm_notifier), std::ref(view_update_generator)).get();
            auto stop_view_builder = defer_verbose_shutdown("view builder", [cfg] {
                view_builder.stop().get();
            });

            supervisor::notify("starting commit log");
            auto cl = db.local().commitlog();
            if (cl != nullptr) {
                auto paths = cl->get_segments_to_replay().get();
                if (!paths.empty()) {
                    supervisor::notify("replaying commit log");
                    auto rp = db::commitlog_replayer::create_replayer(db, sys_ks).get();
                    rp.recover(paths, db::commitlog::descriptor::FILENAME_PREFIX).get();
                    supervisor::notify("replaying commit log - flushing memtables");
                    db.invoke_on_all(&replica::database::flush_all_memtables).get();
                    supervisor::notify("replaying commit log - removing old commitlog segments");
                    //FIXME: discarded future
                    (void)cl->delete_segments(std::move(paths));
                }
            }

            // Once stuff is replayed, we can empty RP:s from truncation records. 
            // This ensures we can't mis-mash older records with a newer crashed run.
            // I.e: never keep replay_positions alive across a restart cycle.
            sys_ks.local().drop_truncation_rp_records().get();
            sys_ks.local().drop_all_commitlog_cleanup_records().get();

            db.invoke_on_all([] (replica::database& db) {
                db.get_tables_metadata().for_each_table([] (table_id, lw_shared_ptr<replica::table> table) {
                    replica::table& t = *table;
                    t.enable_auto_compaction();
                });
            }).get();

            api::set_server_column_family(ctx, sys_ks).get();
            auto stop_cf_api = defer_verbose_shutdown("column family API", [&ctx] {
                api::unset_server_column_family(ctx).get();
            });
            static seastar::sharded<memory_threshold_guard> mtg;
            mtg.start(cfg->large_memory_allocation_warning_threshold()).get();
            supervisor::notify("initializing storage proxy RPC verbs");
            proxy.invoke_on_all(&service::storage_proxy::start_remote, std::ref(messaging), std::ref(gossiper), std::ref(mm), std::ref(sys_ks)).get();
            auto stop_proxy_handlers = defer_verbose_shutdown("storage proxy RPC verbs", [&proxy] {
                proxy.invoke_on_all(&service::storage_proxy::stop_remote).get();
            });

            debug::the_stream_manager = &stream_manager;
            supervisor::notify("starting streaming service");
            stream_manager.start(std::ref(*cfg), std::ref(db), std::ref(view_builder), std::ref(messaging), std::ref(mm), std::ref(gossiper), maintenance_scheduling_group).get();
            auto stop_stream_manager = defer_verbose_shutdown("stream manager", [&stream_manager] {
                // FIXME -- keep the instances alive, just call .stop on them
                stream_manager.invoke_on_all(&streaming::stream_manager::stop).get();
            });

            stream_manager.invoke_on_all([&stop_signal] (streaming::stream_manager& sm) {
                return sm.start(stop_signal.as_local_abort_source());
            }).get();

            api::set_server_stream_manager(ctx, stream_manager).get();
            auto stop_stream_manager_api = defer_verbose_shutdown("stream manager api", [&ctx] {
                api::unset_server_stream_manager(ctx).get();
            });

            supervisor::notify("starting hinted handoff manager");
            if (!hinted_handoff_enabled.is_disabled_for_all()) {
                hints_dir_initializer.ensure_rebalanced().get();
            }
            view_hints_dir_initializer.ensure_rebalanced().get();

            proxy.invoke_on_all([&lifecycle_notifier] (service::storage_proxy& local_proxy) {
                lifecycle_notifier.local().register_subscriber(&local_proxy);
            }).get();

            auto unsubscribe_proxy = defer_verbose_shutdown("unsubscribe storage proxy", [&proxy, &lifecycle_notifier] {
                proxy.invoke_on_all([&lifecycle_notifier] (service::storage_proxy& local_proxy) mutable {
                    return lifecycle_notifier.local().unregister_subscriber(&local_proxy);
                }).get();
            });

            // ATTN -- sharded repair reference already sits on storage_service and if
            // it calls repair.local() before this place it'll crash (now it doesn't do
            // both)
            supervisor::notify("starting repair service");
            auto max_memory_repair = memory::stats().total_memory() * 0.1;
            repair.start(std::ref(tsm), std::ref(gossiper), std::ref(messaging), std::ref(db), std::ref(proxy), std::ref(raft_address_map), std::ref(bm), std::ref(sys_ks), std::ref(view_builder), std::ref(task_manager), std::ref(mm), max_memory_repair).get();
            auto stop_repair_service = defer_verbose_shutdown("repair service", [&repair] {
                repair.stop().get();
            });
            repair.invoke_on_all(&repair_service::start).get();
            api::set_server_repair(ctx, repair).get();
            auto stop_repair_api = defer_verbose_shutdown("repair API", [&ctx] {
                api::unset_server_repair(ctx).get();
            });

            supervisor::notify("starting CDC Generation Management service");
            /* This service uses the system distributed keyspace.
             * It will only do that *after* the node has joined the token ring, and the token ring joining
             * procedure (`storage_service::init_server`) is responsible for initializing sys_dist_ks.
             * Hence the service will start using sys_dist_ks only after it was initialized.
             *
             * However, there is a problem with the service shutdown order: sys_dist_ks is stopped
             * *before* CDC generation service is stopped (`storage_service::drain_on_shutdown` below),
             * so CDC generation service takes sharded<db::sys_dist_ks> and must check local_is_initialized()
             * every time it accesses it (because it may have been stopped already), then take local_shared()
             * which will prevent sys_dist_ks from being destroyed while the service operates on it.
             */
            cdc::generation_service::config cdc_config;
            cdc_config.ignore_msb_bits = cfg->murmur3_partitioner_ignore_msb_bits();
            cdc_config.ring_delay = std::chrono::milliseconds(cfg->ring_delay_ms());
            cdc_config.dont_rewrite_streams = cfg->cdc_dont_rewrite_streams();
            cdc_generation_service.start(std::move(cdc_config), std::ref(gossiper), std::ref(sys_dist_ks), std::ref(sys_ks),
                    std::ref(stop_signal.as_sharded_abort_source()), std::ref(token_metadata), std::ref(feature_service), std::ref(db),
                    [&ss] () -> bool { return ss.local().raft_topology_change_enabled(); }).get();
            auto stop_cdc_generation_service = defer_verbose_shutdown("CDC Generation Management service", [] {
                cdc_generation_service.stop().get();
            });

            auto get_cdc_metadata = [] (cdc::generation_service& svc) { return std::ref(svc.get_cdc_metadata()); };

            supervisor::notify("starting CDC log service");
            static sharded<cdc::cdc_service> cdc;
            cdc.start(std::ref(proxy), sharded_parameter(get_cdc_metadata, std::ref(cdc_generation_service)), std::ref(mm_notifier)).get();
            auto stop_cdc_service = defer_verbose_shutdown("cdc log service", [] {
                cdc.stop().get();
            });

            supervisor::notify("starting storage service", true);

            gossiper.local().register_(ss.local().shared_from_this());
            auto stop_listening = defer_verbose_shutdown("storage service notifications", [&gossiper, &ss] {
                gossiper.local().unregister_(ss.local().shared_from_this()).get();
            });

            gossiper.local().register_(mm.local().shared_from_this());
            auto stop_mm_listening = defer_verbose_shutdown("migration manager notifications", [&gossiper, &mm] {
                gossiper.local().unregister_(mm.local().shared_from_this()).get();
            });

            utils::loading_cache_config perm_cache_config;
            perm_cache_config.max_size = cfg->permissions_cache_max_entries();
            perm_cache_config.expiry = std::chrono::milliseconds(cfg->permissions_validity_in_ms());
            perm_cache_config.refresh = std::chrono::milliseconds(cfg->permissions_update_interval_in_ms());

            auto start_auth_service = [&mm] (sharded<auth::service>& auth_service, std::any& stop_auth_service, const char* what) {
                supervisor::notify(fmt::format("starting {}", what));
                auth_service.invoke_on_all(&auth::service::start, std::ref(mm), std::ref(sys_ks)).get();

                stop_auth_service = defer_verbose_shutdown(what, [&auth_service] {
                    auth_service.stop().get();
                });
            };

            auto start_cql = [] (cql_transport::controller& controller, std::any& stop_cql, const char* what) {
                supervisor::notify(fmt::format("starting {}", what));
                controller.start_server().get();
                // FIXME -- this should be done via client hooks instead
                stop_cql = defer_verbose_shutdown(what, [&controller] {
                    controller.stop_server().get();
                });
            };

            auth::service_config maintenance_auth_config;
            maintenance_auth_config.authorizer_java_name = sstring{auth::allow_all_authorizer_name};
            maintenance_auth_config.authenticator_java_name = sstring{auth::allow_all_authenticator_name};
            maintenance_auth_config.role_manager_java_name = sstring{auth::maintenance_socket_role_manager_name};

            maintenance_auth_service.start(perm_cache_config, std::ref(qp), std::ref(group0_client),  std::ref(mm_notifier), std::ref(mm), maintenance_auth_config, maintenance_socket_enabled::yes).get();

            cql_transport::controller cql_maintenance_server_ctl(maintenance_auth_service, mm_notifier, gossiper, qp, service_memory_limiter, sl_controller, lifecycle_notifier, *cfg, maintenance_cql_sg_stats_key, maintenance_socket_enabled::yes, dbcfg.statement_scheduling_group);

            std::any stop_maintenance_auth_service;
            std::any stop_maintenance_cql;

            if (cfg->maintenance_socket() != "ignore") {
                start_auth_service(maintenance_auth_service, stop_maintenance_auth_service, "maintenance auth service");
                start_cql(cql_maintenance_server_ctl, stop_maintenance_cql, "maintenance native server");
            }

            db::snapshot_ctl::config snap_cfg = {
                .backup_sched_group = dbcfg.streaming_scheduling_group,
            };
            snapshot_ctl.start(std::ref(db), std::ref(task_manager), std::ref(sstm), snap_cfg).get();
            auto stop_snapshot_ctl = defer_verbose_shutdown("snapshots", [&snapshot_ctl] {
                snapshot_ctl.stop().get();
            });

            api::set_server_snapshot(ctx, snapshot_ctl).get();
            auto stop_api_snapshots = defer_verbose_shutdown("snapshots API", [&ctx] {
                api::unset_server_snapshot(ctx).get();
            });

            api::set_server_tasks_compaction_module(ctx, ss, snapshot_ctl).get();
            auto stop_tasks_api = defer_verbose_shutdown("tasks API", [&ctx] {
                api::unset_server_tasks_compaction_module(ctx).get();
            });

            api::set_server_cache(ctx).get();
            auto stop_cache_api = defer_verbose_shutdown("cache API", [&ctx] {
                api::unset_server_cache(ctx).get();
            });

            if (cfg->maintenance_mode()) {
                startlog.info("entering maintenance mode.");

                ss.local().start_maintenance_mode().get();

                seastar::set_abort_on_ebadf(cfg->abort_on_ebadf());
                api::set_server_done(ctx).get();
                {
                    auto do_drain = defer_verbose_shutdown("local storage", [&ss] {
                        // Flush all memtables and stop ongoing compactions
                        ss.local().drain_on_shutdown().get();
                    });

                    startlog.info("Scylla version {} initialization completed (maintenance mode).", scylla_version());
                    stop_signal.wait().get();
                    startlog.info("Signal received; shutting down");
                }
                startlog.info("Scylla version {} shutdown complete.", scylla_version());
                _exit(0);
                return 0;
            }

            // Register storage_service to migration_notifier so we can update
            // pending ranges when keyspace is changed
            mm_notifier.local().register_listener(&ss.local());
            auto stop_mm_listener = defer_verbose_shutdown("storage service notifications", [&mm_notifier, &ss] {
                mm_notifier.local().unregister_listener(&ss.local()).get();
            });

            supervisor::notify("starting sstables loader");
            sst_loader.start(std::ref(db), std::ref(messaging), std::ref(view_builder), std::ref(task_manager), std::ref(sstm), maintenance_scheduling_group).get();
            auto stop_sst_loader = defer_verbose_shutdown("sstables loader", [&sst_loader] {
                sst_loader.stop().get();
            });
            api::set_server_sstables_loader(ctx, sst_loader).get();
            auto stop_sstl_api = defer_verbose_shutdown("sstables loader API", [&ctx] {
                api::unset_server_sstables_loader(ctx).get();
            });

            /*
             * FIXME. In bb07678346 commit the API toggle for autocompaction was
             * (partially) delayed until system prepared to join the ring. Probably
             * it was an overkill and it can be enabled earlier, even as early as
             * 'by default'. E.g. the per-table toggle was 'enabled' right after
             * the system keyspace started and nobody seemed to have any troubles.
             */
            db.local().enable_autocompaction_toggle();

            group0_service.start().get();
            auto stop_group0_service = defer_verbose_shutdown("group 0 service", [&group0_service] {
                sl_controller.local().abort_group0_operations();
                group0_service.abort().get();
            });

            // Set up group0 service earlier since it is needed by group0 setup just below
            ss.local().set_group0(group0_service);

            const auto generation_number = gms::generation_type(sys_ks.local().increment_and_get_generation().get());

            // Load address_map from system.peers and subscribe to gossiper events to keep it updated.
            ss.local().init_address_map(raft_address_map.local(), generation_number).get();
            auto cancel_address_map_subscription = defer_verbose_shutdown("storage service uninit address map", [&ss] {
                ss.local().uninit_address_map().get();
            });

            // Need to make sure storage service does not use group0 before running group0_service.abort()
            auto stop_group0_usage_in_storage_service = defer_verbose_shutdown("group 0 usage in local storage", [&ss] {
                ss.local().wait_for_group0_stop().get();
            });

            // Setup group0 early in case the node is bootstrapped already and the group exists.
            // Need to do it before allowing incoming messaging service connections since
            // storage proxy's and migration manager's verbs may access group0.
            // This will also disable migration manager schema pulls if needed.
            group0_service.setup_group0_if_exist(sys_ks.local(), ss.local(), qp.local(), mm.local()).get();

            with_scheduling_group(maintenance_scheduling_group, [&] {
                return messaging.invoke_on_all(&netw::messaging_service::start_listen, std::ref(token_metadata));
            }).get();

            with_scheduling_group(maintenance_scheduling_group, [&] {
                return ss.local().join_cluster(sys_dist_ks, proxy, service::start_hint_manager::yes, generation_number);
            }).get();

            supervisor::notify("starting tracing");
            tracing.invoke_on_all(&tracing::tracing::start, std::ref(qp), std::ref(mm)).get();
            auto stop_tracing = defer_verbose_shutdown("tracing", [&tracing] {
                tracing.invoke_on_all(&tracing::tracing::shutdown).get();
            });

            startlog.info("SSTable data integrity checker is {}.",
                    cfg->enable_sstable_data_integrity_check() ? "enabled" : "disabled");

            // This implicitly depends on node joining the cluster (join_cluster())
            // with raft leader elected as only then service level mutation is put
            // into scylla_local table. Calling it here avoids starting new cluster with
            // older version only to immediately migrate it to the latest in the background.
            sl_controller.invoke_on_all([&qp, &group0_client] (qos::service_level_controller& controller) -> future<> {
                return controller.reload_distributed_data_accessor(
                        qp.local(), group0_client, sys_ks.local(), sys_dist_ks.local());
            }).get();

            sl_controller.local().maybe_start_legacy_update_from_distributed_data([cfg] () {
                return std::chrono::duration_cast<steady_clock_type::duration>(std::chrono::milliseconds(cfg->service_levels_interval()));
            }, ss.local(), group0_client);

            const qualified_name qualified_authorizer_name(auth::meta::AUTH_PACKAGE_NAME, cfg->authorizer());
            const qualified_name qualified_authenticator_name(auth::meta::AUTH_PACKAGE_NAME, cfg->authenticator());
            const qualified_name qualified_role_manager_name(auth::meta::AUTH_PACKAGE_NAME, cfg->role_manager());

            auth::service_config auth_config;
            auth_config.authorizer_java_name = qualified_authorizer_name;
            auth_config.authenticator_java_name = qualified_authenticator_name;
            auth_config.role_manager_java_name = qualified_role_manager_name;

            auth_service.start(std::move(perm_cache_config), std::ref(qp), std::ref(group0_client), std::ref(mm_notifier), std::ref(mm), auth_config, maintenance_socket_enabled::no).get();

            std::any stop_auth_service;
            // Has to be called after node joined the cluster (join_cluster())
            // with raft leader elected as only then auth version mutation is put
            // in scylla_local table. This allows to know the version at auth service
            // startup also when creating a new cluster.
            start_auth_service(auth_service, stop_auth_service, "auth service");

            api::set_server_authorization_cache(ctx, auth_service).get();
            auto stop_authorization_cache_api = defer_verbose_shutdown("authorization cache api", [&ctx] {
                api::unset_server_authorization_cache(ctx).get();
            });

            sl_controller.invoke_on_all([&lifecycle_notifier] (qos::service_level_controller& controller) {
                lifecycle_notifier.local().register_subscriber(&controller);
            }).get();
            auto unsubscribe_sl_controller = defer_verbose_shutdown("service level controller subscription", [&lifecycle_notifier] {
                sl_controller.invoke_on_all([&lifecycle_notifier] (qos::service_level_controller& controller) {
                    return lifecycle_notifier.local().unregister_subscriber(&controller);
                }).get();
            });

            supervisor::notify("starting batchlog manager");
            db::batchlog_manager_config bm_cfg;
            bm_cfg.write_request_timeout = cfg->write_request_timeout_in_ms() * 1ms;
            bm_cfg.replay_rate = cfg->batchlog_replay_throttle_in_kb() * 1000;
            bm_cfg.delay = std::chrono::milliseconds(cfg->ring_delay_ms());

            bm.start(std::ref(qp), std::ref(sys_ks), bm_cfg).get();
            auto stop_batchlog_manager = defer_verbose_shutdown("batchlog manager", [&bm] {
                bm.stop().get();
            });

            supervisor::notify("starting load meter");
            load_meter.init(db, gossiper.local()).get();
            auto stop_load_meter = defer_verbose_shutdown("load meter", [&load_meter] {
                load_meter.exit().get();
            });

            api::set_load_meter(ctx, load_meter).get();
            auto stop_load_meter_api = defer_verbose_shutdown("load meter API", [&ctx] {
                api::unset_load_meter(ctx).get();
            });

            supervisor::notify("starting cf cache hit rate calculator");
            cf_cache_hitrate_calculator.start(std::ref(db), std::ref(gossiper)).get();
            auto stop_cache_hitrate_calculator = defer_verbose_shutdown("cf cache hit rate calculator",
                    [&cf_cache_hitrate_calculator] {
                        return cf_cache_hitrate_calculator.stop().get();
                    }
            );
            cf_cache_hitrate_calculator.local().run_on(this_shard_id());

            supervisor::notify("starting view update backlog broker");
            static sharded<service::view_update_backlog_broker> view_backlog_broker;
            view_backlog_broker.start(std::ref(proxy), std::ref(gossiper)).get();
            view_backlog_broker.invoke_on_all(&service::view_update_backlog_broker::start).get();
            auto stop_view_backlog_broker = defer_verbose_shutdown("view update backlog broker", [] {
                view_backlog_broker.stop().get();
            });

            if (!ss.local().raft_topology_change_enabled()) {
                startlog.info("Waiting for gossip to settle before accepting client requests...");
                gossiper.local().wait_for_gossip_to_settle().get();
            }

            supervisor::notify("allow replaying hints");
            proxy.invoke_on_all(&service::storage_proxy::allow_replaying_hints).get();

            api::set_hinted_handoff(ctx, proxy).get();
            auto stop_hinted_handoff_api = defer_verbose_shutdown("hinted handoff API", [&ctx] {
                api::unset_hinted_handoff(ctx).get();
            });

            if (cfg->view_building()) {
                supervisor::notify("Launching generate_mv_updates for non system tables");
                with_scheduling_group(maintenance_scheduling_group, [] {
                    return view_update_generator.invoke_on_all(&db::view::view_update_generator::start);
                }).get();
            }

            if (cfg->view_building()) {
                view_builder.invoke_on_all(&db::view::view_builder::start, std::ref(mm), utils::cross_shard_barrier()).get();
            }

            api::set_server_view_builder(ctx, view_builder).get();
            auto stop_vb_api = defer_verbose_shutdown("view builder API", [&ctx] {
                api::unset_server_view_builder(ctx).get();
            });

            sharded<alternator::expiration_service> es;
            std::any stop_expiration_service;

            if (cfg->alternator_port() || cfg->alternator_https_port()) {
                // Start the expiration service on all shards.
                // Currently we only run it if Alternator is enabled, because
                // only Alternator uses it for its TTL feature. But in the
                // future if we add a CQL interface to it, we may want to
                // start this outside the Alternator if().
                supervisor::notify("starting the expiration service");
                es.start(seastar::sharded_parameter([] (const replica::database& db) { return db.as_data_dictionary(); }, std::ref(db)),
                         std::ref(proxy), std::ref(gossiper)).get();
                stop_expiration_service = defer_verbose_shutdown("expiration service", [&es] {
                    es.stop().get();
                });
                with_scheduling_group(maintenance_scheduling_group, [&es] {
                    return es.invoke_on_all(&alternator::expiration_service::start);
                }).get();
            }

            db.invoke_on_all(&replica::database::revert_initial_system_read_concurrency_boost).get();
            notify_set.notify_all(configurable::system_state::started).get();
            seastar::set_abort_on_ebadf(cfg->abort_on_ebadf());
            api::set_server_done(ctx).get();

            // Create controllers before drain_on_shutdown() below, so that it destructs
            // after drain stops them in stop_transport()
            // Register controllers after drain_on_shutdown() below, so that even on start
            // failure drain is called and stops controllers
            cql_transport::controller cql_server_ctl(auth_service, mm_notifier, gossiper, qp, service_memory_limiter, sl_controller, lifecycle_notifier, *cfg, cql_sg_stats_key, maintenance_socket_enabled::no, dbcfg.statement_scheduling_group);
            alternator::controller alternator_ctl(gossiper, proxy, mm, sys_dist_ks, cdc_generation_service, service_memory_limiter, auth_service, sl_controller, *cfg, dbcfg.statement_scheduling_group);
            redis::controller redis_ctl(proxy, auth_service, mm, *cfg, gossiper, dbcfg.statement_scheduling_group);

            // Register at_exit last, so that storage_service::drain_on_shutdown will be called first
            auto do_drain = defer_verbose_shutdown("local storage", [&ss] {
                ss.local().drain_on_shutdown().get();
            });

            ss.local().register_protocol_server(cql_server_ctl, cfg->start_native_transport()).get();
            api::set_transport_controller(ctx, cql_server_ctl).get();
            auto stop_transport_controller = defer_verbose_shutdown("transport controller API", [&ctx] {
                api::unset_transport_controller(ctx).get();
            });

            api::set_thrift_controller(ctx).get();
            auto stop_thrift_controller = defer_verbose_shutdown("thrift controller API", [&ctx] {
                api::unset_thrift_controller(ctx).get();
            });

#ifndef SCYLLA_BUILD_MODE_RELEASE
            api::set_server_cql_server_test(ctx, cql_server_ctl).get();
            auto stop_cql_server_test_api = defer_verbose_shutdown("cql server API", [&ctx] {
                api::unset_server_cql_server_test(ctx).get();
            });
#endif

            ss.local().register_protocol_server(alternator_ctl, cfg->alternator_port() || cfg->alternator_https_port()).get();

            ss.local().register_protocol_server(redis_ctl, cfg->redis_port() || cfg->redis_ssl_port()).get();

            supervisor::notify("serving");

            startlog.info("Scylla version {} initialization completed.", scylla_version());
            if (after_init_func) {
                after_init_func(cfg);
            }
            stop_signal.wait().get();
            startlog.info("Signal received; shutting down");
	    // At this point, all objects destructors and all shutdown hooks registered with defer() are executed
          } catch (const sleep_aborted&) {
            startlog.info("Startup interrupted");
            // This happens when scylla gets SIGINT in the middle of join_cluster(), so
            // just ignore it and exit normally
            _exit(0);
            return 0;
          } catch (const abort_requested_exception&) {
            startlog.info("Startup interrupted");
            // This happens when scylla gets SIGINT in the middle of join_cluster(), so
            // just ignore it and exit normally
            _exit(0);
            return 0;
          } catch (...) {
            startlog.error("Startup failed: {}", std::current_exception());
            // We should be returning 1 here, but the system is not yet prepared for orderly rollback of main() objects
            // and thread_local variables.
            _exit(1);
            return 1;
          }
          startlog.info("Scylla version {} shutdown complete.", scylla_version());

          // With -fprofile-generate, LLVM inserts an exit hook which saves the profile counters to disk.
          // So does BOLT's instrumentation.
          // But since we exit abruptly and skip those hooks, we have to trigger the dump manually.
          dump_performance_profiles();

          // We should be returning 0 here, but the system is not yet prepared for orderly rollback of main() objects
          // and thread_local variables.
          _exit(0);
          return 0;
        });
    });
  } catch (...) {
      // reactor may not have been initialized, so can't use logger
      fmt::print(std::cerr, "FATAL: Exception during startup, aborting: {}\n", std::current_exception());
      return 7; // 1 has a special meaning for upstart
  }
}

int main(int ac, char** av) {
    // early check to avoid triggering
    if (!cpu_sanity()) {
        _exit(71);
    }

    std::string exec_name;
    if (ac >= 2) {
        exec_name = av[1];
    }

    using main_func_type = std::function<int(int, char**)>;
    struct tool {
        std::string_view name;
        main_func_type func;
        std::string_view desc;
    };
    const tool tools[] = {
        {"server", scylla_main, "the scylladb server"},
        {"types", tools::scylla_types_main, "a command-line tool to examine values belonging to scylla types"},
        {"sstable", tools::scylla_sstable_main, "a multifunctional command-line tool to examine the content of sstables"},
        {"nodetool", tools::scylla_nodetool_main, "a command-line tool to administer local or remote ScyllaDB nodes"},
        {"perf-fast-forward", perf::scylla_fast_forward_main, "run performance tests by fast forwarding the reader on this server"},
        {"perf-row-cache-update", perf::scylla_row_cache_update_main, "run performance tests by updating row cache on this server"},
        {"perf-tablets", perf::scylla_tablets_main, "run performance tests of tablet metadata management"},
        {"perf-load-balancing", perf::scylla_tablet_load_balancing_main, "run tablet load balancer tests"},
        {"perf-simple-query", perf::scylla_simple_query_main, "run performance tests by sending simple queries to this server"},
        {"perf-sstable", perf::scylla_sstable_main, "run performance tests by exercising sstable related operations on this server"},
        {"perf-alternator", perf::alternator(scylla_main, &after_init_func), "run performance tests on full alternator stack"}
    };

    main_func_type main_func;
    if (exec_name.empty() || exec_name[0] == '-') {
        main_func = scylla_main;
    } else if (auto tool = std::ranges::find_if(tools, [exec_name] (auto& tool) {
                               return tool.name == exec_name;
                           });
               tool != std::ranges::end(tools)) {
        main_func = tool->func;
        // shift args to consume the recognized tool name
        std::shift_left(av + 1, av + ac, 1);
        --ac;
    } else {
        fmt::print("error: unrecognized first argument: expected it to be \"server\", a regular command-line argument or a valid tool name (see `scylla --list-tools`), but got {}\n", exec_name);
        return 1;
    }

    // Even on the environment which causes errors during Scylla initialization,
    // "scylla --version" should be able to run without error.
    // To do so, we need to parse and execute these options before
    // initializing Scylla/Seastar classes.
    bpo::options_description preinit_description("Scylla options");
    bpo::variables_map preinit_vm;
    preinit_description.add_options()
        ("version", bpo::bool_switch(), "print version number and exit")
        ("build-id", bpo::bool_switch(), "print build-id and exit")
        ("build-mode", bpo::bool_switch(), "print build mode and exit")
        ("list-tools", bpo::bool_switch(), "list included tools and exit");
    auto preinit_parsed_opts = bpo::command_line_parser(ac, av).options(preinit_description).allow_unregistered().run();
    bpo::store(preinit_parsed_opts, preinit_vm);
    if (preinit_vm["version"].as<bool>()) {
        fmt::print("{}\n", scylla_version());
        return 0;
    }
    if (preinit_vm["build-id"].as<bool>()) {
        fmt::print("{}\n", get_build_id());
        return 0;
    }
    if (preinit_vm["build-mode"].as<bool>()) {
        fmt::print("{}\n", scylla_build_mode());
        return 0;
    }
    if (preinit_vm["list-tools"].as<bool>()) {
        for (auto& tool : tools) {
            fmt::print("{} - {}\n", tool.name, tool.desc);
        }
        return 0;
    }

    return main_func(ac, av);
}
