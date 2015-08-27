/*
 * Copyright 2014 Cloudius Systems
 */


#include "database.hh"
#include "core/app-template.hh"
#include "core/distributed.hh"
#include "thrift/server.hh"
#include "transport/server.hh"
#include "http/httpd.hh"
#include "api/api.hh"
#include "db/config.hh"
#include "service/storage_service.hh"
#include "service/migration_manager.hh"
#include "streaming/stream_session.hh"
#include "db/system_keyspace.hh"
#include "db/batchlog_manager.hh"
#include "db/commitlog/commitlog.hh"
#include "db/commitlog/commitlog_replayer.hh"
#include "utils/runtime.hh"
#include "dns.hh"
#include "log.hh"
#include "debug.hh"
#include "init.hh"
#include <cstdio>

namespace bpo = boost::program_options;

static future<>
read_config(bpo::variables_map& opts, db::config& cfg) {
    if (opts.count("options-file") == 0) {
        return make_ready_future<>();
    }
    return cfg.read_from_file(opts["options-file"].as<sstring>());
}

static void do_help_loggers() {
    print("Available loggers:\n");
    for (auto&& name : logging::logger_registry().get_all_logger_names()) {
        print("    %s\n", name);
    }
}

static logging::log_level to_loglevel(sstring level) {
    try {
        return boost::lexical_cast<logging::log_level>(std::string(level));
    } catch(boost::bad_lexical_cast e) {
        throw std::runtime_error("Unknown log level '" + level + "'");
    }
}

static void apply_logger_settings(sstring default_level, db::config::string_map levels,
        bool log_to_stdout, bool log_to_syslog) {
    logging::logger_registry().set_all_loggers_level(to_loglevel(default_level));
    for (auto&& kv: levels) {
        auto&& k = kv.first;
        auto&& v = kv.second;
        try {
            logging::logger_registry().set_logger_level(k, to_loglevel(v));
        } catch(std::out_of_range e) {
            throw std::runtime_error("Unknown logger '" + k + "'. Use --help-loggers to list available loggers.");
        }
    }
    logging::logger::set_stdout_enabled(log_to_stdout);
    logging::logger::set_syslog_enabled(log_to_syslog);
}


logging::logger startlog("init");

int main(int ac, char** av) {
    runtime::init_uptime();
    std::setvbuf(stdout, nullptr, _IOLBF, 1000);
    app_template app;
    auto opt_add = app.add_options();

    auto cfg = make_lw_shared<db::config>();
    bool help_loggers = false;
    cfg->add_options(opt_add)
        ("api-address", bpo::value<sstring>(), "Http Rest API address")
        ("api-port", bpo::value<uint16_t>()->default_value(10000), "Http Rest API port")
        ("api-dir", bpo::value<sstring>()->default_value("swagger-ui/dist/"),
                "The directory location of the API GUI")
        // TODO : default, always read?
        ("options-file", bpo::value<sstring>()->default_value("conf/scylla.yaml"), "scylla.yaml file to read options from")
        ("help-loggers", bpo::bool_switch(&help_loggers), "print a list of logger names and exit")
        ;

    distributed<database> db;
    debug::db = &db;
    distributed<cql3::query_processor> qp;
    auto& proxy = service::get_storage_proxy();
    auto& mm = service::get_migration_manager();
    api::http_context ctx(db, proxy);

    return app.run(ac, av, [&] {
        if (help_loggers) {
            do_help_loggers();
            engine().exit(1);
            return make_ready_future<>();
        }
        auto&& opts = app.configuration();

        return read_config(opts, *cfg).then([&cfg, &db, &qp, &proxy, &mm, &ctx, &opts]() {
            apply_logger_settings(cfg->default_log_level(), cfg->logger_log_level(),
                    cfg->log_to_stdout(), cfg->log_to_syslog());
            dht::set_global_partitioner(cfg->partitioner());
            uint16_t thrift_port = cfg->rpc_port();
            uint16_t cql_port = cfg->native_transport_port();
            uint16_t api_port = opts["api-port"].as<uint16_t>();
            ctx.api_dir = opts["api-dir"].as<sstring>();
            sstring listen_address = cfg->listen_address();
            sstring rpc_address = cfg->rpc_address();
            sstring api_address = opts.count("api-address") ? opts["api-address"].as<sstring>() : rpc_address;
            auto seed_provider= cfg->seed_provider();
            using namespace locator;
            return i_endpoint_snitch::create_snitch(cfg->endpoint_snitch()).then([] {
                engine().at_exit([] { return i_endpoint_snitch::stop_snitch(); });
            }).then([&db] {
                return init_storage_service(db);
            }).then([&db, cfg] {
                return db.start(std::move(*cfg)).then([&db] {
                    engine().at_exit([&db] { return db.stop(); });
                });
            }).then([listen_address, seed_provider] {
                return init_ms_fd_gossiper(listen_address, seed_provider);
            }).then([&db] {
                return streaming::stream_session::init_streaming_service(db);
            }).then([&proxy, &db] {
                return proxy.start(std::ref(db)).then([&proxy] {
                    engine().at_exit([&proxy] { return proxy.stop(); });
                });
            }).then([&mm] {
                return mm.start().then([&mm] {
                    engine().at_exit([&mm] { return mm.stop(); });
                });
            }).then([&db, &proxy, &qp] {
                return qp.start(std::ref(proxy), std::ref(db)).then([&qp] {
                    engine().at_exit([&qp] { return qp.stop(); });
                });
            }).then([&qp] {
                return db::get_batchlog_manager().start(std::ref(qp)).then([] {
                   engine().at_exit([] { return db::get_batchlog_manager().stop(); });
                });
            }).then([&db] {
                return parallel_for_each(db.local().get_config().data_file_directories(), [] (sstring datadir) {
                    return recursive_touch_directory(datadir).then_wrapped([datadir] (future<> f) {
                        try {
                            f.get();
                        } catch (std::system_error& e) {
                            startlog.error("Directory '{}' not found. Tried to created it but failed: {}", datadir, e.what());
                            throw;
                        }
                    });
                });
            }).then([&db] {
                sstring commitlog = db.local().get_config().commitlog_directory();
                return recursive_touch_directory(commitlog).then_wrapped([commitlog] (future<> f) {
                    try {
                        f.get();
                    } catch (std::system_error& e) {
                        startlog.error("commitlog directory '{}' not found. Tried to created it but failed: {}", commitlog, e.what());
                        throw;
                    }
                });
            }).then([&db] {
                return db.invoke_on_all([] (database& db) {
                    return db.init_system_keyspace();
                });
            }).then([&db, &proxy] {
                return db.invoke_on_all([&proxy] (database& db) {
                    return db.load_sstables(proxy);
                });
            }).then([&db, &qp] {
                return db::system_keyspace::setup(db, qp);
            }).then([&db, &qp] {
                auto cl = db.local().commitlog();
                if (cl == nullptr) {
                    return make_ready_future<>();
                }
                return cl->list_existing_segments().then([&db, &qp](auto paths) {
                    if (paths.empty()) {
                        return make_ready_future<>();
                    }
                    return db::commitlog_replayer::create_replayer(qp).then([paths](auto rp) {
                        return do_with(std::move(rp), [paths = std::move(paths)](auto& rp) {
                            return rp.recover(paths);
                        });
                    }).then([&db] {
                        return db.invoke_on_all([] (database& db) {
                            return db.flush_all_memtables();
                        });
                    }).then([paths] {
                        for (auto& path : paths) {
                            ::unlink(path.c_str());
                        }
                    });
                });
            }).then([] {
                auto& ss = service::get_local_storage_service();
                return ss.init_server();
            }).then([rpc_address] {
                return dns::gethostbyname(rpc_address);
            }).then([&db, &proxy, &qp, rpc_address, cql_port, thrift_port] (dns::hostent e) {
                auto ip = e.addresses[0].in.s_addr;
                auto cserver = new distributed<transport::cql_server>;
                cserver->start(std::ref(proxy), std::ref(qp)).then([server = std::move(cserver), cql_port, rpc_address, ip] () mutable {
                    engine().at_exit([server] {
                        return server->stop();
                    });
                    server->invoke_on_all(&transport::cql_server::listen, ipv4_addr{ip, cql_port});
                }).then([rpc_address, cql_port] {
                    print("CQL server listening on %s:%s ...\n", rpc_address, cql_port);
                });
                auto tserver = new distributed<thrift_server>;
                tserver->start(std::ref(db)).then([server = std::move(tserver), thrift_port, rpc_address, ip] () mutable {
                    engine().at_exit([server] {
                        return server->stop();
                    });
                    server->invoke_on_all(&thrift_server::listen, ipv4_addr{ip, thrift_port});
                }).then([rpc_address, thrift_port] {
                    print("Thrift server listening on %s:%s ...\n", rpc_address, thrift_port);
                });
            }).then([api_address] {
                return dns::gethostbyname(api_address);
            }).then([&db, api_address, api_port, &ctx] (dns::hostent e){
                auto ip = e.addresses[0].in.s_addr;
                ctx.http_server.start().then([api_address, api_port, ip, &ctx] {
                    return set_server(ctx);
                }).then([api_address, api_port, ip, &ctx] {
                    ctx.http_server.listen(ipv4_addr{ip, api_port});
                }).then([api_address, api_port] {
                    print("Seastar HTTP server listening on %s:%s ...\n", api_address, api_port);
                });
            });
        }).or_terminate();
    });
}

namespace debug {

seastar::sharded<database>* db;

}
