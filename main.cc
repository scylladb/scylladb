/*
 * Copyright 2014 Cloudius Systems
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
#include "service/load_broadcaster.hh"
#include "streaming/stream_session.hh"
#include "db/system_keyspace.hh"
#include "db/batchlog_manager.hh"
#include "db/commitlog/commitlog.hh"
#include "db/commitlog/commitlog_replayer.hh"
#include "utils/runtime.hh"
#include "utils/file_lock.hh"
#include "dns.hh"
#include "log.hh"
#include "debug.hh"
#include "init.hh"
#include "release.hh"
#include <cstdio>
#include <core/file.hh>

logging::logger startlog("init");

namespace bpo = boost::program_options;

static future<>
read_config(bpo::variables_map& opts, db::config& cfg) {
    using namespace boost::filesystem;
    sstring file;

    if (opts.count("options-file") > 0) {
        file = opts["options-file"].as<sstring>();
    } else {
        auto file_path = db::config::get_conf_dir();
        file_path /= path("scylla.yaml");

        file = file_path.string();
    }
    return check_direct_io_support(file).then([file, &cfg] {
        return cfg.read_from_file(file);
    }).handle_exception([file](auto ep) {
        startlog.error("Could not read configuration file {}: {}", file, ep);
        return make_exception_future<>(ep);
    });
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

static future<> disk_sanity(sstring path) {
    return check_direct_io_support(path).then([path] {
        return file_system_at(path).then([path] (auto fs) {
            if (fs != fs_type::xfs) {
                startlog.warn("{} is not on XFS. This is a non-supported setup, and performance is expected to be very bad.\n"
                    "For better performance, placing your data on XFS-formatted directories is strongly recommended", path);
            }
        });
    });
};

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

class directories {
public:
    future<> touch_and_lock(sstring path) {
        return recursive_touch_directory(path).then_wrapped([this, path] (future<> f) {
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
            } catch (std::system_error& e) {
                startlog.error("Directory '{}' not found. Tried to created it but failed: {}", path, e.what());
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

class bad_configuration_error : public std::exception {};

int main(int ac, char** av) {
    runtime::init_uptime();
    std::setvbuf(stdout, nullptr, _IOLBF, 1000);
    app_template app;
    auto opt_add = app.add_options();

    auto cfg = make_lw_shared<db::config>();
    bool help_loggers = false;
    cfg->add_options(opt_add)
        // TODO : default, always read?
        ("options-file", bpo::value<sstring>(), "configuration file (i.e. <SCYLLA_HOME>/conf/scylla.yaml)")
        ("help-loggers", bpo::bool_switch(&help_loggers), "print a list of logger names and exit")
        ;

    distributed<database> db;
    debug::db = &db;
    auto& qp = cql3::get_query_processor();
    auto& proxy = service::get_storage_proxy();
    auto& mm = service::get_migration_manager();
    api::http_context ctx(db, proxy);
    directories dirs;

    return app.run_deprecated(ac, av, [&] {
        if (help_loggers) {
            do_help_loggers();
            engine().exit(1);
            return make_ready_future<>();
        }
        print("Scylla version %s starting ...\n", scylla_version());
        auto&& opts = app.configuration();

        // Do this first once set log applied from command line so for example config
        // parse can get right log level.
        apply_logger_settings(cfg->default_log_level(), cfg->logger_log_level(),
                cfg->log_to_stdout(), cfg->log_to_syslog());

        return read_config(opts, *cfg).then([&cfg, &db, &qp, &proxy, &mm, &ctx, &opts, &dirs]() {
            apply_logger_settings(cfg->default_log_level(), cfg->logger_log_level(),
                    cfg->log_to_stdout(), cfg->log_to_syslog());
            dht::set_global_partitioner(cfg->partitioner());
            auto start_thrift = cfg->start_rpc();
            uint16_t api_port = cfg->api_port();
            uint16_t storage_port = cfg->storage_port();
            ctx.api_dir = cfg->api_ui_dir();
            ctx.api_doc = cfg->api_doc_dir();
            double phi = cfg->phi_convict_threshold();
            sstring cluster_name = cfg->cluster_name();
            sstring listen_address = cfg->listen_address();
            sstring rpc_address = cfg->rpc_address();
            sstring api_address = cfg->api_address() != "" ? cfg->api_address() : rpc_address;
            auto seed_provider= cfg->seed_provider();
            sstring broadcast_address = cfg->broadcast_address();
            sstring broadcast_rpc_address = cfg->broadcast_rpc_address();

            if (!broadcast_address.empty()) {
                utils::fb_utilities::set_broadcast_address(broadcast_address);
            } else if (!listen_address.empty()) {
                utils::fb_utilities::set_broadcast_address(listen_address);
            } else {
                startlog.error("Bad configuration: neither listen_address nor broadcast_address are defined\n");
                throw bad_configuration_error();
            }

            if (!broadcast_rpc_address.empty()) {
                utils::fb_utilities::set_broadcast_rpc_address(broadcast_rpc_address);
            } else {
                if (rpc_address == "0.0.0.0") {
                    startlog.error("If rpc_address is set to a wildcard address {}, then you must set broadcast_rpc_address to a value other than {}", rpc_address, rpc_address);
                    throw bad_configuration_error();
                }
                utils::fb_utilities::set_broadcast_rpc_address(rpc_address);
            }

            using namespace locator;
            return i_endpoint_snitch::create_snitch(cfg->endpoint_snitch()).then([] {
                // #293 - do not stop anything
                // engine().at_exit([] { return i_endpoint_snitch::stop_snitch(); });
            }).then([&db] {
                return init_storage_service(db);
            }).then([&db, cfg] {
                return db.start(std::move(*cfg)).then([&db] {
                    engine().at_exit([&db] {

                        // #293 - do not stop anything - not even db (for real)
                        //return db.stop();
                        // call stop on each db instance, but leave the shareded<database> pointers alive.
                        return db.invoke_on_all([](auto& db) {
                            return db.stop();
                        }).then([] {
                            ::_exit(3);
                        });
                    });
                });
            }).then([listen_address, storage_port, seed_provider, cluster_name, phi] {
                return init_ms_fd_gossiper(listen_address, storage_port, seed_provider, cluster_name, phi);
            }).then([&db] {
                return streaming::stream_session::init_streaming_service(db);
            }).then([&proxy, &db] {
                return proxy.start(std::ref(db)).then([&proxy] {
                    // #293 - do not stop anything
                    // engine().at_exit([&proxy] { return proxy.stop(); });
                });
            }).then([&mm] {
                return mm.start().then([&mm] {
                    // #293 - do not stop anything
                    // engine().at_exit([&mm] { return mm.stop(); });
                });
            }).then([&db, &proxy, &qp] {
                return qp.start(std::ref(proxy), std::ref(db)).then([&qp] {
                    // #293 - do not stop anything
                    // engine().at_exit([&qp] { return qp.stop(); });
                });
            }).then([&qp] {
                return db::get_batchlog_manager().start(std::ref(qp)).then([] {
                    // #293 - do not stop anything
                    // engine().at_exit([] { return db::get_batchlog_manager().stop(); });
                });
            }).then([&db, &dirs] {
                return dirs.touch_and_lock(db.local().get_config().data_file_directories());
            }).then([&db, &dirs] {
                return dirs.touch_and_lock(db.local().get_config().commitlog_directory());
            }).then([&db] {
                std::unordered_set<sstring> directories;
                directories.insert(db.local().get_config().data_file_directories().cbegin(),
                        db.local().get_config().data_file_directories().cend());
                directories.insert(db.local().get_config().commitlog_directory());
                return do_with(std::move(directories), [] (auto& directories) {
                    return parallel_for_each(directories, [] (sstring pathname) {
                        return disk_sanity(pathname);
                    });
                });
            }).then([&db] {
                return db.invoke_on_all([] (database& db) {
                    return db.init_system_keyspace();
                }).then([&db] {
                    auto& ks = db.local().find_keyspace(db::system_keyspace::NAME);
                    return parallel_for_each(ks.metadata()->cf_meta_data(), [&ks] (auto& pair) {
                        auto cfm = pair.second;
                        return ks.make_directory_for_column_family(cfm->cf_name(), cfm->id());
                    });
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
            }).then([] {
                return db::get_batchlog_manager().invoke_on_all([] (db::batchlog_manager& b) {
                    return b.start();
                });
            }).then([&db] {
                // should be unique_ptr, but then lambda passed to at_exit will be non copieable and
                // casting to std::function<> will fail to compile
                auto lb = make_shared<service::load_broadcaster>(db, gms::get_local_gossiper());
                lb->start_broadcasting();
                service::get_local_storage_service().set_load_broadcaster(lb);
                engine().at_exit([lb = std::move(lb)] () mutable { return lb->stop_broadcasting(); });
            }).then([start_thrift] () {
                return service::get_local_storage_service().start_native_transport().then([start_thrift] () {
                    if (start_thrift) {
                        return service::get_local_storage_service().start_rpc_server();
                    }
                    return make_ready_future<>();
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
