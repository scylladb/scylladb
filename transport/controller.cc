/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "utils/assert.hh"
#include <grp.h>
#include "transport/controller.hh"
#include <seastar/core/sharded.hh>
#include <seastar/net/socket_defs.hh>
#include <seastar/net/unix_address.hh>
#include <seastar/core/file-types.hh>
#include "transport/server.hh"
#include "service/memory_limiter.hh"
#include "db/config.hh"
#include "gms/gossiper.hh"
#include "log.hh"
#include "cql3/query_processor.hh"

using namespace seastar;

namespace cql_transport {

static logging::logger logger("cql_server_controller");

controller::controller(sharded<auth::service>& auth, sharded<service::migration_notifier>& mn,
        sharded<gms::gossiper>& gossiper, sharded<cql3::query_processor>& qp, sharded<service::memory_limiter>& ml,
        sharded<qos::service_level_controller>& sl_controller, sharded<service::endpoint_lifecycle_notifier>& elc_notif,
        const db::config& cfg, scheduling_group_key cql_opcode_stats_key, maintenance_socket_enabled used_by_maintenance_socket,
        seastar::scheduling_group sg)
    : protocol_server(sg)
    , _ops_sem(1)
    , _auth_service(auth)
    , _mnotifier(mn)
    , _lifecycle_notifier(elc_notif)
    , _gossiper(gossiper)
    , _qp(qp)
    , _mem_limiter(ml)
    , _sl_controller(sl_controller)
    , _config(cfg)
    , _cql_opcode_stats_key(cql_opcode_stats_key)
    , _used_by_maintenance_socket(used_by_maintenance_socket)
{
}

sstring controller::name() const {
    return "native transport";
}

sstring controller::protocol() const {
    return "cql";
}

sstring controller::protocol_version() const {
    return cql3::query_processor::CQL_VERSION;
}

std::vector<socket_address> controller::listen_addresses() const {
    return _server ? _listen_addresses : std::vector<socket_address>();
}

future<> controller::start_server() {
    if (!_ops_sem.try_wait()) {
        throw std::runtime_error(format("CQL server is stopping, try again later"));
    }

    return do_start_server().finally([this] { _ops_sem.signal(); });
}

static future<> listen_on_all_shards(sharded<cql_server>& cserver, socket_address addr, std::shared_ptr<seastar::tls::credentials_builder> creds, bool is_shard_aware, bool keepalive, std::optional<file_permissions> unix_domain_socket_permissions) {
    co_await cserver.invoke_on_all([addr, creds, is_shard_aware, keepalive, unix_domain_socket_permissions] (cql_server& server) {
        return server.listen(addr, creds, is_shard_aware, keepalive, unix_domain_socket_permissions);
    });
}

future<> controller::start_listening_on_tcp_sockets(sharded<cql_server>& cserver) {
    auto& cfg = _config;
    auto preferred = cfg.rpc_interface_prefer_ipv6() ? std::make_optional(net::inet_address::family::INET6) : std::nullopt;
    auto family = cfg.enable_ipv6_dns_lookup() || preferred ? std::nullopt : std::make_optional(net::inet_address::family::INET);
    auto ceo = cfg.client_encryption_options();
    auto keepalive = cfg.rpc_keepalive();

    struct listen_cfg {
        socket_address addr;
        bool is_shard_aware;
        std::shared_ptr<seastar::tls::credentials_builder> cred;
    };

    _listen_addresses.clear();
    std::vector<listen_cfg> configs;

    const seastar::net::inet_address ip = utils::resolve(cfg.rpc_address, family, preferred).get();
    int native_port_idx = -1, native_shard_aware_port_idx = -1;

    if (cfg.native_transport_port.is_set() ||
            (!cfg.native_transport_port_ssl.is_set() && !cfg.native_transport_port.is_set())) {
        // Non-SSL port is specified || neither SSL nor non-SSL ports are specified
        configs.emplace_back(listen_cfg{ socket_address{ip, cfg.native_transport_port()}, false });
        _listen_addresses.push_back(configs.back().addr);
        native_port_idx = 0;
    }
    if (cfg.native_shard_aware_transport_port.is_set() ||
            (!cfg.native_shard_aware_transport_port_ssl.is_set() && !cfg.native_shard_aware_transport_port.is_set())) {
        configs.emplace_back(listen_cfg{ socket_address{ip, cfg.native_shard_aware_transport_port()}, true });
        _listen_addresses.push_back(configs.back().addr);
        native_shard_aware_port_idx = native_port_idx + 1;
    }

    // main should have made sure values are clean and neatish
    if (utils::is_true(utils::get_or_default(ceo, "enabled", "false"))) {
        auto cred = std::make_shared<seastar::tls::credentials_builder>();
        utils::configure_tls_creds_builder(*cred, std::move(ceo)).get();

        logger.info("Enabling encrypted CQL connections between client and server");

        if (cfg.native_transport_port_ssl.is_set() &&
                (!cfg.native_transport_port.is_set() ||
                cfg.native_transport_port_ssl() != cfg.native_transport_port())) {
            // SSL port is specified && non-SSL port is either left out or set to a different value
            configs.emplace_back(listen_cfg{{ip, cfg.native_transport_port_ssl()}, false, cred});
            _listen_addresses.push_back(configs.back().addr);
        } else if (native_port_idx >= 0) {
            configs[native_port_idx].cred = cred;
        }
        if (cfg.native_shard_aware_transport_port_ssl.is_set() &&
                (!cfg.native_shard_aware_transport_port.is_set() ||
                cfg.native_shard_aware_transport_port_ssl() != cfg.native_shard_aware_transport_port())) {
            configs.emplace_back(listen_cfg{{ip, cfg.native_shard_aware_transport_port_ssl()}, true, std::move(cred)});
            _listen_addresses.push_back(configs.back().addr);
        } else if (native_shard_aware_port_idx >= 0) {
            configs[native_shard_aware_port_idx].cred = std::move(cred);
        }
    }

    co_await parallel_for_each(configs, [&cserver, keepalive](const listen_cfg & cfg) -> future<> {
        co_await listen_on_all_shards(cserver, cfg.addr, cfg.cred, cfg.is_shard_aware, keepalive, std::nullopt);

        logger.info("Starting listening for CQL clients on {} ({}, {})"
                , cfg.addr, cfg.cred ? "encrypted" : "unencrypted", cfg.is_shard_aware ? "shard-aware" : "non-shard-aware"
        );
    });
}

future<> controller::start_listening_on_maintenance_socket(sharded<cql_server>& cserver) {
    auto socket = _config.maintenance_socket();

    if (socket == "workdir") {
        socket = _config.work_directory() + "/cql.m";
    }

    auto max_socket_length = sizeof(sockaddr_un::sun_path);
    if (socket.length() > max_socket_length - 1) {
        throw std::runtime_error(format("Maintenance socket path is too long: {}. Change it to string shorter than {} chars.", socket, max_socket_length));
    }

    struct stat statbuf;
    auto stat_result = ::stat(socket.c_str(), &statbuf);
    if (stat_result == 0) {
        // Check if it is a unix domain socket, not a regular file or directory
        if (!S_ISSOCK(statbuf.st_mode)) {
            throw std::runtime_error(format("Under maintenance socket path ({}) there is something else.", socket));
        }
    } else if (errno != ENOENT) {
        // Other error than "file does not exist"
        throw std::runtime_error(format("Failed to stat {}: {}", socket, strerror(errno)));
    }

    // Remove the socket if it already exists, otherwise when the server
    // tries to listen on it, it will hang on bind().
    auto unlink_result = ::unlink(socket.c_str());
    if (unlink_result < 0 && errno != ENOENT) {
        // Other error than "file does not exist"
        throw std::runtime_error(format("Failed to unlink {}: {}", socket, strerror(errno)));
    }

    auto addr = socket_address { unix_domain_addr { socket } };
    _listen_addresses.push_back(addr);

    logger.info("Setting up maintenance socket on {}", socket);

    auto unix_domain_socket_permissions =
        file_permissions::user_read | file_permissions::user_write |
        file_permissions::group_read | file_permissions::group_write;

    co_await listen_on_all_shards(cserver, addr, nullptr, false, _config.rpc_keepalive(), unix_domain_socket_permissions);

    if (_config.maintenance_socket_group.is_set()) {
        auto group_name = _config.maintenance_socket_group();
        struct group *grp;
        grp = ::getgrnam(group_name.c_str());
        if (!grp) {
            throw std::runtime_error(format("Group id of {} not found. Make sure the group exists.", group_name));
        }

        auto chown_result = ::chown(socket.c_str(), ::geteuid(), grp->gr_gid);
        if (chown_result < 0) {
            if (errno == EPERM) {
                throw std::runtime_error(format("Failed to change group of {}: Permission denied. Make sure the user has the root privilege or is a member of the group {}.", socket, group_name));
            } else {
                throw std::runtime_error(format("Failed to chown {}: {} ()", socket, strerror(errno)));
            }
        }
    }

    logger.info("Starting listening for maintenance CQL clients on {} (unencrypted, non-shard-aware)"
            , addr
    );
}

future<> controller::do_start_server() {
    if (_server) {
        return make_ready_future<>();
    }

    seastar::thread_attributes attr;
    attr.sched_group = _sched_group;
    return seastar::async(std::move(attr), [this] {
        auto cserver = std::make_unique<sharded<cql_server>>();

        auto& cfg = _config;
        smp_service_group_config cql_server_smp_service_group_config;
        cql_server_smp_service_group_config.max_nonlocal_requests = 5000;
        auto bounce_request_smp_service_group = create_smp_service_group(cql_server_smp_service_group_config).get();
        auto get_cql_server_config = sharded_parameter([&] {
            std::optional<uint16_t> shard_aware_transport_port;
            if (cfg.native_shard_aware_transport_port.is_set()) {
                // Needed for "SUPPORTED" message
                shard_aware_transport_port = cfg.native_shard_aware_transport_port();
            }
            std::optional<uint16_t> shard_aware_transport_port_ssl;
            if (cfg.native_shard_aware_transport_port_ssl.is_set()) {
                // Needed for "SUPPORTED" message
                shard_aware_transport_port_ssl = cfg.native_shard_aware_transport_port_ssl();
            }
            return cql_server_config {
              .timeout_config = updateable_timeout_config(cfg),
              .max_request_size = _mem_limiter.local().total_memory(),
              .partitioner_name = cfg.partitioner(),
              .sharding_ignore_msb = cfg.murmur3_partitioner_ignore_msb_bits(),
              .shard_aware_transport_port = shard_aware_transport_port,
              .shard_aware_transport_port_ssl = shard_aware_transport_port_ssl,
              .allow_shard_aware_drivers = cfg.enable_shard_aware_drivers(),
              .bounce_request_smp_service_group = bounce_request_smp_service_group,
            };
        });

        cserver->start(std::ref(_qp), std::ref(_auth_service), std::ref(_mem_limiter), std::move(get_cql_server_config), std::ref(cfg), std::ref(_sl_controller), std::ref(_gossiper), _cql_opcode_stats_key, _used_by_maintenance_socket).get();
        auto on_error = defer([&cserver] { cserver->stop().get(); });

        subscribe_server(*cserver).get();
        auto on_error_unsub = defer([this, &cserver] {
            unsubscribe_server(*cserver).get();
        });

        _listen_addresses.clear();
        if (!_used_by_maintenance_socket) {
            start_listening_on_tcp_sockets(*cserver).get();
        } else {
            start_listening_on_maintenance_socket(*cserver).get();
        }

        if (!_used_by_maintenance_socket) {
            set_cql_ready(true).get();
        }

        on_error.cancel();
        on_error_unsub.cancel();
        _server = std::move(cserver);
    });
}

future<> controller::stop_server() {
    SCYLLA_ASSERT(this_shard_id() == 0);

    if (!_stopped) {
        co_await _ops_sem.wait();
        _stopped = true;
        _ops_sem.broken();
        _listen_addresses.clear();
        co_await do_stop_server();
        co_await _bg_stops.close();
    }
}

future<> controller::request_stop_server() {
    if (!_ops_sem.try_wait()) {
        throw std::runtime_error(format("CQL server is starting, try again later"));
    }

    return with_scheduling_group(_sched_group, [this] {
        return do_stop_server();
    }).finally([this] { _ops_sem.signal(); });
}

future<> controller::do_stop_server() {
    auto cserver = std::move(_server);
    if (!cserver) {
        co_return;
    }

    std::exception_ptr ex;

    try {
        co_await set_cql_ready(false);
    } catch (...) {
        ex = std::current_exception();
    }

    auto& server = *cserver;

    try {
        co_await unsubscribe_server(server);
        co_await server.invoke_on_all([] (auto& s) { return s.shutdown(); });
    } catch (...) {
        if (!ex) {
            ex = std::current_exception();
        }
    }

    (void)server.stop().finally([s = std::move(cserver), h = _bg_stops.hold()] {});

    if (ex) {
        std::rethrow_exception(std::move(ex));
    }

    logger.info("CQL server stopped");
}

future<> controller::subscribe_server(sharded<cql_server>& server) {
    return server.invoke_on_all([this] (cql_server& server) -> future<> {
        _mnotifier.local().register_listener(server.get_migration_listener());
        _lifecycle_notifier.local().register_subscriber(server.get_lifecycle_listener());
        if (!_used_by_maintenance_socket) {
            _sl_controller.local().register_subscriber(server.get_qos_configuration_listener());
        }
        co_return;
    });
}

future<> controller::unsubscribe_server(sharded<cql_server>& server) {
    return server.invoke_on_all([this] (cql_server& server) -> future<> {
        co_await _mnotifier.local().unregister_listener(server.get_migration_listener());
        co_await _lifecycle_notifier.local().unregister_subscriber(server.get_lifecycle_listener());
        if (!_used_by_maintenance_socket) {
            co_await _sl_controller.local().unregister_subscriber(server.get_qos_configuration_listener());
        }
    });
}

future<> controller::set_cql_ready(bool ready) {
    return _gossiper.local().add_local_application_state(gms::application_state::RPC_READY, gms::versioned_value::cql_ready(ready));
}

future<utils::chunked_vector<client_data>> controller::get_client_data() {
    return _server ? _server->local().get_client_data() : protocol_server::get_client_data();
}

future<std::vector<connection_service_level_params>> controller::get_connections_service_level_params() {
    if (!_server) {
        co_return std::vector<connection_service_level_params>();
    }

    auto sl_params_vectors = co_await _server->map([] (cql_server& server) {
        return server.get_connections_service_level_params();
    });    
    std::vector<connection_service_level_params> sl_params;
    for (auto& vec: sl_params_vectors) {
        sl_params.insert(sl_params.end(), std::make_move_iterator(vec.begin()), std::make_move_iterator(vec.end()));
    }
    co_return sl_params;
}

} // namespace cql_transport
