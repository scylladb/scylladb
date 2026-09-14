/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "utils/assert.hh"
#include <grp.h>
#include "transport/controller.hh"
#include <seastar/core/sharded.hh>
#include <seastar/net/socket_defs.hh>
#include <seastar/net/unix_address.hh>
#include <seastar/core/file-types.hh>
#include <ranges>
#include <seastar/core/with_scheduling_group.hh>
#include "transport/server.hh"
#include "service/memory_limiter.hh"
#include "db/config.hh"
#include "gms/gossiper.hh"
#include "utils/log.hh"
#include "cql3/query_processor.hh"
#include "message/messaging_service.hh"
#include <seastar/core/file.hh>
#include <seastar/core/file.hh>

using namespace seastar;

namespace cql_transport {

static logging::logger logger("cql_server_controller");

controller::controller(sharded<auth::service>& auth, sharded<service::migration_notifier>& mn,
        sharded<gms::gossiper>& gossiper, sharded<cql3::query_processor>& qp, sharded<service::memory_limiter>& ml,
        sharded<qos::service_level_controller>& sl_controller, sharded<service::endpoint_lifecycle_notifier>& elc_notif,
        sharded<netw::messaging_service>& ms, sharded<updateable_timeout_config>& timeout_config,
        const db::config& cfg, db::listener_configs listeners, scheduling_group_key cql_opcode_stats_key,
        maintenance_socket_enabled used_by_maintenance_socket,
        seastar::scheduling_group sg)
    : protocol_server(sg)
    , _listeners(std::move(listeners))
    , _ops_sem(1)
    , _bg_stops("transport::controller::bg_stops")
    , _auth_service(auth)
    , _mnotifier(mn)
    , _lifecycle_notifier(elc_notif)
    , _gossiper(gossiper)
    , _qp(qp)
    , _mem_limiter(ml)
    , _sl_controller(sl_controller)
    , _messaging(ms)
    , _timeout_config(timeout_config)
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

static future<> listen_on_all_shards(sharded<cql_server>& cserver, const sstring& name, socket_address addr, cql_listener_config lcfg, std::shared_ptr<seastar::tls::credentials_builder> creds, bool is_shard_aware, bool keepalive, std::optional<file_permissions> unix_domain_socket_permissions, bool proxy_protocol = false) {
    co_await cserver.invoke_on_all([name, addr, lcfg, creds, is_shard_aware, keepalive, unix_domain_socket_permissions, proxy_protocol] (cql_server& server) {
        return server.listen(name, addr, lcfg, creds, is_shard_aware, keepalive, unix_domain_socket_permissions, proxy_protocol, [&c = server.container()]() -> generic_server::server& { return c.local(); });
    });
}

static future<> unlisten_on_all_shards(sharded<cql_server>& cserver, const sstring& name) {
    co_await cserver.invoke_on_all([name] (cql_server& server) {
        return server.unlisten(name);
    });
}

// The shard-aware listener a listener hands its clients over to, resolved by
// name into what the accepted connections need to advertise.
//
// resolved_addresses maps every listener address, as spelled in the
// configuration, to the address it resolves to.
cql_listener_config controller::shard_aware_sibling(const sstring& name, const db::listener_config& listener,
        const db::listener_configs& listeners,
        const std::unordered_map<sstring, net::inet_address>& resolved_addresses) const {
    if (listener.shard_aware_listener.empty()) {
        return {};
    }
    auto sibling = listeners.find(listener.shard_aware_listener);
    if (sibling == listeners.end()) {
        throw std::runtime_error(fmt::format("Listener {}: shard_aware_listener names a listener that doesn't serve CQL: {}",
                name, listener.shard_aware_listener));
    }
    if (!sibling->second.shard_aware) {
        throw std::runtime_error(fmt::format("Listener {}: shard_aware_listener names a listener that isn't shard-aware: {}",
                name, listener.shard_aware_listener));
    }
    // The SUPPORTED message advertises a port, not an address: a client that
    // takes up the offer reconnects to the address it is already connected to.
    // So a shard-aware listener bound to a different address would send the
    // client to a port where nothing listens. Compare the resolved addresses,
    // since two spellings of the same address are the same address.
    auto address_of = [&] (const db::listener_config& l) {
        return resolved_addresses.at(l.address.empty() ? _config.rpc_address() : l.address);
    };
    if (address_of(sibling->second) != address_of(listener)) {
        throw std::runtime_error(fmt::format("Listener {}: shard_aware_listener names a listener that listens on a different address: {}",
                name, listener.shard_aware_listener));
    }
    return cql_listener_config{
        .shard_aware_port = sibling->second.port,
        .shard_aware_port_tls = sibling->second.tls,
    };
}

future<std::vector<controller::listen_cfg>> controller::resolve_listeners(const db::listener_configs& listeners) const {
    auto& cfg = _config;
    auto preferred = cfg.rpc_interface_prefer_ipv6() ? std::make_optional(net::inet_address::family::INET6) : std::nullopt;
    auto family = cfg.enable_ipv6_dns_lookup() || preferred ? std::nullopt : std::make_optional(net::inet_address::family::INET);
    auto default_keepalive = cfg.rpc_keepalive();

    std::vector<listen_cfg> configs;

    // Listeners sharing the same address, or the same set of TLS options, share
    // the resolved address and the credentials built out of those options.
    std::unordered_map<sstring, net::inet_address> resolved_addresses;
    std::vector<std::pair<db::config::string_map, std::shared_ptr<seastar::tls::credentials_builder>>> creds;

    // Resolve every listener's address first, so that a listener can be checked
    // against the address of the shard-aware listener it names, whether that
    // listener comes before or after it.
    for (auto& [name, l] : listeners) {
        auto addr_name = l.address.empty() ? cfg.rpc_address() : l.address;
        auto [addr_it, inserted] = resolved_addresses.try_emplace(addr_name);
        if (inserted) {
            addr_it->second = co_await utils::resolve(addr_name, "listener address", family, preferred);
        }
    }

    for (auto& [name, l] : listeners) {
        auto addr = resolved_addresses.at(l.address.empty() ? cfg.rpc_address() : l.address);

        std::shared_ptr<seastar::tls::credentials_builder> cred;
        db::config::string_map tls_options;
        if (l.tls) {
            // An empty set of options means the CQL-wide defaults.
            auto opts = l.tls_options.empty() ? cfg.client_encryption_options() : l.tls_options;
            // "enabled" is the legacy on/off switch of client_encryption_options,
            // which a listener replaces with its own "tls" key. It is meaningless
            // once TLS was selected per listener, and configure_tls_creds_builder()
            // doesn't know it, so drop it.
            opts.erase("enabled");
            tls_options = opts;
            auto it = std::ranges::find_if(creds, [&] (auto& c) { return c.first == opts; });
            if (it == creds.end()) {
                cred = std::make_shared<seastar::tls::credentials_builder>();
                co_await utils::configure_tls_creds_builder(*cred, opts);
                creds.emplace_back(std::move(opts), cred);
                logger.info("Enabling encrypted CQL connections between client and server");
            } else {
                cred = it->second;
            }
        }

        configs.emplace_back(listen_cfg{
            .name = name,
            .addr = socket_address{addr, l.port},
            .lcfg = shard_aware_sibling(name, l, listeners, resolved_addresses),
            .is_shard_aware = l.shard_aware,
            .cred = std::move(cred),
            .tls_options = std::move(tls_options),
            .proxy_protocol = l.proxy_protocol,
            .keepalive = l.keepalive.value_or(default_keepalive),
        });
    }
    co_return configs;
}

future<> controller::start_listening_on_tcp_sockets(sharded<cql_server>& cserver) {
    auto configs = co_await resolve_listeners(_listeners);

    _listen_addresses.clear();
    _served.clear();
    for (auto& cfg : configs) {
        _listen_addresses.push_back(cfg.addr);
        _served.emplace(cfg.name, cfg);
    }

    co_await parallel_for_each(configs, [&cserver](const listen_cfg & cfg) -> future<> {
        co_await listen_on_all_shards(cserver, cfg.name, cfg.addr, cfg.lcfg, cfg.cred, cfg.is_shard_aware, cfg.keepalive, std::nullopt, cfg.proxy_protocol);

        logger.info("Starting listening for CQL clients on {} as {} ({}, {}{})"
                , cfg.addr, cfg.name, cfg.cred ? "encrypted" : "unencrypted", cfg.is_shard_aware ? "shard-aware" : "non-shard-aware"
                , cfg.proxy_protocol ? ", proxy-protocol" : ""
        );
    });
}

future<> controller::apply_listeners(db::listener_configs listeners) {
    auto units = co_await get_units(_ops_sem, 1);

    if (!_server) {
        // Nothing is being served, so there is nothing to reconcile: remember the set, and
        // let start_server() open it when it runs.
        _listeners = std::move(listeners);
        co_return;
    }

    // Resolve the whole set first. This is everything that can fail - a name that does not
    // resolve, a certificate that cannot be read, a shard-aware sibling that is missing or
    // on another address - and until it has succeeded the server keeps serving what it was.
    auto resolved = co_await resolve_listeners(listeners);
    std::unordered_map<sstring, listen_cfg> wanted;
    for (auto& cfg : resolved) {
        wanted.emplace(cfg.name, std::move(cfg));
    }

    auto& cserver = *_server;

    // Compare what was resolved, not what was configured: a listener whose own
    // configuration did not change still has to be reopened if the shard-aware sibling it
    // advertises did, since that is part of what its connections are told.
    auto unchanged = [&] (const sstring& name) {
        auto it = wanted.find(name);
        return it != wanted.end() && it->second == _served.at(name);
    };

    // Take away the listeners that are gone or have changed. This drains their
    // connections, which is why a listener that did not change is left alone: its clients
    // should not pay for a change to a different listener.
    for (const auto& [name, cfg] : _served) {
        if (unchanged(name)) {
            continue;
        }
        logger.info("Stopping listening for CQL clients on {} as {}", cfg.addr, name);
        co_await unlisten_on_all_shards(cserver, name);
    }

    // Then open the ones that are new, and reopen the ones that changed.
    for (const auto& [name, cfg] : wanted) {
        if (_served.contains(name) && unchanged(name)) {
            continue;
        }
        co_await listen_on_all_shards(cserver, cfg.name, cfg.addr, cfg.lcfg, cfg.cred,
                cfg.is_shard_aware, cfg.keepalive, std::nullopt, cfg.proxy_protocol);
        logger.info("Starting listening for CQL clients on {} as {} ({}, {}{})",
                cfg.addr, cfg.name, cfg.cred ? "encrypted" : "unencrypted",
                cfg.is_shard_aware ? "shard-aware" : "non-shard-aware",
                cfg.proxy_protocol ? ", proxy-protocol" : "");
    }

    _listeners = std::move(listeners);
    _served = std::move(wanted);
    _listen_addresses.clear();
    for (const auto& [name, cfg] : _served) {
        _listen_addresses.push_back(cfg.addr);
    }
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

    auto file_exists = co_await seastar::file_exists(socket.c_str());
    if (file_exists) {
        auto f_stat = co_await seastar::file_stat(socket.c_str());
        if (!S_ISSOCK(f_stat.mode)) {
            throw std::runtime_error(format("Under maintenance socket path ({}) there is something else.", socket));
        }

        // Remove the socket if it already exists, otherwise when the server
        // tries to listen on it, it will hang on bind().
        co_await seastar::remove_file(socket.c_str());
    }

    auto addr = socket_address { unix_domain_addr { socket } };
    _listen_addresses.push_back(addr);

    logger.info("Setting up maintenance socket on {}", socket);

    auto unix_domain_socket_permissions =
        file_permissions::user_read | file_permissions::user_write |
        file_permissions::group_read | file_permissions::group_write;

    co_await listen_on_all_shards(cserver, "maintenance", addr, cql_listener_config{}, nullptr, false, _config.rpc_keepalive(), unix_domain_socket_permissions);

    if (_config.maintenance_socket_group.is_set()) {
        auto group_name = _config.maintenance_socket_group();
        std::optional<struct group_details> grp = co_await seastar::getgrnam(group_name.c_str());

        if (!grp.has_value()) {
            throw std::runtime_error(format("Group id of {} not found. Make sure the group exists.", group_name));
        }

        try {
            co_await seastar::chown(socket.c_str(), ::geteuid(), grp.value().group_id);
        } catch(std::system_error& e) {
            if (e.code().value() == EPERM) {
                throw std::runtime_error(format("Failed to change group of {}: Permission denied. Make sure the user has the root privilege or is a member of the group {}.", socket, group_name));
            } else {
                throw std::runtime_error(format("Failed to chown {}: {} ()", socket, strerror(e.code().value())));
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
            return cql_server_config {
              .timeout_config = _timeout_config.local(),
              .max_request_size = _mem_limiter.local().total_memory(),
              .partitioner_name = cfg.partitioner(),
              .sharding_ignore_msb = cfg.murmur3_partitioner_ignore_msb_bits(),
              .allow_shard_aware_drivers = cfg.enable_shard_aware_drivers(),
              .bounce_request_smp_service_group = bounce_request_smp_service_group,
              .max_concurrent_requests = cfg.max_concurrent_requests_per_shard,
              .cql_duplicate_bind_variable_names_refer_to_same_variable = cfg.cql_duplicate_bind_variable_names_refer_to_same_variable,
              .max_relations_in_where_clause = cfg.max_relations_in_where_clause,
              .cql_in_bind_variable_name_uses_uppercase_operator = cfg.cql_in_bind_variable_name_uses_uppercase_operator,
              .uninitialized_connections_semaphore_cpu_concurrency = cfg.uninitialized_connections_semaphore_cpu_concurrency,
              .request_timeout_on_shutdown_in_seconds = cfg.request_timeout_on_shutdown_in_seconds
            };
        });

        cserver->start(std::ref(_qp), std::ref(_auth_service), std::ref(_mem_limiter), std::move(get_cql_server_config), std::ref(_sl_controller), std::ref(_gossiper), _cql_opcode_stats_key, _used_by_maintenance_socket, std::ref(_messaging)).get();
        auto on_error = defer([&cserver] noexcept { cserver->stop().get(); });

        subscribe_server(*cserver).get();
        auto on_error_unsub = defer([this, &cserver] noexcept {
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

future<utils::chunked_vector<foreign_ptr<std::unique_ptr<client_data>>>> controller::get_client_data() {
    return _server ? _server->local().get_client_data() : protocol_server::get_client_data();
}

future<> controller::update_connections_scheduling_group() {
    if (!_server) {
        co_return;
    }

    co_await _server->invoke_on_all([] (auto& server) {
        return server.update_connections_scheduling_group();
    });
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
