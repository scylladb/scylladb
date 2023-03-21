/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "transport/controller.hh"
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
        const db::config& cfg, scheduling_group_key cql_opcode_stats_key)
    : _ops_sem(1)
    , _auth_service(auth)
    , _mnotifier(mn)
    , _lifecycle_notifier(elc_notif)
    , _gossiper(gossiper)
    , _qp(qp)
    , _mem_limiter(ml)
    , _sl_controller(sl_controller)
    , _config(cfg)
    , _cql_opcode_stats_key(cql_opcode_stats_key)
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

future<> controller::do_start_server() {
    if (_server) {
        return make_ready_future<>();
    }

    return seastar::async([this] {
        auto cserver = std::make_unique<sharded<cql_server>>();

        auto& cfg = _config;
        auto preferred = cfg.rpc_interface_prefer_ipv6() ? std::make_optional(net::inet_address::family::INET6) : std::nullopt;
        auto family = cfg.enable_ipv6_dns_lookup() || preferred ? std::nullopt : std::make_optional(net::inet_address::family::INET);
        auto ceo = cfg.client_encryption_options();
        auto keepalive = cfg.rpc_keepalive();
        cql_server_config cql_server_config;
        cql_server_config.timeout_config = make_timeout_config(cfg);
        cql_server_config.max_request_size = _mem_limiter.local().total_memory();
        cql_server_config.allow_shard_aware_drivers = cfg.enable_shard_aware_drivers();
        cql_server_config.sharding_ignore_msb = cfg.murmur3_partitioner_ignore_msb_bits();
        if (cfg.native_shard_aware_transport_port.is_set()) {
            // Needed for "SUPPORTED" message
            cql_server_config.shard_aware_transport_port = cfg.native_shard_aware_transport_port();
        }
        if (cfg.native_shard_aware_transport_port_ssl.is_set()) {
            // Needed for "SUPPORTED" message
            cql_server_config.shard_aware_transport_port_ssl = cfg.native_shard_aware_transport_port_ssl();
        }
        cql_server_config.partitioner_name = cfg.partitioner();
        smp_service_group_config cql_server_smp_service_group_config;
        cql_server_smp_service_group_config.max_nonlocal_requests = 5000;
        cql_server_config.bounce_request_smp_service_group = create_smp_service_group(cql_server_smp_service_group_config).get0();
        const seastar::net::inet_address ip = utils::resolve(cfg.rpc_address, family, preferred).get0();

        struct listen_cfg {
            socket_address addr;
            bool is_shard_aware;
            std::shared_ptr<seastar::tls::credentials_builder> cred;
        };

        _listen_addresses.clear();
        std::vector<listen_cfg> configs;
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

        cserver->start(std::ref(_qp), std::ref(_auth_service), std::ref(_mem_limiter), cql_server_config, std::ref(cfg), std::ref(_sl_controller), std::ref(_gossiper), _cql_opcode_stats_key).get();
        auto on_error = defer([&cserver] { cserver->stop().get(); });

        subscribe_server(*cserver).get();
        auto on_error_unsub = defer([this, &cserver] {
            unsubscribe_server(*cserver).get();
        });

        parallel_for_each(configs, [&cserver, keepalive](const listen_cfg & cfg) {
            return cserver->invoke_on_all(&cql_server::listen, cfg.addr, cfg.cred, cfg.is_shard_aware, keepalive).then([cfg] {
                logger.info("Starting listening for CQL clients on {} ({}, {})"
                        , cfg.addr, cfg.cred ? "encrypted" : "unencrypted", cfg.is_shard_aware ? "shard-aware" : "non-shard-aware"
                );
            });
        }).get();

        set_cql_ready(true).get();

        on_error.cancel();
        on_error_unsub.cancel();
        _server = std::move(cserver);
    });
}

future<> controller::stop_server() {
    assert(this_shard_id() == 0);

    if (_stopped) {
        return make_ready_future<>();
    }

    return _ops_sem.wait().then([this] {
        _stopped = true;
        _ops_sem.broken();
        _listen_addresses.clear();
        return do_stop_server();
    });
}

future<> controller::request_stop_server() {
    if (!_ops_sem.try_wait()) {
        throw std::runtime_error(format("CQL server is starting, try again later"));
    }

    return do_stop_server().finally([this] { _ops_sem.signal(); });
}

future<> controller::do_stop_server() {
    return do_with(std::move(_server), [this] (std::unique_ptr<sharded<cql_server>>& cserver) {
        if (cserver) {
            // FIXME: cql_server::stop() doesn't kill existing connections and wait for them
            return set_cql_ready(false).finally([this, &cserver] {
                return unsubscribe_server(*cserver).then([&cserver] {
                    return cserver->stop().then([] {
                        logger.info("CQL server stopped");
                    });
                });
            });
        }
        return make_ready_future<>();
    });
}

future<> controller::subscribe_server(sharded<cql_server>& server) {
    return server.invoke_on_all([this] (cql_server& server) {
        _mnotifier.local().register_listener(server.get_migration_listener());
        _lifecycle_notifier.local().register_subscriber(server.get_lifecycle_listener());
        return make_ready_future<>();
    });
}

future<> controller::unsubscribe_server(sharded<cql_server>& server) {
    return server.invoke_on_all([this] (cql_server& server) {
        return _mnotifier.local().unregister_listener(server.get_migration_listener()).then([this, &server]{
            return _lifecycle_notifier.local().unregister_subscriber(server.get_lifecycle_listener());
        });
    });
}

future<> controller::set_cql_ready(bool ready) {
    return _gossiper.local().add_local_application_state(gms::application_state::RPC_READY, gms::versioned_value::cql_ready(ready));
}

future<utils::chunked_vector<client_data>> controller::get_client_data() {
    return _server ? _server->local().get_client_data() : protocol_server::get_client_data();
}

} // namespace cql_transport
