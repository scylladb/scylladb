/*
 * Copyright (C) 2020 ScyllaDB
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

#include "transport/controller.hh"
#include "transport/server.hh"
#include "service/storage_service.hh"
#include "database.hh"
#include "db/config.hh"
#include "gms/gossiper.hh"
#include "log.hh"

using namespace seastar;

namespace cql_transport {

static logging::logger logger("cql_server_controller");

controller::controller(distributed<database>& db, sharded<auth::service>& auth, sharded<service::migration_notifier>& mn, gms::gossiper& gossiper)
    : _ops_sem(1)
    , _db(db)
    , _auth_service(auth)
    , _mnotifier(mn)
    , _gossiper(gossiper) {
}

future<> controller::start_server() {
    return smp::submit_to(0, [this] {
        if (!_ops_sem.try_wait()) {
            throw std::runtime_error(format("CQL server is stopping, try again later"));
        }

        return do_start_server().finally([this] { _ops_sem.signal(); });
    });
}

future<> controller::do_start_server() {
    if (_server) {
        return make_ready_future<>();
    }

    return seastar::async([this] {
        _server = std::make_unique<distributed<cql_transport::cql_server>>();
        auto cserver = &*_server;

        auto& cfg = _db.local().get_config();
        auto addr = cfg.rpc_address();
        auto preferred = cfg.rpc_interface_prefer_ipv6() ? std::make_optional(net::inet_address::family::INET6) : std::nullopt;
        auto family = cfg.enable_ipv6_dns_lookup() || preferred ? std::nullopt : std::make_optional(net::inet_address::family::INET);
        auto ceo = cfg.client_encryption_options();
        auto keepalive = cfg.rpc_keepalive();
        cql_transport::cql_server_config cql_server_config;
        cql_server_config.timeout_config = make_timeout_config(cfg);
        cql_server_config.max_request_size = service::get_local_storage_service()._service_memory_total;
        cql_server_config.max_concurrent_requests = cfg.max_concurrent_requests_per_shard;
        cql_server_config.get_service_memory_limiter_semaphore = [ss = std::ref(service::get_storage_service())] () -> semaphore& { return ss.get().local()._service_memory_limiter; };
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
        const seastar::net::inet_address ip = gms::inet_address::lookup(addr, family, preferred).get0();
        cserver->start(std::ref(cql3::get_query_processor()), std::ref(_auth_service), std::ref(_mnotifier), cql_server_config).get();
        struct listen_cfg {
            socket_address addr;
            bool is_shard_aware;
            std::shared_ptr<seastar::tls::credentials_builder> cred;
        };

        std::vector<listen_cfg> configs;

        if (cfg.native_transport_port() != 0) {
            configs.push_back(listen_cfg{ socket_address{ip, cfg.native_transport_port()}, false });
        }
        if (cfg.native_shard_aware_transport_port.is_set()) {
            configs.push_back(listen_cfg{ socket_address{ip, cfg.native_shard_aware_transport_port()}, true });
        }

        // main should have made sure values are clean and neatish
        if (ceo.at("enabled") == "true") {
            auto cred = std::make_shared<seastar::tls::credentials_builder>();

            cred->set_dh_level(seastar::tls::dh_params::level::MEDIUM);
            cred->set_priority_string(db::config::default_tls_priority);

            if (ceo.contains("priority_string")) {
                cred->set_priority_string(ceo.at("priority_string"));
            }
            if (ceo.contains("require_client_auth") && ceo.at("require_client_auth") == "true") {
                cred->set_client_auth(seastar::tls::client_auth::REQUIRE);
            }

            cred->set_x509_key_file(ceo.at("certificate"), ceo.at("keyfile"), seastar::tls::x509_crt_format::PEM).get();

            if (ceo.contains("truststore")) {
                cred->set_x509_trust_file(ceo.at("truststore"), seastar::tls::x509_crt_format::PEM).get();
            }

            logger.info("Enabling encrypted CQL connections between client and server");

            if (cfg.native_transport_port_ssl.is_set() && cfg.native_transport_port_ssl() != cfg.native_transport_port()) {
                configs.emplace_back(listen_cfg{{ip, cfg.native_transport_port_ssl()}, false, cred});
            } else {
                configs[0].cred = cred;
            }
            if (cfg.native_shard_aware_transport_port_ssl.is_set() && cfg.native_shard_aware_transport_port_ssl() != cfg.native_shard_aware_transport_port()) {
                configs.emplace_back(listen_cfg{{ip, cfg.native_shard_aware_transport_port_ssl()}, true, std::move(cred)});
            } else if (cfg.native_shard_aware_transport_port.is_set()) {
                configs[1].cred = std::move(cred);
            }
        }

        parallel_for_each(configs, [cserver, keepalive](const listen_cfg & cfg) {
            return cserver->invoke_on_all(&cql_transport::cql_server::listen, cfg.addr, cfg.cred, cfg.is_shard_aware, keepalive).then([cfg] {
                logger.info("Starting listening for CQL clients on {} ({}, {})"
                        , cfg.addr, cfg.cred ? "encrypted" : "unencrypted", cfg.is_shard_aware ? "shard-aware" : "non-shard-aware"
                );
            });
        }).get();

        set_cql_ready(true).get();
    });
}

future<> controller::stop() {
    assert(this_shard_id() == 0);

    if (_stopped) {
        return make_ready_future<>();
    }

    return _ops_sem.wait().then([this] {
        _stopped = true;
        _ops_sem.broken();
        return do_stop_server();
    });
}

future<> controller::stop_server() {
    return smp::submit_to(0, [this] {
        if (!_ops_sem.try_wait()) {
            throw std::runtime_error(format("CQL server is starting, try again later"));
        }

        return do_stop_server().finally([this] { _ops_sem.signal(); });
    });
}

future<> controller::do_stop_server() {
    return do_with(std::move(_server), [this] (std::unique_ptr<distributed<cql_transport::cql_server>>& cserver) {
        if (cserver) {
            // FIXME: cql_server::stop() doesn't kill existing connections and wait for them
            return set_cql_ready(false).then([&cserver] {
                return cserver->stop().then([] {
                    logger.info("CQL server stopped");
                });
            });
        }
        return make_ready_future<>();
    });
}

future<bool> controller::is_server_running() {
    return smp::submit_to(0, [this] {
        return make_ready_future<bool>(bool(_server));
    });
}

future<> controller::set_cql_ready(bool ready) {
    return _gossiper.add_local_application_state(gms::application_state::RPC_READY, gms::versioned_value::cql_ready(ready));
}

} // namespace cql_transport
