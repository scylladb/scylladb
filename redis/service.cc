/*
 * Copyright (C) 2019 pengjian.uestc @ gmail.com
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

#include "timeout_config.hh"
#include "redis/service.hh"
#include "redis/keyspace_utils.hh"
#include "redis/server.hh"
#include "db/config.hh"
#include "log.hh"
#include "auth/common.hh"
#include "database.hh"

static logging::logger slogger("redis_service");

redis_service::redis_service()
{
}

redis_service::~redis_service()
{
}

future<> redis_service::listen(distributed<auth::service>& auth_service, db::config& cfg)
{
    if (_server) {
        return make_ready_future<>();
    }
    auto server = make_shared<distributed<redis_transport::redis_server>>();
    _server = server;

    auto addr = cfg.rpc_address();
    auto preferred = cfg.rpc_interface_prefer_ipv6() ? std::make_optional(net::inet_address::family::INET6) : std::nullopt;
    auto family = cfg.enable_ipv6_dns_lookup() || preferred ? std::nullopt : std::make_optional(net::inet_address::family::INET);
    auto ceo = cfg.client_encryption_options();
    auto keepalive = cfg.rpc_keepalive();
    redis_transport::redis_server_config redis_cfg;
    redis_cfg._timeout_config = make_timeout_config(cfg);
    redis_cfg._read_consistency_level = make_consistency_level(cfg.redis_read_consistency_level());
    redis_cfg._write_consistency_level = make_consistency_level(cfg.redis_write_consistency_level());
    redis_cfg._max_request_size = memory::stats().total_memory() / 10;
    redis_cfg._total_redis_db_count = cfg.redis_database_count();
    return gms::inet_address::lookup(addr, family, preferred).then([this, server, addr, &cfg, keepalive, ceo = std::move(ceo), redis_cfg, &auth_service] (seastar::net::inet_address ip) {
        return server->start(std::ref(service::get_storage_proxy()), std::ref(_query_processor), std::ref(auth_service), redis_cfg).then([server, &cfg, addr, ip, ceo, keepalive]() {
            auto f = make_ready_future();
            struct listen_cfg {
                socket_address addr;
                std::shared_ptr<seastar::tls::credentials_builder> cred;
            };

            std::vector<listen_cfg> configs;
            if (cfg.redis_port()) {
                configs.emplace_back(listen_cfg { {socket_address{ip, cfg.redis_port()}} });
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

                f = cred->set_x509_key_file(ceo.at("certificate"), ceo.at("keyfile"), seastar::tls::x509_crt_format::PEM);

                if (ceo.contains("truststore")) {
                    f = f.then([cred, f = ceo.at("truststore")] { return cred->set_x509_trust_file(f, seastar::tls::x509_crt_format::PEM); });
                }

                slogger.info("Enabling encrypted REDIS connections between client and server");

                if (cfg.redis_ssl_port() && cfg.redis_ssl_port() != cfg.redis_port()) {
                    configs.emplace_back(listen_cfg{{ip, cfg.redis_ssl_port()}, std::move(cred)});
                } else {
                    configs.back().cred = std::move(cred);
                }
            }

            return f.then([server, configs = std::move(configs), keepalive] {
                return parallel_for_each(configs, [server, keepalive](const listen_cfg & cfg) {
                    return server->invoke_on_all(&redis_transport::redis_server::listen, cfg.addr, cfg.cred, keepalive).then([cfg] {
                        slogger.info("Starting listening for REDIS clients on {} ({})", cfg.addr, cfg.cred ? "encrypted" : "unencrypted");
                    });
                });
            });
        });
    });
}

future<> redis_service::init(distributed<service::storage_proxy>& proxy, distributed<database>& db, distributed<auth::service>& auth_service, db::config& cfg)
{
    // 1. Create keyspace/tables used by redis API if not exists.
    // 2. Initialize the redis query processor.
    // 3. Listen on the redis transport port.
    return redis::maybe_create_keyspace(cfg).then([this, &proxy, &db] {
        return _query_processor.start(std::ref(proxy), std::ref(db));
    }).then([this] {
        return _query_processor.invoke_on_all([] (auto& processor) {
            return processor.start();
        });
    }).then([this, &cfg, &auth_service] {
        return listen(auth_service, cfg);
    });
}

future<> redis_service::stop()
{
    // If the redis protocol disable, the redis_service::init is not
    // invoked at all. Do nothing if `_server is null.
    if (_server) {
        return _server->stop().then([this] {
            return _query_processor.stop();
        });
    }
    return make_ready_future<>();
}
