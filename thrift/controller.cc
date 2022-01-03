/*
 * Copyright (C) 2020-present ScyllaDB
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

#include "thrift/controller.hh"
#include "thrift/server.hh"
#include "replica/database.hh"
#include "db/config.hh"
#include "log.hh"

static logging::logger clogger("thrift_controller");

thrift_controller::thrift_controller(distributed<replica::database>& db, sharded<auth::service>& auth,
        sharded<cql3::query_processor>& qp, sharded<service::memory_limiter>& ml,
        sharded<service::storage_service>& ss, sharded<service::storage_proxy>& proxy)
    : _ops_sem(1)
    , _db(db)
    , _auth_service(auth)
    , _qp(qp)
    , _mem_limiter(ml)
    , _ss(ss)
    , _proxy(proxy)
{ }

sstring thrift_controller::name() const {
    return "rpc";
}

sstring thrift_controller::protocol() const {
    return "thrift";
}

sstring thrift_controller::protocol_version() const {
     return ::cassandra::thrift_version;
}

std::vector<socket_address> thrift_controller::listen_addresses() const {
    if (_server && _addr) {
        return {*_addr};
    }
    return {};
}

future<> thrift_controller::start_server() {
    if (!_ops_sem.try_wait()) {
        throw std::runtime_error(format("Thrift server is stopping, try again later"));
    }

    return do_start_server().finally([this] { _ops_sem.signal(); });
}

future<> thrift_controller::do_start_server() {
    if (_server) {
        return make_ready_future<>();
    }

    _server = std::make_unique<distributed<thrift_server>>();
    auto tserver = &*_server;

    _addr.reset();

    auto& cfg = _db.local().get_config();
    auto preferred = cfg.rpc_interface_prefer_ipv6() ? std::make_optional(net::inet_address::family::INET6) : std::nullopt;
    auto family = cfg.enable_ipv6_dns_lookup() || preferred ? std::nullopt : std::make_optional(net::inet_address::family::INET);
    auto keepalive = cfg.rpc_keepalive();
    thrift_server_config tsc;
    tsc.timeout_config = make_timeout_config(cfg);
    tsc.max_request_size = cfg.thrift_max_message_length_in_mb() * (uint64_t(1) << 20);
    return utils::resolve(cfg.rpc_address, family, preferred).then([this, tserver, port = cfg.rpc_port(), keepalive, tsc] (gms::inet_address ip) {
        _addr.emplace(ip, port);
        return tserver->start(std::ref(_db), std::ref(_qp), std::ref(_ss), std::ref(_proxy), std::ref(_auth_service), std::ref(_mem_limiter), tsc).then([tserver, port, ip, keepalive] {
            // #293 - do not stop anything
            //engine().at_exit([tserver] {
            //    return tserver->stop();
            //});
            return tserver->invoke_on_all(&thrift_server::listen, socket_address{ip, port}, keepalive);
        }).then([ip, port] {
            clogger.info("Thrift server listening on {}:{} ...", ip, port);
        });
    });
}

future<> thrift_controller::stop_server() {
    assert(this_shard_id() == 0);

    if (_stopped) {
        return make_ready_future<>();
    }

    return _ops_sem.wait().then([this] {
        _stopped = true;
        _ops_sem.broken();
        _addr.reset();
        return do_stop_server();
    });
}

future<> thrift_controller::request_stop_server() {
    if (!_ops_sem.try_wait()) {
        throw std::runtime_error(format("Thrift server is starting, try again later"));
    }

    return do_stop_server().finally([this] { _ops_sem.signal(); });
}

future<> thrift_controller::do_stop_server() {
    return do_with(std::move(_server), [this] (std::unique_ptr<distributed<thrift_server>>& tserver) {
        if (tserver) {
            return tserver->stop().then([] {
                clogger.info("Thrift server stopped");
            });
        }
        return make_ready_future<>();
    });
}
