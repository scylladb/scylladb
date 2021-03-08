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

#include "redis/server.hh"

#include "redis/request.hh"
#include "redis/reply.hh"

#include "auth/authenticator.hh"
#include "db/config.hh"
#include "db/consistency_level_type.hh"
#include "db/write_type.hh"
#include "exceptions/exceptions.hh"
#include "service/query_state.hh"
#include "service/storage_service.hh"

#include <seastar/core/future-util.hh>
#include <seastar/core/seastar.hh>
#include <seastar/net/byteorder.hh>
#include <seastar/core/execution_stage.hh>

#include <cassert>
#include <string>
#include <unordered_map>

namespace redis_transport {

static logging::logger logging("redis_server");

redis_server::redis_server(seastar::sharded<service::storage_proxy>& proxy, seastar::sharded<redis::query_processor>& qp, auth::service& auth_service, redis_server_config config)
    : server(logging)
    , _proxy(proxy)
    , _query_processor(qp)
    , _config(config)
    , _max_request_size(config._max_request_size)
    , _memory_available(_max_request_size)
    , _auth_service(auth_service)
    , _total_redis_db_count(config._total_redis_db_count)
{
}

future<> redis_server::listen(socket_address addr, std::shared_ptr<seastar::tls::credentials_builder> creds, bool keepalive) {
    auto f = make_ready_future<shared_ptr<seastar::tls::server_credentials>>(nullptr);
    if (creds) {
        f = creds->build_reloadable_server_credentials([](const std::unordered_set<sstring>& files, std::exception_ptr ep) {
            if (ep) {
                logging.warn("Exception loading {}: {}", files, ep);
            } else {
                logging.info("Reloaded {}", files);
            }
        });
    }
    return f.then([this, addr, keepalive](shared_ptr<seastar::tls::server_credentials> creds) {
        listen_options lo;
        lo.reuse_address = true;
        server_socket ss;
        try {
            ss = creds
                ? seastar::tls::listen(std::move(creds), addr, lo)
                : seastar::listen(addr, lo);
        } catch (...) {
            throw std::runtime_error(sprint("Redis server error while listening on %s -> %s", addr, std::current_exception()));
        }
        _listeners.emplace_back(std::move(ss));
        _stopped = when_all(std::move(_stopped), do_accepts(_listeners.size() - 1, keepalive, addr)).discard_result();
    });
}

shared_ptr<generic_server::connection>
redis_server::make_connection(socket_address server_addr, connected_socket&& fd, socket_address addr) {
    auto conn = make_shared<connection>(*this, server_addr, std::move(fd), std::move(addr));
    ++_stats._connects;
    ++_stats._connections;
    return conn;
}

future<>
redis_server::unadvertise_connection(shared_ptr<generic_server::connection> raw_conn) {
    --_stats._connections;
    return make_ready_future<>();
}

future<redis_server::result> redis_server::connection::process_request_one(redis::request&& request, redis::redis_options& opts, service_permit permit) {
    return futurize_invoke([this, request = std::move(request), &opts, permit] () mutable {
        return _server._query_processor.local().process(std::move(request), seastar::ref(opts), permit).then([] (auto&& message) {
            return make_ready_future<redis_server::result> (std::move(message));
        });
    });
}

redis_server::connection::connection(redis_server& server, socket_address server_addr, connected_socket&& fd, socket_address addr)
    : generic_server::connection(server, std::move(fd))
    , _server(server)
    , _server_addr(server_addr)
    , _options(server._config._read_consistency_level, server._config._write_consistency_level, server._config._timeout_config, server._auth_service, addr, server._total_redis_db_count)
{
}

redis_server::connection::~connection() {
}

thread_local redis_server::connection::execution_stage_type redis_server::connection::_process_request_stage {"redis_transport", &connection::process_request_one};

future<redis_server::result> redis_server::connection::process_request_internal() {
    return _process_request_stage(this, _parser.get_request(), seastar::ref(_options), empty_service_permit());
}

void redis_server::connection::write_reply(const redis_exception& e)
{
    _ready_to_respond = _ready_to_respond.then([this, exception_message = e.what_message()] () mutable {
        return redis_message::exception(exception_message).then([this] (auto&& result) {
            auto m = result.message();
            return _write_buf.write(std::move(*m)).then([this] {
                return _write_buf.flush();
            });
        });
    });
}

void redis_server::connection::write_reply(redis_server::result result)
{
    _ready_to_respond = _ready_to_respond.then([this, result = std::move(result)] () mutable {
        auto m = result.make_message();
        return _write_buf.write(std::move(*m)).then([this] {
            return _write_buf.flush();
        });
    });
}

future<> redis_server::connection::process_request() {
    _parser.init();
    return _read_buf.consume(_parser).then([this] {
        if (_parser.eof()) {
            return make_ready_future<>();
        }
        ++_server._stats._requests_serving;
        _pending_requests_gate.enter();
        utils::latency_counter lc;
        lc.start();
        auto leave = defer([this] { _pending_requests_gate.leave(); });
        return process_request_internal().then([this, leave = std::move(leave), lc = std::move(lc)] (auto&& result) mutable {
            --_server._stats._requests_serving;
            try {
                if (_parser.failed()) {
                    logging.error("request parse failed");
                    const auto e = redis_exception("unknown command ''");
                    write_reply(std::move(e));
                }else{
                    write_reply(std::move(result));
                }
                ++_server._stats._requests_served;
                _server._stats._requests.mark(lc.stop().latency());
                if (lc.is_start()) {
                    _server._stats._estimated_requests_latency.add(lc.latency(), _server._stats._requests.hist.count);
                }
            } catch (...) {
                logging.error("request processing failed: {}", std::current_exception());
            }
        });
    });
}

void redis_server::connection::handle_error(future<>&& f) {
    try {
        f.get();
    }
    catch (redis_exception& e) {
        write_reply(e);
    }
    catch (std::exception& e) {
        write_reply(redis_exception { e.what() });
    }
    catch (...) {
        write_reply(redis_exception { "Unknown exception" });
    }
}

}

db::consistency_level make_consistency_level(const sstring& level)
{
    std::unordered_map<sstring, db::consistency_level> consistency_levels {
        {"ONE", db::consistency_level::ONE},
        {"QUORUM", db::consistency_level::QUORUM},
        {"LOCAL_QUORUM", db::consistency_level::LOCAL_QUORUM},
        {"EACH_QUORUM", db::consistency_level::EACH_QUORUM},
        {"ALL", db::consistency_level::ALL},
        {"ANY", db::consistency_level::ANY},
        {"TWO", db::consistency_level::TWO},
        {"THREE", db::consistency_level::THREE},
        {"SERIAL", db::consistency_level::SERIAL},
        {"LOCAL_SERIAL", db::consistency_level::LOCAL_SERIAL},
        {"LOCAL_ONE", db::consistency_level::LOCAL_ONE},
    };
    auto iter_cl = consistency_levels.find(level);
    if (iter_cl == consistency_levels.end()) {
        return db::consistency_level::ONE;
    }
    return iter_cl->second;
}
