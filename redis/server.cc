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
#include "service/storage_service.hh"
#include "db/consistency_level_type.hh"
#include "db/config.hh"
#include "db/write_type.hh"
#include <seastar/core/future-util.hh>
#include <seastar/core/seastar.hh>
#include <seastar/net/byteorder.hh>
#include <seastar/core/execution_stage.hh>
#include "service/query_state.hh"
#include "exceptions/exceptions.hh"
#include "auth/authenticator.hh"
#include <cassert>
#include <string>
#include "redis/request.hh"
#include "redis/reply.hh"
#include <unordered_map>

namespace redis_transport {

static logging::logger logging("redis_server");

redis_server::redis_server(distributed<service::storage_proxy>& proxy, distributed<redis::query_processor>& qp, auth::service& auth_service, redis_server_config config)
    : _proxy(proxy)
    , _query_processor(qp)
    , _config(config)
    , _max_request_size(config._max_request_size)
    , _memory_available(_max_request_size)
    , _auth_service(auth_service)
    , _total_redis_db_count(config._total_redis_db_count)
{
}

future<> redis_server::stop() {
    _stopping = true;
    size_t nr = 0;
    size_t nr_total = _listeners.size();
    logging.debug("redis_server: abort accept nr_total={}", nr_total);
    for (auto&& l : _listeners) {
        l.abort_accept();
        logging.debug("redis_server: abort accept {} out of {} done", ++nr, nr_total);
    }
    auto nr_conn = make_lw_shared<size_t>(0);
    auto nr_conn_total = _connections_list.size();
    logging.debug("redis_server: shutdown connection nr_total={}", nr_conn_total);
    return parallel_for_each(_connections_list.begin(), _connections_list.end(), [nr_conn, nr_conn_total] (auto&& c) {
        return c.shutdown().then([nr_conn, nr_conn_total] {
            logging.debug("redis_server: shutdown connection {} out of {} done", ++(*nr_conn), nr_conn_total);
        });
    }).then([this] {
        return std::move(_stopped);
    });
}

future<> redis_server::listen(socket_address addr, std::shared_ptr<seastar::tls::credentials_builder> creds, bool keepalive) {
    listen_options lo;
    lo.reuse_address = true;
    server_socket ss;
    try {
        ss = creds
          ? seastar::tls::listen(creds->build_server_credentials(), addr, lo)
          : seastar::listen(addr, lo);
    } catch (...) {
        throw std::runtime_error(sprint("Redis server error while listening on %s -> %s", addr, std::current_exception()));
    }
    _listeners.emplace_back(std::move(ss));
    _stopped = when_all(std::move(_stopped), do_accepts(_listeners.size() - 1, keepalive, addr)).discard_result();
    return make_ready_future<>();
}

future<> redis_server::do_accepts(int which, bool keepalive, socket_address server_addr) {
    return repeat([this, which, keepalive, server_addr] {
        ++_stats._connections_being_accepted;
        return _listeners[which].accept().then_wrapped([this, which, keepalive, server_addr] (future<accept_result> f_cs_sa) mutable {
            --_stats._connections_being_accepted;
            if (_stopping) {
                f_cs_sa.ignore_ready_future();
                maybe_idle();
                return stop_iteration::yes;
            }
            auto cs_sa = f_cs_sa.get0();
            auto fd = std::move(cs_sa.connection);
            auto addr = std::move(cs_sa.remote_address);
            fd.set_nodelay(true);
            fd.set_keepalive(keepalive);
            auto conn = make_shared<connection>(*this, server_addr, std::move(fd), std::move(addr));
            ++_stats._connects;
            ++_stats._connections;
            (void)conn->process().then_wrapped([this, conn] (future<> f) {
                --_stats._connections;
                try {
                    f.get();
                } catch (...) {
                    logging.debug("connection error: {}", std::current_exception());
                }
            });
            return stop_iteration::no;
        }).handle_exception([] (auto ep) {
            logging.debug("accept failed: {}", ep);
            return stop_iteration::no;
        });
    });
}

future<redis_server::result> redis_server::connection::process_request_one(redis::request&& request, redis::redis_options& opts, service_permit permit) {
    return futurize_invoke([this, request = std::move(request), &opts, permit] () mutable {
        return _server._query_processor.local().process(std::move(request), seastar::ref(opts), permit).then([] (auto&& message) {
            return make_ready_future<redis_server::result> (std::move(message));
        });
    });
}

redis_server::connection::connection(redis_server& server, socket_address server_addr, connected_socket&& fd, socket_address addr)
    : _server(server)
    , _server_addr(server_addr)
    , _fd(std::move(fd))
    , _read_buf(_fd.input())
    , _write_buf(_fd.output())
    , _options(server._config._read_consistency_level, server._config._write_consistency_level, server._config._timeout_config, server._auth_service, addr, server._total_redis_db_count)
{
    ++_server._stats._total_connections;
    ++_server._stats._current_connections;
    _server._connections_list.push_back(*this);
}

redis_server::connection::~connection() {
    --_server._stats._current_connections;
    _server._connections_list.erase(_server._connections_list.iterator_to(*this));
    _server.maybe_idle();
}

future<> redis_server::connection::process()
{
    return do_until([this] {
        return _read_buf.eof();
    }, [this] {
        return with_gate(_pending_requests_gate, [this] {
            return process_request().then_wrapped([this] (auto f) {
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
            });
        });
    }).finally([this] {
        return _pending_requests_gate.close().then([this] {
            return _ready_to_respond.finally([this] {
                return _write_buf.close();
            });
        });
    });
}

future<> redis_server::connection::shutdown()
{
    try {
        _fd.shutdown_input();
        _fd.shutdown_output();
    } catch (...) {
    }
    return make_ready_future<>();
}

thread_local redis_server::connection::execution_stage_type redis_server::connection::_process_request_stage {"redis_transport", &connection::process_request_one};

future<redis_server::result> redis_server::connection::process_request_internal() {
    return _process_request_stage(this, std::move(_parser.get_request()), seastar::ref(_options), empty_service_permit());
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
                write_reply(std::move(result));
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

static inline bytes_view to_bytes_view(temporary_buffer<char>& b)
{
    using byte = bytes_view::value_type;
    return bytes_view(reinterpret_cast<const byte*>(b.get()), b.size());
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
