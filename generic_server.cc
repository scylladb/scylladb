/*
 * Copyright (C) 2021-present ScyllaDB
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

#include "generic_server.hh"

#include "to_string.hh"

#include <seastar/core/when_all.hh>
#include <seastar/core/reactor.hh>

namespace generic_server {

connection::connection(server& server, connected_socket&& fd)
    : _server{server}
    , _fd{std::move(fd)}
    , _read_buf(_fd.input())
    , _write_buf(_fd.output())
{
    ++_server._total_connections;
    ++_server._current_connections;
    _server._connections_list.push_back(*this);
}

connection::~connection()
{
    --_server._current_connections;
    _server._connections_list.erase(_server._connections_list.iterator_to(*this));
    _server.maybe_stop();
}

static bool is_broken_pipe_or_connection_reset(std::exception_ptr ep) {
    try {
        std::rethrow_exception(ep);
    } catch (const std::system_error& e) {
        return e.code().category() == std::system_category()
            && (e.code().value() == EPIPE || e.code().value() == ECONNRESET);
    } catch (...) {}
    return false;
}

future<> connection::process()
{
    return with_gate(_pending_requests_gate, [this] {
        return do_until([this] {
            return _read_buf.eof();
        }, [this] {
            return process_request();
        }).then_wrapped([this] (future<> f) {
            handle_error(std::move(f));
        });
    }).finally([this] {
        return _pending_requests_gate.close().then([this] {
            on_connection_close();
            return _ready_to_respond.handle_exception([] (std::exception_ptr ep) {
                if (is_broken_pipe_or_connection_reset(ep)) {
                    // expected if another side closes a connection or we're shutting down
                    return;
                }
                std::rethrow_exception(ep);
            }).finally([this] {
                 return _write_buf.close();
            });
        });
    });
}

void connection::on_connection_close()
{
}

future<> connection::shutdown()
{
    try {
        _fd.shutdown_input();
        _fd.shutdown_output();
    } catch (...) {
    }
    return make_ready_future<>();
}

server::server(const sstring& server_name, logging::logger& logger)
    : _server_name{server_name}
    , _logger{logger}
{
}

server::~server()
{
}

future<> server::stop() {
    _stopping = true;
    size_t nr = 0;
    size_t nr_total = _listeners.size();
    _logger.debug("abort accept nr_total={}", nr_total);
    for (auto&& l : _listeners) {
        l.abort_accept();
        _logger.debug("abort accept {} out of {} done", ++nr, nr_total);
    }
    auto nr_conn = make_lw_shared<size_t>(0);
    auto nr_conn_total = _connections_list.size();
    _logger.debug("shutdown connection nr_total={}", nr_conn_total);
    return parallel_for_each(_connections_list.begin(), _connections_list.end(), [this, nr_conn, nr_conn_total] (auto&& c) {
        return c.shutdown().then([this, nr_conn, nr_conn_total] {
            _logger.debug("shutdown connection {} out of {} done", ++(*nr_conn), nr_conn_total);
        });
    }).then([this] {
        return on_stop();
    }).then([this] {
        return std::move(_stopped);
    });
}

future<>
server::listen(socket_address addr, std::shared_ptr<seastar::tls::credentials_builder> creds, bool is_shard_aware, bool keepalive) {
    auto f = make_ready_future<shared_ptr<seastar::tls::server_credentials>>(nullptr);
    if (creds) {
        f = creds->build_reloadable_server_credentials([this](const std::unordered_set<sstring>& files, std::exception_ptr ep) {
            if (ep) {
                _logger.warn("Exception loading {}: {}", files, ep);
            } else {
                _logger.info("Reloaded {}", files);
            }
        });
    }
    return f.then([this, addr, is_shard_aware, keepalive](shared_ptr<seastar::tls::server_credentials> creds) {
        listen_options lo;
        lo.reuse_address = true;
        if (is_shard_aware) {
            lo.lba = server_socket::load_balancing_algorithm::port;
        }
        server_socket ss;
        try {
            ss = creds
                ? seastar::tls::listen(std::move(creds), addr, lo)
                : seastar::listen(addr, lo);
        } catch (...) {
            throw std::runtime_error(format("{} error while listening on {} -> {}", _server_name, addr, std::current_exception()));
        }
        _listeners.emplace_back(std::move(ss));
        _stopped = when_all(std::move(_stopped), do_accepts(_listeners.size() - 1, keepalive, addr)).discard_result();
    });
}

future<> server::do_accepts(int which, bool keepalive, socket_address server_addr) {
    return repeat([this, which, keepalive, server_addr] {
        ++_connections_being_accepted;
        return _listeners[which].accept().then_wrapped([this, which, keepalive, server_addr] (future<accept_result> f_cs_sa) mutable {
            --_connections_being_accepted;
            if (_stopping) {
                f_cs_sa.ignore_ready_future();
                maybe_stop();
                return stop_iteration::yes;
            }
            auto cs_sa = f_cs_sa.get0();
            auto fd = std::move(cs_sa.connection);
            auto addr = std::move(cs_sa.remote_address);
            fd.set_nodelay(true);
            fd.set_keepalive(keepalive);
            auto conn = make_connection(server_addr, std::move(fd), std::move(addr));
            // Move the processing into the background.
            (void)futurize_invoke([this, conn] {
                return advertise_new_connection(conn); // Notify any listeners about new connection.
            }).then_wrapped([this, conn] (future<> f) {
                try {
                    f.get();
                } catch (...) {
                    _logger.info("exception while advertising new connection: {}", std::current_exception());
                }
                // Block while monitoring for lifetime/errors.
                return conn->process().finally([this, conn] {
                    return unadvertise_connection(conn);
                }).handle_exception([this] (std::exception_ptr ep) {
                    if (is_broken_pipe_or_connection_reset(ep)) {
                        // expected if another side closes a connection or we're shutting down
                        return;
                    }
                    _logger.info("exception while processing connection: {}", ep);
                });
            });
            return stop_iteration::no;
        }).handle_exception([this] (auto ep) {
            _logger.debug("accept failed: {}", ep);
            return stop_iteration::no;
        });
    });
}

future<>
server::on_stop() {
    return make_ready_future<>();
}

future<>
server::advertise_new_connection(shared_ptr<generic_server::connection> raw_conn) {
    return make_ready_future<>();
}

future<>
server::unadvertise_connection(shared_ptr<generic_server::connection> raw_conn) {
    return make_ready_future<>();
}

// Signal that all connections are stopped if the server is stopping and can be stopped.
void server::maybe_stop() {
    if (_stopping && !_connections_being_accepted && !_current_connections) {
        _all_connections_stopped.set_value();
    }
}

}
