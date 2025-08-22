/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "generic_server.hh"
#include <exception>
#include <fmt/ranges.h>
#include <seastar/core/when_all.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/shared_future.hh>

namespace generic_server {

connection::connection(server& server, connected_socket&& fd)
    : _server{server}
    , _fd{std::move(fd)}
    , _read_buf(_fd.input())
    , _write_buf(_fd.output())
    , _hold_server(_server._gate)
{
    ++_server._total_connections;
    _server._connections_list.push_back(*this);
}

connection::~connection()
{
    server::connections_list_t::iterator iter = _server._connections_list.iterator_to(*this);
    for (auto&& gi : _server._gentle_iterators) {
        if (gi.iter == iter) {
            gi.iter++;
        }
    }
    _server._connections_list.erase(iter);
}

connection::execute_under_tenant_type
connection::no_tenant() {
    // return a function that runs the process loop with no scheduling group games
    return [] (connection_process_loop loop) {
        return loop();
    };
}

void connection::switch_tenant(execute_under_tenant_type exec) {
    _execute_under_current_tenant = std::move(exec);
    _tenant_switch = true;
}

future<> server::for_each_gently(noncopyable_function<void(connection&)> fn) {
    _gentle_iterators.emplace_front(*this);
    std::list<gentle_iterator>::iterator gi = _gentle_iterators.begin();
    return seastar::do_until([ gi ] { return gi->iter == gi->end; },
        [ gi, fn = std::move(fn) ] {
            fn(*(gi->iter++));
            return make_ready_future<>();
        }
    ).finally([ this, gi ] { _gentle_iterators.erase(gi); });
}

static bool is_broken_pipe_or_connection_reset(std::exception_ptr ep) {
    try {
        std::rethrow_exception(ep);
    } catch (const std::system_error& e) {
        auto& code = e.code();
        if (code.category() == std::system_category() && (code.value() == EPIPE || code.value() == ECONNRESET)) {
            return true;
        }
        if (code.category() == tls::error_category()) {
            // Typically ECONNRESET
            if (code.value() == tls::ERROR_PREMATURE_TERMINATION) {
                return true;
            }
            // If we got an actual EPIPE in push/pull of gnutls, it is _not_ translated
            // to anything more useful than generic push/pull error. Need to look at
            // nested exception.
            if (code.value() == tls::ERROR_PULL || code.value() == tls::ERROR_PUSH) {
                if (auto p = dynamic_cast<const std::nested_exception*>(std::addressof(e))) {
                    return is_broken_pipe_or_connection_reset(p->nested_ptr());
                }
            }
        }
        return false;
    } catch (...) {}
    return false;
}

future<> connection::process_until_tenant_switch() {
    _tenant_switch = false;
    {
        return do_until([this] {
            return _read_buf.eof() || _tenant_switch;
        }, [this] {
            return process_request();
        });
    }
}

future<> connection::process()
{
    return with_gate(_pending_requests_gate, [this] {
        return do_until([this] {
            return _read_buf.eof();
        }, [this] {
            return _execute_under_current_tenant([this] {
                return process_until_tenant_switch();
            });
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
    shutdown_input();
    shutdown_output();
    return make_ready_future<>();
}

bool connection::shutdown_input() {
    try {
        _fd.shutdown_input();
    } catch (...) {
        _server._logger.warn("Error shutting down input side of connection {}->{}, exception: {}", _fd.remote_address(), _fd.local_address(), std::current_exception());
        return false;
    }
    return true;
}

bool connection::shutdown_output() {
    try {
        _fd.shutdown_output();
    } catch (...) {
        _server._logger.warn("Error shutting down output side of connection {}->{}, exception: {}", _fd.remote_address(), _fd.local_address(), std::current_exception());
        return false;
    }
    return true;
}

server::server(const sstring& server_name, logging::logger& logger,  uint32_t shutdown_request_timeout)
    : _server_name{server_name}
    , _logger{logger}
    , _shutdown_timeout(std::chrono::seconds{shutdown_request_timeout})
{
}

future<> server::stop() {
    co_await shutdown();
    co_await std::exchange(_all_connections_stopped, make_ready_future<>());
}

future<> server::shutdown() {
    if (_gate.is_closed()) {
        co_return;
    }

    shared_future<> connections_stopped{_gate.close()};
    _all_connections_stopped = connections_stopped.get_future();

     // Stop all listeners.
    size_t nr = 0;
    size_t nr_total = _listeners.size();
    _logger.debug("abort accept nr_total={}", nr_total);
    for (auto&& l : _listeners) {
        l.abort_accept();
        _logger.debug("abort accept {} out of {} done", ++nr, nr_total);
    }
     co_await std::move(_listeners_stopped);

    // Shutdown RX side of the connections, so no new requests could be received.
    // Leave the TX side so the responses to ongoing requests could be sent.
    _logger.debug("Shutting down RX side of {} connections", _connections_list.size());
    co_await for_each_gently([](auto& connection) {
        if (!connection.shutdown_input()) {
            // If failed to shutdown the input side, then attempt to shutdown the output side which should do a complete shutdown of the connection.
            connection.shutdown_output();
        }
    });

    // Wait for the remaining requests to finish.
    _logger.debug("Waiting for connections to stop");
    try {
        co_await connections_stopped.get_future(seastar::lowres_clock::now() + _shutdown_timeout);
    } catch (const timed_out_error& _) {
        _logger.info("Timed out waiting for connections shutdown.");
    }

    // Either all requests stopped or a timeout occurred, do the full shutdown of the connections.
    size_t nr_conn = 0;
    auto nr_conn_total = _connections_list.size();
    _logger.debug("shutdown connection nr_total={}", nr_conn_total);
    co_await for_each_gently([&nr_conn, nr_conn_total, this](connection& c) {
        c.shutdown().get();
        _logger.debug("shutdown connection {} out of {} done", ++nr_conn, nr_conn_total);
    });
}

future<>
server::listen(socket_address addr, std::shared_ptr<seastar::tls::credentials_builder> builder, bool is_shard_aware, bool keepalive, std::optional<file_permissions> unix_domain_socket_permissions) {
    shared_ptr<seastar::tls::server_credentials> creds = nullptr;
    if (builder) {
        creds = co_await builder->build_reloadable_server_credentials([this](const std::unordered_set<sstring>& files, std::exception_ptr ep) {
            if (ep) {
                _logger.warn("Exception loading {}: {}", files, ep);
            } else {
                _logger.info("Reloaded {}", files);
            }
        });
    }
    listen_options lo;
    lo.reuse_address = true;
    lo.unix_domain_socket_permissions = unix_domain_socket_permissions;
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
    _listeners_stopped = when_all(std::move(_listeners_stopped), do_accepts(_listeners.size() - 1, keepalive, addr)).discard_result();
}

future<> server::do_accepts(int which, bool keepalive, socket_address server_addr) {
    return repeat([this, which, keepalive, server_addr] {
        seastar::gate::holder holder(_gate);
        return _listeners[which].accept().then_wrapped([this, keepalive, server_addr, holder = std::move(holder)] (future<accept_result> f_cs_sa) mutable {
            if (_gate.is_closed()) {
                f_cs_sa.ignore_ready_future();
                return stop_iteration::yes;
            }
            auto cs_sa = f_cs_sa.get();
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
                return conn->process().then_wrapped([this, conn] (auto f) {
                    try {
                        f.get();
                    } catch (...) {
                        auto ep = std::current_exception();
                        if (!is_broken_pipe_or_connection_reset(ep)) {
                            // some exceptions are expected if another side closes a connection
                            // or we're shutting down
                            _logger.info("exception while processing connection: {}", ep);
                        }
                    }
                    return unadvertise_connection(conn);
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
server::advertise_new_connection(shared_ptr<generic_server::connection> raw_conn) {
    return make_ready_future<>();
}

future<>
server::unadvertise_connection(shared_ptr<generic_server::connection> raw_conn) {
    return make_ready_future<>();
}

}
