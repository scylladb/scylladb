/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "generic_server.hh"


#include <fmt/ranges.h>
#include <seastar/core/when_all.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/smp.hh>

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
    co_await shutdown();
    co_await std::exchange(_all_connections_stopped, make_ready_future<>());
}

future<> server::shutdown() {
    if (_gate.is_closed()) {
        co_return;
    }
    _all_connections_stopped = _gate.close();
    size_t nr = 0;
    size_t nr_total = _listeners.size();
    _logger.debug("abort accept nr_total={}", nr_total);
    for (auto&& l : _listeners) {
        l.abort_accept();
        _logger.debug("abort accept {} out of {} done", ++nr, nr_total);
    }
    size_t nr_conn = 0;
    auto nr_conn_total = _connections_list.size();
    _logger.debug("shutdown connection nr_total={}", nr_conn_total);
    co_await coroutine::parallel_for_each(_connections_list, [&] (auto&& c) -> future<> {
        co_await c.shutdown();
        _logger.debug("shutdown connection {} out of {} done", ++nr_conn, nr_conn_total);
    });
    co_await std::move(_listeners_stopped);
}

future<>
server::listen(socket_address addr, std::shared_ptr<seastar::tls::credentials_builder> builder, bool is_shard_aware, bool keepalive, std::optional<file_permissions> unix_domain_socket_permissions, std::function<server&()> get_shard_instance) {
    // Note: We are making the assumption that if builder is provided it will be the same for each
    // invocation, regardless of address etc. In general, only CQL server will call this multiple times,
    // and if TLS, it will use the same cert set.
    // Could hold certs in a map<addr, certs> and ensure separation, but then we will for all
    // current uses of this class create duplicate reloadable certs for shard 0, which is
    // kind of what we wanted to avoid in the first place...
    if (builder && !_credentials) {
        if (!get_shard_instance || this_shard_id() == 0) {
            _credentials = co_await builder->build_reloadable_server_credentials([this, get_shard_instance = std::move(get_shard_instance)](const tls::credentials_builder& b, const std::unordered_set<sstring>& files, std::exception_ptr ep) -> future<> {
                if (ep) {
                    _logger.warn("Exception loading {}: {}", files, ep);
                } else {
                    if (get_shard_instance) {
                        co_await smp::invoke_on_others([&]() {
                            auto& s = get_shard_instance();
                            if (s._credentials) {
                                b.rebuild(*s._credentials);
                            }
                        });

                    }
                    _logger.info("Reloaded {}", files);
                }
            });
        } else {
            _credentials = builder->build_server_credentials();
        }
    }
    listen_options lo;
    lo.reuse_address = true;
    lo.unix_domain_socket_permissions = unix_domain_socket_permissions;
    if (is_shard_aware) {
        lo.lba = server_socket::load_balancing_algorithm::port;
    }
    server_socket ss;
    try {
        ss = builder
            ? seastar::tls::listen(_credentials, addr, lo)
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
