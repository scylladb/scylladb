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
#include <seastar/core/smp.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <utility>

namespace generic_server {

class counted_data_source_impl : public data_source_impl {
    data_source _ds;
    connection::cpu_concurrency_t& _cpu_concurrency;

    template <typename F>
    future<temporary_buffer<char>> invoke_with_counting(F&& fun) {
        if (_cpu_concurrency.stopped) {
            return fun();
        }
        return futurize_invoke([this] () {
            _cpu_concurrency.units.return_all();
        }).then([fun = std::move(fun)] () {
            return fun();
        }).finally([this] () {
            _cpu_concurrency.units.adopt(consume_units(_cpu_concurrency.semaphore, 1));
        });
    };
public:
    counted_data_source_impl(data_source ds, connection::cpu_concurrency_t& cpu_concurrency) : _ds(std::move(ds)), _cpu_concurrency(cpu_concurrency) {};
    virtual ~counted_data_source_impl() = default;
    virtual future<temporary_buffer<char>> get() override {
        return invoke_with_counting([this] {return _ds.get();});
    };
    virtual future<temporary_buffer<char>> skip(uint64_t n) override {
        return invoke_with_counting([this, n] {return _ds.skip(n);});
    };
    virtual future<> close() override {
        return _ds.close();
    };
};

class counted_data_sink_impl : public data_sink_impl {
    data_sink _ds;
    connection::cpu_concurrency_t& _cpu_concurrency;

    template <typename F>
    future<> invoke_with_counting(F&& fun) {
        if (_cpu_concurrency.stopped) {
            return fun();
        }
        return futurize_invoke([this] () {
            _cpu_concurrency.units.return_all();
        }).then([fun = std::move(fun)] () mutable {
            return fun();
        }).finally([this] () {
            _cpu_concurrency.units.adopt(consume_units(_cpu_concurrency.semaphore, 1));
        });
    };
public:
    counted_data_sink_impl(data_sink ds, connection::cpu_concurrency_t& cpu_concurrency) : _ds(std::move(ds)), _cpu_concurrency(cpu_concurrency) {};
    virtual ~counted_data_sink_impl() = default;
    virtual temporary_buffer<char> allocate_buffer(size_t size) override {
        return _ds.allocate_buffer(size);
    }
    virtual future<> put(net::packet data) override {
        return invoke_with_counting([this, data = std::move(data)] () mutable {
            return _ds.put(std::move(data));
        });
    }
    virtual future<> put(std::vector<temporary_buffer<char>> data)  override {
        return invoke_with_counting([this, data = std::move(data)] () mutable {
            return _ds.put(std::move(data));
        });
    }
    virtual future<> put(temporary_buffer<char> buf) override {
        return invoke_with_counting([this, buf = std::move(buf)] () mutable {
            return _ds.put(std::move(buf));
        });
    }
    virtual future<> flush() override {
        return invoke_with_counting([this] (void) mutable {
            return _ds.flush();
        });
    }
    virtual future<> close() override {
        return _ds.close();
    }
    virtual size_t buffer_size() const noexcept override {
        return _ds.buffer_size();
    }
    virtual bool can_batch_flushes() const noexcept override {
        return _ds.can_batch_flushes();
    }
    virtual void on_batch_flush_error() noexcept override {
        _ds.on_batch_flush_error();
    }
};

connection::connection(server& server, connected_socket&& fd, named_semaphore& sem, semaphore_units<named_semaphore_exception_factory> initial_sem_units)
    : _conns_cpu_concurrency{sem, std::move(initial_sem_units), false}
    , _server{server}
    , _connections_list_entry(_server._connections_list.emplace(*this))
    , _fd{std::move(fd)}
    , _read_buf(data_source(std::make_unique<counted_data_source_impl>(_fd.input().detach(), _conns_cpu_concurrency)))
    , _write_buf(output_stream<char>(data_sink(std::make_unique<counted_data_sink_impl>(_fd.output().detach(), _conns_cpu_concurrency)), 8192, output_stream_options{.batch_flushes = true}))
    , _pending_requests_gate("generic_server::connection")
    , _hold_server(_server._gate)
{
    ++_server._total_connections;
}

connection::~connection()
{
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
    return _connections_list.for_each_gently([fn = std::move(fn)](std::reference_wrapper<connection> c) {
        fn(c.get());
    });
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

void connection::on_connection_ready()
{
    _conns_cpu_concurrency.stopped = true;
    _conns_cpu_concurrency.units.return_all();
}

void connection::shutdown()
{
    shutdown_input();
    shutdown_output();
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

server::server(const sstring& server_name, logging::logger& logger, config cfg)
    : _server_name{server_name}
    , _logger{logger}
    , _gate("generic_server::server")
    , _conns_cpu_concurrency(cfg.uninitialized_connections_semaphore_cpu_concurrency)
    , _conns_cpu_concurrency_observer(_conns_cpu_concurrency.observe([this] (const uint32_t &concurrency) {
        if (concurrency == _prev_conns_cpu_concurrency) {
            return;
        }
        _logger.info("Updating uninitialized_connections_semaphore_cpu_concurrency from {} to {} due to config update", _prev_conns_cpu_concurrency, concurrency);

        if (concurrency > _prev_conns_cpu_concurrency) {
            _conns_cpu_concurrency_semaphore.signal(concurrency - _prev_conns_cpu_concurrency);
        } else {
            _conns_cpu_concurrency_semaphore.consume(_prev_conns_cpu_concurrency - concurrency);
        }
        _prev_conns_cpu_concurrency = concurrency;
    }))
    , _prev_conns_cpu_concurrency(_conns_cpu_concurrency)
    , _conns_cpu_concurrency_semaphore(_conns_cpu_concurrency, named_semaphore_exception_factory{"connections cpu concurrency semaphore"})
    , _shutdown_timeout(std::chrono::seconds{cfg.shutdown_timeout_in_seconds})
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
    co_await for_each_gently([this, &nr_conn, nr_conn_total] (std::reference_wrapper<connection> c) {
        c.get().shutdown();
        _logger.debug("shutdown connection {} out of {} done", ++nr_conn, nr_conn_total);
    });
    _abort_source.request_abort();
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
    bool is_tls = false;
    try {
        ss = builder
            ? is_tls = true, seastar::tls::listen(_credentials, addr, lo)
            : seastar::listen(addr, lo);
    } catch (...) {
        throw std::runtime_error(format("{} error while listening on {} -> {}", _server_name, addr, std::current_exception()));
    }
    _listeners.emplace_back(std::move(ss));
    _listeners_stopped = when_all(std::move(_listeners_stopped), do_accepts(_listeners.size() - 1, keepalive, addr, is_tls)).discard_result();
}

future<> server::do_accepts(int which, bool keepalive, socket_address server_addr, bool is_tls) {
    while (!_gate.is_closed()) {
        seastar::gate::holder holder(_gate);
        bool shed = false;
        try {
            semaphore_units<named_semaphore_exception_factory> units(_conns_cpu_concurrency_semaphore, 0);
            if (_conns_cpu_concurrency != std::numeric_limits<uint32_t>::max()) {
                auto u = try_get_units(_conns_cpu_concurrency_semaphore, 1);
                if (u) {
                    units = std::move(*u);
                } else {
                    _blocked_connections++;
                    try {
                        units = co_await get_units(_conns_cpu_concurrency_semaphore, 1, std::chrono::minutes(1));
                    } catch (const semaphore_timed_out&) {
                        shed = true;
                    }
                }
            }
            accept_result cs_sa = co_await _listeners[which].accept();
            if (_gate.is_closed()) {
                break;
            }
            auto fd = std::move(cs_sa.connection);
            auto addr = std::move(cs_sa.remote_address);
            fd.set_nodelay(true);
            fd.set_keepalive(keepalive);
            auto conn = make_connection(server_addr, std::move(fd), std::move(addr),
                    _conns_cpu_concurrency_semaphore, std::move(units));
            if (shed) {
                // We establish a connection even during shedding to notify the client;
                // otherwise, they might hang waiting for a response.
                _shed_connections++;
                static thread_local logger::rate_limit rate_limit{std::chrono::seconds(10)};
                _logger.log(log_level::warn, rate_limit,
                        "too many in-flight connection attempts: {}, connection dropped",
                        _conns_cpu_concurrency_semaphore.waiters());
                conn->shutdown();
                continue;
            }
            // Move the processing into the background.
            (void)futurize_invoke([this, conn, is_tls] {
                // Build a future<> that completes after TLS info is populated (or reset on failure).
                future<> tls_init = make_ready_future<>();

                if (is_tls) {
                    conn->_ssl_enabled = true;

                    tls_init = tls::get_protocol_version(conn->_fd).then([conn](const sstring& protocol) {
                        conn->_ssl_protocol = protocol;
                        return tls::get_cipher_suite(conn->_fd);
                    }).then([conn](const sstring& cipher_suite) {
                        conn->_ssl_cipher_suite = cipher_suite;
                        return make_ready_future<>();
                    }).handle_exception([conn](std::exception_ptr) {
                        conn->_ssl_enabled = true;
                        conn->_ssl_protocol.reset();
                        conn->_ssl_cipher_suite.reset();
                    });
                } else {
                    conn->_ssl_enabled = false;
                    conn->_ssl_protocol.reset();
                    conn->_ssl_cipher_suite.reset();
                }

                return tls_init.then([conn] {
                    // Block while monitoring for lifetime/errors.
                    return conn->process();
                }).then_wrapped([this, conn](auto f) {
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
                });
            });
        } catch (...) {
            _logger.debug("accept failed: {}", std::current_exception());
        }
    }
}

}
