/*
 * Copyright (C) 2014-present ScyllaDB
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

#include "server.hh"
#include "handler.hh"
#include "db/config.hh"
#include "database.hh"
#include <seastar/core/future-util.hh>
#include <seastar/core/circular_buffer.hh>
#include <seastar/core/metrics.hh>
#include <seastar/net/byteorder.hh>
#include <seastar/core/scattered_message.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/semaphore.hh>
#include "log.hh"
#include <thrift/server/TServer.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/TProcessor.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/async/TAsyncProcessor.h>
#include <iostream>
#include <algorithm>
#include <unordered_map>
#include <queue>
#include <bitset>
#include <limits>
#include <cctype>
#include <vector>

#ifdef THRIFT_USES_BOOST
#include <boost/make_shared.hpp>
#endif

static logging::logger tlogger("thrift");

using namespace apache::thrift;
using namespace apache::thrift::transport;
using namespace apache::thrift::protocol;
using namespace apache::thrift::async;
using namespace ::cassandra;

using namespace std::chrono_literals;

class thrift_stats {
    seastar::metrics::metric_groups _metrics;
public:
    thrift_stats(thrift_server& server);
};

thrift_server::thrift_server(distributed<database>& db,
                             distributed<cql3::query_processor>& qp,
                             auth::service& auth_service,
                             service::memory_limiter& ml,
                             thrift_server_config config)
        : _stats(new thrift_stats(*this))
        , _handler_factory(create_handler_factory(db, qp, auth_service, config.timeout_config, _current_permit).release())
        , _protocol_factory(new TBinaryProtocolFactoryT<TMemoryBuffer>())
        , _processor_factory(new CassandraAsyncProcessorFactory(_handler_factory))
        , _memory_available(ml.get_semaphore())
        , _max_concurrent_requests(db.local().get_config().max_concurrent_requests_per_shard)
        , _config(config) {
}

thrift_server::~thrift_server() {
}

future<> thrift_server::stop() {
    auto f = _stop_gate.close();
    std::for_each(_listeners.begin(), _listeners.end(), std::mem_fn(&server_socket::abort_accept));
    std::for_each(_connections_list.begin(), _connections_list.end(), std::mem_fn(&connection::shutdown));
    return f;
}

struct handler_deleter {
    CassandraCobSvIfFactory* hf;
    void operator()(CassandraCobSvIf* h) const {
        hf->releaseHandler(h);
    }
};

// thrift uses a shared_ptr to refer to the transport (= connection),
// while we do not, so we can't have connection inherit from TTransport.
struct thrift_server::connection::fake_transport : TTransport {
    fake_transport(thrift_server::connection* c) : conn(c) {}
    thrift_server::connection* conn;
};

thrift_server::connection::connection(thrift_server& server, connected_socket&& fd, socket_address addr)
        : _server(server), _fd(std::move(fd)), _read_buf(_fd.input())
        , _write_buf(_fd.output())
        , _transport(thrift_std::make_shared<thrift_server::connection::fake_transport>(this))
        , _input(thrift_std::make_shared<TMemoryBuffer>())
        , _output(thrift_std::make_shared<TMemoryBuffer>())
        , _in_proto(_server._protocol_factory->getProtocol(_input))
        , _out_proto(_server._protocol_factory->getProtocol(_output))
        , _processor(_server._processor_factory->getProcessor({ _in_proto, _out_proto, _transport })) {
    ++_server._total_connections;
    ++_server._current_connections;
    _server._connections_list.push_back(*this);
}

thrift_server::connection::~connection() {
    if (is_linked()) {
        --_server._current_connections;
        _server._connections_list.erase(_server._connections_list.iterator_to(*this));
    }
}

thrift_server::connection::connection(connection&& other)
        : _server(other._server)
        , _fd(std::move(other._fd))
        , _read_buf(std::move(other._read_buf))
        , _write_buf(std::move(other._write_buf))
        , _transport(std::move(other._transport))
        , _input(std::move(other._input))
        , _output(std::move(other._output))
        , _in_proto(std::move(other._in_proto))
        , _out_proto(std::move(other._out_proto))
        , _processor(std::move(other._processor)) {
    if (other.is_linked()) {
        boost::intrusive::list<connection>::node_algorithms::init(this_ptr());
        boost::intrusive::list<connection>::node_algorithms::swap_nodes(other.this_ptr(), this_ptr());
    }
}

future<>
thrift_server::connection::process() {
    return do_until([this] { return _read_buf.eof(); },
            [this] { return process_one_request(); })
        .finally([this] {
            return _write_buf.close();
        });
}

future<>
thrift_server::connection::process_one_request() {
    _input->resetBuffer();
    _output->resetBuffer();
    co_await read();
    if (_server._requests_serving >= _server._max_concurrent_requests) {
        _server._requests_shed++;
        tlogger.debug("message dropped due to overload");
        co_return;
    }
    ++_server._requests_serving;
    ++_server._requests_served;
    auto ret = _processor_promise.get_future().handle_exception([&server = _server] (const std::exception_ptr&) {
        server._requests_serving--;
    });
    // adapt from "continuation object style" to future/promise
    auto complete = [this] (bool success) mutable {
        // FIXME: look at success?
        _server._requests_serving--;
        write().forward_to(std::move(_processor_promise));
        _processor_promise = promise<>();
    };
    // Heuristics copied from transport/server.cc
    size_t mem_estimate = 8000 + 2 * _input->available_read();
    auto fut = get_units(_server._memory_available, mem_estimate);
    if (_server._memory_available.waiters()) {
        ++_server._requests_blocked_memory;
    }
    auto units = co_await std::move(fut);
    // NOTICE: this permit is put in the server under the assumption that no other
    // connection will overwrite this permit *until* it's extracted by the code
    // which handles the Thrift request (via calling obtain_permit()).
    // This assumption is true because there are no preemption points between this
    // insertion and the call to obtain_permit(), which was verified both by
    // code inspection and confirmed empirically by running manual tests.
    if (_server._current_permit.count() > 0) {
        tlogger.debug("Current service permit is overwritten while its units are still held ({}). "
                "This situation likely means that there's a bug in passing service permits to message handlers.",
                _server._current_permit.count());
    }
    _server._current_permit = make_service_permit(std::move(units));
    _processor->process(complete, _in_proto, _out_proto);
    co_return co_await std::move(ret);
}

future<>
thrift_server::connection::read() {
    return _read_buf.read_exactly(4).then([this] (temporary_buffer<char> size_buf) {
        if (size_buf.size() != 4) {
            return make_ready_future<>();
        }
        union {
            uint32_t n;
            char b[4];
        } data;
        std::copy_n(size_buf.get(), 4, data.b);
        auto n = ntohl(data.n);
        if (n > _server._config.max_request_size) {
            // Close connection silently, we can't return a response because we did not
            // read a complete frame.
            tlogger.info("message size {} exceeds configured maximum {}, closing connection", n, _server._config.max_request_size);
            return make_ready_future<>();
        }
        return _read_buf.read_exactly(n).then([this, n] (temporary_buffer<char> buf) {
            if (buf.size() != n) {
                // FIXME: exception perhaps?
                return;
            }
            _in_tmp = std::move(buf); // keep ownership of the data
            auto b = reinterpret_cast<uint8_t*>(_in_tmp.get_write());
            _input->resetBuffer(b, _in_tmp.size());
        });
    });
}

future<>
thrift_server::connection::write() {
    uint8_t* data;
    uint32_t len;
    _output->getBuffer(&data, &len);
    net::packed<uint32_t> plen = { net::hton(len) };
    return _write_buf.write(reinterpret_cast<char*>(&plen), 4).then([this, data, len] {
        // FIXME: zero-copy
        return _write_buf.write(reinterpret_cast<char*>(data), len);
    }).then([this] {
        return _write_buf.flush();
    });
}

void
thrift_server::connection::shutdown() {
    try {
        _fd.shutdown_input();
        _fd.shutdown_output();
    } catch (...) {
    }
}

future<>
thrift_server::listen(socket_address addr, bool keepalive) {
    listen_options lo;
    lo.reuse_address = true;
    _listeners.push_back(seastar::listen(addr, lo));
    do_accepts(_listeners.size() - 1, keepalive, 0);
    return make_ready_future<>();
}

void
thrift_server::do_accepts(int which, bool keepalive, int num_attempts) {
    if (_stop_gate.is_closed()) {
        return;
    }
    // Future is waited on indirectly in `stop()` (via `_stop_gate`).
    (void)with_gate(_stop_gate, [&, this] {
        return _listeners[which].accept().then([this, which, keepalive] (accept_result ar) {
            auto&& fd = ar.connection;
            auto&& addr = ar.remote_address;
            fd.set_nodelay(true);
            fd.set_keepalive(keepalive);
            // Future is waited on indirectly in `stop()` (via `_stop_gate`).
            (void)with_gate(_stop_gate, [&, this] {
                return do_with(connection(*this, std::move(fd), addr), [this] (auto& conn) {
                    return conn.process().then_wrapped([this, &conn] (future<> f) {
                        conn.shutdown();
                        try {
                            f.get();
                        } catch (std::exception& ex) {
                            tlogger.debug("request error {}", ex.what());
                        }
                    });
                });
            });
            do_accepts(which, keepalive, 0);
        }).handle_exception([this, which, keepalive, num_attempts] (auto ex) {
            tlogger.debug("accept failed {}", ex);
            try {
                std::rethrow_exception(std::move(ex));
            } catch (const seastar::gate_closed_exception&) {
                return;
            } catch (...) {
                if (_stop_gate.is_closed()) {
                    return;
                }
                // Done in the background.
                (void)with_gate(_stop_gate, [this, which, keepalive, num_attempts] {
                    int backoff = 2 << std::max(num_attempts, 10);
                    tlogger.debug("sleeping for {}ms", backoff);
                    return sleep(std::chrono::milliseconds(backoff)).then([this, which, keepalive, num_attempts] {
                        tlogger.debug("retrying accept after failure");
                        do_accepts(which, keepalive, num_attempts + 1);
                    });
                });
            }
        });
    });
}

uint64_t
thrift_server::total_connections() const {
    return _total_connections;
}

uint64_t
thrift_server::current_connections() const {
    return _current_connections;
}

uint64_t
thrift_server::requests_served() const {
    return _requests_served;
}

uint64_t
thrift_server::requests_serving() const {
    return _requests_serving;
}

size_t
thrift_server::max_request_size() const {
    return _config.max_request_size;
}

const semaphore&
thrift_server::memory_available() const {
    return _memory_available;
}

uint64_t
thrift_server::requests_blocked_memory() const {
    return _requests_blocked_memory;
}

uint64_t
thrift_server::requests_shed() const {
    return _requests_shed;
}

thrift_stats::thrift_stats(thrift_server& server) {
    namespace sm = seastar::metrics;

    _metrics.add_group("thrift", {
        sm::make_derive("thrift-connections", [&server] { return server.total_connections(); },
                        sm::description("Rate of creation of new Thrift connections.")),

        sm::make_gauge("current_connections", [&server] { return server.current_connections(); },
                        sm::description("Holds a current number of opened Thrift connections.")),

        sm::make_derive("served", [&server] { return server.requests_served(); },
                        sm::description("Rate of serving Thrift requests.")),
        sm::make_gauge("serving", [&server] { return server.requests_serving(); },
                        sm::description("Number of Thrift requests being currently served.")),
        sm::make_gauge("requests_blocked_memory_current", [&server] { return server.memory_available().waiters(); },
                        sm::description(
                            seastar::format("Holds the number of Thrift requests that are currently blocked due to reaching the memory quota limit ({}B). "
                                            "Non-zero value indicates that our bottleneck is memory and more specifically - the memory quota allocated for the \"Thrift transport\" component.", server.max_request_size()))),
        sm::make_derive("requests_blocked_memory", [&server] { return server.requests_blocked_memory(); },
                        sm::description(
                            seastar::format("Holds an incrementing counter with the Thrift requests that ever blocked due to reaching the memory quota limit ({}B). "
                                            "The first derivative of this value shows how often we block due to memory exhaustion in the \"Thrift transport\" component.", server.max_request_size()))),
        sm::make_derive("requests_shed", [&server] { return server.requests_shed(); },
                        sm::description("Holds an incrementing counter with the requests that were shed due to exceeding the threshold configured via max_concurrent_requests_per_shard. "
                                            "The first derivative of this value shows how often we shed requests due to exceeding the limit in the \"Thrift transport\" component.")),
        sm::make_gauge("requests_memory_available", [&server] { return server.memory_available().current(); },
                        sm::description(
                            seastar::format("Holds the amount of available memory for admitting new Thrift requests (max is {}B)."
                                            "Zero value indicates that our bottleneck is memory and more specifically - the memory quota allocated for the \"Thrift transport\" component.", server.max_request_size())))
    });
}

