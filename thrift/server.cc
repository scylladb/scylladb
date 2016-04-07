/*
 * Copyright (C) 2014 ScyllaDB
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
#include "core/future-util.hh"
#include "core/circular_buffer.hh"
#include "core/scollectd.hh"
#include "net/byteorder.hh"
#include "core/scattered_message.hh"
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
#include <boost/make_shared.hpp>

static logging::logger logger("thrift");

using namespace apache::thrift;
using namespace apache::thrift::transport;
using namespace apache::thrift::protocol;
using namespace apache::thrift::async;
using namespace org::apache::cassandra;

class thrift_stats {
    scollectd::registrations _regs;
public:
    thrift_stats(thrift_server& server);
};

thrift_server::thrift_server(distributed<database>& db)
        : _stats(new thrift_stats(*this))
        , _handler_factory(create_handler_factory(db).release())
        , _protocol_factory(new TBinaryProtocolFactoryT<TMemoryBuffer>())
        , _processor_factory(new CassandraAsyncProcessorFactory(_handler_factory)) {
}

thrift_server::~thrift_server() {
}

// FIXME: this is here because we must have a stop function. But we should actually
// do something useful - or be sure it is not needed
future<> thrift_server::stop() {
    return make_ready_future<>();
}

struct handler_deleter {
    CassandraCobSvIfFactory* hf;
    void operator()(CassandraCobSvIf* h) const {
        hf->releaseHandler(h);
    }
};

class thrift_server::connection {
    // thrift uses a shared_ptr to refer to the transport (= connection),
    // while we do not, so we can't have connection inherit from TTransport.
    struct fake_transport : TTransport {
        fake_transport(connection* c) : conn(c) {}
        connection* conn;
    };
    thrift_server& _server;
    connected_socket _fd;
    input_stream<char> _read_buf;
    output_stream<char> _write_buf;
    temporary_buffer<char> _in_tmp;
    boost::shared_ptr<fake_transport> _transport = boost::make_shared<fake_transport>(this);
    boost::shared_ptr<TMemoryBuffer> _input = boost::make_shared<TMemoryBuffer>();
    boost::shared_ptr<TMemoryBuffer> _output = boost::make_shared<TMemoryBuffer>();
    boost::shared_ptr<TProtocol> _in_proto = _server._protocol_factory->getProtocol(_input);
    boost::shared_ptr<TProtocol> _out_proto = _server._protocol_factory->getProtocol(_output);
    boost::shared_ptr<TAsyncProcessor> _processor;
    promise<> _processor_promise;
public:
    connection(thrift_server& server, connected_socket&& fd, socket_address addr)
        : _server(server), _fd(std::move(fd)), _read_buf(_fd.input())
        , _write_buf(_fd.output())
        , _processor(_server._processor_factory->getProcessor(get_conn_info())) {
        ++_server._total_connections;
        ++_server._current_connections;
    }
    ~connection() {
        --_server._current_connections;
    }
    TConnectionInfo get_conn_info() {
        return { _in_proto, _out_proto, _transport };
    }
    future<> process() {
        return do_until([this] { return _read_buf.eof(); },
                [this] { return process_one_request(); });
    }
    future<> process_one_request() {
        _input->resetBuffer();
        _output->resetBuffer();
        return read().then([this] {
            ++_server._requests_served;
            auto ret = _processor_promise.get_future();
            // adapt from "continuation object style" to future/promise
            auto complete = [this] (bool success) mutable {
                // FIXME: look at success?
                write().forward_to(std::move(_processor_promise));
                _processor_promise = promise<>();
            };
            _processor->process(complete, _in_proto, _out_proto);
            return ret;
        });
    }
    future<> read() {
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
    future<> write() {
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
};

future<>
thrift_server::listen(ipv4_addr addr, bool keepalive) {
    listen_options lo;
    lo.reuse_address = true;
    _listeners.push_back(engine().listen(make_ipv4_address(addr), lo));
    do_accepts(_listeners.size() - 1, keepalive);
    return make_ready_future<>();
}

void
thrift_server::do_accepts(int which, bool keepalive) {
    _listeners[which].accept().then([this, which, keepalive] (connected_socket fd, socket_address addr) mutable {
        fd.set_nodelay(true);
        fd.set_keepalive(keepalive);
        auto conn = new connection(*this, std::move(fd), addr);
        conn->process().then_wrapped([this, conn] (future<> f) {
            delete conn;
            try {
                f.get();
            } catch (std::exception& ex) {
                logger.debug("request error {}", ex.what());
            }
        });
        do_accepts(which, keepalive);
    }).then_wrapped([] (future<> f) {
        try {
            f.get();
        } catch (std::exception& ex) {
            std::cout << "accept failed: " << ex.what() << "\n";
        }
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

thrift_stats::thrift_stats(thrift_server& server)
        : _regs{
            scollectd::add_polled_metric(
                scollectd::type_instance_id("thrift", scollectd::per_cpu_plugin_instance,
                        "connections", "thrift-connections"),
                scollectd::make_typed(scollectd::data_type::DERIVE,
                        [&server] { return server.total_connections(); })),
            scollectd::add_polled_metric(
                scollectd::type_instance_id("thrift", scollectd::per_cpu_plugin_instance,
                        "current_connections", "current"),
                scollectd::make_typed(scollectd::data_type::GAUGE,
                        [&server] { return server.current_connections(); })),
            scollectd::add_polled_metric(
                scollectd::type_instance_id("thrift", scollectd::per_cpu_plugin_instance,
                        "total_requests", "served"),
                scollectd::make_typed(scollectd::data_type::DERIVE,
                        [&server] { return server.requests_served(); })),
        } {
}

