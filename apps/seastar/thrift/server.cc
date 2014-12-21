/*
 * Copyright 2014 Cloudius Systems
 */

#include "server.hh"
#include "core/future-util.hh"
#include "core/circular_buffer.hh"
#include "core/scollectd.hh"
#include "net/byteorder.hh"
#include "core/scattered_message.hh"
#include <thrift/server/TServer.h>
#include <thrift/transport/TBufferTransports.h>
#include <iostream>
#include <algorithm>
#include <unordered_map>
#include <queue>
#include <bitset>
#include <limits>
#include <cctype>
#include <vector>

using namespace apache::thrift::transport;

class thrift_stats {
    std::vector<scollectd::registration> _regs;
public:
    thrift_stats(thrift_server& server);
};

thrift_server::thrift_server()
        : _stats(new thrift_stats(*this)) {
}

class thrift_server::connection {
    thrift_server& _server;
    connected_socket _fd;
    input_stream<char> _read_buf;
    output_stream<char> _write_buf;
    temporary_buffer<char> _in_tmp;
    TMemoryBuffer _input;
    TMemoryBuffer _output;
public:
    connection(thrift_server& server, connected_socket&& fd, socket_address addr)
        : _server(server), _fd(std::move(fd)), _read_buf(_fd.input())
        , _write_buf(_fd.output()) {
        ++_server._total_connections;
        ++_server._current_connections;
    }
    ~connection() {
        --_server._current_connections;
    }
    future<> process() {
        return do_until([this] { return _read_buf.eof(); },
                [this] { return process_one_request(); });
    }
    future<> process_one_request() {
        return read().then([this] {
            ++_server._requests_served;
            // FIXME: serve the request
            return write();
        });
    }
    future<> read() {
        return _read_buf.read_exactly(4).then([this] (temporary_buffer<char> size_buf) {
            union {
                uint32_t n;
                char b[4];
            } data;
            std::copy_n(size_buf.get(), 4, data.b);
            auto n = ntohl(data.n);
            return _read_buf.read_exactly(n).then([this] (temporary_buffer<char> buf) {
                _in_tmp = std::move(buf); // keep ownership of the data
                auto b = reinterpret_cast<uint8_t*>(_in_tmp.get_write());
                _input.resetBuffer(b, _in_tmp.size());
            });
        });
    }
    future<> write() {
        uint8_t* data;
        uint32_t len;
        _output.getBuffer(&data, &len);
        net::packed<uint32_t> plen = { net::hton(len) };
        scattered_message<char> msg;
        msg.append(sstring(reinterpret_cast<char*>(&plen), 4));
        // _output protects data until the write is complete
        msg.append_static(reinterpret_cast<char*>(data), len);
        return _write_buf.write(std::move(msg));
    }
};

future<>
thrift_server::listen(ipv4_addr addr) {
    listen_options lo;
    lo.reuse_address = true;
    _listeners.push_back(engine.listen(make_ipv4_address(addr), lo));
    do_accepts(_listeners.size() - 1);
    return make_ready_future<>();
}

void
thrift_server::do_accepts(int which) {
    _listeners[which].accept().then([this, which] (connected_socket fd, socket_address addr) mutable {
        auto conn = new connection(*this, std::move(fd), addr);
        conn->process().rescue([this, conn] (auto&& get_ex) {
            delete conn;
            try {
                get_ex();
            } catch (std::exception& ex) {
                std::cout << "request error " << ex.what() << "\n";
            }
        });
        do_accepts(which);
    }).rescue([] (auto get_ex) {
        try {
            get_ex();
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
                        "thrift_requests", "served"),
                scollectd::make_typed(scollectd::data_type::DERIVE,
                        [&server] { return server.requests_served(); })),
        } {
}

