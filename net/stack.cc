/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#include "stack.hh"
#include "net.hh"
#include "ip.hh"
#include "tcp.hh"
#include "virtio.hh"
#include <memory>
#include <queue>

namespace net {

class native_networking_stack;

template <typename Protocol>
class native_server_socket_impl;

template <typename Protocol>
class native_connected_socket_impl;

template <typename Protocol>
class native_connected_socket_impl : public connected_socket_impl {
    typename Protocol::connection _conn;
    class native_data_source_impl;
    class native_data_sink_impl;
public:
    explicit native_connected_socket_impl(typename Protocol::connection conn)
        : _conn(std::move(conn)) {}
    virtual input_stream<char> input() override;
    virtual output_stream<char> output() override;
};

template <typename Protocol>
class native_server_socket_impl : public server_socket_impl {
    Protocol& _proto;
    uint16_t _port;
public:
    native_server_socket_impl(Protocol& proto, uint16_t port, listen_options opt);
    virtual future<connected_socket, socket_address> accept() override;
};

class native_networking_stack : public networking_stack {
    static std::unique_ptr<native_networking_stack> _s;
    interface _netif;
    ipv4 _inet;
    using tcp4 = tcp<ipv4_traits>;
public:
    explicit native_networking_stack(std::unique_ptr<device> dev);
    explicit native_networking_stack() : native_networking_stack(create_virtio_net_device("tap0")) {}
    virtual server_socket listen(socket_address sa, listen_options opt) override;
    friend class native_server_socket_impl<tcp4>;
};

native_networking_stack::native_networking_stack(std::unique_ptr<device> dev)
    : _netif(std::move(dev))
    , _inet(&_netif) {
    _netif.run();
    _inet.set_host_address(ipv4_address(0xc0a87a02));
}

template <typename Protocol>
native_server_socket_impl<Protocol>::native_server_socket_impl(Protocol& proto, uint16_t port, listen_options opt)
    : _proto(proto), _port(port) {
}

template <typename Protocol>
future<connected_socket, socket_address>
native_server_socket_impl<Protocol>::accept() {
    return _proto.listen(_port).then([this] (typename Protocol::connection conn) {
        return make_ready_future<connected_socket, socket_address>(
                connected_socket(std::make_unique<native_connected_socket_impl<Protocol>>(std::move(conn))),
                socket_address()); // FIXME: don't fake it
    });
}

server_socket
native_networking_stack::listen(socket_address sa, listen_options opts) {
    assert(sa.as_posix_sockaddr().sa_family == AF_INET);
    return server_socket(std::make_unique<native_server_socket_impl<tcp4>>(
            _inet.get_tcp(), sa.as_posix_sockaddr_in().sin_port, opts));
}

template <typename Protocol>
class native_connected_socket_impl<Protocol>::native_data_source_impl final
    : public data_source_impl {
    typename Protocol::connection& _conn;
    size_t _cur_frag = 0;
    packet _buf;
public:
    explicit native_data_source_impl(typename Protocol::connection& conn)
        : _conn(conn) {}
    virtual future<temporary_buffer<char>> get() override {
        if (_cur_frag != _buf.fragments.size()) {
            auto& f = _buf.fragments[_cur_frag++];
            return make_ready_future<temporary_buffer<char>>(
                    temporary_buffer<char>(f.base, f.size,
                            make_deleter(nullptr, [p = _buf.share()] () mutable {})));
        }
        return _conn.wait_for_data().then([this] {
            _buf = _conn.read();
            _cur_frag = 0;
            return get();
        });
    }
};

template <typename Protocol>
class native_connected_socket_impl<Protocol>::native_data_sink_impl final
    : public data_sink_impl {
    typename Protocol::connection& _conn;
public:
    explicit native_data_sink_impl(typename Protocol::connection& conn)
        : _conn(conn) {}
    virtual future<> put(std::vector<temporary_buffer<char>> data) override {
        std::vector<fragment> frags;
        frags.reserve(data.size());
        for (auto& e : data) {
            frags.push_back(fragment{e.get_write(), e.size()});
        }
        return _conn.send(packet(std::move(frags), [tmp = std::move(data)] () mutable {}));
    }
};

template <typename Protocol>
input_stream<char>
native_connected_socket_impl<Protocol>::input() {
    data_source ds(std::make_unique<native_data_source_impl>(_conn));
    return input_stream<char>(std::move(ds));
}

template <typename Protocol>
output_stream<char>
native_connected_socket_impl<Protocol>::output() {
    data_sink ds(std::make_unique<native_data_sink_impl>(_conn));
    return output_stream<char>(std::move(ds), 8192);
}


std::unique_ptr<native_networking_stack> native_networking_stack::_s;

networking_stack_registrator<native_networking_stack> nns_registrator{"native"};

}
