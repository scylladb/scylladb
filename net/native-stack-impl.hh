/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef NET_NATIVE_STACK_IMPL_HH_
#define NET_NATIVE_STACK_IMPL_HH_

#include "core/reactor.hh"

namespace net {

template <typename Protocol>
class native_server_socket_impl;

template <typename Protocol>
class native_connected_socket_impl;

class native_network_stack;

// native_server_socket_impl
template <typename Protocol>
class native_server_socket_impl : public server_socket_impl {
    typename Protocol::listener _listener;
public:
    native_server_socket_impl(Protocol& proto, uint16_t port, listen_options opt);
    virtual future<connected_socket, socket_address> accept() override;
};

template <typename Protocol>
native_server_socket_impl<Protocol>::native_server_socket_impl(Protocol& proto, uint16_t port, listen_options opt)
    : _listener(proto.listen(port)) {
}

template <typename Protocol>
future<connected_socket, socket_address>
native_server_socket_impl<Protocol>::accept() {
    return _listener.accept().then([this] (typename Protocol::connection conn) {
        return make_ready_future<connected_socket, socket_address>(
                connected_socket(std::make_unique<native_connected_socket_impl<Protocol>>(std::move(conn))),
                socket_address()); // FIXME: don't fake it
    });
}

// native_connected_socket_impl
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
class native_connected_socket_impl<Protocol>::native_data_source_impl final
    : public data_source_impl {
    typename Protocol::connection& _conn;
    size_t _cur_frag = 0;
    bool _eof = false;
    packet _buf;
public:
    explicit native_data_source_impl(typename Protocol::connection& conn)
        : _conn(conn) {}
    virtual future<temporary_buffer<char>> get() override {
        if (_eof) {
            return make_ready_future<temporary_buffer<char>>(temporary_buffer<char>(0));
        }
        if (_cur_frag != _buf.nr_frags()) {
            auto& f = _buf.fragments()[_cur_frag++];
            return make_ready_future<temporary_buffer<char>>(
                    temporary_buffer<char>(f.base, f.size,
                            make_deleter(deleter(), [p = _buf.share()] () mutable {})));
        }
        return _conn.wait_for_data().then([this] {
            _buf = _conn.read();
            _cur_frag = 0;
            _eof = !_buf.len();
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
    virtual future<> put(temporary_buffer<char> data) override {
        return _conn.send(packet({data.get_write(), data.size()}, data.release()));
    }
    virtual future<> close() override {
        _conn.close_write();
        return make_ready_future<>();
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

}



#endif /* NET_NATIVE_STACK_IMPL_HH_ */
