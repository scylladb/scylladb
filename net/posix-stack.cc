/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#include "posix-stack.hh"
#include "net.hh"
#include "packet.hh"
#include "api.hh"

namespace net {

class posix_connected_socket_impl final : public connected_socket_impl {
    pollable_fd _fd;
private:
    explicit posix_connected_socket_impl(pollable_fd fd) : _fd(std::move(fd)) {}
public:
    virtual input_stream<char> input() override { return input_stream<char>(posix_data_source(_fd)); }
    virtual output_stream<char> output() override { return output_stream<char>(posix_data_sink(_fd), 8192); }
    friend class posix_server_socket_impl;
    friend class posix_ap_server_socket_impl;
};

future<connected_socket, socket_address>
posix_server_socket_impl::accept() {
    return _lfd.accept().then([this] (pollable_fd fd, socket_address sa) {
        static unsigned balance = 0;
        auto cpu = balance++ % smp::count;

        if (cpu == engine.cpu_id()) {
            std::unique_ptr<connected_socket_impl> csi(new posix_connected_socket_impl(std::move(fd)));
            return make_ready_future<connected_socket, socket_address>(
                    connected_socket(std::move(csi)), sa);
        } else {
            smp::submit_to(cpu, [this, fd = std::move(fd.get_file_desc()), sa] () mutable {
                posix_ap_server_socket_impl::move_connected_socket(_sa, pollable_fd(std::move(fd)), sa);
            });
            return accept();
        }
    });
}

future<connected_socket, socket_address> posix_ap_server_socket_impl::accept() {
    auto conni = conn_q.find(_sa.as_posix_sockaddr_in());
    if (conni != conn_q.end()) {
        connection c = std::move(conni->second);
        conn_q.erase(conni);
        std::unique_ptr<connected_socket_impl> csi(new posix_connected_socket_impl(std::move(c.fd)));
        return make_ready_future<connected_socket, socket_address>(connected_socket(std::move(csi)), std::move(c.addr));
    } else {
        auto i = sockets.emplace(std::piecewise_construct, std::make_tuple(_sa.as_posix_sockaddr_in()), std::make_tuple());
        assert(i.second);
        return i.first->second.get_future();
    }
}

void  posix_ap_server_socket_impl::move_connected_socket(socket_address sa, pollable_fd fd, socket_address addr) {
    auto i = sockets.find(sa.as_posix_sockaddr_in());
    if (i != sockets.end()) {
        std::unique_ptr<connected_socket_impl> csi(new posix_connected_socket_impl(std::move(fd)));
        i->second.set_value(connected_socket(std::move(csi)), std::move(addr));
        sockets.erase(i);
    } else {
        conn_q.emplace(std::piecewise_construct, std::make_tuple(sa.as_posix_sockaddr_in()), std::make_tuple(std::move(fd), std::move(addr)));
    }
}

data_source posix_data_source(pollable_fd& fd) {
    return data_source(std::make_unique<posix_data_source_impl>(fd));
}

future<temporary_buffer<char>>
posix_data_source_impl::get() {
    return _fd.read_some(_buf.get_write(), _buf_size).then([this] (size_t size) {
        _buf.trim(size);
        auto ret = std::move(_buf);
        _buf = temporary_buffer<char>(_buf_size);
        return make_ready_future<temporary_buffer<char>>(std::move(ret));
    });
}

data_sink posix_data_sink(pollable_fd& fd) {
    return data_sink(std::make_unique<posix_data_sink_impl>(fd));
}

future<>
posix_data_sink_impl::put(std::vector<temporary_buffer<char>> data) {
    std::swap(data, _data);
    return do_write(0);
}

future<>
posix_data_sink_impl::do_write(size_t idx) {
    // FIXME: use writev
    return _fd.write_all(_data[idx].get(), _data[idx].size()).then([this, idx] (size_t size) mutable {
        assert(size == _data[idx].size()); // FIXME: exception? short write?
        if (++idx == _data.size()) {
            _data.clear();
            return make_ready_future<>();
        }
        return do_write(idx);
    });
}

server_socket
posix_network_stack::listen(socket_address sa, listen_options opt) {
    return server_socket(std::make_unique<posix_server_socket_impl>(sa, engine.posix_listen(sa, opt)));
}

thread_local std::unordered_map<::sockaddr_in, promise<connected_socket, socket_address>> posix_ap_server_socket_impl::sockets;
thread_local std::unordered_multimap<::sockaddr_in, posix_ap_server_socket_impl::connection> posix_ap_server_socket_impl::conn_q;

server_socket
posix_ap_network_stack::listen(socket_address sa, listen_options opt) {
    return server_socket(std::make_unique<posix_ap_server_socket_impl>(sa));
}

struct cmsg_with_pktinfo {
    struct cmsghdrcmh;
    struct in_pktinfo pktinfo;
};

std::vector<struct iovec> to_iovec(const packet& p) {
    std::vector<struct iovec> v;
    v.reserve(p.nr_frags());
    for (auto&& f : p.fragments()) {
        v.push_back({.iov_base = f.base, .iov_len = f.size});
    }
    return v;
}

class posix_udp_channel : public udp_channel_impl {
private:
    static constexpr int MAX_DATAGRAM_SIZE = 65507;
    struct recv_ctx {
        struct msghdr _hdr;
        struct iovec _iov;
        socket_address _src_addr;
        char* _buffer;
        cmsg_with_pktinfo _cmsg;

        recv_ctx() {
            memset(&_hdr, 0, sizeof(_hdr));
            _hdr.msg_iov = &_iov;
            _hdr.msg_iovlen = 1;
            _hdr.msg_name = &_src_addr.u.sa;
            _hdr.msg_namelen = sizeof(_src_addr.u.sas);
            memset(&_cmsg, 0, sizeof(_cmsg));
            _hdr.msg_control = &_cmsg;
            _hdr.msg_controllen = sizeof(_cmsg);
        }

        void prepare() {
            _buffer = new char[MAX_DATAGRAM_SIZE];
            _iov.iov_base = _buffer;
            _iov.iov_len = MAX_DATAGRAM_SIZE;
        }
    };
    struct send_ctx {
        struct msghdr _hdr;
        std::vector<struct iovec> _iovecs;
        socket_address _dst;
        packet _p;

        send_ctx() {
            memset(&_hdr, 0, sizeof(_hdr));
            _hdr.msg_name = &_dst.u.sa;
            _hdr.msg_namelen = sizeof(_dst.u.sas);
        }

        void prepare(ipv4_addr dst, packet p) {
            _dst = make_ipv4_address(dst);
            _p = std::move(p);
            _iovecs = std::move(to_iovec(_p));
            _hdr.msg_iov = _iovecs.data();
            _hdr.msg_iovlen = _iovecs.size();
        }
    };
    std::unique_ptr<pollable_fd> _fd;
    ipv4_addr _address;
    recv_ctx _recv;
    send_ctx _send;
    bool _closed;
public:
    posix_udp_channel(ipv4_addr bind_address)
            : _closed(false) {
        auto sa = make_ipv4_address(bind_address);
        file_desc fd = file_desc::socket(sa.u.sa.sa_family, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
        bool pktinfo_flag = true;
        ::setsockopt(fd.get(), SOL_IP, IP_PKTINFO, &pktinfo_flag, sizeof(pktinfo_flag));
        fd.bind(sa.u.sa, sizeof(sa.u.sas));
        _address = ipv4_addr(fd.get_address());
        _fd = std::make_unique<pollable_fd>(std::move(fd));
    }
    virtual ~posix_udp_channel() {};
    virtual future<udp_datagram> receive() override;
    virtual future<> send(ipv4_addr dst, const char *msg);
    virtual future<> send(ipv4_addr dst, packet p);
    virtual void close() override {
        _closed = true;
        _fd.reset();
    }
    virtual bool is_closed() const override { return _closed; }
};

future<> posix_udp_channel::send(ipv4_addr dst, const char *message) {
    auto len = strlen(message);
    return _fd->sendto(make_ipv4_address(dst), message, len)
            .then([len] (size_t size) { assert(size == len); });
}

future<> posix_udp_channel::send(ipv4_addr dst, packet p) {
    auto len = p.len();
    _send.prepare(dst, std::move(p));
    return _fd->sendmsg(&_send._hdr)
            .then([len] (size_t size) { assert(size == len); });
}

udp_channel
posix_network_stack::make_udp_channel(ipv4_addr addr) {
    return udp_channel(std::make_unique<posix_udp_channel>(addr));
}

class posix_datagram : public udp_datagram_impl {
private:
    ipv4_addr _src;
    ipv4_addr _dst;
    packet _p;
public:
    posix_datagram(ipv4_addr src, ipv4_addr dst, packet p) : _src(src), _dst(dst), _p(std::move(p)) {}
    virtual ipv4_addr get_src() override { return _src; }
    virtual ipv4_addr get_dst() override { return _dst; }
    virtual uint16_t get_dst_port() override { return _dst.port; }
    virtual packet& get_data() override { return _p; }
};

future<udp_datagram>
posix_udp_channel::receive() {
    _recv.prepare();
    return _fd->recvmsg(&_recv._hdr).then([this] (size_t size) {
        auto dst = ipv4_addr(_recv._cmsg.pktinfo.ipi_addr.s_addr, _address.port);
        return make_ready_future<udp_datagram>(udp_datagram(std::make_unique<posix_datagram>(
            _recv._src_addr, dst, packet(fragment{_recv._buffer, size}, [buf = _recv._buffer] { delete[] buf; }))));
    });
}

network_stack_registrator nsr_posix{"posix",
    boost::program_options::options_description(),
    [](boost::program_options::variables_map ops) {
        return smp::main_thread() ? posix_network_stack::create(ops) : posix_ap_network_stack::create(ops);
    },
    true
};

}
