/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef POSIX_STACK_HH_
#define POSIX_STACK_HH_

#include "core/reactor.hh"
#include <boost/program_options.hpp>

namespace net {

data_source posix_data_source(pollable_fd& fd);
data_sink posix_data_sink(pollable_fd& fd);

class posix_data_source_impl final : public data_source_impl {
    pollable_fd& _fd;
    temporary_buffer<char> _buf;
    size_t _buf_size;
public:
    explicit posix_data_source_impl(pollable_fd& fd, size_t buf_size = 8192)
        : _fd(fd), _buf(buf_size), _buf_size(buf_size) {}
    virtual future<temporary_buffer<char>> get() override;
};

class posix_data_sink_impl : public data_sink_impl {
    pollable_fd& _fd;
    packet _p;
public:
    explicit posix_data_sink_impl(pollable_fd& fd) : _fd(fd) {}
    future<> put(packet p) override;
    future<> put(temporary_buffer<char> buf) override;
    future<> close() override {
        _fd.close();
        return make_ready_future<>();
    }
};

class posix_ap_server_socket_impl : public server_socket_impl {
    struct connection {
        pollable_fd fd;
        socket_address addr;
        connection(pollable_fd xfd, socket_address xaddr) : fd(std::move(xfd)), addr(xaddr) {}
    };
    static thread_local std::unordered_map<::sockaddr_in, promise<connected_socket, socket_address>> sockets;
    static thread_local std::unordered_multimap<::sockaddr_in, connection> conn_q;
    socket_address _sa;
public:
    explicit posix_ap_server_socket_impl(socket_address sa) : _sa(sa) {}
    virtual future<connected_socket, socket_address> accept();
    void abort_accept();
    static void move_connected_socket(socket_address sa, pollable_fd fd, socket_address addr);
};

class posix_server_socket_impl : public server_socket_impl {
    socket_address _sa;
    pollable_fd _lfd;
public:
    explicit posix_server_socket_impl(socket_address sa, pollable_fd lfd) : _sa(sa), _lfd(std::move(lfd)) {}
    virtual future<connected_socket, socket_address> accept();
    void abort_accept();
};

class posix_reuseport_server_socket_impl : public server_socket_impl {
    socket_address _sa;
    pollable_fd _lfd;
public:
    explicit posix_reuseport_server_socket_impl(socket_address sa, pollable_fd lfd) : _sa(sa), _lfd(std::move(lfd)) {}
    virtual future<connected_socket, socket_address> accept();
    void abort_accept();
};

class posix_network_stack : public network_stack {
private:
    const bool _reuseport;
public:
    explicit posix_network_stack(boost::program_options::variables_map opts) : _reuseport(engine().posix_reuseport_available()) {}
    virtual server_socket listen(socket_address sa, listen_options opts) override;
    virtual future<connected_socket> connect(socket_address sa, socket_address local) override;
    virtual net::udp_channel make_udp_channel(ipv4_addr addr) override;
    static future<std::unique_ptr<network_stack>> create(boost::program_options::variables_map opts) {
        return make_ready_future<std::unique_ptr<network_stack>>(std::unique_ptr<network_stack>(new posix_network_stack(opts)));
    }
    virtual bool has_per_core_namespace() override { return _reuseport; };
};

class posix_ap_network_stack : public posix_network_stack {
private:
    const bool _reuseport;
public:
    posix_ap_network_stack(boost::program_options::variables_map opts) : posix_network_stack(std::move(opts)), _reuseport(engine().posix_reuseport_available()) {}
    virtual server_socket listen(socket_address sa, listen_options opts) override;
    virtual future<connected_socket> connect(socket_address sa, socket_address local) override;
    static future<std::unique_ptr<network_stack>> create(boost::program_options::variables_map opts) {
        return make_ready_future<std::unique_ptr<network_stack>>(std::unique_ptr<network_stack>(new posix_ap_network_stack(opts)));
    }
};

}

#endif
