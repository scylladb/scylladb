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

#ifndef NET_API_HH_
#define NET_API_HH_

#include <memory>
#include <vector>
#include <cstring>
#include "core/future.hh"
#include "net/byteorder.hh"
#include "net/packet.hh"
#include "core/print.hh"
#include "core/temporary_buffer.hh"
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/ip.h>

class socket_address {
public:
    union {
        ::sockaddr_storage sas;
        ::sockaddr sa;
        ::sockaddr_in in;
    } u;
    ::sockaddr& as_posix_sockaddr() { return u.sa; }
    ::sockaddr_in& as_posix_sockaddr_in() { return u.in; }
};

struct listen_options {
    bool reuse_address = false;
};

struct ipv4_addr {
    uint32_t ip;
    uint16_t port;

    ipv4_addr() : ip(0), port(0) {}
    ipv4_addr(uint32_t ip, uint16_t port) : ip(ip), port(port) {}
    ipv4_addr(uint16_t port) : ip(0), port(port) {}
    ipv4_addr(const std::string &addr);

    ipv4_addr(const socket_address &sa) {
        ip = net::ntoh(sa.u.in.sin_addr.s_addr);
        port = net::ntoh(sa.u.in.sin_port);
    }

    ipv4_addr(socket_address &&sa) : ipv4_addr(sa) {}
};

static inline
bool is_ip_unspecified(ipv4_addr &addr) {
    return addr.ip == 0;
}

static inline
bool is_port_unspecified(ipv4_addr &addr) {
    return addr.port == 0;
}

static inline
std::ostream& operator<<(std::ostream &os, ipv4_addr addr) {
    fprint(os, "%d.%d.%d.%d",
            (addr.ip >> 24) & 0xff,
            (addr.ip >> 16) & 0xff,
            (addr.ip >> 8) & 0xff,
            (addr.ip) & 0xff);
    return os << ":" << addr.port;
}

static inline
socket_address make_ipv4_address(ipv4_addr addr) {
    socket_address sa;
    sa.u.in.sin_family = AF_INET;
    sa.u.in.sin_port = htons(addr.port);
    sa.u.in.sin_addr.s_addr = htonl(addr.ip);
    return sa;
}

namespace net {

class udp_datagram_impl {
public:
    virtual ~udp_datagram_impl() {};
    virtual ipv4_addr get_src() = 0;
    virtual ipv4_addr get_dst() = 0;
    virtual uint16_t get_dst_port() = 0;
    virtual packet& get_data() = 0;
};

class udp_datagram final {
private:
    std::unique_ptr<udp_datagram_impl> _impl;
public:
    udp_datagram(std::unique_ptr<udp_datagram_impl>&& impl) : _impl(std::move(impl)) {};
    ipv4_addr get_src() { return _impl->get_src(); }
    ipv4_addr get_dst() { return _impl->get_dst(); }
    uint16_t get_dst_port() { return _impl->get_dst_port(); }
    packet& get_data() { return _impl->get_data(); }
};

class udp_channel_impl {
public:
    virtual ~udp_channel_impl() {};
    virtual future<udp_datagram> receive() = 0;
    virtual future<> send(ipv4_addr dst, const char* msg) = 0;
    virtual future<> send(ipv4_addr dst, packet p) = 0;
    virtual bool is_closed() const = 0;
    virtual void close() = 0;
};

class udp_channel {
private:
    std::unique_ptr<udp_channel_impl> _impl;
public:
    udp_channel() {}
    udp_channel(std::unique_ptr<udp_channel_impl> impl) : _impl(std::move(impl)) {}
    future<udp_datagram> receive() { return _impl->receive(); }
    future<> send(ipv4_addr dst, const char* msg) { return _impl->send(std::move(dst), msg); }
    future<> send(ipv4_addr dst, packet p) { return _impl->send(std::move(dst), std::move(p)); }
    bool is_closed() const { return _impl->is_closed(); }
    void close() { return _impl->close(); }
};

} /* namespace net */

#endif
