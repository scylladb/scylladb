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

    ipv4_addr(uint32_t ip, uint16_t port) : ip(ip), port(port) {}
    ipv4_addr(uint16_t port) : ip(0), port(port) {}
    ipv4_addr(socket_address& sa) {
        ip = sa.u.in.sin_addr.s_addr;
        net::ntoh(ip);
        port = sa.u.in.sin_port;
        net::ntoh(port);
    }
};

static inline
bool is_unspecified(ipv4_addr& addr) {
    return addr.ip == 0;
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

#endif
