/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

// tcp/network-stack integration

#ifndef NET_TCP_STACK_HH
#define NET_TCP_STACK_HH

#include "core/future.hh"

class listen_options;
class server_socket;
class connected_socket;

namespace net {

class ipv4_traits;
template <typename InetTraits>
class tcp;

server_socket
tcpv4_listen(tcp<ipv4_traits>& tcpv4, uint16_t port, listen_options opts);

future<connected_socket>
tcpv4_connect(tcp<ipv4_traits>& tcpv4, socket_address sa);

}

#endif
