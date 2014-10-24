/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

// tcp/network-stack integration

#ifndef NET_TCP_STACK_HH
#define NET_TCP_STACK_HH

class listen_options;
class server_socket;

namespace net {

class ipv4_traits;
template <typename InetTraits>
class tcp;

server_socket
tcpv4_listen(tcp<ipv4_traits>& tcpv4, uint16_t port, listen_options opts);

}

#endif
