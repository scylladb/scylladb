/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#include "net/arp.hh"
#include "net/ip.hh"
#include "net/net.hh"
#include "core/reactor.hh"
#include "net/virtio.hh"

using namespace net;

int main(int ac, char** av) {
    auto vnet = create_virtio_net_device("tap0");
    interface netif(std::move(vnet));
    ipv4 inet(&netif);
    inet.set_host_address(ipv4_address("192.168.122.2"));
    engine.run();
    return 0;
}



