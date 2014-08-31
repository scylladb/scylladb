/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#include "arp.hh"
#include "ip.hh"
#include "net.hh"
#include "reactor.hh"
#include "virtio.hh"

using namespace net;

int main(int ac, char** av) {
    auto vnet = create_virtio_net_device("tap0");
    interface netif(std::move(vnet));
    netif.run();
    ipv4 inet(&netif);
    inet.set_host_address(ipv4_address(0xc0a87a02));
    the_reactor.run();
    return 0;
}



