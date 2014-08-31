/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#include "net.hh"
#include "reactor.hh"
#include "virtio.hh"

using namespace net;

void dump_arp_packets(l3_protocol& proto) {
    proto.receive().then([&proto] (packet p, ethernet_address from) {
        std::cout << "seen arp packet\n";
        dump_arp_packets(proto);
    });
}

int main(int ac, char** av) {
    auto vnet = create_virtio_net_device("tap0");
    interface netif(std::move(vnet));
    netif.run();
    l3_protocol arp(&netif, 0x0806);
    dump_arp_packets(arp);
    the_reactor.run();
    return 0;
}

