/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#include "net/net.hh"
#include "core/reactor.hh"
#include "net/virtio.hh"

using namespace net;

void dump_arp_packets(l3_protocol& proto) {
    proto.receive([&proto] (packet p, ethernet_address from) {
        std::cout << "seen arp packet\n";
        return make_ready_future<>();
    });
}

int main(int ac, char** av) {
    auto vnet = create_virtio_net_device("tap0");
    interface netif(std::move(vnet));
    l3_protocol arp(&netif, 0x0806);
    dump_arp_packets(arp);
    engine.run();
    return 0;
}

