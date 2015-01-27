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
    boost::program_options::variables_map opts;
    opts.insert(std::make_pair("tap-device", boost::program_options::variable_value(std::string("tap0"), false)));

    auto vnet = create_virtio_net_device(opts);
    vnet->set_local_queue(vnet->init_local_queue(opts, 0));

    interface netif(std::move(vnet));
    ipv4 inet(&netif);
    inet.set_host_address(ipv4_address("192.168.122.2"));
    engine().run();
    return 0;
}



