/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#include "arp.hh"

namespace net {

arp_for_protocol::arp_for_protocol(arp& a, uint16_t proto_num)
    : _arp(a), _proto_num(proto_num) {
    _arp.add(proto_num, this);
}

arp_for_protocol::~arp_for_protocol() {
    _arp.del(_proto_num);
}

arp::arp(interface* netif) : _netif(netif), _proto(netif, 0x0806) {
    run();
}

void arp::add(uint16_t proto_num, arp_for_protocol* afp) {
    _arp_for_protocol[proto_num] = afp;
}

void arp::del(uint16_t proto_num) {
    _arp_for_protocol.erase(proto_num);
}

void arp::run() {
    _proto.receive().then([this] (packet p, ethernet_address from) {
        auto ah = p.get_header<arp_hdr>();
        ntoh(*ah);
        auto i = _arp_for_protocol.find(ah->ptype);
        hton(*ah); // return to raw state for further processing
        if (i != _arp_for_protocol.end()) {
            i->second->received(std::move(p));
        }
        run();
    });
}

}
