/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 *
 */

#include "ip.hh"
#include "core/print.hh"

namespace net {

std::ostream& operator<<(std::ostream& os, ipv4_address a) {
    auto ip = a.ip;
    return fprint(os, "%d.%d.%d.%d",
            (ip >> 24) & 0xff,
            (ip >> 16) & 0xff,
            (ip >> 8) & 0xff,
            (ip >> 0) & 0xff);
}

ipv4::ipv4(interface* netif)
    : _netif(netif)
    , _global_arp(netif)
    , _arp(_global_arp)
    , _l3(netif, 0x0800)
    , _rx_packets(_l3.receive([this] (packet p, ethernet_address ea) {
        return handle_received_packet(std::move(p), ea); },
      [this] (packet& p, size_t off) {
        return handle_on_cpu(p, off);}))
    , _tcp(*this)
    , _icmp(*this)
    , _l4({ { uint8_t(ip_protocol_num::tcp), &_tcp }, { uint8_t(ip_protocol_num::icmp), &_icmp }}) {
}

unsigned ipv4::handle_on_cpu(packet& p, size_t off)
{
    auto iph = p.get_header<ip_hdr>(off);
    auto l4 = _l4[iph->ip_proto];
    if (!l4) {
        return engine._id;
    }
    return l4->forward(p, off + sizeof(ip_hdr), iph->src_ip, iph->dst_ip);
}

bool ipv4::in_my_netmask(ipv4_address a) const {
    return !((a.ip ^ _host_address.ip) & _netmask.ip);
}

future<>
ipv4::handle_received_packet(packet p, ethernet_address from) {
    auto iph = p.get_header<ip_hdr>(0);
    if (!iph) {
        return make_ready_future<>();
    }
    checksummer csum;
    csum.sum(reinterpret_cast<char*>(iph), sizeof(*iph));
    if (csum.get() != 0) {
        return make_ready_future<>();
    }
    ntoh(*iph);
    // FIXME: process options
    if (in_my_netmask(iph->src_ip) && iph->src_ip != _host_address) {
        _arp.learn(from, iph->src_ip);
    }
    if (iph->frag & 0x3fff) {
        // FIXME: defragment
        return make_ready_future<>();
    }
    if (iph->dst_ip != _host_address) {
        // FIXME: forward
        return make_ready_future<>();
    }
    auto l4 = _l4[iph->ip_proto];
    if (l4) {
        p.trim_front(iph->ihl * 4);
        l4->received(std::move(p), iph->src_ip, iph->dst_ip);
    }
    return make_ready_future<>();
}

future<> ipv4::send(ipv4_address to, ip_protocol_num proto_num, packet p) {
    // FIXME: fragment
    auto iph = p.prepend_header<ip_hdr>();
    iph->ihl = sizeof(*iph) / 4;
    iph->ver = 4;
    iph->dscp = 0;
    iph->ecn = 0;
    iph->len = p.len();
    iph->id = 0;
    iph->frag = 0;
    iph->ttl = 64;
    iph->ip_proto = (uint8_t)proto_num;
    iph->csum = 0;
    iph->src_ip = _host_address;

    iph->dst_ip = to;
    hton(*iph);
    checksummer csum;
    csum.sum(reinterpret_cast<char*>(iph), sizeof(*iph));
    iph->csum = csum.get();

    ipv4_address dst;
    if (in_my_netmask(to)) {
        dst = to;
    } else {
        dst = _gw_address;
    }

    return _arp.lookup(dst).then([this, p = std::move(p)] (ethernet_address e_dst) mutable {
        return _l3.send(e_dst, std::move(p));
    });
}

void ipv4::set_host_address(ipv4_address ip) {
    _host_address = ip;
    _arp.set_self_addr(ip);
}

ipv4_address ipv4::host_address() {
    return _host_address;
}

void ipv4::set_gw_address(ipv4_address ip) {
    _gw_address = ip;
}

void ipv4::set_netmask_address(ipv4_address ip) {
    _netmask = ip;
}

void ipv4::register_l4(ipv4::proto_type id, ip_protocol *protocol) {
    _l4.at(id) = protocol;
}

void icmp::received(packet p, ipaddr from, ipaddr to) {
    auto hdr = p.get_header<icmp_hdr>(0);
    if (!hdr || hdr->type != icmp_hdr::msg_type::echo_request) {
        return;
    }
    hdr->type = icmp_hdr::msg_type::echo_reply;
    hdr->code = 0;
    hdr->csum = 0;
    checksummer csum;
    csum.sum(reinterpret_cast<char*>(hdr), p.len());
    hdr->csum = csum.get();
    _inet.send(to, from, std::move(p));
}

}
