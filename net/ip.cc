/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 *
 */

#include "ip.hh"
#include "core/print.hh"
#include "core/future-util.hh"
#include "core/shared_ptr.hh"

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
    , _l3(netif, eth_protocol_num::ipv4)
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

    if (_packet_filter) {
        bool h = false;
        auto r = _packet_filter->forward(p, off + sizeof(ip_hdr), iph->src_ip, iph->dst_ip, h);
        if (h) {
            return r;
        }
    }

    auto l4 = _l4[iph->ip_proto];
    if (!l4) {
        return engine.cpu_id();
    }

    if (iph->mf() == false && iph->offset() == 0) {
        // This IP datagram is atomic, forward according to tcp or udp connection hash
        return l4->forward(p, off + sizeof(ip_hdr), iph->src_ip, iph->dst_ip);
    } else {
        // otherwise, forward according to frag_id hash
        auto frag_id = ipv4_frag_id{ntoh(iph->src_ip), ntoh(iph->dst_ip), iph->id, iph->ip_proto};
        return ipv4_frag_id::hash()(frag_id) % smp::count;
    }
}

bool ipv4::in_my_netmask(ipv4_address a) const {
    return !((a.ip ^ _host_address.ip) & _netmask.ip);
}

bool ipv4::needs_frag(packet& p, ip_protocol_num prot_num, net::hw_features hw_features) {
    if (p.len() + ipv4_hdr_len_min <= hw_features.mtu) {
        return false;
    }

    if ((prot_num == ip_protocol_num::tcp && hw_features.tx_tso) ||
        (prot_num == ip_protocol_num::udp && hw_features.tx_ufo)) {
        return false;
    }

    return true;
}

future<>
ipv4::handle_received_packet(packet p, ethernet_address from) {
    auto iph = p.get_header<ip_hdr>(0);
    if (!iph) {
        return make_ready_future<>();
    }

    // Skip checking csum of reassembled IP datagram
    if (p.offload_info().needs_ip_csum) {
        checksummer csum;
        csum.sum(reinterpret_cast<char*>(iph), sizeof(*iph));
        if (csum.get() != 0) {
            return make_ready_future<>();
        }
    }

    auto h = ntoh(*iph);
    unsigned ip_len = h.len;
    unsigned ip_hdr_len = h.ihl * 4;
    unsigned pkt_len = p.len();
    auto offset = h.offset();
    if (pkt_len > ip_len) {
        // Trim extra data in the packet beyond IP total length
        p.trim_back(pkt_len - ip_len);
    } else if (pkt_len < ip_len) {
        // Drop if it contains less than IP total length
        return make_ready_future<>();
    }
    // Drop if the reassembled datagram will be larger than maximum IP size
    if (offset + p.len() > net::ip_packet_len_max) {
        return make_ready_future<>();
    }

    // FIXME: process options
    if (in_my_netmask(h.src_ip) && h.src_ip != _host_address) {
        _arp.learn(from, h.src_ip);
    }

    if (_packet_filter) {
        bool handled = false;
        auto r = _packet_filter->handle(p, &h, from, handled);
        if (handled) {
            return std::move(r);
        }
    }

    if (h.dst_ip != _host_address) {
        // FIXME: forward
        return make_ready_future<>();
    }

    // Does this IP datagram need reassembly
    auto mf = h.mf();
    if (mf == true || offset != 0) {
        auto frag_id = ipv4_frag_id{h.src_ip, h.dst_ip, h.id, h.ip_proto};
        auto& frag = _frags[frag_id];
        if (mf == false) {
            frag.last_frag_received = true;
        }
        frag.merge(h, offset, std::move(p));
        if (frag.is_complete()) {
            // All the fragments are received
            auto& ip_data = frag.data.map.begin()->second;
            // Choose a cpu to forward this packet
            auto cpu_id = engine.cpu_id();
            auto l4 = _l4[h.ip_proto];
            if (l4) {
                size_t l4_offset = 0;
                cpu_id = l4->forward(ip_data, l4_offset, h.src_ip, h.dst_ip);
            }

            // No need to forward if the dst cpu is the current cpu
            if (cpu_id == engine.cpu_id()) {
                l4->received(std::move(ip_data), h.src_ip, h.dst_ip);
            } else {
                auto to = _netif->hw_address();
                auto pkt = frag.get_assembled_packet(from, to);
                _netif->forward(cpu_id, std::move(pkt));
            }

            // Delete this frag from _frags
            _frags.erase(frag_id);
        } else {
            // Some of the fragments are missing
            // TODO: Update the fragment timeout timer
        }
        return make_ready_future<>();
    }

    auto l4 = _l4[h.ip_proto];
    if (l4) {
        // Trim IP header and pass to upper layer
        p.trim_front(ip_hdr_len);
        l4->received(std::move(p), h.src_ip, h.dst_ip);
    }
    return make_ready_future<>();
}

future<> ipv4::send(ipv4_address to, ip_protocol_num proto_num, packet p) {
    uint16_t remaining = p.len();
    uint16_t offset = 0;
    auto needs_frag = this->needs_frag(p, proto_num, hw_features());

    // Figure out where to send the packet to. If it is a directly connected
    // host, send to it directly, otherwise send to the default gateway.
    ipv4_address dst;
    if (in_my_netmask(to)) {
        dst = to;
    } else {
        dst = _gw_address;
    }


    auto send_pkt = [this, to, dst, proto_num, needs_frag] (packet& pkt, uint16_t remaining, uint16_t offset) {
        auto iph = pkt.prepend_header<ip_hdr>();
        iph->ihl = sizeof(*iph) / 4;
        iph->ver = 4;
        iph->dscp = 0;
        iph->ecn = 0;
        iph->len = pkt.len();
        // FIXME: a proper id
        iph->id = 0;
        if (needs_frag) {
            uint16_t mf = remaining > 0;
            // The fragment offset is measured in units of 8 octets (64 bits)
            auto off = offset / 8;
            iph->frag = (mf << uint8_t(ip_hdr::frag_bits::mf)) | off;
        } else {
            iph->frag = 0;
        }
        iph->ttl = 64;
        iph->ip_proto = (uint8_t)proto_num;
        iph->csum = 0;
        iph->src_ip = _host_address;
        iph->dst_ip = to;
        *iph = hton(*iph);

        checksummer csum;
        csum.sum(reinterpret_cast<char*>(iph), sizeof(*iph));
        iph->csum = csum.get();

        return _arp.lookup(dst).then([this, pkt = std::move(pkt)] (ethernet_address e_dst) mutable {
            return send_raw(e_dst, std::move(pkt));
        });
    };

    if (needs_frag) {
        struct send_info {
            packet p;
            uint16_t remaining;
            uint16_t offset;
        };
        auto si = make_shared<send_info>({std::move(p), remaining, offset});
        auto stop = [si] { return si->remaining == 0; };
        auto send_frag = [this, send_pkt, si] () mutable {
            auto& remaining = si->remaining;
            auto& offset = si->offset;
            auto mtu = hw_features().mtu;
            auto can_send = std::min(uint16_t(mtu - net::ipv4_hdr_len_min), remaining);
            remaining -= can_send;
            auto pkt = si->p.share(offset, can_send);
            auto ret = send_pkt(pkt, remaining, offset);
            offset += can_send;
            return ret;
        };
        return do_until(stop, send_frag);
    } else {
        // The whole packet can be send in one shot
        remaining = 0;
        return send_pkt(p, remaining, offset);
    }
}

future<> ipv4::send_raw(ethernet_address dst, packet p) {
    return _l3.send(dst, std::move(p));
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

ipv4_address ipv4::gw_address() const {
    return _gw_address;
}

void ipv4::set_netmask_address(ipv4_address ip) {
    _netmask = ip;
}

ipv4_address ipv4::netmask_address() const {
    return _netmask;
}

void ipv4::set_packet_filter(ip_packet_filter * f) {
    _packet_filter = f;
}

ip_packet_filter * ipv4::packet_filter() const {
    return _packet_filter;
}

void ipv4::register_l4(ipv4::proto_type id, ip_protocol *protocol) {
    _l4.at(id) = protocol;
}

void ipv4::frag::merge(ip_hdr &h, uint16_t offset, packet p) {
    unsigned ip_hdr_len = h.ihl * 4;
    // Store IP header
    if (offset == 0) {
        header = p.share(0, ip_hdr_len);
    }
    // Sotre IP payload
    p.trim_front(ip_hdr_len);
    data.merge(offset, std::move(p));
}

bool ipv4::frag::is_complete() {
    // If all the fragments are received, ipv4::frag::merge() should merge all
    // the fragments into a single packet
    auto offset = data.map.begin()->first;
    auto nr_packet = data.map.size();
    return last_frag_received && nr_packet == 1 && offset == 0;
}

packet ipv4::frag::get_assembled_packet(ethernet_address from, ethernet_address to) {
    auto& ip_header = header;
    auto& ip_data = data.map.begin()->second;
    // Append a ethernet header, needed for forwarding
    auto eh = ip_header.prepend_header<eth_hdr>();
    eh->src_mac = from;
    eh->dst_mac = to;
    eh->eth_proto = uint16_t(eth_protocol_num::ipv4);
    *eh = hton(*eh);
    // Prepare a packet contains both ethernet header, ip header and ip data
    ip_header.append(std::move(ip_data));
    auto pkt = std::move(ip_header);
    auto iph = pkt.get_header<ip_hdr>(sizeof(eth_hdr));
    // len is the sum of each fragment
    iph->len = hton(uint16_t(pkt.len() - sizeof(eth_hdr)));
    // No fragmentation for the assembled datagram
    iph->frag = 0;
    // Since each fragment's csum is checked, no need to csum
    // again for the assembled datagram
    offload_info oi;
    oi.needs_ip_csum = false;
    pkt.set_offload_info(oi);
    return pkt;
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
