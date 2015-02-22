/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 *
 */

#include "ip.hh"
#include "core/print.hh"
#include "core/future-util.hh"
#include "core/shared_ptr.hh"
#include "toeplitz.hh"

namespace net {

std::ostream& operator<<(std::ostream& os, ipv4_address a) {
    auto ip = a.ip;
    return fprint(os, "%d.%d.%d.%d",
            (ip >> 24) & 0xff,
            (ip >> 16) & 0xff,
            (ip >> 8) & 0xff,
            (ip >> 0) & 0xff);
}

constexpr std::chrono::seconds ipv4::_frag_timeout;
constexpr uint32_t ipv4::_frag_low_thresh;
constexpr uint32_t ipv4::_frag_high_thresh;

ipv4::ipv4(interface* netif)
    : _netif(netif)
    , _global_arp(netif)
    , _arp(_global_arp)
    , _host_address(0)
    , _gw_address(0)
    , _netmask(0)
    , _l3(netif, eth_protocol_num::ipv4, [this] { return get_packet(); })
    , _rx_packets(_l3.receive([this] (packet p, ethernet_address ea) {
        return handle_received_packet(std::move(p), ea); },
      [this] (forward_hash& out_hash_data, packet& p, size_t off) {
        return forward(out_hash_data, p, off);}))
    , _tcp(*this)
    , _icmp(*this)
    , _udp(*this)
    , _l4({ { uint8_t(ip_protocol_num::tcp), &_tcp }, { uint8_t(ip_protocol_num::icmp), &_icmp }, { uint8_t(ip_protocol_num::udp), &_udp }}) {
    _frag_timer.set_callback([this] { frag_timeout(); });
}

bool ipv4::forward(forward_hash& out_hash_data, packet& p, size_t off)
{
    auto iph = p.get_header<ip_hdr>(off);

    out_hash_data.push_back(iph->src_ip.ip);
    out_hash_data.push_back(iph->dst_ip.ip);

    auto h = ntoh(*iph);
    auto l4 = _l4[h.ip_proto];
    if (l4) {
        if (h.mf() == false && h.offset() == 0) {
            // This IP datagram is atomic, forward according to tcp or udp connection hash
            l4->forward(out_hash_data, p, off + sizeof(ip_hdr));
        }
        // else forward according to ip fields only
    }
    return true;
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
    if (!hw_features().rx_csum_offload && !p.offload_info_ref().reassembled) {
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
        frag_limit_mem();
        auto frag_id = ipv4_frag_id{h.src_ip, h.dst_ip, h.id, h.ip_proto};
        auto& frag = _frags[frag_id];
        if (mf == false) {
            frag.last_frag_received = true;
        }
        // This is a newly created frag_id
        if (frag.mem_size == 0) {
            _frags_age.push_back(frag_id);
            frag.rx_time = clock_type::now();
        }
        auto added_size = frag.merge(h, offset, std::move(p));
        _frag_mem += added_size;
        if (frag.is_complete()) {
            // All the fragments are received
            auto dropped_size = frag.mem_size;
            auto& ip_data = frag.data.map.begin()->second;
            // Choose a cpu to forward this packet
            auto cpu_id = engine().cpu_id();
            auto l4 = _l4[h.ip_proto];
            if (l4) {
                size_t l4_offset = 0;
                forward_hash hash_data;
                hash_data.push_back(hton(h.src_ip.ip));
                hash_data.push_back(hton(h.dst_ip.ip));
                l4->forward(hash_data, ip_data, l4_offset);
                cpu_id = _netif->hash2cpu(toeplitz_hash(rsskey, hash_data));
            }

            // No need to forward if the dst cpu is the current cpu
            if (cpu_id == engine().cpu_id()) {
                l4->received(std::move(ip_data), h.src_ip, h.dst_ip);
            } else {
                auto to = _netif->hw_address();
                auto pkt = frag.get_assembled_packet(from, to);
                _netif->forward(cpu_id, std::move(pkt));
            }

            // Delete this frag from _frags and _frags_age
            frag_drop(frag_id, dropped_size);
            _frags_age.remove(frag_id);
        } else {
            // Some of the fragments are missing
            if (!_frag_timer.armed()) {
                frag_arm();
            }
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

future<ethernet_address> ipv4::get_l2_dst_address(ipv4_address to) {
    // Figure out where to send the packet to. If it is a directly connected
    // host, send to it directly, otherwise send to the default gateway.
    ipv4_address dst;
    if (in_my_netmask(to)) {
        dst = to;
    } else {
        dst = _gw_address;
    }

    return _arp.lookup(dst);
}

void ipv4::send(ipv4_address to, ip_protocol_num proto_num, packet p, ethernet_address e_dst) {
    auto needs_frag = this->needs_frag(p, proto_num, hw_features());

    auto send_pkt = [this, to, proto_num, needs_frag, e_dst] (packet& pkt, uint16_t remaining, uint16_t offset) mutable  {
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

        if (hw_features().tx_csum_ip_offload) {
            iph->csum = 0;
            pkt.offload_info_ref().needs_ip_csum = true;
        } else {
            checksummer csum;
            csum.sum(reinterpret_cast<char*>(iph), sizeof(*iph));
            iph->csum = csum.get();
        }

        _packetq.push_back(l3_protocol::l3packet{eth_protocol_num::ipv4, e_dst, std::move(pkt)});
    };

    if (needs_frag) {
        uint16_t offset = 0;
        uint16_t remaining = p.len();
        auto mtu = hw_features().mtu;

        while (remaining) {
            auto can_send = std::min(uint16_t(mtu - net::ipv4_hdr_len_min), remaining);
            remaining -= can_send;
            auto pkt = p.share(offset, can_send);
            send_pkt(pkt, remaining, offset);
            offset += can_send;
        }
    } else {
        // The whole packet can be send in one shot
        send_pkt(p, 0, 0);
    }
}

std::experimental::optional<l3_protocol::l3packet> ipv4::get_packet() {
    // _packetq will be mostly empty here unless it hold remnants of previously
    // fragmented packet
    if (_packetq.empty()) {
        for (size_t i = 0; i < _pkt_providers.size(); i++) {
            auto l4p = _pkt_providers[_pkt_provider_idx++]();
            if (_pkt_provider_idx == _pkt_providers.size()) {
                _pkt_provider_idx = 0;
            }
            if (l4p) {
                auto l4pv = std::move(l4p.value());
                send(l4pv.to, l4pv.proto_num, std::move(l4pv.p), l4pv.e_dst);
                break;
            }
        }
    }

    std::experimental::optional<l3_protocol::l3packet> p;
    if (!_packetq.empty()) {
        p = std::move(_packetq.front());
        _packetq.pop_front();
    }
    return p;
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

void ipv4::frag_limit_mem() {
    if (_frag_mem <= _frag_high_thresh) {
        return;
    }
    auto drop = _frag_mem - _frag_low_thresh;
    while (drop) {
        if (_frags_age.empty()) {
            return;
        }
        // Drop the oldest frag (first element) from _frags_age
        auto frag_id = _frags_age.front();
        _frags_age.pop_front();

        // Drop from _frags as well
        auto& frag = _frags[frag_id];
        auto dropped_size = frag.mem_size;
        frag_drop(frag_id, dropped_size);

        drop -= std::min(drop, dropped_size);
    }
}

void ipv4::frag_timeout() {
    if (_frags.empty()) {
        return;
    }
    auto now = clock_type::now();
    for (auto it = _frags_age.begin(); it != _frags_age.end();) {
        auto frag_id = *it;
        auto& frag = _frags[frag_id];
        if (now > frag.rx_time + _frag_timeout) {
            auto dropped_size = frag.mem_size;
            // Drop from _frags
            frag_drop(frag_id, dropped_size);
            // Drop from _frags_age
            it = _frags_age.erase(it);
        } else {
            // The further items can only be younger
            break;
        }
    }
    if (_frags.size() != 0) {
        frag_arm(now);
    } else {
        _frag_mem = 0;
    }
}

void ipv4::frag_drop(ipv4_frag_id frag_id, uint32_t dropped_size) {
    _frags.erase(frag_id);
    _frag_mem -= dropped_size;
}

int32_t ipv4::frag::merge(ip_hdr &h, uint16_t offset, packet p) {
    uint32_t old = mem_size;
    unsigned ip_hdr_len = h.ihl * 4;
    // Store IP header
    if (offset == 0) {
        header = p.share(0, ip_hdr_len);
    }
    // Sotre IP payload
    p.trim_front(ip_hdr_len);
    data.merge(offset, std::move(p));
    // Update mem size
    mem_size = header.memory();
    for (const auto& x : data.map) {
        mem_size += x.second.memory();
    }
    auto added_size = mem_size - old;
    return added_size;
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
    oi.reassembled = true;
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

    if (_queue_space.try_wait(p.len())) { // drop packets that do not fit the queue
        _inet.get_l2_dst_address(from).then([this, from, p = std::move(p)] (ethernet_address e_dst) mutable {
            _packetq.emplace_back(ipv4_traits::l4packet{from, std::move(p), e_dst, ip_protocol_num::icmp});
        });
    }
}

}
