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
 */

#include "ip.hh"

using namespace net;

namespace net {
namespace ipv4_udp_impl {

static inline
ipv4_addr
to_ipv4_addr(ipv4_address a, uint16_t port) {
    return {a.ip, port};
}

class native_datagram : public udp_datagram_impl {
private:
    ipv4_addr _src;
    ipv4_addr _dst;
    packet _p;
public:
    native_datagram(ipv4_address src, ipv4_address dst, packet p)
            : _p(std::move(p)) {
        udp_hdr* hdr = _p.get_header<udp_hdr>();
        auto h = ntoh(*hdr);
        _p.trim_front(sizeof(*hdr));
        _src = to_ipv4_addr(src, h.src_port);
        _dst = to_ipv4_addr(dst, h.dst_port);
    }

    virtual ipv4_addr get_src() override {
        return _src;
    };

    virtual ipv4_addr get_dst() override {
        return _dst;
    };

    virtual uint16_t get_dst_port() override {
        return _dst.port;
    }

    virtual packet& get_data() override {
        return _p;
    }
};

class native_channel : public udp_channel_impl {
private:
    ipv4_udp& _proto;
    ipv4_udp::registration _reg;
    bool _closed;
    lw_shared_ptr<udp_channel_state> _state;

public:
    native_channel(ipv4_udp &proto, ipv4_udp::registration reg, lw_shared_ptr<udp_channel_state> state)
            : _proto(proto)
            , _reg(reg)
            , _closed(false)
            , _state(state)
    {
    }

    virtual future<udp_datagram> receive() override {
        return _state->_queue.pop_eventually();
    }

    virtual future<> send(ipv4_addr dst, const char* msg) override {
        return send(dst, packet::from_static_data(msg, strlen(msg)));
    }

    virtual future<> send(ipv4_addr dst, packet p) override {
        auto len = p.len();
        return _state->wait_for_send_buffer(len).then([this, dst, p = std::move(p), len] () mutable {
            _proto.send(_reg.port(), dst, std::move(p), _state);
        });
    }

    virtual bool is_closed() const {
        return _closed;
    }

    virtual void close() override {
        _reg.unregister();
        _closed = true;
    }
};

} /* namespace ipv4_udp_impl */

using namespace net::ipv4_udp_impl;

const int ipv4_udp::default_queue_size = 1024;

ipv4_udp::ipv4_udp(ipv4& inet)
    : _inet(inet)
{
    _inet.register_packet_provider([this] {
        std::experimental::optional<ipv4_traits::l4packet> l4p;
        if (!_packetq.empty()) {
            ipv4_traits::l4packet p;
            lw_shared_ptr<udp_channel_state> channel;
            size_t len;
            std::tie(p, channel, len) = std::move(_packetq.front());
            _packetq.pop_front();
            l4p = std::move(p);
            channel->complete_send(len);
        }
        return l4p;
    });
}

bool ipv4_udp::forward(forward_hash& out_hash_data, packet& p, size_t off)
{
    auto uh = p.get_header<udp_hdr>(off);

    if (!uh) {
        out_hash_data.push_back(uh->src_port);
        out_hash_data.push_back(uh->dst_port);
    }
    return true;
}

void ipv4_udp::received(packet p, ipv4_address from, ipv4_address to)
{
    udp_datagram dgram(std::make_unique<native_datagram>(from, to, std::move(p)));

    auto chan_it = _channels.find(dgram.get_dst_port());
    if (chan_it != _channels.end()) {
        auto chan = chan_it->second;
        chan->_queue.push(std::move(dgram));
    }
}

void ipv4_udp::send(uint16_t src_port, ipv4_addr dst, packet &&p, lw_shared_ptr<udp_channel_state> channel)
{
    size_t len = p.len();
    auto src = _inet.host_address();
    auto hdr = p.prepend_header<udp_hdr>();
    hdr->src_port = src_port;
    hdr->dst_port = dst.port;
    hdr->len = p.len();
    *hdr = hton(*hdr);

    offload_info oi;
    checksummer csum;
    ipv4_traits::udp_pseudo_header_checksum(csum, src, dst, p.len());
    bool needs_frag = ipv4::needs_frag(p, ip_protocol_num::udp, _inet.hw_features());
    if (_inet.hw_features().tx_csum_l4_offload && !needs_frag) {
        hdr->cksum = ~csum.get();
        oi.needs_csum = true;
    } else {
        csum.sum(p);
        hdr->cksum = csum.get();
        oi.needs_csum = false;
    }
    oi.protocol = ip_protocol_num::udp;
    p.set_offload_info(oi);

    _inet.get_l2_dst_address(dst).then([this, dst, p = std::move(p), channel = std::move(channel), len] (ethernet_address e_dst) mutable {
        _packetq.emplace_back(std::make_tuple(ipv4_traits::l4packet{dst, std::move(p), e_dst, ip_protocol_num::udp}, std::move(channel), len));
    });
}

uint16_t ipv4_udp::next_port(uint16_t port) {
    return (port + 1) == 0 ? min_anonymous_port : port + 1;
}

udp_channel
ipv4_udp::make_channel(ipv4_addr addr) {
    if (!is_ip_unspecified(addr)) {
        throw std::runtime_error("Binding to specific IP not supported yet");
    }

    uint16_t bind_port;

    if (!is_port_unspecified(addr)) {
        if (_channels.count(addr.port)) {
            throw std::runtime_error("Address already in use");
        }
        bind_port = addr.port;
    } else {
        auto starting_port = _next_anonymous_port;
        while (_channels.count(_next_anonymous_port)) {
            _next_anonymous_port = next_port(_next_anonymous_port);
            if (starting_port == _next_anonymous_port) {
                throw std::runtime_error("No free port");
            }
        }

        bind_port = _next_anonymous_port;
        _next_anonymous_port = next_port(_next_anonymous_port);
    }

    auto chan_state = make_lw_shared<udp_channel_state>(_queue_size);
    _channels[bind_port] = chan_state;
    return udp_channel(std::make_unique<native_channel>(*this, registration(*this, bind_port), chan_state));
}

} /* namespace net */
