/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 *
 */

#ifndef UDP_HH_
#define UDP_HH_

#include <net/ip.hh>
#include <core/reactor.hh>
#include <core/shared_ptr.hh>
#include <unordered_map>
#include <assert.h>
#include "net/api.hh"

namespace net {

struct udp_hdr {
    packed<uint16_t> src_port;
    packed<uint16_t> dst_port;
    packed<uint16_t> len;
    packed<uint16_t> cksum;

    template<typename Adjuster>
    auto adjust_endianness(Adjuster a) {
        return a(src_port, dst_port, len, cksum);
    }
} __attribute__((packed));

static inline
ipv4_addr
to_ipv4_addr(ipv4_address a, uint16_t port) {
    return {a.ip, port};
}

struct udp_channel_state {
    queue<udp_datagram> _queue;
    udp_channel_state(size_t queue_size) : _queue(queue_size) {}
};

class udp_v4 : ip_protocol {
public:
    static const int default_queue_size;
private:
    static const int protocol_number = 17;
    static const uint16_t min_anonymous_port = 32768;
    ipv4 &_inet;
    std::unordered_map<uint16_t, shared_ptr<udp_channel_state>> _channels;
    int _queue_size = default_queue_size;
    uint16_t _next_anonymous_port = min_anonymous_port;
private:
    uint16_t next_port(uint16_t port);
public:
    class registration {
    private:
        udp_v4 &_proto;
        uint16_t _port;
    public:
        registration(udp_v4 &proto, uint16_t port) : _proto(proto), _port(port) {};

        void unregister() {
            _proto._channels.erase(_proto._channels.find(_port));
        }

        uint16_t port() const {
            return _port;
        }
    };

    udp_v4(ipv4& inet);
    udp_channel make_channel(ipv4_addr addr);
    virtual void received(packet p, ipv4_address from, ipv4_address to) override;
    future<> send(uint16_t src_port, ipv4_addr dst, packet &&p);
    void set_queue_size(int size) { _queue_size = size; }
};

}

#endif
