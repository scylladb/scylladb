/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 *
 */

#ifndef UDP_HH_
#define UDP_HH_

#include <unordered_map>
#include <assert.h>
#include "core/reactor.hh"
#include "core/shared_ptr.hh"
#include "net/api.hh"
#include "const.hh"
#include "net.hh"

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

struct udp_channel_state {
    queue<udp_datagram> _queue;
    // Limit number of data queued into send queue
    semaphore _user_queue_space = {212992};
    udp_channel_state(size_t queue_size) : _queue(queue_size) {}
    future<> wait_for_send_buffer(size_t len) { return _user_queue_space.wait(len); }
    void complete_send(size_t len) { _user_queue_space.signal(len); }
};

}

#endif
