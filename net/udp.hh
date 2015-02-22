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
