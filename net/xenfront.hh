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

#ifndef XENFRONT_HH_
#define XENFRONT_HH_

#include <memory>
#include "net.hh"
#include "core/sstring.hh"
#include "core/xen/gntalloc.hh"
#include "core/queue.hh"

namespace xen {

std::unique_ptr<net::device> create_xenfront_net_device(boost::program_options::variables_map opts, bool userspace);
boost::program_options::options_description get_xenfront_net_options_description();

struct netif_tx_request {
    uint32_t gref;
    uint16_t offset;
    struct {
        uint16_t csum_blank : 1;
        uint16_t data_validated : 1;
        uint16_t more_data : 1;
        uint16_t extra_info : 1;
        uint16_t pad : 12;
    } flags;
    uint16_t id;
    uint16_t size;
};

struct netif_tx_response {
    uint16_t id;
    int16_t  status;
};

struct netif_rx_request {
    uint16_t id;
    uint32_t gref;
};

struct netif_rx_response {
    uint16_t id;
    uint16_t offset;       /* Offset in page of start of received packet  */
    uint16_t flags;        /* NETRXF_* */
    int16_t  status;       /* -ve: NETIF_RSP_* ; +ve: Rx'ed response size. */
};

union tx {
    struct netif_tx_request  req;
    struct netif_tx_response rsp;
};

union rx {
    struct netif_rx_request  req;
    struct netif_rx_response rsp;
};

template <typename T>
class sring {
public:
    uint32_t req_prod = 0;
    uint32_t req_event = 1;
    uint32_t rsp_prod = 0;
    uint32_t rsp_event = 1;
    uint8_t  pad[48] = { 0 };
    T _ring[1];
    void dump() {
        printf("Shared ring status: req_prod: %d, req_event %d, rsp_prod %d, rsp_event %d\n", req_prod, req_event, rsp_prod, rsp_event);
    }
    sring<T>() = default;
};

using phys = uint64_t;
class xenfront_qp;

template <typename T>
class front_ring {
public:
    static constexpr uint32_t nr_ents = 256; /* FIXME : DYN */
    class entries {
    protected:
        std::queue<unsigned, circular_buffer<unsigned>> _ids;
        semaphore _available = { front_ring::nr_ents };
    private:
        std::array<gntref, front_ring<T>::nr_ents> _entries;
        front_ring<T> *_ring;
        std::atomic<uint32_t> _next_idx = { 0 };
    public:
        entries(front_ring<T> *ring) : _ring(ring) {}
        gntref& operator[](std::size_t i) { return _entries[_ring->idx(i)]; }
        friend front_ring;
        future<> has_room();
        unsigned get_index();
        void free_index(unsigned index);
    };
protected:
    static uint32_t idx(int i) { return i & (nr_ents - 1); }
public:
    uint32_t req_prod_pvt = 0;
    uint32_t rsp_cons = 0;
    int32_t  ref = -1;

    front_ring(gntref r, xenfront_qp& dev)
        : ref(r.xen_id), entries(this)
        , _sring(new (r.page) sring<T>()), _dev(dev) {
    }

    entries entries;

    void process_ring(std::function<bool (gntref &entry, T& el)> func, grant_head *refs);

    void dump() {
        _sring->dump();
        printf("Ring status: req_prod_pvt: %d, rsp cons %d\n\n", req_prod_pvt, rsp_cons);
    }

    void dump(const char *str, netif_tx_response &r) {
        printf("%s: tx_response: id %hu, status %hd\n", str, r.id, r.status);
        dump();

    }

    void dump(const char *str, netif_rx_response &r) {
        printf("%s: rx_response: id %hu, offset %hu, flags %hx, status %hd\n", str, r.id, r.offset, r.flags, r.status);
        dump();
    }

    sring<T> *_sring;
    T& operator[](std::size_t i) { return _sring->_ring[idx(i)]; }
private:
    xenfront_qp& _dev;
};

}

#endif /* XENFRONT_HH_ */
