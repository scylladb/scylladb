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

#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string.hpp>
#include "core/posix.hh"
#include "core/vla.hh"
#include "core/reactor.hh"
#include "core/future-util.hh"
#include "core/stream.hh"
#include "core/circular_buffer.hh"
#include "core/align.hh"
#include <atomic>
#include <list>
#include <queue>
#include <fcntl.h>
#include <linux/if_tun.h>
#include "ip.hh"
#include "net/native-stack.hh"

#include <xen/xen.h>

#include <xen/memory.h>
#include <xen/sys/gntalloc.h>

#include "core/xen/xenstore.hh"
#include "core/xen/evtchn.hh"

#include "xenfront.hh"
#include <unordered_set>

using namespace net;

namespace xen {

using phys = uint64_t;

class xenfront_device : public device {
public:
    xenstore* _xenstore = xenstore::instance();
private:
    net::hw_features _hw_features;
    ethernet_address _hw_address;
    std::string _device_str;

public:
    bool _userspace;

public:
    xenfront_device(boost::program_options::variables_map opts, bool userspace)
    : _hw_address(net::parse_ethernet_address(_xenstore->read(path("mac"))))
    , _device_str("device/vif/" + std::to_string(opts["vif"].as<unsigned>()))
    , _userspace(userspace) {
        _hw_features.rx_csum_offload = true;
        _hw_features.tx_csum_l4_offload = true;
    }

    std::string path(std::string s) { return _device_str + "/" + s; }

    ethernet_address hw_address() override {
        return _hw_address;
    }
    net::hw_features hw_features() override {
        return _hw_features;
    }
    virtual std::unique_ptr<qp> init_local_queue(boost::program_options::variables_map opts, uint16_t qid) override;
};

class xenfront_qp : public net::qp {
private:
    xenfront_device* _dev;

    unsigned _otherend;
    std::string _backend;
    gntalloc *_gntalloc;
    evtchn   *_evtchn;
    port _tx_evtchn;
    port _rx_evtchn;

    front_ring<tx> _tx_ring;
    front_ring<rx> _rx_ring;

    grant_head *_tx_refs;
    grant_head *_rx_refs;

    std::unordered_map<std::string, int> _features;
    static std::unordered_map<std::string, std::string> _supported_features;

    port bind_tx_evtchn(bool split);
    port bind_rx_evtchn(bool split);

    future<> alloc_rx_references();
    future<> handle_tx_completions();
    future<> queue_rx_packet();

    void alloc_one_rx_reference(unsigned id);
    std::string path(std::string s) { return _dev->path(s); }

public:
    explicit xenfront_qp(xenfront_device* dev, boost::program_options::variables_map opts);
    ~xenfront_qp();
    virtual void rx_start() override;
    virtual future<> send(packet p) override;
    void inc_rx_error_count() { ++_stats.rx.bad.total; }
};

std::unordered_map<std::string, std::string>
xenfront_qp::_supported_features = {
    { "feature-split-event-channels", "feature-split-event-channels" },
    { "feature-rx-copy", "request-rx-copy" }
};

void
xenfront_qp::rx_start() {
    keep_doing([this] {
        return _rx_evtchn.pending().then([this] {
            return queue_rx_packet();
        });
    });
}

future<>
xenfront_qp::send(packet _p) {

    uint32_t frag = 0;
    // There doesn't seem to be a way to tell xen, when using the userspace
    // drivers, to map a particular page. Therefore, the only alternative
    // here is to copy. All pages shared must come from the gntalloc mmap.
    //
    // A better solution could be to change the packet allocation path to
    // use a pre-determined page for data.
    //
    // In-kernel should be fine

    if (_p.nr_frags() > 1) {
        _stats.tx.good.update_copy_stats(_p.nr_frags(), _p.len());
    }

    // FIXME: negotiate and use scatter/gather
    _p.linearize();

    return _tx_ring.entries.has_room().then([this, p = std::move(_p), frag] () mutable {

        auto req_prod = _tx_ring._sring->req_prod;

        auto f = p.frag(frag);

        auto ref = _tx_refs->new_ref(f.base, f.size);

        unsigned idx = _tx_ring.entries.get_index();
        assert(!_tx_ring.entries[idx]);

        _tx_ring.entries[idx] = ref;

        auto req = &_tx_ring._sring->_ring[idx].req;
        req->gref = ref.xen_id;
        req->offset = 0;
        req->flags = {};
        if (p.offload_info().protocol != ip_protocol_num::unused) {
            req->flags.csum_blank = true;
            req->flags.data_validated = true;
        } else {
            req->flags.data_validated = true;
        }
        req->id = idx;
        req->size = f.size;

        _tx_ring.req_prod_pvt = idx;
        _tx_ring._sring->req_prod = req_prod + 1;
        _tx_ring._sring->req_event++;

        if ((frag + 1) == p.nr_frags()) {
            _tx_evtchn.notify();
        }

        _stats.tx.good.update_frags_stats(1, f.size);

        return make_ready_future<>();
    });

    // FIXME: Don't forget to clear all grant refs when frontend closes. Or is it automatic?
}

#define rmb() asm volatile("lfence":::"memory");
#define wmb() asm volatile("":::"memory");

template <typename T>
future<> front_ring<T>::entries::has_room() {
    return _available.wait();
}

template <typename T>
void front_ring<T>::entries::free_index(unsigned id) {
    _available.signal();
}

template <typename T>
unsigned front_ring<T>::entries::get_index() {
    return front_ring<T>::idx(_next_idx++);
}

template <typename T>
void front_ring<T>::process_ring(std::function<bool (gntref &entry, T& el)> func, grant_head *refs)
{
    auto prod = _sring->rsp_prod;
    rmb();

    for (unsigned i = rsp_cons; i != prod; i++) {
        auto el = _sring->_ring[idx(i)];

        if (el.rsp.status < 0) {
            dump("Packet error", el.rsp);
            _dev.inc_rx_error_count();
            continue;
        }

        auto& entry = entries[i];

        if (!func(entry, el)) {
            continue;
        }

        assert(entry.xen_id >= 0);

        refs->free_ref(entry);
        entries.free_index(i);

        prod = _sring->rsp_prod;
    }
    rsp_cons = prod;
    _sring->rsp_event = prod + 1;
}

future<> xenfront_qp::queue_rx_packet()
{
    uint64_t bunch = 0;
    uint64_t bytes = 0;

    _rx_ring.process_ring([this, &bunch, &bytes] (gntref &entry, rx &rx) mutable {
        packet p(static_cast<char *>(entry.page) + rx.rsp.offset, rx.rsp.status);

        _dev->l2receive(std::move(p));

        bytes += rx.rsp.status;
        bunch++;

        return true;
    }, _rx_refs);

    _stats.rx.good.update_pkts_bunch(bunch);
    //
    // Our XEN implementation only supports packets with a single fragment
    // at the moment.
    //
    _stats.rx.good.update_frags_stats(bunch, bytes);

    return make_ready_future<>();
}

void xenfront_qp::alloc_one_rx_reference(unsigned index) {

    _rx_ring.entries[index] = _rx_refs->new_ref();

    // This is how the backend knows where to put data.
    auto req =  &_rx_ring._sring->_ring[index].req;
    req->id = index;
    req->gref = _rx_ring.entries[index].xen_id;
}

future<> xenfront_qp::alloc_rx_references() {
    return _rx_ring.entries.has_room().then([this] () {
        unsigned i = _rx_ring.entries.get_index();

        auto req_prod = _rx_ring.req_prod_pvt;
        alloc_one_rx_reference(i);
        ++req_prod;
        _rx_ring.req_prod_pvt = req_prod;
        wmb();
        _rx_ring._sring->req_prod = req_prod;
        /* ready */
        _rx_evtchn.notify();
    });
}

future<> xenfront_qp::handle_tx_completions() {

    _tx_ring.process_ring([this] (gntref &entry, tx &tx) {
        if (tx.rsp.status == 1) {
            return false;
        }

        if (tx.rsp.status != 0) {
            _tx_ring.dump("TX positive packet error", tx.rsp);
            return false;
        }

        return true;
    }, _tx_refs);

    return make_ready_future<>();
}

port xenfront_qp::bind_tx_evtchn(bool split) {
    return _evtchn->bind();
}

port xenfront_qp::bind_rx_evtchn(bool split) {

    if (split) {
        return _evtchn->bind();
    }
    return _evtchn->bind(_tx_evtchn.number());
}

xenfront_qp::xenfront_qp(xenfront_device* dev, boost::program_options::variables_map opts)
    : qp(true), _dev(dev)
    , _otherend(_dev->_xenstore->read<int>(path("backend-id")))
    , _backend(_dev->_xenstore->read(path("backend")))
    , _gntalloc(gntalloc::instance(_dev->_userspace, _otherend))
    , _evtchn(evtchn::instance(_dev->_userspace, _otherend))
    , _tx_ring(_gntalloc->alloc_ref(), *this)
    , _rx_ring(_gntalloc->alloc_ref(), *this)
    , _tx_refs(_gntalloc->alloc_ref(front_ring<tx>::nr_ents))
    , _rx_refs(_gntalloc->alloc_ref(front_ring<rx>::nr_ents)) {

    auto all_features = _dev->_xenstore->ls(_backend);

    for (auto&& feat : all_features) {
        if (feat.compare(0, 8, "feature-") == 0) {
            auto val = _dev->_xenstore->read<int>(_backend + "/" + feat);
            try {
                auto key = _supported_features.at(feat);
                _features[key] = val;
            } catch (const std::out_of_range& oor) {
                _features[feat] = 0;
            }
        }
    }
    if (!opts["split-event-channels"].as<bool>()) {
        _features["feature-split-event-channels"] = 0;
    }

    bool split = _features["feature-split-event-channels"];

    _tx_evtchn = bind_tx_evtchn(split);
    _rx_evtchn = bind_rx_evtchn(split);

    {
        auto t = xenstore::xenstore_transaction();

        for (auto&& f: _features) {
            _dev->_xenstore->write(path(f.first), f.second, t);
        }

        if (split) {
            _dev->_xenstore->write<int>(path("event-channel-tx"), _tx_evtchn.number(), t);
            _dev->_xenstore->write<int>(path("event-channel-rx"), _rx_evtchn.number(), t);
        } else {
            _dev->_xenstore->write<int>(path("event-channel"), _rx_evtchn.number(), t);
        }
        _dev->_xenstore->write<int>(path("tx-ring-ref"), _tx_ring.ref, t);
        _dev->_xenstore->write<int>(path("rx-ring-ref"), _rx_ring.ref, t);
        _dev->_xenstore->write<int>(path("state"), 4, t);
    }

    // Register Rx error statistics
    _collectd_regs.push_back(
        scollectd::add_polled_metric(scollectd::type_instance_id("network"
                    , scollectd::per_cpu_plugin_instance
                    , "requests", "rx-errors")
                    , scollectd::make_typed(scollectd::data_type::GAUGE
                    , _stats.rx.bad.total)
    ));

    keep_doing([this] {
        return alloc_rx_references();
    });

    _rx_evtchn.umask();

    keep_doing([this] () {
        return _tx_evtchn.pending().then([this] {
            handle_tx_completions();
        });
    });

    _tx_evtchn.umask();
}

xenfront_qp::~xenfront_qp() {

    {
        auto t = xenstore::xenstore_transaction();

        for (auto& f: _features) {
            _dev->_xenstore->remove(path(f.first), t);
        }
        _dev->_xenstore->remove(path("event-channel-tx"), t);
        _dev->_xenstore->remove(path("event-channel-rx"), t);
        _dev->_xenstore->remove(path("event-channel"), t);
        _dev->_xenstore->remove(path("tx-ring-ref"), t);
        _dev->_xenstore->remove(path("rx-ring-ref"), t);
        _dev->_xenstore->write<int>(path("state"), 6, t);
    }

    _dev->_xenstore->write<int>(path("state"), 1);
}

boost::program_options::options_description
get_xenfront_net_options_description() {

    boost::program_options::options_description opts(
            "xenfront net options");
    opts.add_options()
        ("vif",
                boost::program_options::value<unsigned>()->default_value(0),
                "vif number to hijack")
        ("split-event-channels",
                boost::program_options::value<bool>()->default_value(true),
                "Split event channel support")
        ;
    return opts;
}

std::unique_ptr<qp> xenfront_device::init_local_queue(boost::program_options::variables_map opts, uint16_t qid) {
    assert(!qid);
    return std::make_unique<xenfront_qp>(this, opts);
}

std::unique_ptr<net::device> create_xenfront_net_device(boost::program_options::variables_map opts, bool userspace) {
    static bool called = false;
    assert(!called);
    called = true;

    return std::make_unique<xenfront_device>(opts, userspace);
}

}
