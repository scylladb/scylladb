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

#include <xen/xen.h>

#include <xen/memory.h>
#include <xen/sys/gntalloc.h>

#include "core/xen/xenstore.hh"
#include "core/xen/evtchn.hh"

#include "xenfront.hh"

using namespace net;

using phys = uint64_t;

class xenfront_net_device : public net::device {
private:
    bool _userspace;
    stream<packet> _rx_stream;
    net::hw_features _hw_features;

    std::string _device_str;
    xenstore* _xenstore = xenstore::instance();
    unsigned _otherend;
    std::string _backend;
    gntalloc *_gntalloc;
    evtchn   *_evtchn;
    int _tx_evtchn;
    int _rx_evtchn;

    front_ring<tx> _tx_ring;
    front_ring<rx> _rx_ring;

    grant_head *_tx_refs;
    grant_head *_rx_refs;

    std::list<std::pair<std::string, std::string>> _features;

    ethernet_address _hw_address;

    int bind_tx_evtchn();
    int bind_rx_evtchn();

    future<> alloc_rx_references(unsigned refs);
    future<> queue_rx_packet();

    std::string path(std::string s) { return _device_str + "/" + s; }

public:
    explicit xenfront_net_device(boost::program_options::variables_map opts, bool userspace);
    ~xenfront_net_device();
    virtual subscription<packet> receive(std::function<future<> (packet)> next) override;
    virtual future<> send(packet p) override;

    ethernet_address hw_address();
    net::hw_features hw_features();
};

subscription<packet>
xenfront_net_device::receive(std::function<future<> (packet)> next) {
    auto sub = _rx_stream.listen(std::move(next));
    keep_doing([this] () {
        return _evtchn->pending(_rx_evtchn).then([this] {
            return queue_rx_packet();
        });
    });

    return std::move(sub);
}

future<>
xenfront_net_device::send(packet _p) {

    uint32_t frag = 0;
    // There doesn't seem to be a way to tell xen, when using the userspace
    // drivers, to map a particular page. Therefore, the only alternative
    // here is to copy. All pages shared must come from the gntalloc mmap.
    //
    // A better solution could be to change the packet allocation path to
    // use a pre-determined page for data.
    //
    // In-kernel should be fine

    // FIXME: negotiate and use scatter/gather
    _p.linearize();

    return _tx_ring.free_idx().then([this, p = std::move(_p), frag] (uint32_t idx)  {

        auto req_prod = _tx_ring._sring->req_prod;

        auto f = p.frag(frag);

        auto ref = _tx_refs->new_ref(f.base, f.size);

        _tx_ring.entries[idx] = ref;

        auto req = &_tx_ring._sring->_ring[idx].req;
        req->gref = ref.first;
        req->offset = 0;
        req->flags = 0;
        req->id = idx;
        req->size = f.size;

        _tx_ring.req_prod_pvt = idx;
        _tx_ring._sring->req_prod = req_prod + 1;

        _tx_ring._sring->req_event++;
        if ((frag + 1) == p.nr_frags()) {
            printf("NOTIFY!!\n");
            _evtchn->notify(_tx_evtchn);
            return make_ready_future<>();
        } else {
            return make_ready_future<>();
        }
    });

    // FIXME: Don't forget to clear all grant refs when frontend closes. Or is it automatic?
}

#define rmb() asm volatile("lfence":::"memory");
#define wmb() asm volatile("":::"memory");

// FIXME: This is totally wrong, just coded so we can gt started with sending
template <typename T>
future<uint32_t> front_ring<T>::free_idx() {
    static uint32_t idx = 0;
    return make_ready_future<uint32_t>(idx++);
}

future<> xenfront_net_device::queue_rx_packet() {

    printf("Got at queue\n");
    auto rsp_cons = _rx_ring.rsp_cons;
    rmb();
    auto rsp_prod = _rx_ring._sring->rsp_prod;

    while (rsp_cons < rsp_prod) {
        auto rsp = _rx_ring[rsp_cons].rsp;
        auto entry = _rx_ring.entries[rsp.id];

        rsp_cons++;
        _rx_ring.rsp_cons = rsp_cons;

        if (rsp.status < 0) {
            printf("Packet error: Handle it\n");
            continue;
        }
        auto rsp_size = rsp.status;

        packet p(static_cast<char *>(entry.second) + rsp.offset, rsp_size);
        _rx_stream.produce(std::move(p));

        auto req_prod = _rx_ring._sring->req_prod;
        if (req_prod >= _rx_ring.req_prod_pvt) {
            printf("Allocate more\n"); // FIXME: This is futurized as well,
        }

        _rx_ring._sring->rsp_event = rsp_cons + 1;
        _rx_ring._sring->req_prod  = rsp_cons + 1;

        rsp_prod = _rx_ring._sring->rsp_prod;
        // FIXME: END GRANT. FIXME: ALLOCATE MORE MEMORY
    }

    // FIXME: Queue_rx maybe should not be a future then
    return make_ready_future<>();
}

future<> xenfront_net_device::alloc_rx_references(unsigned refs) {
    auto req_prod = _rx_ring.req_prod_pvt;
    rmb();

    for (auto i = req_prod; (i < _rx_ring.nr_ents) && (i < refs); ++i) {
        _rx_ring.entries[i] = _rx_refs->new_ref();

        // This is how the backend knows where to put data.
        auto req =  &_rx_ring._sring->_ring[i].req;
        req->id = i;
        req->gref = _rx_ring.entries[i].first;
        ++req_prod;
    }

    _rx_ring.req_prod_pvt = req_prod;
    wmb();
    _rx_ring._sring->req_prod = req_prod;
    /* ready */
    _evtchn->notify(_rx_evtchn);
    return make_ready_future();
}

ethernet_address xenfront_net_device::hw_address() {
    return _hw_address;
}

net::hw_features xenfront_net_device::hw_features() {
    return _hw_features;
}

int xenfront_net_device::bind_tx_evtchn() {
    return _evtchn->bind();
}

int xenfront_net_device::bind_rx_evtchn() {

    auto split = _xenstore->read<bool>(_backend + "/feature-split-event-channels");
    if (split) {
        return _evtchn->bind();
    }
    return _tx_evtchn;
}

xenfront_net_device::xenfront_net_device(boost::program_options::variables_map opts, bool userspace)
    : _userspace(userspace)
    , _rx_stream()
    , _device_str("device/vif/" + std::to_string(opts["vif"].as<unsigned>()))
    , _otherend(std::stoi(_xenstore->read(path("backend-id"))))
    , _backend(_xenstore->read(path("backend")))
    , _gntalloc(gntalloc::instance(_userspace, _otherend))
    , _evtchn(evtchn::instance(_userspace, _otherend))
    , _tx_evtchn(bind_tx_evtchn())
    , _rx_evtchn(bind_rx_evtchn())
    , _tx_ring(_gntalloc->alloc_ref())
    , _rx_ring(_gntalloc->alloc_ref())
    , _tx_refs(_gntalloc->alloc_ref(front_ring<tx>::nr_ents, false))
    , _rx_refs(_gntalloc->alloc_ref(front_ring<rx>::nr_ents, true))
    , _hw_address(net::parse_ethernet_address(_xenstore->read(path("mac")))) {

    _rx_stream.started();

    auto all_features = _xenstore->ls(_backend);

    // FIXME: Also have to remove what xenstar don't support,
    // but ATM we should support it all
    all_features.remove_if([](std::string& el) {
        return !!el.compare(0, 8, "feature-") && !!el.compare(0, 8, "request-");
    });

    for (auto&s : all_features) {
        auto value = _xenstore->read(_backend + "/" + s);
        _features.push_back(std::make_pair(s, value));
    }

    {
        auto t = xenstore::xenstore_transaction();

        for (auto& f: _features) {
            _xenstore->write(path(f.first), f.second, t);
        }

        _xenstore->write<int>(path("event-channel-tx"), _tx_evtchn, t);
        _xenstore->write<int>(path("event-channel-rx"), _rx_evtchn, t);
        _xenstore->write<int>(path("tx-ring-ref"), _tx_ring.ref, t);
        _xenstore->write<int>(path("rx-ring-ref"), _rx_ring.ref, t);
        _xenstore->write<int>(path("state"), 4, t);
    }

    alloc_rx_references(_rx_ring.nr_ents);
}

xenfront_net_device::~xenfront_net_device() {

    {
        auto t = xenstore::xenstore_transaction();

        for (auto& f: _features) {
            _xenstore->remove(path(f.first), t);
        }
        _xenstore->remove(path("event-channel-tx"), t);
        _xenstore->remove(path("event-channel-rx"), t);
        _xenstore->remove(path("tx-ring-ref"), t);
        _xenstore->remove(path("rx-ring-ref"), t);
        _xenstore->write<int>(path("state"), 6, t);
    }

    _xenstore->write<int>(path("state"), 1);
}

boost::program_options::options_description
get_xenfront_net_options_description() {

    boost::program_options::options_description opts(
            "xenfront net options");
    opts.add_options()
        ("vif",
                boost::program_options::value<unsigned>()->default_value(0),
                "vif number to hijack")
        ;
    return opts;
}


std::unique_ptr<net::device> create_xenfront_net_device(boost::program_options::variables_map opts, bool userspace) {
    auto ptr = std::make_unique<xenfront_net_device>(opts, userspace);
    // This assumes only one device per cpu. Will need to be fixed when
    // this assumption will no longer hold.
    dev = ptr.get();
    return std::move(ptr);
}
