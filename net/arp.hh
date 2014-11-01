/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 *
 */

#ifndef ARP_HH_
#define ARP_HH_

#include "net.hh"
#include "core/reactor.hh"
#include "byteorder.hh"
#include "ethernet.hh"
#include "core/print.hh"
#include <unordered_map>

namespace net {

class arp;
class arp_for_protocol;
template <typename L3>
class arp_for;

class arp_for_protocol {
protected:
    arp& _arp;
    uint16_t _proto_num;
public:
    arp_for_protocol(arp& a, uint16_t proto_num);
    virtual ~arp_for_protocol();
    virtual future<> received(packet p) = 0;
    virtual unsigned forward(packet& p, size_t off) { return engine.cpu_id(); }
};

class arp {
    interface* _netif;
    l3_protocol _proto;
    subscription<packet, ethernet_address> _rx_packets;
    std::unordered_map<uint16_t, arp_for_protocol*> _arp_for_protocol;
private:
    struct arp_hdr {
        packed<uint16_t> htype;
        packed<uint16_t> ptype;

        template <typename Adjuster>
        void adjust_endianness(Adjuster a) { return a(htype, ptype); }
    };
public:
    explicit arp(interface* netif);
    void add(uint16_t proto_num, arp_for_protocol* afp);
    void del(uint16_t proto_num);
private:
    ethernet_address l2self() { return _netif->hw_address(); }
    future<> process_packet(packet p, ethernet_address from);
    unsigned handle_on_cpu(packet& p, size_t off);
    template <class l3_proto>
    friend class arp_for;
};

template <typename L3>
class arp_for : public arp_for_protocol {
public:
    using l2addr = ethernet_address;
    using l3addr = typename L3::address_type;
private:
    static constexpr auto max_waiters = 512;
    enum oper {
        op_request = 1,
        op_reply = 2,
    };
    struct arp_hdr {
        packed<uint16_t> htype;
        packed<uint16_t> ptype;
        uint8_t hlen;
        uint8_t plen;
        packed<uint16_t> oper;
        l2addr sender_hwaddr;
        l3addr sender_paddr;
        l2addr target_hwaddr;
        l3addr target_paddr;

        template <typename Adjuster>
        void adjust_endianness(Adjuster a) {
            a(htype, ptype, oper, sender_hwaddr, sender_paddr, target_hwaddr, target_paddr);
        }
    };
    struct resolution {
        std::vector<promise<l2addr>> _waiters;
        timer _timeout_timer;
    };
private:
    l3addr _l3self = L3::broadcast_address();
    std::unordered_map<l3addr, l2addr> _table;
    std::unordered_map<l3addr, resolution> _in_progress;
private:
    packet make_query_packet(l3addr paddr);
    virtual future<> received(packet p) override;
    virtual unsigned forward(packet& p, size_t off) override;
    future<> handle_request(arp_hdr* ah);
    l2addr l2self() { return _arp.l2self(); }
public:
    future<> send_query(const l3addr& paddr);
    explicit arp_for(arp& a) : arp_for_protocol(a, L3::arp_protocol_type()) {}
    future<ethernet_address> lookup(const l3addr& addr);
    void learn(l2addr l2, l3addr l3);
    void run();
    void set_self_addr(l3addr addr) { _l3self = addr; }
    friend class arp;
};

template <typename L3>
packet
arp_for<L3>::make_query_packet(l3addr paddr) {
    arp_hdr hdr;
    hdr.htype = ethernet::arp_hardware_type();
    hdr.ptype = L3::arp_protocol_type();
    hdr.hlen = sizeof(l2addr);
    hdr.plen = sizeof(l3addr);
    hdr.oper = op_request;
    hdr.sender_hwaddr = l2self();
    hdr.sender_paddr = _l3self;
    hdr.target_hwaddr = ethernet::broadcast_address();
    hdr.target_paddr = paddr;
    hton(hdr);
    return packet(reinterpret_cast<char*>(&hdr), sizeof(hdr));
}

template <typename L3>
future<>
arp_for<L3>::send_query(const l3addr& paddr) {
    return _arp._proto.send(ethernet::broadcast_address(), make_query_packet(paddr));
}

class arp_error : public std::runtime_error {
public:
    arp_error(const std::string& msg) : std::runtime_error(msg) {}
};

class arp_timeout_error : public arp_error {
public:
    arp_timeout_error() : arp_error("ARP timeout") {}
};

class arp_queue_full_error : public arp_error {
public:
    arp_queue_full_error() : arp_error("ARP waiter's queue is full") {}
};

template <typename L3>
future<ethernet_address>
arp_for<L3>::lookup(const l3addr& paddr) {
    auto i = _table.find(paddr);
    if (i != _table.end()) {
        return make_ready_future<ethernet_address>(i->second);
    }
    auto j = _in_progress.find(paddr);
    auto first_request = j == _in_progress.end();
    auto& res = first_request ? _in_progress[paddr] : j->second;

    if (first_request) {
        res._timeout_timer.set_callback([paddr, this, &res] {
            send_query(paddr);
            for (auto& w : res._waiters) {
                w.set_exception(arp_timeout_error());
            }
            res._waiters.clear();
        });
        res._timeout_timer.arm_periodic(std::chrono::seconds(1));
        send_query(paddr);
    }

    if (res._waiters.size() >= max_waiters) {
        return make_exception_future<ethernet_address>(arp_queue_full_error());
    }

    res._waiters.emplace_back();
    return res._waiters.back().get_future();
}

template <typename L3>
void
arp_for<L3>::learn(l2addr hwaddr, l3addr paddr) {
    _table[paddr] = hwaddr;
    auto i = _in_progress.find(paddr);
    if (i != _in_progress.end()) {
        auto& res = i->second;
        res._timeout_timer.cancel();
        for (auto &&pr : res._waiters) {
            pr.set_value(hwaddr);
        }
        _in_progress.erase(i);
    }
}

template <typename L3>
unsigned arp_for<L3>::forward(packet& p, size_t off)
{
    auto ah = p.get_header<arp_hdr>(off);
    auto oper = ah->oper;
    ntoh(oper);
    if (oper == op_reply) {
        return std::numeric_limits<unsigned>::max(); // broadcast reply
    }
    return engine.cpu_id();
}

template <typename L3>
future<>
arp_for<L3>::received(packet p) {
    auto ah = p.get_header<arp_hdr>();
    if (!ah) {
        return make_ready_future<>();
    }
    ntoh(*ah);
    if (ah->hlen != sizeof(l2addr) || ah->plen != sizeof(l3addr)) {
        return make_ready_future<>();
    }
    switch (ah->oper) {
    case op_request:
        return handle_request(ah);
    case op_reply:
        learn(ah->sender_hwaddr, ah->sender_paddr);
        return make_ready_future<>();
    default:
        return make_ready_future<>();
    }
}

template <typename L3>
future<>
arp_for<L3>::handle_request(arp_hdr* ah) {
    if (ah->target_paddr == _l3self
            && _l3self != L3::broadcast_address()) {
        ah->oper = op_reply;
        ah->target_hwaddr = ah->sender_hwaddr;
        ah->target_paddr = ah->sender_paddr;
        ah->sender_hwaddr = l2self();
        ah->sender_paddr = _l3self;
        hton(*ah);
        packet p(reinterpret_cast<char*>(ah), sizeof(*ah));
        return _arp._proto.send(ah->target_hwaddr, std::move(p));
    } else {
        return make_ready_future<>();
    }
}

}

#endif /* ARP_HH_ */
