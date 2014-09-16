/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef NET_HH_
#define NET_HH_

#include "core/reactor.hh"
#include "core/deleter.hh"
#include "core/queue.hh"
#include "ethernet.hh"
#include "packet.hh"
#include <unordered_map>

namespace net {

class packet;
class interface;
class device;
class l3_protocol;

class l3_protocol {
    interface* _netif;
    uint16_t _proto_num;
public:
    explicit l3_protocol(interface* netif, uint16_t proto_num);
    future<> send(ethernet_address to, packet p);
    future<packet, ethernet_address> receive();
private:
    void received(packet p);
    friend class interface;
};

class interface {
    std::unique_ptr<device> _dev;
    std::unordered_map<uint16_t, queue<std::tuple<packet, ethernet_address>>> _proto_map;
    ethernet_address _hw_address;
private:
    future<packet, ethernet_address> receive(uint16_t proto_num);
    future<> send(uint16_t proto_num, ethernet_address to, packet p);
public:
    explicit interface(std::unique_ptr<device> dev);
    ethernet_address hw_address() { return _hw_address; }
    void run();
    void register_l3(uint16_t proto_num, size_t queue_length = 100);
    friend class l3_protocol;
};

class device {
public:
    virtual ~device() {}
    virtual future<packet> receive() = 0;
    virtual future<> send(packet p) = 0;
    virtual ethernet_address hw_address() = 0;
};
}

#endif /* NET_HH_ */
