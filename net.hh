/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef NET_HH_
#define NET_HH_

#include "reactor.hh"

namespace net {

struct fragment {
    char* base;
    size_t size;
};

struct packet {
    ~packet() {
        if (len) {
            completed.set_value();
        }
    }
    packet() = default;
    packet(packet&& x)
        : fragments(std::move(x.fragments)), len(x.len), completed(std::move(x.completed)) {
        x.len = 0;
    }
    std::vector<fragment> fragments;
    unsigned len = 0;
    promise<> completed;
};

class device {
public:
    virtual ~device() {}
    virtual future<packet> receive() = 0;
    virtual future<> send(packet p) = 0;
};

}

#endif /* NET_HH_ */
