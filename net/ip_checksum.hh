/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef IP_CHECKSUM_HH_
#define IP_CHECKSUM_HH_

#include "packet.hh"
#include <cstdint>
#include <cstddef>

namespace net {

uint16_t ip_checksum(const void* data, size_t len);

struct checksummer {
    __int128 csum = 0;
    bool odd = false;
    void sum(const char* data, size_t len);
    void sum(const packet& p);
    uint16_t get() const;
};

}

#endif /* IP_CHECKSUM_HH_ */
