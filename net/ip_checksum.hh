/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef IP_CHECKSUM_HH_
#define IP_CHECKSUM_HH_

#include "packet.hh"
#include <cstdint>
#include <cstddef>
#include <arpa/inet.h>

namespace net {

uint16_t ip_checksum(const void* data, size_t len);

struct checksummer {
    __int128 csum = 0;
    bool odd = false;
    void sum(const char* data, size_t len);
    void sum(const packet& p);
    void sum(uint8_t data) {
        if (!odd) {
            csum += data << 8;
        } else {
            csum += data;
        }
        odd = !odd;
    }
    void sum(uint16_t data) {
        if (odd) {
            sum(uint8_t(data >> 8));
            sum(uint8_t(data));
        } else {
            csum += data;
        }
    }
    void sum(uint32_t data) {
        if (odd) {
            sum(uint16_t(data));
            sum(uint16_t(data >> 16));
        } else {
            csum += data;
        }
    }
    void sum_many() {}
    template <typename T0, typename... T>
    void sum_many(T0 data, T... rest) {
        sum(data);
        sum_many(rest...);
    }
    uint16_t get() const;
};

}

#endif /* IP_CHECKSUM_HH_ */
