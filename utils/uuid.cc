/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */


#include "UUID.hh"
#include "net/byteorder.hh"
#include <random>
#include <boost/iterator/function_input_iterator.hpp>

namespace utils {

UUID
make_random_uuid() {
    // FIXME: keep in userspace
    static thread_local std::random_device urandom;
    static thread_local std::uniform_int_distribution<uint8_t> dist(0, 255);
    union {
        uint8_t b[16];
        struct {
            uint64_t msb, lsb;
        } w;
    } v;
    for (auto& b : v.b) {
        b = dist(urandom);
    }
    v.b[6] &= 0x0f;
    v.b[6] |= 0x40; // version 4
    v.b[8] &= 0x3f;
    v.b[8] |= 0x80; // IETF variant
    return UUID(net::hton(v.w.msb), net::hton(v.w.lsb));
}

}
