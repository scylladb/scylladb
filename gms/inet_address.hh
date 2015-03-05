/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#pragma once

#include "net/ip.hh"
#include "util/serialization.hh"
#include <sstream>

namespace gms {

class inet_address {
private:
    // FIXME: ipv6
    net::ipv4_address _addr;
public:
    inet_address() = default;
    inet_address(int32_t ip)
        : _addr(uint32_t(ip)) {
    }
    void serialize(std::ostream& out) const {
        int8_t inet_address_size = sizeof(inet_address);
        serialize_int8(out, inet_address_size);
        serialize_int32(out, _addr.ip);
    }
    static inet_address deserialize(std::istream& in) {
        int8_t inet_address_size = deserialize_int8(in);
        assert(inet_address_size == sizeof(inet_address));
        return inet_address(deserialize_int32(in));
    }
    size_t serialized_size() const {
        return serialize_int8_size + serialize_int32_size;
    }
    friend inline bool operator<(const inet_address& x, const inet_address& y) {
        return x._addr.ip < y._addr.ip;
    }
    friend inline std::ostream& operator<<(std::ostream& os, const inet_address& x) {
        return os << x._addr;
    }
};

}
