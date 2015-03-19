/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#pragma once

#include "types.hh"
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
    inet_address(const sstring& addr)
        : _addr(addr) {
    }
    bool is_broadcast_address() {
        return _addr == net::ipv4::broadcast_address();
    }
    void serialize(bytes::iterator& out) const {
        int8_t inet_address_size = sizeof(inet_address);
        serialize_int8(out, inet_address_size);
        serialize_int32(out, _addr.ip);
    }
    static inet_address deserialize(bytes_view& v) {
        int8_t inet_address_size = read_simple<int8_t>(v);
        assert(inet_address_size == sizeof(inet_address));
        return inet_address(read_simple<int32_t>(v));
    }
    size_t serialized_size() const {
        return serialize_int8_size + serialize_int32_size;
    }
    friend inline bool operator==(const inet_address& x, const inet_address& y) {
        return x._addr == y._addr;
    }
    friend inline bool operator<(const inet_address& x, const inet_address& y) {
        return x._addr.ip < y._addr.ip;
    }
    friend inline std::ostream& operator<<(std::ostream& os, const inet_address& x) {
        return os << x._addr;
    }
};

}
