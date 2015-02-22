/*
 * Copyright 2015 Cloudius Systems
 */

#include "i_partitioner.hh"

namespace dht {

token
minimum_token() {
    return { token::kind::before_all_keys, {} };
}

// result + overflow bit
std::pair<bytes, bool>
add_bytes(const bytes& b1, const bytes& b2, bool carry = false) {
    auto sz = std::max(b1.size(), b2.size());
    auto expand = [sz] (const bytes& b) {
        bytes ret(bytes::initialized_later(), sz);
        auto bsz = b.size();
        auto p = std::copy(b.begin(), b.end(), ret.begin());
        std::fill_n(p, sz - bsz, 0);
        return ret;
    };
    auto eb1 = expand(b1);
    auto eb2 = expand(b2);
    auto p1 = eb1.begin();
    auto p2 = eb2.begin();
    unsigned tmp = carry;
    for (size_t idx = 0; idx < sz; ++idx) {
        tmp += uint8_t(p1[sz - idx - 1]);
        tmp += uint8_t(p2[sz - idx - 1]);
        p1[sz - idx - 1] = tmp;
        tmp >>= std::numeric_limits<uint8_t>::digits;
    }
    return { std::move(eb1), bool(tmp) };
}

bytes
shift_right(bool carry, bytes b) {
    unsigned tmp = carry;
    auto sz = b.size();
    auto p = b.begin();
    for (size_t i = 0; i < sz; ++i) {
        auto lsb = p[i] & 1;
        p[i] = (tmp << std::numeric_limits<uint8_t>::digits) | uint8_t(p[i]) >> 1;
        tmp = lsb;
    }
    return b;
}

token
midpoint(const token& t1, const token& t2) {
    // calculate the average of the two tokens.
    // before_all_keys is implicit 0, after_all_keys is implicit 1.
    bool c1 = t1._kind == token::kind::after_all_keys;
    bool c2 = t1._kind == token::kind::after_all_keys;
    if (c1 && c2) {
        // both end-of-range tokens?
        return t1;
    }
    // we can ignore beginning-of-range, since their representation is 0.0
    auto sum_carry = add_bytes(t1._data, t2._data);
    auto& sum = sum_carry.first;
    // if either was end-of-range, we added 0.0, so pretend we added 1.0 and
    // and got a carry:
    bool carry = sum_carry.second || c1 || c2;
    auto avg = shift_right(carry, std::move(sum));
    return token{token::kind::key, std::move(avg)};
}


}
