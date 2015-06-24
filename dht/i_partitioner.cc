/*
 * Copyright 2015 Cloudius Systems
 */

#include "i_partitioner.hh"
#include "murmur3_partitioner.hh"

namespace dht {

token
minimum_token() {
    return { token::kind::before_all_keys, {} };
}

token
maximum_token() {
    return { token::kind::after_all_keys, {} };
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
midpoint_unsigned_tokens(const token& t1, const token& t2) {
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
    if (t1 > t2) {
        // wrap around the ring.  We really want (t1 + (t2 + 1.0)) / 2, so add 0.5.
        // example: midpoint(0.9, 0.2) == midpoint(0.9, 1.2) == 1.05 == 0.05
        //                             == (0.9 + 0.2) / 2 + 0.5 (mod 1)
        if (avg.size() > 0) {
            avg[0] ^= 0x80;
        }
    }
    return token{token::kind::key, std::move(avg)};
}

static inline unsigned char get_byte(const bytes& b, size_t off) {
    if (off < b.size()) {
        return b[off];
    } else {
        return 0;
    }
}

bool i_partitioner::is_equal(const token& t1, const token& t2) {

    size_t sz = std::max(t1._data.size(), t2._data.size());

    for (size_t i = 0; i < sz; i++) {
        auto b1 = get_byte(t1._data, i);
        auto b2 = get_byte(t2._data, i);
        if (b1 != b2) {
            return false;
        }
    }
    return true;

}

bool i_partitioner::is_less(const token& t1, const token& t2) {

    size_t sz = std::max(t1._data.size(), t2._data.size());

    for (size_t i = 0; i < sz; i++) {
        auto b1 = get_byte(t1._data, i);
        auto b2 = get_byte(t2._data, i);
        if (b1 < b2) {
            return true;
        } else if (b1 > b2) {
            return false;
        }
    }
    return false;
}

bool operator==(const token& t1, const token& t2)
{
    if (t1._kind != t2._kind) {
        return false;
    } else if (t1._kind == token::kind::key) {
        return global_partitioner().is_equal(t1, t2);
    }
    return true;
}

bool operator<(const token& t1, const token& t2)
{
    if (t1._kind < t2._kind) {
        return true;
    } else if (t1._kind == token::kind::key && t2._kind == token::kind::key) {
        return global_partitioner().is_less(t1, t2);
    }
    return false;
}

std::ostream& operator<<(std::ostream& out, const token& t) {
    auto flags = out.flags();
    for (auto c : t._data) {
        unsigned char x = c;
        out << std::hex << std::setw(2) << std::setfill('0') << +x << " ";
    }
    out.flags(flags);
    return out;
}

std::ostream& operator<<(std::ostream& out, const decorated_key& dk) {
    return out << "{key: " << dk._key << ", token:" << dk._token << "}";
}

// FIXME: get from global config
// FIXME: make it per-keyspace
murmur3_partitioner default_partitioner;

i_partitioner&
global_partitioner() {
    return default_partitioner;
}

bool
decorated_key::equal(const schema& s, const decorated_key& other) const {
    if (_token == other._token) {
        return _key.legacy_equal(s, other._key);
    }
    return false;
}

int
decorated_key::tri_compare(const schema& s, const decorated_key& other) const {
    if (_token == other._token) {
        return _key.legacy_tri_compare(s, other._key);
    } else {
        return _token < other._token ? -1 : 1;
    }
}

bool
decorated_key::less_compare(const schema& s, const decorated_key& other) const {
    return tri_compare(s, other) < 0;
}

decorated_key::less_comparator::less_comparator(schema_ptr s)
    : s(std::move(s))
{ }

bool
decorated_key::less_comparator::operator()(const decorated_key& lhs, const decorated_key& rhs) const {
    return lhs.less_compare(*s, rhs);
}

std::ostream& operator<<(std::ostream& out, const ring_position& pos) {
    out << "{" << pos.token();
    if (pos.has_key()) {
        out << ", " << *pos.key();
    }
    return out << "}";
}

}
