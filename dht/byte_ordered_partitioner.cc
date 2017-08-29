/*
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "byte_ordered_partitioner.hh"
#include "utils/class_registrator.hh"
#include "utils/div_ceil.hh"
#include <boost/multiprecision/cpp_int.hpp>
#include <boost/multiprecision/cpp_dec_float.hpp>

namespace dht {

static const boost::multiprecision::cpp_int cppint_one{1};

token byte_ordered_partitioner::get_random_token()
{
    bytes b(bytes::initialized_later(), 16);
    *unaligned_cast<uint64_t>(b.begin()) = dht::get_random_number<uint64_t>();
    *unaligned_cast<uint64_t>(b.begin() + 8) = dht::get_random_number<uint64_t>();
    return token(token::kind::key, std::move(b));
}

static float ratio_helper(boost::multiprecision::cpp_int a, boost::multiprecision::cpp_int b, unsigned sigbits) {
    static boost::multiprecision::cpp_int cppint_max = cppint_one << sigbits;
    boost::multiprecision::cpp_int val;
    if (a >= b) {
        val = a - b;
    } else {
        val = cppint_max - (b - a);
    }
    boost::multiprecision::cpp_dec_float_100 f1(val);
    boost::multiprecision::cpp_dec_float_100 f2(cppint_max);
    boost::multiprecision::cpp_dec_float_100 ratio = f1 / f2;
    return ratio.convert_to<float>();
}

boost::multiprecision::cpp_int cppint_token(const token& t) {
    boost::multiprecision::cpp_int ret{0};

    // If the token is minimum token, token._data will be empty,
    // zero will be returned
    for (uint8_t d : t._data) {
        ret = (ret << 8) + d;
    }

    return ret;
}

std::map<token, float> byte_ordered_partitioner::describe_ownership(const std::vector<token>& sorted_tokens) {
    std::map<token, float> ownerships;
    auto i = sorted_tokens.begin();

    // 0-case
    if (i == sorted_tokens.end()) {
        throw runtime_exception("No nodes present in the cluster. Has this node finished starting up?");
    }
    // 1-case
    if (sorted_tokens.size() == 1) {
        ownerships[sorted_tokens[0]] = 1.0;
    // n-case
    } else {
        unsigned sigbits = 0;
        for (auto const& t : sorted_tokens) {
            sigbits = std::max(sigbits, t._data.size() * 8);
        }

        const token& start = sorted_tokens[0];
        auto ti = cppint_token(start);  // The first token and its value
        auto cppint_start = ti;
        auto tim1 = ti; // The last token and its value (after loop)
        for (i++; i != sorted_tokens.end(); i++) {
            ti = cppint_token(*i); // The next token and its value
            ownerships[*i]= ratio_helper(ti, tim1, sigbits);  // save (T(i) -> %age)
            tim1 = ti;
        }

        // The start token's range extends backward to the last token, which is why both were saved above.
        ownerships[start] = ratio_helper(cppint_start, ti, sigbits);
    }

    return ownerships;
}

token byte_ordered_partitioner::midpoint(const token& t1, const token& t2) const {
    unsigned sigbytes = std::max(t1._data.size(), t2._data.size());
    if (sigbytes == 0) {
        // The midpoint of two minimum token is minimum token
        return minimum_token();
    }

    auto l1 = cppint_token(t1);
    auto l2 = cppint_token(t2);
    auto sum = l1 + l2;
    bool remainder = bit_test(sum, 0);
    boost::multiprecision::cpp_int mid;
    if (t1 <= t2) {
        mid = sum / 2;
    } else {
        boost::multiprecision::cpp_int max = cppint_one << (sigbytes * 8);
        mid = (sum / 2 + max / 2) % max;
    }

    std::vector<int8_t> t;
    t.reserve(sigbytes + (remainder ? 1 : 0));
    // E.g., mid = 0x123456, sigbytes = 4, remainder = true
    while (mid) {
        t.push_back(mid.convert_to<int8_t>());
        mid >>= 8;
    }
    // now t = 0x56 0x34 0x12

    // Make the midpoint token of the same length as t1 or t2 whichever is longer
    while (t.size() < sigbytes) {
        t.push_back(0x00);
    }
    // now t = 0x56 0x34 0x12 0x00

    std::reverse(t.begin(), t.end());
    // now t = 0x00 0x12 0x34 0x56

    // Add one byte with the value 0x80 to the end of the byte array to present
    // the remainder
    if (remainder) {
        t.push_back(0x80);
    }
    // now t = 0x00 0x12 0x34 0x56 0x80

    return token(token::kind::key, managed_bytes(t.data(), t.size()));
}

unsigned
byte_ordered_partitioner::shard_of(const token& t) const {
    switch (t._kind) {
        case token::kind::before_all_keys:
            return 0;
        case token::kind::after_all_keys:
            return _shard_count - 1;
        case token::kind::key:
            if (t._data.empty()) {
                return 0;
            }
            // treat first byte as a fraction in the range [0, 1) and divide it evenly:
            return (uint8_t(t._data[0]) * _shard_count) >> 8;
    }
    abort();
}

token
byte_ordered_partitioner::token_for_next_shard(const token& t, shard_id shard, unsigned spans) const {
    switch (t._kind) {
    case token::kind::after_all_keys:
        return maximum_token();
    case token::kind::before_all_keys:
    case token::kind::key:
        auto orig = shard_of(t);
        if (shard <= orig || spans != 1) {
            return maximum_token();
        }
        auto e = div_ceil(shard << 8, _shard_count);
        return token(token::kind::key, managed_bytes({int8_t(e)}));
    }
    assert(0);
    throw std::invalid_argument("invalid token");
}


using registry = class_registrator<i_partitioner, byte_ordered_partitioner, const unsigned&, const unsigned&>;
static registry registrator("org.apache.cassandra.dht.ByteOrderedPartitioner");
static registry registrator_short_name("ByteOrderedPartitioner");

}
