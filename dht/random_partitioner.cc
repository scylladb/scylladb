/*
 * Copyright (C) 2016 ScyllaDB
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

#include "md5_hasher.hh"
#include "random_partitioner.hh"
#include "utils/class_registrator.hh"
#include "utils/div_ceil.hh"
#include <boost/multiprecision/cpp_int.hpp>

namespace dht {

static const boost::multiprecision::uint128_t cppint_one{1};
static const boost::multiprecision::uint128_t cppint127_max = cppint_one << 127;

// Convert token's byte array to integer value.
static boost::multiprecision::uint128_t token_to_cppint(token_view t) {
    boost::multiprecision::uint128_t ret{0};
    // If the token is minimum token, token._data will be empty,
    // zero will be returned
    for (uint8_t d : t._data) {
        ret = (ret << 8) + d;
    }
    return ret;
}

// Store integer value for the token into token's byte array. The value must be within [0, 2 ^ 127].
static token cppint_to_token(boost::multiprecision::uint128_t i) {
    if (i == 0) {
        return minimum_token();
    }
    if (i > cppint127_max) {
        throw std::runtime_error(sprint("RandomPartitioner value %s must be within [0, 2 ^ 127]", i));
    }
    std::vector<int8_t> t;
    while (i) {
        static boost::multiprecision::uint128_t byte_mask = 0xFF;
        auto data = (i & byte_mask).convert_to<uint8_t>();
        t.push_back(data);
        i >>= 8;
    }
    std::reverse(t.begin(), t.end());
    return token(token::kind::key, managed_bytes(t.data(), t.size()));
}

// Convert a 16 bytes long raw byte array to token. Byte 0 is the most significant byte.
static token bytes_to_token(bytes digest) {
    if (digest.size() != 16) {
        throw std::runtime_error(sprint("RandomPartitioner digest should be 16 bytes, it is %d", digest.size()));
    }
    // Translates the bytes array to signed integer i,
    // abs(i) is stored in token's _data array.
    if (digest[0] & 0x80) {
        boost::multiprecision::uint128_t i = 0;
        for (uint8_t d : digest) {
            i = (i << 8) + d;
        }
        // i = abs(i) = ~i + 1
        i = ~i + 1;
        return cppint_to_token(i);
    } else {
        return token(token::kind::key, std::move(digest));
    }
}

static float ratio_helper(boost::multiprecision::uint128_t a, boost::multiprecision::uint128_t b) {
    boost::multiprecision::uint128_t val;
    if (a >= b) {
        val = a - b;
    } else {
        val = cppint127_max - (b - a);
    }
    return static_cast<float>(val.convert_to<double>() * 0x1p-127);
}

token random_partitioner::get_token(bytes data) {
    md5_hasher h;
    h.update(reinterpret_cast<const char*>(data.c_str()), data.size());
    return bytes_to_token(h.finalize());
}

token random_partitioner::get_token(const schema& s, partition_key_view key) {
    auto&& legacy = key.legacy_form(s);
    return get_token(bytes(legacy.begin(), legacy.end()));
}

token random_partitioner::get_token(const sstables::key_view& key) {
    auto v = bytes_view(key);
    if (v.empty()) {
        return minimum_token();
    }
    return get_token(bytes(v.begin(), v.end()));
}

int random_partitioner::tri_compare(token_view t1, token_view t2) const {
    auto l1 = token_to_cppint(t1);
    auto l2 = token_to_cppint(t2);

    if (l1 == l2) {
        return 0;
    } else {
        return l1 < l2 ? -1 : 1;
    }
}

token random_partitioner::get_random_token() {
    boost::multiprecision::uint128_t i = dht::get_random_number<uint64_t>();
    i = (i << 64) + dht::get_random_number<uint64_t>();
    if (i > cppint127_max) {
        i = ~i + 1;
    }
    return cppint_to_token(i);
}

std::map<token, float> random_partitioner::describe_ownership(const std::vector<token>& sorted_tokens) {
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
        const token& start = sorted_tokens[0];
        auto ti = token_to_cppint(start);  // The first token and its value
        auto cppint_start = ti;
        auto tim1 = ti; // The last token and its value (after loop)
        for (i++; i != sorted_tokens.end(); i++) {
            ti = token_to_cppint(*i); // The next token and its value
            ownerships[*i]= ratio_helper(ti, tim1);  // save (T(i) -> %age)
            tim1 = ti;
        }

        // The start token's range extends backward to the last token, which is why both were saved above.
        ownerships[start] = ratio_helper(cppint_start, ti);
    }

    return ownerships;
}

token random_partitioner::midpoint(const token& t1, const token& t2) const {
    unsigned sigbytes = std::max(t1._data.size(), t2._data.size());
    if (sigbytes == 0) {
        // The midpoint of two minimum token is minimum token
        return minimum_token();
    }
    static boost::multiprecision::uint128_t max = cppint_one << 127;
    auto l1 = token_to_cppint(t1);
    auto l2 = token_to_cppint(t2);
    auto sum = l1 + l2;
    boost::multiprecision::uint128_t mid;
    // t1 <= t2 is the same as l1 <= l2
    if (l1 <= l2) {
        mid = sum / 2;
    } else {
        mid = (sum / 2 + max / 2) % max;
    }
    return cppint_to_token(mid);
}

sstring random_partitioner::to_sstring(const dht::token& t) const {
    if (t._kind == dht::token::kind::before_all_keys) {
        return sstring();
    } else {
        return token_to_cppint(t).str();
    }
}

dht::token random_partitioner::from_sstring(const sstring& t) const {
    if (t.empty()) {
        return minimum_token();
    } else {
        boost::multiprecision::uint128_t x(t.c_str());
        return cppint_to_token(x);
    }
}

dht::token random_partitioner::from_bytes(bytes_view bytes) const {
    if (bytes.empty()) {
        return minimum_token();
    } else {
        return dht::token(dht::token::kind::key, bytes);
    }
}

unsigned random_partitioner::shard_of(const token& t) const {
    switch (t._kind) {
        case token::kind::before_all_keys:
            return 0;
        case token::kind::after_all_keys:
            return _shard_count - 1;
        case token::kind::key:
            auto i = (boost::multiprecision::uint256_t(token_to_cppint(t)) * _shard_count) >> 127;
            // token can be [0, 2^127], make sure smp be [0, _shard_count)
            auto smp = i.convert_to<unsigned>();
            if (smp >= _shard_count) {
                return _shard_count - 1;
            }
            return smp;
    }
    abort();
}

token
random_partitioner::token_for_next_shard(const token& t, shard_id shard, unsigned spans) const {
    if (_shard_count == 1) {
        return maximum_token();
    }
    switch (t._kind) {
        case token::kind::after_all_keys:
            return maximum_token();
        case token::kind::before_all_keys:
        case token::kind::key:
            auto orig = shard_of(t);
            if (shard <= orig || spans != 1) {
                return maximum_token();
            }
            auto t = div_ceil(boost::multiprecision::uint256_t(shard) << 127, _shard_count);
            return cppint_to_token(t.convert_to<boost::multiprecision::uint128_t>());
    }
    assert(0);
    throw std::invalid_argument("invalid token");
}


bytes random_partitioner::token_to_bytes(const token& t) const {
    static const bytes zero_byte(1, int8_t(0x00));
    if (t.is_minimum() || t._data.empty()) {
        return zero_byte;
    }
    auto data = bytes(t._data.begin(), t._data.end());
    if (t._data[0] & 0x80) {
        // Prepend 0x00 to the byte array to mimic BigInteger.toByteArray's
        // byte array representation which has a sign bit.
        return zero_byte + data;
    }
    return data;
}

using registry = class_registrator<i_partitioner, random_partitioner, const unsigned&, const unsigned&>;
static registry registrator("org.apache.cassandra.dht.RandomPartitioner");
static registry registrator_short_name("RandomPartitioner");

}
