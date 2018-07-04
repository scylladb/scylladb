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

#include "murmur3_partitioner.hh"
#include "utils/murmur_hash.hh"
#include "sstables/key.hh"
#include "utils/class_registrator.hh"
#include <boost/lexical_cast.hpp>
#include <boost/range/irange.hpp>

namespace dht {

inline
unsigned
murmur3_partitioner::zero_based_shard_of(uint64_t token, unsigned shards, unsigned sharding_ignore_msb_bits) {
    // This is the master function, the inverses have to match it wrt. rounding errors.
    token <<= sharding_ignore_msb_bits;
    // Treat "token" as a fraction in the interval [0, 1); compute:
    //    shard = floor((0.token) * shards)
    return (uint128_t(token) * shards) >> 64;
}

std::vector<uint64_t>
murmur3_partitioner::init_zero_based_shard_start(unsigned shards, unsigned sharding_ignore_msb_bits) {
    // computes the inverse of zero_based_shard_of(). ret[s] will return the smallest token that belongs to s
    if (shards == 1) {
        // Avoid the while loops below getting confused finding the "edge" between two nonexistent shards
        return std::vector<uint64_t>(1, uint64_t(0));
    }
    auto ret = std::vector<uint64_t>(shards);
    for (auto s : boost::irange<unsigned>(0, shards)) {
        uint64_t token = (uint128_t(s) << 64) / shards;
        token >>= sharding_ignore_msb_bits;   // leftmost bits are ignored by zero_based_shard_of
        // token is the start of the next shard, and can be slightly before due to rounding errors; adjust
        while (zero_based_shard_of(token, shards, sharding_ignore_msb_bits) != s) {
            ++token;
        }
        ret[s] = token;
    }
    return ret;
}

inline
int64_t
murmur3_partitioner::normalize(int64_t in) {
    return in == std::numeric_limits<int64_t>::lowest()
            ? std::numeric_limits<int64_t>::max()
            : in;
}

token
murmur3_partitioner::get_token(bytes_view key) {
    if (key.empty()) {
        return minimum_token();
    }
    std::array<uint64_t, 2> hash;
    utils::murmur_hash::hash3_x64_128(key, 0, hash);
    return get_token(hash[0]);
}

token
murmur3_partitioner::get_token(uint64_t value) const {
    // We don't normalize() the value, since token includes an is-before-everything
    // indicator.
    // FIXME: will this require a repair when importing a database?
    auto t = net::hton(normalize(value));
    bytes b(bytes::initialized_later(), 8);
    std::copy_n(reinterpret_cast<int8_t*>(&t), 8, b.begin());
    return token{token::kind::key, std::move(b)};
}

token
murmur3_partitioner::get_token(const sstables::key_view& key) {
    return get_token(bytes_view(key));
}

token
murmur3_partitioner::get_token(const schema& s, partition_key_view key) {
    std::array<uint64_t, 2> hash;
    auto&& legacy = key.legacy_form(s);
    utils::murmur_hash::hash3_x64_128(legacy.begin(), legacy.size(), 0, hash);
    return get_token(hash[0]);
}

token murmur3_partitioner::get_random_token() {
    auto rand = dht::get_random_number<uint64_t>();
    return get_token(rand);
}

inline int64_t long_token(token_view t) {
    if (t.is_minimum() || t.is_maximum()) {
        return std::numeric_limits<long>::min();
    }

    if (t._data.size() != sizeof(int64_t)) {
        throw runtime_exception(sprint("Invalid token. Should have size %ld, has size %ld\n", sizeof(int64_t), t._data.size()));
    }

    auto ptr = t._data.begin();
    auto lp = unaligned_cast<const int64_t *>(ptr);
    return net::ntoh(*lp);
}

uint64_t
murmur3_partitioner::unbias(const token& t) const {
    return uint64_t(long_token(t)) + uint64_t(std::numeric_limits<int64_t>::min());
}

token
murmur3_partitioner::bias(uint64_t n) const {
    return get_token(n - uint64_t(std::numeric_limits<int64_t>::min()));
}

sstring murmur3_partitioner::to_sstring(const token& t) const {
    return seastar::to_sstring<sstring>(long_token(t));
}

dht::token murmur3_partitioner::from_sstring(const sstring& t) const {
    auto lp = boost::lexical_cast<long>(t);
    if (lp == std::numeric_limits<long>::min()) {
        return minimum_token();
    } else {
        return get_token(uint64_t(lp));
    }
}

dht::token murmur3_partitioner::from_bytes(bytes_view bytes) const {
    if (bytes.size() != sizeof(int64_t)) {
        throw runtime_exception(sprint("Invalid token. Should have size %ld, has size %ld\n", sizeof(int64_t), bytes.size()));
    }

    int64_t v;
    std::copy_n(bytes.begin(), sizeof(v), reinterpret_cast<int8_t *>(&v));
    auto tok = net::ntoh(v);
    if (tok == std::numeric_limits<int64_t>::min()) {
        return minimum_token();
    } else {
        return dht::token(dht::token::kind::key, bytes);
    }
}

int murmur3_partitioner::tri_compare(token_view t1, token_view t2) const {
    auto l1 = long_token(t1);
    auto l2 = long_token(t2);

    if (l1 == l2) {
        return 0;
    } else {
        return l1 < l2 ? -1 : 1;
    }
}

// Assuming that x>=y, return the positive difference x-y.
// The return type is an unsigned type, as the difference may overflow
// a signed type (e.g., consider very positive x and very negative y).
template <typename T>
static std::make_unsigned_t<T> positive_subtract(T x, T y) {
        return std::make_unsigned_t<T>(x) - std::make_unsigned_t<T>(y);
}

token murmur3_partitioner::midpoint(const token& t1, const token& t2) const {
    auto l1 = long_token(t1);
    auto l2 = long_token(t2);
    int64_t mid;
    if (l1 <= l2) {
        // To find the midpoint, we cannot use the trivial formula (l1+l2)/2
        // because the addition can overflow the integer. To avoid this
        // overflow, we first notice that the above formula is equivalent to
        // l1 + (l2-l1)/2. Now, "l2-l1" can still overflow a signed integer
        // (e.g., think of a very positive l2 and very negative l1), but
        // because l1 <= l2 in this branch, we note that l2-l1 is positive
        // and fits an *unsigned* int's range. So,
        mid = l1 + positive_subtract(l2, l1)/2;
    } else {
        // When l2 < l1, we need to switch l1 and and l2 in the above
        // formula, because now l1 - l2 is positive.
        // Additionally, we consider this case is a "wrap around", so we need
        // to behave as if l2 + 2^64 was meant instead of l2, i.e., add 2^63
        // to the average.
        mid = l2 + positive_subtract(l1, l2)/2 + 0x8000'0000'0000'0000;
    }
    return get_token(mid);
}

static float ratio_helper(int64_t a, int64_t b) {

    uint64_t val = (a > b)? static_cast<uint64_t>(a) - static_cast<uint64_t>(b) : (static_cast<uint64_t>(a) - static_cast<uint64_t>(b) - 1);
    return val/(float)std::numeric_limits<uint64_t>::max();
}

std::map<token, float>
murmur3_partitioner::describe_ownership(const std::vector<token>& sorted_tokens) {
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

        int64_t ti = long_token(start);  // The first token and its value
        int64_t start_long = ti;
        int64_t tim1 = ti; // The last token and its value (after loop)
        for (i++; i != sorted_tokens.end(); i++) {
            ti = long_token(*i); // The next token and its value
            ownerships[*i]= ratio_helper(ti, tim1);  // save (T(i) -> %age)
            tim1 = ti;
        }

        // The start token's range extends backward to the last token, which is why both were saved above.
        ownerships[start] = ratio_helper(start_long, ti);
    }

    return ownerships;
}

data_type
murmur3_partitioner::get_token_validator() {
    return long_type;
}

unsigned
murmur3_partitioner::shard_of(const token& t) const {
    switch (t._kind) {
        case token::kind::before_all_keys:
            return 0;
        case token::kind::after_all_keys:
            return _shard_count - 1;
        case token::kind::key:
            uint64_t adjusted = unbias(t);
            return zero_based_shard_of(adjusted, _shard_count, _sharding_ignore_msb_bits);
    }
    abort();
}

token
murmur3_partitioner::token_for_next_shard(const token& t, shard_id shard, unsigned spans) const {
    uint64_t n = 0;
    switch (t._kind) {
        case token::kind::before_all_keys:
            break;
        case token::kind::after_all_keys:
            return maximum_token();
        case token::kind::key:
            n = unbias(t);
            break;
    }
    auto s = zero_based_shard_of(n, _shard_count, _sharding_ignore_msb_bits);

    if (!_sharding_ignore_msb_bits) {
        // This ought to be the same as the else branch, but avoids shifts by 64
        n = _shard_start[shard];
        if (spans > 1 || shard <= s) {
            return maximum_token();
        }
    } else {
        auto left_part = n >> (64 - _sharding_ignore_msb_bits);
        left_part += spans - unsigned(shard > s);
        if (left_part >= (1u << _sharding_ignore_msb_bits)) {
            return maximum_token();
        }
        left_part <<= (64 - _sharding_ignore_msb_bits);
        auto right_part = _shard_start[shard];
        n = left_part | right_part;
    }
    return bias(n);
}

unsigned
murmur3_partitioner::sharding_ignore_msb() const {
    return _sharding_ignore_msb_bits;
}


using registry = class_registrator<i_partitioner, murmur3_partitioner, const unsigned&, const unsigned&>;
static registry registrator("org.apache.cassandra.dht.Murmur3Partitioner");
static registry registrator_short_name("Murmur3Partitioner");

}


