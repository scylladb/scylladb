/*
 * Copyright (C) 2020-present ScyllaDB
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

#include <algorithm>
#include <limits>
#include <ostream>
#include <utility>

#include "dht/token.hh"
#include "dht/token-sharding.hh"
#include "dht/i_partitioner.hh"

namespace dht {

using uint128_t = unsigned __int128;

std::ostream& operator<<(std::ostream& out, token t) {
    if (t.is_minimum()) {
        return out << "minimum token";
    } else {
        return out << t.to_sstring();
    }
}

sstring token::to_sstring() const {
    return seastar::to_sstring<sstring>(to_int64());
}

token token::midpoint(token t1, token t2) {
    uint64_t l1 = t1.to_int64();
    uint64_t l2 = t2.to_int64();
    int64_t mid = l1 + (l2 - l1)/2;
    return token{kind::key, mid};
}

token token::get_random_token() {
    static thread_local std::default_random_engine re{std::random_device{}()};
    // std::numeric_limits<int64_t>::min() value is reserved and shouldn't
    // be used for regular tokens.
    static thread_local std::uniform_int_distribution<int64_t> dist(
            std::numeric_limits<int64_t>::min() + 1);
    return token(kind::key, dist(re));
}

token token::from_sstring(const sstring& t) {
    auto lp = boost::lexical_cast<long>(t);
    if (lp == std::numeric_limits<long>::min()) {
        return minimum_token();
    } else {
        return token(kind::key, uint64_t(lp));
    }
}

token token::from_bytes(bytes_view bytes) {
    if (bytes.size() != sizeof(int64_t)) {
        throw runtime_exception(format("Invalid token. Should have size {:d}, has size {:d}\n", sizeof(int64_t), bytes.size()));
    }

    auto tok = net::ntoh(read_unaligned<int64_t>(bytes.begin()));
    if (tok == std::numeric_limits<int64_t>::min()) {
        return minimum_token();
    } else {
        return dht::token(dht::token::kind::key, tok);
    }
}

static float ratio_helper(int64_t a, int64_t b) {

    uint64_t val = (a > b)? static_cast<uint64_t>(a) - static_cast<uint64_t>(b) : (static_cast<uint64_t>(a) - static_cast<uint64_t>(b) - 1);
    return val/(float)std::numeric_limits<uint64_t>::max();
}

std::map<token, float>
token::describe_ownership(const std::vector<token>& sorted_tokens) {
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
        token start = sorted_tokens[0];

        int64_t ti = start.to_int64();  // The first token and its value
        int64_t start_long = ti;
        int64_t tim1 = ti; // The last token and its value (after loop)
        for (i++; i != sorted_tokens.end(); i++) {
            ti = i->to_int64(); // The next token and its value
            ownerships[*i]= ratio_helper(ti, tim1);  // save (T(i) -> %age)
            tim1 = ti;
        }

        // The start token's range extends backward to the last token, which is why both were saved above.
        ownerships[start] = ratio_helper(start_long, ti);
    }

    return ownerships;
}

data_type
token::get_token_validator() {
    return long_type;
}

static uint64_t unbias(token t) {
    return uint64_t(t.to_int64()) + uint64_t(std::numeric_limits<int64_t>::min());
}

static token bias(uint64_t n) {
    return token(token::kind::key, n - uint64_t(std::numeric_limits<int64_t>::min()));
}

inline
unsigned
zero_based_shard_of(uint64_t token, unsigned shards, unsigned sharding_ignore_msb_bits) {
    // This is the master function, the inverses have to match it wrt. rounding errors.
    token <<= sharding_ignore_msb_bits;
    // Treat "token" as a fraction in the interval [0, 1); compute:
    //    shard = floor((0.token) * shards)
    return (uint128_t(token) * shards) >> 64;
}

std::vector<uint64_t>
init_zero_based_shard_start(unsigned shards, unsigned sharding_ignore_msb_bits) {
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

unsigned
shard_of(unsigned shard_count, unsigned sharding_ignore_msb_bits, token t) {
    return zero_based_shard_of(unbias(t), shard_count, sharding_ignore_msb_bits);
}

std::optional<token>
token_for_next_shard(const std::vector<uint64_t>& shard_start, unsigned shard_count, unsigned sharding_ignore_msb_bits, token t, shard_id shard, unsigned spans) {
    uint64_t n = unbias(t);
    auto s = zero_based_shard_of(n, shard_count, sharding_ignore_msb_bits);

    if (!sharding_ignore_msb_bits) {
        // This ought to be the same as the else branch, but avoids shifts by 64
        n = shard_start[shard];
        if (spans > 1 || shard <= s) {
            return std::nullopt;
        }
    } else {
        auto left_part = n >> (64 - sharding_ignore_msb_bits);
        left_part += spans - unsigned(shard > s);
        if (left_part >= (1u << sharding_ignore_msb_bits)) {
            return std::nullopt;
        }
        left_part <<= (64 - sharding_ignore_msb_bits);
        auto right_part = shard_start[shard];
        n = left_part | right_part;
    }
    return bias(n);
}

dht::token token::from_int64(int64_t i) {
    return {kind::key, i};
}

static
std::optional<dht::token> find_first_token_for_shard_in_not_wrap_around_range(const dht::sharder& sharder, dht::token start, dht::token end, size_t shard_idx) {
    // Invariant start < end
    // It is guaranteed that start is not MAX_INT64 because end is greater
    auto t = dht::token::from_int64(start.to_int64() + 1);
    if (sharder.shard_of(t) == shard_idx) {
        return t;
    }
    if (auto x = sharder.token_for_next_shard(t, shard_idx)) {
        if (*x <= end) {
            return x;
        }
    }
    return std::nullopt;
}

std::optional<dht::token> find_first_token_for_shard(const dht::sharder& sharder, dht::token start, dht::token end, size_t shard_idx) {
    if (start < end) { // Not a wrap around token range
        return find_first_token_for_shard_in_not_wrap_around_range(sharder, start, end, shard_idx);
    } else { // A wrap around token range
        // First search in (start, 2^63 - 1]
        if (auto t = find_first_token_for_shard_in_not_wrap_around_range(sharder, start, dht::greatest_token(), shard_idx)) {
            return t;
        }
        // No token owned by shard shard_idx was found in (start, 2^63 - 1]
        // so we have to search in (-2^63, end]
        return find_first_token_for_shard_in_not_wrap_around_range(sharder, dht::minimum_token(), end, shard_idx);
    }
}

} // namespace dht
