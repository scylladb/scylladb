/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <algorithm>
#include <limits>
#include <ostream>
#include <boost/lexical_cast.hpp>

#include "dht/token.hh"
#include "dht/token-sharding.hh"

namespace dht {

using uint128_t = unsigned __int128;

inline int64_t long_token(const token& t) {
    return t._data;
}

std::ostream& operator<<(std::ostream& out, const token& t) {
    fmt::print(out, "{}", t);
    return out;
}

sstring token::to_sstring() const {
    return seastar::to_sstring<sstring>(long_token(*this));
}

token token::midpoint(const token& t1, const token& t2) {
    uint64_t l1 = long_token(t1);
    uint64_t l2 = long_token(t2);
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
token::get_token_validator() {
    return long_type;
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
shard_of(unsigned shard_count, unsigned sharding_ignore_msb_bits, const token& t) {
    switch (t._kind) {
        case token::kind::before_all_keys:
            return token::shard_of_minimum_token();
        case token::kind::after_all_keys:
            return shard_count - 1;
        case token::kind::key:
            uint64_t adjusted = unbias(t);
            return zero_based_shard_of(adjusted, shard_count, sharding_ignore_msb_bits);
    }
    abort();
}

token
token_for_next_shard(const std::vector<uint64_t>& shard_start, unsigned shard_count, unsigned sharding_ignore_msb_bits, const token& t, shard_id shard, unsigned spans) {
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
    auto s = zero_based_shard_of(n, shard_count, sharding_ignore_msb_bits);

    if (!sharding_ignore_msb_bits) {
        // This ought to be the same as the else branch, but avoids shifts by 64
        n = shard_start[shard];
        if (spans > 1 || shard <= s) {
            return maximum_token();
        }
    } else {
        auto left_part = n >> (64 - sharding_ignore_msb_bits);
        left_part += spans - unsigned(shard > s);
        if (left_part >= (1u << sharding_ignore_msb_bits)) {
            return maximum_token();
        }
        left_part <<= (64 - sharding_ignore_msb_bits);
        auto right_part = shard_start[shard];
        n = left_part | right_part;
    }
    return bias(n);
}

int64_t token::to_int64(token t) {
    return long_token(t);
}

dht::token token::from_int64(int64_t i) {
    return {kind::key, i};
}

static
dht::token find_first_token_for_shard_in_not_wrap_around_range(const dht::static_sharder& sharder, dht::token start, dht::token end, size_t shard_idx) {
    // Invariant start < end
    // It is guaranteed that start is not MAX_INT64 because end is greater
    auto t = dht::token::from_int64(dht::token::to_int64(start) + 1);
    if (sharder.shard_of(t) != shard_idx) {
        t = sharder.token_for_next_shard(t, shard_idx);
    }
    return std::min(t, end);
}

dht::token find_first_token_for_shard(
        const dht::static_sharder& sharder, dht::token start, dht::token end, size_t shard_idx) {
    if (start < end) { // Not a wrap around token range
        return find_first_token_for_shard_in_not_wrap_around_range(sharder, start, end, shard_idx);
    } else { // A wrap around token range
        dht::token t;
        if (dht::token::to_int64(start) != std::numeric_limits<int64_t>::max()) {
            t = find_first_token_for_shard_in_not_wrap_around_range(sharder, start, dht::maximum_token(), shard_idx);
            if (!t.is_maximum()) {
                // This means we have found a token for shard shard_idx before 2^63
                return t;
            }
        }
        // No token owned by shard shard_idx was found in (start, 2^63 - 1]
        // so we have to search in (-2^63, end]
        return find_first_token_for_shard_in_not_wrap_around_range(
                sharder, dht::minimum_token(), end, shard_idx);
    }
}

size_t
compaction_group_of(unsigned most_significant_bits, const token& t) {
    if (!most_significant_bits) {
        return 0;
    }
    switch (t._kind) {
        case token::kind::before_all_keys:
            return 0;
        case token::kind::after_all_keys:
            return (1 << most_significant_bits) - 1;
        case token::kind::key:
            uint64_t adjusted = unbias(t);
            return adjusted >> (64 - most_significant_bits);
    }
    __builtin_unreachable();
}

token last_token_of_compaction_group(unsigned most_significant_bits, size_t group) {
    uint64_t n;
    if (group == ((1ul << most_significant_bits) - 1)) {
        n = std::numeric_limits<uint64_t>::max();
    } else {
        n = ((uint64_t(group) + 1) << (64 - most_significant_bits)) - 1;
    }
    return bias(n);
}

} // namespace dht
