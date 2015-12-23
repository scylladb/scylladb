/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
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

namespace dht {

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

inline int64_t long_token(const token& t) {
    if (t.is_minimum()) {
        return std::numeric_limits<long>::min();
    }

    if (t._data.size() != sizeof(int64_t)) {
        throw runtime_exception(sprint("Invalid token. Should have size %ld, has size %ld\n", sizeof(int64_t), t._data.size()));
    }

    auto ptr = t._data.begin();
    auto lp = unaligned_cast<const int64_t *>(ptr);
    return net::ntoh(*lp);
}

// XXX: Technically, this should be inside long token. However, long_token is
// used quite a lot in hot paths, so it is better to keep the branches of, if
// we can. Most our comparators will check for _kind separately,
// so this should be fine.
sstring murmur3_partitioner::to_sstring(const token& t) const {
    int64_t lt;
    if (t._kind == dht::token::kind::before_all_keys) {
        lt = std::numeric_limits<long>::min();
    } else {
        lt = long_token(t);
    }
    return ::to_sstring(lt);
}

dht::token murmur3_partitioner::from_sstring(const sstring& t) const {
    auto lp = boost::lexical_cast<long>(t);
    if (lp == std::numeric_limits<long>::min()) {
        return minimum_token();
    } else {
        return get_token(uint64_t(lp));
    }
}

int murmur3_partitioner::tri_compare(const token& t1, const token& t2) {
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
            return smp::count - 1;
        case token::kind::key:
            int64_t l = long_token(t);
            // treat l as a fraction between 0 and 1 and use 128-bit arithmetic to
            // divide that range evenly among shards:
            uint64_t adjusted = uint64_t(l) + uint64_t(std::numeric_limits<int64_t>::min());
            return (__int128(adjusted) * smp::count) >> 64;
    }
    assert(0);
}

using registry = class_registrator<i_partitioner, murmur3_partitioner>;
static registry registrator("org.apache.cassandra.dht.Murmur3Partitioner");
static registry registrator_short_name("Murmur3Partitioner");

}


