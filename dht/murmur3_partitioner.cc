/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#include "murmur3_partitioner.hh"
#include "utils/murmur_hash.hh"
#include "sstables/key.hh"
#include "utils/class_registrator.hh"

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

int murmur3_partitioner::tri_compare(const token& t1, const token& t2) {
    auto l1 = long_token(t1);
    auto l2 = long_token(t2);

    if (l1 == l2) {
        return 0;
    } else {
        return l1 < l2 ? -1 : 1;
    }
}

token murmur3_partitioner::midpoint(const token& t1, const token& t2) const {
    auto l1 = long_token(t1);
    auto l2 = long_token(t2);
    // long_token is defined as signed, but the arithmetic works out the same
    // without invoking undefined behavior with a signed type.
    auto delta = (uint64_t(l2) - uint64_t(l1)) / 2;
    if (l1 > l2) {
        // wraparound
        delta += 0x8000'0000'0000'0000;
    }
    auto mid = uint64_t(l1) + delta;
    return get_token(mid);
}

std::map<token, float>
murmur3_partitioner::describe_ownership(const std::vector<token>& sorted_tokens) {
    abort();
}

data_type
murmur3_partitioner::get_token_validator() {
    return long_type;
}

unsigned
murmur3_partitioner::shard_of(const token& t) const {
    int64_t l = long_token(t);
    // treat l as a fraction between 0 and 1 and use 128-bit arithmetic to
    // divide that range evenly among shards:
    uint64_t adjusted = uint64_t(l) + uint64_t(std::numeric_limits<int64_t>::min());
    return (__int128(adjusted) * smp::count) >> 64;
}

using registry = class_registrator<i_partitioner, murmur3_partitioner>;
static registry registrator("org.apache.cassandra.dht.Murmur3Partitioner");
static registry registrator_short_name("Murmur3Partitioner");

}


