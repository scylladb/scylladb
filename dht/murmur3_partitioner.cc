/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#include "murmur3_partitioner.hh"
#include "utils/murmur_hash.hh"

namespace dht {

inline
int64_t
murmur3_partitioner::normalize(int64_t in) {
    return in == std::numeric_limits<int64_t>::lowest()
            ? std::numeric_limits<int64_t>::max()
            : in;
}

token
murmur3_partitioner::get_token(const partition_key& key_) {
    bytes_view key(key_);
    if (key.empty()) {
        return minimum_token();
    }
    std::array<uint64_t, 2> hash;
    utils::murmur_hash::hash3_x64_128(key, 0, hash);
    // We don't normalize() the value, since token includes an is-before-everything
    // indicator.
    // FIXME: will this require a repair when importing a database?
    auto t = net::hton(normalize(hash[0]));
    bytes b(bytes::initialized_later(), 8);
    std::copy_n(reinterpret_cast<int8_t*>(&t), 8, b.begin());
    return token{token::kind::key, std::move(b)};
}

inline long long_token(const token& t) {

    if (t._data.size() != sizeof(long)) {
        throw runtime_exception(sprint("Invalid token. Should have size %ld, has size %ld\n", sizeof(long), t._data.size()));
    }

    auto ptr = const_cast<int8_t *>(t._data.c_str());
    auto lp = reinterpret_cast<long *>(ptr);
    return net::ntoh(*lp);
}

bool murmur3_partitioner::is_equal(const token& t1, const token& t2) {

    auto l1 = long_token(t1);
    auto l2 = long_token(t2);

    return l1 == l2;
}

bool murmur3_partitioner::is_less(const token& t1, const token& t2) {

    auto l1 = long_token(t1);
    auto l2 = long_token(t2);

    return l1 < l2;
}

std::map<token, float>
murmur3_partitioner::describe_ownership(const std::vector<token>& sorted_tokens) {
    abort();
}

data_type
murmur3_partitioner::get_token_validator() {
    abort();
}

}


