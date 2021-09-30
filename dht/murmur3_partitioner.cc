/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "murmur3_partitioner.hh"
#include "utils/murmur_hash.hh"
#include "sstables/key.hh"
#include "utils/class_registrator.hh"
#include <boost/lexical_cast.hpp>
#include <boost/range/irange.hpp>

namespace dht {

token
murmur3_partitioner::get_token(bytes_view key) const {
    if (key.empty()) {
        return minimum_token();
    }
    std::array<uint64_t, 2> hash;
    utils::murmur_hash::hash3_x64_128(key, 0, hash);
    return get_token(hash[0]);
}

token
murmur3_partitioner::get_token(uint64_t value) const {
    return token(token::kind::key, value);
}

token
murmur3_partitioner::get_token(const sstables::key_view& key) const {
    return key.with_linearized([&] (bytes_view v) {
        return get_token(v);
    });
}

token
murmur3_partitioner::get_token(const schema& s, partition_key_view key) const {
    std::array<uint64_t, 2> hash;
    auto&& legacy = key.legacy_form(s);
    auto size = legacy.size();
    if (size == 0) {
        return minimum_token();
    }
    utils::murmur_hash::hash3_x64_128(legacy.begin(), size, 0, hash);
    return get_token(hash[0]);
}

using registry = class_registrator<i_partitioner, murmur3_partitioner>;
static registry registrator("org.apache.cassandra.dht.Murmur3Partitioner");
static registry registrator_short_name("Murmur3Partitioner");

}


