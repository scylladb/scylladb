/*
 * Copyright (C) 2017 ScyllaDB
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

#pragma once

#include "sstables/sstables.hh"
#include "dht/i_partitioner.hh"
#include <boost/range/irange.hpp>
#include <boost/range/adaptor/map.hpp>

sstables::shared_sstable make_sstable_containing(std::function<sstables::shared_sstable()> sst_factory, std::vector<mutation> muts);

//
// Make set of keys sorted by token for current shard.
//
static std::vector<sstring> make_local_keys(unsigned n, const schema_ptr& s, size_t min_key_size = 1) {
    std::vector<std::pair<sstring, dht::decorated_key>> p;
    p.reserve(n);

    auto key_id = 0U;
    auto generated = 0U;
    while (generated < n) {
        auto raw_key = sstring(std::max(min_key_size, sizeof(key_id)), int8_t(0));
        std::copy_n(reinterpret_cast<int8_t*>(&key_id), sizeof(key_id), raw_key.begin());
        auto dk = dht::global_partitioner().decorate_key(*s, partition_key::from_single_value(*s, to_bytes(raw_key)));
        key_id++;

        if (engine_is_ready() && engine().cpu_id() != dht::global_partitioner().shard_of(dk.token())) {
            continue;
        }
        generated++;
        p.emplace_back(std::move(raw_key), std::move(dk));
    }
    boost::sort(p, [&] (auto& p1, auto& p2) {
        return p1.second.less_compare(*s, p2.second);
    });
    return boost::copy_range<std::vector<sstring>>(p | boost::adaptors::map_keys);
}

//
// Return one key for current shard. Note that it always returns the same key for a given shard.
//
inline sstring make_local_key(const schema_ptr& s, size_t min_key_size = 1) {
    return make_local_keys(1, s, min_key_size).front();
}
