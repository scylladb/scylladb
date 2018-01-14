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

#include "sstable_utils.hh"

#include <tests/test-utils.hh>

#include "database.hh"
#include "memtable-sstable.hh"
#include "mutation_reader_assertions.hh"
#include "dht/i_partitioner.hh"
#include <boost/range/irange.hpp>
#include <boost/range/adaptor/map.hpp>

sstables::shared_sstable make_sstable_containing(std::function<sstables::shared_sstable()> sst_factory, std::vector<mutation> muts) {
    auto sst = sst_factory();
    schema_ptr s = muts[0].schema();
    auto mt = make_lw_shared<memtable>(s);

    std::size_t i{0};
    for (auto&& m : muts) {
        mt->apply(m);
        ++i;

        // Give the reactor some time to breathe
        if(i == 10) {
            seastar::thread::yield();
            i = 0;
        }
    }
    write_memtable_to_sstable(*mt, sst).get();
    sst->open_data().get();

    std::set<mutation, mutation_decorated_key_less_comparator> merged;
    for (auto&& m : muts) {
        auto result = merged.insert(m);
        if (!result.second) {
            auto old = *result.first;
            merged.erase(result.first);
            merged.insert(old + m);
        }
    }

    // validate the sstable
    auto rd = assert_that(sst->as_mutation_source()(s));
    for (auto&& m : merged) {
        rd.produces(m);
    }
    rd.produces_end_of_stream();

    return sst;
}

std::vector<sstring> make_local_keys(unsigned n, const schema_ptr& s) {
    std::vector<std::pair<sstring, dht::decorated_key>> p;
    p.reserve(n);

    auto key_id = 0U;
    auto generated = 0U;
    while (generated < n) {
        auto raw_key = to_sstring(key_id++);
        auto dk = dht::global_partitioner().decorate_key(*s, partition_key::from_single_value(*s, to_bytes(raw_key)));

        if (engine().cpu_id() != dht::global_partitioner().shard_of(dk.token())) {
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

sstring make_local_key(const schema_ptr& s) {
    return make_local_keys(1, s).front();
}
