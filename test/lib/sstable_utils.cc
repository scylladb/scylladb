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

#include "test/lib/sstable_utils.hh"

#include "database.hh"
#include "memtable-sstable.hh"
#include "dht/i_partitioner.hh"
#include "dht/murmur3_partitioner.hh"
#include <boost/range/irange.hpp>
#include <boost/range/adaptor/map.hpp>
#include "test/lib/flat_mutation_reader_assertions.hh"

using namespace sstables;
using namespace std::chrono_literals;

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
    write_memtable_to_sstable_for_test(*mt, sst).get();
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
    auto rd = assert_that(sst->as_mutation_source().make_reader(s));
    for (auto&& m : merged) {
        rd.produces(m);
    }
    rd.produces_end_of_stream();

    return sst;
}

shared_sstable make_sstable(sstables::test_env& env, schema_ptr s, sstring dir, std::vector<mutation> mutations,
        sstable_writer_config cfg, sstables::sstable::version_types version, gc_clock::time_point query_time) {
    auto sst = env.make_sstable(s,
        dir,
        1 /* generation */,
        version,
        sstables::sstable::format_types::big,
        default_sstable_buffer_size,
        query_time);

    auto mt = make_lw_shared<memtable>(s);

    for (auto&& m : mutations) {
        mt->apply(m);
    }

    sst->write_components(mt->make_flat_reader(s), mutations.size(), s, cfg, mt->get_encoding_stats()).get();
    sst->load().get();

    return sst;
}

std::vector<std::pair<sstring, dht::token>>
token_generation_for_shard(unsigned tokens_to_generate, unsigned shard,
        unsigned ignore_msb, unsigned smp_count) {
    unsigned tokens = 0;
    unsigned key_id = 0;
    std::vector<std::pair<sstring, dht::token>> key_and_token_pair;

    key_and_token_pair.reserve(tokens_to_generate);
    dht::murmur3_partitioner partitioner;
    dht::sharder sharder(smp_count, ignore_msb);

    while (tokens < tokens_to_generate) {
        sstring key = to_sstring(key_id++);
        dht::token token = create_token_from_key(partitioner, key);
        if (shard != sharder.shard_of(token)) {
            continue;
        }
        tokens++;
        key_and_token_pair.emplace_back(key, token);
    }
    assert(key_and_token_pair.size() == tokens_to_generate);

    std::sort(key_and_token_pair.begin(),key_and_token_pair.end(), [] (auto& i, auto& j) {
        return i.second < j.second;
    });

    return key_and_token_pair;
}

std::vector<std::pair<sstring, dht::token>> token_generation_for_current_shard(unsigned tokens_to_generate) {
    return token_generation_for_shard(tokens_to_generate, this_shard_id());
}
