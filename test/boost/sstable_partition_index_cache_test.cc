/*
 * Copyright (C) 2021-present ScyllaDB
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

#include <boost/test/unit_test.hpp>

#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>

#include "sstables/partition_index_cache.hh"
#include "test/lib/simple_schema.hh"

using namespace sstables;

static void add_entry(logalloc::region& r,
      const schema& s,
      partition_index_page& page,
      const partition_key& key,
      uint64_t position)
{
    logalloc::allocating_section as;
    as(r, [&] {
        sstables::key sst_key = sstables::key::from_partition_key(s, key);
        page._entries.push_back(make_managed<index_entry>(
                managed_bytes(sst_key.get_bytes()),
                position,
                managed_ref<promoted_index>()));
    });
}

static partition_index_page make_page0(logalloc::region& r, simple_schema& s) {
    partition_index_page page;
    add_entry(r, *s.schema(), page, s.make_pkey(0).key(), 0);
    add_entry(r, *s.schema(), page, s.make_pkey(1).key(), 1);
    add_entry(r, *s.schema(), page, s.make_pkey(2).key(), 2);
    add_entry(r, *s.schema(), page, s.make_pkey(3).key(), 3);
    return page;
}

static void has_page0(partition_index_cache::list_ptr ptr) {
    BOOST_REQUIRE(!ptr->empty());
    BOOST_REQUIRE_EQUAL(ptr->_entries.size(), 4);
    BOOST_REQUIRE_EQUAL(ptr->_entries[0]->position(), 0);
    BOOST_REQUIRE_EQUAL(ptr->_entries[1]->position(), 1);
    BOOST_REQUIRE_EQUAL(ptr->_entries[2]->position(), 2);
    BOOST_REQUIRE_EQUAL(ptr->_entries[3]->position(), 3);
};

SEASTAR_THREAD_TEST_CASE(test_caching) {
    ::lru lru;
    simple_schema s;
    logalloc::region r;
    partition_index_cache cache(lru, r);

    auto page0_loader = [&] (partition_index_cache::key_type k) {
        return later().then([&] {
            return make_page0(r, s);
        });
    };

    auto old_stats = cache.shard_stats();

    auto f0 = cache.get_or_load(0, page0_loader);
    auto f1 = cache.get_or_load(0, page0_loader);

    BOOST_REQUIRE_EQUAL(cache.shard_stats().hits, old_stats.hits);
    BOOST_REQUIRE_EQUAL(cache.shard_stats().misses, old_stats.misses + 1);
    BOOST_REQUIRE_EQUAL(cache.shard_stats().blocks, old_stats.blocks + 2);

    r.full_compaction();
    with_allocator(r.allocator(), [&] {
        lru.evict_all();
    });

    partition_index_cache::list_ptr ptr0 = f0.get0();
    partition_index_cache::list_ptr ptr1 = f1.get0();

    r.full_compaction();
    with_allocator(r.allocator(), [&] {
        lru.evict_all();
    });

    BOOST_REQUIRE_EQUAL(cache.shard_stats().populations, old_stats.populations + 1);
    BOOST_REQUIRE_EQUAL(cache.shard_stats().evictions, old_stats.evictions);
    BOOST_REQUIRE(cache.shard_stats().used_bytes > 0);

    has_page0(ptr0);
    has_page0(ptr1);

    BOOST_REQUIRE(&*ptr0 == &*ptr1);

    {
        auto ptr2 = ptr1;
        auto ptr3 = std::move(ptr2);
        BOOST_REQUIRE(!ptr2);
        BOOST_REQUIRE(ptr3);
        ptr0 = nullptr;
        ptr1 = nullptr;
        BOOST_REQUIRE(!ptr1);

        with_allocator(r.allocator(), [&] {
            lru.evict_all();
        });
        // ptr3 prevents page 0 evictions
        BOOST_REQUIRE_EQUAL(cache.shard_stats().evictions, old_stats.evictions);

        has_page0(ptr3);

        ptr3 = nullptr;
        with_allocator(r.allocator(), [&] {
            lru.evict_all();
        });

        BOOST_REQUIRE_EQUAL(cache.shard_stats().evictions, old_stats.evictions + 1);
        BOOST_REQUIRE_EQUAL(cache.shard_stats().used_bytes, old_stats.used_bytes);
    }

    {
        auto ptr4 = cache.get_or_load(0, page0_loader).get0();
        has_page0(ptr4);

        BOOST_REQUIRE_EQUAL(cache.shard_stats().misses, old_stats.misses + 2);
        BOOST_REQUIRE_EQUAL(cache.shard_stats().populations, old_stats.populations + 2);
    }
}

SEASTAR_THREAD_TEST_CASE(test_auto_clear) {
    ::lru lru;
    simple_schema s;
    logalloc::region r;

    partition_index_cache::stats old_stats;

    {
        partition_index_cache cache(lru, r);

        auto page0_loader = [&] (partition_index_cache::key_type k) {
            return make_page0(r, s);
        };

        old_stats = cache.shard_stats();

        cache.get_or_load(0, page0_loader).get();
        cache.get_or_load(1, page0_loader).get();
        cache.get_or_load(2, page0_loader).get();
    }

    partition_index_cache cache2(lru, r); // to get stats
    BOOST_REQUIRE_EQUAL(cache2.shard_stats().evictions, old_stats.evictions + 3);
    BOOST_REQUIRE_EQUAL(cache2.shard_stats().used_bytes, old_stats.used_bytes);
    BOOST_REQUIRE_EQUAL(cache2.shard_stats().populations, old_stats.populations + 3);
}

SEASTAR_THREAD_TEST_CASE(test_destroy) {
    ::lru lru;
    simple_schema s;
    logalloc::region r;

    partition_index_cache::stats old_stats;

    partition_index_cache cache(lru, r);

    auto page0_loader = [&] (partition_index_cache::key_type k) {
        return make_page0(r, s);
    };

    old_stats = cache.shard_stats();

    cache.get_or_load(0, page0_loader).get();
    cache.get_or_load(1, page0_loader).get();
    cache.get_or_load(2, page0_loader).get();

    cache.evict_gently().get();

    BOOST_REQUIRE_EQUAL(cache.shard_stats().evictions, old_stats.evictions + 3);
    BOOST_REQUIRE_EQUAL(cache.shard_stats().used_bytes, old_stats.used_bytes);
    BOOST_REQUIRE_EQUAL(cache.shard_stats().populations, old_stats.populations + 3);
}

SEASTAR_THREAD_TEST_CASE(test_evict_gently) {
    ::lru lru;
    simple_schema s;
    logalloc::region r;

    partition_index_cache::stats old_stats;

    partition_index_cache cache(lru, r);

    auto page0_loader = [&] (partition_index_cache::key_type k) {
        return make_page0(r, s);
    };

    old_stats = cache.shard_stats();

    cache.get_or_load(0, page0_loader).get();
    cache.get_or_load(1, page0_loader).get();
    cache.get_or_load(2, page0_loader).get();

    cache.evict_gently().get();

    BOOST_REQUIRE_EQUAL(cache.shard_stats().evictions, old_stats.evictions + 3);
    BOOST_REQUIRE_EQUAL(cache.shard_stats().used_bytes, old_stats.used_bytes);
    BOOST_REQUIRE_EQUAL(cache.shard_stats().populations, old_stats.populations + 3);

    // kept alive around evict_gently()
    auto page = cache.get_or_load(1, page0_loader).get();
    BOOST_REQUIRE_EQUAL(cache.shard_stats().populations, old_stats.populations + 4);

    cache.evict_gently().get();

    BOOST_REQUIRE_EQUAL(cache.shard_stats().evictions, old_stats.evictions + 3);

    auto no_loader = [&] (partition_index_cache::key_type k) -> future<partition_index_page> {
        throw std::runtime_error("should not have been invoked");
    };
    cache.get_or_load(1, no_loader).get(); // page keeps the page alive

    cache.evict_gently().get();
}
