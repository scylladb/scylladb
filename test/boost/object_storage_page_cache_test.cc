/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Tests for the disk page cache of sstables on object storage.

#include <boost/test/unit_test.hpp>

#undef SEASTAR_TESTING_MAIN
#include <seastar/core/smp.hh>
#include <seastar/core/thread.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>

#include "cql3/query_processor.hh"
#include "cql3/untyped_result_set.hh"
#include "db/config.hh"
#include "db/object_storage_page_cache_table.hh"
#include "dht/fixed_shard.hh"
#include "replica/database.hh"
#include "replica/memtable.hh"
#include "test/lib/cql_test_env.hh"
#include "tombstone_gc_options.hh"
#include "utils/UUID_gen.hh"

BOOST_AUTO_TEST_SUITE(object_storage_page_cache_test)

static cql_test_config page_cache_config() {
    auto cfg = make_shared<db::config>();
    cfg->experimental_features({db::experimental_features_t::feature::OBJECT_STORAGE_PAGE_CACHE});
    return cql_test_config(std::move(cfg));
}

// The system_cache.pagecache table holds the cache. It and its keyspace
// only exist when the experimental feature is enabled.
SEASTAR_TEST_CASE(test_pagecache_table_needs_experimental_feature) {
    co_await do_with_cql_env([] (cql_test_env& env) -> future<> {
        BOOST_REQUIRE(!env.local_db().has_keyspace(db::object_storage_page_cache_table::SYSTEM_CACHE));
        co_return;
    });
    co_await do_with_cql_env([] (cql_test_env& env) -> future<> {
        BOOST_REQUIRE(env.local_db().has_schema(db::object_storage_page_cache_table::SYSTEM_CACHE, db::object_storage_page_cache_table::PAGECACHE));
        co_return;
    }, page_cache_config());
}

// The properties of system_cache.pagecache which make it suitable for a cache.
SEASTAR_TEST_CASE(test_pagecache_table_properties) {
    return do_with_cql_env([] (cql_test_env& env) -> future<> {
        auto& table = env.local_db().find_column_family(db::object_storage_page_cache_table::SYSTEM_CACHE, db::object_storage_page_cache_table::PAGECACHE);
        auto s = table.schema();
        // Check that each partition is stored on the shard named in its key.
        BOOST_REQUIRE_EQUAL(s->get_partitioner().name(), dht::fixed_shard_partitioner::classname);
        // Check that the pages are not also cached in the row cache.
        BOOST_REQUIRE(!s->caching_options().enabled());
        // Check that writes skip the commitlog.
        BOOST_REQUIRE(!env.local_db().find_keyspace(db::object_storage_page_cache_table::SYSTEM_CACHE).metadata()->durable_writes());
        BOOST_REQUIRE(!table.durable_writes());
        // Check that tombstones, e.g., of evicted pages, can be purged at once.
        BOOST_REQUIRE(s->tombstone_gc_options().mode() == tombstone_gc_mode::immediate);
        co_return;
    }, page_cache_config());
}

// A row of system_cache.pagecache is stored on the shard named by its
// "shard" column, whichever shard writes it.
SEASTAR_TEST_CASE(test_pagecache_row_stored_on_its_shard) {
    return do_with_cql_env([] (cql_test_env& env) -> future<> {
        auto sstable = utils::UUID_gen::get_time_UUID();
        auto& qp = env.local_qp();
        for (unsigned shard = 0; shard < this_smp_shard_count(); shard++) {
            co_await qp.execute_internal(
                    "INSERT INTO system_cache.pagecache (shard, sstable, component, offset, page) VALUES (?, ?, ?, ?, ?)",
                    {int16_t(shard), sstable, sstring("Data"), int64_t(0), data_value(bytes(1, int8_t(shard)))},
                    cql3::query_processor::cache_internal::no);
        }

        // Check that each shard's memtable holds exactly one partition: the
        // one naming that shard.
        co_await env.db().invoke_on_all([] (replica::database& db) {
            auto& table = db.find_column_family(db::object_storage_page_cache_table::SYSTEM_CACHE, db::object_storage_page_cache_table::PAGECACHE);
            size_t partitions = 0;
            table.for_each_active_memtable([&] (replica::memtable& m) {
                partitions += m.partition_count();
            });
            BOOST_REQUIRE_EQUAL(partitions, 1);
        });

        // Check that each shard's row is read back.
        for (unsigned shard = 0; shard < this_smp_shard_count(); shard++) {
            auto rs = co_await qp.execute_internal(
                    "SELECT page FROM system_cache.pagecache WHERE shard = ? AND sstable = ?",
                    {int16_t(shard), sstable},
                    cql3::query_processor::cache_internal::no);
            BOOST_REQUIRE_EQUAL(rs->size(), 1);
            BOOST_REQUIRE_EQUAL(rs->one().get_blob_unfragmented("page"), bytes(1, int8_t(shard)));
        }
    }, page_cache_config());
}

using page = sstables::object_storage_page_cache::page;
using enum sstables::component_type;

static temporary_buffer<char> make_page(char fill, size_t size = 4096) {
    temporary_buffer<char> buf(size);
    std::fill_n(buf.get_write(), size, fill);
    return buf;
}

struct expected_page {
    uint64_t offset;
    char fill;
    size_t size = 4096;
};

static void check_pages(const std::vector<page>& pages, std::vector<expected_page> expected) {
    BOOST_REQUIRE_EQUAL(pages.size(), expected.size());
    for (size_t i = 0; i < pages.size(); i++) {
        BOOST_REQUIRE_EQUAL(pages[i].offset, expected[i].offset);
        BOOST_REQUIRE_EQUAL(pages[i].data.size(), expected[i].size);
        BOOST_REQUIRE(std::ranges::all_of(pages[i].data, [&] (char c) { return c == expected[i].fill; }));
    }
}

static sstables::sstable_id new_sstable_id() {
    return sstables::sstable_id(utils::UUID_gen::get_time_UUID());
}

// get_pages() returns the cached pages which start in the given range, in
// offset order.
SEASTAR_TEST_CASE(test_page_cache_get_pages_in_range) {
    return do_with_cql_env_thread([] (cql_test_env& env) {
        db::object_storage_page_cache_table cache(env.local_qp());
        auto sst = new_sstable_id();
        // The pages are put out of order, so get_pages() has to sort them.
        cache.put_page(sst, Data, 8192, make_page('c')).get();
        cache.put_page(sst, Data, 0, make_page('a')).get();
        cache.put_page(sst, Data, 4096, make_page('b')).get();

        // Check that a range covering all the pages returns all of them.
        check_pages(cache.get_pages(sst, Data, 0, 12288, 100).get(), {{0, 'a'}, {4096, 'b'}, {8192, 'c'}});
        // Check that the range includes a page starting at "from", and
        // excludes a page starting at "to".
        check_pages(cache.get_pages(sst, Data, 4096, 8192, 100).get(), {{4096, 'b'}});
        // Check that a page starting before "from" is not returned, even if
        // it extends into the range.
        check_pages(cache.get_pages(sst, Data, 1, 4096, 100).get(), {});
        // Check that a range without any cached page returns nothing.
        check_pages(cache.get_pages(sst, Data, 12288, 20000, 100).get(), {});
    }, page_cache_config());
}

// get_pages() returns at most max_pages pages, and the caller can continue
// reading the range after the last page returned.
SEASTAR_TEST_CASE(test_page_cache_get_pages_max_pages) {
    return do_with_cql_env_thread([] (cql_test_env& env) {
        db::object_storage_page_cache_table cache(env.local_qp());
        auto sst = new_sstable_id();
        for (int i = 0; i < 5; i++) {
            cache.put_page(sst, Data, i * 4096, make_page('a' + i)).get();
        }

        auto pages = cache.get_pages(sst, Data, 0, 5 * 4096, 2).get();
        check_pages(pages, {{0, 'a'}, {4096, 'b'}});
        pages = cache.get_pages(sst, Data, pages.back().offset + 1, 5 * 4096, 2).get();
        check_pages(pages, {{8192, 'c'}, {12288, 'd'}});
        pages = cache.get_pages(sst, Data, pages.back().offset + 1, 5 * 4096, 2).get();
        check_pages(pages, {{16384, 'e'}});
    }, page_cache_config());
}

// The cached pages of different components of an sstable are separate,
// and so are the cached pages of different sstables.
SEASTAR_TEST_CASE(test_page_cache_components_and_sstables_are_separate) {
    return do_with_cql_env_thread([] (cql_test_env& env) {
        db::object_storage_page_cache_table cache(env.local_qp());
        auto sst1 = new_sstable_id();
        auto sst2 = new_sstable_id();
        cache.put_page(sst1, Data, 0, make_page('a')).get();
        cache.put_page(sst1, Rows, 0, make_page('b')).get();
        cache.put_page(sst2, Data, 0, make_page('c')).get();

        check_pages(cache.get_pages(sst1, Data, 0, 4096, 100).get(), {{0, 'a'}});
        check_pages(cache.get_pages(sst1, Rows, 0, 4096, 100).get(), {{0, 'b'}});
        check_pages(cache.get_pages(sst1, Partitions, 0, 4096, 100).get(), {});
        check_pages(cache.get_pages(sst2, Data, 0, 4096, 100).get(), {{0, 'c'}});
    }, page_cache_config());
}

// Putting a page at the offset of a cached page replaces it.
SEASTAR_TEST_CASE(test_page_cache_put_replaces_page) {
    return do_with_cql_env_thread([] (cql_test_env& env) {
        db::object_storage_page_cache_table cache(env.local_qp());
        auto sst = new_sstable_id();
        cache.put_page(sst, Data, 0, make_page('a')).get();
        cache.put_page(sst, Data, 0, make_page('b', 100)).get();

        check_pages(cache.get_pages(sst, Data, 0, 4096, 100).get(), {{0, 'b', 100}});
    }, page_cache_config());
}

// Pages of any size are cached and read back intact - in particular, both
// pages short enough to be stored inline in a bytes object (up to 31 bytes)
// and longer ones.
SEASTAR_TEST_CASE(test_page_cache_page_sizes) {
    return do_with_cql_env_thread([] (cql_test_env& env) {
        db::object_storage_page_cache_table cache(env.local_qp());
        auto sst = new_sstable_id();
        const std::vector<expected_page> pages = {
            {0, 'a', 1}, {1, 'b', 31}, {32, 'c', 32}, {64, 'd', 100000},
        };
        for (const auto& p : pages) {
            cache.put_page(sst, Data, p.offset, make_page(p.fill, p.size)).get();
        }

        check_pages(cache.get_pages(sst, Data, 0, 100064, 100).get(), pages);
    }, page_cache_config());
}

// evict_sstable() drops the cached pages of all the components of the
// sstable, and only of that sstable.
SEASTAR_TEST_CASE(test_page_cache_evict_sstable) {
    return do_with_cql_env_thread([] (cql_test_env& env) {
        db::object_storage_page_cache_table cache(env.local_qp());
        auto sst1 = new_sstable_id();
        auto sst2 = new_sstable_id();
        cache.put_page(sst1, Data, 0, make_page('a')).get();
        cache.put_page(sst1, Data, 4096, make_page('b')).get();
        cache.put_page(sst1, Rows, 0, make_page('c')).get();
        cache.put_page(sst2, Data, 0, make_page('d')).get();

        cache.evict_sstable(sst1).get();

        // Check that all of sst1's pages are gone.
        check_pages(cache.get_pages(sst1, Data, 0, 8192, 100).get(), {});
        check_pages(cache.get_pages(sst1, Rows, 0, 4096, 100).get(), {});
        // Check that sst2's page remains.
        check_pages(cache.get_pages(sst2, Data, 0, 4096, 100).get(), {{0, 'd'}});
    }, page_cache_config());
}

// Each shard has its own cache: different shards can cache different
// pages for the same sstable, and each shard sees only its own pages,
// which are stored on that shard.
SEASTAR_TEST_CASE(test_page_cache_per_shard) {
    return do_with_cql_env_thread([] (cql_test_env& env) {
        auto sst = new_sstable_id();
        smp::invoke_on_all([&env, sst] {
            return seastar::async([&env, sst] {
                db::object_storage_page_cache_table cache(env.qp().local());
                cache.put_page(sst, Data, 0, make_page('a' + this_shard_id())).get();
            });
        }).get();

        smp::invoke_on_all([&env, sst] {
            return seastar::async([&env, sst] {
                db::object_storage_page_cache_table cache(env.qp().local());
                // Check that this shard sees only the page it put.
                check_pages(cache.get_pages(sst, Data, 0, 4096, 100).get(), {{0, char('a' + this_shard_id())}});
                // Check that the page was written on this shard: its memtable
                // holds one partition, the cache of this shard.
                auto& table = env.local_db().find_column_family(db::object_storage_page_cache_table::SYSTEM_CACHE,
                        db::object_storage_page_cache_table::PAGECACHE);
                size_t partitions = 0;
                table.for_each_active_memtable([&] (replica::memtable& m) {
                    partitions += m.partition_count();
                });
                BOOST_REQUIRE_EQUAL(partitions, 1);
            });
        }).get();
    }, page_cache_config());
}

BOOST_AUTO_TEST_SUITE_END()
