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
#include <seastar/testing/test_case.hh>

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

BOOST_AUTO_TEST_SUITE_END()
