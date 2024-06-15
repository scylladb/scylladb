/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <fmt/ranges.h>
#include <seastar/testing/test_case.hh>
#include "test/lib/cql_test_env.hh"
#include "db/config.hh"

// These tests are slow, and tuned to a particular amount of memory
// (and --memory is ignored in debug mode).
// Hence they are not run in debug.
#ifndef SEASTAR_DEBUG

// The problem with naive index caching is that every uncached read drags a full
// index page into the cache. But the index page can be orders of magnitude bigger
// than the ingested row. Depending on the workload, this can effectively bloat the
// memory usage of every cached row by orders of magnitude, and ruin cache
// effectiveness.
//
// This test checks for the above problem.
//
// The table created by the test has the following properties:
// - There is only one column in the schema -- the partition key --
//   because this results in the biggest `index page size`:`row size` ratio,
//   due to details of the SSTable format.
//   This makes the effects drastic, and so easy to test.
// - The total size of all keys is at least 2 times greater than the size of RAM.
//   This ensures that most of the index is uncached. This is necessary for
//   the issue to become a visible.
// - The size of user data (1000 per partition) is significantly bigger
//   than several hundred bytes, to make various constant overheads
//   (per-cell, per-row, per-partition) smaller than the size of user data.
//   This simplifies reasoning about the test.
//   In particular, it should ensure that each index page contains about 2000 keys,
//   so it has size about 2 MiB.
//
// After populating this table, the test reads (sequentially) a subset of 1000 rows
// multiple times. Since the total size of this hot subset (including overheads) is
// only about 1 MiB, the test expects it to be perfectly cached.
// This should be true unless index cache is flooding the cache.
SEASTAR_TEST_CASE(test_index_doesnt_flood_cache_in_small_partition_workload) {
    cql_test_config cfg;
    // Prevents an unnecessary sleep at the end of the test.
    cfg.db_config->task_ttl_seconds.set(0);
    // Scylla refreshes query permissions periodically.
    // This causes background queries to system_auth.roles, which in turn can cause
    // spurious cache misses to appear in cache_tracker stats, which breaks
    // the assumptions of the test.
    // The lines below effectively disable the permission refresh to get rid of that problem.
    cfg.db_config->permissions_validity_in_ms.set(uint32_t(-1));
    cfg.db_config->permissions_update_interval_in_ms.set(uint32_t(-1));
    // As of this writing, uncommenting the below should make the test fail.
    // cfg.db_config->index_cache_fraction.set(1.0);
    return do_with_cql_env_thread([] (cql_test_env& e) {
        // We disable compactions because they cause confusing cache mispopulations.
        e.execute_cql("CREATE TABLE ks.t(pk blob PRIMARY KEY) WITH compaction = { 'class' : 'NullCompactionStrategy' };").get();
        auto insert_query = e.prepare("INSERT INTO ks.t(pk) VALUES (?)").get();
        auto select_query = e.prepare("SELECT * FROM t WHERE pk = ?").get();

        constexpr uint64_t pk_number = 600000;
        constexpr uint64_t pk_size = 1000;
        // Sanity check. The test assumes that the total index size is significantly bigger than RAM.
        BOOST_REQUIRE_GT(pk_size * pk_number, 2 * seastar::memory::stats().total_memory());

        // A bijection between uint64_t and blobs of size pk_size.
        auto make_key = [pk_size] (uint64_t x) {
            bytes b(bytes::initialized_later(), std::max(pk_size, sizeof(x)));
            auto i = b.begin();
            write<uint64_t>(i, x);
            return b;
        };

        // Populate the table.
        for (size_t i = 0; i < pk_number; ++i) {
            e.execute_prepared(insert_query, {{cql3::raw_value::make_value(make_key(i))}}).get();
        }
        // Flushing makes reasoning easier.
        e.db().invoke_on_all(&replica::database::flush_all_memtables).get();


        constexpr uint64_t hot_subset_size = 1000;
        // Sanity check. The test assumes that the total *hot* index size is significantly bigger than RAM.
        //
        // data_summary_ratio is the target `data file size : summary file size` ratio.
        // In a table containing only primary keys, the approximate size of an index page is `pk_size * data_summary_ratio`.
        //
        // The sanity check here is that the maximum total size of the touched index pages is much greater than RAM.
        // (Maximum is reached when each hot row lands on a different index page.)
        const uint64_t data_summary_ratio = static_cast<uint64_t>(1 / e.local_db().get_config().sstable_summary_ratio());
        BOOST_REQUIRE_GT(hot_subset_size * pk_size * data_summary_ratio, 2 * seastar::memory::stats().total_memory());

        auto get_misses = [&e] { return e.local_db().row_cache_tracker().get_stats().partition_misses; };
        uint64_t misses_before = get_misses();
        for (size_t i = 0; i < hot_subset_size; ++i) {
            e.execute_prepared(select_query, {{cql3::raw_value::make_value(make_key(i))}}).get();
            // Sanity check. If a single query is causing multiple partition misses,
            // something unexpected is happening.
            BOOST_REQUIRE_LE(get_misses() - misses_before, i + 1);
        }

        misses_before = get_misses();
        // The rows we just read have a small total size. They should be perfectly cached.
        for (size_t repeat = 0; repeat < 3; ++repeat) {
            for (size_t i = 0; i < hot_subset_size; ++i) {
                e.execute_prepared(select_query, {{cql3::raw_value::make_value(make_key(i))}}).get();
                // If the rows were perfectly cached, there were no new misses.
                BOOST_REQUIRE_EQUAL(get_misses(), misses_before);
            }
        }
    }, std::move(cfg));
}

// The previous test checks that index_cache_fraction doesn't allow index cache to
// flood the memory.
//
// This test checks that it doesn't completely kill caching.
SEASTAR_TEST_CASE(test_index_is_cached_in_big_partition_workload) {
    cql_test_config cfg;
    // Prevents an unnecessary sleep at the end of the test.
    cfg.db_config->task_ttl_seconds.set(0);
    // Scylla refreshes query permissions periodically.
    // This causes background queries to system_auth.roles, which in turn can cause
    // spurious cache misses to appear in cache_tracker stats, which breaks
    // the assumptions of the test.
    // The lines below effectively disable the permission refresh to get rid of that problem.
    cfg.db_config->permissions_validity_in_ms.set(uint32_t(-1));
    cfg.db_config->permissions_update_interval_in_ms.set(uint32_t(-1));
    // As of this writing, uncommenting the below should make the test fail.
    // cfg.db_config->index_cache_fraction.set(0.0);
    return do_with_cql_env_thread([] (cql_test_env& e) {
        // We disable compactions because they cause confusing cache mispopulations.
        e.execute_cql("CREATE TABLE ks.t(pk bigint, ck bigint, v blob, primary key (pk, ck)) WITH compaction = { 'class' : 'NullCompactionStrategy' };").get();
        auto insert_query = e.prepare("INSERT INTO ks.t(pk, ck, v) VALUES (?, ?, ?)").get();
        auto select_query = e.prepare("SELECT * FROM t WHERE pk = ? AND ck = ?").get();

        constexpr uint64_t pk_number = 10;
        constexpr uint64_t ck_number = 600;
        constexpr uint64_t v_size = 100000;
        // Sanity check. The test assumes that the total table size is significantly bigger than RAM.
        BOOST_REQUIRE_GT(pk_number * ck_number * v_size, 2 * seastar::memory::stats().total_memory());

        // A bijection between uint64_t and blobs of size x.
        auto make_key = [] (uint64_t x) {
            bytes b(bytes::initialized_later(), sizeof(x));
            auto i = b.begin();
            write<uint64_t>(i, x);
            return b;
        };

        // Populate the table.
        for (size_t pk = 0; pk < pk_number; ++pk) {
            for (size_t ck = 0; ck < ck_number; ++ck) {
                e.execute_prepared(insert_query, {{cql3::raw_value::make_value(make_key(pk))}, {cql3::raw_value::make_value(make_key(ck))}, {cql3::raw_value::make_value(bytes(v_size, 0))}}).get();
            }
        }
        // Flushing makes reasoning easier.
        e.db().invoke_on_all(&replica::database::flush_all_memtables).get();

        // Populate the index cache.
        for (size_t ck = 0; ck < ck_number; ++ck) {
            for (size_t pk = 0; pk < pk_number; ++pk) {
                e.execute_prepared(select_query, {{cql3::raw_value::make_value(make_key(pk))}, {cql3::raw_value::make_value(make_key(ck))}}).get();
            }
        }

        // The index is small and used once every few reads, so it should be perfectly cached.
        auto get_misses = [&e] { return e.local_db().row_cache_tracker().get_partition_index_cache_stats().misses; };
        uint64_t misses_before = get_misses();
        for (size_t ck = 0; ck < ck_number; ++ck) {
            for (size_t pk = 0; pk < pk_number; ++pk) {
                e.execute_prepared(select_query, {{cql3::raw_value::make_value(make_key(pk))}, {cql3::raw_value::make_value(make_key(ck))}}).get();
            }
        }
        BOOST_REQUIRE_EQUAL(get_misses(), misses_before);
    }, std::move(cfg));
}

#endif
