/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Tests for the object storage page cache.

#include <boost/test/unit_test.hpp>

#undef SEASTAR_TESTING_MAIN
#include <seastar/core/smp.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/with_scheduling_group.hh>
#include <seastar/core/with_timeout.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>

#include "cql3/query_processor.hh"
#include "cql3/untyped_result_set.hh"
#include "db/config.hh"
#include "db/object_storage_page_cache_table.hh"
#include "dht/fixed_shard.hh"
#include "replica/database.hh"
#include "replica/memtable.hh"
#include "sstables/compress.hh"
#include "sstables/object_storage_page_cache_geometry.hh"
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

// The timestamps of the cache's writes increase, even when they are taken in
// the same microsecond - so a write of a row, its deletion, and a write
// again apply in this order.
SEASTAR_THREAD_TEST_CASE(test_page_cache_timestamps_increase) {
    auto previous = db::object_storage_page_cache_table::next_timestamp();
    for (int i = 0; i < 1000; i++) {
        auto ts = db::object_storage_page_cache_table::next_timestamp();
        BOOST_REQUIRE_GT(ts, previous);
        previous = ts;
    }
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
        db::object_storage_page_cache_table cache(env.local_db(), env.local_qp());
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

// get_pages() and a cursor of read_pages() complete even when the reads'
// semaphore is short of memory, and lets only one read - the one which looks
// the pages up - proceed, while it makes the others wait for memory. The
// lookup reads the cache's sstables, which request memory, so it must not do
// that from the same semaphore, or it would wait behind the read which waits
// for it.
SEASTAR_TEST_CASE(test_page_cache_get_pages_when_short_of_memory) {
    auto cfg = page_cache_config();
    // Make reads wait for memory as soon as they use more than their
    // semaphore's memory, but never kill them.
    // (cql_test_env makes both unlimited, unless they are set as if on the
    // command line.)
    cfg.db_config->reader_concurrency_semaphore_serialize_limit_multiplier.set(1, utils::config_file::config_source::CommandLine);
    cfg.db_config->reader_concurrency_semaphore_kill_limit_multiplier.set(std::numeric_limits<uint32_t>::max() - 1,
            utils::config_file::config_source::CommandLine);
    return do_with_cql_env_thread([] (cql_test_env& env) {
        db::object_storage_page_cache_table cache(env.local_db(), env.local_qp());
        auto sst = new_sstable_id();
        cache.put_page(sst, Data, 0, make_page('a')).get();
        // Flush the page to an sstable, which the lookup reads.
        env.local_db().find_column_family(db::object_storage_page_cache_table::SYSTEM_CACHE,
                db::object_storage_page_cache_table::PAGECACHE).flush().get();

        // In the scheduling group of user reads, a read uses far more memory
        // than its semaphore has, and requests more: it is the one read which
        // may proceed.
        auto user_reads = get_scheduling_groups().get().statement_scheduling_group;
        with_scheduling_group(user_reads, [&] {
            return seastar::async([&] {
                auto& semaphore = env.local_db().get_reader_concurrency_semaphore();
                auto permit = semaphore.make_tracking_only_permit(nullptr, "test", db::no_timeout, {});
                auto consumed = permit.consume_memory(size_t(1) << 40);
                auto requested = permit.request_memory(1).get();

                // Check that the read can look up its pages, with get_pages()
                // and with a cursor.
                auto pages = with_timeout(lowres_clock::now() + std::chrono::seconds(60),
                        cache.get_pages(sst, Data, 0, 4096, 100)).get();
                check_pages(pages, {{0, 'a'}});
                auto cursor = cache.read_pages(sst, Data, 0, 4096);
                auto close = defer([&] () noexcept { cursor->close().get(); });
                auto page = with_timeout(lowres_clock::now() + std::chrono::seconds(60), cursor->get_page(0)).get();
                BOOST_REQUIRE_EQUAL(sstring(page.get(), page.size()), sstring(4096, 'a'));
            });
        }).get();
    }, std::move(cfg));
}

// get_pages() returns at most max_pages pages, and the caller can continue
// reading the range after the last page returned.
SEASTAR_TEST_CASE(test_page_cache_get_pages_max_pages) {
    return do_with_cql_env_thread([] (cql_test_env& env) {
        db::object_storage_page_cache_table cache(env.local_db(), env.local_qp());
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

// Reads the pages at the given offsets with a cursor of read_pages() of the
// range [from, to): the page at each offset, or an empty string if the
// cursor returned none.
static std::vector<sstring> read_with_cursor(db::object_storage_page_cache_table& cache, sstables::sstable_id sst,
        sstables::component_type component, uint64_t from, uint64_t to, std::vector<uint64_t> offsets) {
    auto cursor = cache.read_pages(sst, component, from, to);
    auto close = defer([&] () noexcept { cursor->close().get(); });
    std::vector<sstring> ret;
    for (auto offset : offsets) {
        auto buf = cursor->get_page(offset).get();
        ret.emplace_back(buf.get(), buf.size());
    }
    return ret;
}

// The contents of a page of make_page().
static sstring page_of(char fill, size_t size = 4096) {
    return sstring(size, fill);
}

// How many lookups the cache made in its table: each lookup reads the table
// with a new permit of the cache's semaphore.
static uint64_t lookups(cql_test_env& env) {
    return env.local_db().object_storage_page_cache_semaphore().get_stats().total_permits;
}

static replica::table& pagecache_table(cql_test_env& env) {
    return env.local_db().find_column_family(db::object_storage_page_cache_table::SYSTEM_CACHE,
            db::object_storage_page_cache_table::PAGECACHE);
}

// A cursor returns the cached pages at the offsets which it is asked for,
// and an empty buffer at an offset where no page is cached, in its range,
// of its sstable's component - and it continues after such an offset.
SEASTAR_TEST_CASE(test_page_cache_cursor_reads_pages) {
    return do_with_cql_env_thread([] (cql_test_env& env) {
        db::object_storage_page_cache_table cache(env.local_db(), env.local_qp());
        auto sst = new_sstable_id();
        cache.put_page(sst, Data, 0, make_page('a')).get();
        cache.put_page(sst, Data, 4096, make_page('b')).get();
        cache.put_page(sst, Data, 8192, make_page('c')).get();
        cache.put_page(sst, Data, 16384, make_page('d')).get();
        // Pages of components whose names sort before and after "Data", and
        // of another sstable, at an offset where Data has no page.
        cache.put_page(sst, CompressionInfo, 20480, make_page('x')).get();
        cache.put_page(sst, Index, 20480, make_page('y')).get();
        cache.put_page(new_sstable_id(), Data, 20480, make_page('z')).get();
        const auto end = std::numeric_limits<int64_t>::max();

        // Check that the cursor returns each page, and nothing where no page
        // is cached, and continues after it.
        BOOST_REQUIRE(read_with_cursor(cache, sst, Data, 0, end, {0, 4096, 12288, 16384, 20480})
                == std::vector<sstring>({page_of('a'), page_of('b'), "", page_of('d'), ""}));
        // Check that it returns a page after one which it wasn't asked for.
        BOOST_REQUIRE(read_with_cursor(cache, sst, Data, 0, end, {0, 8192})
                == std::vector<sstring>({page_of('a'), page_of('c')}));
        // Check that an offset between pages finds nothing, and doesn't
        // stop the cursor from returning the next page.
        BOOST_REQUIRE(read_with_cursor(cache, sst, Data, 0, end, {100, 4096})
                == std::vector<sstring>({"", page_of('b')}));
        // Check that a page starting before "from" isn't returned, and
        // neither is one starting at "to".
        BOOST_REQUIRE(read_with_cursor(cache, sst, Data, 4096, 8192, {0, 4096, 8192})
                == std::vector<sstring>({"", page_of('b'), ""}));
        // Check that an offset beyond "to" finds nothing, even far beyond it.
        BOOST_REQUIRE(read_with_cursor(cache, sst, Data, 0, 4096, {0, 16384})
                == std::vector<sstring>({page_of('a'), ""}));
        BOOST_REQUIRE(read_with_cursor(cache, sst, Data, 0, 4096, {1024 * 1024 * 1024})
                == std::vector<sstring>({""}));
        // Check that a cursor which isn't asked for any page can be closed.
        BOOST_REQUIRE(read_with_cursor(cache, sst, Data, 0, end, {}).empty());
        // Check that a cursor of a component which has no pages finds nothing.
        BOOST_REQUIRE(read_with_cursor(cache, sst, Rows, 0, end, {0}) == std::vector<sstring>({""}));
    }, page_cache_config());
}

// A cursor looks its range up in the cache's table once, and reads the
// following pages without looking them up again - however many they are -
// unless it is asked for a page far after the previous one, which it looks
// up anew rather than read the pages before it.
SEASTAR_TEST_CASE(test_page_cache_cursor_looks_up_once) {
    return do_with_cql_env_thread([] (cql_test_env& env) {
        db::object_storage_page_cache_table cache(env.local_db(), env.local_qp());
        auto sst = new_sstable_id();
        constexpr int pages = 100;
        for (int i = 0; i < pages; i++) {
            cache.put_page(sst, Data, i * 4096, make_page('a' + i % 26)).get();
        }
        // A page far after the others.
        constexpr uint64_t far = 100 * 1024 * 1024;
        cache.put_page(sst, Data, far, make_page('z')).get();
        // Flush some of the pages, so the cursor reads both the table's
        // memtable and its sstable.
        pagecache_table(env).flush().get();
        cache.put_page(sst, Data, pages * 4096, make_page('y')).get();

        // Check that reading all the consecutive pages takes one lookup.
        std::vector<uint64_t> offsets;
        std::vector<sstring> expected;
        for (int i = 0; i < pages; i++) {
            offsets.push_back(i * 4096);
            expected.push_back(page_of('a' + i % 26));
        }
        offsets.push_back(pages * 4096);
        expected.push_back(page_of('y'));
        auto before = lookups(env);
        BOOST_REQUIRE(read_with_cursor(cache, sst, Data, 0, far + 1, offsets) == expected);
        BOOST_REQUIRE_EQUAL(lookups(env) - before, 1);

        // Check that a page a little after the previous one is read without
        // another lookup, and each page far after the previous one, with
        // another lookup.
        before = lookups(env);
        BOOST_REQUIRE(read_with_cursor(cache, sst, Data, 0, far + 1, {0, 10 * 4096})
                == std::vector<sstring>({page_of('a'), page_of('k')}));
        BOOST_REQUIRE_EQUAL(lookups(env) - before, 1);
        before = lookups(env);
        BOOST_REQUIRE(read_with_cursor(cache, sst, Data, 0, far + 1, {0, 90 * 4096, far})
                == std::vector<sstring>({page_of('a'), page_of('a' + 90 % 26), page_of('z')}));
        BOOST_REQUIRE_EQUAL(lookups(env) - before, 3);

        // Check that after the last page of its range, a cursor is asked for
        // more pages without another lookup.
        before = lookups(env);
        BOOST_REQUIRE(read_with_cursor(cache, sst, Data, far, far + 1, {far, far + 4096, 2 * far})
                == std::vector<sstring>({page_of('z'), "", ""}));
        BOOST_REQUIRE_EQUAL(lookups(env) - before, 1);
    }, page_cache_config());
}

// A cursor doesn't return a page which a tombstone deleted - whether the
// tombstone and the page are in the same memtable or sstable of the cache's
// table, or in different ones - but returns a page which was cached again,
// after the tombstone.
SEASTAR_TEST_CASE(test_page_cache_cursor_skips_deleted_pages) {
    return do_with_cql_env_thread([] (cql_test_env& env) {
        db::object_storage_page_cache_table cache(env.local_db(), env.local_qp());
        auto s = db::object_storage_page_cache_table::schema();
        auto sst = new_sstable_id();
        auto pk = partition_key::from_exploded(*s, {data_value(int16_t(this_shard_id())).serialize_nonnull(),
                data_value(sst.uuid()).serialize_nonnull()});
        auto ck = [&] (int64_t offset) {
            return clustering_key::from_exploded(*s, {data_value(sstring("Data")).serialize_nonnull(),
                    data_value(offset).serialize_nonnull()});
        };
        auto apply = [&] (mutation m) {
            env.local_db().apply(s, freeze(m), {}, db::commitlog_force_sync::no, db::no_timeout).get();
        };
        auto tomb = [] {
            return tombstone(db::object_storage_page_cache_table::next_timestamp(), gc_clock::now());
        };
        const auto end = std::numeric_limits<int64_t>::max();
        auto read_all = [&] {
            std::vector<uint64_t> offsets;
            for (int i = 0; i < 8; i++) {
                offsets.push_back(i * 4096);
            }
            return read_with_cursor(cache, sst, Data, 0, end, offsets);
        };
        for (int i = 0; i < 8; i++) {
            cache.put_page(sst, Data, i * 4096, make_page('a' + i)).get();
        }
        pagecache_table(env).flush().get();

        // Delete page 1 with a row tombstone in the memtable, over the page
        // in the sstable, and page 2 with a row tombstone which is flushed
        // too. Delete pages 4 and 5 with a range tombstone [4, 6).
        mutation m1(s, pk);
        m1.partition().apply_delete(*s, ck(2 * 4096), tomb());
        apply(std::move(m1));
        pagecache_table(env).flush().get();
        mutation m2(s, pk);
        m2.partition().apply_delete(*s, ck(1 * 4096), tomb());
        m2.partition().apply_delete(*s, range_tombstone(position_in_partition::before_key(ck(4 * 4096)),
                position_in_partition::before_key(ck(6 * 4096)), tomb()));
        apply(std::move(m2));
        // Check that the deleted pages aren't returned, and the others are.
        BOOST_REQUIRE(read_all() == std::vector<sstring>({page_of('a'), "", "", page_of('d'), "", "", page_of('g'), page_of('h')}));
        // Check that a page cached again after its deletion is returned.
        cache.put_page(sst, Data, 4 * 4096, make_page('E')).get();
        BOOST_REQUIRE(read_all() == std::vector<sstring>({page_of('a'), "", "", page_of('d'), page_of('E'), "", page_of('g'), page_of('h')}));

        // Check that evicting the sstable - with a partition tombstone - hides
        // all its pages, in the memtable and in the sstables, and that a page
        // cached after it is returned.
        cache.evict_sstable(sst).get();
        BOOST_REQUIRE(read_all() == std::vector<sstring>(8, ""));
        pagecache_table(env).flush().get();
        cache.put_page(sst, Data, 3 * 4096, make_page('D')).get();
        BOOST_REQUIRE(read_all() == std::vector<sstring>({"", "", "", page_of('D'), "", "", "", ""}));
        pagecache_table(env).flush().get();
        BOOST_REQUIRE(read_all() == std::vector<sstring>({"", "", "", page_of('D'), "", "", "", ""}));
    }, page_cache_config());
}

// A cursor which a read keeps open - such as one saved in the querier cache,
// between pages of a query - keeps returning the pages after its cache's
// table was flushed or compacted.
SEASTAR_TEST_CASE(test_page_cache_cursor_across_flush_and_compaction) {
    return do_with_cql_env_thread([] (cql_test_env& env) {
        db::object_storage_page_cache_table cache(env.local_db(), env.local_qp());
        auto sst = new_sstable_id();
        for (int i = 0; i < 4; i++) {
            cache.put_page(sst, Data, i * 4096, make_page('a' + i)).get();
        }
        pagecache_table(env).flush().get();
        cache.put_page(sst, Data, 4 * 4096, make_page('e')).get();

        auto cursor = cache.read_pages(sst, Data, 0, 5 * 4096);
        auto close = defer([&] () noexcept { cursor->close().get(); });
        auto page = [&] (uint64_t offset) {
            auto buf = cursor->get_page(offset).get();
            return sstring(buf.get(), buf.size());
        };
        BOOST_REQUIRE_EQUAL(page(0), page_of('a'));
        // Check that the cursor continues after a flush.
        pagecache_table(env).flush().get();
        BOOST_REQUIRE_EQUAL(page(4096), page_of('b'));
        // Check that it continues after a major compaction, which replaces the
        // sstables which it reads.
        pagecache_table(env).compact_all_sstables(tasks::make_empty_task_info()).get();
        BOOST_REQUIRE_EQUAL(page(8192), page_of('c'));
        BOOST_REQUIRE_EQUAL(page(12288), page_of('d'));
        BOOST_REQUIRE_EQUAL(page(16384), page_of('e'));
    }, page_cache_config());
}

// The cached pages of different components of an sstable are separate,
// and so are the cached pages of different sstables.
SEASTAR_TEST_CASE(test_page_cache_components_and_sstables_are_separate) {
    return do_with_cql_env_thread([] (cql_test_env& env) {
        db::object_storage_page_cache_table cache(env.local_db(), env.local_qp());
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
        db::object_storage_page_cache_table cache(env.local_db(), env.local_qp());
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
        db::object_storage_page_cache_table cache(env.local_db(), env.local_qp());
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
        db::object_storage_page_cache_table cache(env.local_db(), env.local_qp());
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
                db::object_storage_page_cache_table cache(env.db().local(), env.qp().local());
                cache.put_page(sst, Data, 0, make_page('a' + this_shard_id())).get();
            });
        }).get();

        smp::invoke_on_all([&env, sst] {
            return seastar::async([&env, sst] {
                db::object_storage_page_cache_table cache(env.db().local(), env.qp().local());
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

static void check_page(sstables::page_geometry::page_bounds page, uint64_t start, uint64_t end) {
    BOOST_REQUIRE_EQUAL(page.start, start);
    BOOST_REQUIRE_EQUAL(page.end, end);
}

// Fixed-size pages are aligned to the page size, and the last page ends at
// the end of the file.
BOOST_AUTO_TEST_CASE(test_fixed_size_page_geometry) {
    sstables::fixed_size_page_geometry geometry(10000, 4096);
    BOOST_REQUIRE_EQUAL(geometry.file_size(), 10000);
    check_page(geometry.page_at(0), 0, 4096);
    check_page(geometry.page_at(4095), 0, 4096);
    check_page(geometry.page_at(4096), 4096, 8192);
    // Check that the last page is shorter, and ends at the end of the file.
    check_page(geometry.page_at(8192), 8192, 10000);
    check_page(geometry.page_at(9999), 8192, 10000);
    // Check that a position at or beyond the end of the file is refused.
    BOOST_REQUIRE_THROW(geometry.page_at(10000), std::out_of_range);

    // Check that if the file size is a multiple of the page size, the last
    // page is a whole page.
    sstables::fixed_size_page_geometry whole_pages(8192, 4096);
    check_page(whole_pages.page_at(8191), 4096, 8192);
    BOOST_REQUIRE_THROW(whole_pages.page_at(8192), std::out_of_range);
}

// The pages of a compressed Data component are its compressed chunks.
BOOST_AUTO_TEST_CASE(test_compressed_page_geometry) {
    sstables::compression c;
    c.set_uncompressed_chunk_length(4096);
    auto writer = c.offsets.get_writer();
    for (uint64_t offset : {0, 1000, 2500, 2600}) {
        writer.push_back(offset);
    }
    c.set_compressed_file_length(4000);

    sstables::compressed_page_geometry geometry(c);
    BOOST_REQUIRE_EQUAL(geometry.file_size(), 4000);
    check_page(geometry.page_at(0), 0, 1000);
    check_page(geometry.page_at(999), 0, 1000);
    check_page(geometry.page_at(1000), 1000, 2500);
    check_page(geometry.page_at(2599), 2500, 2600);
    // Check that the last chunk ends at the end of the file.
    check_page(geometry.page_at(2600), 2600, 4000);
    check_page(geometry.page_at(3999), 2600, 4000);
    BOOST_REQUIRE_THROW(geometry.page_at(4000), std::out_of_range);
}

// The chunk containing a position is found among many chunks of different
// sizes.
BOOST_AUTO_TEST_CASE(test_compressed_page_geometry_many_chunks) {
    sstables::compression c;
    c.set_uncompressed_chunk_length(4096);
    auto writer = c.offsets.get_writer();
    std::vector<uint64_t> starts;
    uint64_t offset = 0;
    for (int i = 0; i < 1000; i++) {
        starts.push_back(offset);
        writer.push_back(offset);
        offset += 100 + (i * 37) % 3000;
    }
    c.set_compressed_file_length(offset);

    sstables::compressed_page_geometry geometry(c);
    for (size_t i = 0; i < starts.size(); i++) {
        uint64_t end = i + 1 < starts.size() ? starts[i + 1] : offset;
        check_page(geometry.page_at(starts[i]), starts[i], end);
        check_page(geometry.page_at(end - 1), starts[i], end);
    }
}

// pages_covering() extends a byte range to the whole pages containing it.
BOOST_AUTO_TEST_CASE(test_pages_covering) {
    sstables::fixed_size_page_geometry geometry(10000, 4096);
    // Check a range inside one page.
    check_page(geometry.pages_covering(100, 200), 0, 4096);
    // Check a range of exactly one page.
    check_page(geometry.pages_covering(4096, 8192), 4096, 8192);
    // Check a range crossing a page boundary.
    check_page(geometry.pages_covering(4000, 4100), 0, 8192);
    // Check a range ending at the end of the file.
    check_page(geometry.pages_covering(5000, 10000), 4096, 10000);
    // Check that an empty range is refused.
    BOOST_REQUIRE_THROW(geometry.pages_covering(100, 100), std::out_of_range);
}

BOOST_AUTO_TEST_SUITE_END()
