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
#include <seastar/util/memory-data-source.hh>

#include "cql3/query_processor.hh"
#include "cql3/untyped_result_set.hh"
#include "db/config.hh"
#include "db/object_storage_page_cache_table.hh"
#include "dht/fixed_shard.hh"
#include "replica/database.hh"
#include "replica/memtable.hh"
#include "sstables/compress.hh"
#include "sstables/object_storage_page_cache_geometry.hh"
#include "sstables/object_storage_page_cache_file.hh"
#include "sstables/object_storage_page_cache_source.hh"
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

static void check_page(sstables::page_geometry::page_bounds page, uint64_t start, uint64_t end) {
    BOOST_REQUIRE_EQUAL(page.start, start);
    BOOST_REQUIRE_EQUAL(page.end, end);
}

// Fixed-size pages are aligned to the page size, and the last page ends at
// the end of the file.
BOOST_AUTO_TEST_CASE(test_fixed_size_page_geometry) {
    sstables::fixed_size_page_geometry geometry(10000, 4096);
    BOOST_REQUIRE_EQUAL(geometry.file_size(), 10000);
    BOOST_REQUIRE_EQUAL(geometry.average_page_size(), 4096);
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
    // Check that the average page size is that of the 4 chunks.
    BOOST_REQUIRE_EQUAL(geometry.average_page_size(), 1000);
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

// A page cache in memory, which counts its reads. It keeps the pages of
// one sstable.
class fake_page_cache : public sstables::object_storage_page_cache {
public:
    std::map<std::pair<sstables::component_type, uint64_t>, sstring> pages;
    int reads = 0;

    // The cached Data page at the given offset, or an empty string.
    sstring data_page(uint64_t offset) const {
        auto it = pages.find({Data, offset});
        return it == pages.end() ? sstring() : it->second;
    }

    virtual future<> put_page(sstables::sstable_id, sstables::component_type component,
            uint64_t offset, temporary_buffer<char> data) override {
        pages[{component, offset}] = sstring(data.get(), data.size());
        return make_ready_future<>();
    }
    virtual future<std::vector<page>> get_pages(sstables::sstable_id, sstables::component_type component,
            uint64_t from, uint64_t to, size_t max_pages) override {
        reads++;
        std::vector<page> ret;
        for (auto it = pages.lower_bound({component, from});
                it != pages.end() && it->first.first == component && it->first.second < to && ret.size() < max_pages; ++it) {
            ret.push_back(page{it->first.second, temporary_buffer<char>(it->second.data(), it->second.size())});
        }
        return make_ready_future<std::vector<page>>(std::move(ret));
    }
    virtual future<> evict_sstable(sstables::sstable_id) override {
        pages.clear();
        return make_ready_future<>();
    }
};

// An object in a fake object store, which records the byte range of every
// request to read it. It returns the data in buffers of 1000 bytes, which
// are not aligned to pages, as a real object store would.
struct fake_object {
    sstring content;
    std::vector<std::pair<uint64_t, uint64_t>> requests;

    explicit fake_object(size_t size) : content(sstring::initialized_later(), size) {
        // A different byte at every offset (mod 251), so data read from the
        // wrong offset is noticed.
        for (size_t i = 0; i < size; i++) {
            content[i] = char(i % 251);
        }
    }

    sstables::object_storage_range_reader reader() {
        return [this] (uint64_t offset, uint64_t length) {
            requests.emplace_back(offset, length);
            std::vector<temporary_buffer<char>> bufs;
            uint64_t end = std::min<uint64_t>(offset + length, content.size());
            for (uint64_t pos = offset; pos < end; pos += 1000) {
                bufs.emplace_back(content.data() + pos, std::min<uint64_t>(1000, end - pos));
            }
            return make_ready_future<data_source>(data_source(std::make_unique<util::memory_data_source>(std::move(bufs))));
        };
    }

    sstring bytes(uint64_t offset, uint64_t length) const {
        return content.substr(offset, length);
    }
};

static const auto test_sstable = sstables::sstable_id(utils::UUID_gen::get_time_UUID());

static input_stream<char> cached_stream(fake_page_cache& cache, fake_object& object,
        shared_ptr<const sstables::page_geometry> geometry, uint64_t offset, uint64_t length) {
    return input_stream<char>(sstables::make_page_cached_source(cache, test_sstable, Data, std::move(geometry),
            offset, length, object.reader()));
}

static sstring read_to_end(input_stream<char>& in) {
    sstring result;
    while (auto buf = in.read().get()) {
        result += sstring(buf.get(), buf.size());
    }
    in.close().get();
    return result;
}

// Reads the byte range [offset, offset + length) through the cache.
static sstring read_cached(fake_page_cache& cache, fake_object& object,
        shared_ptr<const sstables::page_geometry> geometry, uint64_t offset, uint64_t length) {
    auto in = cached_stream(cache, object, std::move(geometry), offset, length);
    return read_to_end(in);
}

using request = std::pair<uint64_t, uint64_t>;

// The first read of a range misses the cache, reads the range from the
// object store in a single request, and caches its pages; reading it again
// is served from the cache without any request.
SEASTAR_THREAD_TEST_CASE(test_page_cached_source_miss_then_hit) {
    fake_page_cache cache;
    fake_object object(10000);
    auto geometry = make_shared<sstables::fixed_size_page_geometry>(10000);

    BOOST_REQUIRE_EQUAL(read_cached(cache, object, geometry, 0, 10000), object.bytes(0, 10000));
    BOOST_REQUIRE(object.requests == std::vector<request>({{0, 10000}}));
    // Check that all the pages were cached, with their right contents.
    BOOST_REQUIRE_EQUAL(cache.pages.size(), 3);
    BOOST_REQUIRE_EQUAL(cache.data_page(0), object.bytes(0, 4096));
    BOOST_REQUIRE_EQUAL(cache.data_page(4096), object.bytes(4096, 4096));
    BOOST_REQUIRE_EQUAL(cache.data_page(8192), object.bytes(8192, 1808));

    // Check that reading again sends no request to the object store.
    BOOST_REQUIRE_EQUAL(read_cached(cache, object, geometry, 0, 10000), object.bytes(0, 10000));
    BOOST_REQUIRE_EQUAL(object.requests.size(), 1);
}

// A range which does not start and end at page boundaries is extended to
// whole pages when it is read from the object store and cached, but only
// the requested bytes are returned.
SEASTAR_THREAD_TEST_CASE(test_page_cached_source_unaligned_range) {
    fake_page_cache cache;
    fake_object object(10000);
    auto geometry = make_shared<sstables::fixed_size_page_geometry>(10000);

    BOOST_REQUIRE_EQUAL(read_cached(cache, object, geometry, 5000, 1000), object.bytes(5000, 1000));
    BOOST_REQUIRE(object.requests == std::vector<request>({{4096, 4096}}));
    BOOST_REQUIRE_EQUAL(cache.pages.size(), 1);
    BOOST_REQUIRE_EQUAL(cache.data_page(4096), object.bytes(4096, 4096));

    // Check that another range inside the cached page is served from the
    // cache.
    BOOST_REQUIRE_EQUAL(read_cached(cache, object, geometry, 4100, 3900), object.bytes(4100, 3900));
    BOOST_REQUIRE_EQUAL(object.requests.size(), 1);
}

// When the first pages of a range are cached but a later one is not, the
// cached pages are read from the cache, and everything from the first
// missing page to the end of the range is read in a single request - even
// if some of these pages are cached.
SEASTAR_THREAD_TEST_CASE(test_page_cached_source_gap) {
    fake_page_cache cache;
    fake_object object(10000);
    auto geometry = make_shared<sstables::fixed_size_page_geometry>(10000);
    // Cache the first and the last page, but not the middle one.
    read_cached(cache, object, geometry, 0, 100);
    read_cached(cache, object, geometry, 9000, 100);
    BOOST_REQUIRE_EQUAL(object.requests.size(), 2);

    BOOST_REQUIRE_EQUAL(read_cached(cache, object, geometry, 0, 10000), object.bytes(0, 10000));
    BOOST_REQUIRE(object.requests.back() == request(4096, 10000 - 4096));
    BOOST_REQUIRE_EQUAL(object.requests.size(), 3);
    BOOST_REQUIRE_EQUAL(cache.data_page(4096), object.bytes(4096, 4096));
}

// A long cached range is read from the cache in several reads, each of a
// batch of pages, without any request to the object store.
SEASTAR_THREAD_TEST_CASE(test_page_cached_source_many_cached_pages) {
    fake_page_cache cache;
    fake_object object(100 * 4096);
    auto geometry = make_shared<sstables::fixed_size_page_geometry>(100 * 4096);
    read_cached(cache, object, geometry, 0, 100 * 4096);
    BOOST_REQUIRE_EQUAL(object.requests.size(), 1);
    BOOST_REQUIRE_EQUAL(cache.pages.size(), 100);

    cache.reads = 0;
    BOOST_REQUIRE_EQUAL(read_cached(cache, object, geometry, 0, 100 * 4096), object.bytes(0, 100 * 4096));
    BOOST_REQUIRE_EQUAL(object.requests.size(), 1);
    BOOST_REQUIRE_GT(cache.reads, 1);
}

// Pages are read from the cache in batches of about 128 KiB, so the number
// of pages in each batch depends on their size: 1 KiB pages are read 128 at
// a time.
SEASTAR_THREAD_TEST_CASE(test_page_cached_source_batch_size) {
    fake_page_cache cache;
    fake_object object(300 * 1024);
    auto geometry = make_shared<sstables::fixed_size_page_geometry>(300 * 1024, 1024);
    read_cached(cache, object, geometry, 0, 300 * 1024);
    BOOST_REQUIRE_EQUAL(cache.pages.size(), 300);

    cache.reads = 0;
    BOOST_REQUIRE_EQUAL(read_cached(cache, object, geometry, 0, 300 * 1024), object.bytes(0, 300 * 1024));
    // Check that 300 pages took 3 reads, of 128, 128 and 44 pages.
    BOOST_REQUIRE_EQUAL(cache.reads, 3);
    BOOST_REQUIRE_EQUAL(object.requests.size(), 1);
}

// skip() in a cached range skips the pages without reading them.
SEASTAR_THREAD_TEST_CASE(test_page_cached_source_skip_cached) {
    fake_page_cache cache;
    fake_object object(10 * 4096);
    auto geometry = make_shared<sstables::fixed_size_page_geometry>(10 * 4096);
    read_cached(cache, object, geometry, 0, 10 * 4096);
    BOOST_REQUIRE_EQUAL(object.requests.size(), 1);

    auto in = cached_stream(cache, object, geometry, 0, 10 * 4096);
    BOOST_REQUIRE_EQUAL(sstring(in.read_exactly(100).get().get(), 100), object.bytes(0, 100));
    in.skip(3 * 4096).get();
    BOOST_REQUIRE_EQUAL(read_to_end(in), object.bytes(100 + 3 * 4096, 10 * 4096 - 100 - 3 * 4096));
    BOOST_REQUIRE_EQUAL(object.requests.size(), 1);
}

// skip() after a request to the object store has started continues with the
// same request, and the pages that it skips are cached too.
SEASTAR_THREAD_TEST_CASE(test_page_cached_source_skip_uncached) {
    fake_page_cache cache;
    fake_object object(10 * 4096);
    auto geometry = make_shared<sstables::fixed_size_page_geometry>(10 * 4096);

    auto in = cached_stream(cache, object, geometry, 0, 10 * 4096);
    BOOST_REQUIRE_EQUAL(sstring(in.read_exactly(100).get().get(), 100), object.bytes(0, 100));
    in.skip(3 * 4096).get();
    BOOST_REQUIRE_EQUAL(read_to_end(in), object.bytes(100 + 3 * 4096, 10 * 4096 - 100 - 3 * 4096));
    BOOST_REQUIRE(object.requests == std::vector<request>({{0, 10 * 4096}}));
    BOOST_REQUIRE_EQUAL(cache.pages.size(), 10);
}

// The following tests of skip() call it on the data_source directly, as an
// input_stream does with what it has not buffered.

static data_source cached_source(fake_page_cache& cache, fake_object& object,
        shared_ptr<const sstables::page_geometry> geometry, uint64_t offset, uint64_t length) {
    return sstables::make_page_cached_source(cache, test_sstable, Data, std::move(geometry), offset, length, object.reader());
}

static sstring read_to_end(data_source& src) {
    sstring result;
    while (auto buf = src.get().get()) {
        result += sstring(buf.get(), buf.size());
    }
    src.close().get();
    return result;
}

// A skip of up to max_read_through_skip bytes beyond what the request to the
// object store returned continues with the same request, and caches the
// pages it skipped.
SEASTAR_THREAD_TEST_CASE(test_page_cached_source_skip_up_to_limit) {
    constexpr uint64_t size = sstables::max_read_through_skip + 4 * 4096;
    fake_page_cache cache;
    fake_object object(size);
    auto geometry = make_shared<sstables::fixed_size_page_geometry>(size);

    auto src = cached_source(cache, object, geometry, 0, size);
    BOOST_REQUIRE_EQUAL(sstring(src.get().get().get(), 4096), object.bytes(0, 4096));
    src.skip(sstables::max_read_through_skip).get();
    auto pos = 4096 + sstables::max_read_through_skip;
    BOOST_REQUIRE_EQUAL(read_to_end(src), object.bytes(pos, size - pos));
    BOOST_REQUIRE(object.requests == std::vector<request>({{0, size}}));
    // Check that the skipped pages were cached.
    BOOST_REQUIRE_EQUAL(cache.data_page(4096), object.bytes(4096, 4096));
    BOOST_REQUIRE_EQUAL(cache.pages.size(), size / 4096);
}

// A longer skip closes the request to the object store, without reading or
// caching the pages it skipped, and the read continues with a new request.
SEASTAR_THREAD_TEST_CASE(test_page_cached_source_long_skip_uncached) {
    constexpr uint64_t size = sstables::max_read_through_skip + 4 * 4096;
    fake_page_cache cache;
    fake_object object(size);
    auto geometry = make_shared<sstables::fixed_size_page_geometry>(size);

    auto src = cached_source(cache, object, geometry, 0, size);
    BOOST_REQUIRE_EQUAL(sstring(src.get().get().get(), 4096), object.bytes(0, 4096));
    src.skip(sstables::max_read_through_skip + 4096).get();
    auto pos = 2 * 4096 + sstables::max_read_through_skip;
    BOOST_REQUIRE_EQUAL(read_to_end(src), object.bytes(pos, size - pos));
    BOOST_REQUIRE(object.requests == std::vector<request>({{0, size}, {pos, size - pos}}));
    // Check that the skipped pages were not cached.
    BOOST_REQUIRE_EQUAL(cache.data_page(4096), "");
    BOOST_REQUIRE_EQUAL(cache.data_page(pos - 4096), "");
    BOOST_REQUIRE_EQUAL(cache.pages.size(), 3);
}

// After a long skip, the read continues from the cache, so if the rest of
// the range is cached, it needs no new request.
SEASTAR_THREAD_TEST_CASE(test_page_cached_source_long_skip_to_cached) {
    constexpr uint64_t size = sstables::max_read_through_skip + 4 * 4096;
    fake_page_cache cache;
    fake_object object(size);
    auto geometry = make_shared<sstables::fixed_size_page_geometry>(size);
    auto pos = 2 * 4096 + sstables::max_read_through_skip;
    read_cached(cache, object, geometry, pos, size - pos);
    BOOST_REQUIRE_EQUAL(object.requests.size(), 1);

    auto src = cached_source(cache, object, geometry, 0, size);
    BOOST_REQUIRE_EQUAL(sstring(src.get().get().get(), 4096), object.bytes(0, 4096));
    src.skip(sstables::max_read_through_skip + 4096).get();
    BOOST_REQUIRE_EQUAL(read_to_end(src), object.bytes(pos, size - pos));
    // Check that only the first page needed a request.
    BOOST_REQUIRE(object.requests.back() == request(0, size));
    BOOST_REQUIRE_EQUAL(object.requests.size(), 2);
}

// skip(0) changes nothing.
SEASTAR_THREAD_TEST_CASE(test_page_cached_source_skip_zero) {
    fake_page_cache cache;
    fake_object object(10000);
    auto geometry = make_shared<sstables::fixed_size_page_geometry>(10000);

    auto src = cached_source(cache, object, geometry, 0, 10000);
    BOOST_REQUIRE_EQUAL(src.skip(0).get().size(), 0);
    BOOST_REQUIRE_EQUAL(read_to_end(src), object.bytes(0, 10000));
}

// A skip inside the first page, before anything was read, starts the read
// in the middle of that page, which is still read and cached whole.
SEASTAR_THREAD_TEST_CASE(test_page_cached_source_skip_within_page) {
    fake_page_cache cache;
    fake_object object(10000);
    auto geometry = make_shared<sstables::fixed_size_page_geometry>(10000);

    auto src = cached_source(cache, object, geometry, 0, 10000);
    src.skip(100).get();
    BOOST_REQUIRE_EQUAL(read_to_end(src), object.bytes(100, 9900));
    BOOST_REQUIRE(object.requests == std::vector<request>({{0, 10000}}));
    BOOST_REQUIRE_EQUAL(cache.data_page(0), object.bytes(0, 4096));
}

// A skip into the last page of a range which ends in the middle of a page
// returns only the requested part of that page.
SEASTAR_THREAD_TEST_CASE(test_page_cached_source_skip_into_last_page) {
    fake_page_cache cache;
    fake_object object(10 * 4096);
    auto geometry = make_shared<sstables::fixed_size_page_geometry>(10 * 4096);

    auto src = cached_source(cache, object, geometry, 0, 10000);
    src.skip(9000).get();
    BOOST_REQUIRE_EQUAL(read_to_end(src), object.bytes(9000, 1000));
    // Only the last page is read, extended to the end of the page.
    BOOST_REQUIRE(object.requests == std::vector<request>({{8192, 4096}}));
}

// A skip to exactly the end of the range leaves nothing to read, and needs
// no read from the cache and no request.
SEASTAR_THREAD_TEST_CASE(test_page_cached_source_skip_to_end) {
    fake_page_cache cache;
    fake_object object(10000);
    auto geometry = make_shared<sstables::fixed_size_page_geometry>(10000);

    auto src = cached_source(cache, object, geometry, 1000, 4000);
    src.skip(4000).get();
    BOOST_REQUIRE_EQUAL(read_to_end(src), "");
    BOOST_REQUIRE_EQUAL(cache.reads, 0);
    BOOST_REQUIRE(object.requests.empty());
}

// A skip beyond the end of the range fails, like the default skip() of a
// data_source.
SEASTAR_THREAD_TEST_CASE(test_page_cached_source_skip_beyond_end) {
    fake_page_cache cache;
    fake_object object(10000);
    auto geometry = make_shared<sstables::fixed_size_page_geometry>(10000);

    auto src = cached_source(cache, object, geometry, 1000, 4000);
    BOOST_REQUIRE_THROW(src.skip(4001).get(), std::runtime_error);
    src.close().get();
}

// A skip from cached pages over pages which are not cached does not read the
// skipped pages: the request to the object store starts at the page where
// reading resumes.
SEASTAR_THREAD_TEST_CASE(test_page_cached_source_skip_over_uncached) {
    fake_page_cache cache;
    fake_object object(10 * 4096);
    auto geometry = make_shared<sstables::fixed_size_page_geometry>(10 * 4096);
    // Cache only the first two pages.
    read_cached(cache, object, geometry, 0, 2 * 4096);
    BOOST_REQUIRE_EQUAL(object.requests.size(), 1);

    auto src = cached_source(cache, object, geometry, 0, 10 * 4096);
    BOOST_REQUIRE_EQUAL(sstring(src.get().get().get(), 4096), object.bytes(0, 4096));
    src.skip(5 * 4096).get();
    BOOST_REQUIRE_EQUAL(read_to_end(src), object.bytes(6 * 4096, 4 * 4096));
    BOOST_REQUIRE(object.requests.back() == request(6 * 4096, 4 * 4096));
    // Check that the skipped pages were not cached.
    BOOST_REQUIRE_EQUAL(cache.data_page(2 * 4096), "");
    BOOST_REQUIRE_EQUAL(cache.data_page(5 * 4096), "");
}

// The pages of a compressed Data component are its compressed chunks, so
// the source caches whole chunks, which a later read of one chunk finds.
SEASTAR_THREAD_TEST_CASE(test_page_cached_source_compressed) {
    sstables::compression c;
    c.set_uncompressed_chunk_length(4096);
    auto writer = c.offsets.get_writer();
    for (uint64_t offset : {0, 1000, 2500, 2600}) {
        writer.push_back(offset);
    }
    c.set_compressed_file_length(4000);
    auto geometry = make_shared<sstables::compressed_page_geometry>(c);
    fake_page_cache cache;
    fake_object object(4000);

    // The compressed reader reads from the start of a chunk to the end of
    // the last chunk it needs.
    BOOST_REQUIRE_EQUAL(read_cached(cache, object, geometry, 1000, 3000), object.bytes(1000, 3000));
    BOOST_REQUIRE(object.requests == std::vector<request>({{1000, 3000}}));
    BOOST_REQUIRE_EQUAL(cache.pages.size(), 3);
    BOOST_REQUIRE_EQUAL(cache.data_page(1000), object.bytes(1000, 1500));
    BOOST_REQUIRE_EQUAL(cache.data_page(2500), object.bytes(2500, 100));
    BOOST_REQUIRE_EQUAL(cache.data_page(2600), object.bytes(2600, 1400));

    // Check that a later read of one chunk is served from the cache.
    BOOST_REQUIRE_EQUAL(read_cached(cache, object, geometry, 2500, 100), object.bytes(2500, 100));
    BOOST_REQUIRE_EQUAL(object.requests.size(), 1);
}

// A cached page whose size is not the size of the page cannot be right, so
// it is read again from the object store, and replaced in the cache.
SEASTAR_THREAD_TEST_CASE(test_page_cached_source_wrong_size_page) {
    fake_page_cache cache;
    fake_object object(10000);
    auto geometry = make_shared<sstables::fixed_size_page_geometry>(10000);
    read_cached(cache, object, geometry, 0, 100);
    cache.pages[{Data, 4096}] = sstring(100, 'x');

    BOOST_REQUIRE_EQUAL(read_cached(cache, object, geometry, 0, 10000), object.bytes(0, 10000));
    BOOST_REQUIRE(object.requests.back() == request(4096, 10000 - 4096));
    BOOST_REQUIRE_EQUAL(cache.data_page(4096), object.bytes(4096, 4096));
}

// An empty range is read without any read from the cache or request.
SEASTAR_THREAD_TEST_CASE(test_page_cached_source_empty_range) {
    fake_page_cache cache;
    fake_object object(10000);
    auto geometry = make_shared<sstables::fixed_size_page_geometry>(10000);

    BOOST_REQUIRE_EQUAL(read_cached(cache, object, geometry, 5000, 0), "");
    BOOST_REQUIRE_EQUAL(cache.reads, 0);
    BOOST_REQUIRE(object.requests.empty());
}

// If the object store returns less data than the page geometry expects,
// the read fails rather than return or cache a partial page.
SEASTAR_THREAD_TEST_CASE(test_page_cached_source_object_too_short) {
    fake_page_cache cache;
    fake_object object(9000);
    auto geometry = make_shared<sstables::fixed_size_page_geometry>(10000);

    auto in = cached_stream(cache, object, geometry, 0, 10000);
    BOOST_REQUIRE_THROW(read_to_end(in), std::runtime_error);
    in.close().get();
    BOOST_REQUIRE_EQUAL(cache.data_page(8192), "");
}

// A file on a fake object store, whose dma_read_bulk() is one request to read
// the object, like that of a file on S3. Only reading is supported.
class fake_object_file : public file_impl {
    fake_object& _object;
public:
    explicit fake_object_file(fake_object& object) : _object(object) {}

    virtual future<temporary_buffer<uint8_t>> dma_read_bulk(uint64_t offset, size_t range_size, io_intent*) override {
        _object.requests.emplace_back(offset, range_size);
        auto len = offset < _object.content.size() ? std::min<uint64_t>(range_size, _object.content.size() - offset) : 0;
        return make_ready_future<temporary_buffer<uint8_t>>(
                temporary_buffer<uint8_t>(reinterpret_cast<const uint8_t*>(_object.content.data()) + offset, len));
    }
    virtual future<uint64_t> size() override {
        return make_ready_future<uint64_t>(_object.content.size());
    }
    virtual future<> close() override {
        return make_ready_future<>();
    }
    virtual std::unique_ptr<file_handle_impl> dup() override {
        struct handle : public file_handle_impl {
            fake_object& object;
            explicit handle(fake_object& o) : object(o) {}
            virtual std::unique_ptr<file_handle_impl> clone() const override {
                return std::make_unique<handle>(object);
            }
            virtual shared_ptr<file_impl> to_file() && override {
                return make_shared<fake_object_file>(object);
            }
        };
        return std::make_unique<handle>(_object);
    }
    virtual future<size_t> read_dma(uint64_t, void*, size_t, io_intent*) override { unsupported(); }
    virtual future<size_t> read_dma(uint64_t, std::vector<iovec>, io_intent*) override { unsupported(); }
    virtual future<size_t> write_dma(uint64_t, const void*, size_t, io_intent*) override { unsupported(); }
    virtual future<size_t> write_dma(uint64_t, std::vector<iovec>, io_intent*) override { unsupported(); }
    virtual future<> flush() override { unsupported(); }
    virtual future<struct stat> stat() override { unsupported(); }
    virtual future<> truncate(uint64_t) override { unsupported(); }
    virtual future<> discard(uint64_t, uint64_t) override { unsupported(); }
    virtual future<> allocate(uint64_t, uint64_t) override { unsupported(); }
    virtual subscription<directory_entry> list_directory(std::function<future<> (directory_entry)>) override { unsupported(); }
private:
    [[noreturn]] static void unsupported() {
        throw std::logic_error("unsupported operation on fake_object_file");
    }
};

static file cached_file_of(shared_ptr<fake_page_cache> cache, fake_object& object) {
    return sstables::make_page_cached_file(std::move(cache), test_sstable, Rows, file(make_shared<fake_object_file>(object)));
}

static sstring bulk_read(file& f, uint64_t offset, size_t size) {
    auto buf = f.dma_read_bulk<char>(offset, size).get();
    return sstring(buf.get(), buf.size());
}

// The first read of a page misses the cache, reads it with one request, and
// caches it; reading it again is served from the cache.
SEASTAR_THREAD_TEST_CASE(test_page_cached_file_miss_then_hit) {
    auto cache = make_shared<fake_page_cache>();
    fake_object object(10000);
    auto f = cached_file_of(cache, object);

    BOOST_REQUIRE_EQUAL(bulk_read(f, 4096, 4096), object.bytes(4096, 4096));
    BOOST_REQUIRE(object.requests == std::vector<request>({{4096, 4096}}));
    BOOST_REQUIRE_EQUAL(cache->pages.size(), 1);

    BOOST_REQUIRE_EQUAL(bulk_read(f, 4096, 4096), object.bytes(4096, 4096));
    BOOST_REQUIRE_EQUAL(object.requests.size(), 1);
    f.close().get();
}

// A read of several pages, e.g., with read-ahead, of which only the first is
// cached, reads the rest in a single request.
SEASTAR_THREAD_TEST_CASE(test_page_cached_file_several_pages) {
    auto cache = make_shared<fake_page_cache>();
    fake_object object(5 * 4096);
    auto f = cached_file_of(cache, object);
    bulk_read(f, 0, 4096);

    BOOST_REQUIRE_EQUAL(bulk_read(f, 0, 3 * 4096), object.bytes(0, 3 * 4096));
    BOOST_REQUIRE(object.requests.back() == request(4096, 2 * 4096));
    BOOST_REQUIRE_EQUAL(object.requests.size(), 2);
    BOOST_REQUIRE_EQUAL(cache->pages.size(), 3);
    f.close().get();
}

// A read beyond the end of the file returns only the bytes up to the end, or
// nothing - like any file - and only whole pages are read and cached.
SEASTAR_THREAD_TEST_CASE(test_page_cached_file_end_of_file) {
    auto cache = make_shared<fake_page_cache>();
    fake_object object(10000);
    auto f = cached_file_of(cache, object);

    // Check a read of a whole page which extends beyond the end of the file.
    BOOST_REQUIRE_EQUAL(bulk_read(f, 8192, 4096), object.bytes(8192, 1808));
    BOOST_REQUIRE(object.requests == std::vector<request>({{8192, 1808}}));
    // Check a read which starts at the end of the file.
    BOOST_REQUIRE_EQUAL(bulk_read(f, 10000, 4096), "");
    BOOST_REQUIRE_EQUAL(object.requests.size(), 1);
    f.close().get();
}

// read_dma(), into a buffer or into an iovec, reads through the cache too.
SEASTAR_THREAD_TEST_CASE(test_page_cached_file_read_dma) {
    auto cache = make_shared<fake_page_cache>();
    fake_object object(10000);
    auto f = cached_file_of(cache, object);

    std::vector<char> buf(100);
    BOOST_REQUIRE_EQUAL(f.dma_read(5000, buf.data(), buf.size()).get(), 100);
    BOOST_REQUIRE_EQUAL(sstring(buf.data(), buf.size()), object.bytes(5000, 100));
    BOOST_REQUIRE(object.requests == std::vector<request>({{4096, 4096}}));

    // Check that a read into an iovec of the same page is served from the cache.
    std::vector<char> buf1(50), buf2(70);
    std::vector<iovec> iov = {{buf1.data(), buf1.size()}, {buf2.data(), buf2.size()}};
    BOOST_REQUIRE_EQUAL(f.dma_read(6000, std::move(iov)).get(), 120);
    BOOST_REQUIRE_EQUAL(sstring(buf1.data(), buf1.size()), object.bytes(6000, 50));
    BOOST_REQUIRE_EQUAL(sstring(buf2.data(), buf2.size()), object.bytes(6050, 70));
    BOOST_REQUIRE_EQUAL(object.requests.size(), 1);
    f.close().get();
}

// A duplicate of the file, which resharding opens on other shards, reads the
// underlying file directly, without the cache.
SEASTAR_THREAD_TEST_CASE(test_page_cached_file_dup) {
    auto cache = make_shared<fake_page_cache>();
    fake_object object(10000);
    auto f = cached_file_of(cache, object);

    auto dup = f.dup().to_file();
    BOOST_REQUIRE_EQUAL(bulk_read(dup, 4096, 4096), object.bytes(4096, 4096));
    BOOST_REQUIRE(object.requests == std::vector<request>({{4096, 4096}}));
    BOOST_REQUIRE(cache->pages.empty());
    dup.close().get();
    f.close().get();
}

// Operations other than reads are forwarded to the underlying file.
SEASTAR_THREAD_TEST_CASE(test_page_cached_file_forwards_size) {
    auto cache = make_shared<fake_page_cache>();
    fake_object object(10000);
    auto f = cached_file_of(cache, object);

    BOOST_REQUIRE_EQUAL(f.size().get(), 10000);
    f.close().get();
}

BOOST_AUTO_TEST_SUITE_END()
