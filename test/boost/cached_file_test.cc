/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <boost/range/irange.hpp>
#include "test/lib/scylla_test_case.hh"
#include <seastar/testing/thread_test_case.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/when_all.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/file.hh>
#include <seastar/core/reactor.hh>
#include <seastar/util/defer.hh>

#include "test/lib/random_utils.hh"
#include "test/lib/log.hh"
#include "test/lib/tmpdir.hh"

#include "utils/cached_file.hh"

using namespace seastar;

static lru cf_lru;

static sstring read_to_string(cached_file::stream& s, size_t limit = std::numeric_limits<size_t>::max()) {
    sstring b;
    while (auto buf = s.next().get()) {
        b += sstring(buf.get(), buf.size());
        if (b.size() >= limit) {
            break;
        }
    }
    return b.substr(0, limit);
}

static void read_to_void(cached_file::stream& s, size_t limit = std::numeric_limits<size_t>::max()) {
    while (auto buf = s.next().get()) {
        if (buf.size() >= limit) {
            break;
        }
        limit -= buf.size();
    }
}

static sstring read_to_string(file& f, size_t start, size_t len) {
    file_input_stream_options opt;
    auto in = make_file_input_stream(f, start, len, opt);
    auto buf = in.read_exactly(len).get();
    return sstring(buf.get(), buf.size());
}

static sstring read_to_string(cached_file& cf, size_t off, size_t limit = std::numeric_limits<size_t>::max()) {
    auto s = cf.read(off, std::nullopt);
    return read_to_string(s, limit);
}

[[gnu::unused]]
static void read_to_void(cached_file& cf, size_t off, size_t limit = std::numeric_limits<size_t>::max()) {
    auto s = cf.read(off, std::nullopt);
    read_to_void(s, limit);
}

struct test_file {
    tmpdir dir;
    file f;
    sstring contents;

    ~test_file() {
        f.close().get();
    }
};

test_file make_test_file(size_t size) {
    tmpdir dir;
    auto contents = tests::random::get_sstring(size);

    auto path = dir.path() / "file";
    file f = open_file_dma(path.c_str(), open_flags::create | open_flags::rw).get();

    testlog.debug("file contents: {}", contents);

    output_stream<char> out = make_file_output_stream(f).get();
    auto close_out = defer([&] { out.close().get(); });
    out.write(contents.begin(), contents.size()).get();
    out.flush().get();

    f = open_file_dma(path.c_str(), open_flags::ro).get();

    return test_file{
        .dir = std::move(dir),
        .f = std::move(f),
        .contents = std::move(contents)
    };
}

SEASTAR_THREAD_TEST_CASE(test_file_wrapper) {
    auto page_size = cached_file::page_size;
    cached_file_stats metrics;
    test_file tf = make_test_file(page_size * 3);
    logalloc::region region;
    cached_file cf(tf.f, metrics, cf_lru, region, page_size * 3);
    seastar::file f = make_cached_seastar_file(cf);

    BOOST_REQUIRE_EQUAL(tf.contents.substr(0, 1),
        read_to_string(f, 0, 1));

    BOOST_REQUIRE_EQUAL(tf.contents.substr(page_size - 1, 10),
        read_to_string(f, page_size - 1, 10));

    BOOST_REQUIRE_EQUAL(tf.contents.substr(page_size - 1, cf.size() - (page_size - 1)),
        read_to_string(f, page_size - 1, cf.size() - (page_size - 1)));

    BOOST_REQUIRE_EQUAL(tf.contents.substr(0, cf.size()),
        read_to_string(f, 0, cf.size()));

    BOOST_CHECK_THROW(read_to_string(f, 0, cf.size() + 1), seastar::file::eof_error);
}

/* Reproducer for issue https://github.com/scylladb/scylladb/issues/14814 */
SEASTAR_THREAD_TEST_CASE(test_no_crash_on_dtor_after_oom) {
    auto page_size = cached_file::page_size;
    cached_file_stats metrics;
    test_file tf = make_test_file(page_size * 32); // 128k.
    logalloc::region region;
    cached_file cf(tf.f, metrics, cf_lru, region, page_size * 32);
    seastar::file f = make_cached_seastar_file(cf);

    utils::get_local_injector().enable("cached_file_get_first_page", true /* oneshot */);

    try {
        BOOST_REQUIRE_EQUAL(tf.contents.substr(0, page_size * 32),
                            read_to_string(f, 0, page_size * 32));
    } catch (...) {
        testlog.info("exception caught: {}", std::current_exception());
    }
}

SEASTAR_THREAD_TEST_CASE(test_concurrent_population) {
    auto page_size = cached_file::page_size;
    cached_file_stats metrics;
    test_file tf = make_test_file(page_size * 3);
    logalloc::region region;
    cached_file cf(tf.f, metrics, cf_lru, region, page_size * 3);
    seastar::file f = make_cached_seastar_file(cf);

    seastar::when_all(
        seastar::async([&] {
            BOOST_REQUIRE_EQUAL(tf.contents.substr(0, 1), read_to_string(f, 0, 1));
        }),
        seastar::async([&] {
            BOOST_REQUIRE_EQUAL(tf.contents.substr(0, 1), read_to_string(f, 0, 1));
        })
    ).get();

    BOOST_REQUIRE_EQUAL(page_size, metrics.cached_bytes);
    BOOST_REQUIRE_EQUAL(2, metrics.page_misses);
    BOOST_REQUIRE_EQUAL(0, metrics.page_evictions);
    BOOST_REQUIRE_EQUAL(0, metrics.page_hits);
    BOOST_REQUIRE_EQUAL(1, metrics.page_populations);
}

SEASTAR_THREAD_TEST_CASE(test_reading_from_small_file) {
    test_file tf = make_test_file(1024);

    {
        cached_file_stats metrics;
        logalloc::region region;
        cached_file cf(tf.f, metrics, cf_lru, region, tf.contents.size());

        {
            BOOST_REQUIRE_EQUAL(tf.contents, read_to_string(cf, 0));

            BOOST_REQUIRE_EQUAL(cached_file::page_size, metrics.cached_bytes);
            BOOST_REQUIRE_EQUAL(1, metrics.page_misses);
            BOOST_REQUIRE_EQUAL(0, metrics.page_evictions);
            BOOST_REQUIRE_EQUAL(0, metrics.page_hits);
            BOOST_REQUIRE_EQUAL(1, metrics.page_populations);
        }

        {
            BOOST_REQUIRE_EQUAL(tf.contents.substr(2), read_to_string(cf, 2));

            BOOST_REQUIRE_EQUAL(cached_file::page_size, metrics.cached_bytes);
            BOOST_REQUIRE_EQUAL(1, metrics.page_misses);
            BOOST_REQUIRE_EQUAL(0, metrics.page_evictions);
            BOOST_REQUIRE_EQUAL(1, metrics.page_hits); // change here
            BOOST_REQUIRE_EQUAL(1, metrics.page_populations);
        }

        {
            BOOST_REQUIRE_EQUAL(sstring(), read_to_string(cf, 3000));

            // no change
            BOOST_REQUIRE_EQUAL(cached_file::page_size, metrics.cached_bytes);
            BOOST_REQUIRE_EQUAL(1, metrics.page_misses);
            BOOST_REQUIRE_EQUAL(0, metrics.page_evictions);
            BOOST_REQUIRE_EQUAL(1, metrics.page_hits);
            BOOST_REQUIRE_EQUAL(1, metrics.page_populations);
        }
    }
}

SEASTAR_THREAD_TEST_CASE(test_eviction_via_lru) {
    auto page = cached_file::page_size;
    auto file_size = page * 2 + 12;
    test_file tf = make_test_file(file_size);

    {
        cached_file_stats metrics;
        logalloc::region region;
        cached_file cf(tf.f, metrics, cf_lru, region, tf.contents.size());

        {
            BOOST_REQUIRE_EQUAL(tf.contents, read_to_string(cf, 0));

            BOOST_REQUIRE_EQUAL(page * 3, metrics.cached_bytes);
            BOOST_REQUIRE_EQUAL(page * 3, cf.cached_bytes());
            BOOST_REQUIRE_EQUAL(3, metrics.page_misses);
            BOOST_REQUIRE_EQUAL(0, metrics.page_evictions);
            BOOST_REQUIRE_EQUAL(0, metrics.page_hits);
            BOOST_REQUIRE_EQUAL(3, metrics.page_populations);
        }

        {
            with_allocator(region.allocator(), [] {
                cf_lru.evict_all();
            });

            BOOST_REQUIRE_EQUAL(0, metrics.cached_bytes); // change here
            BOOST_REQUIRE_EQUAL(0, cf.cached_bytes()); // change here
            BOOST_REQUIRE_EQUAL(3, metrics.page_misses);
            BOOST_REQUIRE_EQUAL(3, metrics.page_evictions); // change here
            BOOST_REQUIRE_EQUAL(0, metrics.page_hits);
            BOOST_REQUIRE_EQUAL(3, metrics.page_populations);

            BOOST_REQUIRE_EQUAL(region.occupancy().used_space(), 0);
        }

        {
            BOOST_REQUIRE_EQUAL(tf.contents, read_to_string(cf, 0));

            BOOST_REQUIRE_EQUAL(page * 3, metrics.cached_bytes); // change here
            BOOST_REQUIRE_EQUAL(page * 3, cf.cached_bytes());
            BOOST_REQUIRE_EQUAL(6, metrics.page_misses); // change here
            BOOST_REQUIRE_EQUAL(3, metrics.page_evictions);
            BOOST_REQUIRE_EQUAL(0, metrics.page_hits);
            BOOST_REQUIRE_EQUAL(6, metrics.page_populations); // change here
        }

        {
            // Test that the page which is touched is evicted last
            BOOST_REQUIRE_EQUAL(tf.contents.substr(page, 1), read_to_string(cf, page, 1)); // hit page 1

            BOOST_REQUIRE_EQUAL(6, metrics.page_misses);
            BOOST_REQUIRE_EQUAL(3, metrics.page_evictions);
            BOOST_REQUIRE_EQUAL(1, metrics.page_hits);
            BOOST_REQUIRE_EQUAL(6, metrics.page_populations);

            cf_lru.evict();

            BOOST_REQUIRE_EQUAL(tf.contents.substr(page, 1), read_to_string(cf, page, 1)); // hit page 1

            BOOST_REQUIRE_EQUAL(6, metrics.page_misses);
            BOOST_REQUIRE_EQUAL(4, metrics.page_evictions); // change
            BOOST_REQUIRE_EQUAL(2, metrics.page_hits); // change
            BOOST_REQUIRE_EQUAL(6, metrics.page_populations);

            cf_lru.evict();

            BOOST_REQUIRE_EQUAL(tf.contents.substr(page, 1), read_to_string(cf, page, 1)); // hit page 1

            BOOST_REQUIRE_EQUAL(6, metrics.page_misses);
            BOOST_REQUIRE_EQUAL(5, metrics.page_evictions); // change
            BOOST_REQUIRE_EQUAL(3, metrics.page_hits); // change
            BOOST_REQUIRE_EQUAL(6, metrics.page_populations);
        }
    }
}

// A file which serves garbage but is very fast.
class garbage_file_impl : public file_impl {
private:
    [[noreturn]] void unsupported() {
        throw_with_backtrace<std::logic_error>("unsupported operation");
    }
public:
    // unsupported
    virtual future<size_t> write_dma(uint64_t pos, const void* buffer, size_t len, io_intent*) override { unsupported(); }
    virtual future<size_t> write_dma(uint64_t pos, std::vector<iovec> iov, io_intent*) override { unsupported(); }
    virtual future<> flush(void) override { unsupported(); }
    virtual future<> truncate(uint64_t length) override { unsupported(); }
    virtual future<> discard(uint64_t offset, uint64_t length) override { unsupported(); }
    virtual future<> allocate(uint64_t position, uint64_t length) override { unsupported(); }
    virtual subscription<directory_entry> list_directory(std::function<future<>(directory_entry)>) override { unsupported(); }
    virtual future<struct stat> stat(void) override { unsupported(); }
    virtual future<uint64_t> size(void) override { unsupported(); }
    virtual std::unique_ptr<seastar::file_handle_impl> dup() override { unsupported(); }

    virtual future<> close() override { return make_ready_future<>(); }

    virtual future<temporary_buffer<uint8_t>> dma_read_bulk(uint64_t offset, size_t size, io_intent*) override {
        return make_ready_future<temporary_buffer<uint8_t>>(temporary_buffer<uint8_t>(size));
    }

    virtual future<size_t> read_dma(uint64_t pos, void* buffer, size_t len, io_intent*) override {
        unsupported(); // FIXME
    }

    virtual future<size_t> read_dma(uint64_t pos, std::vector<iovec> iov, io_intent*) override {
        unsupported(); // FIXME
    }
};

#ifndef SEASTAR_DEFAULT_ALLOCATOR // Eviction works only with the seastar allocator
SEASTAR_THREAD_TEST_CASE(test_stress_eviction) {
    auto page_size = cached_file::page_size;
    auto n_pages = 8'000'000 / page_size;
    auto file_size = page_size * n_pages;
    auto cached_size = 4'000'000;

    cached_file_stats metrics;
    logalloc::region region;

    auto f = file(make_shared<garbage_file_impl>());
    cached_file cf(f, metrics, cf_lru, region, file_size);

    region.make_evictable([&] {
        testlog.trace("Evicting");
        cf.invalidate_at_most_front(file_size / 2);
        return cf_lru.evict();
    });

    for (size_t i = 0; i < (cached_size / page_size); ++i) {
        read_to_string(cf, page_size * i, page_size);
    }

    testlog.debug("Saturating memory...");

    // Disable background reclaiming which will prevent bugs from reproducing
    // We want reclamation to happen synchronously with page cache population in read_to_void()
    seastar::memory::set_min_free_pages(0);

    // Saturate std memory
    chunked_fifo<bytes> blobs;
    auto rc = region.reclaim_counter();
    while (region.reclaim_counter() == rc) {
        blobs.emplace_back(bytes(bytes::initialized_later(), 1024));
    }

    testlog.debug("Memory: allocated={}, free={}", seastar::memory::stats().allocated_memory(), seastar::memory::stats().free_memory());
    testlog.debug("Starting test...");

    for (size_t j = 0; j < n_pages * 16; ++j) {
        testlog.trace("Allocating");
        auto stride = tests::random::get_int(1, 20);
        auto page_idx = tests::random::get_int(n_pages - stride);
        read_to_void(cf, page_idx * page_size, page_size * stride);
    }
}
#endif

SEASTAR_THREAD_TEST_CASE(test_invalidation) {
    auto page_size = cached_file::page_size;
    test_file tf = make_test_file(page_size * 2);

    cached_file_stats metrics;
    logalloc::region region;
    cached_file cf(tf.f, metrics, cf_lru, region, page_size * 2);

    // Reads one page, half of the first page and half of the second page.
    auto read = [&] {
        BOOST_REQUIRE_EQUAL(
            tf.contents.substr(page_size / 2, page_size),
            read_to_string(cf, page_size / 2, page_size));
    };

    read();
    BOOST_REQUIRE_EQUAL(2, metrics.page_populations);
    BOOST_REQUIRE_EQUAL(2, metrics.page_misses);

    metrics = {};
    read();
    BOOST_REQUIRE_EQUAL(0, metrics.page_misses);
    BOOST_REQUIRE_EQUAL(2, metrics.page_hits);

    metrics = {};
    cf.invalidate_at_most(0, page_size / 2);
    BOOST_REQUIRE_EQUAL(0, metrics.page_evictions);
    read();
    BOOST_REQUIRE_EQUAL(0, metrics.page_misses);
    BOOST_REQUIRE_EQUAL(2, metrics.page_hits);

    metrics = {};
    cf.invalidate_at_most(0, page_size - 1);
    BOOST_REQUIRE_EQUAL(0, metrics.page_evictions);
    read();
    BOOST_REQUIRE_EQUAL(0, metrics.page_misses);
    BOOST_REQUIRE_EQUAL(2, metrics.page_hits);

    metrics = {};
    cf.invalidate_at_most(0, page_size);
    BOOST_REQUIRE_EQUAL(1, metrics.page_evictions);
    read();
    BOOST_REQUIRE_EQUAL(1, metrics.page_misses);
    BOOST_REQUIRE_EQUAL(1, metrics.page_populations);
    BOOST_REQUIRE_EQUAL(1, metrics.page_hits);

    metrics = {};
    cf.invalidate_at_most(page_size, page_size + 1);
    BOOST_REQUIRE_EQUAL(0, metrics.page_evictions);
    read();
    BOOST_REQUIRE_EQUAL(0, metrics.page_misses);
    BOOST_REQUIRE_EQUAL(2, metrics.page_hits);

    metrics = {};
    cf.invalidate_at_most(page_size, page_size + page_size);
    BOOST_REQUIRE_EQUAL(1, metrics.page_evictions);
    read();
    BOOST_REQUIRE_EQUAL(1, metrics.page_misses);
    BOOST_REQUIRE_EQUAL(1, metrics.page_populations);
    BOOST_REQUIRE_EQUAL(1, metrics.page_hits);

    metrics = {};
    cf.invalidate_at_most(0, page_size * 3);
    BOOST_REQUIRE_EQUAL(2, metrics.page_evictions);
    read();
    BOOST_REQUIRE_EQUAL(2, metrics.page_misses);
    BOOST_REQUIRE_EQUAL(2, metrics.page_populations);
    BOOST_REQUIRE_EQUAL(0, metrics.page_hits);

    metrics = {};
    cf.invalidate_at_most_front(0);
    BOOST_REQUIRE_EQUAL(0, metrics.page_evictions);

    metrics = {};
    cf.invalidate_at_most_front(1);
    BOOST_REQUIRE_EQUAL(0, metrics.page_evictions);

    metrics = {};
    cf.invalidate_at_most_front(page_size);
    BOOST_REQUIRE_EQUAL(1, metrics.page_evictions);

    metrics = {};
    cf.invalidate_at_most_front(page_size * 2);
    BOOST_REQUIRE_EQUAL(1, metrics.page_evictions);

    read();
    BOOST_REQUIRE_EQUAL(2, metrics.page_misses);
    BOOST_REQUIRE_EQUAL(2, metrics.page_populations);
    BOOST_REQUIRE_EQUAL(0, metrics.page_hits);
}
