/*
 * Copyright (C) 2020-present ScyllaDB
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

#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/file.hh>
#include <seastar/core/reactor.hh>
#include <seastar/util/defer.hh>

#include "test/lib/random_utils.hh"
#include "test/lib/log.hh"
#include "test/lib/tmpdir.hh"
#include "test/lib/reader_permit.hh"

#include "utils/cached_file.hh"

using namespace seastar;

static sstring read_to_string(cached_file::stream& s, size_t limit = std::numeric_limits<size_t>::max()) {
    sstring b;
    while (auto buf = s.next().get0()) {
        b += sstring(buf.get(), buf.size());
        if (b.size() >= limit) {
            break;
        }
    }
    return b.substr(0, limit);
}

static sstring read_to_string(cached_file& cf, size_t off, size_t limit = std::numeric_limits<size_t>::max()) {
    auto s = cf.read(off, default_priority_class());
    return read_to_string(s, limit);
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
    file f = open_file_dma(path.c_str(), open_flags::create | open_flags::rw).get0();

    testlog.debug("file contents: {}", contents);

    output_stream<char> out = make_file_output_stream(f).get0();
    auto close_out = defer([&] { out.close().get(); });
    out.write(contents.begin(), contents.size()).get();
    out.flush().get();

    f = open_file_dma(path.c_str(), open_flags::ro).get0();

    return test_file{
        .dir = std::move(dir),
        .f = std::move(f),
        .contents = std::move(contents)
    };
}

SEASTAR_THREAD_TEST_CASE(test_reading_from_small_file) {
    test_file tf = make_test_file(1024);

    {
        cached_file::metrics metrics;
        cached_file cf(tf.f, tests::make_permit(), metrics, 0, tf.contents.size());

        {
            BOOST_REQUIRE_EQUAL(tf.contents, read_to_string(cf, 0));

            BOOST_REQUIRE_EQUAL(1024, metrics.cached_bytes);
            BOOST_REQUIRE_EQUAL(1, metrics.page_misses);
            BOOST_REQUIRE_EQUAL(0, metrics.page_evictions);
            BOOST_REQUIRE_EQUAL(0, metrics.page_hits);
            BOOST_REQUIRE_EQUAL(1, metrics.page_populations);
        }

        {
            BOOST_REQUIRE_EQUAL(tf.contents.substr(2), read_to_string(cf, 2));

            BOOST_REQUIRE_EQUAL(1024, metrics.cached_bytes);
            BOOST_REQUIRE_EQUAL(1, metrics.page_misses);
            BOOST_REQUIRE_EQUAL(0, metrics.page_evictions);
            BOOST_REQUIRE_EQUAL(1, metrics.page_hits); // change here
            BOOST_REQUIRE_EQUAL(1, metrics.page_populations);
        }

        {
            BOOST_REQUIRE_EQUAL(sstring(), read_to_string(cf, 3000));

            // no change
            BOOST_REQUIRE_EQUAL(1024, metrics.cached_bytes);
            BOOST_REQUIRE_EQUAL(1, metrics.page_misses);
            BOOST_REQUIRE_EQUAL(0, metrics.page_evictions);
            BOOST_REQUIRE_EQUAL(1, metrics.page_hits);
            BOOST_REQUIRE_EQUAL(1, metrics.page_populations);
        }
    }

    {
        size_t off = 100;
        cached_file::metrics metrics;
        cached_file cf(tf.f, tests::make_permit(), metrics, off, tf.contents.size() - off);

        BOOST_REQUIRE_EQUAL(tf.contents.substr(off), read_to_string(cf, 0));
        BOOST_REQUIRE_EQUAL(tf.contents.substr(off + 2), read_to_string(cf, 2));
        BOOST_REQUIRE_EQUAL(sstring(), read_to_string(cf, 3000));
    }
}

SEASTAR_THREAD_TEST_CASE(test_invalidation) {
    auto page_size = cached_file::page_size;
    test_file tf = make_test_file(page_size * 2);

    cached_file::metrics metrics;
    cached_file cf(tf.f, tests::make_permit(), metrics, 0, page_size * 2);

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

SEASTAR_THREAD_TEST_CASE(test_invalidation_skewed_cached_file) {
    auto page_size = cached_file::page_size;
    test_file tf = make_test_file(page_size * 3);

    size_t offset = page_size / 2;
    cached_file::metrics metrics;
    cached_file cf(tf.f, tests::make_permit(), metrics, offset, page_size * 2);

    // Reads one page, half of the first page and half of the second page.
    auto read = [&] {
        BOOST_REQUIRE_EQUAL(
            tf.contents.substr(offset, page_size),
            read_to_string(cf, 0, page_size));
    };

    read();
    BOOST_REQUIRE_EQUAL(2, metrics.page_populations);
    BOOST_REQUIRE_EQUAL(2, metrics.page_misses);

    metrics = {};
    read();
    BOOST_REQUIRE_EQUAL(0, metrics.page_misses);
    BOOST_REQUIRE_EQUAL(2, metrics.page_hits);

    metrics = {};
    cf.invalidate_at_most(0, 1);
    BOOST_REQUIRE_EQUAL(0, metrics.page_evictions);
    read();
    BOOST_REQUIRE_EQUAL(0, metrics.page_misses);
    BOOST_REQUIRE_EQUAL(2, metrics.page_hits);

    metrics = {};
    cf.invalidate_at_most(1, page_size - align_up(offset, page_size) - 1);
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
    cf.invalidate_at_most(1, page_size);
    BOOST_REQUIRE_EQUAL(0, metrics.page_evictions);
    read();
    BOOST_REQUIRE_EQUAL(0, metrics.page_misses);
    BOOST_REQUIRE_EQUAL(2, metrics.page_hits);

    metrics = {};
    cf.invalidate_at_most(1, align_up(offset, page_size) - offset + page_size);
    BOOST_REQUIRE_EQUAL(1, metrics.page_evictions);
    read();
    BOOST_REQUIRE_EQUAL(1, metrics.page_misses);
    BOOST_REQUIRE_EQUAL(1, metrics.page_populations);
    BOOST_REQUIRE_EQUAL(1, metrics.page_hits);

    metrics = {};
    cf.invalidate_at_most(1, align_up(offset, page_size) - offset + page_size - 1);
    BOOST_REQUIRE_EQUAL(0, metrics.page_evictions);
    read();
    BOOST_REQUIRE_EQUAL(0, metrics.page_misses);
    BOOST_REQUIRE_EQUAL(0, metrics.page_populations);
    BOOST_REQUIRE_EQUAL(2, metrics.page_hits);

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
    cf.invalidate_at_most_front(align_up(offset, page_size) - offset + page_size);
    BOOST_REQUIRE_EQUAL(2, metrics.page_evictions);
    read();

    metrics = {};
    cf.invalidate_at_most_front(page_size * 3);
    BOOST_REQUIRE_EQUAL(2, metrics.page_evictions);

    read();
    BOOST_REQUIRE_EQUAL(2, metrics.page_misses);
    BOOST_REQUIRE_EQUAL(2, metrics.page_populations);
    BOOST_REQUIRE_EQUAL(0, metrics.page_hits);
}
