/*
 * Copyright (C) 2018 ScyllaDB
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

#include "sstables/data_consume_context.hh"
#include "sstables/mp_row_consumer.hh"
#include "tests/test-utils.hh"
#include "sstable_test.hh"

using namespace sstables;

struct my_mp_row_consumer_reader : public mp_row_consumer_reader {
    my_mp_row_consumer_reader(schema_ptr s) : mp_row_consumer_reader(std::move(s)) {}
    virtual future<> fill_buffer(db::timeout_clock::time_point) override {
        BOOST_FAIL("unexpected");
        abort();
    }
    virtual void next_partition() override { BOOST_FAIL("unexpected"); }
    virtual future<> fast_forward_to(const dht::partition_range&, db::timeout_clock::time_point) {
        BOOST_FAIL("unexpected");
        abort();
    }
    virtual void on_end_of_stream() override { BOOST_FAIL("unexpected"); }
    virtual future<> fast_forward_to(position_range, db::timeout_clock::time_point) {
        BOOST_FAIL("unexpected");
        abort();
    }
};

static void broken_sst(sstring dir, unsigned long generation, sstring msg) {
    // Using an empty schema for this function, which is only about loading
    // a malformed component and checking that it fails.
    auto s = make_lw_shared(schema({}, "ks", "cf", {}, {}, {}, {}, utf8_type));
    try {
        sstable_ptr sstp = std::get<0>(reusable_sst(s, dir, generation).get());
        auto r = std::make_unique<my_mp_row_consumer_reader>(s);
        auto c = std::make_unique<mp_row_consumer_k_l>(r.get(), s, default_priority_class(),
                                                       no_resource_tracking(),
                                                       streamed_mutation::forwarding::no, sstp);
        auto ctx = data_consume_rows<data_consume_rows_context>(*s, sstp, *c);
        auto fut = repeat([&ctx] {
            return ctx.read().then([&ctx] {
                return ctx.eof() ? stop_iteration::yes : stop_iteration::no;
            });
        });
        fut.get();
        BOOST_FAIL("expecting exception");
    } catch (malformed_sstable_exception& e) {
        BOOST_REQUIRE_EQUAL(sstring(e.what()), msg);
    }
}

SEASTAR_THREAD_TEST_CASE(bad_column_name) {
    broken_sst("tests/sstables/bad_column_name", 58,
               "Found 3 clustering elements in column name. Was not expecting that! in sstable "
               "tests/sstables/bad_column_name/la-58-big-Data.db");
}

SEASTAR_THREAD_TEST_CASE(empty_toc) {
    broken_sst("tests/sstables/badtoc", 1,
               "Empty TOC in sstable tests/sstables/badtoc/la-1-big-TOC.txt");
}

SEASTAR_THREAD_TEST_CASE(alien_toc) {
    broken_sst("tests/sstables/badtoc", 2,
               "tests/sstables/badtoc/la-2-big-Statistics.db: file not found");
}

SEASTAR_THREAD_TEST_CASE(truncated_toc) {
    broken_sst("tests/sstables/badtoc", 3,
               "tests/sstables/badtoc/la-3-big-Statistics.db: file not found");
}

SEASTAR_THREAD_TEST_CASE(wrong_format_toc) {
    broken_sst("tests/sstables/badtoc", 4,
               "tests/sstables/badtoc/la-4-big-TOC.txt: file not found");
}

SEASTAR_THREAD_TEST_CASE(compression_truncated) {
    broken_sst("tests/sstables/badcompression", 1,
               "tests/sstables/badcompression/la-1-big-Statistics.db: file not found");
}

SEASTAR_THREAD_TEST_CASE(compression_bytes_flipped) {
    broken_sst("tests/sstables/badcompression", 2,
               "tests/sstables/badcompression/la-2-big-Statistics.db: file not found");
}
