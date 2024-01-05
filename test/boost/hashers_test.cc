/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "db/timeout_clock.hh"

#include <seastar/util/closeable.hh>
#include "test/lib/scylla_test_case.hh"
#include <seastar/testing/thread_test_case.hh>
#include "utils/hashers.hh"
#include "utils/xx_hasher.hh"
#include "utils/simple_hashers.hh"
#include "gc_clock.hh"
#include "test/lib/simple_schema.hh"
#include "reader_concurrency_semaphore.hh"

bytes text_part1("sanity");
bytes text_part2("check");
bytes text_full("sanitycheck");

BOOST_AUTO_TEST_CASE(xx_hasher_sanity_check) {
    xx_hasher hasher;
    hasher.update(reinterpret_cast<const char*>(std::data(text_part1)), std::size(text_part1));
    hasher.update(reinterpret_cast<const char*>(std::data(text_part2)), std::size(text_part2));
    bytes hash = hasher.finalize();
    bytes expected = from_hex("00000000000000001b1308f9e7c7dcf4");
    BOOST_CHECK_EQUAL(hash, expected);
}

BOOST_AUTO_TEST_CASE(md5_hasher_sanity_check) {
    md5_hasher hasher;
    hasher.update(reinterpret_cast<const char*>(std::data(text_part1)), std::size(text_part1));
    hasher.update(reinterpret_cast<const char*>(std::data(text_part2)), std::size(text_part2));
    bytes hash = hasher.finalize();
    bytes expected = from_hex("a9221b2b5a53b9d9adf07f3305ed1a3e");
    BOOST_CHECK_EQUAL(hash, expected);
}

BOOST_AUTO_TEST_CASE(sha256_hasher_sanity_check) {
    sha256_hasher hasher;
    hasher.update(reinterpret_cast<const char*>(std::data(text_part1)), std::size(text_part1));
    hasher.update(reinterpret_cast<const char*>(std::data(text_part2)), std::size(text_part2));
    bytes hash = hasher.finalize();
    bytes expected = from_hex("62bcb3e6160172824e1939116f48ae3680df989583c6d1bfbfa84fa9a080d003");
    BOOST_REQUIRE_EQUAL(hash, expected);
}

BOOST_AUTO_TEST_CASE(bytes_view_hasher_sanity_check) {
    bytes_view_hasher hasher1;
    hasher1.update(reinterpret_cast<const char*>(std::data(text_part1)), std::size(text_part1));
    hasher1.update(reinterpret_cast<const char*>(std::data(text_part2)), std::size(text_part2));
    size_t hash1 = hasher1.finalize();

    bytes_view_hasher hasher2;
    hasher2.update(reinterpret_cast<const char*>(std::data(text_full)), std::size(text_full));
    size_t hash2 = hasher2.finalize();

    BOOST_REQUIRE_EQUAL(hash1, hash2);
}

SEASTAR_THREAD_TEST_CASE(mutation_fragment_sanity_check) {
    reader_concurrency_semaphore semaphore(reader_concurrency_semaphore::no_limits{}, __FILE__, reader_concurrency_semaphore::register_metrics::no);
    auto stop_semaphore = deferred_stop(semaphore);
    simple_schema s;
    auto permit = semaphore.make_tracking_only_permit(s.schema(), "test", db::no_timeout, {});
    gc_clock::time_point ts(gc_clock::duration(1234567890000));

    auto check_hash = [&] (const mutation_fragment& mf, uint64_t expected) {
        xx_hasher h;
        feed_hash(h, mf, *s.schema());
        auto v = h.finalize_uint64();
        BOOST_REQUIRE_EQUAL(v, expected);
    };



    {
        mutation_fragment f(*s.schema(), permit, partition_start{ s.make_pkey(0), {} });
        check_hash(f, 0xfb4f06dd4de434c2ull);
    }

    {
        mutation_fragment f(*s.schema(), permit, partition_start{ s.make_pkey(1), tombstone(42, ts) });
        check_hash(f, 0xcd9299d785a70d8dull);
    }

    {
        mutation_fragment f(*s.schema(), permit, s.make_row(permit, s.make_ckey(1), "abc"));
        check_hash(f, 0x8ae8c4860ca108bbull);
    }

    {
        mutation_fragment f(*s.schema(), permit, s.make_static_row(permit, "def"));
        check_hash(f, 0x2b8119e27581bbeeull);
    }

    {
        mutation_fragment f(*s.schema(), permit, s.make_range_tombstone(query::clustering_range::make(s.make_ckey(2), s.make_ckey(3)), ts));
        check_hash(f, 0x5092daca1b27ea26ull);
    }
}

BOOST_AUTO_TEST_CASE(basic_xx_hasher_sanity_check) {
    simple_xx_hasher hasher1;
    hasher1.update(reinterpret_cast<const char*>(std::data(text_part1)), std::size(text_part1));
    hasher1.update(reinterpret_cast<const char*>(std::data(text_part2)), std::size(text_part2));
    auto hash1 = hasher1.finalize();

    bytes_view_hasher hasher2;
    hasher2.update(reinterpret_cast<const char*>(std::data(text_part1)), std::size(text_part1));
    hasher2.update(reinterpret_cast<const char*>(std::data(text_part2)), std::size(text_part2));
    auto hash2 = hasher2.finalize();

    BOOST_CHECK_EQUAL(hash1, hash2);
}
