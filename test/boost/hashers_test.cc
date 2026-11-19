/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
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
#include "test/lib/random_utils.hh"
#include "schema/schema_builder.hh"
#include "types/map.hh"
#include "mutation/collection_mutation.hh"
#include "reader_concurrency_semaphore.hh"
#include "tracing/trace_state.hh"

bytes text_part1("sanity");
bytes text_part2("check");
bytes text_full("sanitycheck");
constexpr const char* xx_hasher_sanity_digest = "00000000000000001b1308f9e7c7dcf4";

BOOST_AUTO_TEST_CASE(xx_hasher_sanity_check) {
    xx_hasher hasher;
    hasher.update(reinterpret_cast<const char*>(std::data(text_part1)), std::size(text_part1));
    hasher.update(reinterpret_cast<const char*>(std::data(text_part2)), std::size(text_part2));
    bytes hash = hasher.finalize();
    bytes expected = from_hex(xx_hasher_sanity_digest);
    BOOST_CHECK_EQUAL(hash, expected);
}

BOOST_AUTO_TEST_CASE(buffered_xx_hasher_matches_xx_hasher) {
    auto data = tests::random::get_bytes(50000);
    auto seed = tests::random::get_int<uint64_t>();
    // Tiny chunks mimic per-field feeding; large ones cross and exceed the internal buffer.
    for (size_t max_chunk : {size_t(16), size_t(512), size_t(9000)}) {
        xx_hasher plain(seed);
        buffered_xx_hasher buffered(seed);
        // Empty values may arrive as a null pointer.
        plain.update(nullptr, 0);
        buffered.update(nullptr, 0);
        size_t pos = 0;
        while (pos < data.size()) {
            auto n = std::min(data.size() - pos, tests::random::get_int<size_t>(1, max_chunk));
            auto p = reinterpret_cast<const char*>(data.data() + pos);
            plain.update(p, n);
            buffered.update(p, n);
            pos += n;
        }
        BOOST_REQUIRE_EQUAL(plain.finalize_uint64(), buffered.finalize_uint64());
    }
    // Bypass on an empty buffer, fill it exactly, overflow it by one, straddle the bypass size, then bypass twice.
    xx_hasher plain(seed);
    buffered_xx_hasher buffered(seed);
    size_t pos = 0;
    constexpr size_t small = buffered_xx_hasher::bypass_size - 1;
    std::vector<size_t> sizes{buffered_xx_hasher::bypass_size};
    sizes.insert(sizes.end(), buffered_xx_hasher::buffer_size / small, small);
    for (size_t n : {buffered_xx_hasher::buffer_size % small, size_t(1), buffered_xx_hasher::bypass_size, small, buffered_xx_hasher::bypass_size,
                 buffered_xx_hasher::bypass_size}) {
        sizes.push_back(n);
    }
    for (size_t n : sizes) {
        auto p = reinterpret_cast<const char*>(data.data() + pos);
        plain.update(p, n);
        buffered.update(p, n);
        pos += n;
    }
    BOOST_REQUIRE_EQUAL(plain.finalize_uint64(), buffered.finalize_uint64());
}

BOOST_AUTO_TEST_CASE(buffered_xx_hasher_finalize_array) {
    auto data = tests::random::get_bytes(10000);
    auto p = reinterpret_cast<const char*>(data.data());
    xx_hasher plain;
    buffered_xx_hasher buffered;
    // Small updates leave bytes pending, so finalize_array() must flush them.
    for (size_t pos = 0; pos < data.size(); pos += 10) {
        plain.update(p + pos, std::min<size_t>(10, data.size() - pos));
        buffered.update(p + pos, std::min<size_t>(10, data.size() - pos));
    }
    BOOST_REQUIRE(plain.finalize_array() == buffered.finalize_array());
    BOOST_REQUIRE_EQUAL(plain.finalize(), buffered.finalize());

    buffered_xx_hasher text;
    text.update(reinterpret_cast<const char*>(std::data(text_full)), std::size(text_full));
    BOOST_CHECK_EQUAL(text.finalize(), from_hex(xx_hasher_sanity_digest));
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

template <typename Hasher>
void fragmented_hash_test() {
    const auto random_buffer = tests::random::get_bytes(512 * 1024);
    const auto random_buffer_fragmented = managed_bytes(random_buffer);

    // Sanity check that the buffer is actually fragmented, so we are testing what we intend to test.
    BOOST_REQUIRE(!is_single_fragment(managed_bytes_view(random_buffer_fragmented)));

    const auto fragmented_hash = Hasher::calculate(random_buffer_fragmented);
    const auto linearized_hash = Hasher::calculate(std::string_view(reinterpret_cast<const char*>(random_buffer.data()), random_buffer.size()));

    BOOST_CHECK_EQUAL(fragmented_hash, linearized_hash);
}

SEASTAR_TEST_CASE(md5_hasher_fragmented_buffer_hash) {
    fragmented_hash_test<md5_hasher>();
    return make_ready_future<>();
}

SEASTAR_TEST_CASE(sha256_hasher_fragmented_buffer_hash) {
    fragmented_hash_test<sha256_hasher>();
    return make_ready_future<>();
}

BOOST_AUTO_TEST_CASE(basic_xx_hasher_sanity_check) {
    simple_xx_hasher hasher1;
    hasher1.update(reinterpret_cast<const char*>(std::data(text_part1)), std::size(text_part1));
    hasher1.update(reinterpret_cast<const char*>(std::data(text_part2)), std::size(text_part2));
    size_t hash1 = hasher1.finalize();

    simple_xx_hasher hasher2;
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

    // Repair hashes with buffered_xx_hasher; its digests must stay pinned too.
    auto check_hash = [&] (const mutation_fragment& mf, uint64_t expected) {
        xx_hasher h;
        feed_hash(h, mf, *s.schema());
        BOOST_REQUIRE_EQUAL(h.finalize_uint64(), expected);
        buffered_xx_hasher bh;
        feed_hash(bh, mf, *s.schema());
        BOOST_REQUIRE_EQUAL(bh.finalize_uint64(), expected);
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

    {
        // A value of bypass_size or more skips the buffer, so pending bytes must be flushed first.
        static_assert(300 >= buffered_xx_hasher::bypass_size);
        mutation_fragment f(*s.schema(), permit, s.make_row(permit, s.make_ckey(4), sstring(300, 'x')));
        check_hash(f, 0xb07375d6da01bf3eull);
    }

    {
        // A collection larger than the buffer forces flushes inside the cell.
        auto ms = schema_builder(1, "ks", "coll")
                          .with_column("pk", int32_type, column_kind::partition_key)
                          .with_column("ck", int32_type, column_kind::clustering_key)
                          .with_column("m", map_type_impl::get_instance(int32_type, bytes_type, true))
                          .build();
        collection_mutation_writer w({});
        for (int32_t e = 0; e < 1000; ++e) {
            w.push_back(managed_bytes_view(int32_type->decompose(e)),
                    atomic_cell::make_live(*bytes_type, 1, bytes(32, int8_t(e)), atomic_cell::collection_member::yes));
        }
        clustering_row cr(clustering_key::from_singular(*ms, int32_t(1)));
        cr.cells().apply(*ms->get_column_definition(to_bytes("m")), atomic_cell_or_collection(std::move(w).finish()));
        auto mpermit = semaphore.make_tracking_only_permit(ms, "test", db::no_timeout, {});
        mutation_fragment f(*ms, mpermit, std::move(cr));
        xx_hasher h;
        feed_hash(h, f, *ms);
        buffered_xx_hasher bh;
        feed_hash(bh, f, *ms);
        BOOST_REQUIRE_EQUAL(h.finalize_uint64(), bh.finalize_uint64());
    }
}
