/*
 * Copyright 2015 Cloudius Systems
 */

#include <boost/test/included/unit_test.hpp>
#include "core/sstring.hh"
#include "sstables/sstables.hh"
#include "tests/test-utils.hh"
#include <memory>

using namespace sstables;

static auto la = sstable::version_types::la;
static auto big = sstable::format_types::big;

static future<> broken_sst(sstring dir, unsigned long epoch) {

    auto sst = std::make_unique<sstable>(dir, epoch, la, big);
    auto fut = sst->load();
    return std::move(fut).then_wrapped([sst = std::move(sst)] (future<> f) mutable {
        try {
            f.get();
            BOOST_FAIL("expecting exception");
        } catch (malformed_sstable_exception& e) {
            // ok
        }
        return make_ready_future<>();
    });
}

static future<> working_sst(sstring dir, unsigned long epoch) {
    auto sst = std::make_unique<sstable>(dir, epoch, la, big);
    auto fut = sst->load();
    return std::move(fut).then([sst = std::move(sst)] {});
}

SEASTAR_TEST_CASE(empty_toc) {
    return broken_sst("tests/urchin/sstables/badtoc", 1);
}

SEASTAR_TEST_CASE(alien_toc) {
    return broken_sst("tests/urchin/sstables/badtoc", 2);
}

SEASTAR_TEST_CASE(truncated_toc) {
    return broken_sst("tests/urchin/sstables/badtoc", 3);
}

SEASTAR_TEST_CASE(wrong_format_toc) {
    return broken_sst("tests/urchin/sstables/badtoc", 4);
}

SEASTAR_TEST_CASE(compression_truncated) {
    return broken_sst("tests/urchin/sstables/badcompression", 1);
}

SEASTAR_TEST_CASE(compression_bytes_flipped) {
    return broken_sst("tests/urchin/sstables/badcompression", 2);
}

SEASTAR_TEST_CASE(uncompressed_data) {
    return working_sst("tests/urchin/sstables/uncompressed", 1);
}

SEASTAR_TEST_CASE(compressed_data) {
    return working_sst("tests/urchin/sstables/compressed", 1);
}

SEASTAR_TEST_CASE(composite_index) {
    return working_sst("tests/urchin/sstables/composite", 1);
}
