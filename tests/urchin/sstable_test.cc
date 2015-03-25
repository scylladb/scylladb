/*
 * Copyright 2015 Cloudius Systems
 */

#include <boost/test/included/unit_test.hpp>
#include "core/sstring.hh"
#include "core/future-util.hh"
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

using sstable_ptr = lw_shared_ptr<sstable>;
static future<sstable_ptr> reusable_sst(sstring dir, unsigned long epoch) {
    auto sst = make_lw_shared<sstable>(dir, epoch, la, big);
    auto fut = sst->load();
    return std::move(fut).then([sst = std::move(sst)] {
        return make_ready_future<sstable_ptr>(std::move(sst));
    });
}

static future<> working_sst(sstring dir, unsigned long epoch) {
    return reusable_sst(dir, epoch).then([] (auto ptr) { return make_ready_future<>(); });
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

template<uint64_t Position, uint64_t Howmany, uint64_t Expected>
future<> index_read(sstring path) {
    return reusable_sst(path, 1).then([] (sstable_ptr ptr) {
        return ptr->read_indexes_for_testing(Position, Howmany).then([ptr] (auto vec) {
            BOOST_REQUIRE(vec.size() == Expected);
            return make_ready_future<>();
        });
    });
}

template<uint64_t Position, uint64_t HowMany, uint64_t Expected>
future<> simple_index_read() {
    return index_read<Position, HowMany, Expected>("tests/urchin/sstables/uncompressed");
}

template<uint64_t Position, uint64_t HowMany, uint64_t Expected>
future<> composite_index_read() {
    return index_read<Position, HowMany, Expected>("tests/urchin/sstables/composite");
}

SEASTAR_TEST_CASE(simple_index_read_0_0_0) {
    return simple_index_read<0, 0, 0>();
}

SEASTAR_TEST_CASE(simple_index_read_0_1_1) {
    return simple_index_read<0, 1, 1>();
}

SEASTAR_TEST_CASE(simple_index_read_0_4_4) {
    return simple_index_read<0, 4, 4>();
}

SEASTAR_TEST_CASE(simple_index_read_0_10_4) {
    return simple_index_read<0, 10, 4>();
}

SEASTAR_TEST_CASE(simple_index_read_x13_1_1) {
    return simple_index_read<0x13, 1, 1>();
}

SEASTAR_TEST_CASE(simple_index_read_x50_0_0) {
    return simple_index_read<0x50, 0, 0>();
}

SEASTAR_TEST_CASE(composite_index_read_0_0_0) {
    return composite_index_read<0, 0, 0>();
}

SEASTAR_TEST_CASE(composite_index_read_0_1_1) {
    return composite_index_read<0, 1, 1>();
}

SEASTAR_TEST_CASE(composite_index_read_0_10_10) {
    return composite_index_read<0, 10, 10>();
}

SEASTAR_TEST_CASE(composite_index_read_0_20_20) {
    return composite_index_read<0, 20, 20>();
}

SEASTAR_TEST_CASE(composite_index_read_0_21_20) {
    return composite_index_read<0, 21, 20>();
}

template<uint64_t Position, uint64_t EntryPosition, uint64_t EntryKeySize>
future<> summary_query(sstring path, int generation) {
    return reusable_sst(path, generation).then([] (sstable_ptr ptr) {
        return ptr->read_summary_entry(Position).then([ptr] (auto entry) {
            BOOST_REQUIRE(entry.position == EntryPosition);
            BOOST_REQUIRE(entry.key.size() == EntryKeySize);
            return make_ready_future<>();
        });
    });
}

template<uint64_t Position, uint64_t EntryPosition, uint64_t EntryKeySize>
future<> summary_query_fail(sstring path, int generation) {
    return summary_query<Position, EntryPosition, EntryKeySize>(path, generation).then_wrapped([] (auto fut) {
        try {
            fut.get();
        } catch (std::out_of_range& ok) {
            return make_ready_future<>();
        }
        return make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(small_summary_query_ok) {
    return summary_query<0, 0, 5>("tests/urchin/sstables/uncompressed", 1);
}

SEASTAR_TEST_CASE(small_summary_query_fail) {
    return summary_query_fail<2, 0, 5>("tests/urchin/sstables/uncompressed", 1);
}

SEASTAR_TEST_CASE(small_summary_query_negative_fail) {
    return summary_query_fail<-2, 0, 5>("tests/urchin/sstables/uncompressed", 1);
}

SEASTAR_TEST_CASE(big_summary_query_0) {
    return summary_query<0, 0, 182>("tests/urchin/sstables/bigsummary", 76);
}

SEASTAR_TEST_CASE(big_summary_query_32) {
    return summary_query<32, 0x400c0000000000, 182>("tests/urchin/sstables/bigsummary", 76);
}

// Data file reading tests.
// We need these to be in the sstables namespace - and be friends of sstable -
// so we can test private functions too.
namespace sstables {

SEASTAR_TEST_CASE(uncompressed_random_access_read) {
    return reusable_sst("tests/urchin/sstables/uncompressed", 1).then([] (auto sstp) {
        // note: it's important to pass on a shared copy of sstp to prevent its
        // destruction until the continuation finishes reading!
        return sstp->data_read(97, 6).then([sstp] (temporary_buffer<char> buf) {
            BOOST_REQUIRE(sstring(buf.get(), buf.size()) == "gustaf");
            return make_ready_future<>();
        });
    });
}

SEASTAR_TEST_CASE(compressed_random_access_read) {
    return reusable_sst("tests/urchin/sstables/compressed", 1).then([] (auto sstp) {
        return sstp->data_read(97, 6).then([sstp] (temporary_buffer<char> buf) {
            BOOST_REQUIRE(sstring(buf.get(), buf.size()) == "gustaf");
            return make_ready_future<>();
        });
    });
}

}
