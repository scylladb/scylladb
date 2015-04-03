/*
 * Copyright 2015 Cloudius Systems
 */

#include <boost/test/included/unit_test.hpp>
#include "core/sstring.hh"
#include "core/future-util.hh"
#include "core/align.hh"
#include "sstables/sstables.hh"
#include "tests/test-utils.hh"
#include <memory>

using namespace sstables;

static auto la = sstable::version_types::la;
static auto big = sstable::format_types::big;

using sstable_ptr = lw_shared_ptr<sstable>;

namespace sstables {

class test {
    sstable_ptr _sst;
public:
    test(sstable_ptr s) : _sst(s) {}
    future<temporary_buffer<char>> data_read(uint64_t pos, size_t len) {
        return _sst->data_read(pos, len);
    }
    future<index_list> read_indexes(uint64_t position, uint64_t quantity) {
        return _sst->read_indexes(position, quantity);
    }
};
}

static future<> broken_sst(sstring dir, unsigned long generation) {

    auto sst = std::make_unique<sstable>(dir, generation, la, big);
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

static future<sstable_ptr> reusable_sst(sstring dir, unsigned long generation) {
    auto sst = make_lw_shared<sstable>(dir, generation, la, big);
    auto fut = sst->load();
    return std::move(fut).then([sst = std::move(sst)] {
        return make_ready_future<sstable_ptr>(std::move(sst));
    });
}

static future<> working_sst(sstring dir, unsigned long generation) {
    return reusable_sst(dir, generation).then([] (auto ptr) { return make_ready_future<>(); });
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
        return sstables::test(ptr).read_indexes(Position, Howmany).then([ptr] (auto vec) {
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

const sstring filename(sstring dir, sstring version, unsigned long generation, sstring format, sstring component) {
    return dir + "/" + version + "-" + to_sstring(generation) + "-" + format + "-" + component;
}

static future<> write_sst_info(sstring dir, unsigned long generation) {
    auto sst = make_lw_shared<sstable>(dir, generation, la, big);
    return sst->load().then([sst, generation] {
        sst->set_generation(generation + 1);
        return sst->store().then([sst] {
            return make_ready_future<>();
        });
    });
}

static future<std::pair<char*, size_t>> read_file(sstring file_path)
{
    return engine().open_file_dma(file_path, open_flags::rw).then([] (file f) {
        auto fp = make_shared<file>(std::move(f));
        return fp->size().then([fp] (auto size) {
            auto aligned_size = align_up(size, 512UL);
            auto rbuf = reinterpret_cast<char*>(::memalign(4096, aligned_size));
            ::memset(rbuf, 0, aligned_size);
            return fp->dma_read(0, rbuf, aligned_size).then([size, rbuf, fp] (auto ret) {
                BOOST_REQUIRE(ret == size);
                std::pair<char*, size_t> p = { rbuf, size };
                return make_ready_future<std::pair<char*, size_t>>(p);
            });
        });
    });
}

static future<> check_component_integrity(sstring component) {
    return write_sst_info("tests/urchin/sstables/compressed", 1).then([component] {
        auto file_path = filename("tests/urchin/sstables/compressed", "la", 1, "big", component);
        return read_file(file_path).then([component] (auto ret) {
            auto file_path = filename("tests/urchin/sstables/compressed", "la", 2, "big", component);
            return read_file(file_path).then([ret] (auto ret2) {
                // assert that both files have the same size.
                BOOST_REQUIRE(ret.second == ret2.second);
                // assert that both files have the same content.
                BOOST_REQUIRE(::memcmp(ret.first, ret2.first, ret.second) == 0);
                // free buf from both files.
                ::free(ret.first);
                ::free(ret2.first);
            });
        });
    });
}

SEASTAR_TEST_CASE(check_compressed_info_func) {
    return check_component_integrity("CompressionInfo.db");
}

SEASTAR_TEST_CASE(check_filter_func) {
    return check_component_integrity("Filter.db");
}

SEASTAR_TEST_CASE(uncompressed_random_access_read) {
    return reusable_sst("tests/urchin/sstables/uncompressed", 1).then([] (auto sstp) {
        // note: it's important to pass on a shared copy of sstp to prevent its
        // destruction until the continuation finishes reading!
        return sstables::test(sstp).data_read(97, 6).then([sstp] (temporary_buffer<char> buf) {
            BOOST_REQUIRE(sstring(buf.get(), buf.size()) == "gustaf");
            return make_ready_future<>();
        });
    });
}

SEASTAR_TEST_CASE(compressed_random_access_read) {
    return reusable_sst("tests/urchin/sstables/compressed", 1).then([] (auto sstp) {
        return sstables::test(sstp).data_read(97, 6).then([sstp] (temporary_buffer<char> buf) {
            BOOST_REQUIRE(sstring(buf.get(), buf.size()) == "gustaf");
            return make_ready_future<>();
        });
    });
}
