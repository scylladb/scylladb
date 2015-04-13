/*
 * Copyright 2015 Cloudius Systems
 */

#include <boost/test/included/unit_test.hpp>
#include "core/sstring.hh"
#include "core/future-util.hh"
#include "core/align.hh"
#include "core/do_with.hh"
#include "sstables/sstables.hh"
#include "tests/test-utils.hh"
#include <memory>

using namespace sstables;

static auto la = sstable::version_types::la;
static auto big = sstable::format_types::big;

using sstable_ptr = lw_shared_ptr<sstable>;

bytes as_bytes(const sstring& s) {
    return { reinterpret_cast<const int8_t*>(s.begin()), s.size() };
}

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

    future<> read_statistics() {
        return _sst->read_statistics();
    }

    statistics& get_statistics() {
        return _sst->_statistics;
    }

    future<> read_summary() {
        return _sst->read_summary();
    }

    summary& get_summary() {
        return _sst->_summary;
    }

    future<> read_toc() {
        return _sst->read_toc();
    }

    auto& get_components() {
        return _sst->_components;
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

static future<sstable_ptr> do_write_sst(sstring dir, unsigned long generation) {
    auto sst = make_lw_shared<sstable>(dir, generation, la, big);
    return sst->load().then([sst, generation] {
        sst->set_generation(generation + 1);
        auto fut = sst->store();
        return std::move(fut).then([sst = std::move(sst)] {
            return make_ready_future<sstable_ptr>(std::move(sst));
        });
    });
}

static future<> write_sst_info(sstring dir, unsigned long generation) {
    return do_write_sst(dir, generation).then([] (auto ptr) { return make_ready_future<>(); });
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

SEASTAR_TEST_CASE(check_summary_func) {
    return do_write_sst("tests/urchin/sstables/compressed", 1).then([] (auto sst1) {
        auto sst2 = make_lw_shared<sstable>("tests/urchin/sstables/compressed", 2, la, big);
        return sstables::test(sst2).read_summary().then([sst1, sst2] {
            summary& sst1_s = sstables::test(sst1).get_summary();
            summary& sst2_s = sstables::test(sst2).get_summary();

            BOOST_REQUIRE(::memcmp(&sst1_s.header, &sst2_s.header, sizeof(summary::header)) == 0);
            BOOST_REQUIRE(sst1_s.positions == sst2_s.positions);
            BOOST_REQUIRE(sst1_s.entries == sst2_s.entries);
            BOOST_REQUIRE(sst1_s.first_key.value == sst2_s.first_key.value);
            BOOST_REQUIRE(sst1_s.last_key.value == sst2_s.last_key.value);
        });
    });
}

SEASTAR_TEST_CASE(check_filter_func) {
    return check_component_integrity("Filter.db");
}

SEASTAR_TEST_CASE(check_statistics_func) {
    return do_write_sst("tests/urchin/sstables/compressed", 1).then([] (auto sst1) {
        auto sst2 = make_lw_shared<sstable>("tests/urchin/sstables/compressed", 2, la, big);
        return sstables::test(sst2).read_statistics().then([sst1, sst2] {
            statistics& sst1_s = sstables::test(sst1).get_statistics();
            statistics& sst2_s = sstables::test(sst2).get_statistics();

            BOOST_REQUIRE(sst1_s.hash.map.size() == sst2_s.hash.map.size());
            BOOST_REQUIRE(sst1_s.contents.size() == sst2_s.contents.size());

            return do_for_each(sst1_s.hash.map.begin(), sst1_s.hash.map.end(),
                    [sst1, sst2, &sst1_s, &sst2_s] (auto val) {
                BOOST_REQUIRE(val.second == sst2_s.hash.map[val.first]);
                return make_ready_future<>();
            });
            // TODO: compare the field contents from both sstables.
        });
    });
}

SEASTAR_TEST_CASE(check_toc_func) {
    return do_write_sst("tests/urchin/sstables/compressed", 1).then([] (auto sst1) {
        auto sst2 = make_lw_shared<sstable>("tests/urchin/sstables/compressed", 2, la, big);
        return sstables::test(sst2).read_toc().then([sst1, sst2] {
            auto& sst1_c = sstables::test(sst1).get_components();
            auto& sst2_c = sstables::test(sst2).get_components();

            BOOST_REQUIRE(sst1_c == sst2_c);
        });
    });
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

class test_row_consumer : public row_consumer {
public:
    const uint64_t desired_timestamp;
    test_row_consumer(uint64_t t) : desired_timestamp(t) { }
    int count_row_start = 0;
    int count_cell = 0;
    int count_range_tombstone = 0;
    int count_row_end = 0;
    virtual void consume_row_start(bytes_view key, sstables::deletion_time deltime) override {
        BOOST_REQUIRE(key == as_bytes("vinna"));
        BOOST_REQUIRE(deltime.local_deletion_time == (uint32_t)std::numeric_limits<int32_t>::max());
        BOOST_REQUIRE(deltime.marked_for_delete_at == (uint64_t)std::numeric_limits<int64_t>::min());
        count_row_start++;
    }

    virtual void consume_cell(bytes_view col_name, bytes_view value, uint64_t timestamp) override {
        switch (count_cell) {
        case 0:
            // The silly "cql marker" column
            BOOST_REQUIRE(col_name.size() == 3 && col_name[0] == 0 && col_name[1] == 0 && col_name[2] == 0);
            BOOST_REQUIRE(value.size() == 0);
            BOOST_REQUIRE(timestamp == desired_timestamp);
            break;
        case 1:
            BOOST_REQUIRE(col_name.size() == 7 && col_name[0] == 0 &&
                    col_name[1] == 4 && col_name[2] == 'c' &&
                    col_name[3] == 'o' && col_name[4] == 'l' &&
                    col_name[5] == '1' && col_name[6] == '\0');
            BOOST_REQUIRE(value == as_bytes("daughter"));
            BOOST_REQUIRE(timestamp == desired_timestamp);
            break;
        case 2:
            BOOST_REQUIRE(col_name.size() == 7 && col_name[0] == 0 &&
                    col_name[1] == 4 && col_name[2] == 'c' &&
                    col_name[3] == 'o' && col_name[4] == 'l' &&
                    col_name[5] == '2' && col_name[6] == '\0');
            BOOST_REQUIRE(value.size() == 4 && value[0] == 0 &&
                    value[1] == 0 && value[2] == 0 && value[3] == 3);
            BOOST_REQUIRE(timestamp == desired_timestamp);
            break;
        }
        count_cell++;
    }
    virtual void consume_range_tombstone(
            bytes_view start_col, bytes_view end_col,
            sstables::deletion_time deltime) override {
        count_range_tombstone++;
    }
    virtual void consume_row_end() override {
        count_row_end++;
    }
};

SEASTAR_TEST_CASE(uncompressed_row_read_at_once) {
    return reusable_sst("tests/urchin/sstables/uncompressed", 1).then([] (auto sstp) {
        return do_with(test_row_consumer(1418656871665302), [sstp] (auto& c) {
            return sstp->data_consume_rows_at_once(c, 0, 95).then([sstp, &c] {
                BOOST_REQUIRE(c.count_row_start == 1);
                BOOST_REQUIRE(c.count_cell == 3);
                BOOST_REQUIRE(c.count_row_end == 1);
                BOOST_REQUIRE(c.count_range_tombstone == 0);
                return make_ready_future<>();
            });
        });
    });
}

SEASTAR_TEST_CASE(compressed_row_read_at_once) {
    return reusable_sst("tests/urchin/sstables/compressed", 1).then([] (auto sstp) {
        return do_with(test_row_consumer(1418654707438005), [sstp] (auto& c) {
            return sstp->data_consume_rows_at_once(c, 0, 95).then([sstp, &c] {
                BOOST_REQUIRE(c.count_row_start == 1);
                BOOST_REQUIRE(c.count_cell == 3);
                BOOST_REQUIRE(c.count_row_end == 1);
                BOOST_REQUIRE(c.count_range_tombstone == 0);
                return make_ready_future<>();
            });
        });
    });
}

SEASTAR_TEST_CASE(uncompressed_rows_read_one) {
    return reusable_sst("tests/urchin/sstables/uncompressed", 1).then([] (auto sstp) {
        return do_with(test_row_consumer(1418656871665302), [sstp] (auto& c) {
            return sstp->data_consume_rows(c, 0, 95).then([sstp, &c] {
                BOOST_REQUIRE(c.count_row_start == 1);
                BOOST_REQUIRE(c.count_cell == 3);
                BOOST_REQUIRE(c.count_row_end == 1);
                BOOST_REQUIRE(c.count_range_tombstone == 0);
                return make_ready_future<>();
            });
        });
    });
}

SEASTAR_TEST_CASE(compressed_rows_read_one) {
    return reusable_sst("tests/urchin/sstables/compressed", 1).then([] (auto sstp) {
        return do_with(test_row_consumer(1418654707438005), [sstp] (auto& c) {
            return sstp->data_consume_rows(c, 0, 95).then([sstp, &c] {
                BOOST_REQUIRE(c.count_row_start == 1);
                BOOST_REQUIRE(c.count_cell == 3);
                BOOST_REQUIRE(c.count_row_end == 1);
                BOOST_REQUIRE(c.count_range_tombstone == 0);
                return make_ready_future<>();
            });
        });
    });
}

// Tests for iterating over all rows.

class count_row_consumer : public row_consumer {
public:
    int count_row_start = 0;
    int count_cell = 0;
    int count_row_end = 0;
    int count_range_tombstone = 0;
    virtual void consume_row_start(bytes_view key, sstables::deletion_time deltime) override {
        count_row_start++;
    }
    virtual void consume_cell(bytes_view col_name, bytes_view value, uint64_t timestamp) override {
        count_cell++;
    }
    virtual void consume_row_end() override {
        count_row_end++;
    }
    virtual void consume_range_tombstone(
            bytes_view start_col, bytes_view end_col,
            sstables::deletion_time deltime) override {
        count_range_tombstone++;
    }

};


SEASTAR_TEST_CASE(uncompressed_rows_read_all) {
    return reusable_sst("tests/urchin/sstables/uncompressed", 1).then([] (auto sstp) {
        return do_with(count_row_consumer(), [sstp] (auto& c) {
            return sstp->data_consume_rows(c).then([sstp, &c] {
                BOOST_REQUIRE(c.count_row_start == 4);
                BOOST_REQUIRE(c.count_row_end == 4);
                BOOST_REQUIRE(c.count_cell == 4*3);
                BOOST_REQUIRE(c.count_range_tombstone == 0);
                return make_ready_future<>();
            });
        });
    });
}

SEASTAR_TEST_CASE(compressed_rows_read_all) {
    return reusable_sst("tests/urchin/sstables/compressed", 1).then([] (auto sstp) {
        return do_with(count_row_consumer(), [sstp] (auto& c) {
            return sstp->data_consume_rows(c).then([sstp, &c] {
                BOOST_REQUIRE(c.count_row_start == 4);
                BOOST_REQUIRE(c.count_row_end == 4);
                BOOST_REQUIRE(c.count_cell == 4*3);
                BOOST_REQUIRE(c.count_range_tombstone == 0);
                return make_ready_future<>();
            });
        });
    });
}

// Test reading range tombstone (which we we have in collections such as set)
class set_consumer : public count_row_consumer {
public:
    virtual void consume_range_tombstone(
            bytes_view start_col, bytes_view end_col,
            sstables::deletion_time deltime) override {
        count_row_consumer::consume_range_tombstone(start_col, end_col, deltime);
        // Note the unique end-of-component markers -1 and 1, specifying a
        // range between start and end of row.
        BOOST_REQUIRE(start_col == bytes({0, 9, 'f', 'a', 'v', 'o', 'r', 'i', 't', 'e', 's', -1}));
        BOOST_REQUIRE(end_col == as_bytes({0, 9, 'f', 'a', 'v', 'o', 'r', 'i', 't', 'e', 's', 1}));
        // Note the range tombstone have an interesting, not default, deltime.
        BOOST_REQUIRE(deltime.local_deletion_time == 1297062948U);
        BOOST_REQUIRE(deltime.marked_for_delete_at == 1428855312063525UL);
    }
};

SEASTAR_TEST_CASE(read_set) {
    return reusable_sst("tests/urchin/sstables/set", 1).then([] (auto sstp) {
        return do_with(set_consumer(), [sstp] (auto& c) {
            return sstp->data_consume_rows(c).then([sstp, &c] {
                BOOST_REQUIRE(c.count_row_start == 1);
                BOOST_REQUIRE(c.count_row_end == 1);
                BOOST_REQUIRE(c.count_cell == 3);
                BOOST_REQUIRE(c.count_range_tombstone == 1);
               return make_ready_future<>();
            });
        });
    });
}
