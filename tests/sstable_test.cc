/*
 * Copyright 2015 Cloudius Systems
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

#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>

#include "core/sstring.hh"
#include "core/future-util.hh"
#include "core/align.hh"
#include "core/do_with.hh"
#include "core/sleep.hh"
#include "sstables/sstables.hh"
#include "sstables/key.hh"
#include "tests/test-utils.hh"
#include "schema.hh"
#include "compress.hh"
#include "database.hh"
#include <memory>
#include "sstable_test.hh"
#include "tmpdir.hh"

using namespace sstables;

bytes as_bytes(const sstring& s) {
    return { reinterpret_cast<const int8_t*>(s.begin()), s.size() };
}

static future<> broken_sst(sstring dir, unsigned long generation) {

    auto sst = std::make_unique<sstable>("ks", "cf", dir, generation, la, big);
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

SEASTAR_TEST_CASE(empty_toc) {
    return broken_sst("tests/sstables/badtoc", 1);
}

SEASTAR_TEST_CASE(alien_toc) {
    return broken_sst("tests/sstables/badtoc", 2);
}

SEASTAR_TEST_CASE(truncated_toc) {
    return broken_sst("tests/sstables/badtoc", 3);
}

SEASTAR_TEST_CASE(wrong_format_toc) {
    return broken_sst("tests/sstables/badtoc", 4);
}

SEASTAR_TEST_CASE(compression_truncated) {
    return broken_sst("tests/sstables/badcompression", 1);
}

SEASTAR_TEST_CASE(compression_bytes_flipped) {
    return broken_sst("tests/sstables/badcompression", 2);
}

SEASTAR_TEST_CASE(uncompressed_data) {
    return working_sst("tests/sstables/uncompressed", 1);
}

SEASTAR_TEST_CASE(compressed_data) {
    return working_sst("tests/sstables/compressed", 1);
}

SEASTAR_TEST_CASE(composite_index) {
    return working_sst("tests/sstables/composite", 1);
}

template<uint64_t SummaryIdx, uint64_t Expected>
future<> index_read(sstring path) {
    return reusable_sst(path, 1).then([] (sstable_ptr ptr) {
        return sstables::test(ptr).read_indexes(SummaryIdx).then([ptr] (auto vec) {
            BOOST_REQUIRE(vec.size() == Expected);
            return make_ready_future<>();
        });
    });
}

template<uint64_t SummaryIdx, uint64_t Expected>
future<> simple_index_read() {
    return index_read<SummaryIdx, Expected>("tests/sstables/uncompressed");
}

template<uint64_t SummaryIdx, uint64_t Expected>
future<> composite_index_read() {
    return index_read<SummaryIdx, Expected>("tests/sstables/composite");
}

SEASTAR_TEST_CASE(simple_index_read_0_4) {
    return simple_index_read<0, 4>();
}

SEASTAR_TEST_CASE(simple_index_read_1_0) {
    return simple_index_read<1, 0>();
}

SEASTAR_TEST_CASE(composite_index_read_0_20) {
    return composite_index_read<0, 20>();
}

template<uint64_t Position, uint64_t EntryPosition, uint64_t EntryKeySize>
future<> summary_query(sstring path, int generation) {
    return reusable_sst(path, generation).then([] (sstable_ptr ptr) {
        return sstables::test(ptr).read_summary_entry(Position).then([ptr] (auto entry) {
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
    return summary_query<0, 0, 5>("tests/sstables/uncompressed", 1);
}

SEASTAR_TEST_CASE(small_summary_query_fail) {
    return summary_query_fail<2, 0, 5>("tests/sstables/uncompressed", 1);
}

SEASTAR_TEST_CASE(small_summary_query_negative_fail) {
    return summary_query_fail<-2, 0, 5>("tests/sstables/uncompressed", 1);
}

SEASTAR_TEST_CASE(big_summary_query_0) {
    return summary_query<0, 0, 182>("tests/sstables/bigsummary", 76);
}

SEASTAR_TEST_CASE(big_summary_query_32) {
    return summary_query<32, 0xc4000, 182>("tests/sstables/bigsummary", 76);
}

static future<sstable_ptr> do_write_sst(sstring load_dir, sstring write_dir, unsigned long generation) {
    auto sst = make_lw_shared<sstable>("ks", "cf", load_dir, generation, la, big);
    return sst->load().then([sst, write_dir, generation] {
        sstables::test(sst).change_generation_number(generation + 1);
        sstables::test(sst).change_dir(write_dir);
        auto fut = sstables::test(sst).store();
        return std::move(fut).then([sst = std::move(sst)] {
            return make_ready_future<sstable_ptr>(std::move(sst));
        });
    });
}

static future<> write_sst_info(sstring load_dir, sstring write_dir, unsigned long generation) {
    return do_write_sst(load_dir, write_dir, generation).then([] (auto ptr) { return make_ready_future<>(); });
}

using bufptr_t = std::unique_ptr<char [], free_deleter>;
static future<std::pair<bufptr_t, size_t>> read_file(sstring file_path)
{
    return open_file_dma(file_path, open_flags::rw).then([] (file f) {
        return f.size().then([f] (auto size) mutable {
            auto aligned_size = align_up(size, 512UL);
            auto buf = allocate_aligned_buffer<char>(aligned_size, 512UL);
            auto rbuf = buf.get();
            ::memset(rbuf, 0, aligned_size);
            return f.dma_read(0, rbuf, aligned_size).then([size, buf = std::move(buf), f] (auto ret) mutable {
                BOOST_REQUIRE(ret == size);
                std::pair<bufptr_t, size_t> p(std::move(buf), std::move(size));
                return make_ready_future<std::pair<bufptr_t, size_t>>(std::move(p));
            }).finally([f] () mutable { return f.close().finally([f] {}); });
        });
    });
}

struct sstdesc {
    sstring dir;
    int64_t gen;
};

static future<> compare_files(sstdesc file1, sstdesc file2, sstable::component_type component) {
    auto file_path = sstable::filename(file1.dir, "ks", "cf", la, file1.gen, big, component);
    return read_file(file_path).then([component, file2] (auto ret) {
        auto file_path = sstable::filename(file2.dir, "ks", "cf", la, file2.gen, big, component);
        return read_file(file_path).then([ret = std::move(ret)] (auto ret2) {
            // assert that both files have the same size.
            BOOST_REQUIRE(ret.second == ret2.second);
            // assert that both files have the same content.
            BOOST_REQUIRE(::memcmp(ret.first.get(), ret2.first.get(), ret.second) == 0);
            // free buf from both files.
        });
    });

}

static future<> check_component_integrity(sstable::component_type component) {
    auto tmp = make_lw_shared<tmpdir>();
    return write_sst_info("tests/sstables/compressed", tmp->path, 1).then([component, tmp] {
        return compare_files(sstdesc{"tests/sstables/compressed", 1 },
                             sstdesc{tmp->path, 2 },
                             component);
    }).then([tmp] {});
}

SEASTAR_TEST_CASE(check_compressed_info_func) {
    return check_component_integrity(sstable::component_type::CompressionInfo);
}

SEASTAR_TEST_CASE(check_summary_func) {
    auto tmp = make_lw_shared<tmpdir>();
    return do_write_sst("tests/sstables/compressed", tmp->path, 1).then([tmp] (auto sst1) {
        auto sst2 = make_lw_shared<sstable>("ks", "cf", tmp->path, 2, la, big);
        return sstables::test(sst2).read_summary().then([sst1, sst2] {
            summary& sst1_s = sstables::test(sst1).get_summary();
            summary& sst2_s = sstables::test(sst2).get_summary();

            BOOST_REQUIRE(::memcmp(&sst1_s.header, &sst2_s.header, sizeof(summary::header)) == 0);
            BOOST_REQUIRE(sst1_s.positions == sst2_s.positions);
            BOOST_REQUIRE(sst1_s.entries == sst2_s.entries);
            BOOST_REQUIRE(sst1_s.first_key.value == sst2_s.first_key.value);
            BOOST_REQUIRE(sst1_s.last_key.value == sst2_s.last_key.value);
        });
    }).then([tmp] {});
}

SEASTAR_TEST_CASE(check_filter_func) {
    return check_component_integrity(sstable::component_type::Filter);
}

SEASTAR_TEST_CASE(check_statistics_func) {
    auto tmp = make_lw_shared<tmpdir>();
    return do_write_sst("tests/sstables/compressed", tmp->path, 1).then([tmp] (auto sst1) {
        auto sst2 = make_lw_shared<sstable>("ks", "cf", tmp->path, 2, la, big);
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
    }).then([tmp] {});
}

SEASTAR_TEST_CASE(check_toc_func) {
    auto tmp = make_lw_shared<tmpdir>();
    return do_write_sst("tests/sstables/compressed", tmp->path, 1).then([tmp] (auto sst1) {
        auto sst2 = make_lw_shared<sstable>("ks", "cf", tmp->path, 2, la, big);
        return sstables::test(sst2).read_toc().then([sst1, sst2] {
            auto& sst1_c = sstables::test(sst1).get_components();
            auto& sst2_c = sstables::test(sst2).get_components();

            BOOST_REQUIRE(sst1_c == sst2_c);
        });
    }).then([tmp] {});
}

SEASTAR_TEST_CASE(uncompressed_random_access_read) {
    return reusable_sst("tests/sstables/uncompressed", 1).then([] (auto sstp) {
        // note: it's important to pass on a shared copy of sstp to prevent its
        // destruction until the continuation finishes reading!
        return sstables::test(sstp).data_read(97, 6).then([sstp] (temporary_buffer<char> buf) {
            BOOST_REQUIRE(sstring(buf.get(), buf.size()) == "gustaf");
            return make_ready_future<>();
        });
    });
}

SEASTAR_TEST_CASE(compressed_random_access_read) {
    return reusable_sst("tests/sstables/compressed", 1).then([] (auto sstp) {
        return sstables::test(sstp).data_read(97, 6).then([sstp] (temporary_buffer<char> buf) {
            BOOST_REQUIRE(sstring(buf.get(), buf.size()) == "gustaf");
            return make_ready_future<>();
        });
    });
}

class test_row_consumer : public row_consumer {
public:
    const int64_t desired_timestamp;
    test_row_consumer(int64_t t) : desired_timestamp(t) { }
    int count_row_start = 0;
    int count_cell = 0;
    int count_deleted_cell = 0;
    int count_range_tombstone = 0;
    int count_row_end = 0;
    virtual void consume_row_start(sstables::key_view key, sstables::deletion_time deltime) override {
        BOOST_REQUIRE(bytes_view(key) == as_bytes("vinna"));
        BOOST_REQUIRE(deltime.local_deletion_time == std::numeric_limits<int32_t>::max());
        BOOST_REQUIRE(deltime.marked_for_delete_at == std::numeric_limits<int64_t>::min());
        count_row_start++;
    }

    virtual void consume_cell(bytes_view col_name, bytes_view value,
            int64_t timestamp, int32_t ttl, int32_t expiration) override {
        BOOST_REQUIRE(ttl == 0);
        BOOST_REQUIRE(expiration == 0);
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

    virtual void consume_deleted_cell(bytes_view col_name, sstables::deletion_time deltime) override {
        count_deleted_cell++;
    }

    virtual void consume_range_tombstone(
            bytes_view start_col, bytes_view end_col,
            sstables::deletion_time deltime) override {
        count_range_tombstone++;
    }
    virtual proceed consume_row_end() override {
        count_row_end++;
        return proceed::yes;
    }
    virtual const io_priority_class& io_priority() override {
        return default_priority_class();
    }
};

SEASTAR_TEST_CASE(uncompressed_row_read_at_once) {
    return reusable_sst("tests/sstables/uncompressed", 1).then([] (auto sstp) {
        return do_with(test_row_consumer(1418656871665302), [sstp] (auto& c) {
            return sstp->data_consume_rows_at_once(c, 0, 95).then([sstp, &c] {
                BOOST_REQUIRE(c.count_row_start == 1);
                BOOST_REQUIRE(c.count_cell == 3);
                BOOST_REQUIRE(c.count_deleted_cell == 0);
                BOOST_REQUIRE(c.count_row_end == 1);
                BOOST_REQUIRE(c.count_range_tombstone == 0);
                return make_ready_future<>();
            });
        });
    });
}

SEASTAR_TEST_CASE(compressed_row_read_at_once) {
    return reusable_sst("tests/sstables/compressed", 1).then([] (auto sstp) {
        return do_with(test_row_consumer(1418654707438005), [sstp] (auto& c) {
            return sstp->data_consume_rows_at_once(c, 0, 95).then([sstp, &c] {
                BOOST_REQUIRE(c.count_row_start == 1);
                BOOST_REQUIRE(c.count_cell == 3);
                BOOST_REQUIRE(c.count_deleted_cell == 0);
                BOOST_REQUIRE(c.count_row_end == 1);
                BOOST_REQUIRE(c.count_range_tombstone == 0);
                return make_ready_future<>();
            });
        });
    });
}

SEASTAR_TEST_CASE(uncompressed_rows_read_one) {
    return reusable_sst("tests/sstables/uncompressed", 1).then([] (auto sstp) {
        return do_with(test_row_consumer(1418656871665302), [sstp] (auto& c) {
            auto context = sstp->data_consume_rows(c, 0, 95);
            auto fut = context.read();
            return fut.then([sstp, &c, context = std::move(context)] {
                BOOST_REQUIRE(c.count_row_start == 1);
                BOOST_REQUIRE(c.count_cell == 3);
                BOOST_REQUIRE(c.count_deleted_cell == 0);
                BOOST_REQUIRE(c.count_row_end == 1);
                BOOST_REQUIRE(c.count_range_tombstone == 0);
                return make_ready_future<>();
            });
        });
    });
}

SEASTAR_TEST_CASE(compressed_rows_read_one) {
    return reusable_sst("tests/sstables/compressed", 1).then([] (auto sstp) {
        return do_with(test_row_consumer(1418654707438005), [sstp] (auto& c) {
            auto context = sstp->data_consume_rows(c, 0, 95);
            auto fut = context.read();
            return fut.then([sstp, &c, context = std::move(context)] {
                BOOST_REQUIRE(c.count_row_start == 1);
                BOOST_REQUIRE(c.count_cell == 3);
                BOOST_REQUIRE(c.count_deleted_cell == 0);
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
    int count_deleted_cell = 0;
    int count_row_end = 0;
    int count_range_tombstone = 0;
    virtual void consume_row_start(sstables::key_view key, sstables::deletion_time deltime) override {
        count_row_start++;
    }
    virtual void consume_cell(bytes_view col_name, bytes_view value,
            int64_t timestamp, int32_t ttl, int32_t expiration) override {
        count_cell++;
    }
    virtual void consume_deleted_cell(bytes_view col_name, sstables::deletion_time deltime) override {
        count_deleted_cell++;
    }
    virtual proceed consume_row_end() override {
        count_row_end++;
        return proceed::yes;
    }
    virtual void consume_range_tombstone(
            bytes_view start_col, bytes_view end_col,
            sstables::deletion_time deltime) override {
        count_range_tombstone++;
    }
    virtual const io_priority_class& io_priority() override {
        return default_priority_class();
    }
};


SEASTAR_TEST_CASE(uncompressed_rows_read_all) {
    return reusable_sst("tests/sstables/uncompressed", 1).then([] (auto sstp) {
        return do_with(count_row_consumer(), [sstp] (auto& c) {
            auto context = sstp->data_consume_rows(c);
            auto fut = context.read();
            return fut.then([sstp, &c, context = std::move(context)] {
                BOOST_REQUIRE(c.count_row_start == 4);
                BOOST_REQUIRE(c.count_row_end == 4);
                BOOST_REQUIRE(c.count_cell == 4*3);
                BOOST_REQUIRE(c.count_deleted_cell == 0);
                BOOST_REQUIRE(c.count_range_tombstone == 0);
                return make_ready_future<>();
            });
        });
    });
}

SEASTAR_TEST_CASE(compressed_rows_read_all) {
    return reusable_sst("tests/sstables/compressed", 1).then([] (auto sstp) {
        return do_with(count_row_consumer(), [sstp] (auto& c) {
            auto context = sstp->data_consume_rows(c);
            auto fut = context.read();
            return fut.then([sstp, &c, context = std::move(context)] {
                BOOST_REQUIRE(c.count_row_start == 4);
                BOOST_REQUIRE(c.count_row_end == 4);
                BOOST_REQUIRE(c.count_cell == 4*3);
                BOOST_REQUIRE(c.count_deleted_cell == 0);
                BOOST_REQUIRE(c.count_range_tombstone == 0);
                return make_ready_future<>();
            });
        });
    });
}

// test reading all the rows one by one, using the feature of the
// consume_row_end returning proceed::no message
class pausable_count_row_consumer : public count_row_consumer {
public:
    virtual proceed consume_row_end() override {
        count_row_consumer::consume_row_end();
        return proceed::no;
    }
};

SEASTAR_TEST_CASE(pausable_uncompressed_rows_read_all) {
    return reusable_sst("tests/sstables/uncompressed", 1).then([] (auto sstp) {
        return do_with(pausable_count_row_consumer(), [sstp] (auto& c) {
            auto context = sstp->data_consume_rows(c);
            auto fut = context.read();
            return fut.then([sstp, &c, context = std::move(context)] () mutable {
                // After one read, we only get one row
                BOOST_REQUIRE(c.count_row_start == 1);
                BOOST_REQUIRE(c.count_row_end == 1);
                BOOST_REQUIRE(c.count_cell == 1*3);
                BOOST_REQUIRE(c.count_deleted_cell == 0);
                BOOST_REQUIRE(c.count_range_tombstone == 0);
                auto fut = context.read();
                return fut.then([&c, context = std::move(context)] () mutable {
                    // After two reads
                    BOOST_REQUIRE(c.count_row_start == 2);
                    BOOST_REQUIRE(c.count_row_end == 2);
                    BOOST_REQUIRE(c.count_cell == 2*3);
                    BOOST_REQUIRE(c.count_deleted_cell == 0);
                    BOOST_REQUIRE(c.count_range_tombstone == 0);
                    return make_ready_future<>();
                    // FIXME: read until the last row.
                });
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
        BOOST_REQUIRE(deltime.local_deletion_time == 1428855312U);
        BOOST_REQUIRE(deltime.marked_for_delete_at == 1428855312063524UL);
    }
};

SEASTAR_TEST_CASE(read_set) {
    return reusable_sst("tests/sstables/set", 1).then([] (auto sstp) {
        return do_with(set_consumer(), [sstp] (auto& c) {
            auto context = sstp->data_consume_rows(c);
            auto fut = context.read();
            return fut.then([sstp, &c, context = std::move(context)] {
                BOOST_REQUIRE(c.count_row_start == 1);
                BOOST_REQUIRE(c.count_row_end == 1);
                BOOST_REQUIRE(c.count_cell == 3);
                BOOST_REQUIRE(c.count_deleted_cell == 0);
                BOOST_REQUIRE(c.count_range_tombstone == 1);
               return make_ready_future<>();
            });
        });
    });
}

class ttl_row_consumer : public count_row_consumer {
public:
    const int64_t desired_timestamp;
    ttl_row_consumer(int64_t t) : desired_timestamp(t) { }
    virtual void consume_row_start(sstables::key_view key, sstables::deletion_time deltime) override {
        count_row_consumer::consume_row_start(key, deltime);
        BOOST_REQUIRE(bytes_view(key) == as_bytes("nadav"));
        BOOST_REQUIRE(deltime.local_deletion_time == std::numeric_limits<int32_t>::max());
        BOOST_REQUIRE(deltime.marked_for_delete_at == std::numeric_limits<int64_t>::min());
    }

    virtual void consume_cell(bytes_view col_name, bytes_view value,
            int64_t timestamp, int32_t ttl, int32_t expiration) override {
        switch (count_cell) {
        case 0:
            // The silly "cql row marker" cell
            BOOST_REQUIRE(col_name.size() == 3 && col_name[0] == 0 && col_name[1] == 0 && col_name[2] == 0);
            BOOST_REQUIRE(value.size() == 0);
            BOOST_REQUIRE(timestamp == desired_timestamp);
            BOOST_REQUIRE(ttl == 3600);
            BOOST_REQUIRE(expiration == 1430154618);
            break;
        case 1:
            BOOST_REQUIRE(col_name.size() == 6 && col_name[0] == 0 &&
                    col_name[1] == 3 && col_name[2] == 'a' &&
                    col_name[3] == 'g' && col_name[4] == 'e' &&
                    col_name[5] == '\0');
            BOOST_REQUIRE(value.size() == 4 && value[0] == 0 && value[1] == 0
                    && value[2] == 0 && value[3] == 40);
            BOOST_REQUIRE(timestamp == desired_timestamp);
            BOOST_REQUIRE(ttl == 3600);
            BOOST_REQUIRE(expiration == 1430154618);
            break;
        }
        count_row_consumer::consume_cell(col_name, value, timestamp, ttl, expiration);
    }
};

SEASTAR_TEST_CASE(ttl_read) {
    return reusable_sst("tests/sstables/ttl", 1).then([] (auto sstp) {
        return do_with(ttl_row_consumer(1430151018675502), [sstp] (auto& c) {
            auto context = sstp->data_consume_rows(c);
            auto fut = context.read();
            return fut.then([sstp, &c, context = std::move(context)] {
                BOOST_REQUIRE(c.count_row_start == 1);
                BOOST_REQUIRE(c.count_cell == 2);
                BOOST_REQUIRE(c.count_deleted_cell == 0);
                BOOST_REQUIRE(c.count_row_end == 1);
                BOOST_REQUIRE(c.count_range_tombstone == 0);
                return make_ready_future<>();
            });
        });
    });
}

class deleted_cell_row_consumer : public count_row_consumer {
public:
    virtual void consume_row_start(sstables::key_view key, sstables::deletion_time deltime) override {
        count_row_consumer::consume_row_start(key, deltime);
        BOOST_REQUIRE(bytes_view(key) == as_bytes("nadav"));
        BOOST_REQUIRE(deltime.local_deletion_time == std::numeric_limits<int32_t>::max());
        BOOST_REQUIRE(deltime.marked_for_delete_at == std::numeric_limits<int64_t>::min());
    }

    virtual void consume_deleted_cell(bytes_view col_name, sstables::deletion_time deltime) override {
        count_row_consumer::consume_deleted_cell(col_name, deltime);
        BOOST_REQUIRE(col_name.size() == 6 && col_name[0] == 0 &&
                col_name[1] == 3 && col_name[2] == 'a' &&
                col_name[3] == 'g' && col_name[4] == 'e' &&
                col_name[5] == '\0');
        BOOST_REQUIRE(deltime.local_deletion_time == 1430200516);
        BOOST_REQUIRE(deltime.marked_for_delete_at == 1430200516937621UL);
    }
};

SEASTAR_TEST_CASE(deleted_cell_read) {
    return reusable_sst("tests/sstables/deleted_cell", 2).then([] (auto sstp) {
        return do_with(deleted_cell_row_consumer(), [sstp] (auto& c) {
            auto context = sstp->data_consume_rows(c);
            auto fut = context.read();
            return fut.then([sstp, &c, context = std::move(context)] {
                BOOST_REQUIRE(c.count_row_start == 1);
                BOOST_REQUIRE(c.count_cell == 0);
                BOOST_REQUIRE(c.count_deleted_cell == 1);
                BOOST_REQUIRE(c.count_row_end == 1);
                BOOST_REQUIRE(c.count_range_tombstone == 0);
                return make_ready_future<>();
            });
        });
    });
}


SEASTAR_TEST_CASE(find_key_map) {
    return reusable_sst("tests/sstables/map_pk", 1).then([] (auto sstp) {
        schema_ptr s = map_schema();
        auto& summary = sstables::test(sstp)._summary();
        std::vector<data_value> kk;

        auto b1 = to_bytes("2");
        auto b2 = to_bytes("2");

        auto map_type = map_type_impl::get_instance(bytes_type, bytes_type, true);
        auto map_element = std::make_pair<data_value, data_value>(data_value(b1), data_value(b2));
        std::vector<std::pair<data_value, data_value>> map;
        map.push_back(map_element);

        kk.push_back(make_map_value(map_type, map));

        auto key = sstables::key::from_deeply_exploded(*s, kk);
        BOOST_REQUIRE(sstables::test(sstp).binary_search(summary.entries, key) == 0);
    });
}

SEASTAR_TEST_CASE(find_key_set) {
    return reusable_sst("tests/sstables/set_pk", 1).then([] (auto sstp) {
        schema_ptr s = set_schema();
        auto& summary = sstables::test(sstp)._summary();
        std::vector<data_value> kk;

        std::vector<data_value> set;

        bytes b1("1");
        bytes b2("2");

        set.push_back(data_value(b1));
        set.push_back(data_value(b2));
        auto set_type = set_type_impl::get_instance(bytes_type, true);
        kk.push_back(make_set_value(set_type, set));

        auto key = sstables::key::from_deeply_exploded(*s, kk);
        BOOST_REQUIRE(sstables::test(sstp).binary_search(summary.entries, key) == 0);
    });
}

SEASTAR_TEST_CASE(find_key_list) {
    return reusable_sst("tests/sstables/list_pk", 1).then([] (auto sstp) {
        schema_ptr s = set_schema();
        auto& summary = sstables::test(sstp)._summary();
        std::vector<data_value> kk;

        std::vector<data_value> list;

        bytes b1("1");
        bytes b2("2");
        list.push_back(data_value(b1));
        list.push_back(data_value(b2));

        auto list_type = list_type_impl::get_instance(bytes_type, true);
        kk.push_back(make_list_value(list_type, list));

        auto key = sstables::key::from_deeply_exploded(*s, kk);
        BOOST_REQUIRE(sstables::test(sstp).binary_search(summary.entries, key) == 0);
    });
}


SEASTAR_TEST_CASE(find_key_composite) {
    return reusable_sst("tests/sstables/composite", 1).then([] (auto sstp) {
        schema_ptr s = composite_schema();
        auto& summary = sstables::test(sstp)._summary();
        std::vector<data_value> kk;

        auto b1 = bytes("HCG8Ee7ENWqfCXipk4-Ygi2hzrbfHC8pTtH3tEmV3d9p2w8gJPuMN_-wp1ejLRf4kNEPEgtgdHXa6NoFE7qUig==");
        auto b2 = bytes("VJizqYxC35YpLaPEJNt_4vhbmKJxAg54xbiF1UkL_9KQkqghVvq34rZ6Lm8eRTi7JNJCXcH6-WtNUSFJXCOfdg==");

        kk.push_back(data_value(b1));
        kk.push_back(data_value(b2));

        auto key = sstables::key::from_deeply_exploded(*s, kk);
        BOOST_REQUIRE(sstables::test(sstp).binary_search(summary.entries, key) == 0);
    });
}

SEASTAR_TEST_CASE(all_in_place) {
    return reusable_sst("tests/sstables/bigsummary", 76).then([] (auto sstp) {
        auto& summary = sstables::test(sstp)._summary();

        int idx = 0;
        for (auto& e: summary.entries) {
            auto key = sstables::key::from_bytes(e.key);
            BOOST_REQUIRE(sstables::test(sstp).binary_search(summary.entries, key) == idx++);
        }
    });
}

SEASTAR_TEST_CASE(full_index_search) {
    return reusable_sst("tests/sstables/uncompressed", 1).then([] (auto sstp) {
        return sstables::test(sstp).read_indexes(0).then([sstp] (auto index_list) {
            int idx = 0;
            for (auto& ie: index_list) {
                auto key = key::from_bytes(to_bytes(ie.get_key_bytes()));
                BOOST_REQUIRE(sstables::test(sstp).binary_search(index_list, key) == idx++);
            }
        });
    });
}

SEASTAR_TEST_CASE(not_find_key_composite_bucket0) {
    return reusable_sst("tests/sstables/composite", 1).then([] (auto sstp) {
        schema_ptr s = composite_schema();
        auto& summary = sstables::test(sstp)._summary();
        std::vector<data_value> kk;

        auto b1 = bytes("ZEunFCoqAidHOrPiU3U6UAvUU01IYGvT3kYtYItJ1ODTk7FOsEAD-dqmzmFNfTDYvngzkZwKrLxthB7ItLZ4HQ==");
        auto b2 = bytes("K-GpWx-QtyzLb12z5oNS0C03d3OzNyBKdYJh1XjHiC53KudoqdoFutHUMFLe6H9Emqv_fhwIJEKEb5Csn72f9A==");

        kk.push_back(data_value(b1));
        kk.push_back(data_value(b2));

        auto key = sstables::key::from_deeply_exploded(*s, kk);
        // (result + 1) * -1 -1 = 0
        BOOST_REQUIRE(sstables::test(sstp).binary_search(summary.entries, key) == -2);
    });
}

// See CASSANDRA-7593. This sstable writes 0 in the range_start. We need to handle that case as well
SEASTAR_TEST_CASE(wrong_range) {
    return reusable_sst("tests/sstables/wrongrange", 114).then([] (auto sstp) {
        return do_with(sstables::key("todata"), [sstp] (auto& key) {
            auto s = columns_schema();
            return sstp->read_row(s, key).then([sstp, s, &key] (auto mutation) {
                return make_ready_future<>();
            });
        });
    });
}

static future<>
test_sstable_exists(sstring dir, unsigned long generation, bool exists) {
    auto file_path = sstable::filename(dir, "ks", "cf", la, generation, big, sstable::component_type::Data);
    return open_file_dma(file_path, open_flags::ro).then_wrapped([exists] (future<file> f) {
        if (exists) {
            BOOST_CHECK_NO_THROW(f.get0());
        } else {
            BOOST_REQUIRE_THROW(f.get0(), std::system_error);
        }
        return make_ready_future<>();
    });
}

// We need to be careful not to allow failures in this test to contaminate subsequent runs.
// We will therefore run it in an empty directory, and first link a known SSTable from another
// directory to it.
SEASTAR_TEST_CASE(set_generation) {
    return test_setup::do_with_test_directory([] {
        return reusable_sst("tests/sstables/uncompressed", 1).then([] (auto sstp) {
            return sstp->create_links("tests/sstables/generation").then([sstp] {});
        }).then([] {
            return reusable_sst("tests/sstables/generation", 1).then([] (auto sstp) {
                return sstp->set_generation(2).then([sstp] {});
            });
        }).then([] {
            return test_sstable_exists("tests/sstables/generation", 1, false);
        }).then([] {
            return compare_files(sstdesc{"tests/sstables/uncompressed", 1 },
                                 sstdesc{"tests/sstables/generation", 2 },
                                 sstable::component_type::Data);
        });
    }, "tests/sstables/generation");
}

SEASTAR_TEST_CASE(reshuffle) {
    return test_setup::do_with_test_directory([] {
        return reusable_sst("tests/sstables/uncompressed", 1).then([] (auto sstp) {
            return sstp->create_links("tests/sstables/generation", 1).then([sstp] {
                return sstp->create_links("tests/sstables/generation", 5).then([sstp] {
                    return sstp->create_links("tests/sstables/generation", 10);
                });
            }).then([sstp] {});
        }).then([] {
            auto cm = make_lw_shared<compaction_manager>();
            cm->start(2); // starting two task handlers.

            column_family::config cfg;
            cfg.datadir = "tests/sstables/generation";
            cfg.enable_commitlog = false;
            cfg.enable_incremental_backups = false;
            auto cf = make_lw_shared<column_family>(uncompressed_schema(), cfg, column_family::no_commitlog(), *cm);
            cf->start();
            return cf->reshuffle_sstables(3).then([cm, cf] (std::vector<sstables::entry_descriptor> reshuffled) {
                BOOST_REQUIRE(reshuffled.size() == 2);
                BOOST_REQUIRE(reshuffled[0].generation  == 3);
                BOOST_REQUIRE(reshuffled[1].generation  == 4);
                return when_all(
                    test_sstable_exists("tests/sstables/generation", 1, true),
                    test_sstable_exists("tests/sstables/generation", 2, false),
                    test_sstable_exists("tests/sstables/generation", 3, true),
                    test_sstable_exists("tests/sstables/generation", 4, true),
                    test_sstable_exists("tests/sstables/generation", 5, false),
                    test_sstable_exists("tests/sstables/generation", 10, false)
                ).discard_result().then([cm] {
                    return cm->stop();
                });
            }).then([cm, cf] {});
        });
    }, "tests/sstables/generation");
}
