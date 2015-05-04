/*
 * Copyright 2015 Cloudius Systems
 */

#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>

#include "core/sstring.hh"
#include "core/future-util.hh"
#include "core/align.hh"
#include "core/do_with.hh"
#include "sstables/sstables.hh"
#include "sstables/key.hh"
#include "tests/test-utils.hh"
#include "schema.hh"
#include "database.hh"
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

    summary& _summary() {
        return _sst->_summary;
    }

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

    future<summary_entry&> read_summary_entry(size_t i) {
        return _sst->read_summary_entry(i);
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

    template <typename T>
    int binary_search(const T& entries, const key& sk) {
        return _sst->binary_search(entries, sk);
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

static sstring some_keyspace("ks");
static sstring some_column_family("cf");

static atomic_cell make_atomic_cell(bytes value) {
    return atomic_cell::make_live(0, ttl_opt{}, std::move(value));
};

SEASTAR_TEST_CASE(datafile_generation_01) {
    // Data file with clustering key
    //
    // Respective CQL table and CQL insert:
    // CREATE TABLE test (
    //    p1 text,
    //    c1 text,
    //    r1 int,
    //    r2 int,
    //    PRIMARY KEY (p1, c1)
    //  ) WITH compression = {};
    // INSERT INTO test (p1, c1, r1) VALUES ('key1', 'abc', 1);

    auto s = make_lw_shared(schema({}, some_keyspace, some_column_family,
        {{"p1", utf8_type}}, {{"c1", utf8_type}}, {{"r1", int32_type}, {"r2", int32_type}}, {}, utf8_type));

    column_family cf(s);

    const column_definition& r1_col = *s->get_column_definition("r1");

    auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
    auto c_key = clustering_key::from_exploded(*s, {to_bytes("abc")});

    mutation m(key, s);
    m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type->decompose(1)));
    cf.apply(std::move(m));

    auto cfp = make_shared<column_family>(std::move(cf));

    return sstables::write_datafile(*cfp, "tests/urchin/sstables/Data.tmp.db").then([cfp, s] {
        return engine().open_file_dma("tests/urchin/sstables/Data.tmp.db", open_flags::ro).then([] (file f) {
            auto bufptr = allocate_aligned_buffer<char>(4096, 4096);

            auto fut = f.dma_read(0, bufptr.get(), 4096);
            return std::move(fut).then([f = std::move(f), bufptr = std::move(bufptr)] (size_t size) {
                auto buf = bufptr.get();
                size_t offset = 0;
                std::vector<uint8_t> key = { 0, 4, 'k', 'e', 'y', '1' };
                BOOST_REQUIRE(::memcmp(key.data(), &buf[offset], key.size()) == 0);
                offset += key.size();
                std::vector<uint8_t> deletion_time = { 0x7f, 0xff, 0xff, 0xff, 0x80, 0, 0, 0, 0, 0, 0, 0 };
                BOOST_REQUIRE(::memcmp(deletion_time.data(), &buf[offset], deletion_time.size()) == 0);
                offset += deletion_time.size();
                std::vector<uint8_t> row_mark = { /* name */ 0, 9, 0, 3, 'a', 'b', 'c', 0, 0, 0, 0 };
                // check if there is a row mark.
                if (::memcmp(row_mark.data(), &buf[offset], row_mark.size()) == 0) {
                    BOOST_REQUIRE(::memcmp(row_mark.data(), &buf[offset], row_mark.size()) == 0);
                    offset += row_mark.size();
                    offset += 13; // skip mask, timestamp and value = 13 bytes.
                }
                std::vector<uint8_t> regular_row = { /* name */ 0, 0xb, 0, 3, 'a', 'b', 'c', 0, 0, 2, 'r', '1', 0,
                    /* mask */ 0, /* timestamp */ 0, 0, 0, 0, 0, 0, 0, 0, /* value */ 0, 0, 0, 4, 0, 0, 0, 1 };
                BOOST_REQUIRE(::memcmp(regular_row.data(), &buf[offset], regular_row.size()) == 0);
                offset += regular_row.size();
                std::vector<uint8_t> end_of_row = { 0, 0 };
                BOOST_REQUIRE(::memcmp(end_of_row.data(), &buf[offset], end_of_row.size()) == 0);
                offset += end_of_row.size();
                BOOST_REQUIRE(size == offset);
            });
        }).then([] {
            return remove_file("tests/urchin/sstables/Data.tmp.db");
        });
    });
}

SEASTAR_TEST_CASE(datafile_generation_02) {
    // Data file with compound partition key and clustering key
    //
    // Respective CQL table and CQL insert:
    // CREATE TABLE table (
    //    p1 text,
    //    p2 text,
    //    c1 text,
    //    r1 int,
    //    PRIMARY KEY ((p1, p2), c1)
    // ) WITH compression = {};
    // INSERT INTO table (p1, p2, c1, r1) VALUES ('key1', 'key2', 'abc', 1);

    auto s = make_lw_shared(schema({}, some_keyspace, some_column_family,
        {{"p1", utf8_type}, {"p2", utf8_type}}, {{"c1", utf8_type}}, {{"r1", int32_type}}, {}, utf8_type));

    column_family cf(s);

    const column_definition& r1_col = *s->get_column_definition("r1");

    auto key = partition_key::from_exploded(*s, {to_bytes("key1"), to_bytes("key2")});
    auto c_key = clustering_key::from_exploded(*s, {to_bytes("abc")});

    mutation m(key, s);
    m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type->decompose(1)));
    cf.apply(std::move(m));

    auto cfp = make_shared<column_family>(std::move(cf));

    return sstables::write_datafile(*cfp, "tests/urchin/sstables/Data2.tmp.db").then([cfp, s] {
        return engine().open_file_dma("tests/urchin/sstables/Data2.tmp.db", open_flags::ro).then([] (file f) {
            auto bufptr = allocate_aligned_buffer<char>(4096, 4096);

            auto fut = f.dma_read(0, bufptr.get(), 4096);
            return std::move(fut).then([f = std::move(f), bufptr = std::move(bufptr)] (size_t size) {
                auto buf = bufptr.get();
                size_t offset = 0;
                // compound partition key
                std::vector<uint8_t> compound_key = { /* first key */ 0, 0xe, 0, 4, 'k', 'e', 'y', '1', 0,
                    0, 4, 'k', 'e', 'y', '2', 0};
                BOOST_REQUIRE(::memcmp(compound_key.data(), &buf[offset], compound_key.size()) == 0);
                offset += compound_key.size();
                std::vector<uint8_t> deletion_time = { 0x7f, 0xff, 0xff, 0xff, 0x80, 0, 0, 0, 0, 0, 0, 0 };
                BOOST_REQUIRE(::memcmp(deletion_time.data(), &buf[offset], deletion_time.size()) == 0);
                offset += deletion_time.size();
                std::vector<uint8_t> row_mark = { /* name */ 0, 9, 0, 3, 'a', 'b', 'c', 0, 0, 0, 0 };
                // check if there is a row mark.
                if (::memcmp(row_mark.data(), &buf[offset], row_mark.size()) == 0) {
                    BOOST_REQUIRE(::memcmp(row_mark.data(), &buf[offset], row_mark.size()) == 0);
                    offset += row_mark.size();
                    offset += 13; // skip mask, timestamp and value = 13 bytes.
                }
                std::vector<uint8_t> regular_row = { /* name */ 0, 0xb, 0, 3, 'a', 'b', 'c', 0, 0, 2, 'r', '1', 0,
                    /* mask */ 0, /* timestamp */ 0, 0, 0, 0, 0, 0, 0, 0, /* value */ 0, 0, 0, 4, 0, 0, 0, 1 };
                BOOST_REQUIRE(::memcmp(regular_row.data(), &buf[offset], regular_row.size()) == 0);
                offset += regular_row.size();
                std::vector<uint8_t> end_of_row = { 0, 0 };
                BOOST_REQUIRE(::memcmp(end_of_row.data(), &buf[offset], end_of_row.size()) == 0);
                offset += end_of_row.size();
                BOOST_REQUIRE(size == offset);
            });
        }).then([] {
            return remove_file("tests/urchin/sstables/Data2.tmp.db");
        });
    });
}

SEASTAR_TEST_CASE(datafile_generation_03) {
    // Data file with compound clustering key
    //
    // Respective CQL table and CQL insert:
    // CREATE TABLE table (
    //    p1 text,
    //    c1 text,
    //    c2 text,
    //    r1 int,
    //    PRIMARY KEY (p1, c1, c2)
    // ) WITH compression = {};
    // INSERT INTO table (p1, c1, c2, r1) VALUES ('key1', 'abc', 'cde', 1);

    auto s = make_lw_shared(schema({}, some_keyspace, some_column_family,
        {{"p1", utf8_type}}, {{"c1", utf8_type}, {"c2", utf8_type}}, {{"r1", int32_type}}, {}, utf8_type));

    column_family cf(s);

    const column_definition& r1_col = *s->get_column_definition("r1");

    auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
    auto c_key = clustering_key::from_exploded(*s, {to_bytes("abc"), to_bytes("cde")});

    mutation m(key, s);
    m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type->decompose(1)));
    cf.apply(std::move(m));

    auto cfp = make_shared<column_family>(std::move(cf));

    return sstables::write_datafile(*cfp, "tests/urchin/sstables/Data3.tmp.db").then([cfp, s] {
        return engine().open_file_dma("tests/urchin/sstables/Data3.tmp.db", open_flags::ro).then([] (file f) {
            auto bufptr = allocate_aligned_buffer<char>(4096, 4096);

            auto fut = f.dma_read(0, bufptr.get(), 4096);
            return std::move(fut).then([f = std::move(f), bufptr = std::move(bufptr)] (size_t size) {
                auto buf = bufptr.get();
                size_t offset = 0;
                std::vector<uint8_t> key = { 0, 4, 'k', 'e', 'y', '1' };
                BOOST_REQUIRE(::memcmp(key.data(), &buf[offset], key.size()) == 0);
                offset += key.size();
                std::vector<uint8_t> deletion_time = { 0x7f, 0xff, 0xff, 0xff, 0x80, 0, 0, 0, 0, 0, 0, 0 };
                BOOST_REQUIRE(::memcmp(deletion_time.data(), &buf[offset], deletion_time.size()) == 0);
                offset += deletion_time.size();
                std::vector<uint8_t> row_mark = { /* NOTE: with compound clustering key */
                    /* name */ 0, 0xf, 0, 3, 'a', 'b', 'c', 0, 0, 3, 'c', 'd', 'e', 0, 0, 0, 0 };
                // check if there is a row mark.
                if (::memcmp(row_mark.data(), &buf[offset], row_mark.size()) == 0) {
                    BOOST_REQUIRE(::memcmp(row_mark.data(), &buf[offset], row_mark.size()) == 0);
                    offset += row_mark.size();
                    offset += 13; // skip mask, timestamp and value = 13 bytes.
                }
                std::vector<uint8_t> regular_row = { /* NOTE: with compound clustering key */
                    /* name */ 0, 0x11, 0, 3, 'a', 'b', 'c', 0, 0, 3, 'c', 'd', 'e', 0, 0, 2, 'r', '1', 0,
                    /* mask */ 0, /* timestamp */ 0, 0, 0, 0, 0, 0, 0, 0, /* value */ 0, 0, 0, 4, 0, 0, 0, 1 };
                BOOST_REQUIRE(::memcmp(regular_row.data(), &buf[offset], regular_row.size()) == 0);
                offset += regular_row.size();
                std::vector<uint8_t> end_of_row = { 0, 0 };
                BOOST_REQUIRE(::memcmp(end_of_row.data(), &buf[offset], end_of_row.size()) == 0);
                offset += end_of_row.size();
                BOOST_REQUIRE(size == offset);
            });
        }).then([] {
            return remove_file("tests/urchin/sstables/Data3.tmp.db");
        });
    });
}

SEASTAR_TEST_CASE(datafile_generation_04) {
    // Data file with clustering key and static row
    //
    // Respective CQL table and CQL insert:
    // CREATE TABLE test (
    //    p1 text,
    //    c1 text,
    //    s1 int static,
    //    r1 int,
    //    PRIMARY KEY (p1, c1)
    //  ) WITH compression = {};
    // INSERT INTO test (p1, s1) VALUES ('key1', 10);
    // INSERT INTO test (p1, c1, r1) VALUES ('key1', 'abc', 1);

    auto s = make_lw_shared(schema({}, some_keyspace, some_column_family,
        {{"p1", utf8_type}}, {{"c1", utf8_type}}, {{"r1", int32_type}}, {{"s1", int32_type}}, utf8_type));

    column_family cf(s);

    const column_definition& r1_col = *s->get_column_definition("r1");
    const column_definition& s1_col = *s->get_column_definition("s1");

    auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
    auto c_key = clustering_key::from_exploded(*s, {to_bytes("abc")});

    mutation m(key, s);
    m.set_static_cell(s1_col, make_atomic_cell(int32_type->decompose(10)));
    m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type->decompose(1)));
    cf.apply(std::move(m));

    auto cfp = make_shared<column_family>(std::move(cf));

    return sstables::write_datafile(*cfp, "tests/urchin/sstables/Data4.tmp.db").then([cfp, s] {
        return engine().open_file_dma("tests/urchin/sstables/Data4.tmp.db", open_flags::ro).then([] (file f) {
            auto bufptr = allocate_aligned_buffer<char>(4096, 4096);

            auto fut = f.dma_read(0, bufptr.get(), 4096);
            return std::move(fut).then([f = std::move(f), bufptr = std::move(bufptr)] (size_t size) {
                auto buf = bufptr.get();
                size_t offset = 0;
                std::vector<uint8_t> key = { 0, 4, 'k', 'e', 'y', '1' };
                BOOST_REQUIRE(::memcmp(key.data(), &buf[offset], key.size()) == 0);
                offset += key.size();
                std::vector<uint8_t> deletion_time = { 0x7f, 0xff, 0xff, 0xff, 0x80, 0, 0, 0, 0, 0, 0, 0 };
                BOOST_REQUIRE(::memcmp(deletion_time.data(), &buf[offset], deletion_time.size()) == 0);
                offset += deletion_time.size();
                // static row representation
                std::vector<uint8_t> static_row = { /* name */ 0, 0xa, 0xff, 0xff, 0, 0, 0, 0, 2, 's', '1', 0,
                    /* mask */ 0, /* timestamp */ 0, 0, 0, 0, 0, 0, 0, 0, /* value */ 0, 0, 0, 4, 0, 0, 0, 0xa };
                BOOST_REQUIRE(::memcmp(static_row.data(), &buf[offset], static_row.size()) == 0);
                offset += static_row.size();
                std::vector<uint8_t> row_mark = { /* name */ 0, 9, 0, 3, 'a', 'b', 'c', 0, 0, 0, 0 };
                // check if there is a row mark.
                if (::memcmp(row_mark.data(), &buf[offset], row_mark.size()) == 0) {
                    BOOST_REQUIRE(::memcmp(row_mark.data(), &buf[offset], row_mark.size()) == 0);
                    offset += row_mark.size();
                    offset += 13; // skip mask, timestamp and value = 13 bytes.
                }
                std::vector<uint8_t> regular_row = { /* name */ 0, 0xb, 0, 3, 'a', 'b', 'c', 0, 0, 2, 'r', '1', 0,
                    /* mask */ 0, /* timestamp */ 0, 0, 0, 0, 0, 0, 0, 0, /* value */ 0, 0, 0, 4, 0, 0, 0, 1 };
                BOOST_REQUIRE(::memcmp(regular_row.data(), &buf[offset], regular_row.size()) == 0);
                offset += regular_row.size();
                std::vector<uint8_t> end_of_row = { 0, 0 };
                BOOST_REQUIRE(::memcmp(end_of_row.data(), &buf[offset], end_of_row.size()) == 0);
                offset += end_of_row.size();
                BOOST_REQUIRE(size == offset);
            });
        }).then([] {
            return remove_file("tests/urchin/sstables/Data4.tmp.db");
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
    int count_deleted_cell = 0;
    int count_range_tombstone = 0;
    int count_row_end = 0;
    virtual void consume_row_start(bytes_view key, sstables::deletion_time deltime) override {
        BOOST_REQUIRE(key == as_bytes("vinna"));
        BOOST_REQUIRE(deltime.local_deletion_time == (uint32_t)std::numeric_limits<int32_t>::max());
        BOOST_REQUIRE(deltime.marked_for_delete_at == (uint64_t)std::numeric_limits<int64_t>::min());
        count_row_start++;
    }

    virtual void consume_cell(bytes_view col_name, bytes_view value,
            uint64_t timestamp, uint32_t ttl, uint32_t expiration) override {
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
                BOOST_REQUIRE(c.count_deleted_cell == 0);
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
                BOOST_REQUIRE(c.count_deleted_cell == 0);
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
                BOOST_REQUIRE(c.count_deleted_cell == 0);
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
    virtual void consume_row_start(bytes_view key, sstables::deletion_time deltime) override {
        count_row_start++;
    }
    virtual void consume_cell(bytes_view col_name, bytes_view value,
            uint64_t timestamp, uint32_t ttl, uint32_t expiration) override {
        count_cell++;
    }
    virtual void consume_deleted_cell(bytes_view col_name, sstables::deletion_time deltime) override {
        count_deleted_cell++;
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
                BOOST_REQUIRE(c.count_deleted_cell == 0);
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
                BOOST_REQUIRE(c.count_deleted_cell == 0);
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
                BOOST_REQUIRE(c.count_deleted_cell == 0);
                BOOST_REQUIRE(c.count_range_tombstone == 1);
               return make_ready_future<>();
            });
        });
    });
}

class ttl_row_consumer : public count_row_consumer {
public:
    const uint64_t desired_timestamp;
    ttl_row_consumer(uint64_t t) : desired_timestamp(t) { }
    virtual void consume_row_start(bytes_view key, sstables::deletion_time deltime) override {
        count_row_consumer::consume_row_start(key, deltime);
        BOOST_REQUIRE(key == as_bytes("nadav"));
        BOOST_REQUIRE(deltime.local_deletion_time == (uint32_t)std::numeric_limits<int32_t>::max());
        BOOST_REQUIRE(deltime.marked_for_delete_at == (uint64_t)std::numeric_limits<int64_t>::min());
    }

    virtual void consume_cell(bytes_view col_name, bytes_view value,
            uint64_t timestamp, uint32_t ttl, uint32_t expiration) override {
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
    return reusable_sst("tests/urchin/sstables/ttl", 1).then([] (auto sstp) {
        return do_with(ttl_row_consumer(1430151018675502), [sstp] (auto& c) {
            return sstp->data_consume_rows(c).then([sstp, &c] {
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
    virtual void consume_row_start(bytes_view key, sstables::deletion_time deltime) override {
        count_row_consumer::consume_row_start(key, deltime);
        BOOST_REQUIRE(key == as_bytes("nadav"));
        BOOST_REQUIRE(deltime.local_deletion_time == (uint32_t)std::numeric_limits<int32_t>::max());
        BOOST_REQUIRE(deltime.marked_for_delete_at == (uint64_t)std::numeric_limits<int64_t>::min());
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
    return reusable_sst("tests/urchin/sstables/deleted_cell", 2).then([] (auto sstp) {
        return do_with(deleted_cell_row_consumer(), [sstp] (auto& c) {
            return sstp->data_consume_rows(c).then([sstp, &c] {
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


schema_ptr composite_schema() {
    static thread_local auto s = make_lw_shared(schema({}, "tests", "composite",
        // partition key
        {{"name", bytes_type}, {"col1", bytes_type}},
        // clustering key
        {},
        // regular columns
        {},
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "Table with a composite key as pkey"
       ));
    return s;
}

schema_ptr set_schema() {
    auto my_set_type = set_type_impl::get_instance(bytes_type, false);
    static thread_local auto s = make_lw_shared(schema({}, "tests", "set_pk",
        // partition key
        {{"ss", my_set_type}},
        // clustering key
        {},
        // regular columns
        {
            {"ns", utf8_type},
        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "Table with a set as pkeys"
       ));
    return s;
}

schema_ptr map_schema() {
    auto my_map_type = map_type_impl::get_instance(bytes_type, bytes_type, false);
    static thread_local auto s = make_lw_shared(schema({}, "tests", "map_pk",
        // partition key
        {{"ss", my_map_type}},
        // clustering key
        {},
        // regular columns
        {
            {"ns", utf8_type},
        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "Table with a map as pkeys"
       ));
    return s;
}

schema_ptr list_schema() {
    auto my_list_type = list_type_impl::get_instance(bytes_type, false);
    static thread_local auto s = make_lw_shared(schema({}, "tests", "list_pk",
        // partition key
        {{"ss", my_list_type}},
        // clustering key
        {},
        // regular columns
        {
            {"ns", utf8_type},
        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "Table with a list as pkeys"
       ));
    return s;
}


SEASTAR_TEST_CASE(find_key_map) {
    return reusable_sst("tests/urchin/sstables/map_pk", 1).then([] (auto sstp) {
        schema_ptr s = map_schema();
        auto& summary = sstables::test(sstp)._summary();
        std::vector<boost::any> kk;

        auto b1 = to_bytes("2");
        auto b2 = to_bytes("2");

        auto map_element = std::make_pair<boost::any, boost::any>(boost::any(b1), boost::any(b2));
        std::vector<std::pair<boost::any, boost::any>> map;
        map.push_back(map_element);

        kk.push_back(map);

        auto key = sstables::key::from_deeply_exploded(*s, kk);
        BOOST_REQUIRE(sstables::test(sstp).binary_search(summary.entries, key) == 0);
    });
}

SEASTAR_TEST_CASE(find_key_set) {
    return reusable_sst("tests/urchin/sstables/set_pk", 1).then([] (auto sstp) {
        schema_ptr s = set_schema();
        auto& summary = sstables::test(sstp)._summary();
        std::vector<boost::any> kk;

        std::vector<boost::any> set;

        bytes b1("1");
        bytes b2("2");

        set.push_back(boost::any(b1));
        set.push_back(boost::any(b2));
        kk.push_back(set);

        auto key = sstables::key::from_deeply_exploded(*s, kk);
        BOOST_REQUIRE(sstables::test(sstp).binary_search(summary.entries, key) == 0);
    });
}

SEASTAR_TEST_CASE(find_key_list) {
    return reusable_sst("tests/urchin/sstables/list_pk", 1).then([] (auto sstp) {
        schema_ptr s = set_schema();
        auto& summary = sstables::test(sstp)._summary();
        std::vector<boost::any> kk;

        std::vector<boost::any> list;

        bytes b1("1");
        bytes b2("2");
        list.push_back(boost::any(b1));
        list.push_back(boost::any(b2));

        kk.push_back(list);

        auto key = sstables::key::from_deeply_exploded(*s, kk);
        BOOST_REQUIRE(sstables::test(sstp).binary_search(summary.entries, key) == 0);
    });
}


SEASTAR_TEST_CASE(find_key_composite) {
    return reusable_sst("tests/urchin/sstables/composite", 1).then([] (auto sstp) {
        schema_ptr s = composite_schema();
        auto& summary = sstables::test(sstp)._summary();
        std::vector<boost::any> kk;

        auto b1 = bytes("HCG8Ee7ENWqfCXipk4-Ygi2hzrbfHC8pTtH3tEmV3d9p2w8gJPuMN_-wp1ejLRf4kNEPEgtgdHXa6NoFE7qUig==");
        auto b2 = bytes("VJizqYxC35YpLaPEJNt_4vhbmKJxAg54xbiF1UkL_9KQkqghVvq34rZ6Lm8eRTi7JNJCXcH6-WtNUSFJXCOfdg==");

        kk.push_back(boost::any(b1));
        kk.push_back(boost::any(b2));

        auto key = sstables::key::from_deeply_exploded(*s, kk);
        BOOST_REQUIRE(sstables::test(sstp).binary_search(summary.entries, key) == 0);
    });
}

SEASTAR_TEST_CASE(all_in_place) {
    return reusable_sst("tests/urchin/sstables/bigsummary", 76).then([] (auto sstp) {
        auto& summary = sstables::test(sstp)._summary();

        int idx = 0;
        for (auto& e: summary.entries) {
            auto key = sstables::key::from_bytes(e.key);
            BOOST_REQUIRE(sstables::test(sstp).binary_search(summary.entries, key) == idx++);
        }
    });
}

SEASTAR_TEST_CASE(full_index_search) {
    return reusable_sst("tests/urchin/sstables/uncompressed", 1).then([] (auto sstp) {
        return sstables::test(sstp).read_indexes(0, 4).then([sstp] (auto index_list) {
            int idx = 0;
            for (auto& ie: index_list) {
                auto key = key::from_bytes(ie.key.value);
                BOOST_REQUIRE(sstables::test(sstp).binary_search(index_list, key) == idx++);
            }
        });
    });
}

SEASTAR_TEST_CASE(not_find_key_composite_bucket0) {
    return reusable_sst("tests/urchin/sstables/composite", 1).then([] (auto sstp) {
        schema_ptr s = composite_schema();
        auto& summary = sstables::test(sstp)._summary();
        std::vector<boost::any> kk;

        auto b1 = bytes("ZEunFCoqAidHOrPiU3U6UAvUU01IYGvT3kYtYItJ1ODTk7FOsEAD-dqmzmFNfTDYvngzkZwKrLxthB7ItLZ4HQ==");
        auto b2 = bytes("K-GpWx-QtyzLb12z5oNS0C03d3OzNyBKdYJh1XjHiC53KudoqdoFutHUMFLe6H9Emqv_fhwIJEKEb5Csn72f9A==");

        kk.push_back(boost::any(b1));
        kk.push_back(boost::any(b2));

        auto key = sstables::key::from_deeply_exploded(*s, kk);
        // (result + 1) * -1 -1 = 0
        BOOST_REQUIRE(sstables::test(sstp).binary_search(summary.entries, key) == -2);
    });
}
