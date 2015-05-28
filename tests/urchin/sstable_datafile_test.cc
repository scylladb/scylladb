/*
 * Copyright 2015 Cloudius Systems
 */

#include "core/sstring.hh"
#include "core/future-util.hh"
#include "core/align.hh"
#include "sstables/sstables.hh"
#include "sstables/key.hh"
#include "tests/test-utils.hh"
#include "schema.hh"
#include "database.hh"
#include <memory>
#include "sstable_test.hh"
#include "core/seastar.hh"

#include <stdio.h>
#include <ftw.h>
#include <unistd.h>

class test_setup {
    file _f;
    std::function<future<> (directory_entry de)> _walker;
    subscription<directory_entry> _listing;
    static sstring path;

public:
    test_setup(file f)
            : _f(std::move(f))
            , _listing(_f.list_directory([this] (directory_entry de) { return _remove(de); })) {
    }
protected:
    future<> _create_directory(sstring name) {
        return engine().make_directory(name);
    }

    future<> _remove(directory_entry de) {
        if (de.type == directory_entry_type::regular) {
            return engine().remove_file(path + "/" + de.name);
        }
        return make_ready_future<>();
    }
    future<> done() { return _listing.done(); }

    static future<> empty_test_dir() {
        return engine().open_directory(path).then([] (file f) {
            auto l = make_lw_shared<test_setup>(std::move(f));
            return l->done().then([l] { });
        });
    }
    static future<> create_empty_test_dir() {
        return engine().make_directory(path).then_wrapped([] (future<> f) {
            try {
                f.get();
            // it's fine if the directory exists, just shut down the exceptional future message
            } catch (std::exception& e) {}
            return empty_test_dir();
        });
    }
public:
    static future<> do_with_test_directory(std::function<future<> ()>&& fut);
};
sstring test_setup::path = "tests/urchin/sstables/tests-temporary";

future<> test_setup::do_with_test_directory(std::function<future<> ()>&& fut) {
    return test_setup::create_empty_test_dir().then([fut = std::move(fut)] () mutable {
            return fut();
    }).finally([] {
        return test_setup::empty_test_dir().then([] {
            return engine().remove_file(path);
        });
    });
}

using namespace sstables;

static sstring some_keyspace("ks");
static sstring some_column_family("cf");

atomic_cell make_atomic_cell(bytes_view value, uint32_t ttl = 0, uint32_t expiration = 0) {
    if (ttl) {
        return atomic_cell::make_live(0, value,
            gc_clock::time_point(gc_clock::duration(expiration)), gc_clock::duration(ttl));
    } else {
        return atomic_cell::make_live(0, value);
    }
}

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

    return test_setup::do_with_test_directory([] {
        auto s = make_lw_shared(schema({}, some_keyspace, some_column_family,
            {{"p1", utf8_type}}, {{"c1", utf8_type}}, {{"r1", int32_type}, {"r2", int32_type}}, {}, utf8_type));

        memtable mt(s);

        const column_definition& r1_col = *s->get_column_definition("r1");

        auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
        auto c_key = clustering_key::from_exploded(*s, {to_bytes("abc")});

        mutation m(key, s);
        m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type->decompose(1)));
        mt.apply(std::move(m));

        auto mtp = make_shared<memtable>(std::move(mt));
        auto sst = make_lw_shared<sstable>("tests/urchin/sstables/tests-temporary", 1, la, big);

        auto fname = sstable::filename("tests/urchin/sstables/tests-temporary", la, 1, big, sstable::component_type::Data);
        return sst->write_components(*mtp).then([mtp, sst, s, fname] {
            return engine().open_file_dma(fname, open_flags::ro).then([] (file f) {
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
            });
        });
    });
}

SEASTAR_TEST_CASE(datafile_generation_02) {
    return test_setup::do_with_test_directory([] {
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

        memtable mt(s);

        const column_definition& r1_col = *s->get_column_definition("r1");

        auto key = partition_key::from_exploded(*s, {to_bytes("key1"), to_bytes("key2")});
        auto c_key = clustering_key::from_exploded(*s, {to_bytes("abc")});

        mutation m(key, s);
        m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type->decompose(1)));
        mt.apply(std::move(m));

        auto mtp = make_shared<memtable>(std::move(mt));
        auto sst = make_lw_shared<sstable>("tests/urchin/sstables/tests-temporary", 2, la, big);

        auto fname = sstable::filename("tests/urchin/sstables/tests-temporary", la, 2, big, sstable::component_type::Data);
        return sst->write_components(*mtp).then([mtp, sst, s, fname] {
            return engine().open_file_dma(fname, open_flags::ro).then([] (file f) {
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
            });
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
    return test_setup::do_with_test_directory([] {
        auto s = make_lw_shared(schema({}, some_keyspace, some_column_family,
            {{"p1", utf8_type}}, {{"c1", utf8_type}, {"c2", utf8_type}}, {{"r1", int32_type}}, {}, utf8_type));

        memtable mt(s);

        const column_definition& r1_col = *s->get_column_definition("r1");

        auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
        auto c_key = clustering_key::from_exploded(*s, {to_bytes("abc"), to_bytes("cde")});

        mutation m(key, s);
        m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type->decompose(1)));
        mt.apply(std::move(m));

        auto mtp = make_shared<memtable>(std::move(mt));
        auto sst = make_lw_shared<sstable>("tests/urchin/sstables/tests-temporary", 3, la, big);

        auto fname = sstable::filename("tests/urchin/sstables/tests-temporary", la, 3, big, sstable::component_type::Data);
        return sst->write_components(*mtp).then([mtp, sst, s, fname] {
            return engine().open_file_dma(fname, open_flags::ro).then([] (file f) {
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
            });
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
    return test_setup::do_with_test_directory([] {
        auto s = make_lw_shared(schema({}, some_keyspace, some_column_family,
            {{"p1", utf8_type}}, {{"c1", utf8_type}}, {{"r1", int32_type}}, {{"s1", int32_type}}, utf8_type));

        memtable mt(s);

        const column_definition& r1_col = *s->get_column_definition("r1");
        const column_definition& s1_col = *s->get_column_definition("s1");

        auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
        auto c_key = clustering_key::from_exploded(*s, {to_bytes("abc")});

        mutation m(key, s);
        m.set_static_cell(s1_col, make_atomic_cell(int32_type->decompose(10)));
        m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type->decompose(1)));
        mt.apply(std::move(m));

        auto mtp = make_shared<memtable>(std::move(mt));
        auto sst = make_lw_shared<sstable>("tests/urchin/sstables/tests-temporary", 4, la, big);

        auto fname = sstable::filename("tests/urchin/sstables/tests-temporary", la, 4, big, sstable::component_type::Data);
        return sst->write_components(*mtp).then([mtp, sst, s, fname] {
            return engine().open_file_dma(fname, open_flags::ro).then([] (file f) {
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
            });
        });
    });
}

SEASTAR_TEST_CASE(datafile_generation_05) {
    // Data file with clustering key and expiring cells.
    //
    // Respective CQL table and CQL insert:
    // CREATE TABLE test (
    //    p1 text,
    //    c1 text,
    //    r1 int,
    //    PRIMARY KEY (p1, c1)
    //  ) WITH compression = {};
    // INSERT INTO test (p1, c1, r1) VALUES ('key1', 'abc', 1) USING TTL 3600;
    return test_setup::do_with_test_directory([] {
        auto s = make_lw_shared(schema({}, some_keyspace, some_column_family,
            {{"p1", utf8_type}}, {{"c1", utf8_type}}, {{"r1", int32_type}}, {}, utf8_type));

        memtable mt(s);

        const column_definition& r1_col = *s->get_column_definition("r1");

        auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
        auto c_key = clustering_key::from_exploded(*s, {to_bytes("abc")});

        mutation m(key, s);
        m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type->decompose(1), 3600, 3600));
        mt.apply(std::move(m));

        auto mtp = make_shared<memtable>(std::move(mt));
        auto sst = make_lw_shared<sstable>("tests/urchin/sstables/tests-temporary", 5, la, big);

        return sst->write_components(*mtp).then([mtp, sst, s] {
            auto fname = sstable::filename("tests/urchin/sstables/tests-temporary", la, 5, big, sstable::component_type::Data);
            return engine().open_file_dma(fname, open_flags::ro).then([] (file f) {
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
                        offset += 21; // skip mask, ttl, expiration, timestamp and value = 21 bytes.
                    }
                    std::vector<uint8_t> expiring_row = { /* name */ 0, 0xb, 0, 3, 'a', 'b', 'c', 0, 0, 2, 'r', '1', 0,
                        /* mask */ 2, /* ttl = 3600 */ 0, 0, 0xe, 0x10, /* expiration = ttl + 0 */ 0, 0, 0xe, 0x10,
                        /* timestamp */ 0, 0, 0, 0, 0, 0, 0, 0, /* value */ 0, 0, 0, 4, 0, 0, 0, 1 };
                    BOOST_REQUIRE(::memcmp(expiring_row.data(), &buf[offset], expiring_row.size()) == 0);
                    offset += expiring_row.size();
                    std::vector<uint8_t> end_of_row = { 0, 0 };
                    BOOST_REQUIRE(::memcmp(end_of_row.data(), &buf[offset], end_of_row.size()) == 0);
                    offset += end_of_row.size();
                    BOOST_REQUIRE(size == offset);
                });
            });
        });
    });
}

atomic_cell make_dead_atomic_cell(uint32_t deletion_time) {
    return atomic_cell::make_dead(0, gc_clock::time_point(gc_clock::duration(deletion_time)));
}

SEASTAR_TEST_CASE(datafile_generation_06) {
    // Data file with clustering key and tombstone cells.
    //
    // Respective CQL table and CQL insert:
    // CREATE TABLE test (
    //    p1 text,
    //    c1 text,
    //    r1 int,
    //    PRIMARY KEY (p1, c1)
    //  ) WITH compression = {};
    // INSERT INTO test (p1, c1, r1) VALUES ('key1', 'abc', 1);
    // after flushed:
    // DELETE r1 FROM test WHERE p1 = 'key1' AND c1 = 'abc';
    return test_setup::do_with_test_directory([] {
        auto s = make_lw_shared(schema({}, some_keyspace, some_column_family,
            {{"p1", utf8_type}}, {{"c1", utf8_type}}, {{"r1", int32_type}}, {}, utf8_type));

        memtable mt(s);

        const column_definition& r1_col = *s->get_column_definition("r1");

        auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
        auto c_key = clustering_key::from_exploded(*s, {to_bytes("abc")});

        mutation m(key, s);
        m.set_clustered_cell(c_key, r1_col, make_dead_atomic_cell(3600));
        mt.apply(std::move(m));

        auto mtp = make_shared<memtable>(std::move(mt));
        auto sst = make_lw_shared<sstable>("tests/urchin/sstables/tests-temporary", 6, la, big);

        return sst->write_components(*mtp).then([mtp, sst, s] {
            auto fname = sstable::filename("tests/urchin/sstables/tests-temporary", la, 6, big, sstable::component_type::Data);
            return engine().open_file_dma(fname, open_flags::ro).then([] (file f) {
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
                        offset += 13; // skip mask, timestamp and expiration (value) = 13 bytes.
                    }
                    // tombstone cell
                    std::vector<uint8_t> row = { /* name */ 0, 0xb, 0, 3, 'a', 'b', 'c', 0, 0, 2, 'r', '1', 0,
                        /* mask */ 1, /* timestamp */ 0, 0, 0, 0, 0, 0, 0, 0,
                        /* expiration (value) */ 0, 0, 0, 4, 0, 0, 0xe, 0x10 };
                    BOOST_REQUIRE(::memcmp(row.data(), &buf[offset], row.size()) == 0);
                    offset += row.size();
                    std::vector<uint8_t> end_of_row = { 0, 0 };
                    BOOST_REQUIRE(::memcmp(end_of_row.data(), &buf[offset], end_of_row.size()) == 0);
                    offset += end_of_row.size();
                    BOOST_REQUIRE(size == offset);
                });
            });
        });
    });
}

SEASTAR_TEST_CASE(datafile_generation_07) {
    // Data file with clustering key and two sstable rows.
    // Only index file is validated in this test case.
    //
    // Respective CQL table and CQL insert:
    // CREATE TABLE test (
    //    p1 text,
    //    c1 text,
    //    r1 int,
    //    PRIMARY KEY (p1, c1)
    //  ) WITH compression = {};
    // INSERT INTO test (p1, c1, r1) VALUES ('key1', 'abc', 1);
    // INSERT INTO test (p1, c1, r1) VALUES ('key2', 'cde', 1);
    return test_setup::do_with_test_directory([] {
        auto s = make_lw_shared(schema({}, some_keyspace, some_column_family,
            {{"p1", utf8_type}}, {{"c1", utf8_type}}, {{"r1", int32_type}}, {}, utf8_type));

        memtable mt(s);

        const column_definition& r1_col = *s->get_column_definition("r1");

        auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
        auto c_key = clustering_key::from_exploded(*s, {to_bytes("abc")});

        mutation m(key, s);
        m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type->decompose(1)));
        mt.apply(std::move(m));

        auto key2 = partition_key::from_exploded(*s, {to_bytes("key2")});
        auto c_key2 = clustering_key::from_exploded(*s, {to_bytes("cde")});

        mutation m2(key2, s);
        m2.set_clustered_cell(c_key2, r1_col, make_atomic_cell(int32_type->decompose(1)));
        mt.apply(std::move(m2));

        auto mtp = make_shared<memtable>(std::move(mt));
        auto sst = make_lw_shared<sstable>("tests/urchin/sstables/tests-temporary", 7, la, big);

        return sst->write_components(*mtp).then([mtp, sst, s] {
            auto fname = sstable::filename("tests/urchin/sstables/tests-temporary", la, 7, big, sstable::component_type::Index);
            return engine().open_file_dma(fname, open_flags::ro).then([] (file f) {
                auto bufptr = allocate_aligned_buffer<char>(4096, 4096);

                auto fut = f.dma_read(0, bufptr.get(), 4096);
                return std::move(fut).then([f = std::move(f), bufptr = std::move(bufptr)] (size_t size) {
                    auto buf = bufptr.get();
                    size_t offset = 0;
                    std::vector<uint8_t> key1 = { 0, 4, 'k', 'e', 'y', '1',
                        /* pos */ 0, 0, 0, 0, 0, 0, 0, 0, /* promoted index */ 0, 0, 0, 0};
                    BOOST_REQUIRE(::memcmp(key1.data(), &buf[offset], key1.size()) == 0);
                    offset += key1.size();
                    std::vector<uint8_t> key2 = { 0, 4, 'k', 'e', 'y', '2',
                        /* pos */ 0, 0, 0, 0, 0, 0, 0, 0x32, /* promoted index */ 0, 0, 0, 0};
                    BOOST_REQUIRE(::memcmp(key2.data(), &buf[offset], key2.size()) == 0);
                    offset += key2.size();
                    BOOST_REQUIRE(size == offset);
                });
            });
        });
    });
}

SEASTAR_TEST_CASE(datafile_generation_08) {
    // Data file with multiple rows.
    // Only summary file is validated in this test case.
    //
    // Respective CQL table and CQL insert:
    // CREATE TABLE test (
    //    p1 int,
    //    c1 text,
    //    r1 int,
    //    PRIMARY KEY (p1, c1)
    //  ) WITH compression = {};
    return test_setup::do_with_test_directory([] {
        auto s = make_lw_shared(schema({}, some_keyspace, some_column_family,
            {{"p1", int32_type}}, {{"c1", utf8_type}}, {{"r1", int32_type}}, {}, utf8_type));

        memtable mt(s);

        const column_definition& r1_col = *s->get_column_definition("r1");

        // Create 150 partitions so that summary file store 2 entries, assuming min index
        // interval is 128.
        for (int32_t i = 0; i < 150; i++) {
            auto key = partition_key::from_exploded(*s, {int32_type->decompose(i)});
            auto c_key = clustering_key::from_exploded(*s, {to_bytes("abc")});

            mutation m(key, s);
            m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type->decompose(1)));
            mt.apply(std::move(m));
        }

        auto mtp = make_shared<memtable>(std::move(mt));
        auto sst = make_lw_shared<sstable>("tests/urchin/sstables/tests-temporary", 8, la, big);

        return sst->write_components(*mtp).then([mtp, sst, s] {
            auto fname = sstable::filename("tests/urchin/sstables/tests-temporary", la, 8, big, sstable::component_type::Summary);
            return engine().open_file_dma(fname, open_flags::ro).then([] (file f) {
                auto bufptr = allocate_aligned_buffer<char>(4096, 4096);

                auto fut = f.dma_read(0, bufptr.get(), 4096);
                return std::move(fut).then([f = std::move(f), bufptr = std::move(bufptr)] (size_t size) {
                    auto buf = bufptr.get();
                    size_t offset = 0;

                    std::vector<uint8_t> header = { /* min_index_interval */ 0, 0, 0, 0x80, /* size */ 0, 0, 0, 2,
                        /* memory_size */ 0, 0, 0, 0, 0, 0, 0, 0x20, /* sampling_level */ 0, 0, 0, 0x80,
                        /* size_at_full_sampling */  0, 0, 0, 2 };
                    BOOST_REQUIRE(::memcmp(header.data(), &buf[offset], header.size()) == 0);
                    offset += header.size();

                    std::vector<uint8_t> positions = { 0x8, 0, 0, 0, 0x14, 0, 0, 0 };
                    BOOST_REQUIRE(::memcmp(positions.data(), &buf[offset], positions.size()) == 0);
                    offset += positions.size();

                    std::vector<uint8_t> first_entry = { /* key */ 0, 0, 0, 0x17, /* position */ 0, 0, 0, 0, 0, 0, 0, 0 };
                    BOOST_REQUIRE(::memcmp(first_entry.data(), &buf[offset], first_entry.size()) == 0);
                    offset += first_entry.size();

                    std::vector<uint8_t> second_entry = { /* key */ 0, 0, 0, 0x65, /* position */ 0, 0x9, 0, 0, 0, 0, 0, 0 };
                    BOOST_REQUIRE(::memcmp(second_entry.data(), &buf[offset], second_entry.size()) == 0);
                    offset += second_entry.size();

                    std::vector<uint8_t> first_key = { 0, 0, 0, 0x4, 0, 0, 0, 0x17 };
                    BOOST_REQUIRE(::memcmp(first_key.data(), &buf[offset], first_key.size()) == 0);
                    offset += first_key.size();

                    std::vector<uint8_t> last_key = { 0, 0, 0, 0x4, 0, 0, 0, 0x67 };
                    BOOST_REQUIRE(::memcmp(last_key.data(), &buf[offset], last_key.size()) == 0);
                    offset += last_key.size();

                    BOOST_REQUIRE(size == offset);
                });
            });
        });
    });
}

SEASTAR_TEST_CASE(datafile_generation_09) {
    // Test that generated sstable components can be successfully loaded.
    return test_setup::do_with_test_directory([] {
        auto s = make_lw_shared(schema({}, some_keyspace, some_column_family,
            {{"p1", utf8_type}}, {{"c1", utf8_type}}, {{"r1", int32_type}}, {}, utf8_type));

        memtable mt(s);

        const column_definition& r1_col = *s->get_column_definition("r1");

        auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
        auto c_key = clustering_key::from_exploded(*s, {to_bytes("abc")});

        mutation m(key, s);
        m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type->decompose(1)));
        mt.apply(std::move(m));

        auto mtp = make_shared<memtable>(std::move(mt));
        auto sst = make_lw_shared<sstable>("tests/urchin/sstables/tests-temporary", 9, la, big);

        return sst->write_components(*mtp).then([mtp, sst, s] {
            auto sst2 = make_lw_shared<sstable>("tests/urchin/sstables/tests-temporary", 9, la, big);

            return sstables::test(sst2).read_summary().then([sst, sst2] {
                summary& sst1_s = sstables::test(sst).get_summary();
                summary& sst2_s = sstables::test(sst2).get_summary();

                BOOST_REQUIRE(::memcmp(&sst1_s.header, &sst2_s.header, sizeof(summary::header)) == 0);
                BOOST_REQUIRE(sst1_s.positions == sst2_s.positions);
                BOOST_REQUIRE(sst1_s.entries == sst2_s.entries);
                BOOST_REQUIRE(sst1_s.first_key.value == sst2_s.first_key.value);
                BOOST_REQUIRE(sst1_s.last_key.value == sst2_s.last_key.value);
            }).then([sst, sst2] {
                return sstables::test(sst2).read_toc().then([sst, sst2] {
                    auto& sst1_c = sstables::test(sst).get_components();
                    auto& sst2_c = sstables::test(sst2).get_components();

                    BOOST_REQUIRE(sst1_c == sst2_c);
                });
            });
        });
    });
}
