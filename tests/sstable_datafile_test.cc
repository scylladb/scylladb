/*
 * Copyright 2015 Cloudius Systems
 */

#include "core/sstring.hh"
#include "core/future-util.hh"
#include "core/align.hh"
#include "sstables/sstables.hh"
#include "sstables/key.hh"
#include "sstables/compress.hh"
#include "sstables/compaction.hh"
#include "tests/test-utils.hh"
#include "schema.hh"
#include "schema_builder.hh"
#include "database.hh"
#include <memory>
#include "sstable_test.hh"
#include "core/seastar.hh"
#include "core/do_with.hh"

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
    ~test_setup() {
        _f.close().finally([save = _f] {});
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
sstring test_setup::path = "tests/sstables/tests-temporary";

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

        auto mt = make_lw_shared<memtable>(s);

        const column_definition& r1_col = *s->get_column_definition("r1");

        auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
        auto c_key = clustering_key::from_exploded(*s, {to_bytes("abc")});

        mutation m(key, s);
        m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type->decompose(1)));
        mt->apply(std::move(m));

        auto sst = make_lw_shared<sstable>("tests/sstables/tests-temporary", 1, la, big);

        auto fname = sstable::filename("tests/sstables/tests-temporary", la, 1, big, sstable::component_type::Data);
        return sst->write_components(*mt).then([mt, sst, s, fname] {
            return engine().open_file_dma(fname, open_flags::ro).then([] (file f) {
                auto bufptr = allocate_aligned_buffer<char>(4096, 4096);

                auto fut = f.dma_read(0, bufptr.get(), 4096);
                return std::move(fut).then([f = std::move(f), bufptr = std::move(bufptr)] (size_t size) mutable {
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
                    return f.close().finally([f] {});
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

        auto mt = make_lw_shared<memtable>(s);

        const column_definition& r1_col = *s->get_column_definition("r1");

        auto key = partition_key::from_exploded(*s, {to_bytes("key1"), to_bytes("key2")});
        auto c_key = clustering_key::from_exploded(*s, {to_bytes("abc")});

        mutation m(key, s);
        m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type->decompose(1)));
        mt->apply(std::move(m));

        auto sst = make_lw_shared<sstable>("tests/sstables/tests-temporary", 2, la, big);

        auto fname = sstable::filename("tests/sstables/tests-temporary", la, 2, big, sstable::component_type::Data);
        return sst->write_components(*mt).then([mt, sst, s, fname] {
            return engine().open_file_dma(fname, open_flags::ro).then([] (file f) {
                auto bufptr = allocate_aligned_buffer<char>(4096, 4096);

                auto fut = f.dma_read(0, bufptr.get(), 4096);
                return std::move(fut).then([f = std::move(f), bufptr = std::move(bufptr)] (size_t size) mutable {
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
                    return f.close().finally([f] {});
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

        auto mt = make_lw_shared<memtable>(s);

        const column_definition& r1_col = *s->get_column_definition("r1");

        auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
        auto c_key = clustering_key::from_exploded(*s, {to_bytes("abc"), to_bytes("cde")});

        mutation m(key, s);
        m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type->decompose(1)));
        mt->apply(std::move(m));

        auto sst = make_lw_shared<sstable>("tests/sstables/tests-temporary", 3, la, big);

        auto fname = sstable::filename("tests/sstables/tests-temporary", la, 3, big, sstable::component_type::Data);
        return sst->write_components(*mt).then([mt, sst, s, fname] {
            return engine().open_file_dma(fname, open_flags::ro).then([] (file f) {
                auto bufptr = allocate_aligned_buffer<char>(4096, 4096);

                auto fut = f.dma_read(0, bufptr.get(), 4096);
                return std::move(fut).then([f = std::move(f), bufptr = std::move(bufptr)] (size_t size) mutable {
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
                    return f.close().finally([f]{});
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

        auto mt = make_lw_shared<memtable>(s);

        const column_definition& r1_col = *s->get_column_definition("r1");
        const column_definition& s1_col = *s->get_column_definition("s1");

        auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
        auto c_key = clustering_key::from_exploded(*s, {to_bytes("abc")});

        mutation m(key, s);
        m.set_static_cell(s1_col, make_atomic_cell(int32_type->decompose(10)));
        m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type->decompose(1)));
        mt->apply(std::move(m));

        auto sst = make_lw_shared<sstable>("tests/sstables/tests-temporary", 4, la, big);

        auto fname = sstable::filename("tests/sstables/tests-temporary", la, 4, big, sstable::component_type::Data);
        return sst->write_components(*mt).then([mt, sst, s, fname] {
            return engine().open_file_dma(fname, open_flags::ro).then([] (file f) {
                auto bufptr = allocate_aligned_buffer<char>(4096, 4096);

                auto fut = f.dma_read(0, bufptr.get(), 4096);
                return std::move(fut).then([f = std::move(f), bufptr = std::move(bufptr)] (size_t size) mutable {
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
                    return f.close().finally([f]{});
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

        auto mt = make_lw_shared<memtable>(s);

        const column_definition& r1_col = *s->get_column_definition("r1");

        auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
        auto c_key = clustering_key::from_exploded(*s, {to_bytes("abc")});

        mutation m(key, s);
        m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type->decompose(1), 3600, 3600));
        mt->apply(std::move(m));

        auto now = to_gc_clock(db_clock::from_time_t(0));
        auto sst = make_lw_shared<sstable>("tests/sstables/tests-temporary", 5, la, big, now);

        return sst->write_components(*mt).then([mt, sst, s] {
            auto fname = sstable::filename("tests/sstables/tests-temporary", la, 5, big, sstable::component_type::Data);
            return engine().open_file_dma(fname, open_flags::ro).then([] (file f) {
                auto bufptr = allocate_aligned_buffer<char>(4096, 4096);

                auto fut = f.dma_read(0, bufptr.get(), 4096);
                return std::move(fut).then([f = std::move(f), bufptr = std::move(bufptr)] (size_t size) mutable {
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
                    return f.close().finally([f]{});
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

        auto mt = make_lw_shared<memtable>(s);

        const column_definition& r1_col = *s->get_column_definition("r1");

        auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
        auto c_key = clustering_key::from_exploded(*s, {to_bytes("abc")});

        mutation m(key, s);
        m.set_clustered_cell(c_key, r1_col, make_dead_atomic_cell(3600));
        mt->apply(std::move(m));

        auto sst = make_lw_shared<sstable>("tests/sstables/tests-temporary", 6, la, big);

        return sst->write_components(*mt).then([mt, sst, s] {
            auto fname = sstable::filename("tests/sstables/tests-temporary", la, 6, big, sstable::component_type::Data);
            return engine().open_file_dma(fname, open_flags::ro).then([] (file f) {
                auto bufptr = allocate_aligned_buffer<char>(4096, 4096);

                auto fut = f.dma_read(0, bufptr.get(), 4096);
                return std::move(fut).then([f = std::move(f), bufptr = std::move(bufptr)] (size_t size) mutable {
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
                    return f.close().finally([f]{});
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

        auto mt = make_lw_shared<memtable>(s);

        const column_definition& r1_col = *s->get_column_definition("r1");

        auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
        auto c_key = clustering_key::from_exploded(*s, {to_bytes("abc")});

        mutation m(key, s);
        m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type->decompose(1)));
        mt->apply(std::move(m));

        auto key2 = partition_key::from_exploded(*s, {to_bytes("key2")});
        auto c_key2 = clustering_key::from_exploded(*s, {to_bytes("cde")});

        mutation m2(key2, s);
        m2.set_clustered_cell(c_key2, r1_col, make_atomic_cell(int32_type->decompose(1)));
        mt->apply(std::move(m2));

        auto sst = make_lw_shared<sstable>("tests/sstables/tests-temporary", 7, la, big);

        return sst->write_components(*mt).then([mt, sst, s] {
            auto fname = sstable::filename("tests/sstables/tests-temporary", la, 7, big, sstable::component_type::Index);
            return engine().open_file_dma(fname, open_flags::ro).then([] (file f) {
                auto bufptr = allocate_aligned_buffer<char>(4096, 4096);

                auto fut = f.dma_read(0, bufptr.get(), 4096);
                return std::move(fut).then([f = std::move(f), bufptr = std::move(bufptr)] (size_t size) mutable {
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
                    return f.close().finally([f]{});
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

        auto mt = make_lw_shared<memtable>(s);

        const column_definition& r1_col = *s->get_column_definition("r1");

        // Create 150 partitions so that summary file store 2 entries, assuming min index
        // interval is 128.
        for (int32_t i = 0; i < 150; i++) {
            auto key = partition_key::from_exploded(*s, {int32_type->decompose(i)});
            auto c_key = clustering_key::from_exploded(*s, {to_bytes("abc")});

            mutation m(key, s);
            m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type->decompose(1)));
            mt->apply(std::move(m));
        }

        auto sst = make_lw_shared<sstable>("tests/sstables/tests-temporary", 8, la, big);

        return sst->write_components(*mt).then([mt, sst, s] {
            auto fname = sstable::filename("tests/sstables/tests-temporary", la, 8, big, sstable::component_type::Summary);
            return engine().open_file_dma(fname, open_flags::ro).then([] (file f) {
                auto bufptr = allocate_aligned_buffer<char>(4096, 4096);

                auto fut = f.dma_read(0, bufptr.get(), 4096);
                return std::move(fut).then([f = std::move(f), bufptr = std::move(bufptr)] (size_t size) mutable {
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
                    return f.close().finally([f]{});
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

        auto mt = make_lw_shared<memtable>(s);

        const column_definition& r1_col = *s->get_column_definition("r1");

        auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
        auto c_key = clustering_key::from_exploded(*s, {to_bytes("abc")});

        mutation m(key, s);
        m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type->decompose(1)));
        mt->apply(std::move(m));

        auto sst = make_lw_shared<sstable>("tests/sstables/tests-temporary", 9, la, big);

        return sst->write_components(*mt).then([mt, sst, s] {
            auto sst2 = make_lw_shared<sstable>("tests/sstables/tests-temporary", 9, la, big);

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

SEASTAR_TEST_CASE(datafile_generation_10) {
    // Check that the component CRC was properly generated by re-computing the
    // checksum of data file and comparing it to the one stored.
    // Check that the component Digest was properly generated by using the
    // approach described above.
    return test_setup::do_with_test_directory([] {
        auto s = make_lw_shared(schema({}, some_keyspace, some_column_family,
            {{"p1", utf8_type}}, {{"c1", utf8_type}}, {{"r1", int32_type}}, {}, utf8_type));

        auto mt = make_lw_shared<memtable>(s);

        const column_definition& r1_col = *s->get_column_definition("r1");

        auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
        auto c_key = clustering_key::from_exploded(*s, {to_bytes("abc")});

        mutation m(key, s);
        m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type->decompose(1)));
        mt->apply(std::move(m));

        auto sst = make_lw_shared<sstable>("tests/sstables/tests-temporary", 10, la, big);

        return sst->write_components(*mt).then([mt, sst, s] {
            auto fname = sstable::filename("tests/sstables/tests-temporary", la, 10, big, sstable::component_type::Data);
            return engine().open_file_dma(fname, open_flags::ro).then([] (file f) {
                auto bufptr = allocate_aligned_buffer<char>(4096, 4096);

                auto fut = f.dma_read(0, bufptr.get(), 4096);
                return std::move(fut).then([f = std::move(f), bufptr = std::move(bufptr)] (size_t size) mutable {
                    assert(size > 0 && size < 4096);
                    const char* buf = bufptr.get();
                    uint32_t adler = checksum_adler32(buf, size);
                    f.close().finally([f]{});

                    auto fname = sstable::filename("tests/sstables/tests-temporary", la, 10, big, sstable::component_type::CRC);
                    return engine().open_file_dma(fname, open_flags::ro).then([adler] (file f) {
                        auto bufptr = allocate_aligned_buffer<char>(4096, 4096);

                        auto fut = f.dma_read(0, bufptr.get(), 4096);
                        return std::move(fut).then([f = std::move(f), bufptr = std::move(bufptr), adler] (size_t size) mutable {
                            size_t offset = 0;
                            auto buf = bufptr.get();

                            std::vector<uint8_t> chunk_size = { 0, 1, 0, 0 };
                            BOOST_REQUIRE(::memcmp(chunk_size.data(), &buf[offset], chunk_size.size()) == 0);
                            offset += chunk_size.size();

                            auto *nr = reinterpret_cast<const net::packed<uint32_t> *>(&buf[offset]);
                            uint32_t stored_adler = net::ntoh(*nr);
                            offset += sizeof(uint32_t);
                            BOOST_REQUIRE(adler == stored_adler);

                            BOOST_REQUIRE(size == offset);
                            return f.close().finally([f]{});
                        });
                    }).then([adler] {
                        auto fname = sstable::filename("tests/sstables/tests-temporary", la, 10, big, sstable::component_type::Digest);
                        return engine().open_file_dma(fname, open_flags::ro).then([adler] (file f) {
                            auto bufptr = allocate_aligned_buffer<char>(4096, 4096);

                            auto fut = f.dma_read(0, bufptr.get(), 4096);
                            return std::move(fut).then([f = std::move(f), bufptr = std::move(bufptr), adler] (size_t size) mutable {
                                auto buf = bufptr.get();

                                bytes stored_digest(reinterpret_cast<const signed char*>(buf), size);
                                bytes expected_digest = to_sstring<bytes>(adler);

                                BOOST_REQUIRE(size == expected_digest.size());
                                BOOST_REQUIRE(stored_digest == to_sstring<bytes>(adler));
                                return f.close().finally([f]{});
                            });
                        });
                    });
                });
            });
        });
    });
}

SEASTAR_TEST_CASE(datafile_generation_11) {
    return test_setup::do_with_test_directory([] {
        auto s = complex_schema();

        auto mt = make_lw_shared<memtable>(s);

        const column_definition& set_col = *s->get_column_definition("reg_set");
        const column_definition& static_set_col = *s->get_column_definition("static_collection");

        auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
        auto c_key = clustering_key::from_exploded(*s, {to_bytes("c1"), to_bytes("c2")});

        mutation m(key, s);

        tombstone tomb(db_clock::now_in_usecs(), gc_clock::now());
        set_type_impl::mutation set_mut{{ tomb }, {
            { to_bytes("1"), make_atomic_cell({}) },
            { to_bytes("2"), make_atomic_cell({}) },
            { to_bytes("3"), make_atomic_cell({}) }
        }};

        auto set_type = static_pointer_cast<const set_type_impl>(set_col.type);
        m.set_clustered_cell(c_key, set_col, set_type->serialize_mutation_form(set_mut));

        auto static_set_type = static_pointer_cast<const set_type_impl>(static_set_col.type);
        m.set_static_cell(static_set_col, static_set_type->serialize_mutation_form(set_mut));

        auto key2 = partition_key::from_exploded(*s, {to_bytes("key2")});
        mutation m2(key2, s);
        set_type_impl::mutation set_mut_single{{}, {{ to_bytes("4"), make_atomic_cell({}) }}};

        m2.set_clustered_cell(c_key, set_col, set_type->serialize_mutation_form(set_mut_single));

        mt->apply(std::move(m));
        mt->apply(std::move(m2));

        auto verifier = [s, set_col, c_key] (auto& mutation) {

            auto& mp = mutation->partition();
            BOOST_REQUIRE(mp.clustered_rows().size() == 1);
            auto r = mp.find_row(c_key);
            BOOST_REQUIRE(r);
            BOOST_REQUIRE(r->size() == 1);
            auto cell = r->find_cell(set_col.id);
            BOOST_REQUIRE(cell);
            auto t = static_pointer_cast<const collection_type_impl>(set_col.type);
            return t->deserialize_mutation_form(cell->as_collection_mutation());
        };

        auto sst = make_lw_shared<sstable>("tests/sstables/tests-temporary", 11, la, big);
        return sst->write_components(*mt).then([s, sst, mt, verifier, tomb, &static_set_col] {
            return reusable_sst("tests/sstables/tests-temporary", 11).then([s, verifier, tomb, &static_set_col] (auto sstp) mutable {
                return do_with(sstables::key("key1"), [sstp, s, verifier, tomb, &static_set_col] (auto& key) {
                    return sstp->read_row(s, key).then([sstp, s, verifier, tomb, &static_set_col] (auto mutation) {
                        auto verify_set = [&tomb] (auto m) {
                            BOOST_REQUIRE(bool(m.tomb) == true);
                            BOOST_REQUIRE(m.tomb == tomb);
                            BOOST_REQUIRE(m.cells.size() == 3);
                            BOOST_REQUIRE(m.cells[0].first == to_bytes("1"));
                            BOOST_REQUIRE(m.cells[1].first == to_bytes("2"));
                            BOOST_REQUIRE(m.cells[2].first == to_bytes("3"));
                        };


                        auto& mp = mutation->partition();
                        auto& ssr = mp.static_row();
                        auto scol = ssr.find_cell(static_set_col.id);
                        BOOST_REQUIRE(scol);

                        // The static set
                        auto t = static_pointer_cast<const collection_type_impl>(static_set_col.type);
                        auto mut = t->deserialize_mutation_form(scol->as_collection_mutation());
                        verify_set(mut);

                        // The clustered set
                        auto m = verifier(mutation);
                        verify_set(m);
                    });
                }).then([sstp, s, verifier] {
                    return do_with(sstables::key("key2"), [sstp, s, verifier] (auto& key) {
                        return sstp->read_row(s, key).then([sstp, s, verifier] (auto mutation) {
                            auto m = verifier(mutation);
                            BOOST_REQUIRE(!m.tomb);
                            BOOST_REQUIRE(m.cells.size() == 1);
                            BOOST_REQUIRE(m.cells[0].first == to_bytes("4"));
                        });
                    });
                });
            });
        }).then([sst, mt] {});
    });
}

SEASTAR_TEST_CASE(datafile_generation_12) {
    return test_setup::do_with_test_directory([] {
        auto s = complex_schema();

        auto mt = make_lw_shared<memtable>(s);

        auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
        auto cp = exploded_clustering_prefix({to_bytes("c1") });

        mutation m(key, s);

        tombstone tomb(db_clock::now_in_usecs(), gc_clock::now());
        m.partition().apply_delete(*s, cp, tomb);
        mt->apply(std::move(m));

        auto sst = make_lw_shared<sstable>("tests/sstables/tests-temporary", 12, la, big);
        return sst->write_components(*mt).then([s, tomb] {
            return reusable_sst("tests/sstables/tests-temporary", 12).then([s, tomb] (auto sstp) mutable {
                return do_with(sstables::key("key1"), [sstp, s, tomb] (auto& key) {
                    return sstp->read_row(s, key).then([sstp, s, tomb] (auto mutation) {
                        auto& mp = mutation->partition();
                        BOOST_REQUIRE(mp.row_tombstones().size() == 1);
                        for (auto& rt: mp.row_tombstones()) {
                            BOOST_REQUIRE(rt.t() == tomb);
                        }
                    });
                });
            });
        }).then([sst, mt] {});
    });
}

static future<> sstable_compression_test(compressor c, unsigned generation) {
    return test_setup::do_with_test_directory([c, generation] {
        // NOTE: set a given compressor algorithm to schema.
        schema_builder builder(complex_schema());
        builder.set_compressor_params(c);
        auto s = builder.build();

        auto mtp = make_lw_shared<memtable>(s);

        auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
        auto cp = exploded_clustering_prefix({to_bytes("c1") });

        mutation m(key, s);

        tombstone tomb(db_clock::now_in_usecs(), gc_clock::now());
        m.partition().apply_delete(*s, cp, tomb);
        mtp->apply(std::move(m));

        auto sst = make_lw_shared<sstable>("tests/sstables/tests-temporary", generation, la, big);
        return sst->write_components(*mtp).then([s, tomb, generation] {
            return reusable_sst("tests/sstables/tests-temporary", generation).then([s, tomb] (auto sstp) mutable {
                return do_with(sstables::key("key1"), [sstp, s, tomb] (auto& key) {
                    return sstp->read_row(s, key).then([sstp, s, tomb] (auto mutation) {
                        auto& mp = mutation->partition();
                        BOOST_REQUIRE(mp.row_tombstones().size() == 1);
                        for (auto& rt: mp.row_tombstones()) {
                            BOOST_REQUIRE(rt.t() == tomb);
                        }
                    });
                });
            });
        }).then([sst, mtp] {});
    });
}

SEASTAR_TEST_CASE(datafile_generation_13) {
    return sstable_compression_test(compressor::lz4, 13);
}

SEASTAR_TEST_CASE(datafile_generation_14) {
    return sstable_compression_test(compressor::snappy, 14);
}

SEASTAR_TEST_CASE(datafile_generation_15) {
    return sstable_compression_test(compressor::deflate, 15);
}

SEASTAR_TEST_CASE(datafile_generation_16) {
    return test_setup::do_with_test_directory([] {
        auto s = uncompressed_schema();

        auto mtp = make_lw_shared<memtable>(s);
        // Create a number of keys that is a multiple of the sampling level
        for (int i = 0; i < 0x80; ++i) {
            sstring k = "key" + to_sstring(i);
            auto key = partition_key::from_exploded(*s, {to_bytes(k)});
            mutation m(key, s);

            auto c_key = clustering_key::make_empty(*s);
            m.set_clustered_cell(c_key, to_bytes("col2"), boost::any(i), api::max_timestamp);
            mtp->apply(std::move(m));
        }

        auto sst = make_lw_shared<sstable>("tests/sstables/tests-temporary", 16, la, big);
        return sst->write_components(*mtp).then([s] {
            return reusable_sst("tests/sstables/tests-temporary", 16).then([] (auto s) {
                // Not crashing is enough
                return make_ready_future<>();
            });
        }).then([sst, mtp] {});
    });
}

////////////////////////////////  Test basic compaction support

// open_sstable() opens the requested sstable for reading only (sstables are
// immutable, so an existing sstable cannot be opened for writing).
// It returns a future because opening requires reading from disk, and
// therefore may block. The future value is a shared sstable - a reference-
// counting pointer to an sstable - allowing for the returned handle to
// be passed around until no longer needed.
static future<sstables::shared_sstable> open_sstable(sstring dir, unsigned long generation) {
    auto sst = make_lw_shared<sstables::sstable>(dir, generation,
            sstables::sstable::version_types::la,
            sstables::sstable::format_types::big);
    auto fut = sst->load();
    return fut.then([sst = std::move(sst)] { return std::move(sst); });
}

// open_sstables() opens several generations of the same sstable, returning,
// after all the tables have been open, their vector.
static future<std::vector<sstables::shared_sstable>> open_sstables(sstring dir, std::vector<unsigned long> generations) {
    return do_with(std::vector<sstables::shared_sstable>(),
            [dir = std::move(dir), generations = std::move(generations)] (auto& ret) mutable {
        return parallel_for_each(generations, [&ret, &dir] (unsigned long generation) {
            return open_sstable(dir, generation).then([&ret] (sstables::shared_sstable sst) {
                ret.push_back(std::move(sst));
            });
        }).then([&ret] {
            return std::move(ret);
        });
    });
}

// mutation_reader for sstable keeping all the required objects alive.
static ::mutation_reader sstable_reader(shared_sstable sst, schema_ptr s) {
    // TODO: s is probably not necessary, as the read_rows() object keeps a copy of it.
    return [sst, s, r = make_lw_shared(sst->read_rows(s))] () mutable { return r->read(); };

}


SEASTAR_TEST_CASE(compact) {
    constexpr int generation = 17;
    // The "compaction" sstable was created with the following schema:
    // CREATE TABLE compaction (
    //        name text,
    //        age int,
    //        height int,
    //        PRIMARY KEY (name)
    //);
    schema_ptr s = make_lw_shared(schema({}, "tests", "compaction",
            // partition key
            {{"name", utf8_type}},
            // clustering key
            {},
            // regular columns
            {
                {"age", int32_type},
                {"height", int32_type},
            },
            // static columns
            {},
            // regular column name type
            utf8_type,
            // comment
            "Example table for compaction"
           ));

    return open_sstables("tests/sstables/compaction", {1,2,3}).then([s = std::move(s), generation] (auto sstables) {
        return test_setup::do_with_test_directory([sstables, s, generation] {
            auto new_sstable = [generation] {
                return make_lw_shared<sstables::sstable>("tests/sstables/tests-temporary",
                        generation, sstables::sstable::version_types::la, sstables::sstable::format_types::big);
            };
            return sstables::compact_sstables(std::move(sstables), std::move(s), new_sstable).then([s, generation] {
                // Verify that the compacted sstable has the right content. We expect to see:
                //  name  | age | height
                // -------+-----+--------
                //  jerry |  40 |    170
                //    tom |  20 |    180
                //   john |  20 |   deleted
                //   nadav - deleted partition
                return open_sstable("tests/sstables/tests-temporary", generation).then([s] (shared_sstable sst) {
                    auto reader = sstable_reader(sst, s); // reader holds sst and s alive.
                    return reader().then([reader, s] (mutation_opt m) {
                        BOOST_REQUIRE(m);
                        BOOST_REQUIRE(m->key().representation() == bytes("jerry"));
                        BOOST_REQUIRE(!m->partition().partition_tombstone());
                        auto &rows = m->partition().clustered_rows();
                        BOOST_REQUIRE(rows.size() == 1);
                        auto &row = rows.begin()->row();
                        BOOST_REQUIRE(!row.deleted_at());
                        auto &cells = row.cells();
                        BOOST_REQUIRE(cells.cell_at(s->get_column_definition("age")->id).as_atomic_cell().value() == bytes({0,0,0,40}));
                        BOOST_REQUIRE(cells.cell_at(s->get_column_definition("height")->id).as_atomic_cell().value() == bytes({0,0,0,(char)170}));
                        return reader();
                    }).then([reader, s] (mutation_opt m) {
                        BOOST_REQUIRE(m);
                        BOOST_REQUIRE(m->key().representation() == bytes("tom"));
                        BOOST_REQUIRE(!m->partition().partition_tombstone());
                        auto &rows = m->partition().clustered_rows();
                        BOOST_REQUIRE(rows.size() == 1);
                        auto &row = rows.begin()->row();
                        BOOST_REQUIRE(!row.deleted_at());
                        auto &cells = row.cells();
                        BOOST_REQUIRE(cells.cell_at(s->get_column_definition("age")->id).as_atomic_cell().value() == bytes({0,0,0,20}));
                        BOOST_REQUIRE(cells.cell_at(s->get_column_definition("height")->id).as_atomic_cell().value() == bytes({0,0,0,(char)180}));
                        return reader();
                    }).then([reader, s] (mutation_opt m) {
                        BOOST_REQUIRE(m);
                        BOOST_REQUIRE(m->key().representation() == bytes("john"));
                        BOOST_REQUIRE(!m->partition().partition_tombstone());
                        auto &rows = m->partition().clustered_rows();
                        BOOST_REQUIRE(rows.size() == 1);
                        auto &row = rows.begin()->row();
                        BOOST_REQUIRE(!row.deleted_at());
                        auto &cells = row.cells();
                        BOOST_REQUIRE(cells.cell_at(s->get_column_definition("age")->id).as_atomic_cell().value() == bytes({0,0,0,20}));
                        BOOST_REQUIRE(cells.find_cell(s->get_column_definition("height")->id) == nullptr);
                        return reader();
                    }).then([reader, s] (mutation_opt m) {
                        BOOST_REQUIRE(m);
                        BOOST_REQUIRE(m->key().representation() == bytes("nadav"));
                        BOOST_REQUIRE(m->partition().partition_tombstone());
                        // FIXME: enable the following test.
                        //auto &rows = m->partition().clustered_rows();
                        //BOOST_REQUIRE(rows.size() == 0);
                        return reader();
                    }).then([reader] (mutation_opt m) {
                        BOOST_REQUIRE(!m);
                    });
                });
            });
        });
    });

    // verify that the compacted sstable look like
}

// Used to be compatible with API provided by size_tiered_most_interesting_bucket().
static lw_shared_ptr<sstable_list> create_sstable_list(std::vector<sstables::shared_sstable>& sstables) {
    sstable_list list;
    for (auto& sst : sstables) {
        list.insert({sst->generation(), sst});
    }
    return make_lw_shared<sstable_list>(std::move(list));
}

static future<> compact_sstables(std::vector<unsigned long> generations_to_compact, unsigned long new_generation, bool create_sstables = true) {
    auto s = make_lw_shared(schema({}, some_keyspace, some_column_family,
        {{"p1", utf8_type}}, {{"c1", utf8_type}}, {{"r1", int32_type}}, {}, utf8_type));

    auto generations = make_lw_shared<std::vector<unsigned long>>(std::move(generations_to_compact));
    auto sstables = make_lw_shared<std::vector<sstables::shared_sstable>>();

    auto f = make_ready_future<>();

    return f.then([generations, sstables, s, create_sstables] () mutable {
        if (!create_sstables) {
            return open_sstables("tests/sstables/tests-temporary", *generations).then([sstables] (auto opened_sstables) mutable {
                for (auto& sst : opened_sstables) {
                    sstables->push_back(sst);
                }
                return make_ready_future<>();
            });
        }
        return do_for_each(*generations, [generations, sstables, s] (unsigned long generation) {
            auto mt = make_lw_shared<memtable>(s);

            const column_definition& r1_col = *s->get_column_definition("r1");

            sstring k = "key" + to_sstring(generation);
            auto key = partition_key::from_exploded(*s, {to_bytes(k)});
            auto c_key = clustering_key::from_exploded(*s, {to_bytes("abc")});

            mutation m(key, s);
            m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type->decompose(1)));
            mt->apply(std::move(m));

            auto sst = make_lw_shared<sstable>("tests/sstables/tests-temporary", generation, la, big);

            return sst->write_components(*mt).then([mt, sst, s, sstables] {
                return sst->load().then([sst, sstables] {
                    sstables->push_back(sst);
                    return make_ready_future<>();
                });
            });
        });
    }).then([s, sstables, new_generation, generations] {
        unsigned long generation = new_generation;
        auto new_sstable = [generation] {
            return make_lw_shared<sstables::sstable>("tests/sstables/tests-temporary",
                    generation, sstables::sstable::version_types::la, sstables::sstable::format_types::big);
        };
        // We must have opened at least all original candidates.
        BOOST_REQUIRE(generations->size() == sstables->size());

        auto sstable_list = create_sstable_list(*sstables);
        // Calling function that will return a list of sstables to compact based on size-tiered strategy.
        auto sstables_to_compact = size_tiered_most_interesting_bucket(sstable_list);
        // We do expect that all candidates were selected for compaction (in this case).
        BOOST_REQUIRE(sstables_to_compact.size() == sstables->size());

        return sstables::compact_sstables(std::move(sstables_to_compact), s, new_sstable).then([s] {});
    });
}

static future<> check_compacted_sstables(unsigned long generation, std::vector<unsigned long> compacted_generations) {
    auto s = make_lw_shared(schema({}, some_keyspace, some_column_family,
        {{"p1", utf8_type}}, {{"c1", utf8_type}}, {{"r1", int32_type}}, {}, utf8_type));

    auto generations = make_lw_shared<std::vector<unsigned long>>(std::move(compacted_generations));

    return open_sstable("tests/sstables/tests-temporary", generation).then([s, generations] (shared_sstable sst) {
        auto reader = sstable_reader(sst, s); // reader holds sst and s alive.
        auto keys = make_lw_shared<std::vector<bytes>>();

        return do_for_each(*generations, [reader, s, keys] (unsigned long generation) {
            return reader().then([generation, keys, reader] (mutation_opt m) {
                BOOST_REQUIRE(m);
                bytes key = to_bytes(m->key().representation());
                keys->push_back(key);
                return make_ready_future<>();
            });
        }).then([keys, generations] {
            // keys from compacted sstable aren't ordered lexographically,
            // thus we must read all keys into a vector, sort the vector
            // lexographically, then proceed with the comparison.
            std::sort(keys->begin(), keys->end());
            BOOST_REQUIRE(keys->size() == generations->size());
            auto i = 0;
            for (auto& k : *keys) {
                sstring original_k = "key" + to_sstring((*generations)[i++]);
                BOOST_REQUIRE(k == to_bytes(original_k));
            }
            return make_ready_future<>();
        });
    });
}

SEASTAR_TEST_CASE(compact_02) {
    // NOTE: generations 18 to 38 are used here.

    // This tests size-tiered compaction strategy by creating 4 sstables of
    // similar size and compacting them to create a new tier.
    // The process above is repeated 4 times until you have 4 compacted
    // sstables of similar size. Then you compact these 4 compacted sstables,
    // and make sure that you have all partition keys.
    // By the way, automatic compaction isn't tested here, instead the
    // strategy algorithm that selects candidates for compaction.

    return test_setup::do_with_test_directory([] {
        // Compact 4 sstables into 1 using size-tiered strategy to select sstables.
        // E.g.: generations 18, 19, 20 and 21 will be compacted into generation 22.
        return compact_sstables({ 18, 19, 20, 21 }, 22).then([] {
            // Check that generation 22 contains all keys of generations 18, 19, 20 and 21.
            return check_compacted_sstables(22, { 18, 19, 20, 21 });
        }).then([] {
            return compact_sstables({ 23, 24, 25, 26 }, 27).then([] {
                return check_compacted_sstables(27, { 23, 24, 25, 26 });
            });
        }).then([] {
            return compact_sstables({ 28, 29, 30, 31 }, 32).then([] {
                return check_compacted_sstables(32, { 28, 29, 30, 31 });
            });
        }).then([] {
            return compact_sstables({ 33, 34, 35, 36 }, 37).then([] {
                return check_compacted_sstables(37, { 33, 34, 35, 36 });
            });
        }).then([] {
            // In this step, we compact 4 compacted sstables.
            return compact_sstables({ 22, 27, 32, 37 }, 38, false).then([] {
                // Check that the compacted sstable contains all keys.
                return check_compacted_sstables(38,
                    { 18, 19, 20, 21, 23, 24, 25, 26, 28, 29, 30, 31, 33, 34, 35, 36 });
            });
        });
    });
}

SEASTAR_TEST_CASE(datafile_generation_37) {
    return test_setup::do_with_test_directory([] {
        auto s = compact_simple_dense_schema();

        auto mtp = make_lw_shared<memtable>(s);

        auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
        mutation m(key, s);

        auto c_key = exploded_clustering_prefix({to_bytes("cl1") });
        const column_definition& cl2 = *s->get_column_definition("cl2");

        m.set_clustered_cell(c_key, cl2, make_atomic_cell(bytes_type->decompose(to_bytes("cl2"))));
        mtp->apply(std::move(m));

        auto sst = make_lw_shared<sstable>("tests/sstables/tests-temporary", 37, la, big);
        return sst->write_components(*mtp).then([s] {
            return reusable_sst("tests/sstables/tests-temporary", 37).then([s] (auto sstp) {
                return do_with(sstables::key("key1"), [sstp, s] (auto& key) {
                    return sstp->read_row(s, key).then([sstp, s] (auto mutation) {
                        auto& mp = mutation->partition();

                        auto exploded = exploded_clustering_prefix({"cl1"});
                        auto clustering = clustering_key::from_clustering_prefix(*s, exploded);

                        auto row = mp.clustered_row(clustering);
                        match_live_cell(row.cells(), *s, "cl2", boost::any(to_bytes("cl2")));
                        return make_ready_future<>();
                    });
                });
            });
        }).then([sst, mtp, s] {});
    });
}

SEASTAR_TEST_CASE(datafile_generation_38) {
    return test_setup::do_with_test_directory([] {
        auto s = compact_dense_schema();

        auto mtp = make_lw_shared<memtable>(s);

        auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
        mutation m(key, s);

        auto exploded = exploded_clustering_prefix({"cl1", "cl2"});
        auto c_key = clustering_key::from_clustering_prefix(*s, exploded);

        const column_definition& cl3 = *s->get_column_definition("cl3");
        m.set_clustered_cell(c_key, cl3, make_atomic_cell(bytes_type->decompose(to_bytes("cl3"))));
        mtp->apply(std::move(m));

        auto sst = make_lw_shared<sstable>("tests/sstables/tests-temporary", 38, la, big);
        return sst->write_components(*mtp).then([s] {
            return reusable_sst("tests/sstables/tests-temporary", 38).then([s] (auto sstp) {
                return do_with(sstables::key("key1"), [sstp, s] (auto& key) {
                    return sstp->read_row(s, key).then([sstp, s] (auto mutation) {
                        auto& mp = mutation->partition();
                        auto exploded = exploded_clustering_prefix({"cl1", "cl2"});
                        auto clustering = clustering_key::from_clustering_prefix(*s, exploded);

                        auto row = mp.clustered_row(clustering);
                        match_live_cell(row.cells(), *s, "cl3", boost::any(to_bytes("cl3")));
                        return make_ready_future<>();
                    });
                });
            });
        }).then([sst, mtp, s] {});
    });
}

SEASTAR_TEST_CASE(datafile_generation_39) {
    return test_setup::do_with_test_directory([] {
        auto s = compact_sparse_schema();

        auto mtp = make_lw_shared<memtable>(s);

        auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
        mutation m(key, s);

        auto c_key = clustering_key::make_empty(*s);

        const column_definition& cl1 = *s->get_column_definition("cl1");
        m.set_clustered_cell(c_key, cl1, make_atomic_cell(bytes_type->decompose(to_bytes("cl1"))));
        const column_definition& cl2 = *s->get_column_definition("cl2");
        m.set_clustered_cell(c_key, cl2, make_atomic_cell(bytes_type->decompose(to_bytes("cl2"))));
        mtp->apply(std::move(m));

        auto sst = make_lw_shared<sstable>("tests/sstables/tests-temporary", 39, la, big);
        return sst->write_components(*mtp).then([s] {
            return reusable_sst("tests/sstables/tests-temporary", 39).then([s] (auto sstp) {
                return do_with(sstables::key("key1"), [sstp, s] (auto& key) {
                    return sstp->read_row(s, key).then([sstp, s] (auto mutation) {
                        auto& mp = mutation->partition();
                        auto row = mp.clustered_row(clustering_key::make_empty(*s));
                        match_live_cell(row.cells(), *s, "cl1", boost::any(to_bytes("cl1")));
                        match_live_cell(row.cells(), *s, "cl2", boost::any(to_bytes("cl2")));
                        return make_ready_future<>();
                    });
                });
            });
        }).then([sst, mtp, s] {});
    });
}

SEASTAR_TEST_CASE(datafile_generation_40) {
    return test_setup::do_with_test_directory([] {
        // Data file with clustering key sorted in descending order
        //
        // Respective CQL table and CQL insert:
        // CREATE TABLE table (
        //    p1 text,
        //    c1 text,
        //    r1 int,
        //    PRIMARY KEY (p1, c1)
        // ) WITH compact storage and compression = {} and clustering order by (cl1 desc);
        // INSERT INTO table (p1, c1, r1) VALUES ('key1', 'a', 1);
        // INSERT INTO table (p1, c1, r1) VALUES ('key1', 'b', 1);

        auto s = [] {
            schema_builder builder(make_lw_shared(schema({}, some_keyspace, some_column_family,
                {{"p1", utf8_type}}, {{"c1", reversed_type_impl::get_instance(utf8_type)}}, {{"r1", int32_type}}, {}, utf8_type
            )));
            return builder.build(schema_builder::compact_storage::yes);
        }();

        auto mt = make_lw_shared<memtable>(s);
        auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
        mutation m(key, s);

        const column_definition& r1_col = *s->get_column_definition("r1");
        auto ca = clustering_key::from_exploded(*s, {to_bytes("a")});
        m.set_clustered_cell(ca, r1_col, make_atomic_cell(int32_type->decompose(1)));
        mt->apply(std::move(m));

        auto cb = clustering_key::from_exploded(*s, {to_bytes("b")});
        m.set_clustered_cell(cb, r1_col, make_atomic_cell(int32_type->decompose(1)));
        mt->apply(std::move(m));

        auto sst = make_lw_shared<sstable>("tests/sstables/tests-temporary", 40, la, big);

        return sst->write_components(*mt).then([mt, sst, s] {
            auto fname = sstable::filename("tests/sstables/tests-temporary", la, 40, big, sstable::component_type::Data);
            return engine().open_file_dma(fname, open_flags::ro).then([] (file f) {
                auto bufptr = allocate_aligned_buffer<char>(4096, 4096);

                auto fut = f.dma_read(0, bufptr.get(), 4096);
                return std::move(fut).then([f = std::move(f), bufptr = std::move(bufptr)] (size_t size) mutable {
                    auto buf = bufptr.get();
                    size_t offset = 0;
                    auto check_chunk = [buf, &offset] (std::vector<uint8_t> vec) {
                        BOOST_REQUIRE(::memcmp(vec.data(), &buf[offset], vec.size()) == 0);
                        offset += vec.size();
                    };
                    check_chunk({ /* first key */ 0, 4, 'k', 'e', 'y', '1' });
                    check_chunk({ /* deletion time */ 0x7f, 0xff, 0xff, 0xff, 0x80, 0, 0, 0, 0, 0, 0, 0 });
                    check_chunk({ /* first expected row name */ 0, 1, 'b' });
                    check_chunk(/* row contents, same for both */ {/* mask */ 0, /* timestamp */ 0, 0, 0, 0, 0, 0, 0, 0, /* value */ 0, 0, 0, 4, 0, 0, 0, 1 });
                    check_chunk({ /* second expected row name */ 0, 1, 'a' });
                    check_chunk(/* row contents, same for both */ {/* mask */ 0, /* timestamp */ 0, 0, 0, 0, 0, 0, 0, 0, /* value */ 0, 0, 0, 4, 0, 0, 0, 1 });
                    return f.close().finally([f] {});
                });
            });
        });
    });
}
