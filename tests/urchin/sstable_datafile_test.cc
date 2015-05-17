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

    return remove_file("tests/urchin/sstables/Data.tmp.db").then_wrapped([mtp] (future<> ret) {
        return sstables::write_datafile(*mtp, "tests/urchin/sstables/Data.tmp.db");
    }).then([mtp, s] {
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

    memtable mt(s);

    const column_definition& r1_col = *s->get_column_definition("r1");

    auto key = partition_key::from_exploded(*s, {to_bytes("key1"), to_bytes("key2")});
    auto c_key = clustering_key::from_exploded(*s, {to_bytes("abc")});

    mutation m(key, s);
    m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type->decompose(1)));
    mt.apply(std::move(m));

    auto mtp = make_shared<memtable>(std::move(mt));

    return remove_file("tests/urchin/sstables/Data2.tmp.db").then_wrapped([mtp] (future<> ret) {
        return sstables::write_datafile(*mtp, "tests/urchin/sstables/Data2.tmp.db");
    }).then([mtp, s] {
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

    memtable mt(s);

    const column_definition& r1_col = *s->get_column_definition("r1");

    auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
    auto c_key = clustering_key::from_exploded(*s, {to_bytes("abc"), to_bytes("cde")});

    mutation m(key, s);
    m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type->decompose(1)));
    mt.apply(std::move(m));

    auto mtp = make_shared<memtable>(std::move(mt));

    return remove_file("tests/urchin/sstables/Data3.tmp.db").then_wrapped([mtp] (future<> ret) {
        return sstables::write_datafile(*mtp, "tests/urchin/sstables/Data3.tmp.db");
    }).then([mtp, s] {
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

    return remove_file("tests/urchin/sstables/Data4.tmp.db").then_wrapped([mtp] (future<> f) {
        return sstables::write_datafile(*mtp, "tests/urchin/sstables/Data4.tmp.db");
    }).then([mtp, s] {
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

    return sstables::write_datafile(*mtp, "tests/urchin/sstables/Data5.tmp.db").then([mtp, s] {
        return engine().open_file_dma("tests/urchin/sstables/Data5.tmp.db", open_flags::ro).then([] (file f) {
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
        }).then([] {
            return remove_file("tests/urchin/sstables/Data5.tmp.db");
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

    return sstables::write_datafile(*mtp, "tests/urchin/sstables/Data6.tmp.db").then([mtp, s] {
        return engine().open_file_dma("tests/urchin/sstables/Data6.tmp.db", open_flags::ro).then([] (file f) {
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
        }).then([] {
            return remove_file("tests/urchin/sstables/Data6.tmp.db");
        });
    });
}
