/*
 * Copyright (C) 2015 ScyllaDB
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
#include "sstables/leveled_manifest.hh"
#include <memory>
#include "sstable_test.hh"
#include "core/seastar.hh"
#include "core/do_with.hh"
#include "sstables/compaction_manager.hh"
#include "tmpdir.hh"
#include "dht/i_partitioner.hh"
#include "dht/murmur3_partitioner.hh"
#include "range.hh"
#include "partition_slice_builder.hh"
#include "sstables/compaction_strategy_impl.hh"
#include "sstables/date_tiered_compaction_strategy.hh"
#include "sstables/time_window_compaction_strategy.hh"
#include "mutation_assertions.hh"
#include "counters.hh"
#include "cell_locking.hh"
#include "simple_schema.hh"
#include "memtable-sstable.hh"
#include "tests/index_reader_assertions.hh"
#include "flat_mutation_reader_assertions.hh"
#include "tests/make_random_string.hh"
#include "tests/normalizing_reader.hh"

#include <stdio.h>
#include <ftw.h>
#include <unistd.h>
#include <boost/range/algorithm/find_if.hpp>
#include <boost/algorithm/cxx11/all_of.hpp>
#include <boost/algorithm/cxx11/is_sorted.hpp>
#include "test_services.hh"

#include "sstable_utils.hh"

using namespace sstables;

static sstring some_keyspace("ks");
static sstring some_column_family("cf");

static db::nop_large_partition_handler nop_lp_handler;

static column_family::config column_family_test_config() {
    column_family::config cfg;
    cfg.large_partition_handler = &nop_lp_handler;
    return cfg;
}

atomic_cell make_atomic_cell(data_type dt, bytes_view value, uint32_t ttl = 0, uint32_t expiration = 0) {
    if (ttl) {
        return atomic_cell::make_live(*dt, 0, value,
            gc_clock::time_point(gc_clock::duration(expiration)), gc_clock::duration(ttl));
    } else {
        return atomic_cell::make_live(*dt, 0, value);
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

    return test_setup::do_with_tmp_directory([] (sstring tmpdir_path) {
        schema_builder builder(make_lw_shared(schema({}, some_keyspace, some_column_family,
            {{"p1", utf8_type}}, {{"c1", utf8_type}}, {{"r1", int32_type}, {"r2", int32_type}}, {}, utf8_type)));
        builder.set_compressor_params(compression_parameters::no_compression());
        auto s = builder.build(schema_builder::compact_storage::no);

        auto mt = make_lw_shared<memtable>(s);

        const column_definition& r1_col = *s->get_column_definition("r1");

        auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
        auto c_key = clustering_key::from_exploded(*s, {to_bytes("abc")});

        mutation m(s, key);
        m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type, int32_type->decompose(1)));
        mt->apply(std::move(m));

        auto sst = make_sstable(s, tmpdir_path, 1, la, big);

        auto fname = sstable::filename(tmpdir_path, "ks", "cf", la, 1, big, component_type::Data);
        return write_memtable_to_sstable_for_test(*mt, sst).then([mt, sst, s, fname] {
            return open_file_dma(fname, open_flags::ro).then([] (file f) {
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
    return test_setup::do_with_tmp_directory([] (sstring tmpdir_path) {
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

        schema_builder builder(make_lw_shared(schema({}, some_keyspace, some_column_family,
            {{"p1", utf8_type}, {"p2", utf8_type}}, {{"c1", utf8_type}}, {{"r1", int32_type}}, {}, utf8_type)));
        builder.set_compressor_params(compression_parameters::no_compression());
        auto s = builder.build(schema_builder::compact_storage::no);

        auto mt = make_lw_shared<memtable>(s);

        const column_definition& r1_col = *s->get_column_definition("r1");

        auto key = partition_key::from_exploded(*s, {to_bytes("key1"), to_bytes("key2")});
        auto c_key = clustering_key::from_exploded(*s, {to_bytes("abc")});

        mutation m(s, key);
        m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type, int32_type->decompose(1)));
        mt->apply(std::move(m));

        auto sst = make_sstable(s, tmpdir_path, 2, la, big);

        auto fname = sstable::filename(tmpdir_path, "ks", "cf", la, 2, big, component_type::Data);
        return write_memtable_to_sstable_for_test(*mt, sst).then([mt, sst, s, fname] {
            return open_file_dma(fname, open_flags::ro).then([] (file f) {
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
    return test_setup::do_with_tmp_directory([] (sstring tmpdir_path) {
        schema_builder builder(make_lw_shared(schema({}, some_keyspace, some_column_family,
            {{"p1", utf8_type}}, {{"c1", utf8_type}, {"c2", utf8_type}}, {{"r1", int32_type}}, {}, utf8_type)));
        builder.set_compressor_params(compression_parameters::no_compression());
        auto s = builder.build(schema_builder::compact_storage::no);

        auto mt = make_lw_shared<memtable>(s);

        const column_definition& r1_col = *s->get_column_definition("r1");

        auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
        auto c_key = clustering_key::from_exploded(*s, {to_bytes("abc"), to_bytes("cde")});

        mutation m(s, key);
        m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type, int32_type->decompose(1)));
        mt->apply(std::move(m));

        auto sst = make_sstable(s, tmpdir_path, 3, la, big);

        auto fname = sstable::filename(tmpdir_path, "ks", "cf", la, 3, big, component_type::Data);
        return write_memtable_to_sstable_for_test(*mt, sst).then([mt, sst, s, fname] {
            return open_file_dma(fname, open_flags::ro).then([] (file f) {
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
    return test_setup::do_with_tmp_directory([] (sstring tmpdir_path) {
        schema_builder builder(make_lw_shared(schema({}, some_keyspace, some_column_family,
            {{"p1", utf8_type}}, {{"c1", utf8_type}}, {{"r1", int32_type}}, {{"s1", int32_type}}, utf8_type)));
        builder.set_compressor_params(compression_parameters::no_compression());
        auto s = builder.build(schema_builder::compact_storage::no);

        auto mt = make_lw_shared<memtable>(s);

        const column_definition& r1_col = *s->get_column_definition("r1");
        const column_definition& s1_col = *s->get_column_definition("s1");

        auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
        auto c_key = clustering_key::from_exploded(*s, {to_bytes("abc")});

        mutation m(s, key);
        m.set_static_cell(s1_col, make_atomic_cell(int32_type, int32_type->decompose(10)));
        m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type, int32_type->decompose(1)));
        mt->apply(std::move(m));

        auto sst = make_sstable(s, tmpdir_path, 4, la, big);

        auto fname = sstable::filename(tmpdir_path, "ks", "cf", la, 4, big, component_type::Data);
        return write_memtable_to_sstable_for_test(*mt, sst).then([mt, sst, s, fname] {
            return open_file_dma(fname, open_flags::ro).then([] (file f) {
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
    return test_setup::do_with_tmp_directory([] (sstring tmpdir_path) {
        schema_builder builder(make_lw_shared(schema({}, some_keyspace, some_column_family,
            {{"p1", utf8_type}}, {{"c1", utf8_type}}, {{"r1", int32_type}}, {}, utf8_type)));
        builder.set_compressor_params(compression_parameters::no_compression());
        auto s = builder.build(schema_builder::compact_storage::no);

        auto mt = make_lw_shared<memtable>(s);

        const column_definition& r1_col = *s->get_column_definition("r1");

        auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
        auto c_key = clustering_key::from_exploded(*s, {to_bytes("abc")});

        mutation m(s, key);
        m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type, int32_type->decompose(1), 3600, 3600));
        mt->apply(std::move(m));

        auto now = to_gc_clock(db_clock::from_time_t(0));
        auto sst = make_sstable(s, tmpdir_path, 5, la, big, now);

        return write_memtable_to_sstable_for_test(*mt, sst).then([mt, sst, s, tmpdir_path] {
            auto fname = sstable::filename(tmpdir_path, "ks", "cf", la, 5, big, component_type::Data);
            return open_file_dma(fname, open_flags::ro).then([] (file f) {
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
    return test_setup::do_with_tmp_directory([] (sstring tmpdir_path) {
        schema_builder builder(make_lw_shared(schema({}, some_keyspace, some_column_family,
            {{"p1", utf8_type}}, {{"c1", utf8_type}}, {{"r1", int32_type}}, {}, utf8_type)));
        builder.set_compressor_params(compression_parameters::no_compression());
        auto s = builder.build(schema_builder::compact_storage::no);

        auto mt = make_lw_shared<memtable>(s);

        const column_definition& r1_col = *s->get_column_definition("r1");

        auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
        auto c_key = clustering_key::from_exploded(*s, {to_bytes("abc")});

        mutation m(s, key);
        m.set_clustered_cell(c_key, r1_col, make_dead_atomic_cell(3600));
        mt->apply(std::move(m));

        auto sst = make_sstable(s, tmpdir_path, 6, la, big);

        return write_memtable_to_sstable_for_test(*mt, sst).then([mt, sst, s, tmpdir_path] {
            auto fname = sstable::filename(tmpdir_path, "ks", "cf", la, 6, big, component_type::Data);
            return open_file_dma(fname, open_flags::ro).then([] (file f) {
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
    return test_setup::do_with_tmp_directory([] (sstring tmpdir_path) {
        auto s = make_lw_shared(schema({}, some_keyspace, some_column_family,
            {{"p1", utf8_type}}, {{"c1", utf8_type}}, {{"r1", int32_type}}, {}, utf8_type));

        auto mt = make_lw_shared<memtable>(s);

        const column_definition& r1_col = *s->get_column_definition("r1");

        auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
        auto c_key = clustering_key::from_exploded(*s, {to_bytes("abc")});

        mutation m(s, key);
        m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type, int32_type->decompose(1)));
        mt->apply(std::move(m));

        auto key2 = partition_key::from_exploded(*s, {to_bytes("key2")});
        auto c_key2 = clustering_key::from_exploded(*s, {to_bytes("cde")});

        mutation m2(s, key2);
        m2.set_clustered_cell(c_key2, r1_col, make_atomic_cell(int32_type, int32_type->decompose(1)));
        mt->apply(std::move(m2));

        auto sst = make_sstable(s, tmpdir_path, 7, la, big);

        return write_memtable_to_sstable_for_test(*mt, sst).then([mt, sst, s, tmpdir_path] {
            auto fname = sstable::filename(tmpdir_path, "ks", "cf", la, 7, big, component_type::Index);
            return open_file_dma(fname, open_flags::ro).then([] (file f) {
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
    return test_setup::do_with_tmp_directory([] (sstring tmpdir_path) {
        auto s = make_lw_shared(schema({}, some_keyspace, some_column_family,
            {{"p1", int32_type}}, {{"c1", utf8_type}}, {{"r1", int32_type}}, {}, utf8_type));

        auto mt = make_lw_shared<memtable>(s);

        const column_definition& r1_col = *s->get_column_definition("r1");

        // TODO: generate sstable which will have 2 samples with size-based sampling.
        for (int32_t i = 0; i < 150; i++) {
            auto key = partition_key::from_exploded(*s, {int32_type->decompose(i)});
            auto c_key = clustering_key::from_exploded(*s, {to_bytes("abc")});

            mutation m(s, key);
            m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type, int32_type->decompose(1)));
            mt->apply(std::move(m));
        }

        auto sst = make_sstable(s, tmpdir_path, 8, la, big);

        return write_memtable_to_sstable_for_test(*mt, sst).then([mt, sst, s, tmpdir_path] {
            auto fname = sstable::filename(tmpdir_path, "ks", "cf", la, 8, big, component_type::Summary);
            return open_file_dma(fname, open_flags::ro).then([] (file f) {
                auto bufptr = allocate_aligned_buffer<char>(4096, 4096);

                auto fut = f.dma_read(0, bufptr.get(), 4096);
                return std::move(fut).then([f = std::move(f), bufptr = std::move(bufptr)] (size_t size) mutable {
                    auto buf = bufptr.get();
                    size_t offset = 0;

                    std::vector<uint8_t> header = { /* min_index_interval */ 0, 0, 0, 0x80, /* size */ 0, 0, 0, 1,
                        /* memory_size */ 0, 0, 0, 0, 0, 0, 0, 0x10, /* sampling_level */ 0, 0, 0, 0x80,
                        /* size_at_full_sampling */  0, 0, 0, 1 };
                    BOOST_REQUIRE(::memcmp(header.data(), &buf[offset], header.size()) == 0);
                    offset += header.size();

                    std::vector<uint8_t> positions = { 0x4, 0, 0, 0 };
                    BOOST_REQUIRE(::memcmp(positions.data(), &buf[offset], positions.size()) == 0);
                    offset += positions.size();

                    std::vector<uint8_t> first_entry = { /* key */ 0, 0, 0, 0x17, /* position */ 0, 0, 0, 0, 0, 0, 0, 0 };
                    BOOST_REQUIRE(::memcmp(first_entry.data(), &buf[offset], first_entry.size()) == 0);
                    offset += first_entry.size();

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
    return test_setup::do_with_tmp_directory([] (sstring tmpdir_path) {
        auto s = make_lw_shared(schema({}, some_keyspace, some_column_family,
            {{"p1", utf8_type}}, {{"c1", utf8_type}}, {{"r1", int32_type}}, {}, utf8_type));

        auto mt = make_lw_shared<memtable>(s);

        const column_definition& r1_col = *s->get_column_definition("r1");

        auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
        auto c_key = clustering_key::from_exploded(*s, {to_bytes("abc")});

        mutation m(s, key);
        m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type, int32_type->decompose(1)));
        mt->apply(std::move(m));

        auto sst = make_sstable(s, tmpdir_path, 9, la, big);

        return write_memtable_to_sstable_for_test(*mt, sst).then([mt, sst, s, tmpdir_path] {
            auto sst2 = make_sstable(s, tmpdir_path, 9, la, big);

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

template <typename ChecksumType>
static future<> test_digest_and_checksum(sstable_version_types version) {
    // Check that the component Digest was properly generated by using the
    // approach described above.
    return test_setup::do_with_tmp_directory([version] (sstring tmpdir_path) {
        schema_builder builder(make_lw_shared(schema({}, some_keyspace, some_column_family,
            {{"p1", utf8_type}}, {{"c1", utf8_type}}, {{"r1", int32_type}}, {}, utf8_type)));
        builder.set_compressor_params(compression_parameters::no_compression());
        auto s = builder.build(schema_builder::compact_storage::no);

        auto mt = make_lw_shared<memtable>(s);

        const column_definition& r1_col = *s->get_column_definition("r1");

        auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
        auto c_key = clustering_key::from_exploded(*s, {to_bytes("abc")});

        mutation m(s, key);
        m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type, int32_type->decompose(1)));
        mt->apply(std::move(m));

        auto sst = make_sstable(s, tmpdir_path, 10, version, big);

        return write_memtable_to_sstable_for_test(*mt, sst).then([mt, sst, s, version, tmpdir_path] {
            auto fname = sstable::filename(tmpdir_path, "ks", "cf", version, 10, big, component_type::Data);
            return open_file_dma(fname, open_flags::ro).then([version, tmpdir_path] (file f) {
                auto bufptr = allocate_aligned_buffer<char>(4096, 4096);

                auto fut = f.dma_read(0, bufptr.get(), 4096);
                return std::move(fut).then([f = std::move(f), bufptr = std::move(bufptr), version, tmpdir_path] (size_t size) mutable {
                    assert(size > 0 && size < 4096);
                    const char* buf = bufptr.get();
                    uint32_t checksum = ChecksumType::checksum(buf, size);
                    f.close().finally([f]{});

                    auto fname = sstable::filename(tmpdir_path, "ks", "cf", version, 10, big, component_type::CRC);
                    return open_file_dma(fname, open_flags::ro).then([checksum] (file f) {
                        auto bufptr = allocate_aligned_buffer<char>(4096, 4096);

                        auto fut = f.dma_read(0, bufptr.get(), 4096);
                        return std::move(fut).then([f = std::move(f), bufptr = std::move(bufptr), checksum] (size_t size) mutable {
                            size_t offset = 0;
                            auto buf = bufptr.get();

                            std::vector<uint8_t> chunk_size = { 0, 1, 0, 0 };
                            BOOST_REQUIRE(::memcmp(chunk_size.data(), &buf[offset], chunk_size.size()) == 0);
                            offset += chunk_size.size();

                            auto *nr = reinterpret_cast<const net::packed<uint32_t> *>(&buf[offset]);
                            uint32_t stored_checksum = net::ntoh(*nr);
                            offset += sizeof(uint32_t);
                            BOOST_REQUIRE(checksum == stored_checksum);

                            BOOST_REQUIRE(size == offset);
                            return f.close().finally([f]{});
                        });
			}).then([tmpdir_path, checksum, version] {
                        auto fname = sstable::filename(tmpdir_path, "ks", "cf", version, 10, big, component_type::Digest);
                        return open_file_dma(fname, open_flags::ro).then([checksum] (file f) {
                            auto bufptr = allocate_aligned_buffer<char>(4096, 4096);

                            auto fut = f.dma_read(0, bufptr.get(), 4096);
                            return std::move(fut).then([f = std::move(f), bufptr = std::move(bufptr), checksum] (size_t size) mutable {
                                auto buf = bufptr.get();

                                bytes stored_digest(reinterpret_cast<const signed char*>(buf), size);
                                bytes expected_digest = to_sstring<bytes>(checksum);

                                BOOST_REQUIRE(size == expected_digest.size());
                                BOOST_REQUIRE(stored_digest == to_sstring<bytes>(checksum));
                                return f.close().finally([f]{});
                            });
                        });
                    });
                });
            });
        });
    });
}

SEASTAR_TEST_CASE(datafile_generation_10) {
    return test_digest_and_checksum<adler32_utils>(sstable_version_types::la).then([]{
        return test_digest_and_checksum<crc32_utils>(sstable_version_types::mc);
    });
}

SEASTAR_TEST_CASE(datafile_generation_11) {
    return test_setup::do_with_tmp_directory([] (sstring tmpdir_path) {
        auto s = complex_schema();

        auto mt = make_lw_shared<memtable>(s);

        const column_definition& set_col = *s->get_column_definition("reg_set");
        const column_definition& static_set_col = *s->get_column_definition("static_collection");

        auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
        auto c_key = clustering_key::from_exploded(*s, {to_bytes("c1"), to_bytes("c2")});

        mutation m(s, key);

        tombstone tomb(api::new_timestamp(), gc_clock::now());
        set_type_impl::mutation set_mut;
        set_mut.tomb = tomb;
        set_mut.cells.emplace_back(to_bytes("1"), make_atomic_cell(bytes_type, {}));
        set_mut.cells.emplace_back(to_bytes("2"), make_atomic_cell(bytes_type, {}));
        set_mut.cells.emplace_back(to_bytes("3"), make_atomic_cell(bytes_type, {}));

        auto set_type = static_pointer_cast<const set_type_impl>(set_col.type);
        m.set_clustered_cell(c_key, set_col, set_type->serialize_mutation_form(set_mut));

        auto static_set_type = static_pointer_cast<const set_type_impl>(static_set_col.type);
        m.set_static_cell(static_set_col, static_set_type->serialize_mutation_form(set_mut));

        auto key2 = partition_key::from_exploded(*s, {to_bytes("key2")});
        mutation m2(s, key2);
        set_type_impl::mutation set_mut_single;
        set_mut_single.cells.emplace_back(to_bytes("4"), make_atomic_cell(bytes_type, {}));

        m2.set_clustered_cell(c_key, set_col, set_type->serialize_mutation_form(set_mut_single));

        mt->apply(std::move(m));
        mt->apply(std::move(m2));

        auto verifier = [s, set_col, c_key] (auto& mutation) {

            auto& mp = mutation->partition();
            BOOST_REQUIRE(mp.clustered_rows().calculate_size() == 1);
            auto r = mp.find_row(*s, c_key);
            BOOST_REQUIRE(r);
            BOOST_REQUIRE(r->size() == 1);
            auto cell = r->find_cell(set_col.id);
            BOOST_REQUIRE(cell);
            auto t = static_pointer_cast<const collection_type_impl>(set_col.type);
            auto bv = cell->as_collection_mutation().data.linearize();
            return t->deserialize_mutation_form(bv).materialize(*t);
        };

        auto sst = make_sstable(s, tmpdir_path, 11, la, big);
        return write_memtable_to_sstable_for_test(*mt, sst).then([s, sst, mt, verifier, tomb, &static_set_col, tmpdir_path] {
            return reusable_sst(s, tmpdir_path, 11).then([s, verifier, tomb, &static_set_col] (auto sstp) mutable {
                return do_with(make_dkey(s, "key1"), [sstp, s, verifier, tomb, &static_set_col] (auto& key) {
                    auto rd = make_lw_shared<flat_mutation_reader>(sstp->read_row_flat(s, key));
                    return read_mutation_from_flat_mutation_reader(*rd, db::no_timeout).then([sstp, s, verifier, tomb, &static_set_col, rd] (auto mutation) {
                        auto verify_set = [&tomb] (const collection_type_impl::mutation& m) {
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
                        auto bv = scol->as_collection_mutation().data.linearize();
                        auto mut = t->deserialize_mutation_form(bv).materialize(*t);
                        verify_set(mut);

                        // The clustered set
                        auto m = verifier(mutation);
                        verify_set(m);
                    });
                }).then([sstp, s, verifier] {
                    return do_with(make_dkey(s, "key2"), [sstp, s, verifier] (auto& key) {
                        auto rd = make_lw_shared<flat_mutation_reader>(sstp->read_row_flat(s, key));
                        return read_mutation_from_flat_mutation_reader(*rd, db::no_timeout).then([sstp, s, verifier, rd] (auto mutation) {
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
    return test_setup::do_with_tmp_directory([] (sstring tmpdir_path) {
        auto s = complex_schema();

        auto mt = make_lw_shared<memtable>(s);

        auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
        auto cp = clustering_key_prefix::from_exploded(*s, {to_bytes("c1")});

        mutation m(s, key);

        tombstone tomb(api::new_timestamp(), gc_clock::now());
        m.partition().apply_delete(*s, cp, tomb);
        mt->apply(std::move(m));

        auto sst = make_sstable(s, tmpdir_path, 12, la, big);
        return write_memtable_to_sstable_for_test(*mt, sst).then([s, tomb, tmpdir_path] {
            return reusable_sst(s, tmpdir_path, 12).then([s, tomb] (auto sstp) mutable {
                return do_with(make_dkey(s, "key1"), [sstp, s, tomb] (auto& key) {
                    auto rd = make_lw_shared<flat_mutation_reader>(sstp->read_row_flat(s, key));
                    return read_mutation_from_flat_mutation_reader(*rd, db::no_timeout).then([sstp, s, tomb, rd] (auto mutation) {
                        auto& mp = mutation->partition();
                        BOOST_REQUIRE(mp.row_tombstones().size() == 1);
                        for (auto& rt: mp.row_tombstones()) {
                            BOOST_REQUIRE(rt.tomb == tomb);
                        }
                    });
                });
            });
        }).then([sst, mt] {});
    });
}

static future<> sstable_compression_test(compressor_ptr c, unsigned generation) {
    return test_setup::do_with_tmp_directory([c, generation] (sstring tmpdir_path) {
        // NOTE: set a given compressor algorithm to schema.
        schema_builder builder(complex_schema());
        builder.set_compressor_params(c);
        auto s = builder.build(schema_builder::compact_storage::no);

        auto mtp = make_lw_shared<memtable>(s);

        auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
        auto cp = clustering_key_prefix::from_exploded(*s, {to_bytes("c1")});

        mutation m(s, key);

        tombstone tomb(api::new_timestamp(), gc_clock::now());
        m.partition().apply_delete(*s, cp, tomb);
        mtp->apply(std::move(m));

        auto sst = make_sstable(s, tmpdir_path, generation, la, big);
        return write_memtable_to_sstable_for_test(*mtp, sst).then([s, tomb, generation, tmpdir_path] {
            return reusable_sst(s, tmpdir_path, generation).then([s, tomb] (auto sstp) mutable {
                return do_with(make_dkey(s, "key1"), [sstp, s, tomb] (auto& key) {
                    auto rd = make_lw_shared<flat_mutation_reader>(sstp->read_row_flat(s, key));
                    return read_mutation_from_flat_mutation_reader(*rd, db::no_timeout).then([sstp, s, tomb, rd] (auto mutation) {
                        auto& mp = mutation->partition();
                        BOOST_REQUIRE(mp.row_tombstones().size() == 1);
                        for (auto& rt: mp.row_tombstones()) {
                            BOOST_REQUIRE(rt.tomb == tomb);
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
    return test_setup::do_with_tmp_directory([] (sstring tmpdir_path) {
        auto s = uncompressed_schema();

        auto mtp = make_lw_shared<memtable>(s);
        // Create a number of keys that is a multiple of the sampling level
        for (int i = 0; i < 0x80; ++i) {
            sstring k = "key" + to_sstring(i);
            auto key = partition_key::from_exploded(*s, {to_bytes(k)});
            mutation m(s, key);

            auto c_key = clustering_key::make_empty();
            m.set_clustered_cell(c_key, to_bytes("col2"), i, api::max_timestamp);
            mtp->apply(std::move(m));
        }

        auto sst = make_sstable(s, tmpdir_path, 16, la, big);
        return write_memtable_to_sstable_for_test(*mtp, sst).then([s, tmpdir_path] {
            return reusable_sst(s, tmpdir_path, 16).then([] (auto s) {
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
static future<sstables::shared_sstable> open_sstable(schema_ptr schema, sstring dir, unsigned long generation) {
    auto sst = sstables::make_sstable(std::move(schema), dir, generation,
            sstables::sstable::version_types::la,
            sstables::sstable::format_types::big);
    auto fut = sst->load();
    return fut.then([sst = std::move(sst)] { return std::move(sst); });
}

// open_sstables() opens several generations of the same sstable, returning,
// after all the tables have been open, their vector.
static future<std::vector<sstables::shared_sstable>> open_sstables(schema_ptr s, sstring dir, std::vector<unsigned long> generations) {
    return do_with(std::vector<sstables::shared_sstable>(),
            [dir = std::move(dir), generations = std::move(generations), s] (auto& ret) mutable {
        return parallel_for_each(generations, [&ret, &dir, s] (unsigned long generation) {
            return open_sstable(s, dir, generation).then([&ret] (sstables::shared_sstable sst) {
                ret.push_back(std::move(sst));
            });
        }).then([&ret] {
            return std::move(ret);
        });
    });
}

// mutation_reader for sstable keeping all the required objects alive.
static flat_mutation_reader sstable_reader(shared_sstable sst, schema_ptr s) {
    return sst->as_mutation_source().make_reader(s, query::full_partition_range, s->full_slice());

}

static flat_mutation_reader sstable_reader(shared_sstable sst, schema_ptr s, const dht::partition_range& pr) {
    return sst->as_mutation_source().make_reader(s, pr, s->full_slice());
}

// We don't need to normalize the sstable reader for 'mc' format
// because it is naturally normalized now.
static flat_mutation_reader make_normalizing_sstable_reader(
        shared_sstable sst, schema_ptr s, const dht::partition_range& pr) {
    auto sstable_reader = sst->as_mutation_source().make_reader(s, pr, s->full_slice());
    if (sst->get_version() == sstables::sstable::version_types::mc) {
        return sstable_reader;
    }

    return make_normalizing_reader(std::move(sstable_reader));
}

static flat_mutation_reader make_normalizing_sstable_reader(shared_sstable sst, schema_ptr s) {
    return make_normalizing_sstable_reader(sst, s, query::full_partition_range);
}

SEASTAR_TEST_CASE(compaction_manager_test) {
  return seastar::async([] {
    storage_service_for_tests ssft;
    BOOST_REQUIRE(smp::count == 1);
    auto s = make_lw_shared(schema({}, some_keyspace, some_column_family,
        {{"p1", utf8_type}}, {{"c1", utf8_type}}, {{"r1", int32_type}}, {}, utf8_type));

    auto cm = make_lw_shared<compaction_manager>();
    cm->start();

    auto tmp = make_lw_shared<tmpdir>();

    column_family::config cfg;
    cfg.datadir = tmp->path;
    cfg.enable_commitlog = false;
    cfg.enable_incremental_backups = false;
    cfg.large_partition_handler = &nop_lp_handler;
    auto cl_stats = make_lw_shared<cell_locker_stats>();
    auto tracker = make_lw_shared<cache_tracker>();
    auto cf = make_lw_shared<column_family>(s, cfg, column_family::no_commitlog(), *cm, *cl_stats, *tracker);
    cf->start();
    cf->mark_ready_for_writes();
    cf->set_compaction_strategy(sstables::compaction_strategy_type::size_tiered);

    auto generations = make_lw_shared<std::vector<unsigned long>>({1, 2, 3, 4});

    do_for_each(*generations, [generations, cf, cm, s, tmp] (unsigned long generation) {
        // create 4 sstables of similar size to be compacted later on.

        auto mt = make_lw_shared<memtable>(s);

        const column_definition& r1_col = *s->get_column_definition("r1");

        sstring k = "key" + to_sstring(generation);
        auto key = partition_key::from_exploded(*s, {to_bytes(k)});
        auto c_key = clustering_key::from_exploded(*s, {to_bytes("abc")});

        mutation m(s, key);
        m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type, int32_type->decompose(1)));
        mt->apply(std::move(m));

        auto sst = make_sstable(s, tmp->path, column_family_test::calculate_generation_for_new_table(*cf), la, big);

        return write_memtable_to_sstable_for_test(*mt, sst).then([mt, sst, cf] {
            return sst->load().then([sst, cf] {
                column_family_test(cf).add_sstable(sst);
                return make_ready_future<>();
            });
        });
    }).then([cf, cm, generations] {
        // submit cf to compaction manager and then check that cf's sstables
        // were compacted.

        BOOST_REQUIRE(cf->sstables_count() == generations->size());
        cf->trigger_compaction();
        BOOST_REQUIRE(cm->get_stats().pending_tasks == 1 || cm->get_stats().active_tasks == 1);

        // wait for submitted job to finish.
        auto end = [cm] { return cm->get_stats().pending_tasks == 0 && cm->get_stats().active_tasks == 0; };
        return do_until(end, [] {
            // sleep until compaction manager selects cf for compaction.
            return sleep(std::chrono::milliseconds(100));
        }).then([cf, cm] {
            BOOST_REQUIRE(cm->get_stats().completed_tasks == 1);
            BOOST_REQUIRE(cm->get_stats().errors == 0);

            // remove cf from compaction manager; this will wait for the
            // ongoing compaction to finish.
            return cf->stop().then([cf, cm] {
                // expect sstables of cf to be compacted.
                BOOST_REQUIRE(cf->sstables_count() == 1);
                // stop all compaction manager tasks.
                return cm->stop().then([cf, cm] {
                    return make_ready_future<>();
                });
            });
        });
    }).finally([s, cm, tmp, cl_stats, tracker] {
        return cm->stop().then([cm] {});
    }).get();
  });
}

SEASTAR_TEST_CASE(compact) {
    BOOST_REQUIRE(smp::count == 1);
    constexpr int generation = 17;
    // The "compaction" sstable was created with the following schema:
    // CREATE TABLE compaction (
    //        name text,
    //        age int,
    //        height int,
    //        PRIMARY KEY (name)
    //);
    auto builder = schema_builder("tests", "compaction")
        .with_column("name", utf8_type, column_kind::partition_key)
        .with_column("age", int32_type)
        .with_column("height", int32_type);
    builder.set_comment("Example table for compaction");
    builder.set_gc_grace_seconds(std::numeric_limits<int32_t>::max());
    auto s = builder.build();
    auto cm = make_lw_shared<compaction_manager>();
    auto cl_stats = make_lw_shared<cell_locker_stats>();
    auto tracker = make_lw_shared<cache_tracker>();
    auto cf = make_lw_shared<column_family>(s, column_family_test_config(), column_family::no_commitlog(), *cm, *cl_stats, *tracker);
    cf->mark_ready_for_writes();

    return open_sstables(s, "tests/sstables/compaction", {1,2,3}).then([s, cf, cm, generation] (auto sstables) {
        return test_setup::do_with_tmp_directory([sstables, s, generation, cf, cm] (sstring tmpdir_path) {
            auto new_sstable = [generation, s, tmpdir_path] {
                return sstables::make_sstable(s, tmpdir_path,
                        generation, sstables::sstable::version_types::la, sstables::sstable::format_types::big);
            };
            return sstables::compact_sstables(sstables::compaction_descriptor(std::move(sstables)), *cf, new_sstable).then([s, generation, cf, cm, tmpdir_path] (auto) {
                // Verify that the compacted sstable has the right content. We expect to see:
                //  name  | age | height
                // -------+-----+--------
                //  jerry |  40 |    170
                //    tom |  20 |    180
                //   john |  20 |   deleted
                //   nadav - deleted partition
                return open_sstable(s, tmpdir_path, generation).then([s] (shared_sstable sst) {
                    auto reader = make_lw_shared(sstable_reader(sst, s)); // reader holds sst and s alive.
                    return read_mutation_from_flat_mutation_reader(*reader, db::no_timeout).then([reader, s] (mutation_opt m) {
                        BOOST_REQUIRE(m);
                        BOOST_REQUIRE(m->key().equal(*s, partition_key::from_singular(*s, data_value(sstring("jerry")))));
                        BOOST_REQUIRE(!m->partition().partition_tombstone());
                        auto &rows = m->partition().clustered_rows();
                        BOOST_REQUIRE(rows.calculate_size() == 1);
                        auto &row = rows.begin()->row();
                        BOOST_REQUIRE(!row.deleted_at());
                        auto &cells = row.cells();
                        auto& cdef1 = *s->get_column_definition("age");
                        auto& cdef2 = *s->get_column_definition("height");
                        BOOST_REQUIRE(cells.cell_at(cdef1.id).as_atomic_cell(cdef1).value() == bytes({0,0,0,40}));
                        BOOST_REQUIRE(cells.cell_at(cdef2.id).as_atomic_cell(cdef2).value() == bytes({0,0,0,(int8_t)170}));
                        return read_mutation_from_flat_mutation_reader(*reader, db::no_timeout);
                    }).then([reader, s] (mutation_opt m) {
                        BOOST_REQUIRE(m);
                        BOOST_REQUIRE(m->key().equal(*s, partition_key::from_singular(*s, data_value(sstring("tom")))));
                        BOOST_REQUIRE(!m->partition().partition_tombstone());
                        auto &rows = m->partition().clustered_rows();
                        BOOST_REQUIRE(rows.calculate_size() == 1);
                        auto &row = rows.begin()->row();
                        BOOST_REQUIRE(!row.deleted_at());
                        auto &cells = row.cells();
                        auto& cdef1 = *s->get_column_definition("age");
                        auto& cdef2 = *s->get_column_definition("height");
                        BOOST_REQUIRE(cells.cell_at(cdef1.id).as_atomic_cell(cdef1).value() == bytes({0,0,0,20}));
                        BOOST_REQUIRE(cells.cell_at(cdef2.id).as_atomic_cell(cdef2).value() == bytes({0,0,0,(int8_t)180}));
                        return read_mutation_from_flat_mutation_reader(*reader, db::no_timeout);
                    }).then([reader, s] (mutation_opt m) {
                        BOOST_REQUIRE(m);
                        BOOST_REQUIRE(m->key().equal(*s, partition_key::from_singular(*s, data_value(sstring("john")))));
                        BOOST_REQUIRE(!m->partition().partition_tombstone());
                        auto &rows = m->partition().clustered_rows();
                        BOOST_REQUIRE(rows.calculate_size() == 1);
                        auto &row = rows.begin()->row();
                        BOOST_REQUIRE(!row.deleted_at());
                        auto &cells = row.cells();
                        auto& cdef1 = *s->get_column_definition("age");
                        auto& cdef2 = *s->get_column_definition("height");
                        BOOST_REQUIRE(cells.cell_at(cdef1.id).as_atomic_cell(cdef1).value() == bytes({0,0,0,20}));
                        BOOST_REQUIRE(cells.find_cell(cdef2.id) == nullptr);
                        return read_mutation_from_flat_mutation_reader(*reader, db::no_timeout);
                    }).then([reader, s] (mutation_opt m) {
                        BOOST_REQUIRE(m);
                        BOOST_REQUIRE(m->key().equal(*s, partition_key::from_singular(*s, data_value(sstring("nadav")))));
                        BOOST_REQUIRE(m->partition().partition_tombstone());
                        auto &rows = m->partition().clustered_rows();
                        BOOST_REQUIRE(rows.calculate_size() == 0);
                        return read_mutation_from_flat_mutation_reader(*reader, db::no_timeout);
                    }).then([reader] (mutation_opt m) {
                        BOOST_REQUIRE(!m);
                    });
                });
            });
        });
    }).finally([cl_stats, tracker] { });

    // verify that the compacted sstable look like
}

static std::vector<sstables::shared_sstable> get_candidates_for_leveled_strategy(column_family& cf) {
    std::vector<sstables::shared_sstable> candidates;
    candidates.reserve(cf.sstables_count());
    for (auto& entry : *cf.get_sstables()) {
        candidates.push_back(entry);
    }
    return candidates;
}

struct column_family_for_tests {
    struct data {
        schema_ptr s;
        cache_tracker tracker;
        column_family::config cfg;
        cell_locker_stats cl_stats;
        compaction_manager cm;
        lw_shared_ptr<column_family> cf;
    };
    lw_shared_ptr<data> _data;

    column_family_for_tests()
        : column_family_for_tests(
            schema_builder(some_keyspace, some_column_family)
                .with_column(utf8_type->decompose("p1"), utf8_type, column_kind::partition_key)
                .build()
        )
    { }

    explicit column_family_for_tests(schema_ptr s)
        : _data(make_lw_shared<data>())
    {
        _data->s = s;
        _data->cfg.enable_disk_writes = false;
        _data->cfg.enable_commitlog = false;
        _data->cfg.large_partition_handler = &nop_lp_handler;
        _data->cf = make_lw_shared<column_family>(_data->s, _data->cfg, column_family::no_commitlog(), _data->cm, _data->cl_stats, _data->tracker);
        _data->cf->mark_ready_for_writes();
    }

    schema_ptr schema() { return _data->s; }

    operator lw_shared_ptr<column_family>() { return _data->cf; }
    
    column_family& operator*() { return *_data->cf; }
    column_family* operator->() { return _data->cf.get(); }
};

// Return vector of sstables generated by compaction. Only relevant for leveled one.
static future<std::vector<unsigned long>> compact_sstables(sstring tmpdir_path, std::vector<unsigned long> generations_to_compact,
        unsigned long new_generation, bool create_sstables, uint64_t min_sstable_size, compaction_strategy_type strategy) {
    BOOST_REQUIRE(smp::count == 1);
    schema_builder builder(make_lw_shared(schema({}, some_keyspace, some_column_family,
        {{"p1", utf8_type}}, {{"c1", utf8_type}}, {{"r1", utf8_type}}, {}, utf8_type)));
    builder.set_compressor_params(compression_parameters::no_compression());
    builder.set_min_compaction_threshold(4);
    auto s = builder.build(schema_builder::compact_storage::no);

    column_family_for_tests cf(s);

    auto generations = make_lw_shared<std::vector<unsigned long>>(std::move(generations_to_compact));
    auto sstables = make_lw_shared<std::vector<sstables::shared_sstable>>();
    auto created = make_lw_shared<std::vector<unsigned long>>();

    auto f = make_ready_future<>();

    return f.then([generations, sstables, s, create_sstables, min_sstable_size, tmpdir_path] () mutable {
        if (!create_sstables) {
            return open_sstables(s, tmpdir_path, *generations).then([sstables] (auto opened_sstables) mutable {
                for (auto& sst : opened_sstables) {
                    sstables->push_back(sst);
                }
                return make_ready_future<>();
            });
        }
        return do_for_each(*generations, [generations, sstables, s, min_sstable_size, tmpdir_path] (unsigned long generation) {
            auto mt = make_lw_shared<memtable>(s);

            const column_definition& r1_col = *s->get_column_definition("r1");

            sstring k = "key" + to_sstring(generation);
            auto key = partition_key::from_exploded(*s, {to_bytes(k)});
            auto c_key = clustering_key::from_exploded(*s, {to_bytes("abc")});

            mutation m(s, key);
            m.set_clustered_cell(c_key, r1_col, make_atomic_cell(utf8_type, bytes(min_sstable_size, 'a')));
            mt->apply(std::move(m));

            auto sst = make_sstable(s, tmpdir_path, generation, la, big);

            return write_memtable_to_sstable_for_test(*mt, sst).then([mt, sst, s, sstables] {
                return sst->load().then([sst, sstables] {
                    sstables->push_back(sst);
                    return make_ready_future<>();
                });
            });
        });
    }).then([cf, sstables, new_generation, generations, strategy, created, min_sstable_size, s, tmpdir_path] () mutable {
        auto generation = make_lw_shared<unsigned long>(new_generation);
        auto new_sstable = [generation, created, s, tmpdir_path] {
            auto gen = (*generation)++;
            created->push_back(gen);
            return sstables::make_sstable(s, tmpdir_path,
                gen, sstables::sstable::version_types::la, sstables::sstable::format_types::big);
        };
        // We must have opened at least all original candidates.
        BOOST_REQUIRE(generations->size() == sstables->size());

        if (strategy == compaction_strategy_type::size_tiered) {
            // Calling function that will return a list of sstables to compact based on size-tiered strategy.
            int min_threshold = cf->schema()->min_compaction_threshold();
            int max_threshold = cf->schema()->max_compaction_threshold();
            auto sstables_to_compact = sstables::size_tiered_compaction_strategy::most_interesting_bucket(*sstables, min_threshold, max_threshold);
            // We do expect that all candidates were selected for compaction (in this case).
            BOOST_REQUIRE(sstables_to_compact.size() == sstables->size());
            return sstables::compact_sstables(sstables::compaction_descriptor(std::move(sstables_to_compact)), *cf, new_sstable).then([generation] (auto) {});
        } else if (strategy == compaction_strategy_type::leveled) {
            for (auto& sst : *sstables) {
                BOOST_REQUIRE(sst->get_sstable_level() == 0);
                BOOST_REQUIRE(sst->data_size() >= min_sstable_size);
                column_family_test(cf).add_sstable(sst);
            }
            auto candidates = get_candidates_for_leveled_strategy(*cf);
            sstables::size_tiered_compaction_strategy_options stcs_options;
            leveled_manifest manifest = leveled_manifest::create(*cf, candidates, 1, stcs_options);
            std::vector<stdx::optional<dht::decorated_key>> last_compacted_keys(leveled_manifest::MAX_LEVELS);
            std::vector<int> compaction_counter(leveled_manifest::MAX_LEVELS);
            auto candidate = manifest.get_compaction_candidates(last_compacted_keys, compaction_counter);
            BOOST_REQUIRE(candidate.sstables.size() == sstables->size());
            BOOST_REQUIRE(candidate.level == 1);
            BOOST_REQUIRE(candidate.max_sstable_bytes == 1024*1024);

            return sstables::compact_sstables(sstables::compaction_descriptor(std::move(candidate.sstables), candidate.level, 1024*1024),
                *cf, new_sstable).then([generation] (auto) {});
        } else {
            throw std::runtime_error("unexpected strategy");
        }
        return make_ready_future<>();
    }).then([cf, created] {
        return std::move(*created);
    });
}

static future<> compact_sstables(sstring tmpdir_path, std::vector<unsigned long> generations_to_compact, unsigned long new_generation, bool create_sstables = true) {
    uint64_t min_sstable_size = 50;
    return compact_sstables(tmpdir_path, std::move(generations_to_compact), new_generation, create_sstables, min_sstable_size,
                            compaction_strategy_type::size_tiered).then([new_generation] (auto ret) {
        // size tiered compaction will output at most one sstable, let's assert that.
        BOOST_REQUIRE(ret.size() == 1);
        BOOST_REQUIRE(ret[0] == new_generation);
        return make_ready_future<>();
    });
}

static future<> check_compacted_sstables(sstring tmpdir_path, unsigned long generation, std::vector<unsigned long> compacted_generations) {
    auto s = make_lw_shared(schema({}, some_keyspace, some_column_family,
        {{"p1", utf8_type}}, {{"c1", utf8_type}}, {{"r1", int32_type}}, {}, utf8_type));

    auto generations = make_lw_shared<std::vector<unsigned long>>(std::move(compacted_generations));

    return open_sstable(s, tmpdir_path, generation).then([s, generations] (shared_sstable sst) {
        auto reader = sstable_reader(sst, s); // reader holds sst and s alive.
        auto keys = make_lw_shared<std::vector<partition_key>>();

        return do_with(std::move(reader), [generations, s, keys] (flat_mutation_reader& reader) {
            return do_for_each(*generations, [&reader, keys] (unsigned long generation) mutable {
                return read_mutation_from_flat_mutation_reader(reader, db::no_timeout).then([generation, keys] (mutation_opt m) {
                    BOOST_REQUIRE(m);
                    keys->push_back(m->key());
                });
            }).then([s, keys, generations] {
                // keys from compacted sstable aren't ordered lexographically,
                // thus we must read all keys into a vector, sort the vector
                // lexographically, then proceed with the comparison.
                std::sort(keys->begin(), keys->end(), partition_key::less_compare(*s));
                BOOST_REQUIRE(keys->size() == generations->size());
                auto i = 0;
                for (auto& k : *keys) {
                    sstring original_k = "key" + to_sstring((*generations)[i++]);
                    BOOST_REQUIRE(k.equal(*s, partition_key::from_singular(*s, data_value(original_k))));
                }
                return make_ready_future<>();
            });
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

    return test_setup::do_with_tmp_directory([] (sstring tmpdir_path) {
        // Compact 4 sstables into 1 using size-tiered strategy to select sstables.
        // E.g.: generations 18, 19, 20 and 21 will be compacted into generation 22.
        return compact_sstables(tmpdir_path, { 18, 19, 20, 21 }, 22).then([tmpdir_path] {
            // Check that generation 22 contains all keys of generations 18, 19, 20 and 21.
            return check_compacted_sstables(tmpdir_path, 22, { 18, 19, 20, 21 });
        }).then([tmpdir_path] {
            return compact_sstables(tmpdir_path, { 23, 24, 25, 26 }, 27).then([tmpdir_path] {
                return check_compacted_sstables(tmpdir_path, 27, { 23, 24, 25, 26 });
            });
        }).then([tmpdir_path] {
            return compact_sstables(tmpdir_path, { 28, 29, 30, 31 }, 32).then([tmpdir_path] {
                return check_compacted_sstables(tmpdir_path, 32, { 28, 29, 30, 31 });
            });
        }).then([tmpdir_path] {
            return compact_sstables(tmpdir_path, { 33, 34, 35, 36 }, 37).then([tmpdir_path] {
                return check_compacted_sstables(tmpdir_path, 37, { 33, 34, 35, 36 });
            });
        }).then([tmpdir_path] {
            // In this step, we compact 4 compacted sstables.
            return compact_sstables(tmpdir_path, { 22, 27, 32, 37 }, 38, false).then([tmpdir_path] {
                // Check that the compacted sstable contains all keys.
                return check_compacted_sstables(tmpdir_path, 38,
                    { 18, 19, 20, 21, 23, 24, 25, 26, 28, 29, 30, 31, 33, 34, 35, 36 });
            });
        });
    });
}

SEASTAR_TEST_CASE(datafile_generation_37) {
    return test_setup::do_with_tmp_directory([] (sstring tmpdir_path) {
        auto s = compact_simple_dense_schema();

        auto mtp = make_lw_shared<memtable>(s);

        auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
        mutation m(s, key);

        auto c_key = clustering_key_prefix::from_exploded(*s, {to_bytes("cl1")});
        const column_definition& cl2 = *s->get_column_definition("cl2");

        m.set_clustered_cell(c_key, cl2, make_atomic_cell(bytes_type, bytes_type->decompose(data_value(to_bytes("cl2")))));
        mtp->apply(std::move(m));

        auto sst = make_sstable(s, tmpdir_path, 37, la, big);
        return write_memtable_to_sstable_for_test(*mtp, sst).then([s, tmpdir_path] {
            return reusable_sst(s, tmpdir_path, 37).then([s, tmpdir_path] (auto sstp) {
                return do_with(make_dkey(s, "key1"), [sstp, s] (auto& key) {
                    auto rd = make_lw_shared<flat_mutation_reader>(sstp->read_row_flat(s, key));
                    return read_mutation_from_flat_mutation_reader(*rd, db::no_timeout).then([sstp, s, rd] (auto mutation) {
                        auto& mp = mutation->partition();

                        auto clustering = clustering_key_prefix::from_exploded(*s, {to_bytes("cl1")});

                        auto& row = mp.clustered_row(*s, clustering);
                        match_live_cell(row.cells(), *s, "cl2", data_value(to_bytes("cl2")));
                        return make_ready_future<>();
                    });
                });
            });
        }).then([sst, mtp, s] {});
    });
}

SEASTAR_TEST_CASE(datafile_generation_38) {
    return test_setup::do_with_tmp_directory([] (sstring tmpdir_path) {
        auto s = compact_dense_schema();

        auto mtp = make_lw_shared<memtable>(s);

        auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
        mutation m(s, key);

        auto c_key = clustering_key_prefix::from_exploded(*s, {to_bytes("cl1"), to_bytes("cl2")});

        const column_definition& cl3 = *s->get_column_definition("cl3");
        m.set_clustered_cell(c_key, cl3, make_atomic_cell(bytes_type, bytes_type->decompose(data_value(to_bytes("cl3")))));
        mtp->apply(std::move(m));

        auto sst = make_sstable(s, tmpdir_path, 38, la, big);
        return write_memtable_to_sstable_for_test(*mtp, sst).then([s, tmpdir_path] {
            return reusable_sst(s, tmpdir_path, 38).then([s] (auto sstp) {
                return do_with(make_dkey(s, "key1"), [sstp, s] (auto& key) {
                    auto rd = make_lw_shared<flat_mutation_reader>(sstp->read_row_flat(s, key));
                    return read_mutation_from_flat_mutation_reader(*rd, db::no_timeout).then([sstp, s, rd] (auto mutation) {
                        auto& mp = mutation->partition();
                        auto clustering = clustering_key_prefix::from_exploded(*s, {to_bytes("cl1"), to_bytes("cl2")});

                        auto& row = mp.clustered_row(*s, clustering);
                        match_live_cell(row.cells(), *s, "cl3", data_value(to_bytes("cl3")));
                        return make_ready_future<>();
                    });
                });
            });
        }).then([sst, mtp, s] {});
    });
}

SEASTAR_TEST_CASE(datafile_generation_39) {
    return test_setup::do_with_tmp_directory([] (sstring tmpdir_path) {
        auto s = compact_sparse_schema();

        auto mtp = make_lw_shared<memtable>(s);

        auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
        mutation m(s, key);

        auto c_key = clustering_key::make_empty();

        const column_definition& cl1 = *s->get_column_definition("cl1");
        m.set_clustered_cell(c_key, cl1, make_atomic_cell(bytes_type, bytes_type->decompose(data_value(to_bytes("cl1")))));
        const column_definition& cl2 = *s->get_column_definition("cl2");
        m.set_clustered_cell(c_key, cl2, make_atomic_cell(bytes_type, bytes_type->decompose(data_value(to_bytes("cl2")))));
        mtp->apply(std::move(m));

        auto sst = make_sstable(s, tmpdir_path, 39, la, big);
        return write_memtable_to_sstable_for_test(*mtp, sst).then([s, tmpdir_path] {
            return reusable_sst(s, tmpdir_path, 39).then([s] (auto sstp) {
                return do_with(make_dkey(s, "key1"), [sstp, s] (auto& key) {
                    auto rd = make_lw_shared<flat_mutation_reader>(sstp->read_row_flat(s, key));
                    return read_mutation_from_flat_mutation_reader(*rd, db::no_timeout).then([sstp, s, rd] (auto mutation) {
                        auto& mp = mutation->partition();
                        auto& row = mp.clustered_row(*s, clustering_key::make_empty());
                        match_live_cell(row.cells(), *s, "cl1", data_value(data_value(to_bytes("cl1"))));
                        match_live_cell(row.cells(), *s, "cl2", data_value(data_value(to_bytes("cl2"))));
                        return make_ready_future<>();
                    });
                });
            });
        }).then([sst, mtp, s] {});
    });
}

SEASTAR_TEST_CASE(datafile_generation_40) {
    return test_setup::do_with_tmp_directory([] (sstring tmpdir_path) {
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
            builder.set_compressor_params(compression_parameters::no_compression());
            return builder.build(schema_builder::compact_storage::yes);
        }();

        auto mt = make_lw_shared<memtable>(s);
        auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
        mutation m(s, key);

        const column_definition& r1_col = *s->get_column_definition("r1");
        auto ca = clustering_key::from_exploded(*s, {to_bytes("a")});
        m.set_clustered_cell(ca, r1_col, make_atomic_cell(int32_type, int32_type->decompose(1)));
        mt->apply(std::move(m));

        auto cb = clustering_key::from_exploded(*s, {to_bytes("b")});
        m.set_clustered_cell(cb, r1_col, make_atomic_cell(int32_type, int32_type->decompose(1)));
        mt->apply(std::move(m));

        auto sst = make_sstable(s, tmpdir_path, 40, la, big);

        return write_memtable_to_sstable_for_test(*mt, sst).then([mt, sst, s, tmpdir_path] {
            auto fname = sstable::filename(tmpdir_path, "ks", "cf", la, 40, big, component_type::Data);
            return open_file_dma(fname, open_flags::ro).then([] (file f) {
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

SEASTAR_TEST_CASE(datafile_generation_41) {
    return test_setup::do_with_tmp_directory([] (sstring tmpdir_path) {
        auto s = make_lw_shared(schema({}, some_keyspace, some_column_family,
            {{"p1", utf8_type}}, {{"c1", utf8_type}}, {{"r1", int32_type}, {"r2", int32_type}}, {}, utf8_type));

        auto mt = make_lw_shared<memtable>(s);

        auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
        auto c_key = clustering_key::from_exploded(*s, {to_bytes("c1")});
        mutation m(s, key);

        tombstone tomb(api::new_timestamp(), gc_clock::now());
        m.partition().apply_delete(*s, std::move(c_key), tomb);
        mt->apply(std::move(m));

        auto sst = make_sstable(s, tmpdir_path, 41, la, big);
        return write_memtable_to_sstable_for_test(*mt, sst).then([s, tomb, tmpdir_path] {
            return reusable_sst(s, tmpdir_path, 41).then([s, tomb] (auto sstp) mutable {
                return do_with(make_dkey(s, "key1"), [sstp, s, tomb] (auto& key) {
                    auto rd = make_lw_shared<flat_mutation_reader>(sstp->read_row_flat(s, key));
                    return read_mutation_from_flat_mutation_reader(*rd, db::no_timeout).then([sstp, s, tomb, rd] (auto mutation) {
                        auto& mp = mutation->partition();
                        BOOST_REQUIRE(mp.clustered_rows().calculate_size() == 1);
                        auto& c_row = *(mp.clustered_rows().begin());
                        BOOST_REQUIRE(c_row.row().deleted_at().tomb() == tomb);
                    });
                });
            });
        }).then([sst, mt] {});
    });
}

SEASTAR_TEST_CASE(check_compaction_ancestor_metadata) {
    // NOTE: generations 42 to 46 are used here.

    // check that ancestors list of compacted sstable is correct.

    return test_setup::do_with_tmp_directory([] (sstring tmpdir_path) {
        return compact_sstables(tmpdir_path, { 42, 43, 44, 45 }, 46).then([tmpdir_path] {
            auto s = make_lw_shared(schema({}, some_keyspace, some_column_family,
                {{"p1", utf8_type}}, {{"c1", utf8_type}}, {{"r1", utf8_type}}, {}, utf8_type));
            return open_sstable(s, tmpdir_path, 46).then([] (shared_sstable sst) {
                std::set<unsigned long> ancestors;
                const compaction_metadata& cm = sst->get_compaction_metadata();
                for (auto& ancestor : cm.ancestors.elements) {
                    ancestors.insert(ancestor);
                }
                BOOST_REQUIRE(ancestors.find(42) != ancestors.end());
                BOOST_REQUIRE(ancestors.find(43) != ancestors.end());
                BOOST_REQUIRE(ancestors.find(44) != ancestors.end());
                BOOST_REQUIRE(ancestors.find(45) != ancestors.end());

                return make_ready_future<>();
            });
        });
    });
}

SEASTAR_TEST_CASE(datafile_generation_47) {
    // Tests the problem in which the sstable row parser would hang.
    return test_setup::do_with_tmp_directory([] (sstring tmpdir_path) {
        auto s = make_lw_shared(schema({}, some_keyspace, some_column_family,
            {{"p1", utf8_type}}, {{"c1", utf8_type}}, {{"r1", utf8_type}}, {}, utf8_type));

        auto mt = make_lw_shared<memtable>(s);

        const column_definition& r1_col = *s->get_column_definition("r1");

        auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
        auto c_key = clustering_key::from_exploded(*s, {to_bytes("c1")});
        mutation m(s, key);
        m.set_clustered_cell(c_key, r1_col, make_atomic_cell(utf8_type, bytes(512*1024, 'a')));
        mt->apply(std::move(m));

        auto sst = make_sstable(s, tmpdir_path, 47, la, big);
        return write_memtable_to_sstable_for_test(*mt, sst).then([s, tmpdir_path] {
            return reusable_sst(s, tmpdir_path, 47).then([s] (auto sstp) mutable {
                auto reader = make_lw_shared(sstable_reader(sstp, s));
                return repeat([reader] {
                    return (*reader)(db::no_timeout).then([] (mutation_fragment_opt m) {
                        if (!m) {
                            return make_ready_future<stop_iteration>(stop_iteration::yes);
                        }
                        return make_ready_future<stop_iteration>(stop_iteration::no);
                    });
                }).then([sstp, reader, s] {});
            });
        }).then([sst, mt] {});
    });
}

SEASTAR_TEST_CASE(test_counter_write) {
    return test_setup::do_with_tmp_directory([] (sstring tmpdir_path) {
        return seastar::async([tmpdir_path] {
            auto s = schema_builder(some_keyspace, some_column_family)
                    .with_column("p1", utf8_type, column_kind::partition_key)
                    .with_column("c1", utf8_type, column_kind::clustering_key)
                    .with_column("r1", counter_type)
                    .with_column("r2", counter_type)
                    .build();
            auto mt = make_lw_shared<memtable>(s);

            auto& r1_col = *s->get_column_definition("r1");
            auto& r2_col = *s->get_column_definition("r2");

            auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
            auto c_key = clustering_key::from_exploded(*s, {to_bytes("c1")});
            auto c_key2 = clustering_key::from_exploded(*s, {to_bytes("c2")});

            mutation m(s, key);

            std::vector<counter_id> ids;
            std::generate_n(std::back_inserter(ids), 3, counter_id::generate_random);
            boost::range::sort(ids);

            counter_cell_builder b1;
            b1.add_shard(counter_shard(ids[0], 5, 1));
            b1.add_shard(counter_shard(ids[1], -4, 1));
            b1.add_shard(counter_shard(ids[2], 9, 1));
            auto ts = api::new_timestamp();
            m.set_clustered_cell(c_key, r1_col, b1.build(ts));

            counter_cell_builder b2;
            b2.add_shard(counter_shard(ids[1], -1, 1));
            b2.add_shard(counter_shard(ids[2], 2, 1));
            m.set_clustered_cell(c_key, r2_col, b2.build(ts));

            m.set_clustered_cell(c_key2, r1_col, make_dead_atomic_cell(1));

            mt->apply(m);

            auto sst = make_sstable(s, tmpdir_path, 900, la, big);
            write_memtable_to_sstable_for_test(*mt, sst).get();

            auto sstp = reusable_sst(s, tmpdir_path, 900).get0();
            assert_that(sstable_reader(sstp, s))
                .produces(m)
                .produces_end_of_stream();
        });
    });
}

// Leveled compaction strategy tests

static dht::token create_token_from_key(sstring key) {
    sstables::key_view key_view = sstables::key_view(bytes_view(reinterpret_cast<const signed char*>(key.c_str()), key.size()));
    dht::token token = dht::global_partitioner().get_token(key_view);
    assert(token == dht::global_partitioner().get_token(key_view));
    return token;
}

static range<dht::token> create_token_range_from_keys(sstring start_key, sstring end_key) {
    dht::token start = create_token_from_key(start_key);
    assert(engine().cpu_id() == dht::global_partitioner().shard_of(start));
    dht::token end = create_token_from_key(end_key);
    assert(engine().cpu_id() == dht::global_partitioner().shard_of(end));
    assert(end >= start);
    return range<dht::token>::make(start, end);
}

namespace dht {
    extern std::unique_ptr<i_partitioner> default_partitioner;
}

static std::vector<std::pair<sstring, dht::token>> token_generation_for_shard(unsigned tokens_to_generate, unsigned shard,
        unsigned ignore_msb = 0, unsigned smp_count = smp::count) {
    unsigned tokens = 0;
    unsigned key_id = 0;
    std::vector<std::pair<sstring, dht::token>> key_and_token_pair;

    key_and_token_pair.reserve(tokens_to_generate);
    dht::default_partitioner = std::make_unique<dht::murmur3_partitioner>(smp_count, ignore_msb);

    while (tokens < tokens_to_generate) {
        sstring key = to_sstring(key_id++);
        dht::token token = create_token_from_key(key);
        if (shard != dht::global_partitioner().shard_of(token)) {
            continue;
        }
        tokens++;
        key_and_token_pair.emplace_back(key, token);
    }
    assert(key_and_token_pair.size() == tokens_to_generate);

    std::sort(key_and_token_pair.begin(),key_and_token_pair.end(), [] (auto& i, auto& j) {
        return i.second < j.second;
    });

    return key_and_token_pair;
}

static std::vector<std::pair<sstring, dht::token>> token_generation_for_current_shard(unsigned tokens_to_generate) {
    return token_generation_for_shard(tokens_to_generate, engine().cpu_id());
}

static void add_sstable_for_leveled_test(lw_shared_ptr<column_family> cf, int64_t gen, uint64_t fake_data_size,
                                         uint32_t sstable_level, sstring first_key, sstring last_key, int64_t max_timestamp = 0) {
    auto sst = make_sstable(cf->schema(), "", gen, la, big);
    sstables::test(sst).set_values_for_leveled_strategy(fake_data_size, sstable_level, max_timestamp, std::move(first_key), std::move(last_key));
    assert(sst->data_size() == fake_data_size);
    assert(sst->get_sstable_level() == sstable_level);
    assert(sst->get_stats_metadata().max_timestamp == max_timestamp);
    assert(sst->generation() == gen);
    column_family_test(cf).add_sstable(sst);
}

static shared_sstable add_sstable_for_overlapping_test(lw_shared_ptr<column_family> cf, int64_t gen, sstring first_key, sstring last_key, stats_metadata stats = {}) {
    auto sst = make_sstable(cf->schema(), "", gen, la, big);
    sstables::test(sst).set_values(std::move(first_key), std::move(last_key), std::move(stats));
    column_family_test(cf).add_sstable(sst);
    return sst;
}
static shared_sstable sstable_for_overlapping_test(const schema_ptr& schema, int64_t gen, sstring first_key, sstring last_key, uint32_t level = 0) {
    auto sst = make_sstable(schema, "", gen, la, big);
    sstables::test(sst).set_values_for_leveled_strategy(0, level, 0, std::move(first_key), std::move(last_key));
    return sst;
}

// ranges: [a,b] and [c,d]
// returns true if token ranges overlap.
static bool key_range_overlaps(sstring a, sstring b, sstring c, sstring d) {
    auto range1 = create_token_range_from_keys(a, b);
    auto range2 = create_token_range_from_keys(c, d);
    return range1.overlaps(range2, dht::token_comparator());
}

static shared_sstable get_sstable(const lw_shared_ptr<column_family>& cf, int64_t generation) {
    auto sstables = cf->get_sstables();
    auto entry = boost::range::find_if(*sstables, [generation] (shared_sstable sst) { return generation == sst->generation(); });
    assert(entry != sstables->end());
    assert((*entry)->generation() == generation);
    return *entry;
}

static bool sstable_overlaps(const lw_shared_ptr<column_family>& cf, int64_t gen1, int64_t gen2) {
    auto candidate1 = get_sstable(cf, gen1);
    auto range1 = range<dht::token>::make(candidate1->get_first_decorated_key()._token, candidate1->get_last_decorated_key()._token);
    auto candidate2 = get_sstable(cf, gen2);
    auto range2 = range<dht::token>::make(candidate2->get_first_decorated_key()._token, candidate2->get_last_decorated_key()._token);
    return range1.overlaps(range2, dht::token_comparator());
}

SEASTAR_TEST_CASE(leveled_01) {
    column_family_for_tests cf;

    auto key_and_token_pair = token_generation_for_current_shard(50);
    auto min_key = key_and_token_pair[0].first;
    auto max_key = key_and_token_pair[key_and_token_pair.size()-1].first;
    auto max_sstable_size_in_mb = 1;
    auto max_sstable_size = max_sstable_size_in_mb*1024*1024;

    // Creating two sstables which key range overlap.
    add_sstable_for_leveled_test(cf, /*gen*/1, max_sstable_size, /*level*/0, min_key, max_key);
    BOOST_REQUIRE(cf->get_sstables()->size() == 1);

    add_sstable_for_leveled_test(cf, /*gen*/2, max_sstable_size, /*level*/0, key_and_token_pair[1].first, max_key);
    BOOST_REQUIRE(cf->get_sstables()->size() == 2);

    BOOST_REQUIRE(key_range_overlaps(min_key, max_key, key_and_token_pair[1].first, max_key) == true);
    BOOST_REQUIRE(sstable_overlaps(cf, 1, 2) == true);

    auto candidates = get_candidates_for_leveled_strategy(*cf);
    sstables::size_tiered_compaction_strategy_options stcs_options;
    leveled_manifest manifest = leveled_manifest::create(*cf, candidates, max_sstable_size_in_mb, stcs_options);
    BOOST_REQUIRE(manifest.get_level_size(0) == 2);
    std::vector<stdx::optional<dht::decorated_key>> last_compacted_keys(leveled_manifest::MAX_LEVELS);
    std::vector<int> compaction_counter(leveled_manifest::MAX_LEVELS);
    auto candidate = manifest.get_compaction_candidates(last_compacted_keys, compaction_counter);
    BOOST_REQUIRE(candidate.sstables.size() == 2);
    BOOST_REQUIRE(candidate.level == 1);

    std::set<unsigned long> gens = { 1, 2 };
    for (auto& sst : candidate.sstables) {
        auto it = gens.find(sst->generation());
        BOOST_REQUIRE(it != gens.end());
        gens.erase(sst->generation());
        BOOST_REQUIRE(sst->get_sstable_level() == 0);
    }
    BOOST_REQUIRE(gens.empty());

    return make_ready_future<>();
}

SEASTAR_TEST_CASE(leveled_02) {
    column_family_for_tests cf;

    auto key_and_token_pair = token_generation_for_current_shard(50);
    auto min_key = key_and_token_pair[0].first;
    auto max_key = key_and_token_pair[key_and_token_pair.size()-1].first;
    auto max_sstable_size_in_mb = 1;
    auto max_sstable_size = max_sstable_size_in_mb*1024*1024;

    // Generation 1 will overlap only with generation 2.
    // Remember that for level0, leveled strategy prefer choosing older sstables as candidates.

    add_sstable_for_leveled_test(cf, /*gen*/1, max_sstable_size, /*level*/0, min_key, key_and_token_pair[10].first);
    BOOST_REQUIRE(cf->get_sstables()->size() == 1);

    add_sstable_for_leveled_test(cf, /*gen*/2, max_sstable_size, /*level*/0, min_key, key_and_token_pair[20].first);
    BOOST_REQUIRE(cf->get_sstables()->size() == 2);

    add_sstable_for_leveled_test(cf, /*gen*/3, max_sstable_size, /*level*/0, key_and_token_pair[30].first, max_key);
    BOOST_REQUIRE(cf->get_sstables()->size() == 3);

    BOOST_REQUIRE(key_range_overlaps(min_key, key_and_token_pair[10].first, min_key, key_and_token_pair[20].first) == true);
    BOOST_REQUIRE(key_range_overlaps(min_key, key_and_token_pair[20].first, key_and_token_pair[30].first, max_key) == false);
    BOOST_REQUIRE(key_range_overlaps(min_key, key_and_token_pair[10].first, key_and_token_pair[30].first, max_key) == false);
    BOOST_REQUIRE(sstable_overlaps(cf, 1, 2) == true);
    BOOST_REQUIRE(sstable_overlaps(cf, 2, 1) == true);
    BOOST_REQUIRE(sstable_overlaps(cf, 1, 3) == false);
    BOOST_REQUIRE(sstable_overlaps(cf, 2, 3) == false);

    auto candidates = get_candidates_for_leveled_strategy(*cf);
    sstables::size_tiered_compaction_strategy_options stcs_options;
    leveled_manifest manifest = leveled_manifest::create(*cf, candidates, max_sstable_size_in_mb, stcs_options);
    BOOST_REQUIRE(manifest.get_level_size(0) == 3);
    std::vector<stdx::optional<dht::decorated_key>> last_compacted_keys(leveled_manifest::MAX_LEVELS);
    std::vector<int> compaction_counter(leveled_manifest::MAX_LEVELS);
    auto candidate = manifest.get_compaction_candidates(last_compacted_keys, compaction_counter);
    BOOST_REQUIRE(candidate.sstables.size() == 3);
    BOOST_REQUIRE(candidate.level == 1);

    std::set<unsigned long> gens = { 1, 2, 3 };
    for (auto& sst : candidate.sstables) {
        auto it = gens.find(sst->generation());
        BOOST_REQUIRE(it != gens.end());
        gens.erase(sst->generation());
        BOOST_REQUIRE(sst->get_sstable_level() == 0);
    }
    BOOST_REQUIRE(gens.empty());

    return make_ready_future<>();
}

SEASTAR_TEST_CASE(leveled_03) {
    column_family_for_tests cf;

    auto key_and_token_pair = token_generation_for_current_shard(50);
    auto min_key = key_and_token_pair[0].first;
    auto max_key = key_and_token_pair[key_and_token_pair.size()-1].first;

    // Creating two sstables of level 0 which overlap
    add_sstable_for_leveled_test(cf, /*gen*/1, /*data_size*/1024*1024, /*level*/0, min_key, key_and_token_pair[10].first);
    add_sstable_for_leveled_test(cf, /*gen*/2, /*data_size*/1024*1024, /*level*/0, min_key, key_and_token_pair[20].first);
    // Creating a sstable of level 1 which overlap with two sstables above.
    add_sstable_for_leveled_test(cf, /*gen*/3, /*data_size*/1024*1024, /*level*/1, min_key, key_and_token_pair[30].first);
    // Creating a sstable of level 1 which doesn't overlap with any sstable.
    add_sstable_for_leveled_test(cf, /*gen*/4, /*data_size*/1024*1024, /*level*/1, key_and_token_pair[40].first, max_key);

    BOOST_REQUIRE(cf->get_sstables()->size() == 4);

    BOOST_REQUIRE(key_range_overlaps(min_key, key_and_token_pair[10].first, min_key, key_and_token_pair[20].first) == true);
    BOOST_REQUIRE(key_range_overlaps(min_key, key_and_token_pair[10].first, min_key, key_and_token_pair[30].first) == true);
    BOOST_REQUIRE(key_range_overlaps(min_key, key_and_token_pair[20].first, min_key, key_and_token_pair[30].first) == true);
    BOOST_REQUIRE(key_range_overlaps(min_key, key_and_token_pair[10].first, key_and_token_pair[40].first, max_key) == false);
    BOOST_REQUIRE(key_range_overlaps(min_key, key_and_token_pair[30].first, key_and_token_pair[40].first, max_key) == false);
    BOOST_REQUIRE(sstable_overlaps(cf, 1, 2) == true);
    BOOST_REQUIRE(sstable_overlaps(cf, 1, 3) == true);
    BOOST_REQUIRE(sstable_overlaps(cf, 2, 3) == true);
    BOOST_REQUIRE(sstable_overlaps(cf, 1, 4) == false);
    BOOST_REQUIRE(sstable_overlaps(cf, 2, 4) == false);
    BOOST_REQUIRE(sstable_overlaps(cf, 3, 4) == false);

    auto max_sstable_size_in_mb = 1;
    auto candidates = get_candidates_for_leveled_strategy(*cf);
    sstables::size_tiered_compaction_strategy_options stcs_options;
    leveled_manifest manifest = leveled_manifest::create(*cf, candidates, max_sstable_size_in_mb, stcs_options);
    BOOST_REQUIRE(manifest.get_level_size(0) == 2);
    BOOST_REQUIRE(manifest.get_level_size(1) == 2);
    std::vector<stdx::optional<dht::decorated_key>> last_compacted_keys(leveled_manifest::MAX_LEVELS);
    std::vector<int> compaction_counter(leveled_manifest::MAX_LEVELS);
    auto candidate = manifest.get_compaction_candidates(last_compacted_keys, compaction_counter);
    BOOST_REQUIRE(candidate.sstables.size() == 3);
    BOOST_REQUIRE(candidate.level == 1);

    std::set<std::pair<unsigned long, uint32_t>> gen_and_level = { {1,0}, {2,0}, {3,1} };
    for (auto& sst : candidate.sstables) {
        std::pair<unsigned long, uint32_t> pair(sst->generation(), sst->get_sstable_level());
        auto it = gen_and_level.find(pair);
        BOOST_REQUIRE(it != gen_and_level.end());
        BOOST_REQUIRE(sst->get_sstable_level() == it->second);
        gen_and_level.erase(pair);
    }
    BOOST_REQUIRE(gen_and_level.empty());

    return make_ready_future<>();
}

SEASTAR_TEST_CASE(leveled_04) {
    column_family_for_tests cf;

    auto key_and_token_pair = token_generation_for_current_shard(50);
    auto min_key = key_and_token_pair[0].first;
    auto max_key = key_and_token_pair[key_and_token_pair.size()-1].first;

    auto max_sstable_size_in_mb = 1;
    auto max_sstable_size_in_bytes = max_sstable_size_in_mb*1024*1024;

    // add 1 level-0 sstable to cf.
    add_sstable_for_leveled_test(cf, /*gen*/1, /*data_size*/max_sstable_size_in_bytes, /*level*/0, min_key, max_key);

    // create two big sstables in level1 to force leveled compaction on it.
    auto max_bytes_for_l1 = leveled_manifest::max_bytes_for_level(1, max_sstable_size_in_bytes);
    // NOTE: SSTables in level1 cannot overlap.
    add_sstable_for_leveled_test(cf, /*gen*/2, /*data_size*/max_bytes_for_l1, /*level*/1, min_key, key_and_token_pair[25].first);
    add_sstable_for_leveled_test(cf, /*gen*/3, /*data_size*/max_bytes_for_l1, /*level*/1, key_and_token_pair[26].first, max_key);

    // Create SSTable in level2 that overlaps with the ones in level1,
    // so compaction in level1 will select overlapping sstables in
    // level2.
    add_sstable_for_leveled_test(cf, /*gen*/4, /*data_size*/max_sstable_size_in_bytes, /*level*/2, min_key, max_key);

    BOOST_REQUIRE(cf->get_sstables()->size() == 4);

    BOOST_REQUIRE(key_range_overlaps(min_key, max_key, min_key, max_key) == true);
    BOOST_REQUIRE(sstable_overlaps(cf, 1, 2) == true);
    BOOST_REQUIRE(sstable_overlaps(cf, 1, 3) == true);
    BOOST_REQUIRE(sstable_overlaps(cf, 2, 3) == false);
    BOOST_REQUIRE(sstable_overlaps(cf, 3, 4) == true);
    BOOST_REQUIRE(sstable_overlaps(cf, 2, 4) == true);

    auto candidates = get_candidates_for_leveled_strategy(*cf);
    sstables::size_tiered_compaction_strategy_options stcs_options;
    leveled_manifest manifest = leveled_manifest::create(*cf, candidates, max_sstable_size_in_mb, stcs_options);
    BOOST_REQUIRE(manifest.get_level_size(0) == 1);
    BOOST_REQUIRE(manifest.get_level_size(1) == 2);
    BOOST_REQUIRE(manifest.get_level_size(2) == 1);

    // checks scores; used to determine the level of compaction to proceed with.
    auto level1_score = (double) manifest.get_total_bytes(manifest.get_level(1)) / (double) manifest.max_bytes_for_level(1);
    BOOST_REQUIRE(level1_score > 1.001);
    auto level2_score = (double) manifest.get_total_bytes(manifest.get_level(2)) / (double) manifest.max_bytes_for_level(2);
    BOOST_REQUIRE(level2_score < 1.001);

    std::vector<stdx::optional<dht::decorated_key>> last_compacted_keys(leveled_manifest::MAX_LEVELS);
    std::vector<int> compaction_counter(leveled_manifest::MAX_LEVELS);
    auto candidate = manifest.get_compaction_candidates(last_compacted_keys, compaction_counter);
    BOOST_REQUIRE(candidate.sstables.size() == 2);
    BOOST_REQUIRE(candidate.level == 2);

    std::set<unsigned long> levels = { 1, 2 };
    for (auto& sst : candidate.sstables) {
        auto it = levels.find(sst->get_sstable_level());
        BOOST_REQUIRE(it != levels.end());
        levels.erase(sst->get_sstable_level());
    }
    BOOST_REQUIRE(levels.empty());

    return make_ready_future<>();
}

SEASTAR_TEST_CASE(leveled_05) {
    // NOTE: Generations from 48 to 51 are used here.
    return test_setup::do_with_tmp_directory([] (sstring tmpdir_path) {

        // Check compaction code with leveled strategy. In this test, two sstables of level 0 will be created.
        return compact_sstables(tmpdir_path, { 48, 49 }, 50, true, 1024*1024, compaction_strategy_type::leveled).then([tmpdir_path] (auto generations) {
            BOOST_REQUIRE(generations.size() == 2);
            BOOST_REQUIRE(generations[0] == 50);
            BOOST_REQUIRE(generations[1] == 51);

            return seastar::async([&, generations = std::move(generations), tmpdir_path] {
                for (auto gen : generations) {
                    auto fname = sstable::filename(tmpdir_path, "ks", "cf", la, gen, big, component_type::Data);
                    BOOST_REQUIRE(file_size(fname).get0() >= 1024*1024);
                }
            });
        });
    });
}

SEASTAR_TEST_CASE(leveled_06) {
    // Test that we can compact a single L1 compaction into an empty L2.

    column_family_for_tests cf;

    auto max_sstable_size_in_mb = 1;
    auto max_sstable_size_in_bytes = max_sstable_size_in_mb*1024*1024;

    auto max_bytes_for_l1 = leveled_manifest::max_bytes_for_level(1, max_sstable_size_in_bytes);
    // Create fake sstable that will be compacted into L2.
    add_sstable_for_leveled_test(cf, /*gen*/1, /*data_size*/max_bytes_for_l1*2, /*level*/1, "a", "a");
    BOOST_REQUIRE(cf->get_sstables()->size() == 1);

    auto candidates = get_candidates_for_leveled_strategy(*cf);
    sstables::size_tiered_compaction_strategy_options stcs_options;
    leveled_manifest manifest = leveled_manifest::create(*cf, candidates, max_sstable_size_in_mb, stcs_options);
    BOOST_REQUIRE(manifest.get_level_size(0) == 0);
    BOOST_REQUIRE(manifest.get_level_size(1) == 1);
    BOOST_REQUIRE(manifest.get_level_size(2) == 0);

    std::vector<stdx::optional<dht::decorated_key>> last_compacted_keys(leveled_manifest::MAX_LEVELS);
    std::vector<int> compaction_counter(leveled_manifest::MAX_LEVELS);
    auto candidate = manifest.get_compaction_candidates(last_compacted_keys, compaction_counter);
    BOOST_REQUIRE(candidate.level == 2);
    BOOST_REQUIRE(candidate.sstables.size() == 1);
    auto& sst = (candidate.sstables)[0];
    BOOST_REQUIRE(sst->get_sstable_level() == 1);
    BOOST_REQUIRE(sst->generation() == 1);

    return make_ready_future<>();
}

SEASTAR_TEST_CASE(leveled_07) {
    column_family_for_tests cf;

    for (auto i = 0; i < leveled_manifest::MAX_COMPACTING_L0*2; i++) {
        add_sstable_for_leveled_test(cf, i, 1024*1024, /*level*/0, "a", "a", i /* max timestamp */);
    }
    auto candidates = get_candidates_for_leveled_strategy(*cf);
    sstables::size_tiered_compaction_strategy_options stcs_options;
    leveled_manifest manifest = leveled_manifest::create(*cf, candidates, 1, stcs_options);
    std::vector<stdx::optional<dht::decorated_key>> last_compacted_keys(leveled_manifest::MAX_LEVELS);
    std::vector<int> compaction_counter(leveled_manifest::MAX_LEVELS);
    auto desc = manifest.get_compaction_candidates(last_compacted_keys, compaction_counter);
    BOOST_REQUIRE(desc.level == 1);
    BOOST_REQUIRE(desc.sstables.size() == leveled_manifest::MAX_COMPACTING_L0);
    // check that strategy returns the oldest sstables
    for (auto& sst : desc.sstables) {
        BOOST_REQUIRE(sst->get_stats_metadata().max_timestamp < leveled_manifest::MAX_COMPACTING_L0);
    }

    return make_ready_future<>();
}

SEASTAR_TEST_CASE(leveled_invariant_fix) {
    column_family_for_tests cf;

    auto sstables_no = cf.schema()->max_compaction_threshold();
    auto key_and_token_pair = token_generation_for_current_shard(sstables_no);
    auto min_key = key_and_token_pair[0].first;
    auto max_key = key_and_token_pair[key_and_token_pair.size()-1].first;
    auto sstable_max_size = 1024*1024;

    // add non overlapping with min token to be discarded by strategy
    add_sstable_for_leveled_test(cf, 0, sstable_max_size, /*level*/1, min_key, min_key);

    for (auto i = 1; i < sstables_no-1; i++) {
        add_sstable_for_leveled_test(cf, i, sstable_max_size, /*level*/1, key_and_token_pair[i].first, key_and_token_pair[i].first);
    }
    // add large token span sstable into level 1, which overlaps with all sstables added in loop above.
    add_sstable_for_leveled_test(cf, sstables_no, sstable_max_size, 1, key_and_token_pair[1].first, max_key);

    auto candidates = get_candidates_for_leveled_strategy(*cf);
    sstables::size_tiered_compaction_strategy_options stcs_options;
    leveled_manifest manifest = leveled_manifest::create(*cf, candidates, 1, stcs_options);
    std::vector<stdx::optional<dht::decorated_key>> last_compacted_keys(leveled_manifest::MAX_LEVELS);
    std::vector<int> compaction_counter(leveled_manifest::MAX_LEVELS);

    auto candidate = manifest.get_compaction_candidates(last_compacted_keys, compaction_counter);
    BOOST_REQUIRE(candidate.level == 1);
    BOOST_REQUIRE(candidate.sstables.size() == size_t(sstables_no-1));
    BOOST_REQUIRE(boost::algorithm::all_of(candidate.sstables, [] (auto& sst) {
        return sst->generation() != 0;
    }));

    return make_ready_future<>();
}

SEASTAR_TEST_CASE(leveled_stcs_on_L0) {
    schema_builder builder(make_lw_shared(schema({}, some_keyspace, some_column_family,
        {{"p1", utf8_type}}, {}, {}, {}, utf8_type)));
    builder.set_min_compaction_threshold(4);
    auto s = builder.build(schema_builder::compact_storage::no);

    column_family_for_tests cf(s);

    auto key_and_token_pair = token_generation_for_current_shard(1);
    auto sstable_max_size_in_mb = 1;
    auto l0_sstables_no = s->min_compaction_threshold();
    // we don't want level 0 to be worth promoting.
    auto l0_sstables_size = (sstable_max_size_in_mb*1024*1024)/(l0_sstables_no+1);

    add_sstable_for_leveled_test(cf, 0, sstable_max_size_in_mb*1024*1024, /*level*/1, key_and_token_pair[0].first, key_and_token_pair[0].first);
    for (auto gen = 0; gen < l0_sstables_no; gen++) {
        add_sstable_for_leveled_test(cf, gen+1, l0_sstables_size, /*level*/0, key_and_token_pair[0].first, key_and_token_pair[0].first);
    }
    auto candidates = get_candidates_for_leveled_strategy(*cf);
    BOOST_REQUIRE(candidates.size() == size_t(l0_sstables_no+1));
    BOOST_REQUIRE(cf->get_sstables()->size() == size_t(l0_sstables_no+1));

    std::vector<stdx::optional<dht::decorated_key>> last_compacted_keys(leveled_manifest::MAX_LEVELS);
    std::vector<int> compaction_counter(leveled_manifest::MAX_LEVELS);
    sstables::size_tiered_compaction_strategy_options stcs_options;

    {
        leveled_manifest manifest = leveled_manifest::create(*cf, candidates, sstable_max_size_in_mb, stcs_options);
        BOOST_REQUIRE(!manifest.worth_promoting_L0_candidates(manifest.get_level(0)));
        auto candidate = manifest.get_compaction_candidates(last_compacted_keys, compaction_counter);
        BOOST_REQUIRE(candidate.level == 0);
        BOOST_REQUIRE(candidate.sstables.size() == size_t(l0_sstables_no));
        BOOST_REQUIRE(boost::algorithm::all_of(candidate.sstables, [] (auto& sst) {
            return sst->generation() != 0;
        }));
    }
    {
        candidates.resize(2);
        leveled_manifest manifest = leveled_manifest::create(*cf, candidates, sstable_max_size_in_mb, stcs_options);
        auto candidate = manifest.get_compaction_candidates(last_compacted_keys, compaction_counter);
        BOOST_REQUIRE(candidate.level == 0);
        BOOST_REQUIRE(candidate.sstables.empty());
    }

    return make_ready_future<>();
}

SEASTAR_TEST_CASE(overlapping_starved_sstables_test) {
    column_family_for_tests cf;

    auto key_and_token_pair = token_generation_for_current_shard(5);
    auto min_key = key_and_token_pair[0].first;
    auto max_sstable_size_in_mb = 1;
    auto max_sstable_size_in_bytes = max_sstable_size_in_mb*1024*1024;

    // we compact 2 sstables: 0->2 in L1 and 0->1 in L2, and rely on strategy
    // to bring a sstable from level 3 that theoretically wasn't compacted
    // for many rounds and won't introduce an overlap.
    auto max_bytes_for_l1 = leveled_manifest::max_bytes_for_level(1, max_sstable_size_in_bytes);
    add_sstable_for_leveled_test(cf, /*gen*/1, max_bytes_for_l1*1.1, /*level*/1, min_key, key_and_token_pair[2].first);
    add_sstable_for_leveled_test(cf, /*gen*/2, max_sstable_size_in_bytes, /*level*/2, min_key, key_and_token_pair[1].first);
    add_sstable_for_leveled_test(cf, /*gen*/3, max_sstable_size_in_bytes, /*level*/3, min_key, key_and_token_pair[1].first);

    std::vector<stdx::optional<dht::decorated_key>> last_compacted_keys(leveled_manifest::MAX_LEVELS);
    std::vector<int> compaction_counter(leveled_manifest::MAX_LEVELS);
    // make strategy think that level 3 wasn't compacted for many rounds
    compaction_counter[3] = leveled_manifest::NO_COMPACTION_LIMIT+1;

    auto candidates = get_candidates_for_leveled_strategy(*cf);
    sstables::size_tiered_compaction_strategy_options stcs_options;
    leveled_manifest manifest = leveled_manifest::create(*cf, candidates, max_sstable_size_in_mb, stcs_options);
    auto candidate = manifest.get_compaction_candidates(last_compacted_keys, compaction_counter);
    BOOST_REQUIRE(candidate.level == 2);
    BOOST_REQUIRE(candidate.sstables.size() == 3);

    return make_ready_future<>();
}

SEASTAR_TEST_CASE(check_overlapping) {
    column_family_for_tests cf;

    auto key_and_token_pair = token_generation_for_current_shard(4);
    auto min_key = key_and_token_pair[0].first;
    auto max_key = key_and_token_pair[key_and_token_pair.size()-1].first;

    auto sst1 = add_sstable_for_overlapping_test(cf, /*gen*/1, min_key, key_and_token_pair[1].first);
    auto sst2 = add_sstable_for_overlapping_test(cf, /*gen*/2, min_key, key_and_token_pair[2].first);
    auto sst3 = add_sstable_for_overlapping_test(cf, /*gen*/3, key_and_token_pair[3].first, max_key);
    auto sst4 = add_sstable_for_overlapping_test(cf, /*gen*/4, min_key, max_key);
    BOOST_REQUIRE(cf->get_sstables()->size() == 4);

    std::vector<shared_sstable> compacting = { sst1, sst2 };
    std::vector<shared_sstable> uncompacting = { sst3, sst4 };

    auto overlapping_sstables = leveled_manifest::overlapping(*cf.schema(), compacting, uncompacting);
    BOOST_REQUIRE(overlapping_sstables.size() == 1);
    BOOST_REQUIRE(overlapping_sstables.front()->generation() == 4);

    return make_ready_future<>();
}

SEASTAR_TEST_CASE(check_read_indexes) {
  return for_each_sstable_version([] (const sstables::sstable::version_types version) {
    auto builder = schema_builder("test", "summary_test")
        .with_column("a", int32_type, column_kind::partition_key);
    builder.set_min_index_interval(256);
    auto s = builder.build();

    auto sst = make_sstable(s, get_test_dir("summary_test", s), 1, version, big);

    auto fut = sst->load();
    return fut.then([sst] {
        return sstables::test(sst).read_indexes().then([sst] (index_list list) {
            BOOST_REQUIRE(list.size() == 130);
            return make_ready_future<>();
        });
    });
  });
}

SEASTAR_TEST_CASE(tombstone_purge_test) {
    BOOST_REQUIRE(smp::count == 1);
    return seastar::async([] {
        storage_service_for_tests ssft;
        cell_locker_stats cl_stats;

        // In a column family with gc_grace_seconds set to 0, check that a tombstone
        // is purged after compaction.
        auto builder = schema_builder("tests", "tombstone_purge")
                .with_column("id", utf8_type, column_kind::partition_key)
                .with_column("value", int32_type);
        builder.set_gc_grace_seconds(0);
        auto s = builder.build();

        auto tmp = make_lw_shared<tmpdir>();
        auto sst_gen = [s, tmp, gen = make_lw_shared<unsigned>(1)] () mutable {
            return make_sstable(s, tmp->path, (*gen)++, la, big);
        };

        auto compact = [&, s] (std::vector<shared_sstable> all, std::vector<shared_sstable> to_compact) -> std::vector<shared_sstable> {
            column_family_for_tests cf(s);
            for (auto&& sst : all) {
                column_family_test(cf).add_sstable(sst);
            }
            return sstables::compact_sstables(sstables::compaction_descriptor(to_compact), *cf, sst_gen).get0().new_sstables;
        };

        auto next_timestamp = [] {
            static thread_local api::timestamp_type next = 1;
            return next++;
        };

        auto make_insert = [&] (partition_key key) {
            mutation m(s, key);
            m.set_clustered_cell(clustering_key::make_empty(), bytes("value"), data_value(int32_t(1)), next_timestamp());
            return m;
        };

        auto make_expiring = [&] (partition_key key, int ttl) {
            mutation m(s, key);
            m.set_clustered_cell(clustering_key::make_empty(), bytes("value"), data_value(int32_t(1)),
                gc_clock::now().time_since_epoch().count(), gc_clock::duration(ttl));
            return m;
        };

        auto make_delete = [&] (partition_key key) {
            mutation m(s, key);
            tombstone tomb(next_timestamp(), gc_clock::now());
            m.partition().apply(tomb);
            return m;
        };

        auto assert_that_produces_dead_cell = [&] (auto& sst, partition_key& key) {
            auto reader = make_lw_shared(sstable_reader(sst, s));
            read_mutation_from_flat_mutation_reader(*reader, db::no_timeout).then([reader, s, &key] (mutation_opt m) {
                BOOST_REQUIRE(m);
                BOOST_REQUIRE(m->key().equal(*s, key));
                auto& rows = m->partition().clustered_rows();
                BOOST_REQUIRE_EQUAL(rows.calculate_size(), 1);
                auto& row = rows.begin()->row();
                auto& cells = row.cells();
                BOOST_REQUIRE_EQUAL(cells.size(), 1);
                auto& cdef = *s->get_column_definition("value");
                BOOST_REQUIRE(!cells.cell_at(cdef.id).as_atomic_cell(cdef).is_live());
                return (*reader)(db::no_timeout);
            }).then([reader, s] (mutation_fragment_opt m) {
                BOOST_REQUIRE(!m);
            }).get();
        };

        auto alpha = partition_key::from_exploded(*s, {to_bytes("alpha")});
        auto beta = partition_key::from_exploded(*s, {to_bytes("beta")});

        auto ttl = 5;

        {
            auto mut1 = make_insert(alpha);
            auto mut2 = make_insert(beta);
            auto mut3 = make_delete(alpha);

            std::vector<shared_sstable> sstables = {
                    make_sstable_containing(sst_gen, {mut1, mut2}),
                    make_sstable_containing(sst_gen, {mut3})
            };

            forward_jump_clocks(std::chrono::seconds(ttl));

            auto result = compact(sstables, sstables);
            BOOST_REQUIRE_EQUAL(1, result.size());

            assert_that(sstable_reader(result[0], s))
                    .produces(mut2)
                    .produces_end_of_stream();
        }

        {
            auto mut1 = make_insert(alpha);
            auto mut2 = make_insert(alpha);
            auto mut3 = make_delete(alpha);

            auto sst1 = make_sstable_containing(sst_gen, {mut1});
            auto sst2 = make_sstable_containing(sst_gen, {mut2, mut3});

            forward_jump_clocks(std::chrono::seconds(ttl));

            auto result = compact({sst1, sst2}, {sst2});
            BOOST_REQUIRE_EQUAL(1, result.size());

            assert_that(sstable_reader(result[0], s))
                    .produces(mut3)
                    .produces_end_of_stream();
        }

        {
            auto mut1 = make_insert(alpha);
            auto mut2 = make_delete(alpha);
            auto mut3 = make_insert(beta);
            auto mut4 = make_insert(alpha);

            auto sst1 = make_sstable_containing(sst_gen, {mut1, mut2, mut3});
            auto sst2 = make_sstable_containing(sst_gen, {mut4});

            forward_jump_clocks(std::chrono::seconds(ttl));

            auto result = compact({sst1, sst2}, {sst1});
            BOOST_REQUIRE_EQUAL(1, result.size());

            assert_that(sstable_reader(result[0], s))
                    .produces(mut3)
                    .produces_end_of_stream();
        }

        {
            auto mut1 = make_insert(alpha);
            auto mut2 = make_delete(alpha);
            auto mut3 = make_insert(beta);
            auto mut4 = make_insert(beta);

            auto sst1 = make_sstable_containing(sst_gen, {mut1, mut2, mut3});
            auto sst2 = make_sstable_containing(sst_gen, {mut4});

            forward_jump_clocks(std::chrono::seconds(ttl));

            auto result = compact({sst1, sst2}, {sst1});
            BOOST_REQUIRE_EQUAL(1, result.size());

            assert_that(sstable_reader(result[0], s))
                    .produces(mut3)
                    .produces_end_of_stream();
        }

        {
            // check that expired cell will not be purged if it will ressurect overwritten data.
            auto mut1 = make_insert(alpha);
            auto mut2 = make_expiring(alpha, ttl);

            auto sst1 = make_sstable_containing(sst_gen, {mut1});
            auto sst2 = make_sstable_containing(sst_gen, {mut2});

            forward_jump_clocks(std::chrono::seconds(ttl));

            auto result = compact({sst1, sst2}, {sst2});
            BOOST_REQUIRE_EQUAL(1, result.size());
            assert_that_produces_dead_cell(result[0], alpha);

            result = compact({sst1, sst2}, {sst1, sst2});
            BOOST_REQUIRE_EQUAL(0, result.size());
        }
        {
            auto mut1 = make_insert(alpha);
            auto mut2 = make_expiring(beta, ttl);

            auto sst1 = make_sstable_containing(sst_gen, {mut1});
            auto sst2 = make_sstable_containing(sst_gen, {mut2});

            forward_jump_clocks(std::chrono::seconds(ttl));

            auto result = compact({sst1, sst2}, {sst2});
            BOOST_REQUIRE_EQUAL(0, result.size());
        }
        {
            auto mut1 = make_insert(alpha);
            auto mut2 = make_expiring(alpha, ttl);
            auto mut3 = make_insert(beta);

            auto sst1 = make_sstable_containing(sst_gen, {mut1});
            auto sst2 = make_sstable_containing(sst_gen, {mut2, mut3});

            forward_jump_clocks(std::chrono::seconds(ttl));

            auto result = compact({sst1, sst2}, {sst1, sst2});
            BOOST_REQUIRE_EQUAL(1, result.size());
            assert_that(sstable_reader(result[0], s))
                    .produces(mut3)
                    .produces_end_of_stream();
        }
    });
}

SEASTAR_TEST_CASE(check_multi_schema) {
    // Schema used to write sstable:
    // CREATE TABLE multi_schema_test (
    //        a int PRIMARY KEY,
    //        b int,
    //        c int,
    //        d set<int>,
    //        e int
    //);

    // Schema used to read sstable:
    // CREATE TABLE multi_schema_test (
    //        a int PRIMARY KEY,
    //        c set<int>,
    //        d int,
    //        e blob
    //);
    return for_each_sstable_version([] (const sstables::sstable::version_types version) {
        auto set_of_ints_type = set_type_impl::get_instance(int32_type, true);
        auto builder = schema_builder("test", "test_multi_schema")
            .with_column("a", int32_type, column_kind::partition_key)
            .with_column("c", set_of_ints_type)
            .with_column("d", int32_type)
            .with_column("e", bytes_type);
        auto s = builder.build();

        auto sst = make_sstable(s, get_test_dir("multi_schema_test", s), 1, version, big);
        auto f = sst->load();
        return f.then([sst, s] {
            auto reader = make_lw_shared(sstable_reader(sst, s));
            return read_mutation_from_flat_mutation_reader(*reader, db::no_timeout).then([reader, s] (mutation_opt m) {
                BOOST_REQUIRE(m);
                BOOST_REQUIRE(m->key().equal(*s, partition_key::from_singular(*s, 0)));
                auto& rows = m->partition().clustered_rows();
                BOOST_REQUIRE_EQUAL(rows.calculate_size(), 1);
                auto& row = rows.begin()->row();
                BOOST_REQUIRE(!row.deleted_at());
                auto& cells = row.cells();
                BOOST_REQUIRE_EQUAL(cells.size(), 1);
                auto& cdef = *s->get_column_definition("e");
                BOOST_REQUIRE_EQUAL(cells.cell_at(cdef.id).as_atomic_cell(cdef).value(), int32_type->decompose(5));
                return (*reader)(db::no_timeout);
            }).then([reader, s] (mutation_fragment_opt m) {
                BOOST_REQUIRE(!m);
            });
        });
        return make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(sstable_rewrite) {
    BOOST_REQUIRE(smp::count == 1);
    return test_setup::do_with_tmp_directory([] (sstring tmpdir_path) {
        auto s = make_lw_shared(schema({}, some_keyspace, some_column_family,
            {{"p1", utf8_type}}, {{"c1", utf8_type}}, {{"r1", utf8_type}}, {}, utf8_type));

        auto mt = make_lw_shared<memtable>(s);

        const column_definition& r1_col = *s->get_column_definition("r1");

        auto key_for_this_shard = token_generation_for_current_shard(1);
        auto apply_key = [mt, s, &r1_col] (sstring key_to_write) {
            auto key = partition_key::from_exploded(*s, {to_bytes(key_to_write)});
            auto c_key = clustering_key::from_exploded(*s, {to_bytes("c1")});
            mutation m(s, key);
            m.set_clustered_cell(c_key, r1_col, make_atomic_cell(utf8_type, bytes("a")));
            mt->apply(std::move(m));
        };
        apply_key(key_for_this_shard[0].first);

        auto sst = make_sstable(s, tmpdir_path, 51, la, big);
        return write_memtable_to_sstable_for_test(*mt, sst).then([s, sst, tmpdir_path] {
            return reusable_sst(s, tmpdir_path, 51);
        }).then([s, key = key_for_this_shard[0].first, tmpdir_path] (auto sstp) mutable {
            auto new_tables = make_lw_shared<std::vector<sstables::shared_sstable>>();
            auto creator = [new_tables, s, tmpdir_path] {
                auto sst = sstables::make_sstable(s, tmpdir_path, 52, la, big);
                sst->set_unshared();
                new_tables->emplace_back(sst);
                return sst;
            };
            column_family_for_tests cf(s);
            std::vector<shared_sstable> sstables;
            sstables.push_back(std::move(sstp));

            return sstables::compact_sstables(sstables::compaction_descriptor(std::move(sstables)), *cf, creator).then([s, key, new_tables] (auto) {
                BOOST_REQUIRE(new_tables->size() == 1);
                auto newsst = (*new_tables)[0];
                BOOST_REQUIRE(newsst->generation() == 52);
                auto reader = make_lw_shared(sstable_reader(newsst, s));
                return (*reader)(db::no_timeout).then([s, reader, key] (mutation_fragment_opt m) {
                    BOOST_REQUIRE(m);
                    BOOST_REQUIRE(m->is_partition_start());
                    auto pkey = partition_key::from_exploded(*s, {to_bytes(key)});
                    BOOST_REQUIRE(m->as_partition_start().key().key().equal(*s, pkey));
                    reader->next_partition();
                    return (*reader)(db::no_timeout);
                }).then([reader] (mutation_fragment_opt m) {
                    BOOST_REQUIRE(!m);
                });
            }).then([cf] {});
        }).then([sst, mt, s] {});
    });
}

void test_sliced_read_row_presence(shared_sstable sst, schema_ptr s, const query::partition_slice& ps,
    std::vector<std::pair<partition_key, std::vector<clustering_key>>> expected)
{
    auto reader = sst->as_mutation_source().make_reader(s, query::full_partition_range, ps);

    partition_key::equality pk_eq(*s);
    clustering_key::equality ck_eq(*s);

    auto mfopt = reader(db::no_timeout).get0();
    while (mfopt) {
        BOOST_REQUIRE(mfopt->is_partition_start());
        auto it = std::find_if(expected.begin(), expected.end(), [&] (auto&& x) {
            return pk_eq(x.first, mfopt->as_partition_start().key().key());
        });
        BOOST_REQUIRE(it != expected.end());
        auto expected_cr = std::move(it->second);
        expected.erase(it);

        mfopt = reader(db::no_timeout).get0();
        BOOST_REQUIRE(mfopt);
        while (!mfopt->is_end_of_partition()) {
            if (mfopt->is_clustering_row()) {
                auto& cr = mfopt->as_clustering_row();
                auto it = std::find_if(expected_cr.begin(), expected_cr.end(), [&] (auto&& x) {
                    return ck_eq(x, cr.key());
                });
                if (it == expected_cr.end()) {
                    std::cout << "unexpected clustering row: " << cr.key() << "\n";
                }
                BOOST_REQUIRE(it != expected_cr.end());
                expected_cr.erase(it);
            }
            mfopt = reader(db::no_timeout).get0();
            BOOST_REQUIRE(mfopt);
        }
        BOOST_REQUIRE(expected_cr.empty());

        mfopt = reader(db::no_timeout).get0();
    }
    BOOST_REQUIRE(expected.empty());
}

SEASTAR_TEST_CASE(test_sliced_mutation_reads) {
    // CREATE TABLE sliced_mutation_reads_test (
    //        pk int,
    //        ck int,
    //        v1 int,
    //        v2 set<int>,
    //        PRIMARY KEY (pk, ck)
    //);
    //
    // insert into sliced_mutation_reads_test (pk, ck, v1) values (0, 0, 1);
    // insert into sliced_mutation_reads_test (pk, ck, v2) values (0, 1, { 0, 1 });
    // update sliced_mutation_reads_test set v1 = 3 where pk = 0 and ck = 2;
    // insert into sliced_mutation_reads_test (pk, ck, v1) values (0, 3, null);
    // insert into sliced_mutation_reads_test (pk, ck, v2) values (0, 4, null);
    // insert into sliced_mutation_reads_test (pk, ck, v1) values (1, 1, 1);
    // insert into sliced_mutation_reads_test (pk, ck, v1) values (1, 3, 1);
    // insert into sliced_mutation_reads_test (pk, ck, v1) values (1, 5, 1);
    return seastar::async([] {
      for (auto version : all_sstable_versions) {
        auto set_of_ints_type = set_type_impl::get_instance(int32_type, true);
        auto builder = schema_builder("ks", "sliced_mutation_reads_test")
            .with_column("pk", int32_type, column_kind::partition_key)
            .with_column("ck", int32_type, column_kind::clustering_key)
            .with_column("v1", int32_type)
            .with_column("v2", set_of_ints_type);
        auto s = builder.build();

        auto sst = make_sstable(s, get_test_dir("sliced_mutation_reads", s), 1, version, big);
        sst->load().get0();

        {
            auto ps = partition_slice_builder(*s)
                          .with_range(query::clustering_range::make_singular(
                              clustering_key_prefix::from_single_value(*s, int32_type->decompose(0))))
                          .with_range(query::clustering_range::make_singular(
                              clustering_key_prefix::from_single_value(*s, int32_type->decompose(5))))
                          .build();
            test_sliced_read_row_presence(sst, s, ps, {
                std::make_pair(partition_key::from_single_value(*s, int32_type->decompose(0)),
                    std::vector<clustering_key> { clustering_key_prefix::from_single_value(*s, int32_type->decompose(0)) }),
                std::make_pair(partition_key::from_single_value(*s, int32_type->decompose(1)),
                    std::vector<clustering_key> { clustering_key_prefix::from_single_value(*s, int32_type->decompose(5)) }),
            });
        }
        {
            auto ps = partition_slice_builder(*s)
                          .with_range(query::clustering_range {
                             query::clustering_range::bound { clustering_key_prefix::from_single_value(*s, int32_type->decompose(0)) },
                             query::clustering_range::bound { clustering_key_prefix::from_single_value(*s, int32_type->decompose(3)), false },
                          }).build();
            test_sliced_read_row_presence(sst, s, ps, {
                std::make_pair(partition_key::from_single_value(*s, int32_type->decompose(0)),
                    std::vector<clustering_key> {
                        clustering_key_prefix::from_single_value(*s, int32_type->decompose(0)),
                        clustering_key_prefix::from_single_value(*s, int32_type->decompose(1)),
                        clustering_key_prefix::from_single_value(*s, int32_type->decompose(2)),
                    }),
                std::make_pair(partition_key::from_single_value(*s, int32_type->decompose(1)),
                    std::vector<clustering_key> { clustering_key_prefix::from_single_value(*s, int32_type->decompose(1)) }),
            });
        }
        {
            auto ps = partition_slice_builder(*s)
                          .with_range(query::clustering_range {
                             query::clustering_range::bound { clustering_key_prefix::from_single_value(*s, int32_type->decompose(3)) },
                             query::clustering_range::bound { clustering_key_prefix::from_single_value(*s, int32_type->decompose(9)) },
                          }).build();
            test_sliced_read_row_presence(sst, s, ps, {
                std::make_pair(partition_key::from_single_value(*s, int32_type->decompose(0)),
                    std::vector<clustering_key> {
                        clustering_key_prefix::from_single_value(*s, int32_type->decompose(3)),
                        clustering_key_prefix::from_single_value(*s, int32_type->decompose(4)),
                    }),
                std::make_pair(partition_key::from_single_value(*s, int32_type->decompose(1)),
                    std::vector<clustering_key> {
                        clustering_key_prefix::from_single_value(*s, int32_type->decompose(3)),
                        clustering_key_prefix::from_single_value(*s, int32_type->decompose(5)),
                    }),
            });
        }
      }
    });
}

SEASTAR_TEST_CASE(test_wrong_range_tombstone_order) {
    // create table wrong_range_tombstone_order (
    //        p int,
    //        a int,
    //        b int,
    //        c int,
    //        r int,
    //        primary key (p,a,b,c)
    // ) with compact storage;
    //
    // delete from wrong_range_tombstone_order where p = 0 and a = 0;
    // insert into wrong_range_tombstone_order (p,a,r) values (0,1,1);
    // insert into wrong_range_tombstone_order (p,a,b,r) values (0,1,1,2);
    // insert into wrong_range_tombstone_order (p,a,b,r) values (0,1,2,3);
    // insert into wrong_range_tombstone_order (p,a,b,c,r) values (0,1,2,3,4);
    // delete from wrong_range_tombstone_order where p = 0 and a = 1 and b = 3;
    // insert into wrong_range_tombstone_order (p,a,b,r) values (0,1,3,5);
    // insert into wrong_range_tombstone_order (p,a,b,c,r) values (0,1,3,4,6);
    // insert into wrong_range_tombstone_order (p,a,b,r) values (0,1,4,7);
    // insert into wrong_range_tombstone_order (p,a,b,c,r) values (0,1,4,0,8);
    // delete from wrong_range_tombstone_order where p = 0 and a = 1 and b = 4 and c = 0;
    // delete from wrong_range_tombstone_order where p = 0 and a = 2;
    // delete from wrong_range_tombstone_order where p = 0 and a = 2 and b = 1;
    // delete from wrong_range_tombstone_order where p = 0 and a = 2 and b = 2;

    return seastar::async([] {
      for (const auto version : all_sstable_versions) {
        auto s = schema_builder("ks", "wrong_range_tombstone_order")
            .with(schema_builder::compact_storage::yes)
            .with_column("p", int32_type, column_kind::partition_key)
            .with_column("a", int32_type, column_kind::clustering_key)
            .with_column("b", int32_type, column_kind::clustering_key)
            .with_column("c", int32_type, column_kind::clustering_key)
            .with_column("r", int32_type)
            .build();
        clustering_key::equality ck_eq(*s);
        auto pkey = partition_key::from_exploded(*s, { int32_type->decompose(0) });
        auto dkey = dht::global_partitioner().decorate_key(*s, std::move(pkey));

        auto sst = make_sstable(s, get_test_dir("wrong_range_tombstone_order", s), 1, version, big);
        sst->load().get0();
        auto reader = make_normalizing_sstable_reader(sst, s);

        using kind = mutation_fragment::kind;
        assert_that(std::move(reader))
            .produces_partition_start(dkey)
            .produces(kind::range_tombstone, { 0 })
            .produces(kind::clustering_row, { 1 })
            .produces(kind::clustering_row, { 1, 1 })
            .produces(kind::clustering_row, { 1, 2 })
            .produces(kind::clustering_row, { 1, 2, 3 })
            .produces(kind::range_tombstone, { 1, 3 })
            .produces(kind::clustering_row, { 1, 3 })
            .produces(kind::range_tombstone, { 1, 3 }, true)
            .produces(kind::clustering_row, { 1, 3, 4 })
            .produces(kind::range_tombstone, { 1, 3, 4 })
            .produces(kind::clustering_row, { 1, 4 })
            .produces(kind::clustering_row, { 1, 4, 0 })
            .produces(kind::range_tombstone, { 2 })
            .produces(kind::range_tombstone, { 2, 1 })
            .produces(kind::range_tombstone, { 2, 1 })
            .produces(kind::range_tombstone, { 2, 2 })
            .produces(kind::range_tombstone, { 2, 2 })
            .produces_partition_end()
            .produces_end_of_stream();
      }
    });
}

SEASTAR_TEST_CASE(test_counter_read) {
        // create table counter_test (
        //      pk int,
        //      ck int,
        //      c1 counter,
        //      c2 counter,
        //      primary key (pk, ck)
        // );
        //
        // Node 1:
        // update counter_test set c1 = c1 + 8 where pk = 0 and ck = 0;
        // update counter_test set c2 = c2 - 99 where pk = 0 and ck = 0;
        // update counter_test set c1 = c1 + 3 where pk = 0 and ck = 0;
        // update counter_test set c1 = c1 + 42 where pk = 0 and ck = 1;
        //
        // Node 2:
        // update counter_test set c2 = c2 + 7 where pk = 0 and ck = 0;
        // update counter_test set c1 = c1 + 2 where pk = 0 and ck = 0;
        // delete c1 from counter_test where pk = 0 and ck = 1;
        //
        // select * from counter_test;
        // pk | ck | c1 | c2
        // ----+----+----+-----
        //  0 |  0 | 13 | -92

        return seastar::async([] {
          for (const auto version : all_sstable_versions) {
            auto s = schema_builder("ks", "counter_test")
                    .with_column("pk", int32_type, column_kind::partition_key)
                    .with_column("ck", int32_type, column_kind::clustering_key)
                    .with_column("c1", counter_type)
                    .with_column("c2", counter_type)
                    .build();

            auto node1 = counter_id(utils::UUID("8379ab99-4507-4ab1-805d-ac85a863092b"));
            auto node2 = counter_id(utils::UUID("b8a6c3f3-e222-433f-9ce9-de56a8466e07"));

            auto sst = make_sstable(s, get_test_dir("counter_test", s), 5, version, big);
            sst->load().get();
            auto reader = sstable_reader(sst, s);

            auto mfopt = reader(db::no_timeout).get0();
            BOOST_REQUIRE(mfopt);
            BOOST_REQUIRE(mfopt->is_partition_start());

            mfopt = reader(db::no_timeout).get0();
            BOOST_REQUIRE(mfopt);
            BOOST_REQUIRE(mfopt->is_clustering_row());
            const clustering_row* cr = &mfopt->as_clustering_row();
            cr->cells().for_each_cell([&] (column_id id, const atomic_cell_or_collection& c) {
              counter_cell_view::with_linearized(c.as_atomic_cell(s->regular_column_at(id)), [&] (counter_cell_view ccv) {
                auto& col = s->column_at(column_kind::regular_column, id);
                if (col.name_as_text() == "c1") {
                    BOOST_REQUIRE_EQUAL(ccv.total_value(), 13);
                    BOOST_REQUIRE_EQUAL(ccv.shard_count(), 2);

                    auto it = ccv.shards().begin();
                    auto shard = *it++;
                    BOOST_REQUIRE_EQUAL(shard.id(), node1);
                    BOOST_REQUIRE_EQUAL(shard.value(), 11);
                    BOOST_REQUIRE_EQUAL(shard.logical_clock(), 2);

                    shard = *it++;
                    BOOST_REQUIRE_EQUAL(shard.id(), node2);
                    BOOST_REQUIRE_EQUAL(shard.value(), 2);
                    BOOST_REQUIRE_EQUAL(shard.logical_clock(), 1);
                } else if (col.name_as_text() == "c2") {
                    BOOST_REQUIRE_EQUAL(ccv.total_value(), -92);
                } else {
                    BOOST_FAIL(sprint("Unexpected column \'%s\'", col.name_as_text()));
                }
              });
            });

            mfopt = reader(db::no_timeout).get0();
            BOOST_REQUIRE(mfopt);
            BOOST_REQUIRE(mfopt->is_clustering_row());
            cr = &mfopt->as_clustering_row();
            cr->cells().for_each_cell([&] (column_id id, const atomic_cell_or_collection& c) {
                auto& col = s->column_at(column_kind::regular_column, id);
                if (col.name_as_text() == "c1") {
                    BOOST_REQUIRE(!c.as_atomic_cell(col).is_live());
                } else {
                    BOOST_FAIL(sprint("Unexpected column \'%s\'", col.name_as_text()));
                }
            });

            mfopt = reader(db::no_timeout).get0();
            BOOST_REQUIRE(mfopt);
            BOOST_REQUIRE(mfopt->is_end_of_partition());

            mfopt = reader(db::no_timeout).get0();
            BOOST_REQUIRE(!mfopt);
          }
        });
}

SEASTAR_TEST_CASE(test_sstable_max_local_deletion_time) {
    return test_setup::do_with_tmp_directory([] (sstring tmpdir_path) {
        return seastar::async([tmpdir_path] {
            for (const auto version : all_sstable_versions) {
                schema_builder builder(some_keyspace, some_column_family);
                builder.with_column("p1", utf8_type, column_kind::partition_key);
                builder.with_column("c1", utf8_type, column_kind::clustering_key);
                builder.with_column("r1", utf8_type);
                schema_ptr s = builder.build(schema_builder::compact_storage::no);
                auto mt = make_lw_shared<memtable>(s);
                int32_t last_expiry = 0;
                for (auto i = 0; i < 10; i++) {
                    auto key = partition_key::from_exploded(*s, {to_bytes("key" + to_sstring(i))});
                    mutation m(s, key);
                    auto c_key = clustering_key::from_exploded(*s, {to_bytes("c1")});
                    last_expiry = (gc_clock::now() + gc_clock::duration(3600 + i)).time_since_epoch().count();
                    m.set_clustered_cell(c_key, *s->get_column_definition("r1"),
                                         make_atomic_cell(utf8_type, bytes("a"), 3600 + i, last_expiry));
                    mt->apply(std::move(m));
                }
                auto sst = make_sstable(s, tmpdir_path, 53, version, big);
                write_memtable_to_sstable_for_test(*mt, sst).get();
                auto sstp = reusable_sst(s, tmpdir_path, 53, version).get0();
                BOOST_REQUIRE(last_expiry == sstp->get_stats_metadata().max_local_deletion_time);
            }
        });
    });
}

SEASTAR_TEST_CASE(test_sstable_max_local_deletion_time_2) {
    // Create sstable A with 5x column with TTL 100 and 1x column with TTL 1000
    // Create sstable B with tombstone for column in sstable A with TTL 1000.
    // Compact them and expect that maximum deletion time is that of column with TTL 100.
    return test_setup::do_with_tmp_directory([] (sstring tmpdir_path) {
        return seastar::async([tmpdir_path] {
            for (auto version : all_sstable_versions) {
                schema_builder builder(some_keyspace, some_column_family);
                builder.with_column("p1", utf8_type, column_kind::partition_key);
                builder.with_column("c1", utf8_type, column_kind::clustering_key);
                builder.with_column("r1", utf8_type);
                schema_ptr s = builder.build(schema_builder::compact_storage::no);
                column_family_for_tests cf(s);
                auto mt = make_lw_shared<memtable>(s);
                auto now = gc_clock::now();
                int32_t last_expiry = 0;
                auto add_row = [&now, &mt, &s, &last_expiry](mutation &m, bytes column_name, uint32_t ttl) {
                    auto c_key = clustering_key::from_exploded(*s, {column_name});
                    last_expiry = (now + gc_clock::duration(ttl)).time_since_epoch().count();
                    m.set_clustered_cell(c_key, *s->get_column_definition("r1"),
                                         make_atomic_cell(utf8_type, bytes(""), ttl, last_expiry));
                    mt->apply(std::move(m));
                };
                auto get_usable_sst = [s, tmpdir_path, version](memtable &mt, int64_t gen) -> future<sstable_ptr> {
                    auto sst = make_sstable(s, tmpdir_path, gen, version, big);
                    return write_memtable_to_sstable_for_test(mt, sst).then([sst, gen, s, tmpdir_path, version] {
                        return reusable_sst(s, tmpdir_path, gen, version);
                    });
                };

                mutation m(s, partition_key::from_exploded(*s, {to_bytes("deletetest")}));
                for (auto i = 0; i < 5; i++) {
                    add_row(m, to_bytes("deletecolumn" + to_sstring(i)), 100);
                }
                add_row(m, to_bytes("todelete"), 1000);
                auto sst1 = get_usable_sst(*mt, 54).get0();
                BOOST_REQUIRE(last_expiry == sst1->get_stats_metadata().max_local_deletion_time);

                mt = make_lw_shared<memtable>(s);
                m = mutation(s, partition_key::from_exploded(*s, {to_bytes("deletetest")}));
                tombstone tomb(api::new_timestamp(), now);
                m.partition().apply_delete(*s, clustering_key::from_exploded(*s, {to_bytes("todelete")}), tomb);
                mt->apply(std::move(m));
                auto sst2 = get_usable_sst(*mt, 55).get0();
                BOOST_REQUIRE(now.time_since_epoch().count() == sst2->get_stats_metadata().max_local_deletion_time);

                auto creator = [s, tmpdir_path, version] { return sstables::make_sstable(s, tmpdir_path, 56, version, big); };
                auto info = sstables::compact_sstables(sstables::compaction_descriptor({sst1, sst2}), *cf,
                                                       creator).get0();
                BOOST_REQUIRE(info.new_sstables.size() == 1);
                BOOST_REQUIRE(((now + gc_clock::duration(100)).time_since_epoch().count()) ==
                              info.new_sstables.front()->get_stats_metadata().max_local_deletion_time);
            }
        });
    });
}

static stats_metadata build_stats(int64_t min_timestamp, int64_t max_timestamp, int32_t max_local_deletion_time) {
    stats_metadata stats = {};
    stats.min_timestamp = min_timestamp;
    stats.max_timestamp = max_timestamp;
    stats.max_local_deletion_time = max_local_deletion_time;
    return stats;
}

SEASTAR_TEST_CASE(get_fully_expired_sstables_test) {
    auto key_and_token_pair = token_generation_for_current_shard(4);
    auto min_key = key_and_token_pair[0].first;
    auto max_key = key_and_token_pair[key_and_token_pair.size()-1].first;

    auto t0 = gc_clock::from_time_t(1).time_since_epoch().count();
    auto t1 = gc_clock::from_time_t(10).time_since_epoch().count();
    auto t2 = gc_clock::from_time_t(15).time_since_epoch().count();
    auto t3 = gc_clock::from_time_t(20).time_since_epoch().count();
    auto t4 = gc_clock::from_time_t(30).time_since_epoch().count();

    {
        column_family_for_tests cf;
        auto sst1 = add_sstable_for_overlapping_test(cf, /*gen*/1, min_key, key_and_token_pair[1].first, build_stats(t0, t1, t1));
        auto sst2 = add_sstable_for_overlapping_test(cf, /*gen*/2, min_key, key_and_token_pair[2].first, build_stats(t0, t1, std::numeric_limits<int32_t>::max()));
        auto sst3 = add_sstable_for_overlapping_test(cf, /*gen*/3, min_key, max_key, build_stats(t3, t4, std::numeric_limits<int32_t>::max()));
        std::vector<sstables::shared_sstable> compacting = { sst1, sst2 };
        auto expired = get_fully_expired_sstables(*cf, compacting, /*gc before*/gc_clock::from_time_t(15));
        BOOST_REQUIRE(expired.size() == 0);
    }

    {
        column_family_for_tests cf;

        auto sst1 = add_sstable_for_overlapping_test(cf, /*gen*/1, min_key, key_and_token_pair[1].first, build_stats(t0, t1, t1));
        auto sst2 = add_sstable_for_overlapping_test(cf, /*gen*/2, min_key, key_and_token_pair[2].first, build_stats(t2, t3, std::numeric_limits<int32_t>::max()));
        auto sst3 = add_sstable_for_overlapping_test(cf, /*gen*/3, min_key, max_key, build_stats(t3, t4, std::numeric_limits<int32_t>::max()));
        std::vector<sstables::shared_sstable> compacting = { sst1, sst2 };
        auto expired = get_fully_expired_sstables(*cf, compacting, /*gc before*/gc_clock::from_time_t(25));
        BOOST_REQUIRE(expired.size() == 1);
        auto expired_sst = *expired.begin();
        BOOST_REQUIRE(expired_sst->generation() == 1);
    }

    return make_ready_future<>();
}

SEASTAR_TEST_CASE(compaction_with_fully_expired_table) {
    return seastar::async([] {
        storage_service_for_tests ssft;
        auto builder = schema_builder("la", "cf")
            .with_column("pk", utf8_type, column_kind::partition_key)
            .with_column("ck1", utf8_type, column_kind::clustering_key)
            .with_column("r1", int32_type);
        builder.set_gc_grace_seconds(0);
        auto s = builder.build();

        auto tmp = make_lw_shared<tmpdir>();
        auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
        auto c_key = clustering_key_prefix::from_exploded(*s, {to_bytes("c1")});
        auto sst_gen = [s, tmp, gen = make_lw_shared<unsigned>(1)] () mutable {
            return make_sstable(s, tmp->path, (*gen)++, la, big);
        };

        auto mt = make_lw_shared<memtable>(s);
        mutation m(s, key);
        tombstone tomb(api::new_timestamp(), gc_clock::now() - std::chrono::seconds(3600));
        m.partition().apply_delete(*s, c_key, tomb);
        mt->apply(std::move(m));
        auto sst = sst_gen();
        write_memtable_to_sstable_for_test(*mt, sst).get();
        sst = reusable_sst(s, tmp->path, 1).get0();

        column_family_for_tests cf;

        auto ssts = std::vector<shared_sstable>{ sst };
        auto expired = get_fully_expired_sstables(*cf, ssts, gc_clock::now());
        BOOST_REQUIRE(expired.size() == 1);
        auto expired_sst = *expired.begin();
        BOOST_REQUIRE(expired_sst->generation() == 1);

        // check that sstable will auto correct potentially bad max_deletion_time and thus sstable will not be fully expired
        sstables::test(sst).rewrite_toc_without_scylla_component();
        sst = reusable_sst(s, tmp->path, 1).get0();
        ssts = std::vector<shared_sstable>{ sst };
        expired = get_fully_expired_sstables(*cf, ssts, gc_clock::now());
        BOOST_REQUIRE(expired.size() == 0);

        auto ret = sstables::compact_sstables(sstables::compaction_descriptor(ssts), *cf, sst_gen).get0();
        BOOST_REQUIRE(ret.start_size == sst->bytes_on_disk());
        BOOST_REQUIRE(ret.total_keys_written == 0);
        BOOST_REQUIRE(ret.new_sstables.empty());
    });
}

SEASTAR_TEST_CASE(basic_date_tiered_strategy_test) {
    schema_builder builder(make_lw_shared(schema({}, some_keyspace, some_column_family,
        {{"p1", utf8_type}}, {}, {}, {}, utf8_type)));
    builder.set_min_compaction_threshold(4);
    auto s = builder.build(schema_builder::compact_storage::no);

    column_family_for_tests cf(s);

    std::vector<sstables::shared_sstable> candidates;
    int min_threshold = cf->schema()->min_compaction_threshold();
    auto now = db_clock::now();
    auto past_hour = now - std::chrono::seconds(3600);
    int64_t timestamp_for_now = now.time_since_epoch().count() * 1000;
    int64_t timestamp_for_past_hour = past_hour.time_since_epoch().count() * 1000;

    for (auto i = 1; i <= min_threshold; i++) {
        auto sst = add_sstable_for_overlapping_test(cf, /*gen*/i, "a", "a",
            build_stats(timestamp_for_now, timestamp_for_now, std::numeric_limits<int32_t>::max()));
        candidates.push_back(sst);
    }
    // add sstable that belong to a different time tier.
    auto sst = add_sstable_for_overlapping_test(cf, /*gen*/min_threshold + 1, "a", "a",
        build_stats(timestamp_for_past_hour, timestamp_for_past_hour, std::numeric_limits<int32_t>::max()));
    candidates.push_back(sst);

    auto gc_before = gc_clock::now() - cf->schema()->gc_grace_seconds();
    std::map<sstring, sstring> options;
    date_tiered_manifest manifest(options);
    auto sstables = manifest.get_next_sstables(*cf, candidates, gc_before);
    BOOST_REQUIRE(sstables.size() == 4);
    for (auto& sst : sstables) {
        BOOST_REQUIRE(sst->generation() != (min_threshold + 1));
    }

    return make_ready_future<>();
}

SEASTAR_TEST_CASE(date_tiered_strategy_test_2) {
    schema_builder builder(make_lw_shared(schema({}, some_keyspace, some_column_family,
        {{"p1", utf8_type}}, {}, {}, {}, utf8_type)));
    builder.set_min_compaction_threshold(4);
    auto s = builder.build(schema_builder::compact_storage::no);

    column_family_for_tests cf(s);

    // deterministic timestamp for Fri, 01 Jan 2016 00:00:00 GMT.
    auto tp = db_clock::from_time_t(1451606400);
    int64_t timestamp = tp.time_since_epoch().count() * 1000; // in microseconds.

    std::vector<sstables::shared_sstable> candidates;
    int min_threshold = cf->schema()->min_compaction_threshold();

    // add sstables that belong to same time window until min threshold is satisfied.
    for (auto i = 1; i <= min_threshold; i++) {
        auto sst = add_sstable_for_overlapping_test(cf, /*gen*/i, "a", "a",
            build_stats(timestamp, timestamp, std::numeric_limits<int32_t>::max()));
        candidates.push_back(sst);
    }
    // belongs to the time window
    auto tp2 = tp + std::chrono::seconds(1800);
    timestamp = tp2.time_since_epoch().count() * 1000;
    auto sst = add_sstable_for_overlapping_test(cf, /*gen*/min_threshold + 1, "a", "a",
        build_stats(timestamp, timestamp, std::numeric_limits<int32_t>::max()));
    candidates.push_back(sst);

    // doesn't belong to the time window above
    auto tp3 = tp + std::chrono::seconds(4000);
    timestamp = tp3.time_since_epoch().count() * 1000;
    auto sst2 = add_sstable_for_overlapping_test(cf, /*gen*/min_threshold + 2, "a", "a",
        build_stats(timestamp, timestamp, std::numeric_limits<int32_t>::max()));
    candidates.push_back(sst2);

    std::map<sstring, sstring> options;
    // Use a 1-hour time window.
    options.emplace(sstring("base_time_seconds"), sstring("3600"));

    date_tiered_manifest manifest(options);
    auto gc_before = gc_clock::time_point(std::chrono::seconds(0)); // disable gc before.
    auto sstables = manifest.get_next_sstables(*cf, candidates, gc_before);
    std::unordered_set<int64_t> gens;
    for (auto sst : sstables) {
        gens.insert(sst->generation());
    }
    BOOST_REQUIRE(sstables.size() == size_t(min_threshold + 1));
    BOOST_REQUIRE(gens.count(min_threshold + 1));
    BOOST_REQUIRE(!gens.count(min_threshold + 2));

    return make_ready_future<>();
}

SEASTAR_TEST_CASE(time_window_strategy_time_window_tests) {
    using namespace std::chrono;

    api::timestamp_type tstamp1 = duration_cast<microseconds>(milliseconds(1451001601000L)).count(); // 2015-12-25 @ 00:00:01, in milliseconds
    api::timestamp_type tstamp2 = duration_cast<microseconds>(milliseconds(1451088001000L)).count(); // 2015-12-26 @ 00:00:01, in milliseconds
    api::timestamp_type low_hour = duration_cast<microseconds>(milliseconds(1451001600000L)).count(); // 2015-12-25 @ 00:00:00, in milliseconds


    // A 1 hour window should round down to the beginning of the hour
    BOOST_REQUIRE(time_window_compaction_strategy::get_window_lower_bound(duration_cast<seconds>(hours(1)), tstamp1) == low_hour);

    // A 1 minute window should round down to the beginning of the hour
    BOOST_REQUIRE(time_window_compaction_strategy::get_window_lower_bound(duration_cast<seconds>(minutes(1)), tstamp1) == low_hour);

    // A 1 day window should round down to the beginning of the hour
    BOOST_REQUIRE(time_window_compaction_strategy::get_window_lower_bound(duration_cast<seconds>(hours(24)), tstamp1) == low_hour);

    // The 2 day window of 2015-12-25 + 2015-12-26 should round down to the beginning of 2015-12-25
    BOOST_REQUIRE(time_window_compaction_strategy::get_window_lower_bound(duration_cast<seconds>(hours(24*2)), tstamp2) == low_hour);

    return make_ready_future<>();
}

SEASTAR_TEST_CASE(time_window_strategy_ts_resolution_check) {
    auto ts = 1451001601000L; // 2015-12-25 @ 00:00:01, in milliseconds
    auto ts_in_ms = std::chrono::milliseconds(ts);
    auto ts_in_us = std::chrono::duration_cast<std::chrono::microseconds>(ts_in_ms);

    auto s = schema_builder("tests", "time_window_strategy")
            .with_column("id", utf8_type, column_kind::partition_key)
            .with_column("value", int32_type).build();

    {
        std::map<sstring, sstring> opts = { { time_window_compaction_strategy_options::TIMESTAMP_RESOLUTION_KEY, "MILLISECONDS" }, };
        time_window_compaction_strategy_options options(opts);

        auto sst = make_sstable(s, "", 1, la, big);
        sstables::test(sst).set_values("key1", "key1", build_stats(ts_in_ms.count(), ts_in_ms.count(), std::numeric_limits<int32_t>::max()));

        auto ret = time_window_compaction_strategy::get_buckets({ sst }, options);
        auto expected = time_window_compaction_strategy::get_window_lower_bound(options.get_sstable_window_size(), ts_in_us.count());

        BOOST_REQUIRE(ret.second == expected);
    }

    {
        std::map<sstring, sstring> opts = { { time_window_compaction_strategy_options::TIMESTAMP_RESOLUTION_KEY, "MICROSECONDS" }, };
        time_window_compaction_strategy_options options(opts);

        auto sst = make_sstable(s, "", 1, la, big);
        sstables::test(sst).set_values("key1", "key1", build_stats(ts_in_us.count(), ts_in_us.count(), std::numeric_limits<int32_t>::max()));

        auto ret = time_window_compaction_strategy::get_buckets({ sst }, options);
        auto expected = time_window_compaction_strategy::get_window_lower_bound(options.get_sstable_window_size(), ts_in_us.count());

        BOOST_REQUIRE(ret.second == expected);
    }
    return make_ready_future<>();
}

SEASTAR_TEST_CASE(time_window_strategy_correctness_test) {
    using namespace std::chrono;

    return seastar::async([] {
        storage_service_for_tests ssft;
        auto s = schema_builder("tests", "time_window_strategy")
                .with_column("id", utf8_type, column_kind::partition_key)
                .with_column("value", int32_type).build();

        auto tmp = make_lw_shared<tmpdir>();
        auto sst_gen = [s, tmp, gen = make_lw_shared<unsigned>(1)] () mutable {
            return make_sstable(s, tmp->path, (*gen)++, la, big);
        };

        auto make_insert = [&] (partition_key key, api::timestamp_type t) {
            mutation m(s, key);
            m.set_clustered_cell(clustering_key::make_empty(), bytes("value"), data_value(int32_t(1)), t);
            return m;
        };

        api::timestamp_type tstamp = api::timestamp_clock::now().time_since_epoch().count();
        api::timestamp_type tstamp2 = tstamp - duration_cast<microseconds>(seconds(2L * 3600L)).count();

        std::vector<shared_sstable> sstables;

        // create 5 sstables
        for (api::timestamp_type t = 0; t < 3; t++) {
            auto key = partition_key::from_exploded(*s, {to_bytes("key" + to_sstring(t))});
            auto mut = make_insert(std::move(key), t);
            sstables.push_back(make_sstable_containing(sst_gen, {std::move(mut)}));
        }
        // Decrement the timestamp to simulate a timestamp in the past hour
        for (api::timestamp_type t = 3; t < 5; t++) {
            // And add progressively more cells into each sstable
            auto key = partition_key::from_exploded(*s, {to_bytes("key" + to_sstring(t))});
            auto mut = make_insert(std::move(key), t);
            sstables.push_back(make_sstable_containing(sst_gen, {std::move(mut)}));
        }

        std::map<api::timestamp_type, std::vector<shared_sstable>> buckets;

        // We'll put 3 sstables into the newest bucket
        for (api::timestamp_type i = 0; i < 3; i++) {
            auto bound = time_window_compaction_strategy::get_window_lower_bound(duration_cast<seconds>(hours(1)), tstamp);
            buckets[bound].push_back(sstables[i]);
        }
        sstables::size_tiered_compaction_strategy_options stcs_options;
        auto now = api::timestamp_clock::now().time_since_epoch().count();
        auto new_bucket = time_window_compaction_strategy::newest_bucket(buckets, 4, 32, duration_cast<seconds>(hours(1)),
            time_window_compaction_strategy::get_window_lower_bound(duration_cast<seconds>(hours(1)), now), stcs_options);
        // incoming bucket should not be accepted when it has below the min threshold SSTables
        BOOST_REQUIRE(new_bucket.empty());

        now = api::timestamp_clock::now().time_since_epoch().count();
        new_bucket = time_window_compaction_strategy::newest_bucket(buckets, 2, 32, duration_cast<seconds>(hours(1)),
            time_window_compaction_strategy::get_window_lower_bound(duration_cast<seconds>(hours(1)), now), stcs_options);
        // incoming bucket should be accepted when it is larger than the min threshold SSTables
        BOOST_REQUIRE(!new_bucket.empty());

        // And 2 into the second bucket (1 hour back)
        for (api::timestamp_type i = 3; i < 5; i++) {
            auto bound = time_window_compaction_strategy::get_window_lower_bound(duration_cast<seconds>(hours(1)), tstamp2);
            buckets[bound].push_back(sstables[i]);
        }

        // "an sstable with a single value should have equal min/max timestamps"
        for (auto& sst : sstables) {
            BOOST_REQUIRE(sst->get_stats_metadata().min_timestamp == sst->get_stats_metadata().max_timestamp);
        }

        // Test trim
        auto num_sstables = 40;
        for (int r = 5; r < num_sstables; r++) {
            auto key = partition_key::from_exploded(*s, {to_bytes("key" + to_sstring(r))});
            std::vector<mutation> mutations;
            for (int i = 0 ; i < r ; i++) {
                mutations.push_back(make_insert(key, tstamp + r));
            }
            sstables.push_back(make_sstable_containing(sst_gen, std::move(mutations)));
        }

        // Reset the buckets, overfill it now
        for (int i = 0 ; i < 40; i++) {
            auto bound = time_window_compaction_strategy::get_window_lower_bound(duration_cast<seconds>(hours(1)),
                sstables[i]->get_stats_metadata().max_timestamp);
            buckets[bound].push_back(sstables[i]);
        }

        now = api::timestamp_clock::now().time_since_epoch().count();
        new_bucket = time_window_compaction_strategy::newest_bucket(buckets, 4, 32, duration_cast<seconds>(hours(1)),
            time_window_compaction_strategy::get_window_lower_bound(duration_cast<seconds>(hours(1)), now), stcs_options);
        // new bucket should be trimmed to max threshold of 32
        BOOST_REQUIRE(new_bucket.size() == size_t(32));
    });
}

SEASTAR_TEST_CASE(test_promoted_index_read) {
    // create table promoted_index_read (
    //        pk int,
    //        ck1 int,
    //        ck2 int,
    //        v int,
    //        primary key (pk, ck1, ck2)
    // );
    //
    // column_index_size_in_kb: 0
    //
    // delete from promoted_index_read where pk = 0 and ck1 = 0;
    // insert into promoted_index_read (pk, ck1, ck2, v) values (0, 0, 0, 0);
    // insert into promoted_index_read (pk, ck1, ck2, v) values (0, 0, 1, 1);
    //
    // SSTable:
    // [
    // {"key": "0",
    //  "cells": [["0:_","0:!",1468923292708929,"t",1468923292],
    //            ["0:_","0:!",1468923292708929,"t",1468923292],
    //            ["0:0:","",1468923308379491],
    //            ["0:_","0:!",1468923292708929,"t",1468923292],
    //            ["0:0:v","0",1468923308379491],
    //            ["0:_","0:!",1468923292708929,"t",1468923292],
    //            ["0:1:","",1468923311744298],
    //            ["0:_","0:!",1468923292708929,"t",1468923292],
    //            ["0:1:v","1",1468923311744298]]}
    // ]

    return seastar::async([] {
      for (const auto version : all_sstable_versions) {
        auto s = schema_builder("ks", "promoted_index_read")
                .with_column("pk", int32_type, column_kind::partition_key)
                .with_column("ck1", int32_type, column_kind::clustering_key)
                .with_column("ck2", int32_type, column_kind::clustering_key)
                .with_column("v", int32_type)
                .build();

        auto sst = make_sstable(s, get_test_dir("promoted_index_read", s), 1, version, big);
        sst->load().get0();

        auto pkey = partition_key::from_exploded(*s, { int32_type->decompose(0) });
        auto dkey = dht::global_partitioner().decorate_key(*s, std::move(pkey));

        auto rd = make_normalizing_sstable_reader(sst, s);
        using kind = mutation_fragment::kind;
        assert_that(std::move(rd))
                .produces_partition_start(dkey)
                .produces(kind::range_tombstone, { 0 })
                .produces(kind::clustering_row, { 0, 0 })
                .produces(kind::range_tombstone, { 0, 0 })
                .produces(kind::clustering_row, { 0, 1 })
                .produces(kind::range_tombstone, { 0, 1 })
                .produces_partition_end()
                .produces_end_of_stream();
      }
    });
}

static void check_min_max_column_names(const sstable_ptr& sst, std::vector<bytes> min_components, std::vector<bytes> max_components) {
    const auto& st = sst->get_stats_metadata();
    BOOST_REQUIRE(st.min_column_names.elements.size() == min_components.size());
    BOOST_REQUIRE(st.min_column_names.elements.size() == st.max_column_names.elements.size());
    for (auto i = 0U; i < st.min_column_names.elements.size(); i++) {
        BOOST_REQUIRE(min_components[i] == st.min_column_names.elements[i].value);
        BOOST_REQUIRE(max_components[i] == st.max_column_names.elements[i].value);
    }
}

static void test_min_max_clustering_key(schema_ptr s, std::vector<bytes> exploded_pk, std::vector<std::vector<bytes>> exploded_cks,
        std::vector<bytes> min_components, std::vector<bytes> max_components, sstable_version_types version, bool remove = false) {
    auto mt = make_lw_shared<memtable>(s);
    auto insert_data = [&mt, &s] (std::vector<bytes>& exploded_pk, std::vector<bytes>&& exploded_ck) {
        const column_definition& r1_col = *s->get_column_definition("r1");
        auto key = partition_key::from_exploded(*s, exploded_pk);
        auto c_key = clustering_key::make_empty();
        if (!exploded_ck.empty()) {
            c_key = clustering_key::from_exploded(*s, exploded_ck);
        }
        mutation m(s, key);
        m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type, int32_type->decompose(1)));
        mt->apply(std::move(m));
    };
    auto remove_data = [&mt, &s] (std::vector<bytes>& exploded_pk, std::vector<bytes>&& exploded_ck) {
        auto key = partition_key::from_exploded(*s, exploded_pk);
        auto c_key = clustering_key::from_exploded(*s, exploded_ck);
        mutation m(s, key);
        tombstone tomb(api::new_timestamp(), gc_clock::now());
        m.partition().apply_delete(*s, c_key, tomb);
        mt->apply(std::move(m));
    };

    if (exploded_cks.empty()) {
        insert_data(exploded_pk, {});
    } else {
        for (auto& exploded_ck : exploded_cks) {
            if (remove) {
                remove_data(exploded_pk, std::move(exploded_ck));
            } else {
                insert_data(exploded_pk, std::move(exploded_ck));
            }
        }
    }
    auto tmp = make_lw_shared<tmpdir>();
    auto sst = make_sstable(s, tmp->path, 1, version, big);
    write_memtable_to_sstable_for_test(*mt, sst).get();
    sst = reusable_sst(s, tmp->path, 1, version).get0();
    check_min_max_column_names(sst, std::move(min_components), std::move(max_components));
}

SEASTAR_TEST_CASE(min_max_clustering_key_test) {
    return seastar::async([] {
        storage_service_for_tests ssft;
        for (auto version : all_sstable_versions) {
            {
                auto s = schema_builder("ks", "cf")
                        .with_column("pk", utf8_type, column_kind::partition_key)
                        .with_column("ck1", utf8_type, column_kind::clustering_key)
                        .with_column("ck2", utf8_type, column_kind::clustering_key)
                        .with_column("r1", int32_type)
                        .build();
                test_min_max_clustering_key(s, {"key1"}, {{"a", "b"},
                                                          {"a", "c"}}, {"a", "b"}, {"a", "c"}, version);
            }
            {
                auto s = schema_builder("ks", "cf")
                        .with(schema_builder::compact_storage::yes)
                        .with_column("pk", utf8_type, column_kind::partition_key)
                        .with_column("ck1", utf8_type, column_kind::clustering_key)
                        .with_column("ck2", utf8_type, column_kind::clustering_key)
                        .with_column("r1", int32_type)
                        .build();
                test_min_max_clustering_key(s, {"key1"}, {{"a", "b"},
                                                          {"a", "c"}}, {"a", "b"}, {"a", "c"}, version);
            }
            {
                auto s = schema_builder("ks", "cf")
                        .with_column("pk", utf8_type, column_kind::partition_key)
                        .with_column("ck1", utf8_type, column_kind::clustering_key)
                        .with_column("r1", int32_type)
                        .build();
                test_min_max_clustering_key(s, {"key1"}, {{"a"},
                                                          {"z"}}, {"a"}, {"z"}, version);
            }
            {
                auto s = schema_builder("ks", "cf")
                        .with_column("pk", utf8_type, column_kind::partition_key)
                        .with_column("ck1", utf8_type, column_kind::clustering_key)
                        .with_column("r1", int32_type)
                        .build();
                test_min_max_clustering_key(s, {"key1"}, {{"a"},
                                                          {"z"}}, {"a"}, {"z"}, version, true);
            }
            {
                auto s = schema_builder("ks", "cf")
                        .with_column("pk", utf8_type, column_kind::partition_key)
                        .with_column("r1", int32_type)
                        .build();
                test_min_max_clustering_key(s, {"key1"}, {}, {}, {}, version);
            }
        }
    });
}

SEASTAR_TEST_CASE(min_max_clustering_key_test_2) {
    return seastar::async([] {
        storage_service_for_tests ssft;
        for (const auto version : all_sstable_versions) {
            auto s = schema_builder("ks", "cf")
                      .with_column("pk", utf8_type, column_kind::partition_key)
                      .with_column("ck1", utf8_type, column_kind::clustering_key)
                      .with_column("r1", int32_type)
                      .build();
            column_family_for_tests cf(s);
            auto tmp = make_lw_shared<tmpdir>();
            auto mt = make_lw_shared<memtable>(s);
            const column_definition &r1_col = *s->get_column_definition("r1");

            for (auto j = 0; j < 8; j++) {
                auto key = partition_key::from_exploded(*s, {to_bytes("key" + to_sstring(j))});
                mutation m(s, key);
                for (auto i = 100; i < 150; i++) {
                    auto c_key = clustering_key::from_exploded(*s, {to_bytes(to_sstring(j) + "ck" + to_sstring(i))});
                    m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type, int32_type->decompose(1)));
                }
                mt->apply(std::move(m));
            }
            auto sst = make_sstable(s, tmp->path, 1, version, big);
            write_memtable_to_sstable_for_test(*mt, sst).get();
            sst = reusable_sst(s, tmp->path, 1, version).get0();
            check_min_max_column_names(sst, {"0ck100"}, {"7ck149"});

            mt = make_lw_shared<memtable>(s);
            auto key = partition_key::from_exploded(*s, {to_bytes("key9")});
            mutation m(s, key);
            for (auto i = 101; i < 299; i++) {
                auto c_key = clustering_key::from_exploded(*s, {to_bytes(to_sstring(9) + "ck" + to_sstring(i))});
                m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type, int32_type->decompose(1)));
            }
            mt->apply(std::move(m));
            auto sst2 = make_sstable(s, tmp->path, 2, version, big);
            write_memtable_to_sstable_for_test(*mt, sst2).get();
            sst2 = reusable_sst(s, tmp->path, 2, version).get0();
            check_min_max_column_names(sst2, {"9ck101"}, {"9ck298"});

            auto creator = [s, tmp, version] { return sstables::make_sstable(s, tmp->path, 3, version, big); };
            auto info = sstables::compact_sstables(sstables::compaction_descriptor({sst, sst2}), *cf, creator).get0();
            BOOST_REQUIRE(info.new_sstables.size() == 1);
            check_min_max_column_names(info.new_sstables.front(), {"0ck100"}, {"9ck298"});
        }
    });
}

SEASTAR_TEST_CASE(sstable_tombstone_metadata_check) {
    return seastar::async([] {
        storage_service_for_tests ssft;
        for (const auto version : all_sstable_versions) {
            auto s = schema_builder("ks", "cf")
                    .with_column("pk", utf8_type, column_kind::partition_key)
                    .with_column("ck1", utf8_type, column_kind::clustering_key)
                    .with_column("r1", int32_type)
                    .build();
            auto tmp = make_lw_shared<tmpdir>();
            auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
            auto c_key = clustering_key_prefix::from_exploded(*s, {to_bytes("c1")});
            const column_definition& r1_col = *s->get_column_definition("r1");

            {
                auto mt = make_lw_shared<memtable>(s);
                mutation m(s, key);
                tombstone tomb(api::new_timestamp(), gc_clock::now());
                m.partition().apply_delete(*s, c_key, tomb);
                mt->apply(std::move(m));
                auto sst = make_sstable(s, tmp->path, 1, version, big);
                write_memtable_to_sstable_for_test(*mt, sst).get();
                sst = reusable_sst(s, tmp->path, 1, version).get0();
                sstables::sstlog.warn("Version {}", (int)version);
                BOOST_REQUIRE(sst->get_stats_metadata().estimated_tombstone_drop_time.bin.size());
            }

            {
                auto mt = make_lw_shared<memtable>(s);
                mutation m(s, key);
                m.set_clustered_cell(c_key, r1_col, make_dead_atomic_cell(3600));
                mt->apply(std::move(m));
                auto sst = make_sstable(s, tmp->path, 2, version, big);
                write_memtable_to_sstable_for_test(*mt, sst).get();
                sst = reusable_sst(s, tmp->path, 2, version).get0();
                BOOST_REQUIRE(sst->get_stats_metadata().estimated_tombstone_drop_time.bin.size());
            }

            {
                auto mt = make_lw_shared<memtable>(s);
                mutation m(s, key);
                m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type, int32_type->decompose(1)));
                mt->apply(std::move(m));
                auto sst = make_sstable(s, tmp->path, 3, version, big);
                write_memtable_to_sstable_for_test(*mt, sst).get();
                sst = reusable_sst(s, tmp->path, 3, version).get0();
                BOOST_REQUIRE(!sst->get_stats_metadata().estimated_tombstone_drop_time.bin.size());
            }

            {
                auto mt = make_lw_shared<memtable>(s);

                mutation m(s, key);
                tombstone tomb(api::new_timestamp(), gc_clock::now());
                m.partition().apply_delete(*s, c_key, tomb);
                mt->apply(std::move(m));

                auto key2 = partition_key::from_exploded(*s, {to_bytes("key2")});
                mutation m2(s, key2);
                m2.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type, int32_type->decompose(1)));
                mt->apply(std::move(m2));

                auto sst = make_sstable(s, tmp->path, 4, version, big);
                write_memtable_to_sstable_for_test(*mt, sst).get();
                sst = reusable_sst(s, tmp->path, 4, version).get0();
                BOOST_REQUIRE(sst->get_stats_metadata().estimated_tombstone_drop_time.bin.size());
            }

            {
                auto mt = make_lw_shared<memtable>(s);
                mutation m(s, key);
                tombstone tomb(api::new_timestamp(), gc_clock::now());
                m.partition().apply(tomb);
                mt->apply(std::move(m));
                auto sst = make_sstable(s, tmp->path, 5, version, big);
                write_memtable_to_sstable_for_test(*mt, sst).get();
                sst = reusable_sst(s, tmp->path, 5, version).get0();
                BOOST_REQUIRE(sst->get_stats_metadata().estimated_tombstone_drop_time.bin.size());
            }

            {
                auto mt = make_lw_shared<memtable>(s);
                mutation m(s, key);
                tombstone tomb(api::new_timestamp(), gc_clock::now());
                range_tombstone rt(clustering_key_prefix::from_single_value(*s, bytes(
                "a")), clustering_key_prefix::from_single_value(*s, bytes("a")), tomb);
                m.partition().apply_delete(*s, std::move(rt));
                mt->apply(std::move(m));
                auto sst = make_sstable(s, tmp->path, 6, version, big);
                write_memtable_to_sstable_for_test(*mt, sst).get();
                sst = reusable_sst(s, tmp->path, 6, version).get0();
                BOOST_REQUIRE(sst->get_stats_metadata().estimated_tombstone_drop_time.bin.size());
            }
        }
    });
}

SEASTAR_TEST_CASE(test_partition_skipping) {
    return seastar::async([] {
      for (const auto version : all_sstable_versions) {
        auto s = schema_builder("ks", "test_skipping_partitions")
                .with_column("pk", int32_type, column_kind::partition_key)
                .with_column("v", int32_type)
                .build();

        auto sst = make_sstable(s, get_test_dir("partition_skipping",s), 1, version, big);
        sst->load().get0();

        std::vector<dht::decorated_key> keys;
        for (int i = 0; i < 10; i++) {
            auto pk = partition_key::from_single_value(*s, int32_type->decompose(i));
            keys.emplace_back(dht::global_partitioner().decorate_key(*s, std::move(pk)));
        }
        dht::decorated_key::less_comparator cmp(s);
        std::sort(keys.begin(), keys.end(), cmp);

        assert_that(sstable_reader(sst, s)).produces(keys);

        auto pr = dht::partition_range::make(dht::ring_position(keys[0]), dht::ring_position(keys[1]));
        assert_that(sstable_reader(sst, s, pr))
            .produces(keys[0])
            .produces(keys[1])
            .produces_end_of_stream()
            .fast_forward_to(dht::partition_range::make_starting_with(dht::ring_position(keys[8])))
            .produces(keys[8])
            .produces(keys[9])
            .produces_end_of_stream();

        pr = dht::partition_range::make(dht::ring_position(keys[1]), dht::ring_position(keys[1]));
        assert_that(sstable_reader(sst, s, pr))
            .produces(keys[1])
            .produces_end_of_stream()
            .fast_forward_to(dht::partition_range::make(dht::ring_position(keys[3]), dht::ring_position(keys[4])))
            .produces(keys[3])
            .produces(keys[4])
            .produces_end_of_stream()
            .fast_forward_to(dht::partition_range::make({ dht::ring_position(keys[4]), false }, dht::ring_position(keys[5])))
            .produces(keys[5])
            .produces_end_of_stream()
            .fast_forward_to(dht::partition_range::make(dht::ring_position(keys[6]), dht::ring_position(keys[6])))
            .produces(keys[6])
            .produces_end_of_stream()
            .fast_forward_to(dht::partition_range::make(dht::ring_position(keys[7]), dht::ring_position(keys[8])))
            .produces(keys[7])
            .fast_forward_to(dht::partition_range::make(dht::ring_position(keys[9]), dht::ring_position(keys[9])))
            .produces(keys[9])
            .produces_end_of_stream();

        pr = dht::partition_range::make({ dht::ring_position(keys[0]), false }, { dht::ring_position(keys[1]), false});
        assert_that(sstable_reader(sst, s, pr))
            .produces_end_of_stream()
            .fast_forward_to(dht::partition_range::make(dht::ring_position(keys[6]), dht::ring_position(keys[6])))
            .produces(keys[6])
            .produces_end_of_stream()
            .fast_forward_to(dht::partition_range::make({ dht::ring_position(keys[8]), false }, { dht::ring_position(keys[9]), false }))
            .produces_end_of_stream();
      }
    });
}

// Must be run in a seastar thread
static
shared_sstable make_sstable_easy(sstring path, flat_mutation_reader rd, sstable_writer_config cfg, const sstables::sstable::version_types version) {
    auto s = rd.schema();
    auto sst = make_sstable(s, path, 1, version, big);
    sst->write_components(std::move(rd), 1, s, cfg).get();
    sst->load().get();
    return sst;
}

SEASTAR_TEST_CASE(test_repeated_tombstone_skipping) {
    return seastar::async([] {
      for (const auto version : all_sstable_versions) {
        storage_service_for_tests ssft;
        simple_schema table;

        std::vector<mutation_fragment> fragments;

        uint32_t count = 1000; // large enough to cross index block several times

        auto rt = table.make_range_tombstone(query::clustering_range::make(
            query::clustering_range::bound(table.make_ckey(0), true),
            query::clustering_range::bound(table.make_ckey(count - 1), true)
        ));

        fragments.push_back(mutation_fragment(range_tombstone(rt)));

        std::vector<range_tombstone> rts;

        uint32_t seq = 1;
        while (seq < count) {
            rts.push_back(table.make_range_tombstone(query::clustering_range::make(
                query::clustering_range::bound(table.make_ckey(seq), true),
                query::clustering_range::bound(table.make_ckey(seq + 1), false)
            )));
            fragments.push_back(range_tombstone(rts.back()));
            ++seq;

            fragments.push_back(table.make_row(table.make_ckey(seq), make_random_string(1)));
            ++seq;
        }

        tmpdir dir;
        sstable_writer_config cfg;
        cfg.promoted_index_block_size = 100;
        cfg.large_partition_handler = &nop_lp_handler;
        auto mut = mutation(table.schema(), table.make_pkey("key"));
        for (auto&& mf : fragments) {
            mut.apply(mf);
        }
        auto sst = make_sstable_easy(dir.path,  flat_mutation_reader_from_mutations({ std::move(mut) }), cfg, version);
        auto ms = as_mutation_source(sst);

        for (uint32_t i = 3; i < seq; i++) {
            auto ck1 = table.make_ckey(1);
            auto ck2 = table.make_ckey((1 + i) / 2);
            auto ck3 = table.make_ckey(i);
            BOOST_TEST_MESSAGE(sprint("checking %s %s", ck2, ck3));
            auto slice = partition_slice_builder(*table.schema())
                .with_range(query::clustering_range::make_singular(ck1))
                .with_range(query::clustering_range::make_singular(ck2))
                .with_range(query::clustering_range::make_singular(ck3))
                .build();
            flat_mutation_reader rd = ms.make_reader(table.schema(), query::full_partition_range, slice);
            assert_that(std::move(rd)).has_monotonic_positions();
        }
      }
    });
}

SEASTAR_TEST_CASE(test_skipping_using_index) {
    return seastar::async([] {
      for (const auto version : all_sstable_versions) {
        storage_service_for_tests ssft;
        simple_schema table;

        const unsigned rows_per_part = 10;
        const unsigned partition_count = 10;

        std::vector<dht::decorated_key> keys;
        for (unsigned i = 0; i < partition_count; ++i) {
            keys.push_back(table.make_pkey(i));
        }
        std::sort(keys.begin(), keys.end(), dht::decorated_key::less_comparator(table.schema()));

        std::vector<mutation> partitions;
        uint32_t row_id = 0;
        for (auto&& key : keys) {
            mutation m(table.schema(), key);
            for (unsigned j = 0; j < rows_per_part; ++j) {
                table.add_row(m, table.make_ckey(row_id++), make_random_string(1));
            }
            partitions.emplace_back(std::move(m));
        }

        std::sort(partitions.begin(), partitions.end(), mutation_decorated_key_less_comparator());

        tmpdir dir;
        sstable_writer_config cfg;
        cfg.promoted_index_block_size = 1; // So that every fragment is indexed
        cfg.large_partition_handler = &nop_lp_handler;
        auto sst = make_sstable_easy(dir.path, flat_mutation_reader_from_mutations(partitions), cfg, version);

        auto ms = as_mutation_source(sst);
        auto rd = ms.make_reader(table.schema(),
            query::full_partition_range,
            table.schema()->full_slice(),
            default_priority_class(),
            nullptr,
            streamed_mutation::forwarding::yes,
            mutation_reader::forwarding::yes);

        auto assertions = assert_that(std::move(rd));
        // Consume first partition completely so that index is stale
        {
            assertions
                .produces_partition_start(keys[0])
                .fast_forward_to(position_range::all_clustered_rows());
            for (auto i = 0u; i < rows_per_part; i++) {
                assertions.produces_row_with_key(table.make_ckey(i));
            }
            assertions.produces_end_of_stream();
        }

        {
            auto base = rows_per_part;
            assertions
                .next_partition()
                .produces_partition_start(keys[1])
                .fast_forward_to(position_range(
                    position_in_partition::for_key(table.make_ckey(base)),
                    position_in_partition::for_key(table.make_ckey(base + 3))))
                .produces_row_with_key(table.make_ckey(base))
                .produces_row_with_key(table.make_ckey(base + 1))
                .produces_row_with_key(table.make_ckey(base + 2))
                .fast_forward_to(position_range(
                    position_in_partition::for_key(table.make_ckey(base + 5)),
                    position_in_partition::for_key(table.make_ckey(base + 6))))
                .produces_row_with_key(table.make_ckey(base + 5))
                .produces_end_of_stream()
                .fast_forward_to(position_range(
                    position_in_partition::for_key(table.make_ckey(base + rows_per_part)), // Skip all rows in current partition
                    position_in_partition::after_all_clustered_rows()))
                .produces_end_of_stream();
        }

        // Consume few fragments then skip
        {
            auto base = rows_per_part * 2;
            assertions
                .next_partition()
                .produces_partition_start(keys[2])
                .fast_forward_to(position_range(
                    position_in_partition::for_key(table.make_ckey(base)),
                    position_in_partition::for_key(table.make_ckey(base + 3))))
                .produces_row_with_key(table.make_ckey(base))
                .produces_row_with_key(table.make_ckey(base + 1))
                .produces_row_with_key(table.make_ckey(base + 2))
                .fast_forward_to(position_range(
                    position_in_partition::for_key(table.make_ckey(base + rows_per_part - 1)), // last row
                    position_in_partition::after_all_clustered_rows()))
                .produces_row_with_key(table.make_ckey(base + rows_per_part - 1))
                .produces_end_of_stream();
        }

        // Consume nothing from the next partition
        {
            assertions
                .next_partition()
                .produces_partition_start(keys[3])
                .next_partition();
        }

        {
            auto base = rows_per_part * 4;
            assertions
                .next_partition()
                .produces_partition_start(keys[4])
                .fast_forward_to(position_range(
                    position_in_partition::for_key(table.make_ckey(base + rows_per_part - 1)), // last row
                    position_in_partition::after_all_clustered_rows()))
                .produces_row_with_key(table.make_ckey(base + rows_per_part - 1))
                .produces_end_of_stream();
        }
      }
    });
}

static void copy_directory(boost::filesystem::path src_dir, boost::filesystem::path dst_dir) {
    namespace fs = boost::filesystem;
    fs::create_directory(dst_dir);
    auto src_dir_components = std::distance(src_dir.begin(), src_dir.end());
    using rdi = fs::recursive_directory_iterator;
    // Boost 1.55.0 doesn't support range for on recursive_directory_iterator
    // (even though previous and later versions do support it)
    for (auto&& dirent = rdi{src_dir}; dirent != rdi(); ++dirent) {
        auto&& path = dirent->path();
        auto new_path = dst_dir;
        for (auto i = std::next(path.begin(), src_dir_components); i != path.end(); ++i) {
            new_path /= *i;
        }
        fs::copy(path, new_path);
    }
}

SEASTAR_TEST_CASE(test_unknown_component) {
    return seastar::async([] {
        auto tmp = make_lw_shared<tmpdir>();
        copy_directory("tests/sstables/unknown_component", std::string(tmp->path) + "/unknown_component");
        auto sstp = reusable_sst(uncompressed_schema(), tmp->path + "/unknown_component", 1).get0();
        sstp->create_links(tmp->path).get();
        // check that create_links() moved unknown component to new dir
        BOOST_REQUIRE(file_exists(tmp->path + "/la-1-big-UNKNOWN.txt").get0());

        sstp = reusable_sst(uncompressed_schema(), tmp->path, 1).get0();
        sstp->set_generation(2).get();
        BOOST_REQUIRE(!file_exists(tmp->path +  "/la-1-big-UNKNOWN.txt").get0());
        BOOST_REQUIRE(file_exists(tmp->path + "/la-2-big-UNKNOWN.txt").get0());

        sstables::delete_atomically({sstp}, nop_lp_handler).get();
        // assure unknown component is deleted
        BOOST_REQUIRE(!file_exists(tmp->path + "/la-2-big-UNKNOWN.txt").get0());
    });
}

SEASTAR_TEST_CASE(size_tiered_beyond_max_threshold_test) {
    column_family_for_tests cf;
    auto cs = sstables::make_compaction_strategy(sstables::compaction_strategy_type::size_tiered, cf.schema()->compaction_strategy_options());

    std::vector<sstables::shared_sstable> candidates;
    int max_threshold = cf->schema()->max_compaction_threshold();
    candidates.reserve(max_threshold+1);
    for (auto i = 0; i < (max_threshold+1); i++) { // (max_threshold+1) sstables of similar size
        auto sst = make_sstable(cf.schema(), "", i, la, big);
        sstables::test(sst).set_data_file_size(1);
        candidates.push_back(std::move(sst));
    }
    auto desc = cs.get_sstables_for_compaction(*cf, std::move(candidates));
    BOOST_REQUIRE(desc.sstables.size() == size_t(max_threshold));
    return make_ready_future<>();
}

SEASTAR_TEST_CASE(sstable_set_incremental_selector) {
    auto s = make_lw_shared(schema({}, some_keyspace, some_column_family,
        {{"p1", utf8_type}}, {}, {}, {}, utf8_type));
    auto cs = sstables::make_compaction_strategy(sstables::compaction_strategy_type::leveled, s->compaction_strategy_options());
    auto key_and_token_pair = token_generation_for_current_shard(8);
    auto decorated_keys = boost::copy_range<std::vector<dht::decorated_key>>(
            key_and_token_pair | boost::adaptors::transformed([&s] (const std::pair<sstring, dht::token>& key_and_token) {
                auto value = bytes(reinterpret_cast<const signed char*>(key_and_token.first.data()), key_and_token.first.size());
                auto pk = sstables::key::from_bytes(value).to_partition_key(*s);
                return dht::global_partitioner().decorate_key(*s, std::move(pk));
            }));

    auto check = [] (sstable_set::incremental_selector& selector, const dht::decorated_key& key, std::unordered_set<int64_t> expected_gens) {
        auto sstables = selector.select(key).sstables;
        BOOST_REQUIRE_EQUAL(sstables.size(), expected_gens.size());
        for (auto& sst : sstables) {
            BOOST_REQUIRE_EQUAL(expected_gens.count(sst->generation()), 1);
        }
    };

    {
        sstable_set set = cs.make_sstable_set(s);
        set.insert(sstable_for_overlapping_test(s, 1, key_and_token_pair[0].first, key_and_token_pair[1].first, 1));
        set.insert(sstable_for_overlapping_test(s, 2, key_and_token_pair[0].first, key_and_token_pair[1].first, 1));
        set.insert(sstable_for_overlapping_test(s, 3, key_and_token_pair[3].first, key_and_token_pair[4].first, 1));
        set.insert(sstable_for_overlapping_test(s, 4, key_and_token_pair[4].first, key_and_token_pair[4].first, 1));
        set.insert(sstable_for_overlapping_test(s, 5, key_and_token_pair[4].first, key_and_token_pair[5].first, 1));

        sstable_set::incremental_selector sel = set.make_incremental_selector();
        check(sel, decorated_keys[0], {1, 2});
        check(sel, decorated_keys[1], {1, 2});
        check(sel, decorated_keys[2], {});
        check(sel, decorated_keys[3], {3});
        check(sel, decorated_keys[4], {3, 4, 5});
        check(sel, decorated_keys[5], {5});
        check(sel, decorated_keys[6], {});
        check(sel, decorated_keys[7], {});
    }

    {
        sstable_set set = cs.make_sstable_set(s);
        set.insert(sstable_for_overlapping_test(s, 0, key_and_token_pair[0].first, key_and_token_pair[1].first, 0));
        set.insert(sstable_for_overlapping_test(s, 1, key_and_token_pair[0].first, key_and_token_pair[1].first, 1));
        set.insert(sstable_for_overlapping_test(s, 2, key_and_token_pair[0].first, key_and_token_pair[1].first, 1));
        set.insert(sstable_for_overlapping_test(s, 3, key_and_token_pair[3].first, key_and_token_pair[4].first, 1));
        set.insert(sstable_for_overlapping_test(s, 4, key_and_token_pair[4].first, key_and_token_pair[4].first, 1));
        set.insert(sstable_for_overlapping_test(s, 5, key_and_token_pair[4].first, key_and_token_pair[5].first, 1));

        sstable_set::incremental_selector sel = set.make_incremental_selector();
        check(sel, decorated_keys[0], {0, 1, 2});
        check(sel, decorated_keys[1], {0, 1, 2});
        check(sel, decorated_keys[2], {0});
        check(sel, decorated_keys[3], {0, 3});
        check(sel, decorated_keys[4], {0, 3, 4, 5});
        check(sel, decorated_keys[5], {0, 5});
        check(sel, decorated_keys[6], {0});
        check(sel, decorated_keys[7], {0});
    }

    return make_ready_future<>();
}

SEASTAR_TEST_CASE(sstable_resharding_strategy_tests) {
    // TODO: move it to sstable_resharding_test.cc. Unable to do so now because of linking issues
    // when using sstables::stats_metadata at sstable_resharding_test.cc.

  for (const auto version : all_sstable_versions) {
    auto s = make_lw_shared(schema({}, "ks", "cf", {{"p1", utf8_type}}, {}, {}, {}, utf8_type));
    auto get_sstable = [&] (int64_t gen, sstring first_key, sstring last_key) mutable {
        auto sst = make_sstable(s, "", gen, version, sstables::sstable::format_types::big);
        stats_metadata stats = {};
        stats.sstable_level = 1;
        sstables::test(sst).set_values(std::move(first_key), std::move(last_key), std::move(stats));
        return sst;
    };

    column_family_for_tests cf;

    auto tokens = token_generation_for_current_shard(2);
    auto stcs = sstables::make_compaction_strategy(sstables::compaction_strategy_type::size_tiered, s->compaction_strategy_options());
    auto lcs = sstables::make_compaction_strategy(sstables::compaction_strategy_type::leveled, s->compaction_strategy_options());

    auto sst1 = get_sstable(1, tokens[0].first, tokens[1].first);
    auto sst2 = get_sstable(2, tokens[1].first, tokens[1].first);

    {
        // TODO: sstable_test runs with smp::count == 1, thus we will not be able to stress it more
        // until we move this test case to sstable_resharding_test.
        auto descriptors = stcs.get_resharding_jobs(*cf, { sst1, sst2 });
        BOOST_REQUIRE(descriptors.size() == 2);
    }
    {
        auto ssts = std::vector<sstables::shared_sstable>{ sst1, sst2 };
        auto descriptors = lcs.get_resharding_jobs(*cf, ssts);
        auto expected_jobs = ssts.size()/smp::count + ssts.size()%smp::count;
        BOOST_REQUIRE(descriptors.size() == expected_jobs);
    }
  }

  return make_ready_future<>();
}

SEASTAR_TEST_CASE(sstable_tombstone_histogram_test) {
    return seastar::async([] {
        storage_service_for_tests ssft;
        for (auto version : all_sstable_versions) {
            auto builder = schema_builder("tests", "tombstone_histogram_test")
                    .with_column("id", utf8_type, column_kind::partition_key)
                    .with_column("value", int32_type);
            auto s = builder.build();

            auto tmp = make_lw_shared<tmpdir>();
            auto sst_gen = [s, tmp, gen = make_lw_shared<unsigned>(1), version]() mutable {
                return make_sstable(s, tmp->path, (*gen)++, version, big);
            };

            auto next_timestamp = [] {
                static thread_local api::timestamp_type next = 1;
                return next++;
            };

            auto make_delete = [&](partition_key key) {
                mutation m(s, key);
                tombstone tomb(next_timestamp(), gc_clock::now());
                m.partition().apply(tomb);
                return m;
            };

            std::vector<mutation> mutations;
            for (auto i = 0; i < sstables::TOMBSTONE_HISTOGRAM_BIN_SIZE * 2; i++) {
                auto key = partition_key::from_exploded(*s, {to_bytes("key" + to_sstring(i))});
                mutations.push_back(make_delete(key));
                forward_jump_clocks(std::chrono::seconds(1));
            }
            auto sst = make_sstable_containing(sst_gen, mutations);
            auto histogram = sst->get_stats_metadata().estimated_tombstone_drop_time;
            sst = reusable_sst(s, tmp->path, sst->generation(), version).get0();
            auto histogram2 = sst->get_stats_metadata().estimated_tombstone_drop_time;

            // check that histogram respected limit
            BOOST_REQUIRE(histogram.bin.size() == TOMBSTONE_HISTOGRAM_BIN_SIZE);
            // check that load procedure will properly load histogram from statistics component
            BOOST_REQUIRE(histogram.bin == histogram2.bin);
        }
    });
}

SEASTAR_TEST_CASE(sstable_bad_tombstone_histogram_test) {
    return seastar::async([] {
        auto builder = schema_builder("tests", "tombstone_histogram_test")
                .with_column("id", utf8_type, column_kind::partition_key)
                .with_column("value", int32_type);
        auto s = builder.build();
        auto sst = reusable_sst(s, "tests/sstables/bad_tombstone_histogram", 1).get0();
        auto histogram = sst->get_stats_metadata().estimated_tombstone_drop_time;
        BOOST_REQUIRE(histogram.max_bin_size == sstables::TOMBSTONE_HISTOGRAM_BIN_SIZE);
        // check that bad histogram was discarded
        BOOST_REQUIRE(histogram.bin.empty());
    });
}

SEASTAR_TEST_CASE(sstable_expired_data_ratio) {
    return seastar::async([] {
        storage_service_for_tests ssft;
        auto tmp = make_lw_shared<tmpdir>();
        auto s = make_lw_shared(schema({}, some_keyspace, some_column_family,
            {{"p1", utf8_type}}, {{"c1", utf8_type}}, {{"r1", utf8_type}}, {}, utf8_type));

        auto mt = make_lw_shared<memtable>(s);

        static constexpr float expired = 0.33;
        // we want number of expired keys to be ~ 1.5*sstables::TOMBSTONE_HISTOGRAM_BIN_SIZE so as to
        // test ability of histogram to return a good estimation after merging keys.
        static int total_keys = std::ceil(sstables::TOMBSTONE_HISTOGRAM_BIN_SIZE/expired)*1.5;

        auto insert_key = [&] (bytes k, uint32_t ttl, uint32_t expiration_time) {
            auto key = partition_key::from_exploded(*s, {k});
            mutation m(s, key);
            auto c_key = clustering_key::from_exploded(*s, {to_bytes("c1")});
            m.set_clustered_cell(c_key, *s->get_column_definition("r1"), make_atomic_cell(utf8_type, bytes("a"), ttl, expiration_time));
            mt->apply(std::move(m));
        };

        auto expired_keys = total_keys*expired;
        auto now = gc_clock::now();
        for (auto i = 0; i < expired_keys; i++) {
            // generate expiration time at different time points or only a few entries would be created in histogram
            auto expiration_time = (now - gc_clock::duration(DEFAULT_GC_GRACE_SECONDS*2+i)).time_since_epoch().count();
            insert_key(to_bytes("expired_key" + to_sstring(i)), 1, expiration_time);
        }
        auto remaining = total_keys-expired_keys;
        auto expiration_time = (now + gc_clock::duration(3600)).time_since_epoch().count();
        for (auto i = 0; i < remaining; i++) {
            insert_key(to_bytes("key" + to_sstring(i)), 3600, expiration_time);
        }
        auto sst = make_sstable(s, tmp->path, 1, la, big);
        write_memtable_to_sstable_for_test(*mt, sst).get();
        sst = reusable_sst(s, tmp->path, 1).get0();
        const auto& stats = sst->get_stats_metadata();
        BOOST_REQUIRE(stats.estimated_tombstone_drop_time.bin.size() == sstables::TOMBSTONE_HISTOGRAM_BIN_SIZE);
        auto gc_before = gc_clock::now() - s->gc_grace_seconds();
        auto uncompacted_size = sst->data_size();
        // Asserts that two keys are equal to within a positive delta
        BOOST_REQUIRE(std::fabs(sst->estimate_droppable_tombstone_ratio(gc_before) - expired) <= 0.1);

        column_family_for_tests cf(s);
        auto creator = [&] {
            auto sst = sstables::make_sstable(s, tmp->path, 2, la, big);
            sst->set_unshared();
            return sst;
        };
        auto info = sstables::compact_sstables(sstables::compaction_descriptor({ sst }), *cf, creator).get0();
        BOOST_REQUIRE(info.new_sstables.size() == 1);
        BOOST_REQUIRE(info.new_sstables.front()->estimate_droppable_tombstone_ratio(gc_before) == 0.0f);
        BOOST_REQUIRE_CLOSE(info.new_sstables.front()->data_size(), uncompacted_size*(1-expired), 5);

        std::map<sstring, sstring> options;
        options.emplace("tombstone_threshold", "0.3f");

        auto cs = sstables::make_compaction_strategy(sstables::compaction_strategy_type::size_tiered, options);
        // that's needed because sstable with expired data should be old enough.
        sstables::test(sst).set_data_file_write_time(db_clock::time_point::min());
        auto descriptor = cs.get_sstables_for_compaction(*cf, { sst });
        BOOST_REQUIRE(descriptor.sstables.size() == 1);
        BOOST_REQUIRE(descriptor.sstables.front() == sst);

        cs = sstables::make_compaction_strategy(sstables::compaction_strategy_type::leveled, options);
        sst->set_sstable_level(1);
        descriptor = cs.get_sstables_for_compaction(*cf, { sst });
        BOOST_REQUIRE(descriptor.sstables.size() == 1);
        BOOST_REQUIRE(descriptor.sstables.front() == sst);
        // make sure sstable picked for tombstone compaction removal won't be promoted or demoted.
        BOOST_REQUIRE(descriptor.sstables.front()->get_sstable_level() == 1U);

        // check tombstone compaction is disabled by default for DTCS
        cs = sstables::make_compaction_strategy(sstables::compaction_strategy_type::date_tiered, {});
        descriptor = cs.get_sstables_for_compaction(*cf, { sst });
        BOOST_REQUIRE(descriptor.sstables.size() == 0);
        cs = sstables::make_compaction_strategy(sstables::compaction_strategy_type::date_tiered, options);
        descriptor = cs.get_sstables_for_compaction(*cf, { sst });
        BOOST_REQUIRE(descriptor.sstables.size() == 1);
        BOOST_REQUIRE(descriptor.sstables.front() == sst);

        // sstable with droppable ratio of 0.3 won't be included due to threshold
        {
            std::map<sstring, sstring> options;
            options.emplace("tombstone_threshold", "0.5f");
            auto cs = sstables::make_compaction_strategy(sstables::compaction_strategy_type::size_tiered, options);
            auto descriptor = cs.get_sstables_for_compaction(*cf, { sst });
            BOOST_REQUIRE(descriptor.sstables.size() == 0);
        }
        // sstable which was recently created won't be included due to min interval
        {
            std::map<sstring, sstring> options;
            options.emplace("tombstone_compaction_interval", "3600");
            auto cs = sstables::make_compaction_strategy(sstables::compaction_strategy_type::size_tiered, options);
            sstables::test(sst).set_data_file_write_time(db_clock::now());
            auto descriptor = cs.get_sstables_for_compaction(*cf, { sst });
            BOOST_REQUIRE(descriptor.sstables.size() == 0);
        }
    });
}

SEASTAR_TEST_CASE(sstable_owner_shards) {
    return seastar::async([] {
        storage_service_for_tests ssft;
        cell_locker_stats cl_stats;

        auto builder = schema_builder("tests", "test")
                .with_column("id", utf8_type, column_kind::partition_key)
                .with_column("value", int32_type);
        auto s = builder.build();

        auto tmp = make_lw_shared<tmpdir>();
        auto sst_gen = [s, tmp, gen = make_lw_shared<unsigned>(1)] () mutable {
            auto sst = make_sstable(s, tmp->path, (*gen)++, la, big);
            sst->set_unshared();
            return sst;
        };
        auto make_insert = [&] (auto p) {
            auto key = partition_key::from_exploded(*s, {to_bytes(p.first)});
            mutation m(s, key);
            m.set_clustered_cell(clustering_key::make_empty(), bytes("value"), data_value(int32_t(1)), 1);
            BOOST_REQUIRE(m.decorated_key().token() == p.second);
            return m;
        };

        auto make_shared_sstable = [&] (std::unordered_set<unsigned> shards, unsigned ignore_msb, unsigned smp_count) {
            auto mut = [&] (auto shard) {
                auto tokens = token_generation_for_shard(1, shard, ignore_msb, smp_count);
                return make_insert(tokens[0]);
            };
            auto muts = boost::copy_range<std::vector<mutation>>(shards
                | boost::adaptors::transformed([&] (auto shard) { return mut(shard); }));
            dht::default_partitioner = std::make_unique<dht::murmur3_partitioner>(1, ignore_msb);
            auto sst = make_sstable_containing(sst_gen, std::move(muts));
            dht::default_partitioner = std::make_unique<dht::murmur3_partitioner>(smp_count, ignore_msb);
            sst = reusable_sst(s, tmp->path, sst->generation()).get0();
            // restore partitioner
            dht::default_partitioner = std::make_unique<dht::murmur3_partitioner>(smp::count);
            return sst;
        };

        auto assert_sstable_owners = [&] (std::unordered_set<unsigned> expected_owners, unsigned ignore_msb, unsigned smp_count) {
            assert(expected_owners.size() <= smp_count);
            auto sst = make_shared_sstable(expected_owners, ignore_msb, smp_count);
            dht::default_partitioner = std::make_unique<dht::murmur3_partitioner>(smp_count, ignore_msb);
            auto owners = boost::copy_range<std::unordered_set<unsigned>>(sst->get_shards_for_this_sstable());
            BOOST_REQUIRE(boost::algorithm::all_of(expected_owners, [&] (unsigned expected_owner) {
                return owners.count(expected_owner);
            }));
        };

        assert_sstable_owners({ 0 }, 0, 1);
        assert_sstable_owners({ 0 }, 0, 1);

        assert_sstable_owners({ 0 }, 0, 4);
        assert_sstable_owners({ 0, 1 }, 0, 4);
        assert_sstable_owners({ 0, 2 }, 0, 4);
        assert_sstable_owners({ 0, 1, 2, 3 }, 0, 4);

        assert_sstable_owners({ 0 }, 12, 4);
        assert_sstable_owners({ 0, 1 }, 12, 4);
        assert_sstable_owners({ 0, 2 }, 12, 4);
        assert_sstable_owners({ 0, 1, 2, 3 }, 12, 4);

        assert_sstable_owners({ 10 }, 0, 63);
        assert_sstable_owners({ 10 }, 12, 63);
        assert_sstable_owners({ 10, 15 }, 0, 63);
        assert_sstable_owners({ 10, 15 }, 12, 63);
        assert_sstable_owners({ 0, 10, 15, 20, 30, 40, 50 }, 0, 63);
        assert_sstable_owners({ 0, 10, 15, 20, 30, 40, 50 }, 12, 63);
    });
}

SEASTAR_TEST_CASE(test_summary_entry_spanning_more_keys_than_min_interval) {
    return seastar::async([] {
        storage_service_for_tests ssft;
        auto s = make_lw_shared(schema({}, some_keyspace, some_column_family,
            {{"p1", int32_type}}, {{"c1", utf8_type}}, {{"r1", int32_type}}, {}, utf8_type));

        const column_definition& r1_col = *s->get_column_definition("r1");
        std::vector<mutation> mutations;
        auto keys_written = 0;
        for (auto i = 0; i < s->min_index_interval()*1.5; i++) {
            auto key = partition_key::from_exploded(*s, {int32_type->decompose(i)});
            auto c_key = clustering_key::from_exploded(*s, {to_bytes("abc")});
            mutation m(s, key);
            m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type, int32_type->decompose(1)));
            mutations.push_back(std::move(m));
            keys_written++;
        }

        auto tmp = make_lw_shared<tmpdir>();
        auto sst_gen = [s, tmp, gen = make_lw_shared<unsigned>(1)] () mutable {
            return make_sstable(s, tmp->path, (*gen)++, la, big);
        };
        auto sst = make_sstable_containing(sst_gen, mutations);
        sst = reusable_sst(s, tmp->path, sst->generation()).get0();

        summary& sum = sstables::test(sst).get_summary();
        BOOST_REQUIRE(sum.entries.size() == 1);

        std::set<mutation, mutation_decorated_key_less_comparator> merged;
        merged.insert(mutations.begin(), mutations.end());
        auto rd = assert_that(sst->as_mutation_source().make_reader(s, query::full_partition_range));
        auto keys_read = 0;
        for (auto&& m : merged) {
            keys_read++;
            rd.produces(m);
        }
        rd.produces_end_of_stream();
        BOOST_REQUIRE(keys_read == keys_written);

        auto r = dht::partition_range::make({mutations.back().decorated_key(), true}, {mutations.back().decorated_key(), true});
        assert_that(sst->as_mutation_source().make_reader(s, r))
            .produces(slice(mutations, r))
            .produces_end_of_stream();
    });
}

SEASTAR_TEST_CASE(test_wrong_counter_shard_order) {
        // CREATE TABLE IF NOT EXISTS scylla_bench.test_counters (
        //     pk bigint,
        //     ck bigint,
        //     c1 counter,
        //     c2 counter,
        //     c3 counter,
        //     c4 counter,
        //     c5 counter,
        //     PRIMARY KEY(pk, ck)
        // ) WITH compression = { }
        //
        // Populated with:
        // scylla-bench -mode counter_update -workload uniform -duration 15s
        //      -replication-factor 3 -partition-count 2 -clustering-row-count 4
        // on a three-node Scylla 1.7.4 cluster.
        return seastar::async([] {
          for (const auto version : all_sstable_versions) {
            auto s = schema_builder("scylla_bench", "test_counters")
                    .with_column("pk", long_type, column_kind::partition_key)
                    .with_column("ck", long_type, column_kind::clustering_key)
                    .with_column("c1", counter_type)
                    .with_column("c2", counter_type)
                    .with_column("c3", counter_type)
                    .with_column("c4", counter_type)
                    .with_column("c5", counter_type)
                    .build();

            auto sst = make_lw_shared<sstable>(s, get_test_dir("wrong_counter_shard_order", s), 2, version, big);
            sst->load().get0();
            auto reader = sstable_reader(sst, s);

            auto verify_row = [&s] (mutation_fragment_opt mfopt, int64_t expected_value) {
                BOOST_REQUIRE(bool(mfopt));
                auto& mf = *mfopt;
                BOOST_REQUIRE(mf.is_clustering_row());
                auto& row = mf.as_clustering_row();
                size_t n = 0;
                row.cells().for_each_cell([&] (column_id id, const atomic_cell_or_collection& ac_o_c) {
                    auto acv = ac_o_c.as_atomic_cell(s->regular_column_at(id));
                  counter_cell_view::with_linearized(acv, [&] (counter_cell_view ccv) {
                    counter_shard_view::less_compare_by_id cmp;
                    BOOST_REQUIRE_MESSAGE(boost::algorithm::is_sorted(ccv.shards(), cmp), ccv << " is expected to be sorted");
                    BOOST_REQUIRE_EQUAL(ccv.total_value(), expected_value);
                    n++;
                  });
                });
                BOOST_REQUIRE_EQUAL(n, 5);
            };

            {
                auto mfopt = reader(db::no_timeout).get0();
                BOOST_REQUIRE(mfopt);
                BOOST_REQUIRE(mfopt->is_partition_start());
                verify_row(reader(db::no_timeout).get0(), 28545);
                verify_row(reader(db::no_timeout).get0(), 27967);
                verify_row(reader(db::no_timeout).get0(), 28342);
                verify_row(reader(db::no_timeout).get0(), 28325);
                mfopt = reader(db::no_timeout).get0();
                BOOST_REQUIRE(mfopt);
                BOOST_REQUIRE(mfopt->is_end_of_partition());
            }

            {
                auto mfopt = reader(db::no_timeout).get0();
                BOOST_REQUIRE(mfopt);
                BOOST_REQUIRE(mfopt->is_partition_start());
                verify_row(reader(db::no_timeout).get0(), 28386);
                verify_row(reader(db::no_timeout).get0(), 28378);
                verify_row(reader(db::no_timeout).get0(), 28129);
                verify_row(reader(db::no_timeout).get0(), 28260);
                mfopt = reader(db::no_timeout).get0();
                BOOST_REQUIRE(mfopt);
                BOOST_REQUIRE(mfopt->is_end_of_partition());
            }

            BOOST_REQUIRE(!reader(db::no_timeout).get0());
        }
      });
}

SEASTAR_TEST_CASE(compaction_correctness_with_partitioned_sstable_set) {
    return seastar::async([] {
        storage_service_for_tests ssft;
        cell_locker_stats cl_stats;

        auto builder = schema_builder("tests", "tombstone_purge")
                .with_column("id", utf8_type, column_kind::partition_key)
                .with_column("value", int32_type);
        builder.set_gc_grace_seconds(0);
        builder.set_compaction_strategy(sstables::compaction_strategy_type::leveled);
        auto s = builder.build();

        auto tmp = make_lw_shared<tmpdir>();
        auto sst_gen = [s, tmp, gen = make_lw_shared<unsigned>(1)] () mutable {
            auto sst = make_sstable(s, tmp->path, (*gen)++, la, big);
            sst->set_unshared();
            return sst;
        };

        auto compact = [&, s] (std::vector<shared_sstable> all) -> std::vector<shared_sstable> {
            // NEEDED for partitioned_sstable_set to actually have an effect
            std::for_each(all.begin(), all.end(), [] (auto& sst) { sst->set_sstable_level(1); });
            column_family_for_tests cf(s);
            return sstables::compact_sstables(sstables::compaction_descriptor(std::move(all), 0, 0 /*std::numeric_limits<uint64_t>::max()*/),
                *cf, sst_gen).get0().new_sstables;
        };

        auto make_insert = [&] (auto p) {
            auto key = partition_key::from_exploded(*s, {to_bytes(p.first)});
            mutation m(s, key);
            m.set_clustered_cell(clustering_key::make_empty(), bytes("value"), data_value(int32_t(1)), 1 /* ts */);
            BOOST_REQUIRE(m.decorated_key().token() == p.second);
            return m;
        };

        auto tokens = token_generation_for_current_shard(4);
        auto mut1 = make_insert(tokens[0]);
        auto mut2 = make_insert(tokens[1]);
        auto mut3 = make_insert(tokens[2]);
        auto mut4 = make_insert(tokens[3]);

        {
            std::vector<shared_sstable> sstables = {
                    make_sstable_containing(sst_gen, {mut1, mut2}),
                    make_sstable_containing(sst_gen, {mut3, mut4})
            };

            auto result = compact(std::move(sstables));
            BOOST_REQUIRE_EQUAL(4, result.size());

            assert_that(sstable_reader(result[0], s))
                    .produces(mut1)
                    .produces_end_of_stream();
            assert_that(sstable_reader(result[1], s))
                    .produces(mut2)
                    .produces_end_of_stream();
            assert_that(sstable_reader(result[2], s))
                    .produces(mut3)
                    .produces_end_of_stream();
            assert_that(sstable_reader(result[3], s))
                    .produces(mut4)
                    .produces_end_of_stream();
        }

        {
            // with partitioned_sstable_set having an interval with exclusive lower boundary, example:
            // [mut1, mut2]
            // (mut2, mut3]
            std::vector<shared_sstable> sstables = {
                    make_sstable_containing(sst_gen, {mut1, mut2}),
                    make_sstable_containing(sst_gen, {mut2, mut3}),
                    make_sstable_containing(sst_gen, {mut3, mut4})
            };

            auto result = compact(std::move(sstables));
            BOOST_REQUIRE_EQUAL(4, result.size());

            assert_that(sstable_reader(result[0], s))
                    .produces(mut1)
                    .produces_end_of_stream();
            assert_that(sstable_reader(result[1], s))
                    .produces(mut2)
                    .produces_end_of_stream();
            assert_that(sstable_reader(result[2], s))
                    .produces(mut3)
                    .produces_end_of_stream();
            assert_that(sstable_reader(result[3], s))
                    .produces(mut4)
                    .produces_end_of_stream();
        }

        {
            // with gap between tables
            std::vector<shared_sstable> sstables = {
                    make_sstable_containing(sst_gen, {mut1, mut2}),
                    make_sstable_containing(sst_gen, {mut4, mut4})
            };

            auto result = compact(std::move(sstables));
            BOOST_REQUIRE_EQUAL(3, result.size());

            assert_that(sstable_reader(result[0], s))
                    .produces(mut1)
                    .produces_end_of_stream();
            assert_that(sstable_reader(result[1], s))
                    .produces(mut2)
                    .produces_end_of_stream();
            assert_that(sstable_reader(result[2], s))
                    .produces(mut4)
                    .produces_end_of_stream();
        }
    });
}

static std::unique_ptr<index_reader> get_index_reader(shared_sstable sst) {
    return std::make_unique<index_reader>(sst, default_priority_class());
}

SEASTAR_TEST_CASE(test_broken_promoted_index_is_skipped) {
    // create table ks.test (pk int, ck int, v int, primary key(pk, ck)) with compact storage;
    //
    // Populated with:
    //
    // insert into ks.test (pk, ck, v) values (1, 1, 1);
    // insert into ks.test (pk, ck, v) values (1, 2, 1);
    // insert into ks.test (pk, ck, v) values (1, 3, 1);
    // delete from ks.test where pk = 1 and ck = 2;
    return seastar::async([] {
      for (const auto version : all_sstable_versions) {
        auto s = schema_builder("ks", "test")
                .with_column("pk", int32_type, column_kind::partition_key)
                .with_column("ck", int32_type, column_kind::clustering_key)
                .with_column("v", int32_type)
                .build(schema_builder::compact_storage::yes);

        auto sst = sstables::make_sstable(s, get_test_dir("broken_non_compound_pi_and_range_tombstone", s), 1, version, big);
        sst->load().get0();

        {
            assert_that(get_index_reader(sst)).is_empty(*s);
        }
      }
    });
}

SEASTAR_TEST_CASE(test_old_format_non_compound_range_tombstone_is_read) {
    // create table ks.test (pk int, ck int, v int, primary key(pk, ck)) with compact storage;
    //
    // Populated with:
    //
    // insert into ks.test (pk, ck, v) values (1, 1, 1);
    // insert into ks.test (pk, ck, v) values (1, 2, 1);
    // insert into ks.test (pk, ck, v) values (1, 3, 1);
    // delete from ks.test where pk = 1 and ck = 2;
    return seastar::async([] {
        for (const auto version : all_sstable_versions) {
            if (version != sstables::sstable::version_types::mc) { // Does not apply to 'mc' format
                auto s = schema_builder("ks", "test")
                    .with_column("pk", int32_type, column_kind::partition_key)
                    .with_column("ck", int32_type, column_kind::clustering_key)
                    .with_column("v", int32_type)
                    .build(schema_builder::compact_storage::yes);

                auto sst = sstables::make_sstable(s, get_test_dir("broken_non_compound_pi_and_range_tombstone", s), 1, version, big);
                sst->load().get0();

                auto pk = partition_key::from_exploded(*s, { int32_type->decompose(1) });
                auto dk = dht::global_partitioner().decorate_key(*s, pk);
                auto ck = clustering_key::from_exploded(*s, {int32_type->decompose(2)});
                mutation m(s, dk);
                m.set_clustered_cell(ck, *s->get_column_definition("v"), atomic_cell::make_live(*int32_type, 1511270919978349, int32_type->decompose(1), { }));
                m.partition().apply_delete(*s, ck, {1511270943827278, gc_clock::from_time_t(1511270943)});

                {
                    auto slice = partition_slice_builder(*s).with_range(query::clustering_range::make_singular({ck})).build();
                    assert_that(sst->as_mutation_source().make_reader(s, dht::partition_range::make_singular(dk), slice))
                            .produces(m)
                            .produces_end_of_stream();
                }
            }
        }
    });
}

SEASTAR_TEST_CASE(summary_rebuild_sanity) {
    return seastar::async([] {
        storage_service_for_tests ssft;
        auto builder = schema_builder("tests", "test")
                .with_column("id", utf8_type, column_kind::partition_key)
                .with_column("value", utf8_type);
        builder.set_compressor_params(compression_parameters::no_compression());
        auto s = builder.build(schema_builder::compact_storage::no);
        const column_definition& col = *s->get_column_definition("value");

        auto make_insert = [&] (partition_key key) {
            mutation m(s, key);
            m.set_clustered_cell(clustering_key::make_empty(), col, make_atomic_cell(utf8_type, bytes(1024, 'a')));
            return m;
        };

        std::vector<mutation> mutations;
        for (auto i = 0; i < s->min_index_interval()*2; i++) {
            auto key = to_bytes("key" + to_sstring(i));
            mutations.push_back(make_insert(partition_key::from_exploded(*s, {std::move(key)})));
        }

        auto tmp = make_lw_shared<tmpdir>();
        auto sst_gen = [s, tmp, gen = make_lw_shared<unsigned>(1)] () mutable {
            return make_sstable(s, tmp->path, (*gen)++, la, big);
        };
        auto sst = make_sstable_containing(sst_gen, mutations);

        summary s1 = sstables::test(sst).move_summary();
        BOOST_REQUIRE(s1.entries.size() > 1);

        sstables::test(sst).remove_component(component_type::Summary).get();
        sst = reusable_sst(s, tmp->path, 1).get0();
        summary& s2 = sstables::test(sst).get_summary();

        BOOST_REQUIRE(::memcmp(&s1.header, &s2.header, sizeof(summary::header)) == 0);
        BOOST_REQUIRE(s1.positions == s2.positions);
        BOOST_REQUIRE(s1.entries == s2.entries);
        BOOST_REQUIRE(s1.first_key.value == s2.first_key.value);
        BOOST_REQUIRE(s1.last_key.value == s2.last_key.value);
    });
}

SEASTAR_TEST_CASE(sstable_partition_estimation_sanity_test) {
    return seastar::async([] {
        storage_service_for_tests ssft;
        auto builder = schema_builder("tests", "test")
                .with_column("id", utf8_type, column_kind::partition_key)
                .with_column("value", utf8_type);
        builder.set_compressor_params(compression_parameters::no_compression());
        auto s = builder.build(schema_builder::compact_storage::no);
        const column_definition& col = *s->get_column_definition("value");

        auto summary_byte_cost = sstables::index_sampling_state::default_summary_byte_cost;

        auto make_large_partition = [&] (partition_key key) {
            mutation m(s, key);
            m.set_clustered_cell(clustering_key::make_empty(), col, make_atomic_cell(utf8_type, bytes(20 * summary_byte_cost, 'a')));
            return m;
        };

        auto make_small_partition = [&] (partition_key key) {
            mutation m(s, key);
            m.set_clustered_cell(clustering_key::make_empty(), col, make_atomic_cell(utf8_type, bytes(100, 'a')));
            return m;
        };

        auto tmp = make_lw_shared<tmpdir>();
        auto sst_gen = [s, tmp, gen = make_lw_shared<unsigned>(1)] () mutable {
            return make_sstable(s, tmp->path, (*gen)++, la, big);
        };

        {
            auto total_partitions = s->min_index_interval()*2;

            std::vector<mutation> mutations;
            for (auto i = 0; i < total_partitions; i++) {
                auto key = to_bytes("key" + to_sstring(i));
                mutations.push_back(make_large_partition(partition_key::from_exploded(*s, {std::move(key)})));
            }
            auto sst = make_sstable_containing(sst_gen, mutations);

            BOOST_REQUIRE(std::abs(int64_t(total_partitions) - int64_t(sst->get_estimated_key_count())) <= s->min_index_interval());
        }

        {
            auto total_partitions = s->min_index_interval()*2;

            std::vector<mutation> mutations;
            for (auto i = 0; i < total_partitions; i++) {
                auto key = to_bytes("key" + to_sstring(i));
                mutations.push_back(make_small_partition(partition_key::from_exploded(*s, {std::move(key)})));
            }
            auto sst = make_sstable_containing(sst_gen, mutations);

            BOOST_REQUIRE(std::abs(int64_t(total_partitions) - int64_t(sst->get_estimated_key_count())) <= s->min_index_interval());
        }
    });
}

SEASTAR_TEST_CASE(sstable_timestamp_metadata_correcness_with_negative) {
    BOOST_REQUIRE(smp::count == 1);
    return seastar::async([] {
        storage_service_for_tests ssft;
        for (auto version : all_sstable_versions) {
            cell_locker_stats cl_stats;

            auto s = schema_builder("tests", "ts_correcness_test")
                    .with_column("id", utf8_type, column_kind::partition_key)
                    .with_column("value", int32_type).build();

            auto tmp = make_lw_shared<tmpdir>();
            auto sst_gen = [s, tmp, gen = make_lw_shared<unsigned>(1), version]() mutable {
                return make_sstable(s, tmp->path, (*gen)++, version, big);
            };

            auto make_insert = [&](partition_key key, api::timestamp_type ts) {
                mutation m(s, key);
                m.set_clustered_cell(clustering_key::make_empty(), bytes("value"), data_value(int32_t(1)), ts);
                return m;
            };

            auto alpha = partition_key::from_exploded(*s, {to_bytes("alpha")});
            auto beta = partition_key::from_exploded(*s, {to_bytes("beta")});

            auto mut1 = make_insert(alpha, -50);
            auto mut2 = make_insert(beta, 5);

            auto sst = make_sstable_containing(sst_gen, {mut1, mut2});

            BOOST_REQUIRE(sst->get_stats_metadata().min_timestamp == -50);
            BOOST_REQUIRE(sst->get_stats_metadata().max_timestamp == 5);
        }
    });
}
