/*
 * Copyright (C) 2018 ScyllaDB
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

#include <set>
#include <fstream>
#include <iterator>

#include <boost/test/unit_test.hpp>

#include <seastar/core/thread.hh>
#include <seastar/tests/test-utils.hh>

#include "sstables/sstables.hh"
#include "compress.hh"
#include "schema_builder.hh"
#include "sstable_test.hh"
#include "flat_mutation_reader_assertions.hh"
#include "sstable_test.hh"
#include "tests/test_services.hh"
#include "tests/tmpdir.hh"
#include "tests/sstable_utils.hh"

using namespace sstables;

class sstable_assertions final {
    shared_sstable _sst;
public:
    sstable_assertions(schema_ptr schema, const sstring& path, int generation = 1)
        : _sst(make_sstable(std::move(schema),
                            path,
                            generation,
                            sstable_version_types::mc,
                            sstable_format_types::big,
                            gc_clock::now(),
                            default_io_error_handler_gen(),
                            1))
    { }
    void read_toc() {
        _sst->read_toc().get();
    }
    void read_summary() {
        _sst->read_summary(default_priority_class()).get();
    }
    void read_filter() {
        _sst->read_filter(default_priority_class()).get();
    }
    void read_statistics() {
        _sst->read_statistics(default_priority_class()).get();
    }
    void load() {
        _sst->load().get();
    }
    future<index_list> read_index() {
        load();
        return sstables::test(_sst).read_indexes();
    }
    flat_mutation_reader read_rows_flat() {
        return _sst->read_rows_flat(_sst->_schema);
    }
    void assert_toc(const std::set<component_type>& expected_components) {
        for (auto& expected : expected_components) {
            if(_sst->_recognized_components.count(expected) == 0) {
                BOOST_FAIL(sprint("Expected component of TOC missing: %s\n ... in: %s",
                                  expected,
                                  std::set<component_type>(
                                      cbegin(_sst->_recognized_components),
                                      cend(_sst->_recognized_components))));
            }
        }
        for (auto& present : _sst->_recognized_components) {
            if (expected_components.count(present) == 0) {
                BOOST_FAIL(sprint("Unexpected component of TOC: %s\n ... when expecting: %s",
                                  present,
                                  expected_components));
            }
        }
    }
};

// Following tests run on files in tests/sstables/3.x/uncompressed/partition_key_only
// They were created using following CQL statements:
//
// CREATE KEYSPACE test_ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
//
// CREATE TABLE test_ks.test_table ( pk INT, PRIMARY KEY(pk))
//      WITH compression = { 'enabled' : false };
//
// INSERT INTO test_ks.test_table(pk) VALUES(1);
// INSERT INTO test_ks.test_table(pk) VALUES(2);
// INSERT INTO test_ks.test_table(pk) VALUES(3);
// INSERT INTO test_ks.test_table(pk) VALUES(4);
// INSERT INTO test_ks.test_table(pk) VALUES(5);

static thread_local const sstring UNCOMPRESSED_PARTITION_KEY_ONLY_PATH =
    "tests/sstables/3.x/uncompressed/partition_key_only";
static thread_local const schema_ptr UNCOMPRESSED_PARTITION_KEY_ONLY_SCHEMA =
    schema_builder("test_ks", "test_table")
        .with_column("pk", int32_type, column_kind::partition_key)
        .build();

SEASTAR_TEST_CASE(test_uncompressed_partition_key_only_load) {
    return seastar::async([] {
        sstable_assertions sst(UNCOMPRESSED_PARTITION_KEY_ONLY_SCHEMA, UNCOMPRESSED_PARTITION_KEY_ONLY_PATH);
        sst.load();
    });
}

SEASTAR_TEST_CASE(test_uncompressed_partition_key_only_read) {
    return seastar::async([] {
        sstable_assertions sst(UNCOMPRESSED_PARTITION_KEY_ONLY_SCHEMA, UNCOMPRESSED_PARTITION_KEY_ONLY_PATH);
        sst.load();
        auto to_key = [] (int key) {
            auto bytes = int32_type->decompose(int32_t(key));
            auto pk = partition_key::from_single_value(*UNCOMPRESSED_PARTITION_KEY_ONLY_SCHEMA, bytes);
            return dht::global_partitioner().decorate_key(*UNCOMPRESSED_PARTITION_KEY_ONLY_SCHEMA, pk);
        };
        assert_that(sst.read_rows_flat())
            .produces_partition_start(to_key(5))
            .produces_partition_end()
            .produces_partition_start(to_key(1))
            .produces_partition_end()
            .produces_partition_start(to_key(2))
            .produces_partition_end()
            .produces_partition_start(to_key(4))
            .produces_partition_end()
            .produces_partition_start(to_key(3))
            .produces_partition_end()
            .produces_end_of_stream();
    });
}

// Following tests run on files in tests/sstables/3.x/uncompressed/simple
// They were created using following CQL statements:
//
// CREATE KEYSPACE test_ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
//
// CREATE TABLE test_ks.test_table ( pk INT, ck INT, val INT, PRIMARY KEY(pk, ck))
//      WITH compression = { 'enabled' : false };
//
// INSERT INTO test_ks.test_table(pk, ck, val) VALUES(1, 101, 1001);
// INSERT INTO test_ks.test_table(pk, ck, val) VALUES(2, 102, 1002);
// INSERT INTO test_ks.test_table(pk, ck, val) VALUES(3, 103, 1003);
// INSERT INTO test_ks.test_table(pk, ck, val) VALUES(4, 104, 1004);
// INSERT INTO test_ks.test_table(pk, ck, val) VALUES(5, 105, 1005);

static thread_local const sstring UNCOMPRESSED_SIMPLE_PATH = "tests/sstables/3.x/uncompressed/simple";
static thread_local const schema_ptr UNCOMPRESSED_SIMPLE_SCHEMA =
    schema_builder("test_ks", "test_table")
        .with_column("pk", int32_type, column_kind::partition_key)
        .with_column("ck", int32_type, column_kind::clustering_key)
        .with_column("val", int32_type)
        .build();

SEASTAR_TEST_CASE(test_uncompressed_simple_read_toc) {
    return seastar::async([] {
        sstable_assertions sst(UNCOMPRESSED_SIMPLE_SCHEMA, UNCOMPRESSED_SIMPLE_PATH);
        sst.read_toc();
        using ct = component_type;
        sst.assert_toc({ct::Index,
                        ct::Data,
                        ct::TOC,
                        ct::Summary,
                        ct::Digest,
                        ct::CRC,
                        ct::Filter,
                        ct::Statistics});
    });
}

SEASTAR_TEST_CASE(test_uncompressed_simple_read_summary) {
    return seastar::async([] {
        sstable_assertions sst(UNCOMPRESSED_SIMPLE_SCHEMA, UNCOMPRESSED_SIMPLE_PATH);
        sst.read_toc();
        sst.read_summary();
    });
}

SEASTAR_TEST_CASE(test_uncompressed_simple_read_filter) {
    return seastar::async([] {
        sstable_assertions sst(UNCOMPRESSED_SIMPLE_SCHEMA, UNCOMPRESSED_SIMPLE_PATH);
        sst.read_toc();
        sst.read_filter();
    });
}

SEASTAR_TEST_CASE(test_uncompressed_simple_read_statistics) {
    return seastar::async([] {
        sstable_assertions sst(UNCOMPRESSED_SIMPLE_SCHEMA, UNCOMPRESSED_SIMPLE_PATH);
        sst.read_toc();
        sst.read_statistics();
    });
}

SEASTAR_TEST_CASE(test_uncompressed_simple_load) {
    return seastar::async([] {
        sstable_assertions sst(UNCOMPRESSED_SIMPLE_SCHEMA, UNCOMPRESSED_SIMPLE_PATH);
        sst.load();
    });
}

SEASTAR_TEST_CASE(test_uncompressed_simple_read_index) {
    return seastar::async([] {
        sstable_assertions sst(UNCOMPRESSED_SIMPLE_SCHEMA, UNCOMPRESSED_SIMPLE_PATH);
        auto vec = sst.read_index().get0();
        BOOST_REQUIRE_EQUAL(1, vec.size());
    });
}

static void compare_files(sstring filename1, sstring filename2) {
    std::ifstream ifs1(filename1);
    std::ifstream ifs2(filename2);

    std::istream_iterator<char> b1(ifs1), e1;
    std::istream_iterator<char> b2(ifs2), e2;
    BOOST_CHECK_EQUAL_COLLECTIONS(b1, e1, b2, e2);
}

static void write_and_compare_sstables(schema_ptr s, lw_shared_ptr<memtable> mt, sstring table_name) {
    storage_service_for_tests ssft;
    tmpdir tmp;
    auto sst = sstables::test::make_test_sstable(4096, s, tmp.path, 1, sstables::sstable_version_types::mc, sstable::format_types::big);
    write_memtable_to_sstable_for_test(*mt, sst).get();

    for (auto file_type : {component_type::Data,
                           component_type::Index,
                           component_type::Statistics,
                           component_type::Filter}) {
        auto orig_filename =
                sstable::filename("tests/sstables/3.x/uncompressed/write_" + table_name, "ks",
                                  table_name, sstables::sstable_version_types::mc, 1, big, file_type);
        auto result_filename =
                sstable::filename(tmp.path, "ks", table_name, sstables::sstable_version_types::mc, 1, big, file_type);
        compare_files(orig_filename, result_filename);
    }
}

static constexpr api::timestamp_type write_timestamp = 1525385507816568;
static constexpr gc_clock::time_point write_time_point = gc_clock::time_point{} + gc_clock::duration{1525385507};

SEASTAR_TEST_CASE(test_write_static_row) {
    return seastar::async([] {
        sstring table_name = "static_row";
        // CREATE TABLE static_row (pk int, ck int, st1 int static, st2 text static, PRIMARY KEY (pk, ck)) WITH compression = {'sstable_compression': ''};
        schema_builder builder(make_lw_shared(schema(generate_legacy_id("ks", table_name), "sst3", table_name,
            // partition key
            {{"pk", utf8_type}},
            // clustering key
            {{"ck", int32_type}},
            // regular columns
            {},
            // static columns
            {{"st1", int32_type}, {"st2", utf8_type}},
            // regular column name type
            utf8_type,
            // comment
            "SSTable 3.0 format write path - static row test"
        )));
        builder.set_compressor_params(compression_parameters());
        schema_ptr s = builder.build(schema_builder::compact_storage::no);

        lw_shared_ptr<memtable> mt = make_lw_shared<memtable>(s);

        // INSERT INTO static_row (pk, st1, st2) values ('key1', 1135, 'hello');
        auto key = make_dkey(s, {to_bytes("key1")});
        mutation mut{s, key};
        mut.set_static_cell("st1", data_value{1135}, write_timestamp);
        mut.set_static_cell("st2", data_value{"hello"}, write_timestamp);
        mt->apply(std::move(mut));

        write_and_compare_sstables(s, mt, table_name);
    });
}

SEASTAR_TEST_CASE(test_write_composite_partition_key) {
    return seastar::async([] {
        sstring table_name = "composite_partition_key";
        // CREATE TABLE composite_partition_key (a int , b text, c boolean, d int, e text, f int, g text, PRIMARY KEY ((a, b, c), d, e)) WITH compression = {'sstable_compression': ''};
        schema_builder builder(make_lw_shared(schema(generate_legacy_id("ks", table_name), "sst3", table_name,
            // partition key
            {{"a", int32_type}, {"b", utf8_type}, {"c", boolean_type}},
            // clustering key
            {{"d", int32_type}, {"e", utf8_type}},
            // regular columns
            {{"f", int32_type}, {"g", utf8_type}},
            // static columns
            {},
            // regular column name type
            utf8_type,
            // comment
            "SSTable 3.0 format write path - composite partition key test"
        )));
        builder.set_compressor_params(compression_parameters());
        schema_ptr s = builder.build(schema_builder::compact_storage::no);

        lw_shared_ptr<memtable> mt = make_lw_shared<memtable>(s);

        // INSERT INTO composite_partition_key (a,b,c,d,e,f,g) values (1, 'hello', true, 2, 'dear', 3, 'world');
        auto key = partition_key::from_deeply_exploded(*s, { data_value{1}, data_value{"hello"}, data_value{true} });
        mutation mut{s, key};
        clustering_key ckey = clustering_key::from_deeply_exploded(*s, { 2, "dear" });
        mut.partition().apply_insert(*s, ckey, write_timestamp);
        mut.set_cell(ckey, "f", data_value{3}, write_timestamp);
        mut.set_cell(ckey, "g", data_value{"world"}, write_timestamp);
        mt->apply(std::move(mut));

        write_and_compare_sstables(s, mt, table_name);
    });
}

SEASTAR_TEST_CASE(test_write_composite_clustering_key) {
    return seastar::async([] {
        sstring table_name = "composite_clustering_key";
        // CREATE TABLE composite_clustering_key (a int , b text, c int, d text, e int, f text, PRIMARY KEY (a, b, c, d)) WITH compression = {'sstable_compression': ''};
        schema_builder builder(make_lw_shared(schema(generate_legacy_id("ks", table_name), "sst3", table_name,
            // partition key
            {{"a", int32_type}},
            // clustering key
            {{"b", utf8_type}, {"c", int32_type}, {"d", utf8_type}},
            // regular columns
            {{"e", int32_type}, {"f", utf8_type}},
            // static columns
            {},
            // regular column name type
            utf8_type,
            // comment
            "SSTable 3.0 format write path - composite clustering key test"
        )));
        builder.set_compressor_params(compression_parameters());
        schema_ptr s = builder.build(schema_builder::compact_storage::no);

        lw_shared_ptr<memtable> mt = make_lw_shared<memtable>(s);

        // INSERT INTO composite_clustering_key (a,b,c,d,e,f) values (1, 'hello', 2, 'dear', 3, 'world');
        auto key = partition_key::from_deeply_exploded(*s, { 1 });
        mutation mut{s, key};
        clustering_key ckey = clustering_key::from_deeply_exploded(*s, { "hello", 2, "dear" });
        mut.partition().apply_insert(*s, ckey, write_timestamp);
        mut.set_cell(ckey, "e", data_value{3}, write_timestamp);
        mut.set_cell(ckey, "f", data_value{"world"}, write_timestamp);
        mt->apply(std::move(mut));

        write_and_compare_sstables(s, mt, table_name);
    });
}

SEASTAR_TEST_CASE(test_write_wide_partitions) {
    return seastar::async([] {
        sstring table_name = "wide_partitions";
        // CREATE TABLE wide_partitions (pk text, ck text, st text, rc text, PRIMARY KEY (pk, ck) WITH compression = {'sstable_compression': ''};
        schema_builder builder(make_lw_shared(schema(generate_legacy_id("ks", table_name), "sst3", table_name,
            // partition key
            {{"pk", utf8_type}},
            // clustering key
            {{"ck", utf8_type}},
            // regular columns
            {{"rc", utf8_type}},
            // static columns
            {{"st", utf8_type}},
            // regular column name type
            utf8_type,
            // comment
            "SSTable 3.0 format write path - wide partitions test"
        )));
        builder.set_compressor_params(compression_parameters());
        schema_ptr s = builder.build(schema_builder::compact_storage::no);

        lw_shared_ptr<memtable> mt = make_lw_shared<memtable>(s);
        api::timestamp_type ts = write_timestamp;
        {
            auto key = make_dkey(s, {to_bytes("key1")});
            mutation mut{s, key};
            mut.set_static_cell("st", data_value{"hello"}, ts);
            sstring ck_base(1024, 'a');
            sstring rc_base(1024, 'b');
            for (auto idx: boost::irange(0, 1024)) {
                clustering_key ckey = clustering_key::from_deeply_exploded(*s, {format("{}{}", ck_base, idx)});
                mut.partition().apply_insert(*s, ckey, ts);
                mut.set_cell(ckey, "rc", data_value{format("{}{}", rc_base, idx)}, ts);
                seastar::thread::yield();
            }
            mt->apply(std::move(mut));
            ts += 10;
        }
        {
            auto key = make_dkey(s, {to_bytes("key2")});
            mutation mut{s, key};
            mut.set_static_cell("st", data_value{"goodbye"}, ts);
            sstring ck_base(1024, 'a');
            sstring rc_base(1024, 'b');
            for (auto idx: boost::irange(0, 1024)) {
                clustering_key ckey = clustering_key::from_deeply_exploded(*s, {format("{}{}", ck_base, idx)});
                mut.partition().apply_insert(*s, ckey, ts);
                mut.set_cell(ckey, "rc", data_value{format("{}{}", rc_base, idx)}, ts);
                seastar::thread::yield();
            }
            mt->apply(std::move(mut));
        }

        write_and_compare_sstables(s, mt, table_name);
    });
}

SEASTAR_TEST_CASE(test_write_ttled_row) {
    return seastar::async([] {
        sstring table_name = "ttled_row";
        // CREATE TABLE ttled_row (pk int, ck int, rc int, PRIMARY KEY (pk));
        schema_builder builder(make_lw_shared(schema(generate_legacy_id("ks", table_name), "sst3", table_name,
            // partition key
            {{"pk", int32_type}},
            // clustering key
            {{"ck", int32_type}},
            // regular columns
            {{"rc", int32_type}},
            // static columns
            {},
            // regular column name type
            utf8_type,
            // comment
            "SSTable 3.0 format write path - TTL-ed row test"
        )));
        builder.set_compressor_params(compression_parameters());
        schema_ptr s = builder.build(schema_builder::compact_storage::no);

        lw_shared_ptr<memtable> mt = make_lw_shared<memtable>(s);

        // INSERT INTO ttled_row (pk, ck, rc) VALUES ( 1, 2, 3) USING TTL 1135;
        auto key = partition_key::from_deeply_exploded(*s, { 1 });
        mutation mut{s, key};
        clustering_key ckey = clustering_key::from_deeply_exploded(*s, { 2 });
        gc_clock::duration ttl{1135};
        mut.partition().apply_insert(*s, ckey, write_timestamp, ttl, write_time_point + ttl);
        mut.set_cell(ckey, "rc", data_value{3}, write_timestamp);
        mt->apply(std::move(mut));

        write_and_compare_sstables(s, mt, table_name);
    });
}

SEASTAR_TEST_CASE(test_write_ttled_column) {
    return seastar::async([] {
        sstring table_name = "ttled_column";
        // CREATE TABLE ttled_column (pk text, rc int, PRIMARY KEY (pk));
        schema_builder builder(make_lw_shared(schema(generate_legacy_id("ks", table_name), "sst3", table_name,
            // partition key
            {{"pk", utf8_type}},
            // clustering key
            {},
            // regular columns
            {{"rc", int32_type}},
            // static columns
            {},
            // regular column name type
            utf8_type,
            // comment
            "SSTable 3.0 format write path - TTL-ed column test"
        )));
        builder.set_compressor_params(compression_parameters());
        schema_ptr s = builder.build(schema_builder::compact_storage::no);

        lw_shared_ptr<memtable> mt = make_lw_shared<memtable>(s);

        // UPDATE ttled_column USING TTL 1135 SET rc = 1 WHERE pk='key';
        auto key = make_dkey(s, {to_bytes("key")});
        mutation mut{s, key};

        gc_clock::duration ttl{1135};
        auto column_def = s->get_column_definition("rc");
        if (!column_def) {
            throw std::runtime_error("no column definition found");
        }
        bytes value = column_def->type->decompose(data_value{1});
        auto cell = atomic_cell::make_live(write_timestamp, value, write_time_point + ttl, ttl);
        mut.set_clustered_cell(clustering_key::make_empty(), *column_def, cell);
        mt->apply(std::move(mut));

        write_and_compare_sstables(s, mt, table_name);
    });
}

SEASTAR_TEST_CASE(test_write_deleted_column) {
    return seastar::async([] {
        sstring table_name = "deleted_column";
        // CREATE TABLE deleted_cell (int pk, int rc, PRIMARY KEY (pk)) WITH compression = {'sstable_compression': ''};
        schema_builder builder(make_lw_shared(schema(generate_legacy_id("ks", table_name), "sst3", table_name,
            // partition key
            {{"pk", int32_type}},
            // clustering key
            {},
            // regular columns
            {{"rc", int32_type}},
            // static columns
            {},
            // regular column name type
            utf8_type,
            // comment
            "SSTable 3.0 format write path - deleted column test"
        )));
        builder.set_compressor_params(compression_parameters());
        schema_ptr s = builder.build(schema_builder::compact_storage::no);

        lw_shared_ptr<memtable> mt = make_lw_shared<memtable>(s);

        // DELETE rc FROM deleted_column WHERE pk=1;
        auto key = partition_key::from_deeply_exploded(*s, { 1 });
        mutation mut{s, key};
        //mut.partition().apply_delete(*s, clustering_key::make_empty(), tombstone{api::new_timestamp(), gc_clock::now()});
        auto column_def = s->get_column_definition("rc");
        if (!column_def) {
            throw std::runtime_error("no column definition found");
        }
        mut.set_cell(clustering_key::make_empty(), *column_def, atomic_cell::make_dead(write_timestamp, write_time_point));
        mt->apply(std::move(mut));

        write_and_compare_sstables(s, mt, table_name);
    });
}

SEASTAR_TEST_CASE(test_write_deleted_row) {
    return seastar::async([] {
        sstring table_name = "deleted_row";
        // CREATE TABLE deleted_row (int pk, int ck, PRIMARY KEY (pk, ck)) WITH compression = {'sstable_compression': ''};
        schema_builder builder(make_lw_shared(schema(generate_legacy_id("ks", table_name), "sst3", table_name,
            // partition key
            {{"pk", int32_type}},
            // clustering key
            {{"ck", int32_type}},
            // regular columns
            {},
            // static columns
            {},
            // regular column name type
            utf8_type,
            // comment
            "SSTable 3.0 format write path - deleted row test"
        )));
        builder.set_compressor_params(compression_parameters());
        schema_ptr s = builder.build(schema_builder::compact_storage::no);

        lw_shared_ptr<memtable> mt = make_lw_shared<memtable>(s);

        // DELETE FROM deleted_row WHERE pk=1 and ck=2;
        auto key = partition_key::from_deeply_exploded(*s, { 1 });
        mutation mut{s, key};
        clustering_key ckey = clustering_key::from_deeply_exploded(*s, { 2 });
        mut.partition().apply_delete(*s, ckey, tombstone{write_timestamp, write_time_point});
        mt->apply(std::move(mut));

        write_and_compare_sstables(s, mt, table_name);
    });
}

SEASTAR_TEST_CASE(test_write_collection_wide_update) {
    return seastar::async([] {
        sstring table_name = "collection_wide_update";
        auto set_of_ints_type = set_type_impl::get_instance(int32_type, true);
        // CREATE TABLE collection_wide_update (pk int, col set<int>, PRIMARY KEY (pk)) with compression = {'sstable_compression': ''};
        schema_builder builder(make_lw_shared(schema(generate_legacy_id("ks", table_name), "sst3", table_name,
            // partition key
            {{"pk", int32_type}},
            // clustering key
            {},
            // regular columns
            {{"col", set_of_ints_type}},
            // static columns
            {},
            // regular column name type
            utf8_type,
            // comment
            "SSTable 3.0 format write path - collection wide update test"
        )));
        builder.set_compressor_params(compression_parameters());
        schema_ptr s = builder.build(schema_builder::compact_storage::no);

        lw_shared_ptr<memtable> mt = make_lw_shared<memtable>(s);

        // INSERT INTO collection_wide_update (pk, col) VALUES (1, {2, 3});
        auto key = partition_key::from_deeply_exploded(*s, { 1 });
        mutation mut{s, key};

        mut.partition().apply_insert(*s, clustering_key::make_empty(), write_timestamp);
        set_type_impl::mutation set_values {
            {write_timestamp - 1, write_time_point}, // tombstone
            {
                {int32_type->decompose(2), atomic_cell::make_live(write_timestamp, bytes_view{})},
                {int32_type->decompose(3), atomic_cell::make_live(write_timestamp, bytes_view{})},
            }
        };

        mut.set_clustered_cell(clustering_key::make_empty(), *s->get_column_definition("col"), set_of_ints_type->serialize_mutation_form(set_values));
        mt->apply(std::move(mut));

        write_and_compare_sstables(s, mt, table_name);
    });
}

SEASTAR_TEST_CASE(test_write_collection_incremental_update) {
    return seastar::async([] {
        sstring table_name = "collection_incremental_update";
        auto set_of_ints_type = set_type_impl::get_instance(int32_type, true);
        // CREATE TABLE collection_incremental_update (pk int, col set<int>, PRIMARY KEY (pk)) with compression = {'sstable_compression': ''};
        schema_builder builder(make_lw_shared(schema(generate_legacy_id("ks", table_name), "sst3", table_name,
            // partition key
            {{"pk", int32_type}},
            // clustering key
            {},
            // regular columns
            {{"col", set_of_ints_type}},
            // static columns
            {},
            // regular column name type
            utf8_type,
            // comment
            "SSTable 3.0 format write path - collection incremental update test"
        )));
        builder.set_compressor_params(compression_parameters());
        schema_ptr s = builder.build(schema_builder::compact_storage::no);

        lw_shared_ptr<memtable> mt = make_lw_shared<memtable>(s);

        // UPDATE collection_incremental_update SET col = col + {2} WHERE pk = 1;
        auto key = partition_key::from_deeply_exploded(*s, { 1 });
        mutation mut{s, key};

        set_type_impl::mutation set_values {
            {}, // tombstone
            {
                {int32_type->decompose(2), atomic_cell::make_live(write_timestamp, bytes_view{})},
            }
        };

        mut.set_clustered_cell(clustering_key::make_empty(), *s->get_column_definition("col"), set_of_ints_type->serialize_mutation_form(set_values));
        mt->apply(std::move(mut));

        write_and_compare_sstables(s, mt, table_name);
    });
}

SEASTAR_TEST_CASE(test_write_multiple_partitions) {
    return seastar::async([] {
        sstring table_name = "multiple_partitions";
        // CREATE TABLE multiple_partitions (pk int, rc1 int, rc2 int, rc3 int, PRIMARY KEY (pk)) WITH compression = {'sstable_compression': ''};
        schema_builder builder(make_lw_shared(schema(generate_legacy_id("ks", table_name), "sst3", table_name,
            // partition key
            {{"pk", int32_type}},
            // clustering key
            {},
            // regular columns
            {{"rc1", int32_type}, {"rc2", int32_type}, {"rc3", int32_type}},
            // static columns
            {},
            // regular column name type
            utf8_type,
            // comment
            "SSTable 3.0 format write path - multiple partitions test"
        )));
        builder.set_compressor_params(compression_parameters());
        schema_ptr s = builder.build(schema_builder::compact_storage::no);

        lw_shared_ptr<memtable> mt = make_lw_shared<memtable>(s);

        api::timestamp_type ts = write_timestamp;
        // INSERT INTO multiple_partitions (pk, rc1) VALUES (1, 10);
        // INSERT INTO multiple_partitions (pk, rc2) VALUES (2, 20);
        // INSERT INTO multiple_partitions (pk, rc3) VALUES (3, 30);
        for (auto i : boost::irange(1, 4)) {
            auto key = partition_key::from_deeply_exploded(*s, {i});
            mutation mut{s, key};

            mut.set_cell(clustering_key::make_empty(), to_bytes(format("rc{}", i)), data_value{i * 10}, ts);
            mt->apply(std::move(mut));
            ts += 10;
        }

        write_and_compare_sstables(s, mt, table_name);
    });
}

SEASTAR_TEST_CASE(test_write_multiple_rows) {
    return seastar::async([] {
        sstring table_name = "multiple_rows";
        // CREATE TABLE multiple_rows (pk int, ck int, rc1 int, rc2 int, rc3 int, PRIMARY KEY (pk, ck)) WITH compression = {'sstable_compression': ''};
        schema_builder builder(make_lw_shared(schema(generate_legacy_id("ks", table_name), "sst3", table_name,
            // partition key
            {{"pk", int32_type}},
            // clustering key
            {{"ck", int32_type}},
            // regular columns
            {{"rc1", int32_type}, {"rc2", int32_type}, {"rc3", int32_type}},
            // static columns
            {},
            // regular column name type
            utf8_type,
            // comment
            "SSTable 3.0 format write path - multiple rows test"
        )));
        builder.set_compressor_params(compression_parameters());
        schema_ptr s = builder.build(schema_builder::compact_storage::no);

        lw_shared_ptr<memtable> mt = make_lw_shared<memtable>(s);

        auto key = partition_key::from_deeply_exploded(*s, {0});
        api::timestamp_type ts = write_timestamp;
        mutation mut{s, key};

        // INSERT INTO multiple_rows (pk, ck, rc1) VALUES (0, 1, 10);
        // INSERT INTO multiple_rows (pk, ck, rc2) VALUES (0, 2, 20);
        // INSERT INTO multiple_rows (pk, ck, rc3) VALUES (0, 3, 30);
        for (auto i : boost::irange(1, 4)) {
            clustering_key ckey = clustering_key::from_deeply_exploded(*s, { i });
            mut.partition().apply_insert(*s, ckey, ts);
            mut.set_cell(ckey, to_bytes(format("rc{}", i)), data_value{i * 10}, ts);
            ts += 10;
        }

        mt->apply(std::move(mut));
        write_and_compare_sstables(s, mt, table_name);
    });
}

// Information on missing columns is serialized differently when the number of columns is > 64.
// This test checks that this information is encoded correctly.
SEASTAR_TEST_CASE(test_write_missing_columns_large_set) {
    return seastar::async([] {
        sstring table_name = "missing_columns_large_set";
        // CREATE TABLE missing_columns_large_set (pk int, ck int, rc1 int, ..., rc64, PRIMARY KEY (pk, ck)) WITH compression = {'sstable_compression': ''};
        std::vector<schema::column> regular_columns;
        regular_columns.reserve(64);
        for (auto idx: boost::irange(1, 65)) {
            regular_columns.push_back({to_bytes(format("rc{}", idx)), int32_type});
        }
        schema_builder builder(make_lw_shared(schema(generate_legacy_id("ks", table_name), "sst3", table_name,
            // partition key
            {{"pk", int32_type}},
            // clustering key
            {{"ck", int32_type}},
            regular_columns,
            // static columns
            {},
            // regular column name type
            utf8_type,
            // comment
            "SSTable 3.0 format write path - missing columns large set test"
        )));
        builder.set_compressor_params(compression_parameters());
        schema_ptr s = builder.build(schema_builder::compact_storage::no);

        lw_shared_ptr<memtable> mt = make_lw_shared<memtable>(s);

        auto key = partition_key::from_deeply_exploded(*s, {0});
        api::timestamp_type ts = write_timestamp;
        mutation mut{s, key};

        // INSERT INTO missing_columns_large_set (pk, ck, rc1, ..., rc62) VALUES (0, 0, 1, ..., 62);
        // For missing columns, the missing ones will be written as majority are present.
        {
            clustering_key ckey = clustering_key::from_deeply_exploded(*s, {0});
            mut.partition().apply_insert(*s, ckey, ts);
            for (auto idx: boost::irange(1, 63)) {
                mut.set_cell(ckey, to_bytes(format("rc{}", idx)), data_value{idx}, ts);
            }
            mt->apply(std::move(mut));
        }
        ts += 10;
        // INSERT INTO missing_columns_large_set (pk, ck, rc63, rc64) VALUES (0, 1, 63, 64);
        // For missing columns, the present ones will be written as majority are missing.
        {
            clustering_key ckey = clustering_key::from_deeply_exploded(*s, {1});
            mut.partition().apply_insert(*s, ckey, ts);
            mut.set_cell(ckey, to_bytes(format("rc63", 63)), data_value{63}, ts);
            mut.set_cell(ckey, to_bytes(format("rc64", 63)), data_value{64}, ts);
            mt->apply(std::move(mut));
        }

        write_and_compare_sstables(s, mt, table_name);
    });
}

