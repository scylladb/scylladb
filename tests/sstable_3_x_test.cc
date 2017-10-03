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
#include "counters.hh"
#include "schema_builder.hh"
#include "sstable_test.hh"
#include "flat_mutation_reader_assertions.hh"
#include "sstable_test.hh"
#include "tests/test_services.hh"
#include "tests/tmpdir.hh"
#include "tests/sstable_utils.hh"
#include "sstables/types.hh"
#include "keys.hh"
#include "types.hh"

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

// Following tests run on files in tests/sstables/3.x/uncompressed/static_row
// They were created using following CQL statements:
//
// CREATE KEYSPACE test_ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
//
// CREATE TABLE test_ks.test_table ( pk INT, ck INT, s INT STATIC, val INT, PRIMARY KEY(pk, ck))
//      WITH compression = { 'enabled' : false };
//
// INSERT INTO test_ks.test_table(pk, ck, s, val) VALUES(1, 11, 101, 1001);
// INSERT INTO test_ks.test_table(pk, ck, s, val) VALUES(2, 12, 102, 1002);
// INSERT INTO test_ks.test_table(pk, ck, s, val) VALUES(3, 13, 103, 1003);
// INSERT INTO test_ks.test_table(pk, ck, s, val) VALUES(4, 14, 104, 1004);
// INSERT INTO test_ks.test_table(pk, ck, s, val) VALUES(5, 15, 105, 1005);

static thread_local const sstring UNCOMPRESSED_STATIC_ROW_PATH =
    "tests/sstables/3.x/uncompressed/static_row";
static thread_local const schema_ptr UNCOMPRESSED_STATIC_ROW_SCHEMA =
    schema_builder("test_ks", "test_table")
        .with_column("pk", int32_type, column_kind::partition_key)
        .with_column("s", int32_type, column_kind::static_column)
        .with_column("val", int32_type)
        .build();

SEASTAR_TEST_CASE(test_uncompressed_static_row_read) {
    return seastar::async([] {
        sstable_assertions sst(UNCOMPRESSED_STATIC_ROW_SCHEMA,
                               UNCOMPRESSED_STATIC_ROW_PATH);
        sst.load();
        auto to_key = [] (int key) {
            auto bytes = int32_type->decompose(int32_t(key));
            auto pk = partition_key::from_single_value(*UNCOMPRESSED_STATIC_ROW_SCHEMA, bytes);
            return dht::global_partitioner().decorate_key(*UNCOMPRESSED_STATIC_ROW_SCHEMA, pk);
        };

        auto s_cdef = UNCOMPRESSED_STATIC_ROW_SCHEMA->get_column_definition(to_bytes("s"));
        BOOST_REQUIRE(s_cdef);
        auto val_cdef = UNCOMPRESSED_STATIC_ROW_SCHEMA->get_column_definition(to_bytes("val"));
        BOOST_REQUIRE(val_cdef);

        assert_that(sst.read_rows_flat())
            .produces_partition_start(to_key(5))
            .produces_static_row({{s_cdef, int32_type->decompose(int32_t(105))}})
            .produces_row(clustering_key::from_single_value(*UNCOMPRESSED_STATIC_ROW_SCHEMA,
                                                            int32_type->decompose(15)),
                          {{val_cdef, int32_type->decompose(int32_t(1005))}})
            .produces_partition_end()
            .produces_partition_start(to_key(1))
            .produces_static_row({{s_cdef, int32_type->decompose(int32_t(101))}})
            .produces_row(clustering_key::from_single_value(*UNCOMPRESSED_STATIC_ROW_SCHEMA,
                                                            int32_type->decompose(11)),
                          {{val_cdef, int32_type->decompose(int32_t(1001))}})
            .produces_partition_end()
            .produces_partition_start(to_key(2))
            .produces_static_row({{s_cdef, int32_type->decompose(int32_t(102))}})
            .produces_row(clustering_key::from_single_value(*UNCOMPRESSED_STATIC_ROW_SCHEMA,
                                                            int32_type->decompose(12)),
                          {{val_cdef, int32_type->decompose(int32_t(1002))}})
            .produces_partition_end()
            .produces_partition_start(to_key(4))
            .produces_static_row({{s_cdef, int32_type->decompose(int32_t(104))}})
            .produces_row(clustering_key::from_single_value(*UNCOMPRESSED_STATIC_ROW_SCHEMA,
                                                            int32_type->decompose(14)),
                          {{val_cdef, int32_type->decompose(int32_t(1004))}})
            .produces_partition_end()
            .produces_partition_start(to_key(3))
            .produces_static_row({{s_cdef, int32_type->decompose(int32_t(103))}})
            .produces_row(clustering_key::from_single_value(*UNCOMPRESSED_STATIC_ROW_SCHEMA,
                                                            int32_type->decompose(13)),
                          {{val_cdef, int32_type->decompose(int32_t(1003))}})
            .produces_partition_end()
            .produces_end_of_stream();
    });
}

// Following tests run on files in tests/sstables/3.x/uncompressed/compound_static_row
// They were created using following CQL statements:
//
// CREATE KEYSPACE test_ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
//
// CREATE TABLE test_ks.test_table ( pk INT,
//                                   ck INT,
//                                   s_int INT STATIC,
//                                   s_text TEXT STATIC,
//                                   s_inet INET STATIC,
//                                   val INT,
//                                   PRIMARY KEY(pk, ck))
//      WITH compression = { 'enabled' : false };
//
// INSERT INTO test_ks.test_table(pk, ck, s_int, s_text, s_inet, val)
//                         VALUES(1, 11, 101, 'Text for 1', '10.0.0.1', 1001);
// INSERT INTO test_ks.test_table(pk, ck, s_int, s_text, s_inet, val)
//                         VALUES(2, 12, 102, 'Text for 2', '10.0.0.2', 1002);
// INSERT INTO test_ks.test_table(pk, ck, s_int, s_text, s_inet, val)
//                         VALUES(3, 13, 103, 'Text for 3', '10.0.0.3', 1003);
// INSERT INTO test_ks.test_table(pk, ck, s_int, s_text, s_inet, val)
//                         VALUES(4, 14, 104, 'Text for 4', '10.0.0.4', 1004);
// INSERT INTO test_ks.test_table(pk, ck, s_int, s_text, s_inet, val)
//                         VALUES(5, 15, 105, 'Text for 5', '10.0.0.5', 1005);

static thread_local const sstring UNCOMPRESSED_COMPOUND_STATIC_ROW_PATH =
    "tests/sstables/3.x/uncompressed/compound_static_row";
static thread_local const schema_ptr UNCOMPRESSED_COMPOUND_STATIC_ROW_SCHEMA =
    schema_builder("test_ks", "test_table")
        .with_column("pk", int32_type, column_kind::partition_key)
        .with_column("s_int", int32_type, column_kind::static_column)
        .with_column("s_text", utf8_type, column_kind::static_column)
        .with_column("s_inet", inet_addr_type, column_kind::static_column)
        .with_column("val", int32_type)
        .build();

SEASTAR_TEST_CASE(test_uncompressed_compound_static_row_read) {
    return seastar::async([] {
        sstable_assertions sst(UNCOMPRESSED_COMPOUND_STATIC_ROW_SCHEMA,
                               UNCOMPRESSED_COMPOUND_STATIC_ROW_PATH);
        sst.load();
        auto to_key = [] (int key) {
            auto bytes = int32_type->decompose(int32_t(key));
            auto pk = partition_key::from_single_value(*UNCOMPRESSED_COMPOUND_STATIC_ROW_SCHEMA, bytes);
            return dht::global_partitioner().decorate_key(*UNCOMPRESSED_COMPOUND_STATIC_ROW_SCHEMA, pk);
        };

        auto s_int_cdef = UNCOMPRESSED_COMPOUND_STATIC_ROW_SCHEMA->get_column_definition(to_bytes("s_int"));
        BOOST_REQUIRE(s_int_cdef);
        auto s_text_cdef = UNCOMPRESSED_COMPOUND_STATIC_ROW_SCHEMA->get_column_definition(to_bytes("s_text"));
        BOOST_REQUIRE(s_text_cdef);
        auto s_inet_cdef = UNCOMPRESSED_COMPOUND_STATIC_ROW_SCHEMA->get_column_definition(to_bytes("s_inet"));
        BOOST_REQUIRE(s_inet_cdef);
        auto val_cdef = UNCOMPRESSED_COMPOUND_STATIC_ROW_SCHEMA->get_column_definition(to_bytes("val"));
        BOOST_REQUIRE(val_cdef);

        auto generate = [&] (int int_val, sstring_view text_val, sstring_view inet_val) {
            std::vector<flat_reader_assertions::expected_column> columns;

            columns.push_back({s_int_cdef, int32_type->decompose(int_val)});
            columns.push_back({s_text_cdef, utf8_type->from_string(text_val)});
            columns.push_back({s_inet_cdef, inet_addr_type->from_string(inet_val)});

            return std::move(columns);
        };

        assert_that(sst.read_rows_flat())
            .produces_partition_start(to_key(5))
            .produces_static_row(generate(105, "Text for 5", "10.0.0.5"))
            .produces_row(clustering_key::from_single_value(*UNCOMPRESSED_STATIC_ROW_SCHEMA,
                                                            int32_type->decompose(15)),
                          {{val_cdef, int32_type->decompose(int32_t(1005))}})
            .produces_partition_end()
            .produces_partition_start(to_key(1))
            .produces_static_row(generate(101, "Text for 1", "10.0.0.1"))
            .produces_row(clustering_key::from_single_value(*UNCOMPRESSED_STATIC_ROW_SCHEMA,
                                                            int32_type->decompose(11)),
                          {{val_cdef, int32_type->decompose(int32_t(1001))}})
            .produces_partition_end()
            .produces_partition_start(to_key(2))
            .produces_static_row(generate(102, "Text for 2", "10.0.0.2"))
            .produces_row(clustering_key::from_single_value(*UNCOMPRESSED_STATIC_ROW_SCHEMA,
                                                            int32_type->decompose(12)),
                          {{val_cdef, int32_type->decompose(int32_t(1002))}})
            .produces_partition_end()
            .produces_partition_start(to_key(4))
            .produces_static_row(generate(104, "Text for 4", "10.0.0.4"))
            .produces_row(clustering_key::from_single_value(*UNCOMPRESSED_STATIC_ROW_SCHEMA,
                                                            int32_type->decompose(14)),
                          {{val_cdef, int32_type->decompose(int32_t(1004))}})
            .produces_partition_end()
            .produces_partition_start(to_key(3))
            .produces_static_row(generate(103, "Text for 3", "10.0.0.3"))
            .produces_row(clustering_key::from_single_value(*UNCOMPRESSED_STATIC_ROW_SCHEMA,
                                                            int32_type->decompose(13)),
                          {{val_cdef, int32_type->decompose(int32_t(1003))}})
            .produces_partition_end()
            .produces_end_of_stream();
    });
}

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
            .produces_row_with_key(clustering_key_prefix::make_empty())
            .produces_partition_end()
            .produces_partition_start(to_key(1))
            .produces_row_with_key(clustering_key_prefix::make_empty())
            .produces_partition_end()
            .produces_partition_start(to_key(2))
            .produces_row_with_key(clustering_key_prefix::make_empty())
            .produces_partition_end()
            .produces_partition_start(to_key(4))
            .produces_row_with_key(clustering_key_prefix::make_empty())
            .produces_partition_end()
            .produces_partition_start(to_key(3))
            .produces_row_with_key(clustering_key_prefix::make_empty())
            .produces_partition_end()
            .produces_end_of_stream();
    });
}

// Following tests run on files in tests/sstables/3.x/uncompressed/partition_key_with_value
// They were created using following CQL statements:
//
// CREATE KEYSPACE test_ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
//
// CREATE TABLE test_ks.test_table ( pk INT, val INT, PRIMARY KEY(pk))
//      WITH compression = { 'enabled' : false };
//
// INSERT INTO test_ks.test_table(pk, val) VALUES(1, 101);
// INSERT INTO test_ks.test_table(pk, val) VALUES(2, 102);
// INSERT INTO test_ks.test_table(pk, val) VALUES(3, 103);
// INSERT INTO test_ks.test_table(pk, val) VALUES(4, 104);
// INSERT INTO test_ks.test_table(pk, val) VALUES(5, 105);

static thread_local const sstring UNCOMPRESSED_PARTITION_KEY_WITH_VALUE_PATH =
    "tests/sstables/3.x/uncompressed/partition_key_with_value";
static thread_local const schema_ptr UNCOMPRESSED_PARTITION_KEY_WITH_VALUE_SCHEMA =
    schema_builder("test_ks", "test_table")
        .with_column("pk", int32_type, column_kind::partition_key)
        .with_column("val", int32_type)
        .build();

SEASTAR_TEST_CASE(test_uncompressed_partition_key_with_value_read) {
    return seastar::async([] {
        sstable_assertions sst(UNCOMPRESSED_PARTITION_KEY_WITH_VALUE_SCHEMA,
                               UNCOMPRESSED_PARTITION_KEY_WITH_VALUE_PATH);
        sst.load();
        auto to_key = [] (int key) {
            auto bytes = int32_type->decompose(int32_t(key));
            auto pk = partition_key::from_single_value(*UNCOMPRESSED_PARTITION_KEY_WITH_VALUE_SCHEMA, bytes);
            return dht::global_partitioner().decorate_key(*UNCOMPRESSED_PARTITION_KEY_WITH_VALUE_SCHEMA, pk);
        };

        auto cdef = UNCOMPRESSED_PARTITION_KEY_WITH_VALUE_SCHEMA->get_column_definition(to_bytes("val"));
        BOOST_REQUIRE(cdef);

        assert_that(sst.read_rows_flat())
            .produces_partition_start(to_key(5))
            .produces_row(clustering_key_prefix::make_empty(), {{cdef, int32_type->decompose(int32_t(105))}})
            .produces_partition_end()
            .produces_partition_start(to_key(1))
            .produces_row(clustering_key_prefix::make_empty(), {{cdef, int32_type->decompose(int32_t(101))}})
            .produces_partition_end()
            .produces_partition_start(to_key(2))
            .produces_row(clustering_key_prefix::make_empty(), {{cdef, int32_type->decompose(int32_t(102))}})
            .produces_partition_end()
            .produces_partition_start(to_key(4))
            .produces_row(clustering_key_prefix::make_empty(), {{cdef, int32_type->decompose(int32_t(104))}})
            .produces_partition_end()
            .produces_partition_start(to_key(3))
            .produces_row(clustering_key_prefix::make_empty(), {{cdef, int32_type->decompose(int32_t(103))}})
            .produces_partition_end()
            .produces_end_of_stream();
    });
}

// Following tests run on files in tests/sstables/3.x/uncompressed/partition_key_with_value_of_different_types
// They were created using following CQL statements:
//
// CREATE KEYSPACE test_ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
//
// CREATE TABLE test_ks.test_table ( pk INT,
//                                   bool_val BOOLEAN,
//                                   double_val DOUBLE,
//                                   float_val FLOAT,
//                                   int_val INT,
//                                   long_val BIGINT,
//                                   timestamp_val TIMESTAMP,
//                                   timeuuid_val TIMEUUID,
//                                   uuid_val UUID,
//                                   text_val TEXT,
//                                   PRIMARY KEY(pk))
//      WITH compression = { 'enabled' : false };
//
// INSERT INTO test_ks.test_table(pk, bool_val, double_val, float_val, int_val, long_val, timestamp_val, timeuuid_val,
//                                uuid_val, text_val)
//                         VALUES(1, true, 0.11, 0.1, 1, 11, '2015-05-01 09:30:54.234+0000',
//                                50554d6e-29bb-11e5-b345-feff819cdc9f, 01234567-0123-0123-0123-0123456789ab,
//                                'variable length text 1');
// INSERT INTO test_ks.test_table(pk, bool_val, double_val, float_val, int_val, long_val, timestamp_val, timeuuid_val,
//                                uuid_val, text_val)
//                         VALUES(2, false, 0.22, 0.2, 2, 22, '2015-05-02 10:30:54.234+0000',
//                                50554d6e-29bb-11e5-b345-feff819cdc9f, 01234567-0123-0123-0123-0123456789ab,
//                                'variable length text 2');
// INSERT INTO test_ks.test_table(pk, bool_val, double_val, float_val, int_val, long_val, timestamp_val, timeuuid_val,
//                                uuid_val, text_val)
//                         VALUES(3, true, 0.33, 0.3, 3, 33, '2015-05-03 11:30:54.234+0000',
//                                50554d6e-29bb-11e5-b345-feff819cdc9f, 01234567-0123-0123-0123-0123456789ab,
//                                'variable length text 3');
// INSERT INTO test_ks.test_table(pk, bool_val, double_val, float_val, int_val, long_val, timestamp_val, timeuuid_val,
//                                uuid_val, text_val)
//                         VALUES(4, false, 0.44, 0.4, 4, 44, '2015-05-04 12:30:54.234+0000',
//                                50554d6e-29bb-11e5-b345-feff819cdc9f, 01234567-0123-0123-0123-0123456789ab,
//                                'variable length text 4');
// INSERT INTO test_ks.test_table(pk, bool_val, double_val, float_val, int_val, long_val, timestamp_val, timeuuid_val,
//                                uuid_val, text_val)
//                         VALUES(5, true, 0.55, 0.5, 5, 55, '2015-05-05 13:30:54.234+0000',
//                                50554d6e-29bb-11e5-b345-feff819cdc9f, 01234567-0123-0123-0123-0123456789ab,
//                                'variable length text 5');

static thread_local const sstring UNCOMPRESSED_PARTITION_KEY_WITH_VALUES_OF_DIFFERENT_TYPES_PATH =
    "tests/sstables/3.x/uncompressed/partition_key_with_values_of_different_types";
static thread_local const schema_ptr UNCOMPRESSED_PARTITION_KEY_WITH_VALUES_OF_DIFFERENT_TYPES_SCHEMA =
    schema_builder("test_ks", "test_table")
        .with_column("pk", int32_type, column_kind::partition_key)
        .with_column("bool_val", boolean_type)
        .with_column("double_val", double_type)
        .with_column("float_val", float_type)
        .with_column("int_val", int32_type)
        .with_column("long_val", long_type)
        .with_column("timestamp_val", timestamp_type)
        .with_column("timeuuid_val", timeuuid_type)
        .with_column("uuid_val", uuid_type)
        .with_column("text_val", utf8_type)
        .build();

SEASTAR_TEST_CASE(test_uncompressed_partition_key_with_values_of_different_types_read) {
    return seastar::async([] {
        sstable_assertions sst(UNCOMPRESSED_PARTITION_KEY_WITH_VALUES_OF_DIFFERENT_TYPES_SCHEMA,
                               UNCOMPRESSED_PARTITION_KEY_WITH_VALUES_OF_DIFFERENT_TYPES_PATH);
        sst.load();
        auto to_key = [] (int key) {
            auto bytes = int32_type->decompose(int32_t(key));
            auto pk = partition_key::from_single_value(*UNCOMPRESSED_PARTITION_KEY_WITH_VALUES_OF_DIFFERENT_TYPES_SCHEMA, bytes);
            return dht::global_partitioner().decorate_key(*UNCOMPRESSED_PARTITION_KEY_WITH_VALUES_OF_DIFFERENT_TYPES_SCHEMA, pk);
        };

        auto bool_cdef =
            UNCOMPRESSED_PARTITION_KEY_WITH_VALUES_OF_DIFFERENT_TYPES_SCHEMA->get_column_definition(to_bytes("bool_val"));
        BOOST_REQUIRE(bool_cdef);
        auto double_cdef =
            UNCOMPRESSED_PARTITION_KEY_WITH_VALUES_OF_DIFFERENT_TYPES_SCHEMA->get_column_definition(to_bytes("double_val"));
        BOOST_REQUIRE(double_cdef);
        auto float_cdef =
            UNCOMPRESSED_PARTITION_KEY_WITH_VALUES_OF_DIFFERENT_TYPES_SCHEMA->get_column_definition(to_bytes("float_val"));
        BOOST_REQUIRE(float_cdef);
        auto int_cdef =
            UNCOMPRESSED_PARTITION_KEY_WITH_VALUES_OF_DIFFERENT_TYPES_SCHEMA->get_column_definition(to_bytes("int_val"));
        BOOST_REQUIRE(int_cdef);
        auto long_cdef =
            UNCOMPRESSED_PARTITION_KEY_WITH_VALUES_OF_DIFFERENT_TYPES_SCHEMA->get_column_definition(to_bytes("long_val"));
        BOOST_REQUIRE(long_cdef);
        auto timestamp_cdef =
            UNCOMPRESSED_PARTITION_KEY_WITH_VALUES_OF_DIFFERENT_TYPES_SCHEMA->get_column_definition(to_bytes("timestamp_val"));
        BOOST_REQUIRE(timestamp_cdef);
        auto timeuuid_cdef =
            UNCOMPRESSED_PARTITION_KEY_WITH_VALUES_OF_DIFFERENT_TYPES_SCHEMA->get_column_definition(to_bytes("timeuuid_val"));
        BOOST_REQUIRE(timeuuid_cdef);
        auto uuid_cdef =
            UNCOMPRESSED_PARTITION_KEY_WITH_VALUES_OF_DIFFERENT_TYPES_SCHEMA->get_column_definition(to_bytes("uuid_val"));
        BOOST_REQUIRE(uuid_cdef);
        auto text_cdef =
            UNCOMPRESSED_PARTITION_KEY_WITH_VALUES_OF_DIFFERENT_TYPES_SCHEMA->get_column_definition(to_bytes("text_val"));
        BOOST_REQUIRE(text_cdef);

        auto generate = [&] (bool bool_val, double double_val, float float_val, int int_val, long long_val,
                             sstring_view timestamp_val, sstring_view timeuuid_val, sstring_view uuid_val,
                             sstring_view text_val) {
            std::vector<flat_reader_assertions::expected_column> columns;

            columns.push_back({bool_cdef, boolean_type->decompose(bool_val)});
            columns.push_back({double_cdef, double_type->decompose(double_val)});
            columns.push_back({float_cdef, float_type->decompose(float_val)});
            columns.push_back({int_cdef, int32_type->decompose(int_val)});
            columns.push_back({long_cdef, long_type->decompose(long_val)});
            columns.push_back({timestamp_cdef, timestamp_type->from_string(timestamp_val)});
            columns.push_back({timeuuid_cdef, timeuuid_type->from_string(timeuuid_val)});
            columns.push_back({uuid_cdef, uuid_type->from_string(uuid_val)});
            columns.push_back({text_cdef, utf8_type->from_string(text_val)});

            return std::move(columns);
        };

        assert_that(sst.read_rows_flat())
            .produces_partition_start(to_key(5))
            .produces_row(clustering_key_prefix::make_empty(),
                          generate(true, 0.55, 0.5, 5, 55, "2015-05-05 13:30:54.234+0000",
                                   "50554d6e-29bb-11e5-b345-feff819cdc9f", "01234567-0123-0123-0123-0123456789ab",
                                   "variable length text 5"))
            .produces_partition_end()
            .produces_partition_start(to_key(1))
            .produces_row(clustering_key_prefix::make_empty(),
                          generate(true, 0.11, 0.1, 1, 11, "2015-05-01 09:30:54.234+0000",
                                   "50554d6e-29bb-11e5-b345-feff819cdc9f", "01234567-0123-0123-0123-0123456789ab",
                                   "variable length text 1"))
            .produces_partition_end()
            .produces_partition_start(to_key(2))
            .produces_row(clustering_key_prefix::make_empty(),
                          generate(false, 0.22, 0.2, 2, 22, "2015-05-02 10:30:54.234+0000",
                                   "50554d6e-29bb-11e5-b345-feff819cdc9f", "01234567-0123-0123-0123-0123456789ab",
                                   "variable length text 2"))
            .produces_partition_end()
            .produces_partition_start(to_key(4))
            .produces_row(clustering_key_prefix::make_empty(),
                          generate(false, 0.44, 0.4, 4, 44, "2015-05-04 12:30:54.234+0000",
                                   "50554d6e-29bb-11e5-b345-feff819cdc9f", "01234567-0123-0123-0123-0123456789ab",
                                   "variable length text 4"))
            .produces_partition_end()
            .produces_partition_start(to_key(3))
            .produces_row(clustering_key_prefix::make_empty(),
                          generate(true, 0.33, 0.3, 3, 33, "2015-05-03 11:30:54.234+0000",
                                   "50554d6e-29bb-11e5-b345-feff819cdc9f", "01234567-0123-0123-0123-0123456789ab",
                                   "variable length text 3"))
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

SEASTAR_TEST_CASE(test_uncompressed_simple_read) {
    return seastar::async([] {
        sstable_assertions sst(UNCOMPRESSED_SIMPLE_SCHEMA,
                               UNCOMPRESSED_SIMPLE_PATH);
        sst.load();
        auto to_key = [] (int key) {
            auto bytes = int32_type->decompose(int32_t(key));
            auto pk = partition_key::from_single_value(*UNCOMPRESSED_SIMPLE_SCHEMA, bytes);
            return dht::global_partitioner().decorate_key(*UNCOMPRESSED_SIMPLE_SCHEMA, pk);
        };

        auto int_cdef =
            UNCOMPRESSED_SIMPLE_SCHEMA->get_column_definition(to_bytes("val"));
        BOOST_REQUIRE(int_cdef);


        assert_that(sst.read_rows_flat())
            .produces_partition_start(to_key(5))
            .produces_row(clustering_key::from_single_value(*UNCOMPRESSED_SIMPLE_SCHEMA,
                                                            int32_type->decompose(105)),
                          {{int_cdef, int32_type->decompose(1005)}})
            .produces_partition_end()
            .produces_partition_start(to_key(1))
            .produces_row(clustering_key::from_single_value(*UNCOMPRESSED_SIMPLE_SCHEMA,
                                                            int32_type->decompose(101)),
                          {{int_cdef, int32_type->decompose(1001)}})
            .produces_partition_end()
            .produces_partition_start(to_key(2))
            .produces_row(clustering_key::from_single_value(*UNCOMPRESSED_SIMPLE_SCHEMA,
                                                            int32_type->decompose(102)),
                          {{int_cdef, int32_type->decompose(1002)}})
            .produces_partition_end()
            .produces_partition_start(to_key(4))
            .produces_row(clustering_key::from_single_value(*UNCOMPRESSED_SIMPLE_SCHEMA,
                                                            int32_type->decompose(104)),
                          {{int_cdef, int32_type->decompose(1004)}})
            .produces_partition_end()
            .produces_partition_start(to_key(3))
            .produces_row(clustering_key::from_single_value(*UNCOMPRESSED_SIMPLE_SCHEMA,
                                                            int32_type->decompose(103)),
                          {{int_cdef, int32_type->decompose(1003)}})
            .produces_partition_end()
            .produces_end_of_stream();
    });
}

// Following tests run on files in tests/sstables/3.x/uncompressed/simple
// They were created using following CQL statements:
//
// CREATE KEYSPACE test_ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
//
// CREATE TABLE test_ks.test_table ( pk INT,
//                                   ck_int INT,
//                                   ck_text TEXT,
//                                   ck_uuid UUID,
//                                   ck_inet INET,
//                                   val INT,
//                                   PRIMARY KEY(pk, ck_int, ck_text, ck_uuid, ck_inet))
//      WITH compression = { 'enabled' : false };
//
// INSERT INTO test_ks.test_table(pk, ck_int, ck_text, ck_uuid, ck_inet, val)
//        VALUES(1, 101, 'This is a string for 1', f7fdcbd2-4544-482c-85fd-d9572adc3cd6, '10.0.0.1', 1001);
// INSERT INTO test_ks.test_table(pk, ck_int, ck_text, ck_uuid, ck_inet, val)
//        VALUES(2, 102, 'This is a string for 2', c25ae960-07a2-467d-8f35-5bd38647b367, '10.0.0.2', 1002);
// INSERT INTO test_ks.test_table(pk, ck_int, ck_text, ck_uuid, ck_inet, val)
//        VALUES(3, 103, 'This is a string for 3', f7e8ebc0-dbae-4c06-bae0-656c23f6af6a, '10.0.0.3', 1003);
// INSERT INTO test_ks.test_table(pk, ck_int, ck_text, ck_uuid, ck_inet, val)
//        VALUES(4, 104, 'This is a string for 4', 4549e2c2-786e-4b30-90aa-5dd37ae1db8f, '10.0.0.4', 1004);
// INSERT INTO test_ks.test_table(pk, ck_int, ck_text, ck_uuid, ck_inet, val)
//        VALUES(5, 105, 'This is a string for 5',  f1badb6f-80a0-4eef-90df-b3651d9a5578, '10.0.0.5', 1005);

static thread_local const sstring UNCOMPRESSED_COMPOUND_CK_PATH = "tests/sstables/3.x/uncompressed/compound_ck";
static thread_local const schema_ptr UNCOMPRESSED_COMPOUND_CK_SCHEMA =
    schema_builder("test_ks", "test_table")
        .with_column("pk", int32_type, column_kind::partition_key)
        .with_column("ck_int", int32_type, column_kind::clustering_key)
        .with_column("ck_text", utf8_type, column_kind::clustering_key)
        .with_column("ck_uuid", uuid_type, column_kind::clustering_key)
        .with_column("ck_inet", inet_addr_type, column_kind::clustering_key)
        .with_column("val", int32_type)
        .build();

SEASTAR_TEST_CASE(test_uncompressed_compound_ck_read) {
    return seastar::async([] {
        sstable_assertions sst(UNCOMPRESSED_COMPOUND_CK_SCHEMA,
                               UNCOMPRESSED_COMPOUND_CK_PATH);
        sst.load();
        auto to_key = [] (int key) {
            auto bytes = int32_type->decompose(int32_t(key));
            auto pk = partition_key::from_single_value(*UNCOMPRESSED_COMPOUND_CK_SCHEMA, bytes);
            return dht::global_partitioner().decorate_key(*UNCOMPRESSED_COMPOUND_CK_SCHEMA, pk);
        };

        auto int_cdef =
            UNCOMPRESSED_SIMPLE_SCHEMA->get_column_definition(to_bytes("val"));
        BOOST_REQUIRE(int_cdef);


        assert_that(sst.read_rows_flat())
            .produces_partition_start(to_key(5))
            .produces_row(clustering_key::from_exploded(*UNCOMPRESSED_SIMPLE_SCHEMA, {
                              int32_type->decompose(105),
                              utf8_type->from_string("This is a string for 5"),
                              uuid_type->from_string("f1badb6f-80a0-4eef-90df-b3651d9a5578"),
                              inet_addr_type->from_string("10.0.0.5")
                          }),
                          {{int_cdef, int32_type->decompose(1005)}})
            .produces_partition_end()
            .produces_partition_start(to_key(1))
            .produces_row(clustering_key::from_exploded(*UNCOMPRESSED_SIMPLE_SCHEMA, {
                              int32_type->decompose(101),
                              utf8_type->from_string("This is a string for 1"),
                              uuid_type->from_string("f7fdcbd2-4544-482c-85fd-d9572adc3cd6"),
                              inet_addr_type->from_string("10.0.0.1")
                          }),
                          {{int_cdef, int32_type->decompose(1001)}})
            .produces_partition_end()
            .produces_partition_start(to_key(2))
            .produces_row(clustering_key::from_exploded(*UNCOMPRESSED_SIMPLE_SCHEMA, {
                              int32_type->decompose(102),
                              utf8_type->from_string("This is a string for 2"),
                              uuid_type->from_string("c25ae960-07a2-467d-8f35-5bd38647b367"),
                              inet_addr_type->from_string("10.0.0.2")
                          }),
                          {{int_cdef, int32_type->decompose(1002)}})
            .produces_partition_end()
            .produces_partition_start(to_key(4))
            .produces_row(clustering_key::from_exploded(*UNCOMPRESSED_SIMPLE_SCHEMA, {
                              int32_type->decompose(104),
                              utf8_type->from_string("This is a string for 4"),
                              uuid_type->from_string("4549e2c2-786e-4b30-90aa-5dd37ae1db8f"),
                              inet_addr_type->from_string("10.0.0.4")
                          }),
                          {{int_cdef, int32_type->decompose(1004)}})
            .produces_partition_end()
            .produces_partition_start(to_key(3))
            .produces_row(clustering_key::from_exploded(*UNCOMPRESSED_SIMPLE_SCHEMA, {
                              int32_type->decompose(103),
                              utf8_type->from_string("This is a string for 3"),
                              uuid_type->from_string("f7e8ebc0-dbae-4c06-bae0-656c23f6af6a"),
                              inet_addr_type->from_string("10.0.0.3")
                          }),
                          {{int_cdef, int32_type->decompose(1003)}})
            .produces_partition_end()
            .produces_end_of_stream();
    });
}

static void compare_files(sstring filename1, sstring filename2) {
    std::ifstream ifs1(filename1);
    std::ifstream ifs2(filename2);

    std::istream_iterator<char> b1(ifs1), e1;
    std::istream_iterator<char> b2(ifs2), e2;
    BOOST_CHECK_EQUAL_COLLECTIONS(b1, e1, b2, e2);
}

static void write_and_compare_sstables(schema_ptr s, lw_shared_ptr<memtable> mt, sstring table_name, bool compressed = false) {
    storage_service_for_tests ssft;
    tmpdir tmp;
    auto sst = sstables::test::make_test_sstable(4096, s, tmp.path, 1, sstables::sstable_version_types::mc, sstable::format_types::big);
    write_memtable_to_sstable_for_test(*mt, sst).get();

    for (auto file_type : {component_type::Data,
                           component_type::Index,
                           component_type::Statistics,
                           component_type::Digest,
                           component_type::Filter}) {
        auto orig_filename =
                sstable::filename(format("tests/sstables/3.x/{}compressed/write_{}", compressed ? "" : "un",table_name),
                                  "ks", table_name, sstables::sstable_version_types::mc, 1, big, file_type);
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
        // CREATE TABLE static_row (pk text, ck int, st1 int static, st2 text static, PRIMARY KEY (pk, ck)) WITH compression = {'sstable_compression': ''};
        schema_builder builder("sst3", table_name);
        builder.with_column("pk", utf8_type, column_kind::partition_key);
        builder.with_column("ck", int32_type, column_kind::clustering_key);
        builder.with_column("st1", int32_type, column_kind::static_column);
        builder.with_column("st2", utf8_type, column_kind::static_column);
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
        schema_builder builder("sst3", table_name);
        builder.with_column("a", int32_type, column_kind::partition_key);
        builder.with_column("b", utf8_type, column_kind::partition_key);
        builder.with_column("c", boolean_type, column_kind::partition_key);
        builder.with_column("d", int32_type, column_kind::clustering_key);
        builder.with_column("e", utf8_type, column_kind::clustering_key);
        builder.with_column("f", int32_type);
        builder.with_column("g", utf8_type);
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
        schema_builder builder("sst3", table_name);
        builder.with_column("a", int32_type, column_kind::partition_key);
        builder.with_column("b", utf8_type, column_kind::clustering_key);
        builder.with_column("c", int32_type, column_kind::clustering_key);
        builder.with_column("d", utf8_type, column_kind::clustering_key);
        builder.with_column("e", int32_type);
        builder.with_column("f", utf8_type);
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
        schema_builder builder("sst3", table_name);
        builder.with_column("pk", utf8_type, column_kind::partition_key);
        builder.with_column("ck", utf8_type, column_kind::clustering_key);
        builder.with_column("st", utf8_type, column_kind::static_column);
        builder.with_column("rc", utf8_type);
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
        // CREATE TABLE ttled_row (pk int, ck int, rc int, PRIMARY KEY (pk)) WITH compression = {'sstable_compression': ''};
        schema_builder builder("sst3", table_name);
        builder.with_column("pk", int32_type, column_kind::partition_key);
        builder.with_column("ck", int32_type, column_kind::clustering_key);
        builder.with_column("rc", int32_type);
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
        // CREATE TABLE ttled_column (pk text, rc int, PRIMARY KEY (pk)) WITH compression = {'sstable_compression': ''};
        schema_builder builder("sst3", table_name);
        builder.with_column("pk", utf8_type, column_kind::partition_key);
        builder.with_column("rc", int32_type);
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
        auto cell = atomic_cell::make_live(*column_def->type, write_timestamp, value, write_time_point + ttl, ttl);
        mut.set_clustered_cell(clustering_key::make_empty(), *column_def, std::move(cell));
        mt->apply(std::move(mut));

        write_and_compare_sstables(s, mt, table_name);
    });
}

SEASTAR_TEST_CASE(test_write_deleted_column) {
    return seastar::async([] {
        sstring table_name = "deleted_column";
        // CREATE TABLE deleted_column (pk int, rc int, PRIMARY KEY (pk)) WITH compression = {'sstable_compression': ''};
        schema_builder builder("sst3", table_name);
        builder.with_column("pk", int32_type, column_kind::partition_key);
        builder.with_column("rc", int32_type);
        builder.set_compressor_params(compression_parameters());
        schema_ptr s = builder.build(schema_builder::compact_storage::no);

        lw_shared_ptr<memtable> mt = make_lw_shared<memtable>(s);

        // DELETE rc FROM deleted_column WHERE pk=1;
        auto key = partition_key::from_deeply_exploded(*s, { 1 });
        mutation mut{s, key};
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
        // CREATE TABLE deleted_row (pk int, ck int, PRIMARY KEY (pk, ck)) WITH compression = {'sstable_compression': ''};
        schema_builder builder("sst3", table_name);
        builder.with_column("pk", int32_type, column_kind::partition_key);
        builder.with_column("ck", int32_type, column_kind::clustering_key);
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
        schema_builder builder("sst3", table_name);
        builder.with_column("pk", int32_type, column_kind::partition_key);
        builder.with_column("col", set_of_ints_type);
        builder.set_compressor_params(compression_parameters());
        schema_ptr s = builder.build(schema_builder::compact_storage::no);

        lw_shared_ptr<memtable> mt = make_lw_shared<memtable>(s);

        // INSERT INTO collection_wide_update (pk, col) VALUES (1, {2, 3});
        auto key = partition_key::from_deeply_exploded(*s, { 1 });
        mutation mut{s, key};

        mut.partition().apply_insert(*s, clustering_key::make_empty(), write_timestamp);
        set_type_impl::mutation set_values;
        set_values.tomb = tombstone {write_timestamp - 1, write_time_point};
        set_values.cells.emplace_back(int32_type->decompose(2), atomic_cell::make_live(*bytes_type, write_timestamp, bytes_view{}));
        set_values.cells.emplace_back(int32_type->decompose(3), atomic_cell::make_live(*bytes_type, write_timestamp, bytes_view{}));

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
        schema_builder builder("sst3", table_name);
        builder.with_column("pk", int32_type, column_kind::partition_key);
        builder.with_column("col", set_of_ints_type);
        builder.set_compressor_params(compression_parameters());
        schema_ptr s = builder.build(schema_builder::compact_storage::no);

        lw_shared_ptr<memtable> mt = make_lw_shared<memtable>(s);

        // UPDATE collection_incremental_update SET col = col + {2} WHERE pk = 1;
        auto key = partition_key::from_deeply_exploded(*s, { 1 });
        mutation mut{s, key};

        set_type_impl::mutation set_values;
        set_values.cells.emplace_back(int32_type->decompose(2), atomic_cell::make_live(*bytes_type, write_timestamp, bytes_view{}));

        mut.set_clustered_cell(clustering_key::make_empty(), *s->get_column_definition("col"), set_of_ints_type->serialize_mutation_form(set_values));
        mt->apply(std::move(mut));

        write_and_compare_sstables(s, mt, table_name);
    });
}

SEASTAR_TEST_CASE(test_write_multiple_partitions) {
    return seastar::async([] {
        sstring table_name = "multiple_partitions";
        // CREATE TABLE multiple_partitions (pk int, rc1 int, rc2 int, rc3 int, PRIMARY KEY (pk)) WITH compression = {'sstable_compression': ''};
        schema_builder builder("sst3", table_name);
        builder.with_column("pk", int32_type, column_kind::partition_key);
        builder.with_column("rc1", int32_type);
        builder.with_column("rc2", int32_type);
        builder.with_column("rc3", int32_type);
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

static future<> test_write_many_partitions(sstring table_name, tombstone partition_tomb, compression_parameters cp) {
    return seastar::async([table_name, partition_tomb, cp] {
        // CREATE TABLE <table_name> (pk int, PRIMARY KEY (pk)) WITH compression = {'sstable_compression': ''};
        schema_builder builder("sst3", table_name);
        builder.with_column("pk", int32_type, column_kind::partition_key);
        builder.set_compressor_params(cp);
        schema_ptr s = builder.build(schema_builder::compact_storage::no);

        lw_shared_ptr<memtable> mt = make_lw_shared<memtable>(s);

        for (auto i : boost::irange(0, 65536)) {
            auto key = partition_key::from_deeply_exploded(*s, {i});
            mutation mut{s, key};
            if (partition_tomb) {
                mut.partition().apply(partition_tomb);
            }
            mt->apply(std::move(mut));
        }

        write_and_compare_sstables(s, mt, table_name, cp != compression_parameters{});
    });
}

SEASTAR_TEST_CASE(test_write_many_live_partitions) {
    return test_write_many_partitions(
            "many_live_partitions",
            tombstone{},
            compression_parameters{});
}

SEASTAR_TEST_CASE(test_write_many_deleted_partitions) {
    return test_write_many_partitions(
            "many_deleted_partitions",
            tombstone{write_timestamp, write_time_point},
            compression_parameters{});
}

SEASTAR_TEST_CASE(test_write_many_partitions_lz4) {
    return test_write_many_partitions(
            "many_partitions_lz4",
            tombstone{},
            compression_parameters{compressor::lz4});
}

SEASTAR_TEST_CASE(test_write_many_partitions_snappy) {
    return test_write_many_partitions(
            "many_partitions_snappy",
            tombstone{},
            compression_parameters{compressor::snappy});
}

SEASTAR_TEST_CASE(test_write_many_partitions_deflate) {
    return test_write_many_partitions(
            "many_partitions_deflate",
            tombstone{},
            compression_parameters{compressor::deflate});
}

SEASTAR_TEST_CASE(test_write_multiple_rows) {
    return seastar::async([] {
        sstring table_name = "multiple_rows";
        // CREATE TABLE multiple_rows (pk int, ck int, rc1 int, rc2 int, rc3 int, PRIMARY KEY (pk, ck)) WITH compression = {'sstable_compression': ''};
        schema_builder builder("sst3", table_name);
        builder.with_column("pk", int32_type, column_kind::partition_key);
        builder.with_column("ck", int32_type, column_kind::clustering_key);
        builder.with_column("rc1", int32_type);
        builder.with_column("rc2", int32_type);
        builder.with_column("rc3", int32_type);
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
        schema_builder builder("sst3", table_name);
        builder.with_column("pk", int32_type, column_kind::partition_key);
        builder.with_column("ck", int32_type, column_kind::clustering_key);
        for (auto idx: boost::irange(1, 65)) {
            builder.with_column(to_bytes(format("rc{}", idx)), int32_type);
        }
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

SEASTAR_TEST_CASE(test_write_counter_table) {
    return seastar::async([] {
        sstring table_name = "counter_table";
        // CREATE TABLE counter_table (pk text, ck text, rc1 counter, rc2 counter, PRIMARY KEY (pk, ck)) WITH compression = {'sstable_compression': ''};
        schema_builder builder("sst3", table_name);
        builder.with_column("pk", utf8_type, column_kind::partition_key);
        builder.with_column("ck", utf8_type, column_kind::clustering_key);
        builder.with_column("rc1", counter_type);
        builder.with_column("rc2", counter_type);
        builder.set_compressor_params(compression_parameters());
        schema_ptr s = builder.build(schema_builder::compact_storage::no);

        lw_shared_ptr<memtable> mt = make_lw_shared<memtable>(s);
        auto key = partition_key::from_exploded(*s, {to_bytes("key")});
        mutation mut{s, key};

        // Keep counter ids fixed to produce the same binary output on each run
        std::vector<counter_id> ids = {
            counter_id{utils::UUID{"19478feb-8e7c-4729-9a78-9b476552f13d"}},
            counter_id{utils::UUID{"e079a6fe-eb79-4bdf-97ca-c87a2c387d5c"}},
            counter_id{utils::UUID{"bbba5897-78b6-4cdc-9a0d-ea9e9a3b833f"}},
        };
        boost::range::sort(ids);

        const column_definition& cdef1 = *s->get_column_definition("rc1");
        const column_definition& cdef2 = *s->get_column_definition("rc2");

        auto ckey1 = clustering_key::from_exploded(*s, {to_bytes("ck1")});

        counter_cell_builder b1;
        b1.add_shard(counter_shard(ids[0], 5, 1));
        b1.add_shard(counter_shard(ids[1], -4, 1));
        b1.add_shard(counter_shard(ids[2], 9, 1));
        mut.set_clustered_cell(ckey1, cdef1, b1.build(write_timestamp));

        counter_cell_builder b2;
        b2.add_shard(counter_shard(ids[1], -1, 1));
        b2.add_shard(counter_shard(ids[2], 2, 1));
        mut.set_clustered_cell(ckey1, cdef2, b2.build(write_timestamp));

        auto ckey2 = clustering_key::from_exploded(*s, {to_bytes("ck2")});
        mut.set_clustered_cell(ckey2, cdef1, atomic_cell::make_dead(write_timestamp, write_time_point));

        mt->apply(mut);

        write_and_compare_sstables(s, mt, table_name);
    });
}

SEASTAR_TEST_CASE(test_write_different_types) {
    return seastar::async([] {
        sstring table_name = "different_types";
        // CREATE TABLE different_types (pk text, asciival ascii, bigintval bigint,
        // blobval blob, boolval boolean, dateval date, decimalval decimal,
        // doubleval double, floatval float, inetval inet, intval int,
        // smallintval smallint, timeval time, tsval timestamp, timeuuidval timeuuid,
        // tinyintval tinyint,  uuidval uuid, varcharval varchar, varintval varint,
        // durationval duration, PRIMARY KEY (pk)) WITH compression = {'sstable_compression': ''};
        schema_builder builder("sst3", table_name);
        builder.with_column("pk", utf8_type, column_kind::partition_key);
        builder.with_column("asciival", ascii_type);
        builder.with_column("bigintval", long_type);
        builder.with_column("blobval", bytes_type);
        builder.with_column("boolval", boolean_type);
        builder.with_column("dateval", simple_date_type);
        builder.with_column("decimalval", decimal_type);
        builder.with_column("doubleval", double_type);
        builder.with_column("floatval", float_type);
        builder.with_column("inetval", inet_addr_type);
        builder.with_column("intval", int32_type);
        builder.with_column("smallintval", short_type);
        builder.with_column("timeval", time_type);
        builder.with_column("tsval", timestamp_type);
        builder.with_column("timeuuidval", timeuuid_type);
        builder.with_column("tinyintval", byte_type);
        builder.with_column("uuidval", uuid_type);
        builder.with_column("varcharval", utf8_type);
        builder.with_column("varintval", varint_type);
        builder.with_column("durationval", duration_type);
        builder.set_compressor_params(compression_parameters());
        schema_ptr s = builder.build(schema_builder::compact_storage::no);

        lw_shared_ptr<memtable> mt = make_lw_shared<memtable>(s);

        // INSERT INTO different_types (pk, asciival, bigintval, blobval, boolval,
        // dateval, decimalval, doubleval, floatval, inetval, intval, smallintval,
        // timeval, tsval, timeuuidval, tinyintval, uuidval, varcharval, varintval,
        // durationval) VALUES ('key', 'hello', 9223372036854775807,
        // textAsBlob('great'), true, '2017-05-05',
        // 5.45, 36.6, 7.62, '192.168.0.110', -2147483648, 32767, '19:45:05.090',
        // '2015-05-01 09:30:54.234+0000', 50554d6e-29bb-11e5-b345-feff819cdc9f, 127,
        // 01234567-0123-0123-0123-0123456789ab, 'привет', 123, 1h4m48s20ms);
        auto key = make_dkey(s, {to_bytes("key")});
        mutation mut{s, key};
        clustering_key ckey = clustering_key::make_empty();
        mut.partition().apply_insert(*s, ckey, write_timestamp);
        mut.set_cell(ckey, "asciival", data_value{"hello"}, write_timestamp);
        mut.set_cell(ckey, "bigintval", data_value{std::numeric_limits<int64_t>::max()}, write_timestamp);
        mut.set_cell(ckey, "blobval", data_value{bytes{'g', 'r', 'e', 'a', 't'}}, write_timestamp);
        mut.set_cell(ckey, "boolval", data_value{true}, write_timestamp);
        mut.set_cell(ckey, "dateval", simple_date_type->deserialize(simple_date_type->from_string("2017-05-05")), write_timestamp);
        mut.set_cell(ckey, "decimalval", decimal_type->deserialize(decimal_type->from_string("5.45")), write_timestamp);
        mut.set_cell(ckey, "doubleval", data_value{36.6}, write_timestamp);
        mut.set_cell(ckey, "floatval", data_value{7.62f}, write_timestamp);
        mut.set_cell(ckey, "inetval", inet_addr_type->deserialize(inet_addr_type->from_string("192.168.0.110")), write_timestamp);
        mut.set_cell(ckey, "intval", data_value{std::numeric_limits<int32_t>::min()}, write_timestamp);
        mut.set_cell(ckey, "smallintval", data_value{int16_t(32767)}, write_timestamp);
        mut.set_cell(ckey, "timeval", time_type->deserialize(time_type->from_string("19:45:05.090")), write_timestamp);
        mut.set_cell(ckey, "tsval", timestamp_type->deserialize(timestamp_type->from_string("2015-05-01 09:30:54.234+0000")), write_timestamp);
        mut.set_cell(ckey, "timeuuidval", timeuuid_type->deserialize(timeuuid_type->from_string("50554d6e-29bb-11e5-b345-feff819cdc9f")), write_timestamp);
        mut.set_cell(ckey, "tinyintval", data_value{int8_t{127}}, write_timestamp);
        mut.set_cell(ckey, "uuidval", data_value{utils::UUID(sstring("01234567-0123-0123-0123-0123456789ab"))}, write_timestamp);
        mut.set_cell(ckey, "varcharval", data_value{"привет"}, write_timestamp);
        mut.set_cell(ckey, "varintval", varint_type->deserialize(varint_type->from_string("123")), write_timestamp);
        mut.set_cell(ckey, "durationval", duration_type->deserialize(duration_type->from_string("1h4m48s20ms")), write_timestamp);
        mt->apply(std::move(mut));

        write_and_compare_sstables(s, mt, table_name);
    });
}

