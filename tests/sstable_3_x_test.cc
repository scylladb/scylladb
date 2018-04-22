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

#include <boost/test/unit_test.hpp>

#include <seastar/core/thread.hh>

#include "sstables/sstables.hh"
#include "compress.hh"
#include "schema_builder.hh"
#include "tests/test-utils.hh"

using namespace sstables;

class sstable_assertions final {
    shared_sstable _sst;
public:
    sstable_assertions(schema_ptr schema, const sstring& path, int generation = 1)
        : _sst(make_sstable(std::move(schema),
                            path,
                            generation,
                            sstable_version_types::mc,
                            sstable_format_types::big))
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
