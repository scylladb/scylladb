/*
 * Copyright (C) 2018-present ScyllaDB
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

#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/closeable.hh>

#include "test/boost/sstable_test.hh"
#include "test/lib/exception_utils.hh"
#include "test/lib/schema_registry.hh"

using namespace sstables;

namespace {
struct my_consumer {
    stop_iteration consume(static_row sr) { return stop_iteration::no; }
    stop_iteration consume(clustering_row cr) { return stop_iteration::no; }
    stop_iteration consume(range_tombstone_change rt) { return stop_iteration::no; }
    stop_iteration consume(tombstone tomb) { return stop_iteration::no; }
    void consume_end_of_stream() {}
    void consume_new_partition(const dht::decorated_key& dk) {}
    stop_iteration consume_end_of_partition() { return stop_iteration::no; }
};
}

static future<> broken_sst(sstring dir, unsigned long generation, schema_ptr s, sstring msg, std::optional<sstring> sst_name,
    sstable_version_types version = la) {
  return sstables::test_env::do_with_async([=] (sstables::test_env& env) {
    try {
        sstable_ptr sstp = env.reusable_sst(s, dir, generation, version).get0();
        auto r = sstp->make_reader(s, env.make_reader_permit(), query::full_partition_range, s->full_slice());
        auto close_r = deferred_close(r);
        r.consume(my_consumer{}).get();
        BOOST_FAIL("expecting exception");
    } catch (malformed_sstable_exception& e) {
        auto ex_what = sstring(e.what());
        BOOST_REQUIRE(ex_what.find(msg) != sstring::npos);
        if (sst_name) {
            BOOST_REQUIRE(ex_what.find(*sst_name) != sstring::npos);
        }
    }
  });
}

static future<> broken_sst(sstring dir, unsigned long generation, schema_registry& registry, sstring msg, std::optional<sstring> sst_name = std::nullopt) {
    // Using an empty schema for this function, which is only about loading
    // a malformed component and checking that it fails.
    auto s = make_shared_schema(registry, {}, "ks", "cf", {}, {}, {}, {}, utf8_type);
    return broken_sst(dir, generation, s, msg, sst_name);
}

SEASTAR_TEST_CASE(test_empty_index) {
  return sstables::test_env::do_with_async([&] (sstables::test_env& env) {
    auto s = schema_builder(env.registry(), "test_ks", "test_table")
                 .with_column("pk", int32_type, column_kind::partition_key)
                 .with_column("ck", int32_type, column_kind::clustering_key)
                 .with_column("val", int32_type)
                 .set_compressor_params(compression_parameters::no_compression())
                 .build();
    sstable_ptr sstp = env.reusable_sst(s, "test/resource/sstables/empty_index", 36, sstable_version_types::mc).get0();
    auto fut = sstables::test(sstp).read_indexes(env.make_reader_permit());
    BOOST_REQUIRE_EXCEPTION(fut.get(), malformed_sstable_exception, exception_predicate::message_equals(
        "missing index entry in sstable test/resource/sstables/empty_index/mc-36-big-Index.db"));
  });
}

SEASTAR_TEST_CASE(missing_column_in_schema) {
    tests::schema_registry_wrapper registry;
    schema_ptr s = schema_builder(registry, "test_ks", "test_table")
                       .with_column("key1", utf8_type, column_kind::partition_key)
                       .with_column("key2", utf8_type, column_kind::clustering_key)
                       .with_column("key3", utf8_type, column_kind::clustering_key)
                       .build(schema_builder::compact_storage::no);
    return broken_sst("test/resource/sstables/incompatible_serialized_type", 122, s,
        "Column val missing in current schema",
        "test/resource/sstables/incompatible_serialized_type/mc-122-big-Data.db",
        sstable::version_types::mc);
}

SEASTAR_TEST_CASE(incompatible_serialized_type) {
    tests::schema_registry_wrapper registry;
    schema_ptr s = schema_builder(registry, "test_ks", "test_table")
                       .with_column("key1", utf8_type, column_kind::partition_key)
                       .with_column("key2", utf8_type, column_kind::clustering_key)
                       .with_column("key3", utf8_type, column_kind::clustering_key)
                       .with_column("val", int32_type, column_kind::regular_column)
                       .build(schema_builder::compact_storage::no);
    return broken_sst("test/resource/sstables/incompatible_serialized_type", 122, s,
        "val definition in serialization header does not match schema. Expected "
        "org.apache.cassandra.db.marshal.Int32Type but got "
        "org.apache.cassandra.db.marshal.UTF8Type",
        "test/resource/sstables/incompatible_serialized_type/mc-122-big-Data.db",
        sstable::version_types::mc);
}

SEASTAR_TEST_CASE(invalid_boundary) {
    tests::schema_registry_wrapper registry;
    schema_ptr s = schema_builder(registry, "test_ks", "test_t")
                       .with_column("p", int32_type, column_kind::partition_key)
                       .with_column("a", int32_type, column_kind::clustering_key)
                       .with_column("b", int32_type, column_kind::clustering_key)
                       .with_column("c", int32_type, column_kind::clustering_key)
                       .with_column("r", int32_type, column_kind::regular_column)
                       .build(schema_builder::compact_storage::no);
    return broken_sst("test/resource/sstables/invalid_boundary", 33, s,
        "Corrupted range tombstone: invalid boundary type static_clustering",
        "test/resource/sstables/invalid_boundary/mc-33-big-Data.db",
        sstable::version_types::mc);
}

SEASTAR_TEST_CASE(mismatched_timestamp) {
    tests::schema_registry_wrapper registry;
    schema_ptr s = schema_builder(registry, "test_ks", "test_table")
                       .with_column("key1", utf8_type, column_kind::partition_key)
                       .with_column("key2", utf8_type, column_kind::clustering_key)
                       .with_column("key3", utf8_type, column_kind::clustering_key)
                       .with_column("val", utf8_type, column_kind::regular_column)
                       .build(schema_builder::compact_storage::no);
    return broken_sst("test/resource/sstables/mismatched_timestamp", 122, s,
        "Range tombstone with ck ckp{00056b65793262} and two different tombstones at ends: "
        "{tombstone: timestamp=1544745393692803, deletion_time=1544745393}, {tombstone: "
        "timestamp=1446576446577440, deletion_time=1442880998}",
        "test/resource/sstables/mismatched_timestamp/mc-122-big-Data.db",
        sstable::version_types::mc);
}

SEASTAR_TEST_CASE(broken_open_tombstone) {
    tests::schema_registry_wrapper registry;
    schema_ptr s = schema_builder(registry, "test_ks", "test_table")
                       .with_column("key1", utf8_type, column_kind::partition_key)
                       .with_column("key2", utf8_type, column_kind::clustering_key)
                       .with_column("key3", utf8_type, column_kind::clustering_key)
                       .with_column("val", utf8_type, column_kind::regular_column)
                       .build(schema_builder::compact_storage::no);
    return broken_sst("test/resource/sstables/broken_open_tombstone", 122, s,
        "Range tombstones have to be disjoint: current opened range tombstone "
        "{tombstone: timestamp=1544745393692803, deletion_time=1544745393}, "
        "new tombstone {tombstone: timestamp=1544745393692803, "
        "deletion_time=1544745393}",
        "test/resource/sstables/broken_open_tombstone/mc-122-big-Data.db",
        sstable::version_types::mc);
}

SEASTAR_TEST_CASE(broken_close_tombstone) {
    tests::schema_registry_wrapper registry;
    schema_ptr s = schema_builder(registry, "test_ks", "test_table")
                       .with_column("key1", utf8_type, column_kind::partition_key)
                       .with_column("key2", utf8_type, column_kind::clustering_key)
                       .with_column("key3", utf8_type, column_kind::clustering_key)
                       .with_column("val", utf8_type, column_kind::regular_column)
                       .build(schema_builder::compact_storage::no);
    return broken_sst("test/resource/sstables/broken_close_tombstone", 122, s,
        "Closing range tombstone that wasn't opened: clustering ckp{00056b65793262}, kind incl "
        "end, tombstone {tombstone: timestamp=1544745393692803, deletion_time=1544745393}",
        "test/resource/sstables/broken_close_tombstone/mc-122-big-Data.db",
        sstable::version_types::mc);
}

SEASTAR_TEST_CASE(broken_start_composite) {
    tests::schema_registry_wrapper registry;
    schema_ptr s =
        schema_builder(registry, "test_ks", "test_table")
            .with_column("test_key", utf8_type, column_kind::partition_key)
            .with_column("test_val", utf8_type, column_kind::clustering_key)
            .build(schema_builder::compact_storage::no);
    return broken_sst("test/resource/sstables/broken_start_composite", 76, s,
        "Unexpected start composite marker 2", "test/resource/sstables/broken_start_composite/la-76-big-Data.db");
}

SEASTAR_TEST_CASE(broken_end_composite) {
    tests::schema_registry_wrapper registry;
    schema_ptr s =
        schema_builder(registry, "test_ks", "test_table")
            .with_column("test_key", utf8_type, column_kind::partition_key)
            .with_column("test_val", utf8_type, column_kind::clustering_key)
            .build(schema_builder::compact_storage::no);
    return broken_sst("test/resource/sstables/broken_end_composite", 76, s,
        "Unexpected end composite marker 3", "test/resource/sstables/broken_end_composite/la-76-big-Data.db");
}

SEASTAR_TEST_CASE(static_mismatch) {
    tests::schema_registry_wrapper registry;
    schema_ptr s =
        schema_builder(registry, "test_foo_bar_zed_baz_ks", "test_foo_bar_zed_baz_table")
            .with_column("test_foo_bar_zed_baz_key", utf8_type, column_kind::partition_key)
            .with_column("test_foo_bar_zed_baz_val", utf8_type, column_kind::clustering_key)
            .with_column("test_foo_bar_zed_baz_static", utf8_type, column_kind::regular_column)
            .build(schema_builder::compact_storage::no);
    return broken_sst("test/resource/sstables/static_column", 58, s,
        "Mismatch between static cell and non-static column definition",
        "test/resource/sstables/static_column/la-58-big-Data.db");
}

SEASTAR_TEST_CASE(static_with_clustering) {
    tests::schema_registry_wrapper registry;
    schema_ptr s =
        schema_builder(registry, "test_foo_bar_zed_baz_ks", "test_foo_bar_zed_baz_table")
            .with_column("test_foo_bar_zed_baz_key", utf8_type, column_kind::partition_key)
            .with_column("test_foo_bar_zed_baz_val", utf8_type, column_kind::clustering_key)
            .with_column("test_foo_bar_zed_baz_static", utf8_type, column_kind::static_column)
            .build(schema_builder::compact_storage::no);
    return broken_sst("test/resource/sstables/static_with_clustering", 58, s,
        "Static row has clustering key information. I didn't expect that!",
        "test/resource/sstables/static_with_clustering/la-58-big-Data.db");
}

SEASTAR_TEST_CASE(zero_sized_histogram) {
    tests::schema_registry_wrapper registry;
    return broken_sst("test/resource/sstables/zero_sized_histogram", 5, registry,
               "Estimated histogram with zero size found. Can't continue!",
               "test/resource/sstables/zero_sized_histogram/la-5-big-Statistics.db");
}

SEASTAR_TEST_CASE(bad_column_name) {
    tests::schema_registry_wrapper registry;
    return broken_sst("test/resource/sstables/bad_column_name", 58, registry,
               "Found 3 clustering elements in column name. Was not expecting that!",
               "test/resource/sstables/bad_column_name/la-58-big-Data.db");
}

SEASTAR_TEST_CASE(empty_toc) {
    tests::schema_registry_wrapper registry;
    return broken_sst("test/resource/sstables/badtoc", 1, registry,
               "Empty TOC in sstable test/resource/sstables/badtoc/la-1-big-TOC.txt");
}

SEASTAR_TEST_CASE(alien_toc) {
    tests::schema_registry_wrapper registry;
    return broken_sst("test/resource/sstables/badtoc", 2, registry,
               "test/resource/sstables/badtoc/la-2-big-Statistics.db: file not found");
}

SEASTAR_TEST_CASE(truncated_toc) {
    tests::schema_registry_wrapper registry;
    return broken_sst("test/resource/sstables/badtoc", 3, registry,
               "test/resource/sstables/badtoc/la-3-big-Statistics.db: file not found");
}

SEASTAR_TEST_CASE(wrong_format_toc) {
    tests::schema_registry_wrapper registry;
    return broken_sst("test/resource/sstables/badtoc", 4, registry,
               "test/resource/sstables/badtoc/la-4-big-TOC.txt: file not found");
}

SEASTAR_TEST_CASE(compression_truncated) {
    tests::schema_registry_wrapper registry;
    return broken_sst("test/resource/sstables/badcompression", 1, registry,
               "test/resource/sstables/badcompression/la-1-big-Statistics.db: file not found");
}

SEASTAR_TEST_CASE(compression_bytes_flipped) {
    tests::schema_registry_wrapper registry;
    return broken_sst("test/resource/sstables/badcompression", 2, registry,
               "test/resource/sstables/badcompression/la-2-big-Statistics.db: file not found");
}
