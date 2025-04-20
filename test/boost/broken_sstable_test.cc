/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "test/lib/scylla_test_case.hh"
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/closeable.hh>

#include "test/boost/sstable_test.hh"
#include "test/lib/exception_utils.hh"

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

static future<> broken_sst(sstring dir, sstables::generation_type::int_t generation, schema_ptr s, sstring msg, std::optional<sstring> sst_name,
    sstable_version_types version = la) {
  return sstables::test_env::do_with_async([=] (sstables::test_env& env) {
    try {
        sstable_ptr sstp = env.reusable_sst(s, dir, generation, version).get();
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

static future<> broken_sst(sstring dir, sstables::generation_type::int_t generation, sstring msg, std::optional<sstring> sst_name = std::nullopt) {
    // Using an empty schema for this function, which is only about loading
    // a malformed component and checking that it fails.
    auto s = schema_builder("ks", "cf", {}, utf8_type).build();
    return broken_sst(dir, generation, s, msg, sst_name);
}

SEASTAR_TEST_CASE(test_empty_index) {
  return sstables::test_env::do_with_async([&] (sstables::test_env& env) {
    auto s = schema_builder("test_ks", "test_table")
                 .with_column("pk", int32_type, column_kind::partition_key)
                 .with_column("ck", int32_type, column_kind::clustering_key)
                 .with_column("val", int32_type)
                 .set_compressor_params(compression_parameters::no_compression())
                 .build();
    future<sstable_ptr> fut = env.reusable_sst(s, "test/resource/sstables/empty_index", 36, sstable_version_types::mc);
    BOOST_REQUIRE_EXCEPTION(fut.get(), malformed_sstable_exception, exception_predicate::message_matches(
        "index_consume_entry_context \\(state=.*\\): cannot finish parsing current entry, no more data in sstable test/resource/sstables/empty_index/mc-36-big-Index.db"));
  });
}

SEASTAR_TEST_CASE(missing_column_in_schema) {
    schema_ptr s = schema_builder("test_ks", "test_table")
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
    schema_ptr s = schema_builder("test_ks", "test_table")
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
    schema_ptr s = schema_builder("test_ks", "test_t")
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
    schema_ptr s = schema_builder("test_ks", "test_table")
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
    schema_ptr s = schema_builder("test_ks", "test_table")
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
    schema_ptr s = schema_builder("test_ks", "test_table")
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
    schema_ptr s =
        schema_builder("test_ks", "test_table")
            .with_column("test_key", utf8_type, column_kind::partition_key)
            .with_column("test_val", utf8_type, column_kind::clustering_key)
            .build(schema_builder::compact_storage::no);
    return broken_sst("test/resource/sstables/broken_start_composite", 76, s,
        "Unexpected start composite marker 2", "test/resource/sstables/broken_start_composite/la-76-big-Data.db");
}

SEASTAR_TEST_CASE(broken_end_composite) {
    schema_ptr s =
        schema_builder("test_ks", "test_table")
            .with_column("test_key", utf8_type, column_kind::partition_key)
            .with_column("test_val", utf8_type, column_kind::clustering_key)
            .build(schema_builder::compact_storage::no);
    return broken_sst("test/resource/sstables/broken_end_composite", 76, s,
        "Unexpected end composite marker 3", "test/resource/sstables/broken_end_composite/la-76-big-Data.db");
}

SEASTAR_TEST_CASE(static_mismatch) {
    schema_ptr s =
        schema_builder("test_foo_bar_zed_baz_ks", "test_foo_bar_zed_baz_table")
            .with_column("test_foo_bar_zed_baz_key", utf8_type, column_kind::partition_key)
            .with_column("test_foo_bar_zed_baz_val", utf8_type, column_kind::clustering_key)
            .with_column("test_foo_bar_zed_baz_static", utf8_type, column_kind::regular_column)
            .build(schema_builder::compact_storage::no);
    return broken_sst("test/resource/sstables/static_column", 58, s,
        "Mismatch between static cell and non-static column definition",
        "test/resource/sstables/static_column/la-58-big-Data.db");
}

SEASTAR_TEST_CASE(static_with_clustering) {
    schema_ptr s =
        schema_builder("test_foo_bar_zed_baz_ks", "test_foo_bar_zed_baz_table")
            .with_column("test_foo_bar_zed_baz_key", utf8_type, column_kind::partition_key)
            .with_column("test_foo_bar_zed_baz_val", utf8_type, column_kind::clustering_key)
            .with_column("test_foo_bar_zed_baz_static", utf8_type, column_kind::static_column)
            .build(schema_builder::compact_storage::no);
    return broken_sst("test/resource/sstables/static_with_clustering", 58, s,
        "Static row has clustering key information. I didn't expect that!",
        "test/resource/sstables/static_with_clustering/la-58-big-Data.db");
}

SEASTAR_TEST_CASE(zero_sized_histogram) {
    return broken_sst("test/resource/sstables/zero_sized_histogram", 5,
               "Estimated histogram with zero size found. Can't continue!",
               "test/resource/sstables/zero_sized_histogram/la-5-big-Statistics.db");
}

SEASTAR_TEST_CASE(bad_column_name) {
    return broken_sst("test/resource/sstables/bad_column_name", 58,
               "Found 3 clustering elements in column name. Was not expecting that!",
               "test/resource/sstables/bad_column_name/la-58-big-Data.db");
}

SEASTAR_TEST_CASE(empty_toc) {
    return broken_sst("test/resource/sstables/badtoc", 1,
               "Empty TOC in sstable test/resource/sstables/badtoc/la-1-big-TOC.txt");
}

SEASTAR_TEST_CASE(alien_toc) {
    return broken_sst("test/resource/sstables/badtoc", 2,
               "test/resource/sstables/badtoc/la-2-big-Statistics.db: file not found");
}

SEASTAR_TEST_CASE(truncated_toc) {
    return broken_sst("test/resource/sstables/badtoc", 3,
               "test/resource/sstables/badtoc/la-3-big-Statistics.db: file not found");
}

SEASTAR_TEST_CASE(wrong_format_toc) {
    return broken_sst("test/resource/sstables/badtoc", 4,
               "test/resource/sstables/badtoc/la-4-big-TOC.txt: file not found");
}

SEASTAR_TEST_CASE(compression_truncated) {
    return broken_sst("test/resource/sstables/badcompression", 1,
               "test/resource/sstables/badcompression/la-1-big-Statistics.db: file not found");
}

SEASTAR_TEST_CASE(compression_bytes_flipped) {
    return broken_sst("test/resource/sstables/badcompression", 2,
               "test/resource/sstables/badcompression/la-2-big-Statistics.db: file not found");
}
