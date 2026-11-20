/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <boost/test/unit_test.hpp>
#include "test/lib/scylla_test_case.hh"
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/closeable.hh>

#include "test/lib/sstable_test_env.hh"
#include "test/lib/sstable_utils.hh"
#include "test/lib/exception_utils.hh"
#include "test/lib/key_utils.hh"
#include "test/lib/mutation_assertions.hh"
#include "test/lib/mutation_source_test.hh"
#include "mutation/frozen_mutation.hh"
#include "readers/mutation_reader.hh"
#include "schema/schema_builder.hh"
#include "sstables/exceptions.hh"
#include "sstables/mx/partition_decoder.hh"
#include "sstables/sstables.hh"

using namespace sstables;

namespace {

// Decodes the first len bytes of the data file of sst as one partition of schema s.
mutation decode_data_file(test_env& env, schema_ptr s, shared_sstable sst, uint64_t len, uint64_t declared_size) {
    auto permit = env.make_reader_permit();
    auto input = sst->data_stream(0, len, permit, {}, {}).get();
    return mx::decode_partition(s, mx::make_data_source_descriptor(*s, *sst), std::move(input), declared_size, permit).get();
}

mutation read_with_full_scan(test_env& env, schema_ptr s, shared_sstable sst) {
    auto reader = sst->make_full_scan_reader(s, env.make_reader_permit());
    auto close_reader = deferred_close(reader);
    auto m = read_mutation_from_mutation_reader(reader).get();
    BOOST_REQUIRE(m);
    BOOST_REQUIRE(!read_mutation_from_mutation_reader(reader).get());
    return std::move(*m);
}

} // anonymous namespace

SEASTAR_TEST_CASE(test_decode_partition_matches_sstable_reader) {
    return test_env::do_with_async([] (test_env& env) {
        using rmg = random_mutation_generator;
        for (auto counters : {rmg::generate_counters::no, rmg::generate_counters::yes}) {
            for (uint32_t seed = 1; seed <= 20; ++seed) {
                rmg gen(counters, local_shard_only::yes, rmg::generate_uncompactable::no, seed, "ks", "cf", rmg::compress::no);
                auto s = gen.schema();
                auto sst = make_sstable_containing(env.make_sstable(s, sstable_version_types::me), {gen()}).get();
                auto decoded = decode_data_file(env, s, sst, sst->data_size(), sst->data_size());
                assert_that(decoded).is_equal_to(read_with_full_scan(env, s, sst));
            }
        }
    });
}

SEASTAR_TEST_CASE(test_decode_partition_rejects_bad_input) {
    return test_env::do_with_async([] (test_env& env) {
        random_mutation_generator gen(random_mutation_generator::generate_counters::no, local_shard_only::yes,
                random_mutation_generator::generate_uncompactable::no, 1, "ks", "cf", random_mutation_generator::compress::no);
        auto s = gen.schema();

        auto two = make_sstable_containing(env.make_sstable(s, sstable_version_types::me), gen(2)).get();
        BOOST_REQUIRE_THROW(decode_data_file(env, s, two, two->data_size(), two->data_size()), malformed_sstable_exception);

        auto one = make_sstable_containing(env.make_sstable(s, sstable_version_types::me), {gen()}).get();
        const auto size = one->data_size();
        // Declared size cuts the partition short.
        BOOST_REQUIRE_THROW(decode_data_file(env, s, one, size, size - 1), malformed_sstable_exception);
        // Input ends before the declared size.
        BOOST_REQUIRE_THROW(decode_data_file(env, s, one, size - 1, size), malformed_sstable_exception);
        BOOST_REQUIRE_THROW(decode_data_file(env, s, one, 0, 0), malformed_sstable_exception);
    });
}

// Data at or before its column's drop is dead and skipped, as the sstable reader
// and a frozen mutation upgrade skip it. Other data the reading schema cannot take
// fails the decode, where a frozen mutation upgrade would drop it silently.
SEASTAR_TEST_CASE(test_decode_partition_rejects_columns_the_schema_cannot_take) {
    return test_env::do_with_async([] (test_env& env) {
        auto v1 = schema_builder(this_smp_shard_count(), "ks", "cf")
                .with_column("pk", utf8_type, column_kind::partition_key)
                .with_column("ck", int32_type, column_kind::clustering_key)
                .with_column("s1", int32_type, column_kind::static_column)
                .with_column("r1", int32_type)
                .with_column("r2", int32_type)
                .with_column("r3", int32_type)
                .with_column("r4", int32_type)
                .with_column("r5", int32_type)
                .build();
        // r2 is gone without a drop record, r3 changed to an incompatible type,
        // r4 was dropped at timestamp 7 and added again, r5 was dropped at 7, r6 is new.
        auto v2 = schema_builder(this_smp_shard_count(), "ks", "cf")
                .with_column("pk", utf8_type, column_kind::partition_key)
                .with_column("ck", int32_type, column_kind::clustering_key)
                .with_column("s1", int32_type, column_kind::static_column)
                .with_column("r1", int32_type)
                .with_column("r3", utf8_type)
                .with_column("r4", int32_type)
                .with_column("r6", int32_type)
                .without_column("r4", int32_type, 7)
                .without_column("r5", int32_type, 7)
                .build();

        // A key of this shard, since the sstable is written here.
        auto pk = tests::generate_partition_key(v1).key();
        auto ck = clustering_key::from_single_value(*v1, serialized(int32_t(1)));
        auto with_cell = [&] (const char* column, api::timestamp_type ts) {
            mutation m(v1, pk);
            m.set_static_cell("s1", data_value(int32_t(0)), 10);
            m.partition().clustered_row(*v1, ck).apply(row_marker(10));
            m.set_clustered_cell(ck, "r1", data_value(int32_t(1)), 10);
            m.set_clustered_cell(ck, column, data_value(int32_t(2)), ts);
            return m;
        };
        auto decode = [&] (const mutation& m) {
            auto sst = make_sstable_containing(env.make_sstable(v1, sstable_version_types::me), {m}).get();
            return decode_data_file(env, v2, sst, sst->data_size(), sst->data_size());
        };

        BOOST_REQUIRE_EXCEPTION(decode(with_cell("r2", 10)), mx::column_mismatch_error,
                exception_predicate::message_contains("is missing from the schema"));
        BOOST_REQUIRE_EXCEPTION(decode(with_cell("r3", 10)), mx::column_mismatch_error,
                exception_predicate::message_contains("incompatible with the schema"));
        BOOST_REQUIRE_EXCEPTION(decode(with_cell("r5", 8)), mx::column_mismatch_error,
                exception_predicate::message_contains("holds data with timestamp 8, after the column was dropped at 7"));

        using dead_or_live = std::tuple<const char*, api::timestamp_type, size_t>;
        for (const auto& [column, ts, cells] : {dead_or_live("r4", 7, 1), dead_or_live("r5", 7, 1), dead_or_live("r4", 8, 2)}) {
            auto m = with_cell(column, ts);
            auto decoded = decode(m);
            assert_that(decoded).is_equal_to(freeze(m).unfreeze_upgrading(v2, v1->get_column_mapping()));
            BOOST_REQUIRE_EQUAL(decoded.partition().clustered_row(*v2, ck).cells().size(), cells);
        }
    });
}
