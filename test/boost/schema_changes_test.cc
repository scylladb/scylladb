/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */


#include <boost/test/unit_test.hpp>
#include "test/lib/scylla_test_case.hh"
#include <seastar/testing/thread_test_case.hh>
#include <seastar/core/thread.hh>
#include "sstables/sstables.hh"
#include "test/lib/mutation_source_test.hh"
#include "test/lib/mutation_reader_assertions.hh"
#include "test/lib/sstable_utils.hh"

using namespace sstables;
using namespace std::chrono_literals;

constexpr std::array<sstable_version_types, 6> expected_writable_sstable_versions = {
sstable_version_types::mc,
sstable_version_types::md,
sstable_version_types::me,
sstable_version_types::ms,
sstable_version_types::mt,
sstable_version_types::pq,
};

// Add/remove test cases if writable_sstable_versions changes
static_assert(writable_sstable_versions.size() == expected_writable_sstable_versions.size(), "writable_sstable_versions changed");
static_assert(writable_sstable_versions[0] == expected_writable_sstable_versions[0], "writable_sstable_versions changed");
static_assert(writable_sstable_versions[1] == expected_writable_sstable_versions[1], "writable_sstable_versions changed");
static_assert(writable_sstable_versions[2] == expected_writable_sstable_versions[2], "writable_sstable_versions changed");
static_assert(writable_sstable_versions[3] == expected_writable_sstable_versions[3], "writable_sstable_versions changed");
static_assert(writable_sstable_versions[4] == expected_writable_sstable_versions[4], "writable_sstable_versions changed");
static_assert(writable_sstable_versions[5] == expected_writable_sstable_versions[5], "writable_sstable_versions changed");

future <> test_schema_changes_int(sstable_version_types sstable_vtype) {
  return sstables::test_env::do_with_async([] (sstables::test_env& env) {
    std::map<schema_ptr, shared_sstable> cache;
    for_each_schema_change([&] (schema_ptr base, const utils::chunked_vector<mutation>& base_mutations,
                                schema_ptr changed, const utils::chunked_vector<mutation>& changed_mutations) {
        auto it = cache.find(base);

        shared_sstable created_with_base_schema;
        shared_sstable created_with_changed_schema;
        if (it == cache.end()) {
            created_with_base_schema = make_sstable_containing(env.make_sstable(base), base_mutations).get();
            cache.emplace(base, created_with_base_schema);
        } else {
            created_with_base_schema = it->second;
        }

        created_with_changed_schema = env.reusable_sst(changed, created_with_base_schema).get();

        const auto pr = dht::partition_range::make_open_ended_both_sides();

        auto mr = assert_that(created_with_base_schema->as_mutation_source()
                    .make_mutation_reader(changed, env.make_reader_permit(), pr, changed->full_slice()));
        for (auto& m : changed_mutations) {
            mr.produces(m);
        }
        mr.produces_end_of_stream();

        mr = assert_that(created_with_changed_schema->as_mutation_source()
                .make_mutation_reader(changed, env.make_reader_permit(), pr, changed->full_slice()));
        for (auto& m : changed_mutations) {
            mr.produces(m);
        }
        mr.produces_end_of_stream();
    });
  });
}

SEASTAR_TEST_CASE(test_schema_changes_mc) {
    return test_schema_changes_int(sstable_version_types::mc);
}

SEASTAR_TEST_CASE(test_schema_changes_md) {
    return test_schema_changes_int(sstable_version_types::md);
}

SEASTAR_TEST_CASE(test_schema_changes_me) {
    return test_schema_changes_int(sstable_version_types::me);
}

SEASTAR_TEST_CASE(test_schema_changes_ms) {
    return test_schema_changes_int(sstable_version_types::ms);
}

SEASTAR_TEST_CASE(test_schema_changes_mt) {
    return test_schema_changes_int(sstable_version_types::mt);
}

// `pq` was added to writable_sstable_versions without updating the guard above, so this file has
// not compiled -- and therefore this suite has not run -- since. Schema evolution over a Parquet
// file is worth covering rather than excluding: the reader reconstructs a row from leaves selected
// by the *writing* schema, so a column added or dropped afterwards exercises exactly the
// leaf-to-column mapping that a row-oriented format gets for free.
SEASTAR_TEST_CASE(test_schema_changes_pq) {
    return test_schema_changes_int(sstable_version_types::pq);
}

// Changing only a parquet option must change both schema equality and the digest.
// user_properties::operator== and calculate_digest once covered storage_format but
// not parquet_options, so an options-only ALTER compared equal and hashed the same.
//
// Both halves need care to actually reach the code under test, and the first
// version of this test reached neither: schemas built without an explicit id
// differ at the _id clause before properties are compared, and the default
// version mode is a time UUID, which differs on every build whatever the digest
// does. Hence the shared table_id and with_hash_version().
SEASTAR_THREAD_TEST_CASE(test_parquet_options_affect_schema_equality_and_digest) {
    const auto id = table_id(utils::UUID_gen::get_time_UUID());
    auto make = [&] (const char* rows) {
        return schema_builder(1, "ks", "parquet_options_digest", id)
            .with_column("pk", int32_type, column_kind::partition_key)
            .with_column("v", int32_type)
            .set_storage_format(storage_format_type::parquet)
            .set_parquet_options({{"rows_per_row_group", rows}})
            .with_hash_version()
            .build();
    };
    auto s1 = make("5000");
    auto s2 = make("9000");
    // The version is the digest here, so this is digest coverage.
    BOOST_REQUIRE(s1->version() != s2->version());

    // Same id, same (pinned) version: only the options differ, so this is
    // equality coverage.
    auto s2_pinned = schema_builder(s2).with_version(s1->version()).build();
    BOOST_REQUIRE(!(*s1 == *s2_pinned));
}
