/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
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

constexpr std::array<sstable_version_types, 3> expected_writable_sstable_versions = {
sstable_version_types::mc,
sstable_version_types::md,
sstable_version_types::me,
};

// Add/remove test cases if writable_sstable_versions changes
static_assert(writable_sstable_versions.size() == expected_writable_sstable_versions.size(), "writable_sstable_versions changed");
static_assert(writable_sstable_versions[0] == expected_writable_sstable_versions[0], "writable_sstable_versions changed");
static_assert(writable_sstable_versions[1] == expected_writable_sstable_versions[1], "writable_sstable_versions changed");
static_assert(writable_sstable_versions[2] == expected_writable_sstable_versions[2], "writable_sstable_versions changed");

future <> test_schema_changes_int(sstable_version_types sstable_vtype) {
  return sstables::test_env::do_with_async([] (sstables::test_env& env) {
    std::map<schema_ptr, shared_sstable> cache;
    for_each_schema_change([&] (schema_ptr base, const std::vector<mutation>& base_mutations,
                                schema_ptr changed, const std::vector<mutation>& changed_mutations) {
        auto it = cache.find(base);

        shared_sstable created_with_base_schema;
        shared_sstable created_with_changed_schema;
        if (it == cache.end()) {
            created_with_base_schema = make_sstable_containing(env.make_sstable(base), base_mutations);
            cache.emplace(base, created_with_base_schema);
        } else {
            created_with_base_schema = it->second;
        }

        created_with_changed_schema = env.reusable_sst(changed, created_with_base_schema).get();

        const auto pr = dht::partition_range::make_open_ended_both_sides();

        auto mr = assert_that(created_with_base_schema->as_mutation_source()
                    .make_reader_v2(changed, env.make_reader_permit(), pr, changed->full_slice()));
        for (auto& m : changed_mutations) {
            mr.produces(m);
        }
        mr.produces_end_of_stream();

        mr = assert_that(created_with_changed_schema->as_mutation_source()
                .make_reader_v2(changed, env.make_reader_permit(), pr, changed->full_slice()));
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
