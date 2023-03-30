/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */


#include <boost/test/unit_test.hpp>
#include "sstables/generation_type.hh"
#include "test/lib/scylla_test_case.hh"
#include <seastar/testing/thread_test_case.hh>
#include <seastar/core/thread.hh>
#include "sstables/sstables.hh"
#include "timestamp.hh"
#include "schema/schema_builder.hh"
#include "test/lib/mutation_source_test.hh"
#include "test/lib/flat_mutation_reader_assertions.hh"
#include "test/lib/sstable_utils.hh"

using namespace sstables;
using namespace std::chrono_literals;

SEASTAR_TEST_CASE(test_schema_changes) {
  return sstables::test_env::do_with_async([] (sstables::test_env& env) {
    std::map<std::tuple<sstables::sstable::version_types, schema_ptr>, shared_sstable> cache;
    for_each_schema_change([&] (schema_ptr base, const std::vector<mutation>& base_mutations,
                                schema_ptr changed, const std::vector<mutation>& changed_mutations) {
        for (auto version : writable_sstable_versions) {
            auto it = cache.find(std::tuple { version, base });

            shared_sstable created_with_base_schema;
            shared_sstable created_with_changed_schema;
            if (it == cache.end()) {
                auto mt = make_lw_shared<replica::memtable>(base);
                for (auto& m : base_mutations) {
                    mt->apply(m);
                }

                created_with_base_schema = make_sstable_containing(env.make_sstable(base), mt);

                cache.emplace(std::tuple { version, base }, created_with_base_schema);
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
        }
    });
  });
}
