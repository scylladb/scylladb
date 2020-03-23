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


#include <boost/test/unit_test.hpp>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/core/thread.hh>
#include "sstables/sstables.hh"
#include "timestamp.hh"
#include "schema_builder.hh"
#include "mutation_reader.hh"
#include "test/lib/mutation_source_test.hh"
#include "test/lib/flat_mutation_reader_assertions.hh"
#include "test/lib/sstable_utils.hh"

using namespace sstables;
using namespace std::chrono_literals;

SEASTAR_THREAD_TEST_CASE(test_schema_changes) {
    auto dir = tmpdir();
    storage_service_for_tests ssft;
    auto wait_bg = seastar::defer([] { sstables::await_background_jobs().get(); });
    int gen = 1;

    std::map<std::tuple<sstables::sstable::version_types, schema_ptr>, std::tuple<shared_sstable, int>> cache;
    for_each_schema_change([&] (schema_ptr base, const std::vector<mutation>& base_mutations,
                                schema_ptr changed, const std::vector<mutation>& changed_mutations) {
        for (auto version : all_sstable_versions) {
            auto it = cache.find(std::tuple { version, base });

            shared_sstable created_with_base_schema;
            shared_sstable created_with_changed_schema;
            sstables::test_env env;
            if (it == cache.end()) {
                auto mt = make_lw_shared<memtable>(base);
                for (auto& m : base_mutations) {
                    mt->apply(m);
                }
                created_with_base_schema = env.make_sstable(base, dir.path().string(), gen, version, sstables::sstable::format_types::big);
                created_with_base_schema->write_components(mt->make_flat_reader(base), base_mutations.size(), base, test_sstables_manager.configure_writer(), mt->get_encoding_stats()).get();
                created_with_base_schema->load().get();

                created_with_changed_schema = env.make_sstable(changed, dir.path().string(), gen, version, sstables::sstable::format_types::big);
                created_with_changed_schema->load().get();

                cache.emplace(std::tuple { version, base }, std::tuple { created_with_base_schema, gen });
                gen++;
            } else {
                created_with_base_schema = std::get<shared_sstable>(it->second);

                created_with_changed_schema = env.make_sstable(changed, dir.path().string(), std::get<int>(it->second), version, sstables::sstable::format_types::big);
                created_with_changed_schema->load().get();
            }

            auto mr = assert_that(created_with_base_schema->as_mutation_source()
                        .make_reader(changed, no_reader_permit(), dht::partition_range::make_open_ended_both_sides(), changed->full_slice()));
            for (auto& m : changed_mutations) {
                mr.produces(m);
            }
            mr.produces_end_of_stream();

            mr = assert_that(created_with_changed_schema->as_mutation_source()
                    .make_reader(changed, no_reader_permit(), dht::partition_range::make_open_ended_both_sides(), changed->full_slice()));
            for (auto& m : changed_mutations) {
                mr.produces(m);
            }
            mr.produces_end_of_stream();
        }
    });
}
