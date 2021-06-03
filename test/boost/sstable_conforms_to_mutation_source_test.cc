/*
 * Copyright (C) 2015-present ScyllaDB
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
#include "test/boost/sstable_test.hh"
#include <seastar/core/thread.hh>
#include "sstables/sstables.hh"
#include "test/lib/mutation_source_test.hh"
#include "test/lib/sstable_utils.hh"

using namespace sstables;
using namespace std::chrono_literals;

static
mutation_source make_sstable_mutation_source(sstables::test_env& env, schema_ptr s, sstring dir, std::vector<mutation> mutations,
        sstable_writer_config cfg, sstables::sstable::version_types version, gc_clock::time_point query_time = gc_clock::now()) {
    return as_mutation_source(make_sstable(env, s, dir, std::move(mutations), cfg, version, query_time));
}

// Must be run in a seastar thread
static
void test_mutation_source(sstables::test_env& env, sstable_writer_config cfg, sstables::sstable::version_types version) {
    std::vector<tmpdir> dirs;
    run_mutation_source_tests([&env, &dirs, &cfg, version] (schema_ptr s, const std::vector<mutation>& partitions,
                gc_clock::time_point query_time) -> mutation_source {
        dirs.emplace_back();
        return make_sstable_mutation_source(env, s, dirs.back().path().string(), partitions, cfg, version, query_time);
    });
}


SEASTAR_TEST_CASE(test_sstable_conforms_to_mutation_source) {
    return sstables::test_env::do_with_async([] (sstables::test_env& env) {
        for (auto version : all_sstable_versions) {
            for (auto index_block_size : {1, 128, 64*1024}) {
                sstable_writer_config cfg = env.manager().configure_writer();
                cfg.promoted_index_block_size = index_block_size;
                test_mutation_source(env, cfg, version);
            }
        }
    });
}
