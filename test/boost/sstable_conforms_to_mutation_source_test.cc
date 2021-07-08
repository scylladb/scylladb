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
#include "row_cache.hh"
#include "test/lib/simple_schema.hh"
#include "partition_slice_builder.hh"
#include "test/lib/flat_mutation_reader_assertions.hh"

using namespace sstables;
using namespace std::chrono_literals;

static
mutation_source make_sstable_mutation_source(sstables::test_env& env, schema_ptr s, sstring dir, std::vector<mutation> mutations,
        sstable_writer_config cfg, sstables::sstable::version_types version, gc_clock::time_point query_time = gc_clock::now()) {
    return as_mutation_source(make_sstable(env, s, dir, std::move(mutations), cfg, version, query_time));
}

static void consume_all(flat_mutation_reader& rd) {
    while (auto mfopt = rd(db::no_timeout).get0()) {}
}

// It is assumed that src won't change.
static snapshot_source snapshot_source_from_snapshot(mutation_source src) {
    return snapshot_source([src = std::move(src)] {
        return src;
    });
}

static
void test_cache_population_with_range_tombstone_adjacent_to_population_range(populate_fn_ex populate) {
    simple_schema s;
    tests::reader_concurrency_semaphore_wrapper semaphore;
    auto cache_mt = make_lw_shared<memtable>(s.schema());

    auto pkey = s.make_pkey();

    // underlying should not be empty, otherwise cache will make the whole range continuous
    mutation m1(s.schema(), pkey);
    s.add_row(m1, s.make_ckey(0), "v1");
    s.add_row(m1, s.make_ckey(1), "v2");
    s.add_row(m1, s.make_ckey(2), "v3");
    s.delete_range(m1, s.make_ckey_range(2, 100));
    cache_mt->apply(m1);

    cache_tracker tracker;
    auto ms = populate(s.schema(), std::vector<mutation>({m1}), gc_clock::now());
    row_cache cache(s.schema(), snapshot_source_from_snapshot(std::move(ms)), tracker);

    auto pr = dht::partition_range::make_singular(pkey);

    auto populate_range = [&] (int start) {
        auto slice = partition_slice_builder(*s.schema())
                .with_range(query::clustering_range::make_singular(s.make_ckey(start)))
                .build();
        auto rd = cache.make_reader(s.schema(), semaphore.make_permit(), pr, slice);
        auto close_rd = deferred_close(rd);
        consume_all(rd);
    };

    populate_range(2);

    // The cache now has only row with ckey 2 populated and the rest is discontinuous.
    // Populating reader which stops populating at entry with ckey 2 should not forget
    // to emit range_tombstone which starts at before(2).

    assert_that(cache.make_reader(s.schema(), semaphore.make_permit()))
            .produces(m1)
            .produces_end_of_stream();
}

SEASTAR_TEST_CASE(test_sstable_conforms_to_mutation_source) {
    return sstables::test_env::do_with_async([] (sstables::test_env& env) {
        for (auto version : writable_sstable_versions) {
            for (auto index_block_size : {1, 128, 64*1024}) {
                sstable_writer_config cfg = env.manager().configure_writer();
                cfg.promoted_index_block_size = index_block_size;

                std::vector<tmpdir> dirs;
                auto populate = [&env, &dirs, &cfg, version] (schema_ptr s, const std::vector<mutation>& partitions,
                                                              gc_clock::time_point query_time) -> mutation_source {
                    dirs.emplace_back();
                    return make_sstable_mutation_source(env, s, dirs.back().path().string(), partitions, cfg, version, query_time);
                };

                run_mutation_source_tests(populate);

                if (index_block_size == 1) {
                    // The tests below are not sensitive to index bock size so run once.
                    test_cache_population_with_range_tombstone_adjacent_to_population_range(populate);
                }
            }
        }
    });
}
