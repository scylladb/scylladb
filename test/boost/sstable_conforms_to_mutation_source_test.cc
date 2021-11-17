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
#include "test/boost/sstable_test.hh"
#include <seastar/core/thread.hh>
#include "sstables/sstables.hh"
#include "test/lib/mutation_source_test.hh"
#include "test/lib/sstable_utils.hh"
#include "test/lib/mutation_assertions.hh"
#include "partition_slice_builder.hh"

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
        storage_service_for_tests ssft;
        for (auto version : all_sstable_versions) {
            for (auto index_block_size : {1, 128, 64*1024}) {
                sstable_writer_config cfg = env.manager().configure_writer();
                cfg.promoted_index_block_size = index_block_size;
                test_mutation_source(env, cfg, version);
            }
        }
    });
}

// Regression test for scylladb/scylla-enterprise#2016
SEASTAR_THREAD_TEST_CASE(test_produces_range_tombstone) {
    auto s = schema_builder("ks", "cf")
                .with_column("pk", int32_type, column_kind::partition_key)
                .with_column("ck", int32_type, column_kind::clustering_key)
                .with_column("v", int32_type, column_kind::regular_column)
                .build();

    mutation m(s, partition_key::from_single_value(*s, int32_type->decompose(0)));
    m.partition().apply_row_tombstone(*s, range_tombstone{
            clustering_key::from_exploded(*s, {int32_type->decompose(6)}), bound_kind::excl_start,
            clustering_key::from_exploded(*s, {int32_type->decompose(10)}), bound_kind::incl_end,
            tombstone(0, gc_clock::time_point())
    });

    {
        auto ckey = clustering_key::from_exploded(*s, {int32_type->decompose(6)});
        deletable_row& row = m.partition().clustered_row(*s, ckey, is_dummy::no, is_continuous(false));
        row.marker() = row_marker(4);
    }
    {
        auto ckey = clustering_key::from_exploded(*s, {int32_type->decompose(8)});
        deletable_row& row = m.partition().clustered_row(*s, ckey, is_dummy::no, is_continuous(false));
        row.apply(tombstone(2, gc_clock::time_point()));
        row.marker() = row_marker(5);
    }

    testlog.info("m: {}", m);

    auto slice = partition_slice_builder(*s)
        .with_range(query::clustering_range::make(
            {clustering_key::from_exploded(*s, {int32_type->decompose(8)}), false},
            {clustering_key::from_exploded(*s, {int32_type->decompose(10)}), true}
        ))
        .build();

    auto pr = dht::partition_range::make_singular(m.decorated_key());

    std::vector<tmpdir> dirs;
    dirs.emplace_back();
    sstables::test_env::do_with_async([&] (sstables::test_env& env) {
        storage_service_for_tests ssft;
        auto version = sstable_version_types::la;
        auto index_block_size = 1;
        sstable_writer_config cfg = env.manager().configure_writer();
        cfg.promoted_index_block_size = index_block_size;

        auto source = make_sstable_mutation_source(env, s, dirs.back().path().string(), {m}, cfg, version, gc_clock::now());

        {
            auto rd = source.make_reader(s, tests::make_permit(), pr, slice);
            while (auto mf = rd(db::no_timeout).get0()) {
                testlog.info("produced {}", mutation_fragment::printer(*s, *mf));
            }
        }

        {
            auto rd = source.make_reader(s, tests::make_permit(), pr, slice);
            mutation_opt sliced_m = read_mutation_from_flat_mutation_reader(rd, db::no_timeout).get0();
            BOOST_REQUIRE(bool(sliced_m));

            assert_that(*sliced_m).is_equal_to(m, slice.row_ranges(*m.schema(), m.key()));
        }
    }).get();
}
