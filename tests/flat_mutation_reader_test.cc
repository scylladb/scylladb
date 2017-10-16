/*
 * Copyright (C) 2017 ScyllaDB
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


#include <seastar/core/thread.hh>
#include <seastar/tests/test-utils.hh>

#include "mutation.hh"
#include "streamed_mutation.hh"
#include "mutation_source_test.hh"
#include "flat_mutation_reader.hh"
#include "mutation_reader.hh"
#include "schema_builder.hh"
#include "memtable.hh"
#include "mutation_reader_assertions.hh"
#include "row_cache.hh"
#include "sstables/sstables.hh"
#include "tmpdir.hh"
#include "sstable_test.hh"

#include "disk-error-handler.hh"

thread_local disk_error_signal_type commit_error;
thread_local disk_error_signal_type general_disk_error;

static void test_double_conversion(const std::vector<mutation>& mutations) {
    BOOST_REQUIRE(!mutations.empty());
    auto schema = mutations[0].schema();
    auto base_reader = make_reader_returning_many(mutations);
    auto flat_reader = flat_mutation_reader_from_mutation_reader(schema,
                                                                 std::move(base_reader),
                                                                 streamed_mutation::forwarding::no);
    auto normal_reader = mutation_reader_from_flat_mutation_reader(schema, std::move(flat_reader));
    for (auto& m : mutations) {
        auto smopt = normal_reader().get0();
        BOOST_REQUIRE(smopt);
        auto mopt = mutation_from_streamed_mutation(std::move(*smopt)).get0();
        BOOST_REQUIRE(mopt);
        BOOST_REQUIRE_EQUAL(m, *mopt);
    }
    BOOST_REQUIRE(!normal_reader().get0());
}

static void check_two_readers_are_the_same(schema_ptr schema, mutation_reader& normal_reader, flat_mutation_reader& flat_reader) {
    auto smopt = normal_reader().get0();
    BOOST_REQUIRE(smopt);
    auto mfopt = flat_reader().get0();
    BOOST_REQUIRE(mfopt);
    BOOST_REQUIRE(mfopt->is_partition_start());
    BOOST_REQUIRE(smopt->decorated_key().equal(*schema, mfopt->as_mutable_partition_start().key()));
    BOOST_REQUIRE_EQUAL(smopt->partition_tombstone(), mfopt->as_mutable_partition_start().partition_tombstone());
    mutation_fragment_opt sm_mfopt;
    while (bool(sm_mfopt = (*smopt)().get0())) {
        mfopt = flat_reader().get0();
        BOOST_REQUIRE(mfopt);
        BOOST_REQUIRE(sm_mfopt->equal(*schema, *mfopt));
    }
    mfopt = flat_reader().get0();
    BOOST_REQUIRE(mfopt);
    BOOST_REQUIRE(mfopt->is_end_of_partition());
}

static void test_conversion_to_flat_mutation_reader(const std::vector<mutation>& mutations) {
    BOOST_REQUIRE(!mutations.empty());
    auto schema = mutations[0].schema();
    auto base_reader = make_reader_returning_many(mutations);
    auto flat_reader = flat_mutation_reader_from_mutation_reader(schema,
                                                                 std::move(base_reader),
                                                                 streamed_mutation::forwarding::no);
    for (auto& m : mutations) {
        auto normal_reader = make_reader_returning(m);
        check_two_readers_are_the_same(schema, normal_reader, flat_reader);
    }
}

/*
 * =================
 * ===== Tests =====
 * =================
 */

SEASTAR_TEST_CASE(test_conversions_single_mutation) {
    return seastar::async([] {
        for_each_mutation([&] (const mutation& m) {
            test_double_conversion({m});
            test_conversion_to_flat_mutation_reader({m});
        });
    });
}

SEASTAR_TEST_CASE(test_double_conversion_two_mutations) {
    return seastar::async([] {
        for_each_mutation_pair([&] (auto&& m, auto&& m2, are_equal) {
            if (m.decorated_key().less_compare(*m.schema(), m2.decorated_key())) {
                test_double_conversion({m, m2});
                test_conversion_to_flat_mutation_reader({m, m2});
            } else if (m2.decorated_key().less_compare(*m.schema(), m.decorated_key())) {
                test_double_conversion({m2, m});
                test_conversion_to_flat_mutation_reader({m2, m});
            }
        });
    });
}
