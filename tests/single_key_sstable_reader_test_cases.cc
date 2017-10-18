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

#include <boost/test/unit_test.hpp>

#include "tests/mutation_reader_assertions.hh"
#include "tests/simple_schema.hh"
#include "tests/sstable_test.hh"
#include "tests/sstable_utils.hh"
#include "tests/test-utils.hh"
#include "tests/tmpdir.hh"

#include "database.hh"
#include "mutation_reader.hh"

/**
 * single_key_sstable_reader related test cases.
 *
 * Compiled into mutation_reader_test.
 */

struct expected_outcome {
    using histogram = std::array<uint64_t, 10>;
    enum class optimization_enabled : uint8_t {
        no = false,
        yes = true,
        unchanged
    };

    static const histogram all_extra_sources_read;
    static const histogram no_extra_sources_read;
    static const histogram half_extra_sources_read;

    int64_t read_count{0};
    int64_t optimization_hit_count{0};
    histogram optimization_extra_read_proportion_histogram{{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}};
    optimization_enabled optimization_enabled_flag{optimization_enabled::unchanged};

    static expected_outcome single_read_doesnt_hit_optimization() {
        return expected_outcome{1, 0, expected_outcome::no_extra_sources_read, optimization_enabled::unchanged};
    }

    static expected_outcome single_read_hits_optimization(expected_outcome::histogram h, optimization_enabled e = optimization_enabled::unchanged) {
        return expected_outcome{1, 1, std::move(h), e};
    }

    expected_outcome& increment(bool hits_optimization, histogram h, optimization_enabled e = optimization_enabled::unchanged) {
        ++read_count;
        if (hits_optimization) {
            ++optimization_hit_count;
        }

        for (std::size_t i = 0; i < 10; ++i) {
            optimization_extra_read_proportion_histogram[i] += h[i];
        }

        optimization_enabled_flag = e;

        return *this;
    }
};

const expected_outcome::histogram expected_outcome::all_extra_sources_read{{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}};
const expected_outcome::histogram expected_outcome::no_extra_sources_read{{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}};
const expected_outcome::histogram expected_outcome::half_extra_sources_read{{1, 1, 1, 1, 1, 0, 0, 0, 0, 0}};

namespace seastar {
namespace metrics {

std::ostream& operator<<(std::ostream& os, const metrics::histogram_bucket& bucket) {
    os << "{ count: " << bucket.count << ", upper_bound: " << bucket.upper_bound << " }";
    return os;
}

}
}

column_family_wrapper make_column_family_wrapper_with_sstables(schema_ptr schema,
        std::vector<std::vector<mutation>> sstables_mutations,
        tmpdir& tmp,
        double single_key_parallel_scan_threshold = 1) {
    column_family::config cfg;
    cfg.datadir = tmp.path;
    cfg.enable_cache = false;
    cfg.single_key_parallel_scan_threshold = single_key_parallel_scan_threshold;

    column_family_wrapper cf_wrapper(schema, cfg);

    auto sst_factory = [&tmp, schema, gen = make_lw_shared<std::size_t>(0)] {
        auto sst = sstables::make_sstable(schema, tmp.path, ++(*gen), sstables::sstable::version_types::la, sstables::sstable::format_types::big);
        sst->set_unshared();
        return sst;
    };

    for (auto& sstable_mutations : sstables_mutations) {
        cf_wrapper.wrapped_test().add_sstable(make_sstable_containing(sst_factory, sstable_mutations));
    }

    return cf_wrapper;
}

void check_read_results(
        schema_ptr schema,
        column_family_wrapper& cf_wrapper,
        const dht::decorated_key& pkey,
        const query::partition_slice& slice,
        mutation& expected_mutation,
        const expected_outcome& outcome) {
    const auto prange = dht::partition_range::make_singular(pkey);

    bool before{true};
    if (outcome.optimization_enabled_flag == expected_outcome::optimization_enabled::unchanged) {
        before = cf_wrapper.wrapped_test().get_single_key_optimization_enabled();
    }

    assert_that(cf_wrapper.wrapped().make_reader(schema, prange, slice))
        .produces(expected_mutation)
        .produces_end_of_stream();

    BOOST_REQUIRE_EQUAL(cf_wrapper.wrapped().get_stats().single_key_reader_read_count, outcome.read_count);
    BOOST_REQUIRE_EQUAL(cf_wrapper.wrapped().get_stats().single_key_reader_optimization_hit_count, outcome.optimization_hit_count);

    if (outcome.optimization_enabled_flag == expected_outcome::optimization_enabled::unchanged) {
        BOOST_REQUIRE_EQUAL(cf_wrapper.wrapped_test().get_single_key_optimization_enabled(), before);
    } else {
        BOOST_REQUIRE_EQUAL(cf_wrapper.wrapped_test().get_single_key_optimization_enabled(), static_cast<bool>(outcome.optimization_enabled_flag));
    }

    const bool are_histograms_equal = boost::equal(cf_wrapper.wrapped().get_stats().single_key_reader_optimization_extra_read_proportion.buckets,
                outcome.optimization_extra_read_proportion_histogram,
                [] (const auto& bucket, const auto& count) { return bucket.count == count; });

    if (!are_histograms_equal) {
        BOOST_FAIL(sprint("Expected (bounds excluded) %s, got %s",
                    outcome.optimization_extra_read_proportion_histogram,
                    cf_wrapper.wrapped().get_stats().single_key_reader_optimization_extra_read_proportion.buckets));
    }
}

void check_read_results(
        schema_ptr schema,
        const dht::decorated_key& pkey,
        const query::partition_slice& slice,
        std::vector<std::vector<mutation>> sstables_mutations,
        mutation& expected_mutation,
        const expected_outcome& outcome) {
    tmpdir tmp;

    auto cf_wrapper = make_column_family_wrapper_with_sstables(schema, std::move(sstables_mutations), tmp);

    check_read_results(schema, cf_wrapper, pkey, slice, expected_mutation, outcome);
}

SEASTAR_TEST_CASE(full_slice) {
    return seastar::async([] {

        simple_schema s;
        auto pkey = s.make_pkey(0);
        const auto ckeys = s.make_ckeys(2);

        mutation table_a_mutation(pkey, s.schema());
        s.add_row(table_a_mutation, ckeys.front(), "val_a_0");
        s.add_row(table_a_mutation, ckeys.back(), "val_a_1");
        s.add_static_row(table_a_mutation, "static_val_a");

        mutation table_b_mutation(pkey, s.schema());
        s.add_row(table_b_mutation, ckeys.front(), "val_b_0");

        mutation table_c_mutation(pkey, s.schema());
        const auto trow1 = s.add_row(table_c_mutation, ckeys.back(), "val_c_1");
        const auto tsrow = s.add_static_row(table_c_mutation, "static_val_c");

        mutation table_d_mutation(pkey, s.schema());
        const auto trow0 = s.add_row(table_d_mutation, ckeys.front(), "val_d_0");

        auto expected_mutation = mutation(pkey, s.schema());
        s.add_row(expected_mutation, ckeys.front(), "val_d_0", trow0);
        s.add_row(expected_mutation, ckeys.back(), "val_c_1", trow1);
        s.add_static_row(expected_mutation, "static_val_c", tsrow);

        check_read_results(s.schema(),
                pkey,
                query::full_slice,
                {{table_a_mutation}, {table_b_mutation}, {table_c_mutation}, {table_d_mutation}},
                expected_mutation,
                expected_outcome::single_read_doesnt_hit_optimization());
    });
};


SEASTAR_TEST_CASE(row_marker_in_first_sst) {
    return seastar::async([] {

        simple_schema s;
        auto pkey = s.make_pkey(0);
        const auto ckeys = s.make_ckeys(2);

        mutation table_a_mutation(pkey, s.schema());
        const auto trm = s.add_row_marker(table_a_mutation, ckeys.front());
        s.add_row(table_a_mutation, ckeys.front(), "val_a_0");
        s.add_row_marker(table_a_mutation, ckeys.back());
        s.add_row(table_a_mutation, ckeys.back(), "val_a_1");
        s.add_static_row(table_a_mutation, "static_val_a");

        mutation table_b_mutation(pkey, s.schema());
        s.add_row(table_b_mutation, ckeys.front(), "val_b_0");

        mutation table_c_mutation(pkey, s.schema());
        s.add_row(table_c_mutation, ckeys.back(), "val_c_1");
        const auto tsrow = s.add_static_row(table_c_mutation, "static_val_c");

        mutation table_d_mutation(pkey, s.schema());
        const auto trow0 = s.add_row(table_d_mutation, ckeys.front(), "val_d_0");

        auto expected_mutation = mutation(pkey, s.schema());
        s.add_row_marker(expected_mutation, ckeys.front(), trm);
        s.add_row(expected_mutation, ckeys.front(), "val_d_0", trow0);
        s.add_static_row(expected_mutation, "static_val_c", tsrow);

        query::partition_slice slice({query::clustering_range::make_singular(ckeys.front())}, {}, {}, {});

        check_read_results(s.schema(),
                pkey,
                slice,
                {{table_a_mutation}, {table_b_mutation}, {table_c_mutation}, {table_d_mutation}},
                expected_mutation,
                expected_outcome::single_read_hits_optimization(expected_outcome::all_extra_sources_read));
    });
}

SEASTAR_TEST_CASE(row_marker_also_in_later_sst) {
    return seastar::async([] {

        simple_schema s;
        auto pkey = s.make_pkey(0);
        const auto ckey = s.make_ckey(1);

        mutation table_a_mutation(pkey, s.schema());
        s.add_row_marker(table_a_mutation, ckey);
        s.add_row(table_a_mutation, ckey, "val_a_0");
        s.add_static_row(table_a_mutation, "static_val_a");

        mutation table_b_mutation(pkey, s.schema());
        s.set_timestamp(105);
        const auto trm = s.add_row_marker(table_b_mutation, ckey);
        s.add_static_row(table_b_mutation, "static_val_b");

        mutation table_c_mutation(pkey, s.schema());
        s.set_timestamp(100);
        s.add_row_marker(table_c_mutation, ckey);
        s.set_timestamp(110);
        const auto trow = s.add_row(table_c_mutation, ckey, "val_c_0");
        const auto tsrow = s.add_static_row(table_c_mutation, "static_val_c");

        auto expected_mutation = mutation(pkey, s.schema());
        s.add_row_marker(expected_mutation, ckey, trm);
        s.add_static_row(expected_mutation, "static_val_c", tsrow);
        s.add_row(expected_mutation, ckey, "val_c_0", trow);

        query::partition_slice slice({query::clustering_range::make_singular(ckey)}, {}, {}, {});

        check_read_results(s.schema(),
                pkey,
                slice,
                {{table_a_mutation}, {table_b_mutation}, {table_c_mutation}},
                expected_mutation,
                expected_outcome::single_read_hits_optimization(expected_outcome::half_extra_sources_read));
    });
}

SEASTAR_TEST_CASE(partition_tombstone_in_second_sst) {
    return seastar::async([] {

        simple_schema s;
        auto pkey = s.make_pkey(0);
        const auto ckey = s.make_ckey(1);

        mutation table_a_mutation(pkey, s.schema());
        const auto trow_marker = s.add_row_marker(table_a_mutation, ckey);
        const auto tcrow = s.add_row(table_a_mutation, ckey, "val_a_0");
        const auto tsrow = s.add_static_row(table_a_mutation, "static_val_a");

        tombstone tomb(100, {});

        mutation table_b_mutation(pkey, s.schema());
        table_b_mutation.partition().apply(tomb);

        auto expected_mutation = mutation(pkey, s.schema());
        s.add_row_marker(expected_mutation, ckey, trow_marker);
        s.add_row(expected_mutation, ckey, "val_a_0", tcrow);
        s.add_static_row(expected_mutation, "static_val_a", tsrow);
        expected_mutation.partition().apply(tomb);

        query::partition_slice slice({query::clustering_range::make_singular(ckey)}, {}, {}, {});

        check_read_results(s.schema(),
                pkey,
                slice,
                {{table_a_mutation}, {table_b_mutation}},
                expected_mutation,
                expected_outcome::single_read_hits_optimization(expected_outcome::all_extra_sources_read));
    });
}

SEASTAR_TEST_CASE(row_tombstone_in_second_sst) {
    return seastar::async([] {

        simple_schema s;
        auto pkey = s.make_pkey(0);
        const auto ckey = s.make_ckey(1);

        mutation table_a_mutation(pkey, s.schema());
        const auto tmarker = s.add_row_marker(table_a_mutation, ckey);
        const auto trow = s.add_row(table_a_mutation, ckey, "val_a_0");
        const auto tsrow = s.add_static_row(table_a_mutation, "static_val_a");

        tombstone tomb(tsrow + 1, {});

        mutation table_b_mutation(pkey, s.schema());
        table_b_mutation.partition().apply_delete(*s.schema(), ckey, tomb);

        auto expected_mutation = mutation(pkey, s.schema());
        expected_mutation.partition().apply_delete(*s.schema(), ckey, tomb);
        s.add_row_marker(expected_mutation, ckey, tmarker);
        s.add_row(expected_mutation, ckey, "val_a_0", trow);
        s.add_static_row(expected_mutation, "static_val_a", tsrow);

        query::partition_slice slice({query::clustering_range::make_singular(ckey)}, {}, {}, {});

        check_read_results(s.schema(),
                pkey,
                slice,
                {{table_a_mutation}, {table_b_mutation}},
                expected_mutation,
                expected_outcome::single_read_hits_optimization(expected_outcome::all_extra_sources_read));
    });
}

SEASTAR_TEST_CASE(range_tombstone_in_second_sst) {
    return seastar::async([] {

        simple_schema s;
        auto pkey = s.make_pkey(0);
        const auto ckeys = s.make_ckeys(2);

        mutation table_a_mutation(pkey, s.schema());
        const auto tsrow = s.add_static_row(table_a_mutation, "static_val_a");

        // row 1
        const auto tmarker = s.add_row_marker(table_a_mutation, ckeys.front());
        const auto trow = s.add_row(table_a_mutation, ckeys.front(), "val_a_0");

        // row 2
        s.add_row_marker(table_a_mutation, ckeys.back());
        s.add_row(table_a_mutation, ckeys.back(), "val_a_1");

        const auto delete_range = query::clustering_range::make(ckeys.front(), ckeys.back());

        mutation table_b_mutation(pkey, s.schema());
        auto range_tomb = s.delete_range(table_b_mutation, delete_range);

        auto expected_mutation = mutation(pkey, s.schema());
        s.add_row_marker(expected_mutation, ckeys.front(), tmarker);
        s.add_row(expected_mutation, ckeys.front(), "val_a_0", trow);
        s.add_static_row(expected_mutation, "static_val_a", tsrow);
        expected_mutation.partition().apply_delete(*s.schema(), range_tomb);

        query::partition_slice slice({query::clustering_range::make_singular(ckeys.front())}, {}, {}, {});

        check_read_results(s.schema(),
                pkey,
                slice,
                {{table_a_mutation}, {table_b_mutation}},
                expected_mutation,
                expected_outcome::single_read_hits_optimization(expected_outcome::all_extra_sources_read));
    });
}


SEASTAR_TEST_CASE(threshold_and_counters) {
    return seastar::async([] {

        simple_schema s;
        auto pkey = s.make_pkey(0);
        const auto ckeys = s.make_ckeys(3);

        // Test data layout:
        // [rm, sr, cr] [rm, --, cr] [rm, --, cr] row1
        // [rm, --, cr] [rm, sr, cr] [--, --, cr] row2
        // [rm, --, cr] [--, --, cr] [--, sr, --] row3
        //
        // rm - row marker
        // sr - static row
        // cr - clustering row

        // sstable a
        mutation table_a_mutation1(pkey, s.schema());
        s.add_row_marker(table_a_mutation1, ckeys[0]);
        s.add_static_row(table_a_mutation1, "static_val_a");
        s.add_row(table_a_mutation1, ckeys[0], "row1_val_a");

        mutation table_a_mutation2(pkey, s.schema());
        s.add_row_marker(table_a_mutation2, ckeys[1]);
        s.add_row(table_a_mutation2, ckeys[1], "row2_val_a");

        mutation table_a_mutation3(pkey, s.schema());
        const auto trow3_rm = s.add_row_marker(table_a_mutation3, ckeys[2]);
        s.add_row(table_a_mutation3, ckeys[2], "row3_val_a");

        // sstable b
        mutation table_b_mutation1(pkey, s.schema());
        s.add_row_marker(table_b_mutation1, ckeys[0]);
        s.add_row(table_b_mutation1, ckeys[0], "row1_val_b");

        mutation table_b_mutation2(pkey, s.schema());
        const auto trow2_rm = s.add_row_marker(table_b_mutation2, ckeys[1]);
        s.add_static_row(table_b_mutation2, "static_val_b");
        s.add_row(table_b_mutation2, ckeys[1], "row2_val_b");

        mutation table_b_mutation3(pkey, s.schema());
        const auto trow3_cr = s.add_row(table_b_mutation3, ckeys[2], "row3_val_b");

        // sstable c
        mutation table_c_mutation1(pkey, s.schema());
        const auto trow1_rm = s.add_row_marker(table_c_mutation1, ckeys[0]);
        const auto trow1_cr = s.add_row(table_c_mutation1, ckeys[0], "row1_val_c");

        mutation table_c_mutation2(pkey, s.schema());
        const auto trow2_cr = s.add_row(table_c_mutation2, ckeys[1], "row2_val_c");

        mutation table_c_mutation3(pkey, s.schema());
        const auto tsr = s.add_static_row(table_c_mutation3, "static_val_c");

        auto expected_mutation_row1 = mutation(pkey, s.schema());
        s.add_row_marker(expected_mutation_row1, ckeys[0], trow1_rm);
        s.add_static_row(expected_mutation_row1, "static_val_c", tsr);
        s.add_row(expected_mutation_row1, ckeys[0], "row1_val_c", trow1_cr);

        auto expected_mutation_row2 = mutation(pkey, s.schema());
        s.add_row_marker(expected_mutation_row2, ckeys[1], trow2_rm);
        s.add_static_row(expected_mutation_row2, "static_val_c", tsr);
        s.add_row(expected_mutation_row2, ckeys[1], "row2_val_c", trow2_cr);

        auto expected_mutation_row3 = mutation(pkey, s.schema());
        s.add_row_marker(expected_mutation_row3, ckeys[2], trow3_rm);
        s.add_static_row(expected_mutation_row3, "static_val_c", tsr);
        s.add_row(expected_mutation_row3, ckeys[2], "row3_val_b", trow3_cr);

        query::partition_slice slice_row1({query::clustering_range::make_singular(ckeys[0])}, {}, {}, {});
        query::partition_slice slice_row2({query::clustering_range::make_singular(ckeys[1])}, {}, {}, {});
        query::partition_slice slice_row3({query::clustering_range::make_singular(ckeys[2])}, {}, {}, {});

        // threshold test
        {
            tmpdir tmp;

            auto cf_wrapper = make_column_family_wrapper_with_sstables(s.schema(),
                    {{table_a_mutation1, table_a_mutation2, table_a_mutation3},
                    {table_b_mutation1, table_b_mutation2, table_b_mutation3},
                    {table_c_mutation1, table_c_mutation2, table_c_mutation3}},
                    tmp,
                    0.3);

            expected_outcome outcome;

            check_read_results(s.schema(), cf_wrapper, pkey, slice_row1, expected_mutation_row1,
                    outcome.increment(true, expected_outcome::no_extra_sources_read, expected_outcome::optimization_enabled::yes));
            check_read_results(s.schema(), cf_wrapper, pkey, slice_row2, expected_mutation_row2,
                    outcome.increment(true, expected_outcome::half_extra_sources_read, expected_outcome::optimization_enabled::no));

            auto do_n_reads = [&] (int n) {
                std::uniform_int_distribution<int> d(0, 1);
                std::random_device rd;

                for (int i = 0; i < n; ++i) {
                    if (d(rd)) {
                        check_read_results(s.schema(), cf_wrapper, pkey, slice_row2, expected_mutation_row2,
                                outcome.increment(false, expected_outcome::no_extra_sources_read));
                    } else {
                        check_read_results(s.schema(), cf_wrapper, pkey, slice_row3, expected_mutation_row3,
                                outcome.increment(false, expected_outcome::no_extra_sources_read));
                    }
                }
            };

            do_n_reads(99);

            // This should be a probing read, re-enabling the optimization.
            check_read_results(s.schema(), cf_wrapper, pkey, slice_row1, expected_mutation_row1,
                    outcome.increment(true, expected_outcome::no_extra_sources_read, expected_outcome::optimization_enabled::yes));

            // This should disable the optimization again.
            check_read_results(s.schema(), cf_wrapper, pkey, slice_row3, expected_mutation_row3,
                    outcome.increment(true, expected_outcome::all_extra_sources_read, expected_outcome::optimization_enabled::no));

            do_n_reads(99);

            // Probing read again. Shouldn't enable.
            check_read_results(s.schema(), cf_wrapper, pkey, slice_row2, expected_mutation_row2,
                    outcome.increment(true, expected_outcome::half_extra_sources_read, expected_outcome::optimization_enabled::no));
        }

        // counter test
        {
            tmpdir tmp;

            auto cf_wrapper = make_column_family_wrapper_with_sstables(s.schema(),
                    {{table_a_mutation1, table_a_mutation2, table_a_mutation3},
                    {table_b_mutation1, table_b_mutation2, table_b_mutation3},
                    {table_c_mutation1, table_c_mutation2, table_c_mutation3}},
                    tmp);

            expected_outcome outcome;

            std::uniform_int_distribution<int> d(0, 1);
            std::random_device rd;

            for (int i = 0; i < 200; ++i) {
                switch (d(rd)) {
                    case 1:
                        check_read_results(s.schema(), cf_wrapper, pkey, slice_row1, expected_mutation_row1,
                                outcome.increment(true, expected_outcome::no_extra_sources_read));
                        break;
                    case 2:
                        check_read_results(s.schema(), cf_wrapper, pkey, slice_row2, expected_mutation_row2,
                                outcome.increment(true, expected_outcome::half_extra_sources_read));
                        break;
                    case 3:
                        check_read_results(s.schema(), cf_wrapper, pkey, slice_row3, expected_mutation_row3,
                                outcome.increment(true, expected_outcome::no_extra_sources_read));
                        break;
                }
            }
        }
    });
}
