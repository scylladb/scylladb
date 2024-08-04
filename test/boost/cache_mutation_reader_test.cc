
/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */


#include "utils/assert.hh"
#include <boost/test/unit_test.hpp>

#include "test/lib/scylla_test_case.hh"
#include <seastar/core/thread.hh>
#include "schema/schema_builder.hh"
#include "keys.hh"
#include "mutation/mutation_partition.hh"
#include "mutation/partition_version.hh"
#include "mutation/mutation.hh"
#include "replica/memtable.hh"
#include "row_cache.hh"

#include "test/lib/memtable_snapshot_source.hh"
#include "test/lib/mutation_reader_assertions.hh"
#include "test/lib/reader_concurrency_semaphore.hh"
#include "test/lib/test_utils.hh"

#include <variant>

/*
 * ===================
 * ====== Utils ======
 * ===================
 */

static schema_ptr make_schema() {
    return schema_builder("ks", "cf")
        .with_column("pk", int32_type, column_kind::partition_key)
        .with_column("ck", int32_type, column_kind::clustering_key)
        .with_column("v", int32_type)
        .build();
}

static const thread_local schema_ptr SCHEMA = make_schema();

static partition_key make_pk(int value) {
    return partition_key::from_exploded(*SCHEMA, { int32_type->decompose(value) });
}

static const thread_local partition_key PK = make_pk(0);
static const thread_local dht::decorated_key DK =
    dht::decorate_key(*SCHEMA, PK);

static clustering_key make_ck(int value) {
    return clustering_key_prefix::from_single_value(*SCHEMA, int32_type->decompose(value));
}

static api::timestamp_type new_timestamp() {
    static thread_local api::timestamp_type last_timestamp = api::min_timestamp;
    auto t = std::max(api::new_timestamp(), last_timestamp + 1);
    last_timestamp = t;
    return t;
}

static void add_row(mutation& m, int ck, int value) {
    m.set_clustered_cell(make_ck(ck), "v", data_value(value), new_timestamp());
}

static void add_tombstone(mutation& m, range_tombstone rt) {
    m.partition().apply_row_tombstone(*SCHEMA, rt);
}

static void set_row_continuous(mutation_partition& mp, int ck, is_continuous value) {
    auto it = mp.clustered_rows().find(make_ck(ck), rows_entry::tri_compare(*SCHEMA));
    SCYLLA_ASSERT(it != mp.clustered_rows().end());
    it->set_continuous(value);
}

static query::partition_slice make_slice(std::vector<query::clustering_range> ranges) {
    return query::partition_slice(std::move(ranges), { }, { }, { });
}

struct expected_fragment {
    std::variant<int, range_tombstone_change> f;

    expected_fragment(int row_key) : f(row_key) { }
    expected_fragment(range_tombstone_change rtc) : f(std::move(rtc)) { }
    expected_fragment(position_in_partition_view pos, tombstone tomb) : expected_fragment(range_tombstone_change(pos, tomb)) { }

    void check(flat_reader_assertions_v2& r) {
        if (f.index() == 0) {
            r.produces_row_with_key(make_ck(std::get<int>(f)));
        } else {
            r.produces_range_tombstone_change(std::get<range_tombstone_change>(f));
        }
    }
};

static
mutation make_incomplete_mutation() {
    return mutation(SCHEMA, DK, mutation_partition::make_incomplete(*SCHEMA));
}

static void assert_single_version(partition_snapshot_ptr snp) {
    BOOST_REQUIRE(snp->at_latest_version());
    BOOST_REQUIRE_EQUAL(snp->version_count(), 1);
}

struct expected_row {
    position_in_partition pos;
    is_continuous continuous;
    is_dummy dummy;

    struct dummy_tag_t { };

    expected_row(int k, is_continuous cont, is_dummy dummy = is_dummy::no)
        : pos(position_in_partition::for_key(make_ck(k))), continuous(cont), dummy(dummy) { }
    expected_row(position_in_partition pos, is_continuous cont, is_dummy dummy = is_dummy::no)
        : pos(pos), continuous(cont), dummy(dummy) { }
    expected_row(dummy_tag_t, is_continuous cont)
        : pos(position_in_partition::after_all_clustered_rows()), continuous(cont), dummy(true) { }

    void check(const rows_entry& r) const {
        position_in_partition::equal_compare ck_eq(*SCHEMA);
        if (!ck_eq(r.position(), pos)) {
            BOOST_FAIL(format("Expected {}, but got {}", pos, r.position()));
        }
        BOOST_REQUIRE_EQUAL(r.continuous(), continuous);
        BOOST_REQUIRE_EQUAL(r.dummy(), dummy);
    }

    position_in_partition key() const {
        return pos;
    }
};

template <> struct fmt::formatter<expected_row> : fmt::formatter<string_view> {
    auto format(const expected_row& e, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "{{pos={}, cont={}, dummy={}}}", e.key(), bool(e.continuous), bool(e.dummy));
    }
};

static void assert_cached_rows(partition_snapshot_ptr snp, std::deque<expected_row> expected) {
    auto&& rows = snp->version()->partition().clustered_rows();
    for (auto&& r : rows) {
        BOOST_REQUIRE(!expected.empty());
        expected.front().check(r);
        expected.pop_front();
    }
    if (!expected.empty()) {
        BOOST_FAIL(format("Expected {} next, but no more rows", expected.front()));
    }
}

struct expected_tombstone {
    int start;
    bool start_inclusive;
    int end;
    bool end_inclusive;

    expected_tombstone(int s, bool s_i, int e, bool e_i)
        : start(s)
        , start_inclusive(s_i)
        , end(e)
        , end_inclusive(e_i)
        { }
    void check(const range_tombstone& rt) const {
        clustering_key::equality ck_eq(*SCHEMA);
        BOOST_REQUIRE(ck_eq(rt.start, make_ck(start)));
        BOOST_REQUIRE_EQUAL(rt.start_kind, start_inclusive ? bound_kind::incl_start : bound_kind::excl_start);
        BOOST_REQUIRE(ck_eq(rt.end, make_ck(end)));
        BOOST_REQUIRE_EQUAL(rt.end_kind, end_inclusive ? bound_kind::incl_end : bound_kind::excl_end);
    }
};

static void assert_cached_tombstones(partition_snapshot_ptr snp, std::deque<range_tombstone> expected, const query::clustering_row_ranges& ck_ranges) {
    range_tombstone_list rts = snp->version()->partition().as_mutation_partition(*SCHEMA).mutable_row_tombstones();
    rts.trim(*SCHEMA, ck_ranges);

    range_tombstone_list expected_list(*SCHEMA);
    for (auto&& rt : expected) {
        expected_list.apply(*SCHEMA, rt);
    }

    expected_list.trim(*SCHEMA, ck_ranges);

    BOOST_REQUIRE(rts.equal(*SCHEMA, expected_list));
}

class cache_tester {
public:
    static partition_snapshot_ptr snapshot_for_key(row_cache& rc, const dht::decorated_key& dk) {
        return rc._read_section(rc._tracker.region(), [&] {
            cache_entry& e = rc.lookup(dk);
            return e.partition().read(rc._tracker.region(), rc._tracker.cleaner(), &rc._tracker);
        });
    }
};

static void check_produces_only(const dht::decorated_key& dk,
                                mutation_reader r,
                                std::deque<expected_fragment> expected) {
    auto ra = assert_that(std::move(r));
    ra.produces_partition_start(dk);
    for (auto&& e : expected) {
        e.check(ra);
    }
    ra.produces_partition_end();
    ra.produces_end_of_stream();
}

void test_slice_single_version(mutation& underlying,
                               mutation& cache_mutation,
                               const query::partition_slice& slice,
                               std::deque<expected_fragment> expected_sm_fragments,
                               std::deque<expected_row> expected_cache_rows,
                               std::deque<range_tombstone> expected_cache_tombstones) {
    tests::reader_concurrency_semaphore_wrapper semaphore;

    // Set up underlying
    memtable_snapshot_source source_mt(SCHEMA);
    source_mt.apply(underlying);
    cache_tracker tracker;
    row_cache cache(SCHEMA, snapshot_source([&] { return source_mt(); }), tracker);

    cache.populate(cache_mutation);

    try {
        auto range = dht::partition_range::make_singular(DK);
        auto reader = cache.make_reader(SCHEMA, semaphore.make_permit(), range, slice);

        check_produces_only(DK, std::move(reader), expected_sm_fragments);

        auto snp = cache_tester::snapshot_for_key(cache, DK);
        assert_single_version(snp);
        assert_cached_rows(snp, expected_cache_rows);
        assert_cached_tombstones(snp, expected_cache_tombstones, slice.row_ranges(*SCHEMA, DK.key()));
    } catch (...) {
        std::cerr << "cache: " << cache << "\n";
        throw;
    }
}

/*
 * ========================================================
 * ====== Tests for single row with a single version ======
 * ========================================================
 */
void test_single_row(int ck,
                     bool cached,
                     is_continuous continuous,
                     const query::partition_slice& slice,
                     std::deque<int> expected_sm_rows,
                     std::deque<expected_row> expected_cache_rows) {
    const int value = 12;

    mutation underlying(SCHEMA, PK);
    add_row(underlying, ck, value);

    auto m = make_incomplete_mutation();
    if (cached) {
        add_row(m, ck, value);
        set_row_continuous(m.partition(), ck, continuous);
    }

    std::deque<expected_fragment> expected_sm_fragments;
    for (int r : expected_sm_rows) {
        expected_sm_fragments.push_back(expected_fragment(r));
    }
    test_slice_single_version(underlying, m, slice, expected_sm_fragments, expected_cache_rows, { });
}

static expected_row after_cont(int ck) {
    return expected_row(position_in_partition::after_key(*SCHEMA, make_ck(ck)), is_continuous::yes, is_dummy::yes);
}

static expected_row after_notc(int ck) {
    return expected_row(position_in_partition::after_key(*SCHEMA, make_ck(ck)), is_continuous::no, is_dummy::yes);
}

static expected_row before_cont(int ck) {
    return expected_row(position_in_partition::before_key(make_ck(ck)), is_continuous::yes, is_dummy::yes);
}

static expected_row before_notc(int ck) {
    return expected_row(position_in_partition::before_key(make_ck(ck)), is_continuous::no, is_dummy::yes);
}

SEASTAR_TEST_CASE(test_single_row_not_cached_full_range) {
    return seastar::async([] {
        test_single_row(1, false, is_continuous::yes, SCHEMA->full_slice(), { 1 }, {
            expected_row(1, is_continuous::yes),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::yes)
        });
    });
}

SEASTAR_TEST_CASE(test_single_row_not_cached_single_row_range) {
    return seastar::async([] {
        test_single_row(1, false, is_continuous::yes, make_slice({ query::clustering_range::make_singular(make_ck(1)) }), { 1 }, {
            expected_row(1, is_continuous::no),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::no)
        });
    });
}

SEASTAR_TEST_CASE(test_single_row_not_cached_range_from_start_to_row) {
    return seastar::async([] {
        test_single_row(1, false, is_continuous::yes, make_slice({ query::clustering_range::make_ending_with(make_ck(1)) }), { 1 }, {
            expected_row(1, is_continuous::yes),
            after_cont(1),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::no)
        });
    });
}

SEASTAR_TEST_CASE(test_single_row_not_cached_range_from_row_to_end) {
    return seastar::async([] {
        test_single_row(1, false, is_continuous::yes, make_slice({ query::clustering_range::make_starting_with(make_ck(1)) }), { 1 }, {
            before_notc(1),
            expected_row(1, is_continuous::yes),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::yes)
        });
    });
}

SEASTAR_TEST_CASE(test_single_row_not_cached_exclusive_range_on_the_left) {
    return seastar::async([] {
        test_single_row(1, false, is_continuous::yes, make_slice({ query::clustering_range::make_ending_with({make_ck(1), false}) }),
            { }, {
                before_cont(1),
                expected_row(expected_row::dummy_tag_t{}, is_continuous::no)
            });
    });
}

SEASTAR_TEST_CASE(test_single_row_not_cached_exclusive_range_on_the_right) {
    return seastar::async([] {
        test_single_row(1, false, is_continuous::yes, make_slice({ query::clustering_range::make_starting_with({make_ck(1), false}) }),
            { }, {
                after_notc(1),
                expected_row(expected_row::dummy_tag_t{}, is_continuous::yes)
            });
    });
}

SEASTAR_TEST_CASE(test_single_row_not_cached_small_range) {
    return seastar::async([] {
        test_single_row(1, false, is_continuous::yes, make_slice({ query::clustering_range::make(make_ck(0), make_ck(2)) }), { 1 }, {
            before_notc(0),
            expected_row(1, is_continuous::yes),
            after_cont(2),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::no)
        });
    });
}

SEASTAR_TEST_CASE(test_single_row_not_cached_small_range_on_left) {
    return seastar::async([] {
        test_single_row(1, false, is_continuous::yes, make_slice({ query::clustering_range::make(make_ck(0), make_ck(1)) }), { 1 }, {
            before_notc(0),
            expected_row(1, is_continuous::yes),
            after_cont(1),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::no)
        });
    });
}

SEASTAR_TEST_CASE(test_single_row_not_cached_small_range_on_right) {
    return seastar::async([] {
        test_single_row(1, false, is_continuous::yes, make_slice({ query::clustering_range::make(make_ck(1), make_ck(2)) }), { 1 }, {
            before_notc(1),
            expected_row(1, is_continuous::yes),
            after_cont(2),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::no)
        });
    });
}

SEASTAR_TEST_CASE(test_single_row_cached_as_continuous_full_range) {
    return seastar::async([] {
        test_single_row(1, true, is_continuous::yes, SCHEMA->full_slice(), { 1 }, {
            expected_row(1, is_continuous::yes),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::yes)
        });
    });
}

SEASTAR_TEST_CASE(test_single_row_cached_as_continuous_single_row_range) {
    return seastar::async([] {
        test_single_row(1, true, is_continuous::yes, make_slice({ query::clustering_range::make_singular(make_ck(1)) }), { 1 }, {
            expected_row(1, is_continuous::yes),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::no)
        });
    });
}

SEASTAR_TEST_CASE(test_single_row_cached_as_continuous_range_from_start_to_row) {
    return seastar::async([] {
        test_single_row(1, true, is_continuous::yes, make_slice({ query::clustering_range::make_ending_with(make_ck(1)) }), { 1 }, {
            expected_row(1, is_continuous::yes),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::no)
        });
    });
}

SEASTAR_TEST_CASE(test_single_row_cached_as_continuous_range_from_row_to_end) {
    return seastar::async([] {
        test_single_row(1, true, is_continuous::yes, make_slice({ query::clustering_range::make_starting_with(make_ck(1)) }), { 1 }, {
            expected_row(1, is_continuous::yes),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::yes)
        });
    });
}

SEASTAR_TEST_CASE(test_single_row_cached_as_continuous_exclusive_range_on_the_left) {
    return seastar::async([] {
        test_single_row(1, true, is_continuous::yes, make_slice({ query::clustering_range::make_ending_with({make_ck(1), false}) }), { }, {
            expected_row(1, is_continuous::yes),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::no)
        });
    });
}

SEASTAR_TEST_CASE(test_single_row_cached_as_continuous_exclusive_range_on_the_right) {
    return seastar::async([] {
        test_single_row(1, true, is_continuous::yes, make_slice({ query::clustering_range::make_starting_with({make_ck(1), false}) }), { }, {
            expected_row(1, is_continuous::yes),
            after_notc(1),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::yes)
        });
    });
}

SEASTAR_TEST_CASE(test_single_row_cached_as_continuous_small_range) {
    return seastar::async([] {
        test_single_row(1, true, is_continuous::yes, make_slice({ query::clustering_range::make(make_ck(0), make_ck(2)) }), { 1 }, {
            expected_row(1, is_continuous::yes),
            after_cont(2),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::no)
        });
    });
}

SEASTAR_TEST_CASE(test_single_row_cached_as_continuous_small_range_on_left) {
    return seastar::async([] {
        test_single_row(1, true, is_continuous::yes, make_slice({ query::clustering_range::make(make_ck(0), make_ck(1)) }), { 1 }, {
            expected_row(1, is_continuous::yes),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::no)
        });
    });
}

SEASTAR_TEST_CASE(test_single_row_cached_as_continuous_small_range_on_right) {
    return seastar::async([] {
        test_single_row(1, true, is_continuous::yes, make_slice({ query::clustering_range::make(make_ck(1), make_ck(2)) }), { 1 }, {
            expected_row(1, is_continuous::yes),
            after_cont(2),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::no)
        });
    });
}

SEASTAR_TEST_CASE(test_single_row_cached_as_noncontinuous_full_range) {
    return seastar::async([] {
        test_single_row(1, true, is_continuous::no, SCHEMA->full_slice(), { 1 }, {
            expected_row(1, is_continuous::yes),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::yes)
        });
    });
}

SEASTAR_TEST_CASE(test_single_row_cached_as_noncontinuous_single_row_range) {
    return seastar::async([] {
        test_single_row(1, true, is_continuous::no, make_slice({ query::clustering_range::make_singular(make_ck(1)) }), { 1 }, {
            expected_row(1, is_continuous::no),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::no)
        });
    });
}

SEASTAR_TEST_CASE(test_single_row_cached_as_noncontinuous_range_from_start_to_row) {
    return seastar::async([] {
        test_single_row(1, true, is_continuous::no, make_slice({ query::clustering_range::make_ending_with(make_ck(1)) }), { 1 }, {
            expected_row(1, is_continuous::yes),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::no)
        });
    });
}

SEASTAR_TEST_CASE(test_single_row_cached_as_noncontinuous_range_from_row_to_end) {
    return seastar::async([] {
        test_single_row(1, true, is_continuous::no, make_slice({ query::clustering_range::make_starting_with(make_ck(1)) }), { 1 }, {
            expected_row(1, is_continuous::no),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::yes)
        });
    });
}

SEASTAR_TEST_CASE(test_single_row_cached_as_noncontinuous_exclusive_range_on_the_left) {
    return seastar::async([] {
        test_single_row(1, true, is_continuous::no, make_slice({ query::clustering_range::make_ending_with({make_ck(1), false}) }), { }, {
            before_cont(1),
            expected_row(1, is_continuous::no),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::no)
        });
    });
}

SEASTAR_TEST_CASE(test_single_row_cached_as_noncontinuous_exclusive_range_on_the_right) {
    return seastar::async([] {
        test_single_row(1, true, is_continuous::no, make_slice({ query::clustering_range::make_starting_with({make_ck(1), false}) }), { }, {
            expected_row(1, is_continuous::no),
            after_notc(1),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::yes)
        });
    });
}

SEASTAR_TEST_CASE(test_single_row_cached_as_noncontinuous_small_range) {
    return seastar::async([] {
        test_single_row(1, true, is_continuous::no, make_slice({ query::clustering_range::make(make_ck(0), make_ck(2)) }), { 1 }, {
            before_notc(0),
            expected_row(1, is_continuous::yes),
            after_cont(2),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::no)
        });
    });
}

SEASTAR_TEST_CASE(test_single_row_cached_as_noncontinuous_small_range_on_left) {
    return seastar::async([] {
        test_single_row(1, true, is_continuous::no, make_slice({ query::clustering_range::make(make_ck(0), make_ck(1)) }), { 1 }, {
            before_notc(0),
            expected_row(1, is_continuous::yes),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::no)
        });
    });
}

SEASTAR_TEST_CASE(test_single_row_cached_as_noncontinuous_small_range_on_right) {
    return seastar::async([] {
        test_single_row(1, true, is_continuous::no, make_slice({ query::clustering_range::make(make_ck(1), make_ck(2)) }), { 1 }, {
            expected_row(1, is_continuous::no),
            after_cont(2),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::no)
        });
    });
}

/*
 * ======================================================
 * ====== Tests for two rows with a single version ======
 * ======================================================
 */

void test_two_rows(int ck1,
                   bool cached1,
                   is_continuous continuous1,
                   int ck2,
                   bool cached2,
                   is_continuous continuous2,
                   const query::partition_slice& slice,
                   std::deque<int> expected_sm_rows,
                   std::deque<expected_row> expected_cache_rows) {
    const int value1 = 12;
    const int value2 = 34;

    mutation underlying(SCHEMA, PK);
    add_row(underlying, ck1, value1);
    add_row(underlying, ck2, value2);

    auto cache = make_incomplete_mutation();
    if (cached1) {
        add_row(cache, ck1, value1);
        set_row_continuous(cache.partition(), ck1, continuous1);
    }
    if (cached2) {
        add_row(cache, ck2, value2);
        set_row_continuous(cache.partition(), ck2, continuous2);
    }
    std::deque<expected_fragment> expected_sm_fragments;
    for (int r : expected_sm_rows) {
        expected_sm_fragments.push_back(expected_fragment(r));
    }
    test_slice_single_version(underlying, cache, slice, expected_sm_fragments, expected_cache_rows, { });
}

SEASTAR_TEST_CASE(test_two_rows_not_cached_full_range) {
    return seastar::async([] {
        test_two_rows(1, false, is_continuous::no, 3, false, is_continuous::no, SCHEMA->full_slice(), { 1, 3 }, {
            expected_row(1, is_continuous::yes),
            expected_row(3, is_continuous::yes),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::yes)
        });
    });
}

SEASTAR_TEST_CASE(test_two_rows_not_cached_single_row_range1) {
    return seastar::async([] {
        test_two_rows(1, false, is_continuous::no, 3, false, is_continuous::no, make_slice({ query::clustering_range::make_singular(make_ck(1)) }), { 1 }, {
            expected_row(1, is_continuous::no),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::no)
        });
    });
}

SEASTAR_TEST_CASE(test_two_rows_not_cached_single_row_range2) {
    return seastar::async([] {
        test_two_rows(1, false, is_continuous::no, 3, false, is_continuous::no, make_slice({ query::clustering_range::make_singular(make_ck(3)) }), { 3 }, {
            expected_row(3, is_continuous::no),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::no)
        });
    });
}

SEASTAR_TEST_CASE(test_two_rows_not_cached_range_from_start_to_row1) {
    return seastar::async([] {
        test_two_rows(1, false, is_continuous::no, 3, false, is_continuous::no, make_slice({ query::clustering_range::make_ending_with(make_ck(1)) }), { 1 }, {
            expected_row(1, is_continuous::yes),
            after_cont(1),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::no)
        });
    });
}

SEASTAR_TEST_CASE(test_two_rows_not_cached_range_from_start_to_row2) {
    return seastar::async([] {
        test_two_rows(1, false, is_continuous::no, 3, false, is_continuous::no, make_slice({ query::clustering_range::make_ending_with(make_ck(3)) }), { 1, 3 }, {
            expected_row(1, is_continuous::yes),
            expected_row(3, is_continuous::yes),
            after_cont(3),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::no)
        });
    });
}

SEASTAR_TEST_CASE(test_two_rows_not_cached_range_from_row1_to_end) {
    return seastar::async([] {
        test_two_rows(1, false, is_continuous::no, 3, false, is_continuous::no, make_slice({ query::clustering_range::make_starting_with(make_ck(1)) }), { 1, 3 }, {
            before_notc(1),
            expected_row(1, is_continuous::yes),
            expected_row(3, is_continuous::yes),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::yes)
        });
    });
}

SEASTAR_TEST_CASE(test_two_rows_not_cached_range_from_row2_to_end) {
    return seastar::async([] {
        test_two_rows(1, false, is_continuous::no, 3, false, is_continuous::no, make_slice({ query::clustering_range::make_starting_with(make_ck(3)) }), { 3 }, {
            before_notc(3),
            expected_row(3, is_continuous::yes),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::yes)
        });
    });
}

SEASTAR_TEST_CASE(test_two_rows_not_cached_exclusive_range_on_the_left) {
    return seastar::async([] {
        test_two_rows(1, false, is_continuous::no, 3, false, is_continuous::no, make_slice({ query::clustering_range::make_ending_with({make_ck(3), false}) }), { 1 }, {
            expected_row(1, is_continuous::yes),
            before_cont(3),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::no)
        });
    });
}

SEASTAR_TEST_CASE(test_two_rows_not_cached_exclusive_range_on_the_right) {
    return seastar::async([] {
        test_two_rows(1, false, is_continuous::no, 3, false, is_continuous::no, make_slice({ query::clustering_range::make_starting_with({make_ck(1), false}) }), { 3 }, {
            after_notc(1),
            expected_row(3, is_continuous::yes),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::yes)
        });
    });
}

SEASTAR_TEST_CASE(test_two_rows_not_cached_exclusive_range_between_rows1) {
    return seastar::async([] {
        test_two_rows(1, false, is_continuous::no, 3, false, is_continuous::no, make_slice({ query::clustering_range::make({make_ck(1), false}, {make_ck(3), false}) }),
            { }, {
                after_notc(1),
                before_cont(3),
                expected_row(expected_row::dummy_tag_t{}, is_continuous::no)
            });
    });
}

SEASTAR_TEST_CASE(test_two_rows_not_cached_exclusive_range_between_rows2) {
    return seastar::async([] {
        test_two_rows(1, false, is_continuous::no, 3, false, is_continuous::no, make_slice({ query::clustering_range::make({make_ck(1), false}, make_ck(3)) }), { 3 }, {
            after_notc(1),
            expected_row(3, is_continuous::yes),
            after_cont(3),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::no)
        });
    });
}

SEASTAR_TEST_CASE(test_two_rows_not_cached_exclusive_range_between_rows3) {
    return seastar::async([] {
        test_two_rows(1, false, is_continuous::no, 3, false, is_continuous::no, make_slice({ query::clustering_range::make(make_ck(1), {make_ck(3), false}) }), { 1 }, {
            before_notc(1),
            expected_row(1, is_continuous::yes),
            before_cont(3),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::no)
        });
    });
}

SEASTAR_TEST_CASE(test_two_rows_not_cached_small_range) {
    return seastar::async([] {
        test_two_rows(1, false, is_continuous::no, 3, false, is_continuous::no, make_slice({ query::clustering_range::make(make_ck(0), make_ck(4)) }), { 1, 3 }, {
            before_notc(0),
            expected_row(1, is_continuous::yes),
            expected_row(3, is_continuous::yes),
            after_cont(4),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::no)
        });
    });
}

SEASTAR_TEST_CASE(test_two_rows_not_cached_small_range_row1) {
    return seastar::async([] {
        test_two_rows(1, false, is_continuous::no, 3, false, is_continuous::no, make_slice({ query::clustering_range::make(make_ck(0), make_ck(2)) }), { 1 }, {
            before_notc(0),
            expected_row(1, is_continuous::yes),
            after_cont(2),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::no)
        });
    });
}

SEASTAR_TEST_CASE(test_two_rows_not_cached_small_range_row2) {
    return seastar::async([] {
        test_two_rows(1, false, is_continuous::no, 3, false, is_continuous::no, make_slice({ query::clustering_range::make(make_ck(2), make_ck(4)) }), { 3 }, {
            before_notc(2),
            expected_row(3, is_continuous::yes),
            after_cont(4),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::no)
        });
    });
}

SEASTAR_TEST_CASE(test_two_rows_cached_continuous_full_range) {
    return seastar::async([] {
        test_two_rows(1, true, is_continuous::yes, 3, true, is_continuous::yes, SCHEMA->full_slice(), { 1, 3 }, {
            expected_row(1, is_continuous::yes),
            expected_row(3, is_continuous::yes),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::yes)
        });
    });
}

SEASTAR_TEST_CASE(test_two_rows_cached_continuous_single_row_range1) {
    return seastar::async([] {
        test_two_rows(1, true, is_continuous::yes, 3, true, is_continuous::yes, make_slice({ query::clustering_range::make_singular(make_ck(1)) }), { 1 }, {
            expected_row(1, is_continuous::yes),
            expected_row(3, is_continuous::yes),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::no)
        });
    });
}

SEASTAR_TEST_CASE(test_two_rows_cached_continuous_single_row_range2) {
    return seastar::async([] {
        test_two_rows(1, true, is_continuous::yes, 3, true, is_continuous::yes, make_slice({ query::clustering_range::make_singular(make_ck(3)) }), { 3 }, {
            expected_row(1, is_continuous::yes),
            expected_row(3, is_continuous::yes),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::no)
        });
    });
}

SEASTAR_TEST_CASE(test_two_rows_cached_continuous_range_from_start_to_row1) {
    return seastar::async([] {
        test_two_rows(1, true, is_continuous::yes, 3, true, is_continuous::yes, make_slice({ query::clustering_range::make_ending_with(make_ck(1)) }), { 1 }, {
            expected_row(1, is_continuous::yes),
            expected_row(3, is_continuous::yes),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::no)
        });
    });
}

SEASTAR_TEST_CASE(test_two_rows_cached_continuous_range_from_start_to_row2) {
    return seastar::async([] {
        test_two_rows(1, true, is_continuous::yes, 3, true, is_continuous::yes, make_slice({ query::clustering_range::make_ending_with(make_ck(3)) }), { 1, 3 }, {
            expected_row(1, is_continuous::yes),
            expected_row(3, is_continuous::yes),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::no)
        });
    });
}

SEASTAR_TEST_CASE(test_two_rows_cached_continuous_range_from_row1_to_end) {
    return seastar::async([] {
        test_two_rows(1, true, is_continuous::yes, 3, true, is_continuous::yes, make_slice({ query::clustering_range::make_starting_with(make_ck(1)) }), { 1, 3 }, {
            expected_row(1, is_continuous::yes),
            expected_row(3, is_continuous::yes),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::yes)
        });
    });
}

SEASTAR_TEST_CASE(test_two_rows_cached_continuous_range_from_row2_to_end) {
    return seastar::async([] {
        test_two_rows(1, true, is_continuous::yes, 3, true, is_continuous::yes, make_slice({ query::clustering_range::make_starting_with(make_ck(3)) }), { 3 }, {
            expected_row(1, is_continuous::yes),
            expected_row(3, is_continuous::yes),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::yes)
        });
    });
}

SEASTAR_TEST_CASE(test_two_rows_cached_continuous_exclusive_range_on_the_left) {
    return seastar::async([] {
        test_two_rows(1, true, is_continuous::yes, 3, true, is_continuous::yes, make_slice({ query::clustering_range::make_ending_with({make_ck(3), false}) }), { 1 }, {
            expected_row(1, is_continuous::yes),
            expected_row(3, is_continuous::yes),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::no)
        });
    });
}

SEASTAR_TEST_CASE(test_two_rows_cached_continuous_exclusive_range_on_the_right) {
    return seastar::async([] {
        test_two_rows(1, true, is_continuous::yes, 3, true, is_continuous::yes, make_slice({ query::clustering_range::make_starting_with({make_ck(1), false}) }), { 3 }, {
            expected_row(1, is_continuous::yes),
            expected_row(3, is_continuous::yes),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::yes)
        });
    });
}

SEASTAR_TEST_CASE(test_two_rows_cached_continuous_exclusive_range_between_rows1) {
    return seastar::async([] {
        test_two_rows(1, true, is_continuous::yes, 3, true, is_continuous::yes, make_slice({ query::clustering_range::make({make_ck(1), false}, {make_ck(3), false}) }), { }, {
            expected_row(1, is_continuous::yes),
            expected_row(3, is_continuous::yes),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::no)
        });
    });
}

SEASTAR_TEST_CASE(test_two_rows_cached_continuous_exclusive_range_between_rows2) {
    return seastar::async([] {
        test_two_rows(1, true, is_continuous::yes, 3, true, is_continuous::yes, make_slice({ query::clustering_range::make({make_ck(1), false}, make_ck(3)) }), { 3 }, {
            expected_row(1, is_continuous::yes),
            expected_row(3, is_continuous::yes),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::no)
        });
    });
}

SEASTAR_TEST_CASE(test_two_rows_cached_continuous_exclusive_range_between_rows3) {
    return seastar::async([] {
        test_two_rows(1, true, is_continuous::yes, 3, true, is_continuous::yes, make_slice({ query::clustering_range::make(make_ck(1), {make_ck(3), false}) }), { 1 }, {
            expected_row(1, is_continuous::yes),
            expected_row(3, is_continuous::yes),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::no)
        });
    });
}

SEASTAR_TEST_CASE(test_two_rows_cached_continuous_small_range) {
    return seastar::async([] {
        test_two_rows(1, true, is_continuous::yes, 3, true, is_continuous::yes, make_slice({ query::clustering_range::make(make_ck(0), make_ck(4)) }), { 1, 3 }, {
            expected_row(1, is_continuous::yes),
            expected_row(3, is_continuous::yes),
            after_cont(4),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::no)
        });
    });
}

SEASTAR_TEST_CASE(test_two_rows_cached_continuous_small_range_row1) {
    return seastar::async([] {
        test_two_rows(1, true, is_continuous::yes, 3, true, is_continuous::yes, make_slice({ query::clustering_range::make(make_ck(0), make_ck(2)) }), { 1 }, {
            expected_row(1, is_continuous::yes),
            expected_row(3, is_continuous::yes),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::no)
        });
    });
}

SEASTAR_TEST_CASE(test_two_rows_cached_continuous_small_range_row2) {
    return seastar::async([] {
        test_two_rows(1, true, is_continuous::yes, 3, true, is_continuous::yes, make_slice({ query::clustering_range::make(make_ck(2), make_ck(4)) }), { 3 }, {
            expected_row(1, is_continuous::yes),
            expected_row(3, is_continuous::yes),
            after_cont(4),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::no)
        });
    });
}

SEASTAR_TEST_CASE(test_two_rows_cached_non_continuous_full_range) {
    return seastar::async([] {
        test_two_rows(1, true, is_continuous::no, 3, true, is_continuous::no, SCHEMA->full_slice(), { 1, 3 }, {
            expected_row(1, is_continuous::yes),
            expected_row(3, is_continuous::yes),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::yes)
        });
    });
}

SEASTAR_TEST_CASE(test_two_rows_cached_non_continuous_single_row_range1) {
    return seastar::async([] {
        test_two_rows(1, true, is_continuous::no, 3, true, is_continuous::no, make_slice({ query::clustering_range::make_singular(make_ck(1)) }), { 1 }, {
            expected_row(1, is_continuous::no),
            expected_row(3, is_continuous::no),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::no)
        });
    });
}

SEASTAR_TEST_CASE(test_two_rows_cached_non_continuous_single_row_range2) {
    return seastar::async([] {
        test_two_rows(1, true, is_continuous::no, 3, true, is_continuous::no, make_slice({ query::clustering_range::make_singular(make_ck(3)) }), { 3 }, {
            expected_row(1, is_continuous::no),
            expected_row(3, is_continuous::no),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::no)
        });
    });
}

SEASTAR_TEST_CASE(test_two_rows_cached_non_continuous_range_from_start_to_row1) {
    return seastar::async([] {
        test_two_rows(1, true, is_continuous::no, 3, true, is_continuous::no, make_slice({ query::clustering_range::make_ending_with(make_ck(1)) }), { 1 }, {
            expected_row(1, is_continuous::yes),
            expected_row(3, is_continuous::no),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::no)
        });
    });
}

SEASTAR_TEST_CASE(test_two_rows_cached_non_continuous_range_from_start_to_row2) {
    return seastar::async([] {
        test_two_rows(1, true, is_continuous::no, 3, true, is_continuous::no, make_slice({ query::clustering_range::make_ending_with(make_ck(3)) }), { 1, 3 }, {
            expected_row(1, is_continuous::yes),
            expected_row(3, is_continuous::yes),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::no)
        });
    });
}

SEASTAR_TEST_CASE(test_two_rows_cached_non_continuous_range_from_row1_to_end) {
    return seastar::async([] {
        test_two_rows(1, true, is_continuous::no, 3, true, is_continuous::no, make_slice({ query::clustering_range::make_starting_with(make_ck(1)) }), { 1, 3 }, {
            expected_row(1, is_continuous::no),
            expected_row(3, is_continuous::yes),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::yes)
        });
    });
}

SEASTAR_TEST_CASE(test_two_rows_cached_non_continuous_range_from_row2_to_end) {
    return seastar::async([] {
        test_two_rows(1, true, is_continuous::no, 3, true, is_continuous::no, make_slice({ query::clustering_range::make_starting_with(make_ck(3)) }), { 3 }, {
            expected_row(1, is_continuous::no),
            expected_row(3, is_continuous::no),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::yes)
        });
    });
}

SEASTAR_TEST_CASE(test_two_rows_cached_non_continuous_exclusive_range_on_the_left) {
    return seastar::async([] {
        test_two_rows(1, true, is_continuous::no, 3, true, is_continuous::no, make_slice({ query::clustering_range::make_ending_with({make_ck(3), false}) }), { 1 }, {
            expected_row(1, is_continuous::yes),
            before_cont(3),
            expected_row(3, is_continuous::no),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::no)
        });
    });
}

SEASTAR_TEST_CASE(test_two_rows_cached_non_continuous_exclusive_range_on_the_right) {
    return seastar::async([] {
        test_two_rows(1, true, is_continuous::no, 3, true, is_continuous::no, make_slice({ query::clustering_range::make_starting_with({make_ck(1), false}) }), { 3 }, {
            expected_row(1, is_continuous::no),
            after_notc(1),
            expected_row(3, is_continuous::yes),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::yes)
        });
    });
}

SEASTAR_TEST_CASE(test_two_rows_cached_non_continuous_exclusive_range_between_rows1) {
    return seastar::async([] {
        test_two_rows(1, true, is_continuous::no, 3, true, is_continuous::no, make_slice({ query::clustering_range::make({make_ck(1), false}, {make_ck(3), false}) }), { }, {
            expected_row(1, is_continuous::no),
            after_notc(1),
            before_cont(3),
            expected_row(3, is_continuous::no),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::no)
        });
    });
}

SEASTAR_TEST_CASE(test_two_rows_cached_non_continuous_exclusive_range_between_rows2) {
    return seastar::async([] {
        test_two_rows(1, true, is_continuous::no, 3, true, is_continuous::no, make_slice({ query::clustering_range::make({make_ck(1), false}, make_ck(3)) }), { 3 }, {
            expected_row(1, is_continuous::no),
            after_notc(1),
            expected_row(3, is_continuous::yes),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::no)
        });
    });
}

SEASTAR_TEST_CASE(test_two_rows_cached_non_continuous_exclusive_range_between_rows3) {
    return seastar::async([] {
        test_two_rows(1, true, is_continuous::no, 3, true, is_continuous::no, make_slice({ query::clustering_range::make(make_ck(1), {make_ck(3), false}) }), { 1 }, {
            expected_row(1, is_continuous::no),
            before_cont(3),
            expected_row(3, is_continuous::no),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::no)
        });
    });
}

SEASTAR_TEST_CASE(test_two_rows_cached_non_continuous_small_range) {
    return seastar::async([] {
        test_two_rows(1, true, is_continuous::no, 3, true, is_continuous::no, make_slice({ query::clustering_range::make(make_ck(0), make_ck(4)) }), { 1, 3 }, {
            before_notc(0),
            expected_row(1, is_continuous::yes),
            expected_row(3, is_continuous::yes),
            after_cont(4),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::no)
        });
    });
}

SEASTAR_TEST_CASE(test_two_rows_cached_non_continuous_small_range_row1) {
    return seastar::async([] {
        test_two_rows(1, true, is_continuous::no, 3, true, is_continuous::no, make_slice({ query::clustering_range::make(make_ck(0), make_ck(2)) }), { 1 }, {
            before_notc(0),
            expected_row(1, is_continuous::yes),
            after_cont(2),
            expected_row(3, is_continuous::no),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::no)
        });
    });
}

SEASTAR_TEST_CASE(test_two_rows_cached_non_continuous_small_range_row2) {
    return seastar::async([] {
        test_two_rows(1, true, is_continuous::no, 3, true, is_continuous::no, make_slice({ query::clustering_range::make(make_ck(2), make_ck(4)) }), { 3 }, {
            expected_row(1, is_continuous::no),
            before_notc(2),
            expected_row(3, is_continuous::yes),
            after_cont(4),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::no)
        });
    });
}

SEASTAR_TEST_CASE(test_two_rows_first_not_cached_second_cached_non_continuous1) {
    return seastar::async([] {
        test_two_rows(1, false, is_continuous::no, 3, true, is_continuous::no, make_slice({ query::clustering_range::make(make_ck(2), make_ck(4)) }), { 3 }, {
            before_notc(2),
            expected_row(3, is_continuous::yes),
            after_cont(4),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::no)
        });
    });
}

SEASTAR_TEST_CASE(test_two_rows_first_not_cached_second_cached_non_continuous2) {
    return seastar::async([] {
        test_two_rows(1, false, is_continuous::no, 3, true, is_continuous::no, make_slice({ query::clustering_range::make(make_ck(0), make_ck(4)) }), { 1, 3 }, {
            before_notc(0),
            expected_row(1, is_continuous::yes),
            expected_row(3, is_continuous::yes),
            after_cont(4),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::no)
        });
    });
}

SEASTAR_TEST_CASE(test_two_rows_first_cached_non_continuous_second_not_cached1) {
    return seastar::async([] {
        test_two_rows(1, true, is_continuous::no, 3, false, is_continuous::no, make_slice({ query::clustering_range::make(make_ck(2), make_ck(4)) }), { 3 }, {
            expected_row(1, is_continuous::no),
            before_notc(2),
            expected_row(3, is_continuous::yes),
            after_cont(4),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::no)
        });
    });
}

SEASTAR_TEST_CASE(test_two_rows_first_cached_non_continuous_second_not_cached2) {
    return seastar::async([] {
        test_two_rows(1, true, is_continuous::no, 3, false, is_continuous::no, make_slice({ query::clustering_range::make(make_ck(0), make_ck(4)) }), { 1, 3 }, {
            before_notc(0),
            expected_row(1, is_continuous::yes),
            expected_row(3, is_continuous::yes),
            after_cont(4),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::no)
        });
    });
}

SEASTAR_TEST_CASE(test_two_rows_first_cached_continuous_second_not_cached1) {
    return seastar::async([] {
        test_two_rows(1, true, is_continuous::yes, 3, false, is_continuous::no, make_slice({ query::clustering_range::make(make_ck(2), make_ck(4)) }), { 3 }, {
            expected_row(1, is_continuous::yes),
            before_notc(2),
            expected_row(3, is_continuous::yes),
            after_cont(4),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::no)
        });
    });
}

SEASTAR_TEST_CASE(test_two_rows_first_cached_continuous_second_not_cached2) {
    return seastar::async([] {
        test_two_rows(1, true, is_continuous::yes, 3, false, is_continuous::no, make_slice({ query::clustering_range::make(make_ck(0), make_ck(4)) }), { 1, 3 }, {
            expected_row(1, is_continuous::yes),
            expected_row(3, is_continuous::yes),
            after_cont(4),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::no)
        });
    });
}

/*
 * ========================================================
 * ====== Tests for three rows with a single version ======
 * ========================================================
 */

void test_three_rows(int ck1,
                     bool cached1,
                     is_continuous continuous1,
                     int ck2,
                     bool cached2,
                     is_continuous continuous2,
                     int ck3,
                     bool cached3,
                     is_continuous continuous3,
                     const query::partition_slice& slice,
                     std::deque<int> expected_sm_rows,
                     std::deque<expected_row> expected_cache_rows) {
    const int value1 = 12;
    const int value2 = 34;
    const int value3 = 56;

    mutation underlying(SCHEMA, PK);
    add_row(underlying, ck1, value1);
    add_row(underlying, ck2, value2);
    add_row(underlying, ck3, value3);

    auto cache = make_incomplete_mutation();
    if (cached1) {
        add_row(cache, ck1, value1);
        set_row_continuous(cache.partition(), ck1, continuous1);
    }
    if (cached2) {
        add_row(cache, ck2, value2);
        set_row_continuous(cache.partition(), ck2, continuous2);
    }
    if (cached3) {
        add_row(cache, ck3, value3);
        set_row_continuous(cache.partition(), ck3, continuous3);
    }
    std::deque<expected_fragment> expected_sm_fragments;
    for (int r : expected_sm_rows) {
        expected_sm_fragments.push_back(expected_fragment(r));
    }
    test_slice_single_version(underlying, cache, slice, expected_sm_fragments, expected_cache_rows, { });
}

SEASTAR_TEST_CASE(test_three_rows_first_continuous1) {
    return seastar::async([] {
        test_three_rows(1, true, is_continuous::yes, 3, false, is_continuous::no, 5, true, is_continuous::no, make_slice({ query::clustering_range::make(make_ck(0), make_ck(6)) }), { 1, 3, 5 }, {
            expected_row(1, is_continuous::yes),
            expected_row(3, is_continuous::yes),
            expected_row(5, is_continuous::yes),
            after_cont(6),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::no)
        });
    });
}

SEASTAR_TEST_CASE(test_three_rows_first_continuous2) {
    return seastar::async([] {
        test_three_rows(1, true, is_continuous::yes, 3, false, is_continuous::no, 5, true, is_continuous::no, make_slice({ query::clustering_range::make(make_ck(2), make_ck(6)) }), { 3, 5 }, {
            expected_row(1, is_continuous::yes),
            before_notc(2),
            expected_row(3, is_continuous::yes),
            expected_row(5, is_continuous::yes),
            after_cont(6),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::no)
        });
    });
}

SEASTAR_TEST_CASE(test_three_rows_first_continuous3) {
    return seastar::async([] {
        test_three_rows(1, true, is_continuous::yes, 3, false, is_continuous::no, 5, true, is_continuous::no, make_slice({ query::clustering_range::make(make_ck(0), make_ck(4)) }), { 1, 3 }, {
            expected_row(1, is_continuous::yes),
            expected_row(3, is_continuous::yes),
            after_cont(4),
            expected_row(5, is_continuous::no),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::no)
        });
    });
}

SEASTAR_TEST_CASE(test_three_rows_first_continuous4) {
    return seastar::async([] {
        test_three_rows(1, true, is_continuous::yes, 3, false, is_continuous::no, 5, true, is_continuous::no, make_slice({ query::clustering_range::make(make_ck(2), make_ck(4)) }), { 3 }, {
            expected_row(1, is_continuous::yes),
            before_notc(2),
            expected_row(3, is_continuous::yes),
            after_cont(4),
            expected_row(5, is_continuous::no),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::no)
        });
    });
}

SEASTAR_TEST_CASE(test_three_rows_first_noncontinuous1) {
    return seastar::async([] {
        test_three_rows(1, true, is_continuous::no, 3, false, is_continuous::no, 5, true, is_continuous::no, make_slice({ query::clustering_range::make(make_ck(0), make_ck(6)) }), { 1, 3, 5 }, {
            before_notc(0),
            expected_row(1, is_continuous::yes),
            expected_row(3, is_continuous::yes),
            expected_row(5, is_continuous::yes),
            after_cont(6),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::no)
        });
    });
}

SEASTAR_TEST_CASE(test_three_rows_first_noncontinuous2) {
    return seastar::async([] {
        test_three_rows(1, true, is_continuous::no, 3, false, is_continuous::no, 5, true, is_continuous::no, make_slice({ query::clustering_range::make(make_ck(2), make_ck(6)) }), { 3, 5 }, {
            expected_row(1, is_continuous::no),
            before_notc(2),
            expected_row(3, is_continuous::yes),
            expected_row(5, is_continuous::yes),
            after_cont(6),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::no)
        });
    });
}

SEASTAR_TEST_CASE(test_three_rows_first_nonecontinuous3) {
    return seastar::async([] {
        test_three_rows(1, true, is_continuous::no, 3, false, is_continuous::no, 5, true, is_continuous::no, make_slice({ query::clustering_range::make(make_ck(0), make_ck(4)) }), { 1, 3 }, {
            before_notc(0),
            expected_row(1, is_continuous::yes),
            expected_row(3, is_continuous::yes),
            after_cont(4),
            expected_row(5, is_continuous::no),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::no)
        });
    });
}

SEASTAR_TEST_CASE(test_three_rows_first_nonecontinuous4) {
    return seastar::async([] {
        test_three_rows(1, true, is_continuous::no, 3, false, is_continuous::no, 5, true, is_continuous::no, make_slice({ query::clustering_range::make(make_ck(2), make_ck(4)) }), { 3 }, {
            expected_row(1, is_continuous::no),
            before_notc(2),
            expected_row(3, is_continuous::yes),
            after_cont(4),
            expected_row(5, is_continuous::no),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::no)
        });
    });
}

/*
 * ================================================================================================
 * ====== Tests for single rows and range tombstone with single version and single row range ======
 * ================================================================================================
 */

static tombstone new_tombstone() {
    return tombstone(new_timestamp(), gc_clock::now());
}

SEASTAR_TEST_CASE(test_single_row_and_tombstone_not_cached_single_row_range1) {
    return seastar::async([] {
        const int ck1 = 1;
        const int value1 = 12;
        range_tombstone rt(make_ck(0), bound_kind::incl_start, make_ck(2), bound_kind::incl_end, new_tombstone());

        mutation underlying(SCHEMA, PK);
        add_row(underlying, ck1, value1);
        add_tombstone(underlying, rt);

        auto cache = make_incomplete_mutation();
        auto slice = make_slice({ query::clustering_range::make_singular(make_ck(ck1)) });

        test_slice_single_version(underlying, cache, slice, {
            expected_fragment(position_in_partition::before_key(make_ck(1)), rt.tomb),
            expected_fragment(1),
            expected_fragment(position_in_partition_view::after_all_prefixed(make_ck(1)), {}),
        }, {
            before_notc(1),
            expected_row(1, is_continuous::yes),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::no)
        }, { rt });
    });
}

SEASTAR_TEST_CASE(test_single_row_and_tombstone_not_cached_single_row_range2) {
    return seastar::async([] {
        const int ck1 = 1;
        const int value1 = 12;
        range_tombstone rt(make_ck(0), bound_kind::incl_start, make_ck(2), bound_kind::incl_end, new_tombstone());

        mutation underlying(SCHEMA, PK);
        add_row(underlying, ck1, value1);
        add_tombstone(underlying, rt);

        auto cache = make_incomplete_mutation();
        auto slice = make_slice({ query::clustering_range::make(make_ck(0), {make_ck(1), false}) });

        test_slice_single_version(underlying, cache, slice, {
            expected_fragment(position_in_partition_view::before_key(make_ck(0)), rt.tomb),
            expected_fragment(position_in_partition_view::before_key(make_ck(1)), {}),
        }, {
            before_notc(0),
            before_cont(1),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::no)
        }, { rt });
    });
}

SEASTAR_TEST_CASE(test_single_row_and_tombstone_not_cached_single_row_range3) {
    return seastar::async([] {
        const int ck1 = 4;
        const int value1 = 12;
        range_tombstone rt(make_ck(0), bound_kind::incl_start, make_ck(2), bound_kind::incl_end, new_tombstone());

        mutation underlying(SCHEMA, PK);
        add_row(underlying, ck1, value1);
        add_tombstone(underlying, rt);

        auto cache = make_incomplete_mutation();
        auto slice = make_slice({ query::clustering_range::make(make_ck(0), make_ck(5)) });

        test_slice_single_version(underlying, cache, slice, {
            expected_fragment(position_in_partition_view::before_key(make_ck(0)), rt.tomb),
            expected_fragment(position_in_partition_view::after_all_prefixed(make_ck(2)), {}),
            expected_fragment(4)
        }, {
            before_notc(0),
            after_cont(2), // upper bound of rt
            expected_row(4, is_continuous::yes),
            after_cont(5),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::no)
        }, { rt });
    });
}

SEASTAR_TEST_CASE(test_single_row_and_tombstone_not_cached_single_row_range4) {
    return seastar::async([] {
        const int ck1 = 4;
        const int value1 = 12;
        range_tombstone rt(make_ck(0), bound_kind::incl_start, make_ck(2), bound_kind::incl_end, new_tombstone());

        mutation underlying(SCHEMA, PK);
        add_row(underlying, ck1, value1);
        add_tombstone(underlying, rt);

        auto cache = make_incomplete_mutation();
        auto slice = make_slice({ query::clustering_range::make(make_ck(3), make_ck(5)) });

        test_slice_single_version(underlying, cache, slice, {
            expected_fragment(4)
        }, {
            before_notc(3),
            expected_row(4, is_continuous::yes),
            after_cont(5),
            expected_row(expected_row::dummy_tag_t{}, is_continuous::no)
        }, {});
    });
}
