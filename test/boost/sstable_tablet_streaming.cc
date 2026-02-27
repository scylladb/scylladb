/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>
#include "dht/token.hh"
#include "sstable_test.hh"
#include "sstables_loader.hh"
#include "test/lib/sstable_test_env.hh"

BOOST_AUTO_TEST_SUITE(sstable_tablet_streaming_test)

using namespace sstables;

std::vector<shared_sstable> make_sstables_with_ranges(test_env& env, const std::vector<std::pair<int64_t, int64_t>>& ranges) {
    std::vector<shared_sstable> ssts;
    for (const auto& [first, last] : ranges) {
        auto sst = env.make_sstable(uncompressed_schema(), uncompressed_dir());
        test(sst).set_first_and_last_keys(dht::decorated_key(dht::token{first}, partition_key(std::vector<bytes>{"1"})),
                                          dht::decorated_key(dht::token{last}, partition_key(std::vector<bytes>{"1"})));
        ssts.push_back(std::move(sst));
    }
    // By sorting SSTables by their primary key, we enable runs to be
    // streamed incrementally. Overlapping fragments can be deduplicated,
    // reducing the amount of data sent over the wire. Elements are
    // popped from the back of the vector, so we sort in descending
    // order to begin with the smaller tokens.
    // See sstable_streamer constructor for more details.
    std::ranges::sort(ssts, [](const shared_sstable& x, const shared_sstable& y) { return x->compare_by_first_key(*y) > 0; });
    return ssts;
}

std::vector<dht::token_range> get_tablet_sstable_collection(auto&&... tablet_ranges) {
    // tablet ranges are left-non-inclusive, see `tablet_map::get_token_range` for details
    std::vector<dht::token_range> collections{dht::token_range::make({tablet_ranges.start()->value(), false}, {tablet_ranges.end()->value(), true})...};

    std::sort(collections.begin(), collections.end(), [](auto const& a, auto const& b) { return a.start()->value() < b.start()->value(); });

    return collections;
}

#define REQUIRE_WITH_CONTEXT(sstables, expected_size)                                                                                                          \
    BOOST_TEST_CONTEXT("Testing with ranges: " << [&] {                                                                                                        \
        std::stringstream ss;                                                                                                                                  \
        for (const auto& sst : (sstables)) {                                                                                                                   \
            ss << dht::token_range(sst->get_first_decorated_key().token(), sst->get_last_decorated_key().token()) << ", ";                                     \
        }                                                                                                                                                      \
        return ss.str();                                                                                                                                       \
    }())                                                                                                                                                       \
    BOOST_REQUIRE_EQUAL(sstables.size(), expected_size)

SEASTAR_TEST_CASE(test_streaming_ranges_distribution) {
    return test_env::do_with_async([](test_env& env) {
        // 1) Exact boundary equality: SSTable == tablet
        {
            auto collection = get_tablet_sstable_collection(dht::token_range{dht::token{5}, dht::token{10}});
            auto ssts = make_sstables_with_ranges(env,
                                                  {
                                                      {5, 10},
                                                  });
            auto res = get_sstables_for_tablets_for_tests(ssts, std::move(collection)).get();
            REQUIRE_WITH_CONTEXT(res[0].sstables_fully_contained, 0);
            REQUIRE_WITH_CONTEXT(res[0].sstables_partially_contained, 1);
        }

        // 2) Single-point overlaps at start/end
        {
            auto collection = get_tablet_sstable_collection(dht::token_range{dht::token{5}, dht::token{10}});
            auto ssts = make_sstables_with_ranges(env,
                                                  {
                                                      {4, 5},   // touches start, non-inclusive, skip
                                                      {10, 11}, // touches end
                                                  });
            auto res = get_sstables_for_tablets_for_tests(ssts, std::move(collection)).get();
            REQUIRE_WITH_CONTEXT(res[0].sstables_fully_contained, 0);
            REQUIRE_WITH_CONTEXT(res[0].sstables_partially_contained, 1);
        }

        // 3) Tablet fully inside a large SSTable
        {
            auto collection = get_tablet_sstable_collection(dht::token_range{dht::token{5}, dht::token{10}});
            auto ssts = make_sstables_with_ranges(env,
                                                  {
                                                      {0, 20},
                                                  });
            auto res = get_sstables_for_tablets_for_tests(ssts, std::move(collection)).get();
            REQUIRE_WITH_CONTEXT(res[0].sstables_fully_contained, 0);
            REQUIRE_WITH_CONTEXT(res[0].sstables_partially_contained, 1);
        }

        // 4) Multiple SSTables fully contained in tablet
        {
            auto collection = get_tablet_sstable_collection(dht::token_range{dht::token{5}, dht::token{10}});
            auto ssts = make_sstables_with_ranges(env,
                                                  {
                                                      {6, 7},
                                                      {7, 8},
                                                      {8, 9},
                                                  });
            auto res = get_sstables_for_tablets_for_tests(ssts, std::move(collection)).get();
            REQUIRE_WITH_CONTEXT(res[0].sstables_fully_contained, 3);
            REQUIRE_WITH_CONTEXT(res[0].sstables_partially_contained, 0);
        }

        // 5) Two overlapping but not fully contained SSTables
        {
            auto collection = get_tablet_sstable_collection(dht::token_range{dht::token{5}, dht::token{10}});
            auto ssts = make_sstables_with_ranges(env,
                                                  {
                                                      {0, 6},  // overlaps at left
                                                      {9, 15}, // overlaps at right
                                                  });
            auto res = get_sstables_for_tablets_for_tests(ssts, std::move(collection)).get();
            REQUIRE_WITH_CONTEXT(res[0].sstables_fully_contained, 0);
            REQUIRE_WITH_CONTEXT(res[0].sstables_partially_contained, 2);
        }

        // 6) Unsorted input (helper sorts) + mixed overlaps
        {
            auto collection = get_tablet_sstable_collection(dht::token_range{dht::token{50}, dht::token{100}});
            // Intentionally unsorted by first token
            auto ssts = make_sstables_with_ranges(env,
                                                  {
                                                      {120, 130},
                                                      {0, 10},
                                                      {60, 70},  // fully contained
                                                      {40, 55},  // partial
                                                      {95, 105}, // partial
                                                      {80, 90},  // fully contained
                                                  });
            auto res = get_sstables_for_tablets_for_tests(ssts, std::move(collection)).get();
            REQUIRE_WITH_CONTEXT(res[0].sstables_fully_contained, 2);
            REQUIRE_WITH_CONTEXT(res[0].sstables_partially_contained, 2);
        }

        // 7) Empty SSTable list
        {
            auto collection = get_tablet_sstable_collection(dht::token_range{dht::token{5}, dht::token{10}});
            std::vector<shared_sstable> ssts;
            auto res = get_sstables_for_tablets_for_tests(ssts, std::move(collection)).get();
            REQUIRE_WITH_CONTEXT(res[0].sstables_fully_contained, 0);
            REQUIRE_WITH_CONTEXT(res[0].sstables_partially_contained, 0);
        }

        // 8) Tablet outside all SSTables
        {
            auto collection = get_tablet_sstable_collection(dht::token_range{dht::token{100}, dht::token{200}});
            auto ssts = make_sstables_with_ranges(env,
                                                  {
                                                      {1, 2},
                                                      {3, 4},
                                                      {10, 20},
                                                      {300, 400},
                                                  });
            auto res = get_sstables_for_tablets_for_tests(ssts, std::move(collection)).get();
            REQUIRE_WITH_CONTEXT(res[0].sstables_fully_contained, 0);
            REQUIRE_WITH_CONTEXT(res[0].sstables_partially_contained, 0);
        }

        // 9) Boundary adjacency with multiple fragments
        {
            auto collection = get_tablet_sstable_collection(dht::token_range{dht::token{100}, dht::token{200}});
            auto ssts = make_sstables_with_ranges(env,
                                                  {
                                                      {50, 100},  // touches start -> non-inclusive, skip
                                                      {100, 120}, // starts at start -> partially contained
                                                      {180, 200}, // ends at end   -> fully contained
                                                      {200, 220}, // touches end   -> partial
                                                  });
            auto res = get_sstables_for_tablets_for_tests(ssts, std::move(collection)).get();
            REQUIRE_WITH_CONTEXT(res[0].sstables_fully_contained, 1);
            REQUIRE_WITH_CONTEXT(res[0].sstables_partially_contained, 2);
        }

        // 10) Large SSTable set where early break should occur
        {
            auto collection = get_tablet_sstable_collection(dht::token_range{dht::token{1000}, dht::token{2000}});
            auto ssts = make_sstables_with_ranges(env,
                                                  {
                                                      {100, 200},
                                                      {300, 400},
                                                      {900, 950},
                                                      {1001, 1100}, // fully contained
                                                      {1500, 1600}, // fully contained
                                                      {2101, 2200}, // entirely after -> should trigger early break in ascending scan
                                                      {1999, 2100}, // overlap, partially contained
                                                      {3000, 3100},
                                                  });
            auto res = get_sstables_for_tablets_for_tests(ssts, std::move(collection)).get();
            REQUIRE_WITH_CONTEXT(res[0].sstables_fully_contained, 2);
            REQUIRE_WITH_CONTEXT(res[0].sstables_partially_contained, 1);
        }

        // 10) https://github.com/scylladb/scylladb/pull/26980 example, tested
        {
            auto collection = get_tablet_sstable_collection(dht::token_range{dht::token{4}, dht::token{5}});
            auto ssts = make_sstables_with_ranges(env,
                                                  {
                                                      {0, 5},
                                                      {0, 3},
                                                      {2, 5},
                                                  });
            auto res = get_sstables_for_tablets_for_tests(ssts, std::move(collection)).get();
            // None fully contained; three partial overlaps
            REQUIRE_WITH_CONTEXT(res[0].sstables_fully_contained, 0);
            REQUIRE_WITH_CONTEXT(res[0].sstables_partially_contained, 2);
        }
    });
}

SEASTAR_TEST_CASE(test_streaming_ranges_distribution_in_tablets) {
    return test_env::do_with_async([](test_env& env) {
        {
            auto collection = get_tablet_sstable_collection(dht::token_range{dht::token{5}, dht::token{10}}, dht::token_range{dht::token{11}, dht::token{15}});
            auto ssts = make_sstables_with_ranges(env,
                                                  {
                                                      {5, 10},
                                                  });
            auto res = get_sstables_for_tablets_for_tests(ssts, std::move(collection)).get();
            REQUIRE_WITH_CONTEXT(res[0].sstables_fully_contained, 0);
            REQUIRE_WITH_CONTEXT(res[0].sstables_partially_contained, 1);
            REQUIRE_WITH_CONTEXT(res[1].sstables_fully_contained, 0);
            REQUIRE_WITH_CONTEXT(res[1].sstables_partially_contained, 0);
        }

        {
            // Multiple tablets with a hole between [10,11]
            auto collection = get_tablet_sstable_collection(dht::token_range{dht::token{0}, dht::token{4}},
                                                            dht::token_range{dht::token{5}, dht::token{9}},
                                                            dht::token_range{dht::token{12}, dht::token{15}});
            auto ssts = make_sstables_with_ranges(env,
                                                  {
                                                      {0, 4},   // T.start==S.start, but non-inclusive -> partial
                                                      {5, 9},   // same as above
                                                      {6, 8},   // fully in second tablet
                                                      {10, 11}, // falls in the hole, should be rejected
                                                      {8, 13},  // overlaps second and third tablets (partial in both)
                                                  });
            auto res = get_sstables_for_tablets_for_tests(ssts, std::move(collection)).get();

            REQUIRE_WITH_CONTEXT(res[0].sstables_fully_contained, 0);
            REQUIRE_WITH_CONTEXT(res[0].sstables_partially_contained, 1);

            REQUIRE_WITH_CONTEXT(res[1].sstables_fully_contained, 1);
            REQUIRE_WITH_CONTEXT(res[1].sstables_partially_contained, 2);

            REQUIRE_WITH_CONTEXT(res[2].sstables_fully_contained, 0);
            REQUIRE_WITH_CONTEXT(res[2].sstables_partially_contained, 1);
        }

        {
            // SSTables outside any tablet range
            auto collection = get_tablet_sstable_collection(dht::token_range{dht::token{20}, dht::token{25}});
            auto ssts = make_sstables_with_ranges(env,
                                                  {
                                                      {0, 5},   // before
                                                      {30, 35}, // after
                                                  });
            auto res = get_sstables_for_tablets_for_tests(ssts, std::move(collection)).get();

            REQUIRE_WITH_CONTEXT(res[0].sstables_fully_contained, 0);
            REQUIRE_WITH_CONTEXT(res[0].sstables_partially_contained, 0);
        }

        {
            // Edge case: SSTable touching tablet boundary
            auto collection = get_tablet_sstable_collection(dht::token_range{dht::token{5}, dht::token{10}});
            auto ssts = make_sstables_with_ranges(env,
                                                  {
                                                      {4, 5},   // touches start, non-inclusive, skip
                                                      {10, 11}, // touches end
                                                  });
            auto res = get_sstables_for_tablets_for_tests(ssts, std::move(collection)).get();

            REQUIRE_WITH_CONTEXT(res[0].sstables_fully_contained, 0);
            REQUIRE_WITH_CONTEXT(res[0].sstables_partially_contained, 1);
        }

        {
            // No tablets, but some SSTables
            auto collection = get_tablet_sstable_collection();
            auto ssts = make_sstables_with_ranges(env,
                                                  {
                                                      {0, 5},
                                                      {10, 15},
                                                  });
            auto res = get_sstables_for_tablets_for_tests(ssts, std::move(collection)).get();
            BOOST_REQUIRE_EQUAL(res.size(), 0); // no tablets → nothing to classify
        }

        {
            // No SSTables, but some tablets
            auto collection = get_tablet_sstable_collection(dht::token_range{dht::token{0}, dht::token{5}}, dht::token_range{dht::token{10}, dht::token{15}});
            std::vector<shared_sstable> ssts; // empty
            auto res = get_sstables_for_tablets_for_tests(ssts, std::move(collection)).get();

            REQUIRE_WITH_CONTEXT(res[0].sstables_fully_contained, 0);
            REQUIRE_WITH_CONTEXT(res[0].sstables_partially_contained, 0);
            REQUIRE_WITH_CONTEXT(res[1].sstables_fully_contained, 0);
            REQUIRE_WITH_CONTEXT(res[1].sstables_partially_contained, 0);
        }

        {
            // No tablets and no SSTables
            auto collection = get_tablet_sstable_collection();
            std::vector<shared_sstable> ssts; // empty
            auto res = get_sstables_for_tablets_for_tests(ssts, std::move(collection)).get();
            BOOST_REQUIRE_EQUAL(res.size(), 0);
        }
        {
            // SSTable spanning two tablets
            auto collection = get_tablet_sstable_collection(dht::token_range{dht::token{0}, dht::token{4}}, dht::token_range{dht::token{5}, dht::token{9}});
            auto ssts = make_sstables_with_ranges(env,
                                                  {
                                                      {2, 7}, // spans both tablets
                                                  });
            auto res = get_sstables_for_tablets_for_tests(ssts, std::move(collection)).get();

            // Tablet [0,4] sees partial overlap
            REQUIRE_WITH_CONTEXT(res[0].sstables_fully_contained, 0);
            REQUIRE_WITH_CONTEXT(res[0].sstables_partially_contained, 1);

            // Tablet [5,9] sees partial overlap
            REQUIRE_WITH_CONTEXT(res[1].sstables_fully_contained, 0);
            REQUIRE_WITH_CONTEXT(res[1].sstables_partially_contained, 1);
        }

        {
            // SSTable spanning three tablets with a hole in between
            auto collection = get_tablet_sstable_collection(dht::token_range{dht::token{0}, dht::token{3}},
                                                            dht::token_range{dht::token{4}, dht::token{6}},
                                                            dht::token_range{dht::token{8}, dht::token{10}});
            auto ssts = make_sstables_with_ranges(env,
                                                  {
                                                      {2, 9}, // spans across tablets 1,2,3 and hole [7]
                                                  });
            auto res = get_sstables_for_tablets_for_tests(ssts, std::move(collection)).get();

            REQUIRE_WITH_CONTEXT(res[0].sstables_partially_contained, 1);
            REQUIRE_WITH_CONTEXT(res[1].sstables_partially_contained, 1);
            REQUIRE_WITH_CONTEXT(res[2].sstables_partially_contained, 1);
        }

        {
            // SSTable fully covering one tablet and partially overlapping another
            auto collection = get_tablet_sstable_collection(dht::token_range{dht::token{0}, dht::token{5}}, dht::token_range{dht::token{6}, dht::token{10}});
            auto ssts = make_sstables_with_ranges(env,
                                                  {
                                                      {0, 7}, // fully covers first tablet, partial in second
                                                  });
            auto res = get_sstables_for_tablets_for_tests(ssts, std::move(collection)).get();

            REQUIRE_WITH_CONTEXT(res[0].sstables_fully_contained, 0);
            REQUIRE_WITH_CONTEXT(res[0].sstables_partially_contained, 1);

            REQUIRE_WITH_CONTEXT(res[1].sstables_fully_contained, 0);
            REQUIRE_WITH_CONTEXT(res[1].sstables_partially_contained, 1);
        }
    });
}

BOOST_AUTO_TEST_SUITE_END()
