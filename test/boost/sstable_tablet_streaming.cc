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

#define REQUIRE_WITH_CONTEXT(sstables, current_size, expected_size)                                                                                            \
    BOOST_TEST_CONTEXT("Testing with ranges: " << [&] {                                                                                                        \
        std::stringstream ss;                                                                                                                                  \
        for (const auto& sst : (sstables)) {                                                                                                                   \
            ss << dht::token_range(sst->get_first_decorated_key().token(), sst->get_last_decorated_key().token()) << ", ";                                     \
        }                                                                                                                                                      \
        return ss.str();                                                                                                                                       \
    }())                                                                                                                                                       \
    BOOST_REQUIRE_EQUAL(current_size, expected_size)

SEASTAR_TEST_CASE(test_streaming_ranges_distribution) {
    return test_env::do_with_async([](test_env& env) {
        // 1) Exact boundary equality: SSTable == tablet
        {
            dht::token_range tablet_range{dht::token{5}, dht::token{10}};
            auto ssts = make_sstables_with_ranges(env,
                                                  {
                                                      {5, 10},
                                                  });
            auto [full, partial] = get_sstables_by_tablet_range(ssts, tablet_range).get();
            REQUIRE_WITH_CONTEXT(ssts, full.size(), 1);
            REQUIRE_WITH_CONTEXT(ssts, partial.size(), 0);
        }

        // 2) Single-point overlaps at start/end
        {
            dht::token_range tablet_range{dht::token{5}, dht::token{10}};
            auto ssts = make_sstables_with_ranges(env,
                                                  {
                                                      {4, 5},   // touches start
                                                      {10, 11}, // touches end
                                                  });
            auto [full, partial] = get_sstables_by_tablet_range(ssts, tablet_range).get();
            REQUIRE_WITH_CONTEXT(ssts, full.size(), 0);
            REQUIRE_WITH_CONTEXT(ssts, partial.size(), 2);
        }

        // 3) Tablet fully inside a large SSTable
        {
            dht::token_range tablet_range{dht::token{5}, dht::token{10}};
            auto ssts = make_sstables_with_ranges(env,
                                                  {
                                                      {0, 20},
                                                  });
            auto [full, partial] = get_sstables_by_tablet_range(ssts, tablet_range).get();
            REQUIRE_WITH_CONTEXT(ssts, full.size(), 0);
            REQUIRE_WITH_CONTEXT(ssts, partial.size(), 1);
        }

        // 4) Multiple SSTables fully contained in tablet
        {
            dht::token_range tablet_range{dht::token{5}, dht::token{10}};
            auto ssts = make_sstables_with_ranges(env,
                                                  {
                                                      {6, 7},
                                                      {7, 8},
                                                      {8, 9},
                                                  });
            auto [full, partial] = get_sstables_by_tablet_range(ssts, tablet_range).get();
            REQUIRE_WITH_CONTEXT(ssts, full.size(), 3);
            REQUIRE_WITH_CONTEXT(ssts, partial.size(), 0);
        }

        // 5) Two overlapping but not fully contained SSTables
        {
            dht::token_range tablet_range{dht::token{5}, dht::token{10}};
            auto ssts = make_sstables_with_ranges(env,
                                                  {
                                                      {0, 6},  // overlaps at left
                                                      {9, 15}, // overlaps at right
                                                  });
            auto [full, partial] = get_sstables_by_tablet_range(ssts, tablet_range).get();
            REQUIRE_WITH_CONTEXT(ssts, full.size(), 0);
            REQUIRE_WITH_CONTEXT(ssts, partial.size(), 2);
        }

        // 6) Unsorted input (helper sorts) + mixed overlaps
        {
            dht::token_range tablet_range{dht::token{50}, dht::token{100}};
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
            auto [full, partial] = get_sstables_by_tablet_range(ssts, tablet_range).get();
            REQUIRE_WITH_CONTEXT(ssts, full.size(), 2);
            REQUIRE_WITH_CONTEXT(ssts, partial.size(), 2);
        }

        // 7) Empty SSTable list
        {
            dht::token_range tablet_range{dht::token{5}, dht::token{10}};
            std::vector<shared_sstable> ssts;
            auto [full, partial] = get_sstables_by_tablet_range(ssts, tablet_range).get();
            REQUIRE_WITH_CONTEXT(ssts, full.size(), 0);
            REQUIRE_WITH_CONTEXT(ssts, partial.size(), 0);
        }

        // 8) Tablet outside all SSTables
        {
            dht::token_range tablet_range{dht::token{100}, dht::token{200}};
            auto ssts = make_sstables_with_ranges(env,
                                                  {
                                                      {1, 2},
                                                      {3, 4},
                                                      {10, 20},
                                                      {300, 400},
                                                  });
            auto [full, partial] = get_sstables_by_tablet_range(ssts, tablet_range).get();
            REQUIRE_WITH_CONTEXT(ssts, full.size(), 0);
            REQUIRE_WITH_CONTEXT(ssts, partial.size(), 0);
        }

        // 9) Boundary adjacency with multiple fragments
        {
            dht::token_range tablet_range{dht::token{100}, dht::token{200}};
            auto ssts = make_sstables_with_ranges(env,
                                                  {
                                                      {50, 100},  // touches start -> partial
                                                      {100, 120}, // starts at start -> fully contained
                                                      {180, 200}, // ends at end   -> fully contained
                                                      {200, 220}, // touches end   -> partial
                                                  });
            auto [full, partial] = get_sstables_by_tablet_range(ssts, tablet_range).get();
            REQUIRE_WITH_CONTEXT(ssts, full.size(), 2);
            REQUIRE_WITH_CONTEXT(ssts, partial.size(), 2);
        }

        // 10) Large SSTable set where early break should occur
        {
            dht::token_range tablet_range{dht::token{1000}, dht::token{2000}};
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
            auto [full, partial] = get_sstables_by_tablet_range(ssts, tablet_range).get();
            REQUIRE_WITH_CONTEXT(ssts, full.size(), 2);
            REQUIRE_WITH_CONTEXT(ssts, partial.size(), 1);
        }

        // 10) https://github.com/scylladb/scylladb/pull/26980 example, tested
        {
            dht::token_range tablet_range{dht::token{4}, dht::token{5}};
            auto ssts = make_sstables_with_ranges(env,
                                                  {
                                                      {0, 5},
                                                      {0, 3},
                                                      {2, 5},
                                                  });
            auto [full, partial] = get_sstables_by_tablet_range(ssts, tablet_range).get();
            // None fully contained; three partial overlaps
            REQUIRE_WITH_CONTEXT(ssts, full.size(), 0);
            REQUIRE_WITH_CONTEXT(ssts, partial.size(), 2);
        }
    });
}

BOOST_AUTO_TEST_SUITE_END()
