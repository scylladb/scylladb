/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <seastar/testing/thread_test_case.hh>
#include "test/lib/scylla_test_case.hh"
#include "test/lib/simple_schema.hh"
#include "query/position_range_utils.hh"
#include "keys/clustering_interval_set.hh"
#include "schema/schema_builder.hh"

using namespace query;

SEASTAR_THREAD_TEST_CASE(test_deoverlap_clustering_row_ranges) {
    simple_schema s;
    
    // Create overlapping ranges
    auto ck1 = s.make_ckey(1);
    auto ck2 = s.make_ckey(2);
    auto ck3 = s.make_ckey(3);
    auto ck4 = s.make_ckey(4);
    
    clustering_row_ranges ranges;
    ranges.push_back(clustering_range::make({ck1, true}, {ck3, true}));  // [1, 3]
    ranges.push_back(clustering_range::make({ck2, true}, {ck4, true}));  // [2, 4]
    
    // Deoverlap should merge these into a single range [1, 4]
    auto deoverlapped = deoverlap_clustering_row_ranges(*s.schema(), ranges);
    
    BOOST_REQUIRE_EQUAL(deoverlapped.size(), 1);
    BOOST_REQUIRE(deoverlapped[0].start());
    BOOST_REQUIRE(deoverlapped[0].end());
    BOOST_REQUIRE(deoverlapped[0].start()->value().equal(*s.schema(), ck1));
    BOOST_REQUIRE(deoverlapped[0].end()->value().equal(*s.schema(), ck4));
}

SEASTAR_THREAD_TEST_CASE(test_deoverlap_with_non_overlapping_ranges) {
    simple_schema s;
    
    auto ck1 = s.make_ckey(1);
    auto ck2 = s.make_ckey(2);
    auto ck3 = s.make_ckey(3);
    auto ck4 = s.make_ckey(4);
    
    clustering_row_ranges ranges;
    ranges.push_back(clustering_range::make({ck1, true}, {ck2, true}));  // [1, 2]
    ranges.push_back(clustering_range::make({ck3, true}, {ck4, true}));  // [3, 4]
    
    // These don't overlap, should remain as two separate ranges
    auto deoverlapped = deoverlap_clustering_row_ranges(*s.schema(), ranges);
    
    BOOST_REQUIRE_EQUAL(deoverlapped.size(), 2);
}

SEASTAR_THREAD_TEST_CASE(test_clustering_row_ranges_conversion) {
    simple_schema s;
    
    auto ck1 = s.make_ckey(1);
    auto ck2 = s.make_ckey(2);
    
    clustering_row_ranges ranges;
    ranges.push_back(clustering_range::make({ck1, true}, {ck2, false}));  // [1, 2)
    
    // Convert to position_ranges and back
    auto pos_ranges = clustering_row_ranges_to_position_ranges(ranges);
    BOOST_REQUIRE_EQUAL(pos_ranges.size(), 1);
    
    auto converted_back = position_ranges_to_clustering_row_ranges(pos_ranges, *s.schema());
    BOOST_REQUIRE_EQUAL(converted_back.size(), 1);
    
    // Check that the conversion is correct
    BOOST_REQUIRE(converted_back[0].start());
    BOOST_REQUIRE(converted_back[0].end());
    BOOST_REQUIRE(converted_back[0].start()->value().equal(*s.schema(), ck1));
    BOOST_REQUIRE(converted_back[0].start()->is_inclusive());
    BOOST_REQUIRE(converted_back[0].end()->value().equal(*s.schema(), ck2));
    BOOST_REQUIRE(!converted_back[0].end()->is_inclusive());
}

SEASTAR_THREAD_TEST_CASE(test_intersect_clustering_row_ranges) {
    simple_schema s;
    
    auto ck1 = s.make_ckey(1);
    auto ck2 = s.make_ckey(2);
    auto ck3 = s.make_ckey(3);
    auto ck4 = s.make_ckey(4);
    
    clustering_row_ranges ranges1;
    ranges1.push_back(clustering_range::make({ck1, true}, {ck3, true}));  // [1, 3]
    
    clustering_row_ranges ranges2;
    ranges2.push_back(clustering_range::make({ck2, true}, {ck4, true}));  // [2, 4]
    
    // Intersection should be [2, 3]
    auto intersected = intersect_clustering_row_ranges(*s.schema(), ranges1, ranges2);
    
    BOOST_REQUIRE_EQUAL(intersected.size(), 1);
    BOOST_REQUIRE(intersected[0].start());
    BOOST_REQUIRE(intersected[0].end());
    BOOST_REQUIRE(intersected[0].start()->value().equal(*s.schema(), ck2));
    BOOST_REQUIRE(intersected[0].end()->value().equal(*s.schema(), ck3));
}

SEASTAR_THREAD_TEST_CASE(test_intersect_non_overlapping_ranges) {
    simple_schema s;
    
    auto ck1 = s.make_ckey(1);
    auto ck2 = s.make_ckey(2);
    auto ck3 = s.make_ckey(3);
    auto ck4 = s.make_ckey(4);
    
    clustering_row_ranges ranges1;
    ranges1.push_back(clustering_range::make({ck1, true}, {ck2, true}));  // [1, 2]
    
    clustering_row_ranges ranges2;
    ranges2.push_back(clustering_range::make({ck3, true}, {ck4, true}));  // [3, 4]
    
    // No overlap, should return empty
    auto intersected = intersect_clustering_row_ranges(*s.schema(), ranges1, ranges2);
    
    BOOST_REQUIRE_EQUAL(intersected.size(), 0);
}
