/*
 * Copyright (C) 2017-present ScyllaDB
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

#define BOOST_TEST_MODULE core

#include "duration.hh"

#include <boost/test/unit_test.hpp>

#include <string_view>
#include "test/lib/exception_utils.hh"

namespace {

//
// To avoid confusing literals.
//

constexpr auto ns_per_us = 1000L;
constexpr auto ns_per_ms = ns_per_us * 1000L;
constexpr auto ns_per_s = ns_per_ms * 1000L;
constexpr auto ns_per_m = ns_per_s * 60L;
constexpr auto ns_per_h = ns_per_m * 60L;

// Normally we want to be explict, but brevity is nice for tests.
constexpr cql_duration make_duration(months_counter::value_type m,
                                 days_counter::value_type d,
                                 nanoseconds_counter::value_type ns) noexcept {
    return {months_counter(m), days_counter(d), nanoseconds_counter(ns)};
}

}

BOOST_AUTO_TEST_CASE(parse_standard) {
    BOOST_REQUIRE_EQUAL(make_duration(14, 0, 0), cql_duration("1y2mo"));
    BOOST_REQUIRE_EQUAL(make_duration(-14, 0, 0), cql_duration("-1y2mo"));
    BOOST_REQUIRE_EQUAL(make_duration(14, 0, 0), cql_duration("1Y2MO"));
    BOOST_REQUIRE_EQUAL(make_duration(0, 14, 0), cql_duration("2w"));
    BOOST_REQUIRE_EQUAL(make_duration(0, 2, 10 * ns_per_h), cql_duration("2d10h"));
    BOOST_REQUIRE_EQUAL(make_duration(0, 2, 0), cql_duration("2d"));
    BOOST_REQUIRE_EQUAL(make_duration(0, 0, 30 * ns_per_h), cql_duration("30h"));
    BOOST_REQUIRE_EQUAL(make_duration(0, 0, (30 * ns_per_h) + (20 * ns_per_m)), cql_duration("30h20m"));
    BOOST_REQUIRE_EQUAL(make_duration(0, 0, 20 * ns_per_m), cql_duration("20m"));
    BOOST_REQUIRE_EQUAL(make_duration(0, 0, 56 * ns_per_s), cql_duration("56s"));
    BOOST_REQUIRE_EQUAL(make_duration(0, 0, 567 * ns_per_ms), cql_duration("567ms"));
    BOOST_REQUIRE_EQUAL(make_duration(0, 0, 1950 * ns_per_us), cql_duration("1950us"));
    BOOST_REQUIRE_EQUAL(make_duration(0, 0, 1950 * ns_per_us), cql_duration("1950µs"));
    BOOST_REQUIRE_EQUAL(make_duration(0, 0, 1950000), cql_duration("1950000ns"));
    BOOST_REQUIRE_EQUAL(make_duration(0, 0, 1950000), cql_duration("1950000NS"));
    BOOST_REQUIRE_EQUAL(make_duration(0, 0, -1950000), cql_duration("-1950000ns"));
    BOOST_REQUIRE_EQUAL(make_duration(15, 0, 130 * ns_per_m), cql_duration("1y3mo2h10m"));
}

using exception_predicate::message_equals;

BOOST_AUTO_TEST_CASE(parse_standard_syntax_error) {
    // Read the entire input.
    BOOST_REQUIRE_EXCEPTION(cql_duration("1y500"), cql_duration_error,
                            message_equals("Unable to convert '1y500' to a duration"));

    // Do not skip invalid characters in the middle.
    BOOST_REQUIRE_EXCEPTION(cql_duration("1y  xxx  500d"), cql_duration_error,
                            message_equals("Unable to convert '1y  xxx  500d' to a duration"));

    // Do not skip invalid characters at the beginning.
    BOOST_REQUIRE_EXCEPTION(cql_duration("xxx1y500d"), cql_duration_error,
                            message_equals("Unable to convert 'xxx1y500d' to a duration"));
}

BOOST_AUTO_TEST_CASE(parse_standard_order_error) {
    BOOST_REQUIRE_EXCEPTION(cql_duration("20s1h3m"), cql_duration_error,
                            message_equals("Invalid duration. The seconds should be after hours"));
}

BOOST_AUTO_TEST_CASE(parse_standard_repeated_error) {
    BOOST_REQUIRE_EXCEPTION(cql_duration("1h2h3m"), cql_duration_error,
        message_equals("Invalid duration. The hours are specified multiple times"));
}

BOOST_AUTO_TEST_CASE(parse_standard_overflow_error) {
    BOOST_REQUIRE_EXCEPTION(cql_duration("178956971y"), cql_duration_error,
        message_equals("Invalid duration. The number of years must be less than or equal to 178956970"));

    BOOST_REQUIRE_EXCEPTION(cql_duration("178956970y14mo"), cql_duration_error,
        message_equals("Invalid duration. The number of months must be less than or equal to 7"));
}

BOOST_AUTO_TEST_CASE(parse_iso8601) {
    BOOST_REQUIRE_EQUAL(make_duration(12, 2, 0), cql_duration("P1Y2D"));
    BOOST_REQUIRE_EQUAL(make_duration(14, 0, 0), cql_duration("P1Y2M"));
    BOOST_REQUIRE_EQUAL(make_duration(0, 14, 0), cql_duration("P2W"));
    BOOST_REQUIRE_EQUAL(make_duration(12, 0, 2 * ns_per_h), cql_duration("P1YT2H"));
    BOOST_REQUIRE_EQUAL(make_duration(-14, 0, 0), cql_duration("-P1Y2M"));
    BOOST_REQUIRE_EQUAL(make_duration(0, 2, 0), cql_duration("P2D"));
    BOOST_REQUIRE_EQUAL(make_duration(0, 0, 30 * ns_per_h), cql_duration("PT30H"));
    BOOST_REQUIRE_EQUAL(make_duration(0, 0, (30 * ns_per_h) + (20 * ns_per_m)), cql_duration("PT30H20M"));
    BOOST_REQUIRE_EQUAL(make_duration(0, 0, 20 * ns_per_m), cql_duration("PT20M"));
    BOOST_REQUIRE_EQUAL(make_duration(0, 0, 56 * ns_per_s), cql_duration("PT56S"));
    BOOST_REQUIRE_EQUAL(make_duration(15, 0, 130 * ns_per_m), cql_duration("P1Y3MT2H10M"));
}

BOOST_AUTO_TEST_CASE(parse_iso8601_syntax_error) {
  BOOST_REQUIRE_EXCEPTION(cql_duration("P2003T23s"), cql_duration_error,
                          message_equals("Unable to convert 'P2003T23s' to a duration"));
}

BOOST_AUTO_TEST_CASE(parse_iso8601_alternative) {
    BOOST_REQUIRE_EQUAL(make_duration(12, 2, 0), cql_duration("P0001-00-02T00:00:00"));
    BOOST_REQUIRE_EQUAL(make_duration(14, 0, 0), cql_duration("P0001-02-00T00:00:00"));
    BOOST_REQUIRE_EQUAL(make_duration(12, 0, 2 * ns_per_h), cql_duration("P0001-00-00T02:00:00"));
    BOOST_REQUIRE_EQUAL(make_duration(-14, 0, 0), cql_duration("-P0001-02-00T00:00:00"));
    BOOST_REQUIRE_EQUAL(make_duration(0, 2, 0), cql_duration("P0000-00-02T00:00:00"));
    BOOST_REQUIRE_EQUAL(make_duration(0, 0, 30 * ns_per_h), cql_duration("P0000-00-00T30:00:00"));
    BOOST_REQUIRE_EQUAL(make_duration(0, 0, (30 * ns_per_h) + (20 * ns_per_m)), cql_duration("P0000-00-00T30:20:00"));
    BOOST_REQUIRE_EQUAL(make_duration(0, 0, 20 * ns_per_m), cql_duration("P0000-00-00T00:20:00"));
    BOOST_REQUIRE_EQUAL(make_duration(0, 0, 56 * ns_per_s), cql_duration("P0000-00-00T00:00:56"));
    BOOST_REQUIRE_EQUAL(make_duration(15, 0, 130 * ns_per_m), cql_duration("P0001-03-00T02:10:00"));
}

BOOST_AUTO_TEST_CASE(parse_iso8601_alternative_syntax_error) {
  BOOST_REQUIRE_EXCEPTION(cql_duration("P0001-00-02T000000"), cql_duration_error,
                          message_equals("Unable to convert 'P0001-00-02T000000' to a duration"));
}

BOOST_AUTO_TEST_CASE(parse_component_overflow) {
  BOOST_REQUIRE_EXCEPTION(cql_duration("10000000000000000000000000000000000m"), cql_duration_error,
                          message_equals("Invalid duration. The count for the minutes is out of range"));

  BOOST_REQUIRE_EXCEPTION(cql_duration("P10000000000000000000000000000000000Y5D"), cql_duration_error,
                          message_equals("Invalid duration. The count for the years is out of range"));
}

BOOST_AUTO_TEST_CASE(pretty_print) {
    BOOST_REQUIRE_EQUAL(to_string(cql_duration("1y3d")), "1y3d");
    BOOST_REQUIRE_EQUAL(to_string(cql_duration("25mo")), "2y1mo");
    BOOST_REQUIRE_EQUAL(to_string(cql_duration("1y2mo3w4d5h6m7s8ms9us10ns")), "1y2mo25d5h6m7s8ms9us10ns");
    BOOST_REQUIRE_EQUAL(to_string(cql_duration("-1d5m")), "-1d5m");
    BOOST_REQUIRE_EQUAL(to_string(cql_duration("0d")), "");
}

BOOST_AUTO_TEST_CASE(equality) {
    BOOST_REQUIRE_EQUAL(make_duration(1, 2, 3), make_duration(1, 2, 3));
    BOOST_REQUIRE_NE(make_duration(1, 2, 3), make_duration(1, 2, 4));
}
