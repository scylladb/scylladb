/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <boost/test/unit_test.hpp>

#include <fmt/format.h>

#include "utils/cidr.hh"

using namespace utils;
using seastar::net::inet_address;

static bool contains(std::string_view prefix, std::string_view address) {
    auto c = cidr::parse(prefix);
    BOOST_REQUIRE_MESSAGE(c, fmt::format("{} did not parse", prefix));
    return c->contains(inet_address(seastar::sstring(address)));
}

BOOST_AUTO_TEST_SUITE(cidr_test)

BOOST_AUTO_TEST_CASE(test_parse_rejects_what_is_not_a_prefix) {
    // No length at all: a plain address is not a prefix, and a caller that accepts both
    // has to be able to tell them apart.
    BOOST_REQUIRE(!cidr::parse("10.0.0.0"));
    BOOST_REQUIRE(!cidr::parse("10.0.0.0/"));
    BOOST_REQUIRE(!cidr::parse("/8"));
    BOOST_REQUIRE(!cidr::parse(""));
    // Not a number, or not only a number.
    BOOST_REQUIRE(!cidr::parse("10.0.0.0/eight"));
    BOOST_REQUIRE(!cidr::parse("10.0.0.0/8x"));
    BOOST_REQUIRE(!cidr::parse("10.0.0.0/ 8"));
    // Longer than the address has bits.
    BOOST_REQUIRE(!cidr::parse("10.0.0.0/33"));
    BOOST_REQUIRE(!cidr::parse("fd00::/129"));
    // Not an address. A name is not resolved, so it does not parse either.
    BOOST_REQUIRE(!cidr::parse("300.0.0.0/8"));
    BOOST_REQUIRE(!cidr::parse("localhost/8"));
}

BOOST_AUTO_TEST_CASE(test_parse_accepts_the_edges) {
    BOOST_REQUIRE(cidr::parse("0.0.0.0/0"));
    BOOST_REQUIRE(cidr::parse("10.1.2.3/32"));
    BOOST_REQUIRE(cidr::parse("::/0"));
    BOOST_REQUIRE(cidr::parse("fd00::1/128"));
}

BOOST_AUTO_TEST_CASE(test_contains_on_byte_boundaries) {
    BOOST_REQUIRE(contains("10.0.0.0/8", "10.0.0.1"));
    BOOST_REQUIRE(contains("10.0.0.0/8", "10.255.255.255"));
    BOOST_REQUIRE(!contains("10.0.0.0/8", "11.0.0.1"));

    BOOST_REQUIRE(contains("192.168.1.0/24", "192.168.1.7"));
    BOOST_REQUIRE(!contains("192.168.1.0/24", "192.168.2.7"));
}

BOOST_AUTO_TEST_CASE(test_contains_inside_a_byte) {
    // /12 stops four bits into the second byte: 172.16 through 172.31 are in, 172.32 is
    // not. This is the case a whole-byte comparison would get wrong.
    BOOST_REQUIRE(contains("172.16.0.0/12", "172.16.0.1"));
    BOOST_REQUIRE(contains("172.16.0.0/12", "172.31.255.254"));
    BOOST_REQUIRE(!contains("172.16.0.0/12", "172.32.0.1"));
    BOOST_REQUIRE(!contains("172.16.0.0/12", "172.15.255.254"));

    // /1 and /7: the leftover bits are in the very first byte.
    BOOST_REQUIRE(contains("0.0.0.0/1", "127.255.255.255"));
    BOOST_REQUIRE(!contains("0.0.0.0/1", "128.0.0.1"));
}

BOOST_AUTO_TEST_CASE(test_contains_everything_and_exactly_one) {
    BOOST_REQUIRE(contains("0.0.0.0/0", "1.2.3.4"));
    BOOST_REQUIRE(contains("0.0.0.0/0", "255.255.255.255"));

    BOOST_REQUIRE(contains("10.1.2.3/32", "10.1.2.3"));
    BOOST_REQUIRE(!contains("10.1.2.3/32", "10.1.2.4"));
}

BOOST_AUTO_TEST_CASE(test_contains_ipv6) {
    BOOST_REQUIRE(contains("fd00::/8", "fd00::1"));
    BOOST_REQUIRE(contains("fd00::/8", "fdff:ffff::1"));
    BOOST_REQUIRE(!contains("fd00::/8", "fe00::1"));

    BOOST_REQUIRE(contains("2001:db8::/32", "2001:db8::1"));
    BOOST_REQUIRE(!contains("2001:db8::/32", "2001:db9::1"));
}

BOOST_AUTO_TEST_CASE(test_families_do_not_mix) {
    // ::ffff:10.0.0.1 is the v4-mapped form of 10.0.0.1, and is still not inside the v4
    // prefix: a configuration that names the 10.0.0.0/8 network means the v4 one.
    BOOST_REQUIRE(!contains("10.0.0.0/8", "::ffff:10.0.0.1"));
    BOOST_REQUIRE(!contains("::/0", "10.0.0.1"));
    // /0 matches everything of its own family, and nothing of the other.
    BOOST_REQUIRE(contains("0.0.0.0/0", "10.0.0.1"));
}

BOOST_AUTO_TEST_CASE(test_is_cidr) {
    BOOST_REQUIRE(is_cidr("10.0.0.0/8"));
    BOOST_REQUIRE(!is_cidr("10.0.0.1"));
    BOOST_REQUIRE(!is_cidr("some.host.name"));
}

BOOST_AUTO_TEST_SUITE_END()
