/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#define BOOST_TEST_MODULE core

#include <boost/test/unit_test.hpp>
#include "utils/http.hh"

using namespace utils::http;

BOOST_AUTO_TEST_CASE(test_parse_ipv6) {

    static const std::string ipv6_addr_str = "2001:db8:4006:812::200e";
    auto info = parse_simple_url("http://[" + ipv6_addr_str + "]:8080");

    BOOST_CHECK_EQUAL(info.host, ipv6_addr_str);
    BOOST_CHECK_EQUAL(info.scheme, "http");
    BOOST_CHECK(!info.is_https());
    BOOST_CHECK_EQUAL(info.port, 8080);
    BOOST_CHECK_EQUAL(info.path, "");
}

BOOST_AUTO_TEST_CASE(test_parse_kmip) {
    auto info = parse_simple_url("kmip://127.0.0.1");

    BOOST_CHECK_EQUAL(info.host, "127.0.0.1");
    BOOST_CHECK_EQUAL(info.scheme, "kmip");
    BOOST_CHECK(!info.is_https());
    BOOST_CHECK_EQUAL(info.port, 80); // default
    BOOST_CHECK_EQUAL(info.path, "");
}

BOOST_AUTO_TEST_CASE(test_parse_https) {
    auto info = parse_simple_url("https://127.0.0.1");

    BOOST_CHECK_EQUAL(info.host, "127.0.0.1");
    BOOST_CHECK_EQUAL(info.scheme, "https");
    BOOST_CHECK(info.is_https());
    BOOST_CHECK_EQUAL(info.port, 443); // default

    info = parse_simple_url("HTTPS://www.apa.org");

    BOOST_CHECK_EQUAL(info.host, "www.apa.org");
    BOOST_CHECK_EQUAL(info.scheme, "HTTPS");
    BOOST_CHECK(info.is_https());
    BOOST_CHECK_EQUAL(info.port, 443); // default
    BOOST_CHECK_EQUAL(info.path, "");
}


BOOST_AUTO_TEST_CASE(test_parse_path) {
    auto info = parse_simple_url("https://127.0.0.1:333/ola/korv");

    BOOST_CHECK_EQUAL(info.host, "127.0.0.1");
    BOOST_CHECK_EQUAL(info.scheme, "https");
    BOOST_CHECK(info.is_https());
    BOOST_CHECK_EQUAL(info.port, 333); // default
    BOOST_CHECK_EQUAL(info.path, "/ola/korv");
}

