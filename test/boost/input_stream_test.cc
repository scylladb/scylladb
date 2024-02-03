/*
 * Copyright (C) 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#define BOOST_TEST_MODULE core

#include "utils/input_stream.hh"
#include "bytes_ostream.hh"

#include <boost/test/unit_test.hpp>
#include "test/lib/random_utils.hh"

BOOST_AUTO_TEST_CASE(test_empty_fragmented_stream) {
    bytes_ostream b;
    utils::input_stream::fragmented in(b.fragments().begin(), b.size());
    BOOST_REQUIRE_EQUAL(in.size(), 0);
    try {
        in.skip(5);
        BOOST_FAIL("should've thrown");
    } catch (const std::out_of_range&) {
        // expected
    }

    auto in2 = in;
    BOOST_REQUIRE_EQUAL(in2.size(), 0);

    auto in3 = std::move(in);
    BOOST_REQUIRE_EQUAL(in3.size(), 0);

    bytes_ostream out;
    in3.copy_to(out);
    BOOST_REQUIRE_EQUAL(out.size(), 0);
}

BOOST_AUTO_TEST_CASE(test_fragmented_stream) {
    bytes big_buffer = tests::random::get_bytes(128 * 1024);
    bytes_ostream b;
    b.write(big_buffer);

    utils::input_stream::fragmented in(b.fragments().begin(), b.size());
    BOOST_REQUIRE_EQUAL(in.size(), b.size());

    auto in2 = in;
    in2.skip(big_buffer.size());
    try {
        in2.skip(5);
        BOOST_FAIL("should've thrown");
    } catch (const std::out_of_range&) {
        // expected
    }

    in2 = in;
    bytes_ostream out;
    in2.copy_to(out);
    BOOST_REQUIRE(out == b);

    bytes buf(bytes::initialized_later(), 23400);
    in2 = in;
    in2.skip(14200);
    in2.read(reinterpret_cast<char*>(buf.data()), buf.size());
    BOOST_REQUIRE(std::equal(buf.begin(), buf.end(), big_buffer.begin() + 14200, big_buffer.begin() + 14200 + 23400));

    bytes buf2(bytes::initialized_later(), big_buffer.size());
    in2 = in;
    in2.read(reinterpret_cast<char*>(buf2.data()), buf2.size());
    BOOST_REQUIRE(std::equal(buf2.begin(), buf2.end(), big_buffer.begin(), big_buffer.end()));
}
