/*
 * Copyright (C) 2016 ScyllaDB
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

#include "utils/input_stream.hh"
#include "bytes_ostream.hh"

#include <random>
#include <boost/test/unit_test.hpp>

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
    bytes big_buffer(bytes::initialized_later(), 128 * 1024);

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<uint8_t> dist;

    std::generate(big_buffer.begin(), big_buffer.end(), [&] { return dist(gen); });

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
