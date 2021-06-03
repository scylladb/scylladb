/*
 * Copyright (C) 2021-present ScyllaDB
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

#include <boost/test/unit_test.hpp>
#include "hashers.hh"
#include "xx_hasher.hh"

bytes text_part1("sanity");
bytes text_part2("check");
bytes text_full("sanitycheck");

BOOST_AUTO_TEST_CASE(xx_hasher_sanity_check) {
    xx_hasher hasher;
    hasher.update(reinterpret_cast<const char*>(std::data(text_part1)), std::size(text_part1));
    hasher.update(reinterpret_cast<const char*>(std::data(text_part2)), std::size(text_part2));
    bytes hash = hasher.finalize();
    bytes expected = from_hex("00000000000000001b1308f9e7c7dcf4");
    BOOST_CHECK_EQUAL(hash, expected);
}

BOOST_AUTO_TEST_CASE(md5_hasher_sanity_check) {
    md5_hasher hasher;
    hasher.update(reinterpret_cast<const char*>(std::data(text_part1)), std::size(text_part1));
    hasher.update(reinterpret_cast<const char*>(std::data(text_part2)), std::size(text_part2));
    bytes hash = hasher.finalize();
    bytes expected = from_hex("a9221b2b5a53b9d9adf07f3305ed1a3e");
    BOOST_CHECK_EQUAL(hash, expected);
}

BOOST_AUTO_TEST_CASE(sha256_hasher_sanity_check) {
    sha256_hasher hasher;
    hasher.update(reinterpret_cast<const char*>(std::data(text_part1)), std::size(text_part1));
    hasher.update(reinterpret_cast<const char*>(std::data(text_part2)), std::size(text_part2));
    bytes hash = hasher.finalize();
    bytes expected = from_hex("62bcb3e6160172824e1939116f48ae3680df989583c6d1bfbfa84fa9a080d003");
    BOOST_REQUIRE_EQUAL(hash, expected);
}

BOOST_AUTO_TEST_CASE(bytes_view_hasher_sanity_check) {
    bytes_view_hasher hasher1;
    hasher1.update(reinterpret_cast<const char*>(std::data(text_part1)), std::size(text_part1));
    hasher1.update(reinterpret_cast<const char*>(std::data(text_part2)), std::size(text_part2));
    size_t hash1 = hasher1.finalize();

    bytes_view_hasher hasher2;
    hasher2.update(reinterpret_cast<const char*>(std::data(text_full)), std::size(text_full));
    size_t hash2 = hasher2.finalize();

    BOOST_REQUIRE_EQUAL(hash1, hash2);
}
