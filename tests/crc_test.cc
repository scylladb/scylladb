/*
 * Copyright (C) 2015 ScyllaDB
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
#include "utils/crc.hh"
#include <seastar/core/print.hh>

inline
uint32_t
do_compute_crc(utils::crc32& c) {
    return c.get();
}

template <typename T, typename... Rest>
inline
uint32_t
do_compute_crc(utils::crc32& c, const T& val, const Rest&... rest) {
    c.process_le(val);
    return do_compute_crc(c, rest...);
}

template <typename... T>
inline
uint32_t
compute_crc(const T&... vals) {
    utils::crc32 c;
    return do_compute_crc(c, vals...);
}

BOOST_AUTO_TEST_CASE(crc_1_vs_4) {
    using b = uint8_t;
    BOOST_REQUIRE_EQUAL(compute_crc(0x01020304), compute_crc(b(4), b(3), b(2), b(1)));
}

BOOST_AUTO_TEST_CASE(crc_121_vs_4) {
    using b = uint8_t;
    using w = uint16_t;
    BOOST_REQUIRE_EQUAL(compute_crc(0x01020304), compute_crc(b(4), w(0x0203), b(1)));
}

BOOST_AUTO_TEST_CASE(crc_44_vs_8) {
    using q = uint64_t;
    BOOST_REQUIRE_EQUAL(compute_crc(q(0x0102030405060708)), compute_crc(0x05060708, 0x01020304));
}
