/*
 * Copyright (C) 2018 ScyllaDB
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
#include "sstables/checksum_utils.hh"
#include "make_random_string.hh"
#include <seastar/core/print.hh>

template<typename ReferenceImpl, typename Impl>
static
void test_combine() {
    auto check = [](size_t len) {
        auto c1 = Impl::checksum_combine(0x12381237, 0x73747474, len);
        auto c2 = ReferenceImpl::checksum_combine(0x12381237, 0x73747474, len);
        BOOST_REQUIRE_EQUAL(c1, c2);
    };

    check(0);
    check(1);
    check(2);
    check(3);
    check(8);
    check(255);
    check(256);
    check(1023);
    check(1024);
    check(1025);
    check(0xffff);
    check(0x10000);
    check(0xdeadbeef);
    check(0xffffffff);
    check(0x100000000);
    check(0x200000000);
    check(0x10000000001);
    check(0x7eadbeefcafebabe);
    check(0x7fffffffffffffff);
}

template<typename ReferenceImpl, typename Impl>
static
void test() {
    auto rolling = Impl::init_checksum();
    BOOST_REQUIRE_EQUAL(rolling, ReferenceImpl::init_checksum());

    for (auto size : {0, 1, 2, 10, 13, 16, 17, 22, 31, 1024, 2000, 80000}) {
        auto data = make_random_string(size);

        auto current = Impl::checksum(data.cbegin(), data.size());
        auto ref_current = ReferenceImpl::checksum(data.cbegin(), data.size());
        BOOST_REQUIRE_EQUAL(current, ref_current);

        auto new_rolling = Impl::checksum(rolling, data.cbegin(), data.size());
        auto ref_new_rolling = ReferenceImpl::checksum(rolling, data.cbegin(), data.size());
        BOOST_REQUIRE_EQUAL(new_rolling, ref_new_rolling);

        auto new_rolling_via_combine = Impl::checksum_combine(rolling, current, data.size());
        BOOST_REQUIRE_EQUAL(new_rolling, new_rolling_via_combine);

        rolling = new_rolling;
    }

    test_combine<ReferenceImpl, Impl>();
}

BOOST_AUTO_TEST_CASE(test_libdeflate_matches_zlib) {
    test<zlib_crc32_checksummer, libdeflate_crc32_checksummer>();
}

BOOST_AUTO_TEST_CASE(test_default_matches_zlib) {
    test<zlib_crc32_checksummer, crc32_utils>();
}
