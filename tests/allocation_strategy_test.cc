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

#include "utils/managed_ref.hh"

BOOST_AUTO_TEST_CASE(test_standrard_allocator_respects_alignment) {
    constexpr auto alignment = 16;
    struct alignas(alignment) A {};
    auto m = make_managed<A>();
    auto addr = reinterpret_cast<uintptr_t>(&*m);
    BOOST_REQUIRE((addr & (alignment - 1)) == 0);
}
