/*
 * Copyright (C) 2014 ScyllaDB
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
#include <utility>
#include "utils/UUID_gen.hh"

BOOST_AUTO_TEST_CASE(test_generation_of_name_based_UUID) {
    auto uuid = utils::UUID_gen::get_name_UUID("systembatchlog");
    BOOST_REQUIRE_EQUAL(uuid.to_sstring(), "0290003c-977e-397c-ac3e-fdfdc01d626b");
}

using utils::UUID;
using namespace std::rel_ops;

BOOST_AUTO_TEST_CASE(test_UUID_comparison) {
    static const std::initializer_list<std::pair<UUID, UUID>> uuid_pairs = {

                    { UUID("ffeeddcc-aa99-8877-6655-443322110000"), UUID("00000000-0000-0000-0000-000000000000") },
                    { UUID("0feeddcc-aa99-8877-6655-443322110000"), UUID("00000000-0000-0000-0000-000000000000") },
                    { UUID("00000000-0000-0000-0000-000000000001"), UUID("00000000-0000-0000-0000-000000000000") },

                    { UUID("ffeeddcc-aa99-8877-6655-443322110001"), UUID("ffeeddcc-aa99-8877-6655-443322110000") },
                    { UUID("0feeddcc-aa99-8877-6655-443322110000"), UUID("0eeeddcc-aa99-8877-6655-443322110000") },
                    { UUID("0290003c-987e-397c-ac3e-fdfdc01d626b"), UUID("0290003c-977e-397c-ac3e-fdfdc01d626b") },

    };

    for (auto& p : uuid_pairs) {
        BOOST_REQUIRE_GT(p.first, p.second);
    }
}
