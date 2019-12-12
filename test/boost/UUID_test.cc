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

BOOST_AUTO_TEST_CASE(test_from_string) {
    auto check = [] (sstring_view sv) {
        auto uuid = UUID(sv);
        BOOST_CHECK_EQUAL(uuid.version(), 4);
        BOOST_CHECK_EQUAL(uuid.to_sstring(), sv);
        BOOST_CHECK_EQUAL((uuid.get_least_significant_bits() >> 62) & 0x3, 2);
    };

    check("b1415756-49c3-4fa8-9b72-d1b867b032af");
    check("85859d5c-fcf3-4b0b-9089-197b8b06735c");
    check("e596c2f2-d29d-44a0-bb89-0a90ff928490");
    check("f28f86f5-cbc2-4526-ba25-db90c226ec6a");
    check("ce84997b-6ea2-4468-9f02-8a65abf4141a");
}

BOOST_AUTO_TEST_CASE(test_make_random_uuid) {
    std::vector<UUID> uuids;
    for (auto i = 0; i < 100; i++) {
        auto uuid = utils::make_random_uuid();
        BOOST_CHECK_EQUAL(uuid.version(), 4);
        BOOST_CHECK_EQUAL((uuid.get_least_significant_bits() >> 62) & 0x3, 2);
        uuids.emplace_back(uuid);
    }
    std::sort(uuids.begin(), uuids.end());
    BOOST_CHECK(std::unique(uuids.begin(), uuids.end()) == uuids.end());
}
