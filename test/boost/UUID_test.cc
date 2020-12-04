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

BOOST_AUTO_TEST_CASE(test_get_time_uuid) {
    using namespace std::chrono;

    auto uuid = utils::UUID_gen::get_time_UUID();
    BOOST_CHECK(uuid.is_timestamp());

    auto tp = system_clock::now();
    uuid = utils::UUID_gen::get_time_UUID(tp);
    BOOST_CHECK(uuid.is_timestamp());

    auto millis = duration_cast<milliseconds>(tp.time_since_epoch()).count();
    uuid = utils::UUID_gen::get_time_UUID(millis);
    BOOST_CHECK(uuid.is_timestamp());

    auto unix_timestamp = utils::UUID_gen::unix_timestamp(uuid);
    BOOST_CHECK(unix_timestamp == millis);
}

BOOST_AUTO_TEST_CASE(test_timeuuid_submicro_is_monotonic) {
    const int64_t PAD6 = 0xFFFF'FFFF'FFFF;
    using namespace std::chrono;
    using utils::UUID, utils::UUID_gen;
    UUID current_timeuuid = UUID_gen::get_time_UUID();
    // Node identifier must be set to avoid collisions
    BOOST_CHECK((current_timeuuid.get_least_significant_bits() & PAD6) != 0);
    int64_t micros = UUID_gen::micros_timestamp(current_timeuuid);
    int maxsubmicro = (1 << 17) - 1;
    int step = 1 + (random() % 169);
    auto prev = UUID_gen::get_time_UUID_bytes_from_micros_and_submicros(micros, 0);
    auto prev_timeuuid = UUID_gen::get_UUID(prev.data());
    // Check prev_timeuuid node identifier is set. It uses
    // a spoof MAC address, not the same as a standard UUID.
    BOOST_CHECK((prev_timeuuid.get_least_significant_bits() & PAD6) != 0);
    auto check_is_valid_timeuuid = [&](UUID uuid) {
        // UUID is version 1
        BOOST_CHECK(uuid.is_timestamp());
        // UUID preserves the original microsecond time
        BOOST_CHECK(UUID_gen::micros_timestamp(uuid) == micros);
        // UUID 100nsec time grows monotonically
        BOOST_CHECK(uuid.timestamp() >= prev_timeuuid.timestamp());
        // UUID node is the same for all generated UUIDs
        BOOST_CHECK((prev_timeuuid.get_least_significant_bits() & PAD6) ==
                    (uuid.get_least_significant_bits() & PAD6));
    };
    for (int i = 1; i <= maxsubmicro; i += step) {
        auto uuid = UUID_gen::get_time_UUID_bytes_from_micros_and_submicros(
                micros, i);
        check_is_valid_timeuuid(UUID_gen::get_UUID(uuid.data()));
        // UUID submicro part grows monotonically
        BOOST_CHECK(utils::timeuuid_tri_compare({uuid.data(), 16}, {prev.data(), 16}) > 0);
        prev = uuid;
    }
    BOOST_CHECK_EXCEPTION(UUID_gen::get_time_UUID_bytes_from_micros_and_submicros(micros, 2 * maxsubmicro),
        utils::timeuuid_submicro_out_of_range, [](auto& x) -> bool { return true; });
}

BOOST_AUTO_TEST_CASE(test_min_time_uuid) {
    using namespace std::chrono;

    auto tp = system_clock::now();
    auto millis = duration_cast<milliseconds>(tp.time_since_epoch()).count();
    auto uuid = utils::UUID_gen::min_time_UUID(millis);
    BOOST_CHECK(uuid.is_timestamp());

    auto unix_timestamp = utils::UUID_gen::unix_timestamp(uuid);
    BOOST_CHECK(unix_timestamp == millis);
}

BOOST_AUTO_TEST_CASE(test_max_time_uuid) {
    using namespace std::chrono;

    auto tp = system_clock::now();
    auto millis = duration_cast<milliseconds>(tp.time_since_epoch()).count();
    auto uuid = utils::UUID_gen::max_time_UUID(millis);
    BOOST_CHECK(uuid.is_timestamp());

    auto unix_timestamp = utils::UUID_gen::unix_timestamp(uuid);
    BOOST_CHECK(unix_timestamp == millis);
}
