/*
 * Copyright (C) 2014-present ScyllaDB
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
#include "types.hh"

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

    auto millis = duration_cast<milliseconds>(tp.time_since_epoch());
    uuid = utils::UUID_gen::get_time_UUID(millis);
    BOOST_CHECK(uuid.is_timestamp());

    auto unix_timestamp = utils::UUID_gen::unix_timestamp(uuid);
    BOOST_CHECK(unix_timestamp == millis);
}

int timeuuid_legacy_tri_compare(bytes_view o1, bytes_view o2) {
    auto compare_pos = [&] (unsigned pos, int mask, int ifequal) {
        int d = (o1[pos] & mask) - (o2[pos] & mask);
        return d ? d : ifequal;
    };
    int res = compare_pos(6, 0xf,
        compare_pos(7, 0xff,
            compare_pos(4, 0xff,
                compare_pos(5, 0xff,
                    compare_pos(0, 0xff,
                        compare_pos(1, 0xff,
                            compare_pos(2, 0xff,
                                compare_pos(3, 0xff, 0))))))));
    if (res == 0) {
        res = lexicographical_tri_compare(o1.begin(), o1.end(), o2.begin(), o2.end(),
            [] (const int8_t& a, const int8_t& b) { return a - b; });
    }
    return res < 0 ? -1 : res > 0;
}

BOOST_AUTO_TEST_CASE(test_timeuuid_msb_is_monotonic) {
    using utils::UUID, utils::UUID_gen;
    auto uuid = UUID_gen::get_time_UUID();
    auto first = uuid.serialize();
    int64_t scale_list[] = { 1, 10, 10000, 10000000, 0 };
    auto str = [&scale_list](int64_t *scale) {
        static std::string name_list[] = { " 100ns", "mc", "ms", "s" };
        return name_list[scale - scale_list];
    };

    for (int64_t *scale = scale_list; *scale; scale++) {
        int step = 1; /* + (random() % 169) ; */
        auto prev = first;
        for (int64_t i = 1; i < 3697; i += step) {
            auto next =  UUID(UUID_gen::create_time(UUID_gen::decimicroseconds{uuid.timestamp() + (i * *scale)}), 0).serialize();
            bool t1 = utils::timeuuid_tri_compare(next, prev) > 0;
            bool t2 = utils::timeuuid_tri_compare(next, first) > 0;
            if (!t1 || !t2) {
                BOOST_CHECK_MESSAGE(t1 && t2, format("a UUID {}{} later is not great than at test start: {} {}", i, str(scale), t1, t2));
            }
            prev = next;
        }
    }
}

BOOST_AUTO_TEST_CASE(test_timeuuid_tri_compare_legacy) {
    using utils::UUID, utils::UUID_gen;
    auto uuid = UUID_gen::get_time_UUID();
    auto first = uuid.serialize();
    int64_t scale_list[] = { 1, 10, 10000, 10000000, 0 };
    auto str = [&scale_list](int64_t *scale) {
        static std::string name_list[] = { " 100ns", "mc", "ms", "s" };
        return name_list[scale - scale_list];
    };


    for (int64_t *scale = scale_list; *scale; scale++) {
        int step = 1; /* + (random() % 169) ; */
        auto prev = first;
        for (int64_t i = 1; i < 3697; i += step) {
            auto next =  UUID(UUID_gen::create_time(UUID_gen::decimicroseconds{uuid.timestamp() + (i * *scale)}), 0).serialize();
            bool t1 = utils::timeuuid_tri_compare(next, prev) == timeuuid_legacy_tri_compare(next, prev);
            bool t2 = utils::timeuuid_tri_compare(next, first) == timeuuid_legacy_tri_compare(next, first);
            if (!t1 || !t2) {
                BOOST_CHECK_MESSAGE(t1 && t2, format("a UUID {}{} later violates compare order", i, str(scale)));
            }
            prev = next;
        }
    }
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
    auto prev = UUID_gen::get_time_UUID_bytes_from_micros_and_submicros(std::chrono::microseconds{micros}, 0);
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
                std::chrono::microseconds{micros}, i);
        check_is_valid_timeuuid(UUID_gen::get_UUID(uuid.data()));
        // UUID submicro part grows monotonically
        BOOST_CHECK(utils::timeuuid_tri_compare({uuid.data(), 16}, {prev.data(), 16}) > 0);
        prev = uuid;
    }
    BOOST_CHECK_EXCEPTION(UUID_gen::get_time_UUID_bytes_from_micros_and_submicros(std::chrono::microseconds{micros}, 2 * maxsubmicro),
        utils::timeuuid_submicro_out_of_range, [](auto& x) -> bool { return true; });
}

BOOST_AUTO_TEST_CASE(test_min_time_uuid) {
    using namespace std::chrono;

    auto tp = system_clock::now();
    auto millis = duration_cast<milliseconds>(tp.time_since_epoch());
    auto uuid = utils::UUID_gen::min_time_UUID(millis);
    BOOST_CHECK(uuid.is_timestamp());

    auto unix_timestamp = utils::UUID_gen::unix_timestamp(uuid);
    BOOST_CHECK(unix_timestamp == millis);
}

BOOST_AUTO_TEST_CASE(test_max_time_uuid) {
    using namespace std::chrono;

    auto tp = system_clock::now();
    auto millis = duration_cast<milliseconds>(tp.time_since_epoch());
    auto uuid = utils::UUID_gen::max_time_UUID(millis);
    BOOST_CHECK(uuid.is_timestamp());

    auto unix_timestamp = utils::UUID_gen::unix_timestamp(uuid);
    BOOST_CHECK(unix_timestamp == millis);
}
