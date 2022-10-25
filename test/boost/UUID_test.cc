/*
 * Copyright (C) 2014-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#define BOOST_TEST_MODULE core

#include <boost/test/unit_test.hpp>
#include <utility>
#include "utils/UUID_gen.hh"
#include "marshal_exception.hh"
#include "utils/UUID_cmp.hh"

BOOST_AUTO_TEST_CASE(test_generation_of_name_based_UUID) {
    auto uuid = utils::UUID_gen::get_name_UUID("systembatchlog");
    BOOST_REQUIRE_EQUAL(fmt::to_string(uuid), "0290003c-977e-397c-ac3e-fdfdc01d626b");
}

using utils::UUID;

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
        BOOST_CHECK_EQUAL(fmt::to_string(uuid), sv);
        BOOST_CHECK_EQUAL((uuid.get_least_significant_bits() >> 62) & 0x3, 2);
    };

    check("b1415756-49c3-4fa8-9b72-d1b867b032af");
    check("85859d5c-fcf3-4b0b-9089-197b8b06735c");
    check("e596c2f2-d29d-44a0-bb89-0a90ff928490");
    check("f28f86f5-cbc2-4526-ba25-db90c226ec6a");
    check("ce84997b-6ea2-4468-9f02-8a65abf4141a");

    // shorter than 16 bytes
    BOOST_CHECK_THROW(UUID(""), marshal_exception);
    BOOST_CHECK_THROW(UUID("dead-beef"), marshal_exception);
    // longer than 16 bytes
    BOOST_CHECK_THROW(UUID("ce84997b-6ea2-4468-9f02-8a65abf4141-long-long-ago"), marshal_exception);
    // unconvertible string
    BOOST_CHECK_THROW(UUID("hellowol-dea2-4468-9f02-8a65abf4141a"), marshal_exception);
    // trailing garbage in msb
    BOOST_CHECK_THROW(UUID("ce84997b-6ea2-wxyz-9f02-8a65abf4141a"), marshal_exception);
    // trailing garbage in lsb
    BOOST_CHECK_THROW(UUID("ce84997b-6ea2-4468-9f02-8a65abf4wxyz"), marshal_exception);
    // spaces at the beginning
    BOOST_CHECK_THROW(UUID("   4997b-6ea2-wxyz-9f02-8a65abf4141a"), marshal_exception);
    // spaces at the end
    BOOST_CHECK_THROW(UUID("ce84997b-6ea2-4468-9f02-8a65abf4    "), marshal_exception);
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

BOOST_AUTO_TEST_CASE(test_get_time_uuid_v1) {
    using namespace std::chrono;

    auto uuid = utils::UUID_gen::get_time_UUID_v1();
    BOOST_CHECK(uuid.is_timestamp());

    auto tp = system_clock::now();
    uuid = utils::UUID_gen::get_time_UUID_v1(tp);
    BOOST_CHECK(uuid.is_timestamp());

    auto millis = duration_cast<milliseconds>(tp.time_since_epoch());
    uuid = utils::UUID_gen::get_time_UUID_v1(millis);
    BOOST_CHECK(uuid.is_timestamp());

    auto unix_timestamp = utils::UUID_gen::unix_timestamp(uuid);
    BOOST_CHECK(unix_timestamp == millis);
}

BOOST_AUTO_TEST_CASE(test_get_time_uuid_v7) {
    using namespace std::chrono;

    auto uuid = utils::UUID_gen::get_time_UUID_v7();
    BOOST_CHECK(uuid.is_timestamp());

    auto tp = system_clock::now();
    uuid = utils::UUID_gen::get_time_UUID_v7(tp);
    BOOST_CHECK(uuid.is_timestamp());

    auto millis = duration_cast<milliseconds>(tp.time_since_epoch());
    uuid = utils::UUID_gen::get_time_UUID_v7(millis);
    BOOST_CHECK(uuid.is_timestamp());

    auto unix_timestamp = utils::UUID_gen::unix_timestamp(uuid);
    BOOST_CHECK(unix_timestamp == millis);
}

BOOST_AUTO_TEST_CASE(test_uuid_to_uint32) {
    // (gdb) p/x 0x3123223d ^ 0x17300 ^ 0x31e31215 ^ 0x98312
    // $2 = 0xc8c03a
    uint64_t x = 0x173003123223d;
    uint64_t y = 0x9831231e31215;
    uint32_t expected_id = 0xc8c03a;
    auto uuid = utils::UUID(x, y);
    uint32_t id = uuid_xor_to_uint32(uuid);
    BOOST_CHECK(id == expected_id);
}

std::strong_ordering timeuuid_legacy_tri_compare(bytes_view o1, bytes_view o2) {
    auto compare_pos = [&] (unsigned pos, int mask, std::strong_ordering ifequal) {
        auto d = (o1[pos] & mask) <=> (o2[pos] & mask);
        return d != 0 ? d : ifequal;
    };
    auto res = compare_pos(6, 0xf,
        compare_pos(7, 0xff,
            compare_pos(4, 0xff,
                compare_pos(5, 0xff,
                    compare_pos(0, 0xff,
                        compare_pos(1, 0xff,
                            compare_pos(2, 0xff,
                                compare_pos(3, 0xff, std::strong_ordering::equal))))))));
    if (res == 0) {
        res = std::lexicographical_compare_three_way(o1.begin(), o1.end(), o2.begin(), o2.end(),
            [] (const int8_t& a, const int8_t& b) { return a <=> b; });
    }
    return res;
}

BOOST_AUTO_TEST_CASE(test_timeuuid_v1_msb_is_monotonic) {
    using utils::UUID, utils::UUID_gen;
    auto uuid = UUID_gen::get_time_UUID_v1();
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
            auto next =  UUID(UUID_gen::create_time_v1(UUID_gen::decimicroseconds{uuid.timestamp() + (i * *scale)}), 0).serialize();
            bool t1 = utils::timeuuid_cmp(next, prev).tri_compare() > 0;
            bool t2 = utils::timeuuid_cmp(next, first).tri_compare() > 0;
            if (!t1 || !t2) {
                BOOST_CHECK_MESSAGE(t1 && t2, format("a UUID_v1 {}{} later is not great than at test start: {} {}", i, str(scale), t1, t2));
            }
            prev = next;
        }
    }
}

static std::string_view to_string(std::strong_ordering order) {
    if (order > 0) {
        return "gt";
    } else if (order < 0) {
        return "lt";
    } else {
        return "eq";
    }
}

BOOST_AUTO_TEST_CASE(test_timeuuid_v7_msb_is_monotonic) {
    using utils::UUID, utils::UUID_gen;
    auto first = UUID_gen::get_time_UUID_v7();
    int64_t scale_list[] = { 1, 10, 10000, 10000000, 0 };
    auto str = [&scale_list](int64_t *scale) {
        static std::string name_list[] = { " 100ns", "mc", "ms", "s" };
        return name_list[scale - scale_list];
    };

    auto tri_compare = [] (const UUID& u1, const UUID& u2) {
        auto res0 = timeuuid_tri_compare(u1, u2);
        auto b1 = u1.serialize();
        auto b2 = u2.serialize();
        auto res2 = utils::timeuuid_cmp(b1, b2).uuid_tri_compare();
        assert(res0 == res2);
        auto res3 = compare_unsigned(b1, b2);
        assert(res0 == res3);
        return res0;
    };

    for (int64_t *scale = scale_list; *scale; scale++) {
        int step = *scale == 1 ? 3 : 1;
        auto prev = first;
        for (int64_t i = step + (*scale == 1); i < 3697; i += step) {
            auto next_decimicros = UUID_gen::decimicroseconds{first.timestamp() + (i * *scale)};
            auto next =  UUID(UUID_gen::create_time_v7(next_decimicros), 0);
            auto c1 = tri_compare(next, prev);
            auto c2 = tri_compare(next, first);
            bool t1 = c1 > 0;
            bool t2 = c2 > 0;
            if (!t1 || !t2) {
                BOOST_CHECK_MESSAGE(t1 && t2, format("a UUID_v7 {}{} later is not greater than at test start: <=>prev={} <=>first={}", i, str(scale), to_string(c1), to_string(c2)));
                BOOST_CHECK_MESSAGE(t1 && t2, format("first={} ({:016x}:{:016x} ts={})", first, (uint64_t)first.get_most_significant_bits(), (uint64_t)first.get_least_significant_bits(), first.timestamp()));
                BOOST_CHECK_MESSAGE(t1 && t2, format(" prev={} ({:016x}:{:016x} ts={})", prev, (uint64_t)prev.get_most_significant_bits(), (uint64_t)prev.get_least_significant_bits(), prev.timestamp()));
                BOOST_CHECK_MESSAGE(t1 && t2, format(" next={} ({:016x}:{:016x} ts={})", next, (uint64_t)next.get_most_significant_bits(), (uint64_t)next.get_least_significant_bits(), next.timestamp()));
                auto millis = duration_cast<UUID_gen::milliseconds>(next_decimicros);
                auto nanos = duration_cast<UUID_gen::nanoseconds>(next_decimicros - millis);
                auto sub_millis = (4096UL * nanos.count()) / 1000000UL;
                BOOST_CHECK_MESSAGE(t1 && t2, format("first_timestamp=0x{:x} prev_timestamp=0x{:x} next_timestamp=0x{:x} next_decimicros=0x{:x} {} millis={} nanos={} sub_millis={}",
                        first.timestamp(), prev.timestamp(), next.timestamp(),
                        next_decimicros.count(), next_decimicros.count(),
                        millis.count(), nanos.count(), sub_millis));
                BOOST_FAIL("test_timeuuid_v7_msb_is_monotonic failed");
            }
            prev = next;
        }
        auto cmp = tri_compare(prev, first);
        BOOST_CHECK_MESSAGE(cmp > 0, format("prev={} ({}) first={} ({}): cmp={}", prev, prev.timestamp(), first, first.timestamp(), cmp > 0 ? "1" : cmp < 0 ? "-1" : "0"));
        BOOST_REQUIRE(cmp > 0);
    }
}

BOOST_AUTO_TEST_CASE(test_timeuuid_v1_tri_compare_legacy) {
    using utils::UUID, utils::UUID_gen;
    auto uuid = UUID_gen::get_time_UUID_v1();
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
            auto next =  UUID(UUID_gen::create_time_v1(UUID_gen::decimicroseconds{uuid.timestamp() + (i * *scale)}), 0).serialize();
            bool t1 = utils::timeuuid_cmp(next, prev).tri_compare() == timeuuid_legacy_tri_compare(next, prev);
            bool t2 = utils::timeuuid_cmp(next, first).tri_compare() == timeuuid_legacy_tri_compare(next, first);
            if (!t1 || !t2) {
                BOOST_CHECK_MESSAGE(t1 && t2, format("a UUID {}{} later violates compare order", i, str(scale)));
            }
            prev = next;
        }
    }
}

BOOST_AUTO_TEST_CASE(test_timeuuid_v1_unique_monotonicity) {
    using utils::UUID, utils::UUID_gen;
    auto u0 = UUID_gen::get_time_UUID_v1();
    auto u1 = UUID_gen::get_time_UUID_v1();
    auto uuid_cmp = utils::timeuuid_cmp(u0.serialize(), u1.serialize()).tri_compare();

    BOOST_REQUIRE(uuid_cmp < 0);
    BOOST_REQUIRE(u0.timestamp() < u1.timestamp());

    auto s0 = fmt::to_string(u0);
    auto s1 = fmt::to_string(u1);
    auto str_cmp = s0.compare(s1);

    BOOST_REQUIRE(str_cmp != 0);
}

BOOST_AUTO_TEST_CASE(test_timeuuid_v7_unique_monotonicity) {
    using utils::UUID, utils::UUID_gen;
    auto first = UUID_gen::get_time_UUID_v7();
    auto prev = first;
    for (auto i = 0; i < 10000; ++i) {
        auto next = UUID_gen::get_time_UUID_v7();
        auto uuid_cmp = utils::timeuuid_cmp(prev.serialize(), next.serialize()).tri_compare();
        BOOST_TEST_MESSAGE(format("u0={}", prev));
        BOOST_TEST_MESSAGE(format("u1={}", next));
        BOOST_TEST_MESSAGE(format("uuid_cmp={}", uuid_cmp < 0 ? "-1" : uuid_cmp > 0 ? "1" : "0"));

        BOOST_REQUIRE(uuid_cmp < 0);
        BOOST_REQUIRE_LT(prev.timestamp(), next.timestamp());

        auto s0 = fmt::to_string(prev);
        auto s1 = fmt::to_string(next);
        auto str_cmp = s0.compare(s1);
        BOOST_TEST_MESSAGE(format("str_cmp={}", str_cmp));

        BOOST_REQUIRE_LT(str_cmp, 0);
        prev = next;
    }
    auto uuid_cmp = utils::timeuuid_cmp(first.serialize(), prev.serialize()).tri_compare();
    BOOST_REQUIRE(uuid_cmp < 0);
    BOOST_REQUIRE_LT(first.timestamp(), prev.timestamp());
}

BOOST_AUTO_TEST_CASE(test_timeuuid_v7_monotonicity) {
    using utils::UUID, utils::UUID_gen;
    auto seed = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    BOOST_TEST_MESSAGE(format("random-seed={}", seed));
    srandom(seed);
    constexpr uint64_t millis_mask = (1UL << 48) - 1;
    auto t0 = UUID_gen::milliseconds(random() & millis_mask);
    auto t1 = UUID_gen::milliseconds(random() & millis_mask);
    auto u0 = UUID_gen::get_time_UUID_v7(t0);
    auto u1 = UUID_gen::get_time_UUID_v7(t1);
    auto s0 = fmt::to_string(u0);
    auto s1 = fmt::to_string(u1);

    BOOST_TEST_MESSAGE(format("t0={} u0={} serialized={}", t0.count(), u0, u0.serialize()));
    BOOST_TEST_MESSAGE(format("t1={} u1={} serialized={}", t1.count(), u1, u1.serialize()));

    BOOST_REQUIRE_EQUAL(u0, u0);
    BOOST_REQUIRE_EQUAL(u1, u1);
    BOOST_REQUIRE_NE(u0, u1);

    BOOST_REQUIRE_EQUAL(UUID_gen::unix_timestamp(u0).count(), t0.count());
    BOOST_REQUIRE_EQUAL(UUID_gen::unix_timestamp(u1).count(), t1.count());

    auto uuid_cmp = utils::timeuuid_cmp(u0.serialize(), u1.serialize()).tri_compare();
    BOOST_TEST_MESSAGE(format("uuid_cmp={}", uuid_cmp > 0 ? "1" : uuid_cmp < 0 ? "-1" : "0"));
    BOOST_REQUIRE(uuid_cmp != 0);

    auto str_cmp = s0.compare(s1);
    BOOST_TEST_MESSAGE(format("str_cmp={}", str_cmp));
    BOOST_REQUIRE_NE(str_cmp, 0);

    if (uuid_cmp > 0) {
        BOOST_REQUIRE_GE(t0.count(), t1.count());
        if ((t0.count() >= 0) == (t1.count() >= 0)) {
            BOOST_REQUIRE_GT(str_cmp, 0);
        }
    } else if (uuid_cmp < 0) {
        BOOST_REQUIRE_LE(t0.count(), t1.count());
        if ((t0.count() >= 0) == (t1.count() >= 0)) {
            BOOST_REQUIRE_LT(str_cmp, 0);
        }
    }
}

BOOST_AUTO_TEST_CASE(test_timeuuid_v1_submicro_is_monotonic) {
    const int64_t PAD6 = 0xFFFF'FFFF'FFFF;
    using namespace std::chrono;
    using utils::UUID, utils::UUID_gen;
    UUID current_timeuuid = UUID_gen::get_time_UUID_v1();
    // Node identifier must be set to avoid collisions
    BOOST_CHECK((current_timeuuid.get_least_significant_bits() & PAD6) != 0);
    int64_t micros = UUID_gen::micros_timestamp(current_timeuuid);
    int maxsubmicro = (1 << 17) - 1;
    int step = 1 + (random() % 169);
    auto prev = UUID_gen::get_time_UUID_v1_bytes_from_micros_and_submicros(std::chrono::microseconds{micros}, 0);
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
        auto uuid = UUID_gen::get_time_UUID_v1_bytes_from_micros_and_submicros(
                std::chrono::microseconds{micros}, i);
        check_is_valid_timeuuid(UUID_gen::get_UUID(uuid.data()));
        // UUID submicro part grows monotonically
        BOOST_CHECK(utils::timeuuid_cmp({uuid.data(), 16}, {prev.data(), 16}).tri_compare() > 0);
        prev = uuid;
    }
    BOOST_CHECK_EXCEPTION(UUID_gen::get_time_UUID_v1_bytes_from_micros_and_submicros(std::chrono::microseconds{micros}, 2 * maxsubmicro),
        utils::timeuuid_submicro_out_of_range, [](auto& x) -> bool { return true; });
}

BOOST_AUTO_TEST_CASE(test_min_time_uuid) {
    using namespace std::chrono;

    auto tp = system_clock::now();
    auto millis = duration_cast<milliseconds>(tp.time_since_epoch());
    auto uuid = utils::UUID_gen::min_time_UUID_v1(millis);
    BOOST_CHECK(uuid.is_timestamp());

    auto unix_timestamp = utils::UUID_gen::unix_timestamp(uuid);
    BOOST_CHECK(unix_timestamp == millis);
}

BOOST_AUTO_TEST_CASE(test_max_time_uuid) {
    using namespace std::chrono;

    auto tp = system_clock::now();
    auto millis = duration_cast<milliseconds>(tp.time_since_epoch());
    auto uuid = utils::UUID_gen::max_time_UUID_v1(millis);
    BOOST_CHECK(uuid.is_timestamp());

    auto unix_timestamp = utils::UUID_gen::unix_timestamp(uuid);
    BOOST_CHECK(unix_timestamp == millis);
}

BOOST_AUTO_TEST_CASE(test_negate) {
    using namespace utils;

  auto do_test = [] (std::function<UUID()> gen_uuid) {
    auto original_uuid = gen_uuid();
    BOOST_TEST_MESSAGE(fmt::format("original_uuid (v{}):   {}", original_uuid.version(), original_uuid));

    auto negated_uuid = UUID_gen::negate(original_uuid);
    BOOST_TEST_MESSAGE(fmt::format("negated_uuid (v{}):    {}", negated_uuid.version(), negated_uuid));

    BOOST_REQUIRE_EQUAL(original_uuid.version(), negated_uuid.version());

    BOOST_REQUIRE(original_uuid != negated_uuid);

    auto re_negated_uuid = UUID_gen::negate(negated_uuid);
    BOOST_TEST_MESSAGE(fmt::format("re_negated_uuid: {}", re_negated_uuid));

    BOOST_REQUIRE(original_uuid == re_negated_uuid);
  };

  do_test([] { return UUID_gen::get_time_UUID_v1(); });
  do_test([] { return UUID_gen::get_time_UUID_v7(); });
  do_test([] { return UUID(); });
  do_test([] { return make_random_uuid(); });
}

BOOST_AUTO_TEST_CASE(test_null_uuid) {
    // Verify that the default-constructed UUID is null
    utils::UUID uuid;
    BOOST_CHECK(uuid.is_null());
    BOOST_CHECK(!uuid);

    // Verify that the null_uuid is indeed null
    uuid = utils::null_uuid();
    BOOST_CHECK(uuid.is_null());
    BOOST_CHECK(!uuid);

    // Verify that the default constructed
    // UUID and the null_uuid are equal.
    BOOST_REQUIRE_EQUAL(UUID(), utils::null_uuid());

    // Verify that a random uuid is not null
    uuid = utils::make_random_uuid();
    BOOST_CHECK(!uuid.is_null());
    BOOST_CHECK(uuid);

    // Verify that a time uuid v1 is not null
    uuid = utils::UUID_gen::get_time_UUID_v1();
    BOOST_CHECK(!uuid.is_null());
    BOOST_CHECK(uuid);

    // Verify that a time uuid v7 is not null
    uuid = utils::UUID_gen::get_time_UUID_v7();
    BOOST_CHECK(!uuid.is_null());
    BOOST_CHECK(uuid);
}
