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

#include <seastar/tests/test-utils.hh>
#include <seastar/net/inet_address.hh>
#include <utils/UUID_gen.hh>
#include <boost/asio/ip/address_v4.hpp>
#include <net/ip.hh>
#include <boost/multiprecision/cpp_int.hpp>
#include "types.hh"
#include "compound.hh"
#include "db/marshal/type_parser.hh"
#include "cql3/cql3_type.hh"
#include "utils/big_decimal.hh"

using namespace std::literals::chrono_literals;

void test_parsing_fails(const shared_ptr<const abstract_type>& type, sstring str)
{
    try {
        type->from_string(str);
        BOOST_FAIL(sprint("Parsing of '%s' should have failed", str));
    } catch (const marshal_exception& e) {
        // expected
    }
}

BOOST_AUTO_TEST_CASE(test_bytes_type_string_conversions) {
    BOOST_REQUIRE(bytes_type->equal(bytes_type->from_string("616263646566"), bytes_type->decompose(data_value(bytes{"abcdef"}))));
}

BOOST_AUTO_TEST_CASE(test_byte_type_string_conversions) {
    BOOST_REQUIRE(byte_type->equal(byte_type->from_string("123"), byte_type->decompose(int8_t(123))));
    BOOST_REQUIRE_EQUAL(byte_type->to_string(byte_type->decompose(int8_t(123))), "123");

    BOOST_REQUIRE(byte_type->equal(byte_type->from_string("12"), byte_type->decompose(int8_t(12))));
    BOOST_REQUIRE(byte_type->equal(byte_type->from_string("0012"), byte_type->decompose(int8_t(12))));
    BOOST_REQUIRE(byte_type->equal(byte_type->from_string("+12"), byte_type->decompose(int8_t(12))));
    BOOST_REQUIRE_EQUAL(byte_type->to_string(byte_type->decompose(int8_t(12))), "12");
    BOOST_REQUIRE(byte_type->equal(byte_type->from_string("-12"), byte_type->decompose(int8_t(-12))));
    BOOST_REQUIRE_EQUAL(byte_type->to_string(byte_type->decompose(int8_t(-12))), "-12");

    BOOST_REQUIRE(byte_type->equal(byte_type->from_string("0"), byte_type->decompose(int8_t(0))));
    BOOST_REQUIRE(byte_type->equal(byte_type->from_string("-0"), byte_type->decompose(int8_t(0))));
    BOOST_REQUIRE(byte_type->equal(byte_type->from_string("+0"), byte_type->decompose(int8_t(0))));
    BOOST_REQUIRE_EQUAL(byte_type->to_string(byte_type->decompose(int8_t(0))), "0");

    BOOST_REQUIRE(byte_type->equal(byte_type->from_string("-128"), byte_type->decompose(int8_t(-128))));
    BOOST_REQUIRE_EQUAL(byte_type->to_string(byte_type->decompose((int8_t(-128)))), "-128");

    BOOST_REQUIRE(byte_type->equal(byte_type->from_string("127"), byte_type->decompose(int8_t(127))));
    BOOST_REQUIRE_EQUAL(byte_type->to_string(byte_type->decompose(int8_t(127))), "127");

    test_parsing_fails(byte_type, "asd");
    test_parsing_fails(byte_type, "-129");
    test_parsing_fails(byte_type, "128");

    BOOST_REQUIRE_EQUAL(byte_type->to_string(bytes()), "");
}

BOOST_AUTO_TEST_CASE(test_short_type_string_conversions) {
    BOOST_REQUIRE(short_type->equal(short_type->from_string("12345"), short_type->decompose(int16_t(12345))));
    BOOST_REQUIRE_EQUAL(short_type->to_string(short_type->decompose(int16_t(12345))), "12345");

    BOOST_REQUIRE(short_type->equal(short_type->from_string("12"), short_type->decompose(int16_t(12))));
    BOOST_REQUIRE(short_type->equal(short_type->from_string("0012"), short_type->decompose(int16_t(12))));
    BOOST_REQUIRE(short_type->equal(short_type->from_string("+12"), short_type->decompose(int16_t(12))));
    BOOST_REQUIRE_EQUAL(short_type->to_string(short_type->decompose(int16_t(12))), "12");
    BOOST_REQUIRE(short_type->equal(short_type->from_string("-12"), short_type->decompose(int16_t(-12))));
    BOOST_REQUIRE_EQUAL(short_type->to_string(short_type->decompose(int16_t(-12))), "-12");

    BOOST_REQUIRE(short_type->equal(short_type->from_string("0"), short_type->decompose(int16_t(0))));
    BOOST_REQUIRE(short_type->equal(short_type->from_string("-0"), short_type->decompose(int16_t(0))));
    BOOST_REQUIRE(short_type->equal(short_type->from_string("+0"), short_type->decompose(int16_t(0))));
    BOOST_REQUIRE_EQUAL(short_type->to_string(short_type->decompose(int16_t(0))), "0");

    BOOST_REQUIRE(short_type->equal(short_type->from_string("-32768"), short_type->decompose(int16_t(-32768))));
    BOOST_REQUIRE_EQUAL(short_type->to_string(short_type->decompose((int16_t(-32768)))), "-32768");

    BOOST_REQUIRE(short_type->equal(short_type->from_string("32677"), short_type->decompose(int16_t(32677))));
    BOOST_REQUIRE_EQUAL(short_type->to_string(short_type->decompose(int16_t(32677))), "32677");

    test_parsing_fails(short_type, "asd");
    test_parsing_fails(short_type, "-32769");
    test_parsing_fails(short_type, "32768");

    BOOST_REQUIRE_EQUAL(short_type->to_string(bytes()), "");
}

BOOST_AUTO_TEST_CASE(test_int32_type_string_conversions) {
    BOOST_REQUIRE(int32_type->equal(int32_type->from_string("1234567890"), int32_type->decompose(1234567890)));
    BOOST_REQUIRE_EQUAL(int32_type->to_string(int32_type->decompose(1234567890)), "1234567890");

    BOOST_REQUIRE(int32_type->equal(int32_type->from_string("12"), int32_type->decompose(12)));
    BOOST_REQUIRE(int32_type->equal(int32_type->from_string("0012"), int32_type->decompose(12)));
    BOOST_REQUIRE(int32_type->equal(int32_type->from_string("+12"), int32_type->decompose(12)));
    BOOST_REQUIRE_EQUAL(int32_type->to_string(int32_type->decompose(12)), "12");
    BOOST_REQUIRE(int32_type->equal(int32_type->from_string("-12"), int32_type->decompose(-12)));
    BOOST_REQUIRE_EQUAL(int32_type->to_string(int32_type->decompose(-12)), "-12");

    BOOST_REQUIRE(int32_type->equal(int32_type->from_string("0"), int32_type->decompose(0)));
    BOOST_REQUIRE(int32_type->equal(int32_type->from_string("-0"), int32_type->decompose(0)));
    BOOST_REQUIRE(int32_type->equal(int32_type->from_string("+0"), int32_type->decompose(0)));
    BOOST_REQUIRE_EQUAL(int32_type->to_string(int32_type->decompose(0)), "0");

    BOOST_REQUIRE(int32_type->equal(int32_type->from_string("-2147483648"), int32_type->decompose((int32_t)-2147483648)));
    BOOST_REQUIRE_EQUAL(int32_type->to_string(int32_type->decompose((int32_t)-2147483648)), "-2147483648");

    BOOST_REQUIRE(int32_type->equal(int32_type->from_string("2147483647"), int32_type->decompose((int32_t)2147483647)));
    BOOST_REQUIRE_EQUAL(int32_type->to_string(int32_type->decompose((int32_t)2147483647)), "2147483647");

    test_parsing_fails(int32_type, "asd");
    test_parsing_fails(int32_type, "-2147483649");
    test_parsing_fails(int32_type, "2147483648");
    test_parsing_fails(int32_type, "2147483648123");

    BOOST_REQUIRE_EQUAL(int32_type->to_string(bytes()), "");
}

BOOST_AUTO_TEST_CASE(test_timeuuid_type_string_conversions) {
    auto now = utils::UUID_gen::get_time_UUID();
    BOOST_REQUIRE(timeuuid_type->equal(timeuuid_type->from_string(now.to_sstring()), timeuuid_type->decompose(now)));
    auto uuid = utils::UUID(sstring("d2177dd0-eaa2-11de-a572-001b779c76e3"));
    BOOST_REQUIRE(timeuuid_type->equal(timeuuid_type->from_string("D2177dD0-EAa2-11de-a572-001B779C76e3"), timeuuid_type->decompose(uuid)));

    test_parsing_fails(timeuuid_type, "something");
    test_parsing_fails(timeuuid_type, "D2177dD0-EAa2-11de-a572-001B779C76e3a");
    test_parsing_fails(timeuuid_type, "D2177dD0-EAa2-11de-a572001-B779C76e3");
    test_parsing_fails(timeuuid_type, "D2177dD0EAa211dea572001B779C76e3");
    test_parsing_fails(timeuuid_type, utils::make_random_uuid().to_sstring());
}

BOOST_AUTO_TEST_CASE(test_simple_date_type_string_conversions) {
    BOOST_REQUIRE(simple_date_type->equal(simple_date_type->from_string("1970-01-01"), simple_date_type->decompose(int32_t(0x80000000))));
    BOOST_REQUIRE_EQUAL(simple_date_type->to_string(simple_date_type->decompose(int32_t(0x80000000))), "1970-01-01");

    BOOST_REQUIRE(simple_date_type->equal(simple_date_type->from_string("-5877641-06-23"), simple_date_type->decompose(int32_t(0x00000000))));
    BOOST_REQUIRE_EQUAL(simple_date_type->to_string(simple_date_type->decompose(int32_t(0x00000000))), "-5877641-06-23");

    BOOST_REQUIRE(simple_date_type->equal(simple_date_type->from_string("5881580-07-11"), simple_date_type->decompose(int32_t(0xffffffff))));
    BOOST_REQUIRE_EQUAL(simple_date_type->to_string(simple_date_type->decompose(int32_t(0xffffffff))), "5881580-07-11");

    test_parsing_fails(simple_date_type, "something");
    test_parsing_fails(simple_date_type, "-5877641-06-22");
    test_parsing_fails(simple_date_type, "5881580-07-12");
}

BOOST_AUTO_TEST_CASE(test_time_type_string_conversions) {
    BOOST_REQUIRE(time_type->equal(time_type->from_string("12:34:56"), time_type->decompose(int64_t(45296000000000))));
    BOOST_REQUIRE_EQUAL(time_type->to_string(time_type->decompose(int64_t(45296000000000))), "12:34:56.000000000");

    BOOST_REQUIRE(time_type->equal(time_type->from_string("12:34:56.000000000"), time_type->decompose(int64_t(45296000000000))));
    BOOST_REQUIRE_EQUAL(time_type->to_string(time_type->decompose(int64_t(45296000000000))), "12:34:56.000000000");

    BOOST_REQUIRE(time_type->equal(time_type->from_string("12:34:56.123456789"), time_type->decompose(int64_t(45296123456789))));
    BOOST_REQUIRE_EQUAL(time_type->to_string(time_type->decompose(int64_t(45296123456789))), "12:34:56.123456789");

    BOOST_REQUIRE(time_type->equal(time_type->from_string("00:00:00.000000000"), time_type->decompose(int64_t(0x00000000))));
    BOOST_REQUIRE_EQUAL(time_type->to_string(time_type->decompose(int64_t(0x00000000))), "00:00:00.000000000");

    BOOST_REQUIRE(time_type->equal(time_type->from_string("23:59:59.999999999"), time_type->decompose(int64_t(86399999999999))));
    BOOST_REQUIRE_EQUAL(time_type->to_string(time_type->decompose(int64_t(86399999999999))), "23:59:59.999999999");

    BOOST_REQUIRE(time_type->equal(time_type->from_string("-00:00:00.000000000"), time_type->decompose(int64_t(0x00000000))));
    BOOST_REQUIRE(time_type->equal(time_type->from_string("-00:00:00.000000001"), time_type->decompose(int64_t(0x00000001))));

    test_parsing_fails(time_type, "something");
    test_parsing_fails(time_type, "00:00");
    test_parsing_fails(time_type, "24:00:00.000000000");
    test_parsing_fails(time_type, "-01:00:00.0000000000");
    test_parsing_fails(time_type, "00:-01:00.0000000000");
    test_parsing_fails(time_type, "00:00:-10.0000000000");
    test_parsing_fails(time_type, "00:00:00.-0000000001");
}


BOOST_AUTO_TEST_CASE(test_duration_type_string_conversions) {
    // See `duration_test.cc` for more conversion tests.
    BOOST_REQUIRE(duration_type->equal(duration_type->from_string("1y3mo5m2s"),
                                       duration_type->decompose(cql_duration("1y3mo5m2s"))));
}

BOOST_AUTO_TEST_CASE(test_uuid_type_comparison) {
    auto uuid1 = uuid_type->decompose(utils::UUID(sstring("ad4d3770-7a50-11e6-ac4d-000000000003")));
    auto uuid2 = uuid_type->decompose(utils::UUID(sstring("c512ba10-7a50-11e6-ac4d-000000000003")));
    
    BOOST_REQUIRE_EQUAL(true, uuid_type->less(uuid1, uuid2));
    BOOST_REQUIRE_EQUAL(false, uuid_type->less(uuid2, uuid1));
}

BOOST_AUTO_TEST_CASE(test_uuid_type_string_conversions) {
    auto now = utils::UUID_gen::get_time_UUID();
    BOOST_REQUIRE(uuid_type->equal(uuid_type->from_string(now.to_sstring()), uuid_type->decompose(now)));
    auto random = utils::make_random_uuid();
    BOOST_REQUIRE(uuid_type->equal(uuid_type->from_string(random.to_sstring()), uuid_type->decompose(random)));
    auto uuid = utils::UUID(sstring("d2177dd0-eaa2-11de-a572-001b779c76e3"));
    BOOST_REQUIRE(uuid_type->equal(uuid_type->from_string("D2177dD0-EAa2-11de-a572-001B779C76e3"), uuid_type->decompose(uuid)));

    test_parsing_fails(uuid_type, "something");
    test_parsing_fails(uuid_type, "D2177dD0-EAa2-11de-a572-001B779C76e3a");
    test_parsing_fails(uuid_type, "D2177dD0-EAa2-11de-a572001-B779C76e3");
    test_parsing_fails(uuid_type, "D2177dD0EAa211dea572001B779C76e3");
}

BOOST_AUTO_TEST_CASE(test_inet_type_string_conversions) {
    net::inet_address addr("127.0.0.1");
    BOOST_REQUIRE(inet_addr_type->equal(inet_addr_type->from_string("127.0.0.1"), inet_addr_type->decompose(addr)));

    test_parsing_fails(inet_addr_type, "something");
    test_parsing_fails(inet_addr_type, "300.127.127.127");
    test_parsing_fails(inet_addr_type, "127-127.127.127");
    test_parsing_fails(inet_addr_type, "127.127.127.127.127");
}

void test_timestamp_like_string_conversions(data_type timestamp_type) {
    timestamp_type->from_string("now");
    db_clock::time_point tp(db_clock::duration(1435881600000));
    BOOST_REQUIRE(timestamp_type->equal(timestamp_type->from_string("1435881600000"), timestamp_type->decompose(tp)));
    BOOST_REQUIRE(timestamp_type->equal(timestamp_type->from_string("2015-07-03+0000"), timestamp_type->decompose(tp)));
    BOOST_REQUIRE(timestamp_type->equal(timestamp_type->from_string("2015-07-03-00"), timestamp_type->decompose(tp)));
    BOOST_REQUIRE(timestamp_type->equal(timestamp_type->from_string("2015-07-03 00:00+0000"), timestamp_type->decompose(tp)));
    BOOST_REQUIRE(timestamp_type->equal(timestamp_type->from_string("2015-07-03 01:00:00+0000"), timestamp_type->decompose(tp + 1h)));
    BOOST_REQUIRE(timestamp_type->equal(timestamp_type->from_string("2015-07-03 01:02:03.123+0000"), timestamp_type->decompose(tp + 123ms + 1h + 2min + 3s)));
    BOOST_REQUIRE(timestamp_type->equal(timestamp_type->from_string("2015-7-3 1:2:3.123+0000"), timestamp_type->decompose(tp + 123ms + 1h + 2min + 3s)));
    BOOST_REQUIRE(timestamp_type->equal(timestamp_type->from_string("2015-007-003 001:002:003.123+0000"), timestamp_type->decompose(tp + 123ms + 1h + 2min + 3s)));
    BOOST_REQUIRE(timestamp_type->equal(timestamp_type->from_string("2015-007-003 001:002:003.1+0000"), timestamp_type->decompose(tp + 100ms + 1h + 2min + 3s)));
    BOOST_REQUIRE(timestamp_type->equal(timestamp_type->from_string("2015-07-03 12:30:00+1230"), timestamp_type->decompose(tp)));
    BOOST_REQUIRE(timestamp_type->equal(timestamp_type->from_string("2015-07-03 12:00:00+12"), timestamp_type->decompose(tp)));
    BOOST_REQUIRE(timestamp_type->equal(timestamp_type->from_string("2015-07-03 12:30:00+12:30"), timestamp_type->decompose(tp)));
    BOOST_REQUIRE(timestamp_type->equal(timestamp_type->from_string("2015-07-02 23:00-0100"), timestamp_type->decompose(tp)));
    BOOST_REQUIRE(timestamp_type->equal(timestamp_type->from_string("2015-07-03T00:00+0000"), timestamp_type->decompose(tp)));
    BOOST_REQUIRE(timestamp_type->equal(timestamp_type->from_string("2015-07-03T01:00:00+0000"), timestamp_type->decompose(tp + 1h)));
    BOOST_REQUIRE(timestamp_type->equal(timestamp_type->from_string("2015-07-03T00:00:00.123+0000"), timestamp_type->decompose(tp + 123ms)));
    BOOST_REQUIRE(timestamp_type->equal(timestamp_type->from_string("2015-07-03T12:30:00+1230"), timestamp_type->decompose(tp)));
    BOOST_REQUIRE(timestamp_type->equal(timestamp_type->from_string("2015-07-02T23:00-0100"), timestamp_type->decompose(tp)));

    BOOST_REQUIRE_EQUAL(timestamp_type->to_string(timestamp_type->decompose(tp)), "2015-07-03T00:00:00");

    auto now = time(nullptr);
    auto local_now = *localtime(&now);
    char buf[100];
    db_clock::time_point now_tp(db_clock::duration(now * 1000));
    strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S%z", &local_now);
    BOOST_REQUIRE(timestamp_type->equal(timestamp_type->from_string(buf), timestamp_type->decompose(now_tp)));
    strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &local_now);
    BOOST_REQUIRE(timestamp_type->equal(timestamp_type->from_string(buf), timestamp_type->decompose(now_tp)));

    struct tm dst = { 0 };
    dst.tm_isdst = -1;
    dst.tm_year = 2015 - 1900;
    dst.tm_mon = 1 - 1;
    dst.tm_mday = 2;
    dst.tm_hour = 3;
    dst.tm_min = 4;
    dst.tm_sec = 5;
    auto dst_jan = db_clock::from_time_t(mktime(&dst));
    strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &dst);
    BOOST_REQUIRE(timestamp_type->equal(timestamp_type->from_string(buf), timestamp_type->decompose(dst_jan)));

    dst.tm_isdst = -1;
    dst.tm_mon = 6 - 1;
    auto dst_jun = db_clock::from_time_t(mktime(&dst));
    strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &dst);
    BOOST_REQUIRE(timestamp_type->equal(timestamp_type->from_string(buf), timestamp_type->decompose(dst_jun)));

    test_parsing_fails(timestamp_type, "something");
    test_parsing_fails(timestamp_type, "2001-99-01");
    test_parsing_fails(timestamp_type, "2001-01-01 12:00:00.0a");
    test_parsing_fails(timestamp_type, "2001-01-01 12:00:00.1234");
    test_parsing_fails(timestamp_type, "2001-01-01 12:00p0000");
    test_parsing_fails(timestamp_type, "2001-01-01 12:00+1200a");
}

BOOST_AUTO_TEST_CASE(test_timestamp_string_conversions) {
    test_timestamp_like_string_conversions(timestamp_type);
}

BOOST_AUTO_TEST_CASE(test_date_string_conversions) {
    test_timestamp_like_string_conversions(date_type);
}

BOOST_AUTO_TEST_CASE(test_boolean_type_string_conversions) {
    BOOST_REQUIRE(boolean_type->equal(boolean_type->from_string(""), boolean_type->decompose(false)));
    BOOST_REQUIRE(boolean_type->equal(boolean_type->from_string("false"), boolean_type->decompose(false)));
    BOOST_REQUIRE(boolean_type->equal(boolean_type->from_string("fAlSe"), boolean_type->decompose(false)));
    BOOST_REQUIRE(boolean_type->equal(boolean_type->from_string("true"), boolean_type->decompose(true)));
    BOOST_REQUIRE(boolean_type->equal(boolean_type->from_string("tRue"), boolean_type->decompose(true)));

    BOOST_REQUIRE_EQUAL(boolean_type->to_string(boolean_type->decompose(false)), "false");
    BOOST_REQUIRE_EQUAL(boolean_type->to_string(boolean_type->decompose(true)), "true");
}

template<typename T>
void test_floating_type_compare(data_type t)
{
    auto nan = t->decompose(std::numeric_limits<T>::quiet_NaN());
    auto pinf = t->decompose(std::numeric_limits<T>::infinity());
    auto ninf = t->decompose(-std::numeric_limits<T>::infinity());
    auto pzero = t->decompose(T(0.));
    auto nzero = t->decompose(T(-0.));

    BOOST_REQUIRE(t->less(ninf, pinf));
    BOOST_REQUIRE(t->less(ninf, nan));
    BOOST_REQUIRE(t->less(pinf, nan));
    BOOST_REQUIRE(t->less(nzero, nan));
    BOOST_REQUIRE(t->less(pzero, nan));
    BOOST_REQUIRE(t->less(nzero, pinf));
    BOOST_REQUIRE(t->less(pzero, pinf));
    BOOST_REQUIRE(t->less(ninf, nzero));
    BOOST_REQUIRE(t->less(ninf, pzero));
    BOOST_REQUIRE(t->less(nzero, pzero));
}

BOOST_AUTO_TEST_CASE(test_floating_types_compare) {
    test_floating_type_compare<float>(float_type);
    test_floating_type_compare<double>(double_type);
}

BOOST_AUTO_TEST_CASE(test_duration_type_compare) {
    duration_type->equal(duration_type->from_string("3d5m"), duration_type->from_string("3d5m"));
}

BOOST_AUTO_TEST_CASE(test_varint) {
    BOOST_REQUIRE(varint_type->equal(varint_type->from_string("-1"), varint_type->decompose(boost::multiprecision::cpp_int(-1))));
    BOOST_REQUIRE(varint_type->equal(varint_type->from_string("255"), varint_type->decompose(boost::multiprecision::cpp_int(255))));
    BOOST_REQUIRE(varint_type->equal(varint_type->from_string("1"), varint_type->decompose(boost::multiprecision::cpp_int(1))));
    BOOST_REQUIRE(varint_type->equal(varint_type->from_string("0"), varint_type->decompose(boost::multiprecision::cpp_int(0))));

    BOOST_CHECK_EQUAL(value_cast<boost::multiprecision::cpp_int>(varint_type->deserialize(varint_type->from_string("-1"))),
                      boost::multiprecision::cpp_int(-1));
    BOOST_CHECK_EQUAL(value_cast<boost::multiprecision::cpp_int>(varint_type->deserialize(varint_type->from_string("255"))),
                      boost::multiprecision::cpp_int(255));
    BOOST_CHECK_EQUAL(value_cast<boost::multiprecision::cpp_int>(varint_type->deserialize(varint_type->from_string("1"))),
                      boost::multiprecision::cpp_int(1));
    BOOST_CHECK_EQUAL(value_cast<boost::multiprecision::cpp_int>(varint_type->deserialize(varint_type->from_string("0"))),
                      boost::multiprecision::cpp_int(0));

    BOOST_CHECK_EQUAL(value_cast<boost::multiprecision::cpp_int>(varint_type->deserialize(varint_type->from_string("-123"))),
                      boost::multiprecision::cpp_int(-123));
    BOOST_CHECK_EQUAL(value_cast<boost::multiprecision::cpp_int>(varint_type->deserialize(varint_type->from_string("123"))),
                      boost::multiprecision::cpp_int(123));

    BOOST_REQUIRE(varint_type->equal(from_hex("000000"), from_hex("00")));
    BOOST_REQUIRE(varint_type->equal(from_hex("ffffff"), from_hex("ff")));
    BOOST_REQUIRE(varint_type->equal(from_hex("001000"), from_hex("1000")));
    BOOST_REQUIRE(varint_type->equal(from_hex("ff9000"), from_hex("9000")));

    BOOST_REQUIRE(varint_type->equal(from_hex("ff"), varint_type->decompose(boost::multiprecision::cpp_int(-1))));

    BOOST_REQUIRE(varint_type->equal(from_hex("00ff"), varint_type->decompose(boost::multiprecision::cpp_int(255))));

    BOOST_CHECK_EQUAL(value_cast<boost::multiprecision::cpp_int>(varint_type->deserialize(from_hex("ff"))), boost::multiprecision::cpp_int(-1));
    BOOST_CHECK_EQUAL(value_cast<boost::multiprecision::cpp_int>(varint_type->deserialize(from_hex("00ff"))), boost::multiprecision::cpp_int(255));

    BOOST_REQUIRE(!varint_type->equal(from_hex("00ff"), varint_type->decompose(boost::multiprecision::cpp_int(-1))));
    BOOST_REQUIRE(!varint_type->equal(from_hex("ff"), varint_type->decompose(boost::multiprecision::cpp_int(255))));

    BOOST_REQUIRE(varint_type->equal(from_hex("00deadbeef"), varint_type->decompose(boost::multiprecision::cpp_int("0xdeadbeef"))));
    BOOST_REQUIRE(varint_type->equal(from_hex("00ffffffffffffffffffffffffffffffff"), varint_type->decompose(boost::multiprecision::cpp_int("340282366920938463463374607431768211455"))));

    BOOST_CHECK_EQUAL(value_cast<boost::multiprecision::cpp_int>(varint_type->deserialize(from_hex("00deadbeef"))), boost::multiprecision::cpp_int("0xdeadbeef"));
    BOOST_CHECK_EQUAL(value_cast<boost::multiprecision::cpp_int>(varint_type->deserialize(from_hex("00ffffffffffffffffffffffffffffffff"))), boost::multiprecision::cpp_int("340282366920938463463374607431768211455"));

    test_parsing_fails(varint_type, "1A");
}

BOOST_AUTO_TEST_CASE(test_decimal) {
    auto bd = value_cast<big_decimal>(decimal_type->deserialize(decimal_type->from_string("-1")));
    BOOST_CHECK_EQUAL(bd.scale(), 0);
    BOOST_CHECK_EQUAL(bd.unscaled_value(), -1);
    BOOST_CHECK_EQUAL(decimal_type->to_string(decimal_type->decompose(bd)), "-1");

    bd = value_cast<big_decimal>(decimal_type->deserialize(decimal_type->from_string("-1.00")));
    BOOST_CHECK_EQUAL(bd.scale(), 2);
    BOOST_CHECK_EQUAL(bd.unscaled_value(), -100);
    BOOST_CHECK_EQUAL(decimal_type->to_string(decimal_type->decompose(bd)), "-1");

    bd = value_cast<big_decimal>(decimal_type->deserialize(decimal_type->from_string("123")));
    BOOST_CHECK_EQUAL(bd.scale(), 0);
    BOOST_CHECK_EQUAL(bd.unscaled_value(), 123);
    BOOST_CHECK_EQUAL(decimal_type->to_string(decimal_type->decompose(bd)), "123");

    bd = value_cast<big_decimal>(decimal_type->deserialize(decimal_type->from_string("1.23e3")));
    BOOST_CHECK_EQUAL(bd.scale(), -1);
    BOOST_CHECK_EQUAL(bd.unscaled_value(), 123);
    BOOST_CHECK_EQUAL(decimal_type->to_string(decimal_type->decompose(bd)), "1230");

    bd = value_cast<big_decimal>(decimal_type->deserialize(decimal_type->from_string("1.23e+3")));
    BOOST_CHECK_EQUAL(bd.scale(), -1);
    BOOST_CHECK_EQUAL(bd.unscaled_value(), 123);
    BOOST_CHECK_EQUAL(decimal_type->to_string(decimal_type->decompose(bd)), "1230");

    bd = value_cast<big_decimal>(decimal_type->deserialize(decimal_type->from_string("1.23")));
    BOOST_CHECK_EQUAL(bd.scale(), 2);
    BOOST_CHECK_EQUAL(bd.unscaled_value(), 123);
    BOOST_CHECK_EQUAL(decimal_type->to_string(decimal_type->decompose(bd)), "1.23");

    bd = value_cast<big_decimal>(decimal_type->deserialize(decimal_type->from_string("0.123")));
    BOOST_CHECK_EQUAL(bd.scale(), 3);
    BOOST_CHECK_EQUAL(bd.unscaled_value(), 123);
    BOOST_CHECK_EQUAL(decimal_type->to_string(decimal_type->decompose(bd)), "0.123");

    bd = value_cast<big_decimal>(decimal_type->deserialize(decimal_type->from_string("0.00123")));
    BOOST_CHECK_EQUAL(bd.scale(), 5);
    BOOST_CHECK_EQUAL(bd.unscaled_value(), 123);
    BOOST_CHECK_EQUAL(decimal_type->to_string(decimal_type->decompose(bd)), "0.00123");

    BOOST_REQUIRE(decimal_type->equal(decimal_type->from_string("-1"), decimal_type->from_string("-1.00")));
    BOOST_REQUIRE(decimal_type->equal(decimal_type->from_string("1.23e5"), decimal_type->from_string("123000.0")));
    BOOST_REQUIRE(decimal_type->equal(decimal_type->from_string("1.23e5"), decimal_type->from_string("1230e2")));
    BOOST_REQUIRE(decimal_type->equal(decimal_type->from_string("1.23e-2"), decimal_type->from_string("0.01230")));

    BOOST_REQUIRE(!decimal_type->equal(decimal_type->from_string("-1"), decimal_type->from_string("-1.01")));
    BOOST_REQUIRE(!decimal_type->equal(decimal_type->from_string("1.23e5"), decimal_type->from_string("123000.1")));
    BOOST_REQUIRE(!decimal_type->equal(decimal_type->from_string("1.23e5"), decimal_type->from_string("1231e2")));
    BOOST_REQUIRE(!decimal_type->equal(decimal_type->from_string("1.23e-2"), decimal_type->from_string("0.01231")));
}

BOOST_AUTO_TEST_CASE(test_compound_type_compare) {
    compound_type<> type({utf8_type, utf8_type, utf8_type});

    BOOST_REQUIRE(type.compare(
        type.serialize_value({bytes("a"), bytes("b"), bytes("c")}),
        type.serialize_value({bytes("a"), bytes("b"), bytes("c")})) == 0);

    BOOST_REQUIRE(type.compare(
        type.serialize_value({bytes("a"), bytes("b"), bytes("c")}),
        type.serialize_value({bytes("a"), bytes("b"), bytes("d")})) < 0);

    BOOST_REQUIRE(type.compare(
        type.serialize_value({bytes("a"), bytes("b"), bytes("d")}),
        type.serialize_value({bytes("a"), bytes("b"), bytes("c")})) > 0);

    BOOST_REQUIRE(type.compare(
        type.serialize_value({bytes("a"), bytes("b"), bytes("d")}),
        type.serialize_value({bytes("a"), bytes("d"), bytes("c")})) < 0);

    BOOST_REQUIRE(type.compare(
        type.serialize_value({bytes("a"), bytes("d"), bytes("c")}),
        type.serialize_value({bytes("c"), bytes("b"), bytes("c")})) < 0);
}

template <typename T>
std::experimental::optional<T>
extract(data_value a) {
    if (a.is_null()) {
        return std::experimental::nullopt;
    } else {
        return std::experimental::make_optional(value_cast<T>(a));
    }
}

template <typename T>
using opt = std::experimental::optional<T>;

BOOST_AUTO_TEST_CASE(test_tuple) {
    auto t = tuple_type_impl::get_instance({int32_type, long_type, utf8_type});
    using native_type = tuple_type_impl::native_type;
    using c_type = std::tuple<opt<int32_t>, opt<int64_t>, opt<sstring>>;
    auto native_to_c = [] (native_type v) {
        return std::make_tuple(extract<int32_t>(v[0]), extract<int64_t>(v[1]), extract<sstring>(v[2]));
    };
    auto c_to_native = [] (std::tuple<opt<int32_t>, opt<int64_t>, opt<sstring>> v) {
        return native_type({std::get<0>(v), std::get<1>(v), std::get<2>(v)});
    };
    auto native_to_bytes = [t] (native_type v) {
        return t->decompose(make_tuple_value(t, v));
    };
    auto bytes_to_native = [t] (bytes v) {
        return value_cast<native_type>(t->deserialize(v));
    };
    auto c_to_bytes = [=] (c_type v) {
        return native_to_bytes(c_to_native(v));
    };
    auto bytes_to_c = [=] (bytes v) {
        return native_to_c(bytes_to_native(v));
    };
    auto round_trip = [=] (c_type v) {
        return bytes_to_c(c_to_bytes(v));
    };
    auto v1 = c_type(int32_t(1), int64_t(2), sstring("abc"));
    BOOST_REQUIRE(v1 == round_trip(v1));
    auto v2 = c_type(int32_t(1), int64_t(2), std::experimental::nullopt);
    BOOST_REQUIRE(v2 == round_trip(v2));
    auto b1 = c_to_bytes(v1);
    auto b2 = c_to_bytes(v2);
    BOOST_REQUIRE(t->compare(b1, b2) > 0);
    BOOST_REQUIRE(t->compare(b2, b2) == 0);
}

void test_validation_fails(const shared_ptr<const abstract_type>& type, bytes_view v)
{
    try {
        type->validate(v);
        BOOST_FAIL("Validation should have failed");
    } catch (const marshal_exception& e) {
        // expected
    }
}

BOOST_AUTO_TEST_CASE(test_ascii_type_validation) {
    ascii_type->validate(bytes());
    ascii_type->validate(bytes("foo"));
    test_validation_fails(ascii_type, bytes("fóo"));
}

BOOST_AUTO_TEST_CASE(test_utf8_type_validation) {
    utf8_type->validate(bytes());
    utf8_type->validate(bytes("foo"));
    utf8_type->validate(bytes("fóo"));
    test_validation_fails(utf8_type, bytes("test") + from_hex("fe"));
}

BOOST_AUTO_TEST_CASE(test_int32_type_validation) {
    int32_type->validate(bytes());
    int32_type->validate(from_hex("deadbeef"));
    test_validation_fails(int32_type, from_hex("00"));
    test_validation_fails(int32_type, from_hex("0000000000"));
}

BOOST_AUTO_TEST_CASE(test_long_type_validation) {
    long_type->validate(bytes());
    long_type->validate(from_hex("deadbeefdeadbeef"));
    test_validation_fails(long_type, from_hex("00"));
    test_validation_fails(long_type, from_hex("00000000"));
    test_validation_fails(long_type, from_hex("000000000000000000"));
}

BOOST_AUTO_TEST_CASE(test_timeuuid_type_validation) {
    auto now = utils::UUID_gen::get_time_UUID();
    timeuuid_type->validate(now.serialize());
    auto random = utils::make_random_uuid();
    test_validation_fails(timeuuid_type, random.serialize());
    test_validation_fails(timeuuid_type, from_hex("00"));
}

BOOST_AUTO_TEST_CASE(test_uuid_type_validation) {
    auto now = utils::UUID_gen::get_time_UUID();
    uuid_type->validate(now.serialize());
    auto random = utils::make_random_uuid();
    uuid_type->validate(random.serialize());
    test_validation_fails(uuid_type, from_hex("00"));
}

BOOST_AUTO_TEST_CASE(test_duration_type_validation) {
    duration_type->validate(duration_type->from_string("1m23us"));

    BOOST_REQUIRE_EXCEPTION(duration_type->validate(from_hex("ff")), marshal_exception, [](auto&& exn) {
        BOOST_REQUIRE_EQUAL("marshaling error: Expected at least 3 bytes for a duration, got 1", exn.what());
        return true;
    });

    BOOST_REQUIRE_EXCEPTION(duration_type->validate(from_hex("fffffffffffffffffe0202")),
                            marshal_exception,
                            [](auto&& exn) {
                                BOOST_REQUIRE_EQUAL("marshaling error: The duration months (9223372036854775807) must be a 32 bit integer", exn.what());
                                return true;
                            });

    BOOST_REQUIRE_EXCEPTION(duration_type->validate(from_hex("010201")), marshal_exception, [](auto&& exn) {
        BOOST_REQUIRE_EQUAL(
                "marshaling error: The duration months, days, and nanoseconds must be all of the same sign (-1, 1, -1)",
                exn.what());

        return true;
    });
}

BOOST_AUTO_TEST_CASE(test_duration_deserialization) {
    BOOST_REQUIRE_EQUAL(
            value_cast<cql_duration>(duration_type->deserialize(duration_type->from_string("1mo3d2m"))),
            cql_duration("1mo3d2m"));
}

BOOST_AUTO_TEST_CASE(test_parse_bad_hex) {
    auto parser = db::marshal::type_parser("636f6c75kd6h:org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.Int32Type)");
    BOOST_REQUIRE_THROW(parser.parse(), exceptions::syntax_exception);
}

BOOST_AUTO_TEST_CASE(test_parse_long_hex) {
    auto parser = db::marshal::type_parser("6636f6c756d6e636f6c756d6e36f6c756d6e:org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.Int32Type)");
    BOOST_REQUIRE_THROW(parser.parse(), exceptions::syntax_exception);
}

BOOST_AUTO_TEST_CASE(test_parse_valid_list) {
    auto parser = db::marshal::type_parser("636f6c756d6e:org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.Int32Type)");
    auto type = parser.parse();
    BOOST_REQUIRE(type->as_cql3_type()->to_string() == "list<int>");
}

BOOST_AUTO_TEST_CASE(test_parse_valid_set) {
    auto parser = db::marshal::type_parser("org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.Int32Type)");
    auto type = parser.parse();
    BOOST_REQUIRE(type->as_cql3_type()->to_string() == "set<int>");
}

BOOST_AUTO_TEST_CASE(test_parse_valid_map) {
    auto parser = db::marshal::type_parser("org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.Int32Type,org.apache.cassandra.db.marshal.Int32Type)");
    auto type = parser.parse();
    BOOST_REQUIRE(type->as_cql3_type()->to_string() == "map<int, int>");
}

BOOST_AUTO_TEST_CASE(test_parse_valid_duration) {
    auto parser = db::marshal::type_parser("org.apache.cassandra.db.marshal.DurationType");
    auto type = parser.parse();
    BOOST_REQUIRE(type->as_cql3_type()->to_string() == "duration");
}

BOOST_AUTO_TEST_CASE(test_parse_valid_tuple) {
    auto parser = db::marshal::type_parser("org.apache.cassandra.db.marshal.TupleType(org.apache.cassandra.db.marshal.Int32Type,org.apache.cassandra.db.marshal.Int32Type)");
    auto type = parser.parse();
    BOOST_REQUIRE(type->as_cql3_type()->to_string() == "frozen<tuple<int, int>>");
}

BOOST_AUTO_TEST_CASE(test_parse_invalid_tuple) {
    auto parser = db::marshal::type_parser("org.apache.cassandra.db.marshal.TupleType()");
    BOOST_REQUIRE_THROW(parser.parse(), exceptions::configuration_exception);
}

BOOST_AUTO_TEST_CASE(test_parse_valid_frozen_set) {
    auto parser = db::marshal::type_parser("org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.Int32Type))");
    auto type = parser.parse();
    BOOST_REQUIRE(type->as_cql3_type()->to_string() == "frozen<set<int>>");
}

BOOST_AUTO_TEST_CASE(test_parse_valid_set_frozen_set) {
    sstring frozen = "org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.Int32Type))";
    auto parser = db::marshal::type_parser("org.apache.cassandra.db.marshal.SetType(" + frozen + ")");
    auto type = parser.parse();
    BOOST_REQUIRE(type->as_cql3_type()->to_string() == "set<frozen<set<int>>>");
}

BOOST_AUTO_TEST_CASE(test_parse_valid_set_frozen_set_set) {
    sstring set_set = "org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.Int32Type))";
    sstring frozen = "org.apache.cassandra.db.marshal.FrozenType(" + set_set + ")";
    auto parser = db::marshal::type_parser("org.apache.cassandra.db.marshal.SetType(" + frozen + ")");
    auto type = parser.parse();
    BOOST_REQUIRE(type->as_cql3_type()->to_string() == "set<frozen<set<set<int>>>>");
}


BOOST_AUTO_TEST_CASE(test_parse_invalid_type) {
    auto parser = db::marshal::type_parser("636f6c756d6e:org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.Int32Type, org.apache.cassandra.db.marshal.UTF8Type)");
    BOOST_REQUIRE_THROW(parser.parse(), exceptions::configuration_exception);
}

BOOST_AUTO_TEST_CASE(test_parse_recursive_type) {
    sstring key("org.apache.cassandra.db.marshal.Int32Type");
    sstring value("org.apache.cassandra.db.marshal.TupleType(org.apache.cassandra.db.marshal.Int32Type,org.apache.cassandra.db.marshal.Int32Type)");
    auto parser = db::marshal::type_parser("org.apache.cassandra.db.marshal.MapType(" + key + "," + value + ")");
    auto type = parser.parse();
    BOOST_REQUIRE_EQUAL(type->as_cql3_type()->to_string(), "map<int, frozen<tuple<int, int>>>");
}

BOOST_AUTO_TEST_CASE(test_create_reversed_type) {
    auto ri = reversed_type_impl::get_instance(bytes_type);
    BOOST_REQUIRE(ri->is_reversed());
    BOOST_REQUIRE(ri->is_value_compatible_with(*bytes_type));
    BOOST_REQUIRE(!ri->is_compatible_with(*bytes_type));
    auto val_lt = bytes_type->decompose(data_value(bytes("a")));
    auto val_gt = bytes_type->decompose(data_value(bytes("b")));
    auto straight_comp = bytes_type->compare(bytes_view(val_lt), bytes_view(val_gt));
    auto reverse_comp = ri->compare(bytes_view(val_lt), bytes_view(val_gt));
    BOOST_REQUIRE(straight_comp == -reverse_comp);
}

BOOST_AUTO_TEST_CASE(test_create_reverse_collection_type) {
    auto my_set_type = set_type_impl::get_instance(bytes_type, true);
    auto ri = reversed_type_impl::get_instance(my_set_type);
    BOOST_REQUIRE(ri->is_reversed());
    BOOST_REQUIRE(ri->is_collection());
    BOOST_REQUIRE(ri->is_multi_cell());

    std::vector<data_value> first_set;
    bytes b1("1");
    bytes b2("2");
    first_set.push_back(data_value(b1));
    first_set.push_back(data_value(b2));

    std::vector<data_value> second_set;
    bytes b3("2");
    second_set.push_back(data_value(b1));
    second_set.push_back(data_value(b3));

    auto bv1 = my_set_type->decompose(make_set_value(my_set_type, first_set));
    auto bv2 = my_set_type->decompose(make_set_value(my_set_type, second_set));

    auto straight_comp = my_set_type->compare(bytes_view(bv1), bytes_view(bv2));
    auto reverse_comp = ri->compare(bytes_view(bv2), bytes_view(bv2));
    BOOST_REQUIRE(straight_comp == -reverse_comp);
}

BOOST_AUTO_TEST_CASE(test_parse_reversed_type) {
    sstring value("org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.Int32Type)");
    auto parser = db::marshal::type_parser(value);
    auto ri = parser.parse();
    BOOST_REQUIRE(ri->as_cql3_type()->to_string() == "int");
    BOOST_REQUIRE(ri->is_reversed());
    BOOST_REQUIRE(ri->is_value_compatible_with(*int32_type));
    BOOST_REQUIRE(!ri->is_compatible_with(*int32_type));

    auto val_lt = int32_type->decompose(1);
    auto val_gt = int32_type->decompose(2);
    auto straight_comp = int32_type->compare(bytes_view(val_lt), bytes_view(val_gt));
    auto reverse_comp = ri->compare(bytes_view(val_lt), bytes_view(val_gt));
    BOOST_REQUIRE(straight_comp == -reverse_comp);
}

BOOST_AUTO_TEST_CASE(test_reversed_type_value_compatibility) {
    auto rb = reversed_type_impl::get_instance(bytes_type);
    auto rs = reversed_type_impl::get_instance(utf8_type);

    BOOST_REQUIRE(!rb->is_compatible_with(*bytes_type));
    BOOST_REQUIRE(!rb->is_compatible_with(*utf8_type));
    BOOST_REQUIRE(rb->is_value_compatible_with(*rs));
    BOOST_REQUIRE(rb->is_value_compatible_with(*utf8_type));
}

BOOST_AUTO_TEST_CASE(test_parsing_of_user_type) {
    sstring text = "org.apache.cassandra.db.marshal.UserType(keyspace1,61646472657373,737472656574:org.apache.cassandra.db.marshal.UTF8Type,63697479:org.apache.cassandra.db.marshal.UTF8Type,7a6970:org.apache.cassandra.db.marshal.Int32Type)";
    auto type = db::marshal::type_parser::parse(text);
    BOOST_REQUIRE(type->name() == text);
}

static auto msg = [] (const char* m, data_type x, data_type y) -> std::string {
    return sprint("%s(%s, %s)", m, x->name(), y->name());
};

// Sort order does not change
auto verify_compat = [] (data_type to, data_type from) {
    BOOST_CHECK_MESSAGE(to->is_compatible_with(*from), msg("verify_compat is_compatible", to, from));
    // value compatibility is implied by compatibility
    BOOST_CHECK_MESSAGE(to->is_value_compatible_with(*from), msg("verify_compat is_value_compatible", to, from));
};
// Sort order may change
auto verify_value_compat = [] (data_type to, data_type from) {
    BOOST_CHECK_MESSAGE(!to->is_compatible_with(*from), msg("verify_value_compat !is_compatible", to, from)); // or verify_compat would be used
    BOOST_CHECK_MESSAGE(to->is_value_compatible_with(*from), msg("verify_value_compat is_value_compatible", to, from));
};
// Cannot be cast
auto verify_not_compat = [] (data_type to, data_type from) {
    BOOST_CHECK_MESSAGE(!to->is_compatible_with(*from), msg("verify_not_compat !is_compatible", to, from));
    BOOST_CHECK_MESSAGE(!to->is_value_compatible_with(*from), msg("verify_not_compat !is_value_compatible", to, from));
};
auto cc = verify_compat;
auto vc = verify_value_compat;
auto nc = verify_not_compat;

struct test_case {
    void (*verify)(data_type to, data_type from);
    data_type to;
    data_type from;
};

BOOST_AUTO_TEST_CASE(test_collection_type_compatibility) {
    auto m__bi = map_type_impl::get_instance(bytes_type, int32_type, true);
    auto mf_bi = map_type_impl::get_instance(bytes_type, int32_type, false);
    auto m__bb = map_type_impl::get_instance(bytes_type, bytes_type, true);
    auto mf_bb = map_type_impl::get_instance(bytes_type, bytes_type, false);
    auto m__ii = map_type_impl::get_instance(int32_type, int32_type, true);
    auto mf_ii = map_type_impl::get_instance(int32_type, int32_type, false);
    auto m__ib = map_type_impl::get_instance(int32_type, bytes_type, true);
    auto mf_ib = map_type_impl::get_instance(int32_type, bytes_type, false);
    auto s__i = set_type_impl::get_instance(int32_type, true);
    auto sf_i = set_type_impl::get_instance(int32_type, false);
    auto s__b = set_type_impl::get_instance(bytes_type, true);
    auto sf_b = set_type_impl::get_instance(bytes_type, false);
    auto l__i = list_type_impl::get_instance(int32_type, true);
    auto lf_i = list_type_impl::get_instance(int32_type, false);
    auto l__b = list_type_impl::get_instance(bytes_type, true);
    auto lf_b = list_type_impl::get_instance(bytes_type, false);

    test_case tests[] = {
            { nc, m__bi, int32_type },  // collection vs. primitiv
            { cc, m__bi, m__bi },       // identity
            { nc, m__bi, m__ib },       // key not compatible
            { nc, mf_bi, mf_ib },       //  "
            { nc, m__bb, mf_bb },       // frozen vs. unfrozen
            { nc, mf_ii, mf_bb },       // key not compatible
            { nc, mf_ii, mf_ib },       // frozen, and value not compatible
            { cc, m__ib, m__ii },       // unfrozen so values don't need to sort
            { nc, m__ii, m__bb },       // key not compatible
            { nc, m__ii, m__bi },       // key not compatible
            { nc, m__ii, m__ib },       // values not compatible
            { vc, mf_ib, mf_ii },       // values value-compatible but don't sort
            { nc, l__i,  s__i },        // different collection kinds
            { nc, s__b,  s__i },        // different sorts
            { nc, sf_b,  sf_i },        // different sorts
            { nc, sf_i,  s__i },        // different temperature
            { nc, sf_i,  sf_b },        // elements not compatible
            { cc, l__b,  l__i },        // unfrozen so values don't need to sort
            { vc, lf_b,  lf_i },        // values don't sort, so only value-compatible
            { nc, lf_i,  l__i },        // different temperature
            { nc, lf_i,  lf_b },        // elements not compatible
    };
    for (auto&& tc : tests) {
        tc.verify(tc.to, tc.from);
    }
}

SEASTAR_TEST_CASE(test_simple_type_compatibility) {
    test_case tests[] = {
        { vc, bytes_type, int32_type },
        { nc, int32_type, bytes_type },
        { vc, varint_type, int32_type },
        { vc, varint_type, long_type },
        { nc, int32_type, varint_type },
        { nc, long_type, varint_type },
        { cc, bytes_type, utf8_type },
        { cc, utf8_type, ascii_type },
        { cc, bytes_type, ascii_type },
        { nc, utf8_type, bytes_type },
        { nc, ascii_type, bytes_type },
        { nc, ascii_type, utf8_type },
        { cc, timestamp_type, date_type },
        { cc, date_type, timestamp_type },
    };
    for (auto&& tc : tests) {
        tc.verify(tc.to, tc.from);
    }
    return make_ready_future<>();
}
