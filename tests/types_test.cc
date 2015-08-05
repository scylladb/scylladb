/*
 * Copyright 2015 Cloudius Systems
 */

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE core

#include <boost/test/unit_test.hpp>
#include <utils/UUID_gen.hh>
#include <boost/asio/ip/address_v4.hpp>
#include <net/ip.hh>
#include <boost/multiprecision/cpp_int.hpp>
#include "types.hh"
#include "compound.hh"
#include "db/marshal/type_parser.hh"
#include "cql3/cql3_type.hh"

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
    BOOST_REQUIRE(bytes_type->equal(bytes_type->from_string("616263646566"), bytes_type->decompose(bytes{"abcdef"})));
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
    BOOST_REQUIRE_EQUAL(int32_type->to_string(int32_type->decompose((int32_t)-2147483647)), "-2147483647");

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
    net::ipv4_address addr("127.0.0.1");
    BOOST_REQUIRE(inet_addr_type->equal(inet_addr_type->from_string("127.0.0.1"), inet_addr_type->decompose(addr)));

    test_parsing_fails(inet_addr_type, "something");
    test_parsing_fails(inet_addr_type, "300.127.127.127");
    test_parsing_fails(inet_addr_type, "127-127.127.127");
    test_parsing_fails(inet_addr_type, "127.127.127.127.127");
}

BOOST_AUTO_TEST_CASE(test_timestamp_type_string_conversions) {
    timestamp_type->from_string("now");
    db_clock::time_point tp(db_clock::duration(1435881600000));
    BOOST_REQUIRE(timestamp_type->equal(timestamp_type->from_string("1435881600000"), timestamp_type->decompose(tp)));
    BOOST_REQUIRE(timestamp_type->equal(timestamp_type->from_string("2015-07-03+0000"), timestamp_type->decompose(tp)));
    BOOST_REQUIRE(timestamp_type->equal(timestamp_type->from_string("2015-07-03-00"), timestamp_type->decompose(tp)));
    BOOST_REQUIRE(timestamp_type->equal(timestamp_type->from_string("2015-07-03 00:00+0000"), timestamp_type->decompose(tp)));
    BOOST_REQUIRE(timestamp_type->equal(timestamp_type->from_string("2015-07-03 01:00:00+0000"), timestamp_type->decompose(tp + 1h)));
    BOOST_REQUIRE(timestamp_type->equal(timestamp_type->from_string("2015-07-03 01:02:03.123+0000"), timestamp_type->decompose(tp + 123ms + 1h + 2min + 3s)));
    BOOST_REQUIRE(timestamp_type->equal(timestamp_type->from_string("2015-07-03 12:30:00+1230"), timestamp_type->decompose(tp)));
    BOOST_REQUIRE(timestamp_type->equal(timestamp_type->from_string("2015-07-03 12:00:00+12"), timestamp_type->decompose(tp)));
    BOOST_REQUIRE(timestamp_type->equal(timestamp_type->from_string("2015-07-03 12:30:00+12:30"), timestamp_type->decompose(tp)));
    BOOST_REQUIRE(timestamp_type->equal(timestamp_type->from_string("2015-07-02 23:00-0100"), timestamp_type->decompose(tp)));
    BOOST_REQUIRE(timestamp_type->equal(timestamp_type->from_string("2015-07-03T00:00+0000"), timestamp_type->decompose(tp)));
    BOOST_REQUIRE(timestamp_type->equal(timestamp_type->from_string("2015-07-03T01:00:00+0000"), timestamp_type->decompose(tp + 1h)));
    BOOST_REQUIRE(timestamp_type->equal(timestamp_type->from_string("2015-07-03T00:00:00.123+0000"), timestamp_type->decompose(tp + 123ms)));
    BOOST_REQUIRE(timestamp_type->equal(timestamp_type->from_string("2015-07-03T12:30:00+1230"), timestamp_type->decompose(tp)));
    BOOST_REQUIRE(timestamp_type->equal(timestamp_type->from_string("2015-07-02T23:00-0100"), timestamp_type->decompose(tp)));

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
    test_parsing_fails(timestamp_type, "2001-01-01 12:00p0000");
    test_parsing_fails(timestamp_type, "2001-01-01 12:00+1200a");
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

BOOST_AUTO_TEST_CASE(test_varint) {
    BOOST_REQUIRE(varint_type->equal(varint_type->from_string("-1"), varint_type->decompose(boost::multiprecision::cpp_int(-1))));
    BOOST_REQUIRE(varint_type->equal(varint_type->from_string("255"), varint_type->decompose(boost::multiprecision::cpp_int(255))));
    BOOST_REQUIRE(varint_type->equal(varint_type->from_string("1"), varint_type->decompose(boost::multiprecision::cpp_int(1))));
    BOOST_REQUIRE(varint_type->equal(varint_type->from_string("0"), varint_type->decompose(boost::multiprecision::cpp_int(0))));

    BOOST_CHECK_EQUAL(boost::any_cast<boost::multiprecision::cpp_int>(varint_type->deserialize(varint_type->from_string("-1"))),
                      boost::multiprecision::cpp_int(-1));
    BOOST_CHECK_EQUAL(boost::any_cast<boost::multiprecision::cpp_int>(varint_type->deserialize(varint_type->from_string("255"))),
                      boost::multiprecision::cpp_int(255));
    BOOST_CHECK_EQUAL(boost::any_cast<boost::multiprecision::cpp_int>(varint_type->deserialize(varint_type->from_string("1"))),
                      boost::multiprecision::cpp_int(1));
    BOOST_CHECK_EQUAL(boost::any_cast<boost::multiprecision::cpp_int>(varint_type->deserialize(varint_type->from_string("0"))),
                      boost::multiprecision::cpp_int(0));

    BOOST_CHECK_EQUAL(boost::any_cast<boost::multiprecision::cpp_int>(varint_type->deserialize(varint_type->from_string("-123"))),
                      boost::multiprecision::cpp_int(-123));
    BOOST_CHECK_EQUAL(boost::any_cast<boost::multiprecision::cpp_int>(varint_type->deserialize(varint_type->from_string("123"))),
                      boost::multiprecision::cpp_int(123));

    BOOST_REQUIRE(varint_type->equal(from_hex("000000"), from_hex("00")));
    BOOST_REQUIRE(varint_type->equal(from_hex("ffffff"), from_hex("ff")));
    BOOST_REQUIRE(varint_type->equal(from_hex("001000"), from_hex("1000")));
    BOOST_REQUIRE(varint_type->equal(from_hex("ff9000"), from_hex("9000")));

    BOOST_REQUIRE(varint_type->equal(from_hex("ff"), varint_type->decompose(boost::multiprecision::cpp_int(-1))));

    BOOST_REQUIRE(varint_type->equal(from_hex("00ff"), varint_type->decompose(boost::multiprecision::cpp_int(255))));

    BOOST_CHECK_EQUAL(boost::any_cast<boost::multiprecision::cpp_int>(varint_type->deserialize(from_hex("ff"))), boost::multiprecision::cpp_int(-1));
    BOOST_CHECK_EQUAL(boost::any_cast<boost::multiprecision::cpp_int>(varint_type->deserialize(from_hex("00ff"))), boost::multiprecision::cpp_int(255));

    BOOST_REQUIRE(!varint_type->equal(from_hex("00ff"), varint_type->decompose(boost::multiprecision::cpp_int(-1))));
    BOOST_REQUIRE(!varint_type->equal(from_hex("ff"), varint_type->decompose(boost::multiprecision::cpp_int(255))));

    BOOST_REQUIRE(varint_type->equal(from_hex("00deadbeef"), varint_type->decompose(boost::multiprecision::cpp_int("0xdeadbeef"))));
    BOOST_REQUIRE(varint_type->equal(from_hex("00ffffffffffffffffffffffffffffffff"), varint_type->decompose(boost::multiprecision::cpp_int("340282366920938463463374607431768211455"))));

    BOOST_CHECK_EQUAL(boost::any_cast<boost::multiprecision::cpp_int>(varint_type->deserialize(from_hex("00deadbeef"))), boost::multiprecision::cpp_int("0xdeadbeef"));
    BOOST_CHECK_EQUAL(boost::any_cast<boost::multiprecision::cpp_int>(varint_type->deserialize(from_hex("00ffffffffffffffffffffffffffffffff"))), boost::multiprecision::cpp_int("340282366920938463463374607431768211455"));

    test_parsing_fails(varint_type, "1A");
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
extract(boost::any a) {
    if (a.empty()) {
        return std::experimental::nullopt;
    } else {
        return std::experimental::make_optional(boost::any_cast<T>(a));
    }
}

template <typename T>
boost::any
unextract(std::experimental::optional<T> v) {
    if (v) {
        return boost::any(*v);
    } else {
        return boost::any();
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
        return native_type({unextract(std::get<0>(v)), unextract(std::get<1>(v)), unextract(std::get<2>(v))});
    };
    auto native_to_bytes = [t] (native_type v) {
        return t->decompose(v);
    };
    auto bytes_to_native = [t] (bytes v) {
        return boost::any_cast<native_type>(t->deserialize(v));
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
    timeuuid_type->validate(now.to_bytes());
    auto random = utils::make_random_uuid();
    test_validation_fails(timeuuid_type, random.to_bytes());
    test_validation_fails(timeuuid_type, from_hex("00"));
}

BOOST_AUTO_TEST_CASE(test_uuid_type_validation) {
    auto now = utils::UUID_gen::get_time_UUID();
    uuid_type->validate(now.to_bytes());
    auto random = utils::make_random_uuid();
    uuid_type->validate(random.to_bytes());
    test_validation_fails(uuid_type, from_hex("00"));
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

BOOST_AUTO_TEST_CASE(test_parse_valid_tuple) {
    auto parser = db::marshal::type_parser("org.apache.cassandra.db.marshal.TupleType(org.apache.cassandra.db.marshal.Int32Type,org.apache.cassandra.db.marshal.Int32Type)");
    auto type = parser.parse();
    BOOST_REQUIRE(type->as_cql3_type()->to_string() == "tuple<int, int>");
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
    BOOST_REQUIRE(type->as_cql3_type()->to_string() == "map<int, tuple<int, int>>");
}

BOOST_AUTO_TEST_CASE(test_create_reversed_type) {
    auto ri = reversed_type_impl::get_instance(bytes_type);
    BOOST_REQUIRE(ri->is_reversed());
    BOOST_REQUIRE(ri->is_value_compatible_with(*bytes_type));
    BOOST_REQUIRE(!ri->is_compatible_with(*bytes_type));
    auto val_lt = bytes_type->decompose(bytes("a"));
    auto val_gt = bytes_type->decompose(bytes("b"));
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

    std::vector<boost::any> first_set;
    bytes b1("1");
    bytes b2("2");
    first_set.push_back(boost::any(b1));
    first_set.push_back(boost::any(b2));

    std::vector<boost::any> second_set;
    bytes b3("2");
    second_set.push_back(boost::any(b1));
    second_set.push_back(boost::any(b3));

    auto bv1 = my_set_type->decompose(first_set);
    auto bv2 = my_set_type->decompose(second_set);

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
