
/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#define BOOST_TEST_MODULE test-serialization
#include <boost/test/unit_test.hpp>

#include <iostream>
#include <assert.h>
#include <sstream>
#include <initializer_list>

#include "utils/serialization.hh"
#include "types/types.hh"
#include "gms/inet_address.hh"
#include "gms/inet_address_serializer.hh"
#include "test/lib/test_utils.hh"
#include "serializer_impl.hh"

void show(std::stringstream &ss) {
	char c;
	while (ss.get(c)) {
		std::cout << (int) (unsigned char) c << ' ';
	}
	ss.str("");
	ss.clear();
}

// Test that the serialization/de-serialization round-trip results in the
// same object we started with, and not with "the vodka is good, but the meat
// is rotten" type of translation :-)
int8_t back_and_forth_8(int8_t a) {
    std::stringstream buf;
    auto it = std::ostream_iterator<char>(buf);
    serialize_int8(it, a);
    auto str = buf.str();
    auto bview = bytes_view(reinterpret_cast<const int8_t*>(str.data()), 1);
    return read_simple<uint8_t>(bview);
}
int16_t back_and_forth_16(int16_t a) {
    std::stringstream buf;
    auto it = std::ostream_iterator<char>(buf);
    serialize_int16(it, a);
    auto str = buf.str();
    auto bview = bytes_view(reinterpret_cast<const int8_t*>(str.data()), 2);
    return read_simple<uint16_t>(bview);
}
int32_t back_and_forth_32(int32_t a) {
    std::stringstream buf;
    auto it = std::ostream_iterator<char>(buf);
    serialize_int32(it, a);
    auto str = buf.str();
    auto bview = bytes_view(reinterpret_cast<const int8_t*>(str.data()), 4);
    return read_simple<uint32_t>(bview);
}
int64_t back_and_forth_64(int64_t a) {
    std::stringstream buf;
    auto it = std::ostream_iterator<char>(buf);
    serialize_int64(it, a);
    auto str = buf.str();
    auto bview = bytes_view(reinterpret_cast<const int8_t*>(str.data()), 8);
    return read_simple<uint64_t>(bview);
}
sstring back_and_forth_sstring(sstring a) {
    std::stringstream buf;
    auto it = std::ostream_iterator<char>(buf);
    serialize_string(it, a);
    auto str = buf.str();
    auto bview = bytes_view(reinterpret_cast<const int8_t*>(str.data()), str.size());
    auto res = read_simple_short_string(bview);
    sstring str_res = sstring(reinterpret_cast<const char*>(res.data()), res.size());
    return str_res;
}
BOOST_AUTO_TEST_CASE(round_trip) {
    BOOST_CHECK_EQUAL(back_and_forth_8('a'), 'a');
    BOOST_CHECK_EQUAL(back_and_forth_16(1), 1);
    BOOST_CHECK_EQUAL(back_and_forth_16(12345), 12345);
    BOOST_CHECK_EQUAL(back_and_forth_32(12345), 12345);
    BOOST_CHECK_EQUAL(back_and_forth_32(1234567), 1234567);
    BOOST_CHECK_EQUAL(back_and_forth_64(1234567), 1234567);
    BOOST_CHECK_EQUAL(back_and_forth_64(1234567890123LL), 1234567890123LL);
    BOOST_CHECK_EQUAL(back_and_forth_sstring(sstring("hello")), sstring("hello"));
}

// Test the result of serialization against expected data as produced by the
// following Java code:
//    static class zzzOutputStream extends OutputStream {
//        @Override
//        public void write(int b) throws IOException {
//            System.out.print(Integer.toString(b & 0xff ) + ' ');
//        }
//    }
//    public static void main(String[] args) throws IOException {
//        DataOutputStream out = new DataOutputStream(new zzzOutputStream());
//        System.out.print("char 'a': ");
//        out.writeByte('a');
//        System.out.println();
//        System.out.print("int '1234567': ");
//        out.writeInt(1234567);
//        System.out.println();
//        System.out.print("16-bit '12345': ");
//        out.writeShort(12345);
//        System.out.println();
//        System.out.print("64-bit '1234567890123': ");
//        out.writeLong(1234567890123L);
//        System.out.println();
//        System.out.print("string 'hello': ");
//        out.writeUTF("hello");
//        System.out.println();
//    }
// its output:
//    char 'a': 97
//    int '1234567': 0 18 214 135
//    16-bit '12345': 48 57
//    64-bit '1234567890123': 0 0 1 31 113 251 4 203
//    string 'hello': 0 5 104 101 108 108 111

bool expect_bytes(std::stringstream &buf, std::initializer_list<unsigned char> chars) {
    bool fail = false;
    for (char e : chars) {
        char c;
        if (!buf.get(c) || c != e) {
            fail = true;
            break;
        }
    }
    if (!fail) {
        // we don't expect to be able to read any more bytes
        char c;
        if (buf.get(c)) {
            fail = true;
        }
    }
    // whatever happened in this test, clear the buffer for the next one
    buf.str("");
    buf.clear();
    return !fail;
}

BOOST_AUTO_TEST_CASE(expected) {
    std::stringstream buf;
    auto it = std::ostream_iterator<char>(buf);

	serialize_int8(it, 'a');
	BOOST_CHECK(expect_bytes(buf, {97}));

    it = std::ostream_iterator<char>(buf);
    serialize_int32(it, 1234567);
    BOOST_CHECK(expect_bytes(buf, {0, 18, 214, 135}));

    it = std::ostream_iterator<char>(buf);
    serialize_int16(it, (uint16_t)12345);
    BOOST_CHECK(expect_bytes(buf, {48, 57}));

    it = std::ostream_iterator<char>(buf);
    serialize_int64(it, 1234567890123UL);
    BOOST_CHECK(expect_bytes(buf, {0, 0, 1, 31, 113, 251, 4, 203}));

    it = std::ostream_iterator<char>(buf);
    serialize_string(it, "hello");
    BOOST_CHECK(expect_bytes(buf, {0, 5, 104, 101, 108, 108, 111}));

    it = std::ostream_iterator<char>(buf);
    serialize_string(it, sstring("hello"));
    BOOST_CHECK(expect_bytes(buf, {0, 5, 104, 101, 108, 108, 111}));
}

BOOST_AUTO_TEST_CASE(inet_address) {
    {
        uint32_t hip = 127u << 24 | 1u;
        gms::inet_address ip(hip);
        BOOST_CHECK(ip.addr().is_ipv4());
        auto buf = ser::serialize_to_buffer<bytes>(ip);
        BOOST_CHECK_EQUAL(buf.size(), sizeof(uint32_t));
        auto res = ser::deserialize_from_buffer(buf, std::type_identity<gms::inet_address>{});
        uint32_t rip = res.addr().as_ipv4_address().ip;
        BOOST_CHECK_EQUAL(hip, rip);
    }
    {
        gms::inet_address ip("2001:6b0:8:2::232");
        BOOST_CHECK(ip.addr().is_ipv6());
        auto buf = ser::serialize_to_buffer<bytes>(ip);
        auto res = ser::deserialize_from_buffer(buf, std::type_identity<gms::inet_address>{});
        BOOST_CHECK_EQUAL(res, ip);
    }

    // stringify tests
    {
        for (sstring s : { "2001:6b0:8:2::232", "2a05:d018:223:f00:97af:f4d9:eac2:6a0f", "fe80::8898:3e04:215b:2cd6" }) {
            gms::inet_address ip(s);
            BOOST_CHECK(ip.addr().is_ipv6());
            auto s2 = fmt::to_string(ip);
            gms::inet_address ip2(s);
            BOOST_CHECK_EQUAL(ip2, ip);
        }
    }
}

template <typename T>
static void test_vector_deserializer(const std::vector<T>& v) {
    auto buf = ser::serialize_to_buffer<bytes>(v);
    auto in = simple_input_stream((const char*)buf.data(), buf.size());
    auto range = ser::vector_deserializer<T>(in);

    auto test_equal = [] (const T& lhs, const T& rhs) {
        if (lhs != rhs) {
            throw std::runtime_error("compared values differ");
        }
    };

    auto required = [] (bool x) {
        if (!x) {
            throw std::runtime_error(format("failed requirement"));
        }
    };

    {
        auto vit = v.begin();
        auto rit = range.begin();
        while (rit != range.end()) {
            test_equal(*rit, *vit);
            ++rit;
            ++vit;
        }
        required(vit == v.end());
    }

    {
        auto vit = v.begin();
        auto rit = range.begin();
        while (rit != range.end()) {
            test_equal(*rit++, *vit++);
        }
        required(vit == v.end());
    }

    {
        auto cvit = v.cbegin();
        auto crit = range.cbegin();
        while (crit != range.cend()) {
            test_equal(*crit++, *cvit++);
        }
        required(cvit == v.cend());
    }

    {
        auto vit = v.begin();
        for (auto i : range) {
            test_equal(i, *vit++);
        }
    }
}

template <typename T>
static void test_reverse_vector_deserializer(const std::vector<T>& v) {
    auto buf = ser::serialize_to_buffer<bytes>(v);
    auto in = simple_input_stream((const char*)buf.data(), buf.size());
    auto range = ser::vector_deserializer<T, false>(in);

    auto test_equal = [] (const T& lhs, const T& rhs) {
        if (lhs != rhs) {
            throw std::runtime_error("compared values differ");
        }
    };

    auto required = [] (bool x) {
        if (!x) {
            throw std::runtime_error(format("failed requirement"));
        }
    };

    {
        auto vit = v.rbegin();
        auto rit = range.begin();
        while (rit != range.end()) {
            test_equal(*rit, *vit);
            ++rit;
            ++vit;
        }
        required(vit == v.rend());
    }

    {
        auto vit = v.rbegin();
        auto rit = range.begin();
        while (rit != range.end()) {
            test_equal(*rit++, *vit++);
        }
        required(vit == v.rend());
    }

    {
        auto cvit = v.crbegin();
        auto crit = range.cbegin();
        while (crit != range.cend()) {
            test_equal(*crit++, *cvit++);
        }
        required(cvit == v.crend());
    }

    {
        auto vit = v.rbegin();
        for (auto i : range) {
            test_equal(i, *vit++);
        }
    }
}

BOOST_AUTO_TEST_CASE(vector_deserializer) {
    std::vector<int> int_vect = { 3, 1, 4 };
    test_vector_deserializer(int_vect);
    test_reverse_vector_deserializer(int_vect);

    std::vector<sstring> sstring_vect = { "testing", "one", "two", "three" };
    test_vector_deserializer(sstring_vect);
    test_reverse_vector_deserializer(sstring_vect);

    std::vector<std::optional<bool>> opt_bool_vect = { true, false, {}, false, true };
    test_vector_deserializer(opt_bool_vect);
    test_reverse_vector_deserializer(opt_bool_vect);
}
