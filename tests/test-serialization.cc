
/*
 * Copyright 2015 Cloudius Systems
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

#include <iostream>
#include <assert.h>
#include <sstream>
#include <initializer_list>

#include "utils/serialization.hh"

#define BOOST_TEST_MODULE test-serialization
#define BOOST_TEST_DYN_LINK
#include <boost/test/unit_test.hpp>

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
    serialize_int8(buf, a);
    return deserialize_int8(buf);
}
int16_t back_and_forth_16(int16_t a) {
    std::stringstream buf;
    serialize_int16(buf, a);
    return deserialize_int16(buf);
}
int32_t back_and_forth_32(int32_t a) {
    std::stringstream buf;
    serialize_int32(buf, a);
    return deserialize_int32(buf);
}
int64_t back_and_forth_64(int64_t a) {
    std::stringstream buf;
    serialize_int64(buf, a);
    return deserialize_int64(buf);
}
sstring back_and_forth_sstring(sstring a) {
    std::stringstream buf;
    serialize_string(buf, a);
    return deserialize_string(buf);
}
BOOST_AUTO_TEST_CASE(round_trip) {
    std::stringstream out;
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

	serialize_int8(buf, 'a');
	BOOST_CHECK(expect_bytes(buf, {97}));

    serialize_int32(buf, 1234567);
    BOOST_CHECK(expect_bytes(buf, {0, 18, 214, 135}));

    serialize_int16(buf, (uint16_t)12345);
    BOOST_CHECK(expect_bytes(buf, {48, 57}));

    serialize_int64(buf, 1234567890123UL);
    BOOST_CHECK(expect_bytes(buf, {0, 0, 1, 31, 113, 251, 4, 203}));

    serialize_string(buf, "hello");
    BOOST_CHECK(expect_bytes(buf, {0, 5, 104, 101, 108, 108, 111}));

    serialize_string(buf, sstring("hello"));
    BOOST_CHECK(expect_bytes(buf, {0, 5, 104, 101, 108, 108, 111}));
}
