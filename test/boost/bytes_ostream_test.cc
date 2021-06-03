/*
 * Copyright (C) 2015-present ScyllaDB
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

#include <boost/range/algorithm/for_each.hpp>

#include <seastar/util/variant_utils.hh>

#include "bytes_ostream.hh"
#include <boost/test/unit_test.hpp>
#include "serializer_impl.hh"

#include "test/lib/random_utils.hh"
#include "test/lib/log.hh"

void append_sequence(bytes_ostream& buf, int count) {
    for (int i = 0; i < count; i++) {
        ser::serialize(buf, i);
    }
}

void assert_sequence(bytes_ostream& buf, int count) {
    auto in = ser::as_input_stream(buf.linearize());
    assert(buf.size() == count * sizeof(int));
    for (int i = 0; i < count; i++) {
        auto val = ser::deserialize(in, boost::type<int>());
        BOOST_REQUIRE_EQUAL(val, i);
    }
}

BOOST_AUTO_TEST_CASE(test_appended_data_is_retained) {
    bytes_ostream buf;
    append_sequence(buf, 1024);
    assert_sequence(buf, 1024);
}

BOOST_AUTO_TEST_CASE(test_copy_constructor) {
    bytes_ostream buf;
    append_sequence(buf, 1024);

    bytes_ostream buf2(buf);

    BOOST_REQUIRE(buf.size() == 1024 * sizeof(int));
    BOOST_REQUIRE(buf2.size() == 1024 * sizeof(int));

    assert_sequence(buf, 1024);
    assert_sequence(buf2, 1024);
}

BOOST_AUTO_TEST_CASE(test_copy_assignment) {
    bytes_ostream buf;
    append_sequence(buf, 512);

    bytes_ostream buf2;
    append_sequence(buf2, 1024);

    buf2 = buf;

    BOOST_REQUIRE(buf.size() == 512 * sizeof(int));
    BOOST_REQUIRE(buf2.size() == 512 * sizeof(int));

    assert_sequence(buf, 512);
    assert_sequence(buf2, 512);
}

BOOST_AUTO_TEST_CASE(test_move_assignment) {
    bytes_ostream buf;
    append_sequence(buf, 512);

    bytes_ostream buf2;
    append_sequence(buf2, 1024);

    buf2 = std::move(buf);

    BOOST_REQUIRE(buf.size() == 0);
    BOOST_REQUIRE(buf2.size() == 512 * sizeof(int));

    assert_sequence(buf2, 512);
}

BOOST_AUTO_TEST_CASE(test_move_constructor) {
    bytes_ostream buf;
    append_sequence(buf, 1024);

    bytes_ostream buf2(std::move(buf));

    BOOST_REQUIRE(buf.size() == 0);
    BOOST_REQUIRE(buf2.size() == 1024 * sizeof(int));

    assert_sequence(buf2, 1024);
}

BOOST_AUTO_TEST_CASE(test_size) {
    bytes_ostream buf;
    append_sequence(buf, 1024);
    BOOST_REQUIRE_EQUAL(buf.size(), sizeof(int) * 1024);
}

BOOST_AUTO_TEST_CASE(test_is_linearized) {
    bytes_ostream buf;

    BOOST_REQUIRE(buf.is_linearized());

    ser::serialize(buf, 1);

    BOOST_REQUIRE(buf.is_linearized());

    append_sequence(buf, 1024);

    BOOST_REQUIRE(!buf.is_linearized()); // probably
}

BOOST_AUTO_TEST_CASE(test_view) {
    bytes_ostream buf;

    ser::serialize(buf, 1);

    BOOST_REQUIRE(buf.is_linearized());

    auto in = ser::as_input_stream(buf.view());
    BOOST_REQUIRE_EQUAL(1, ser::deserialize(in, boost::type<int>()));
}

BOOST_AUTO_TEST_CASE(test_writing_blobs) {
    bytes_ostream buf;

    bytes b("hello");
    bytes_view b_view(b.begin(), b.size());

    buf.write(b_view);
    BOOST_REQUIRE(buf.linearize() == b_view);
}

BOOST_AUTO_TEST_CASE(test_writing_large_blobs) {
    bytes_ostream buf;

    bytes b(bytes::initialized_later(), 1024);
    std::fill(b.begin(), b.end(), 7);
    bytes_view b_view(b.begin(), b.size());

    buf.write(b_view);

    auto buf_view = buf.linearize();
    BOOST_REQUIRE(std::all_of(buf_view.begin(), buf_view.end(), [] (auto&& c) { return c == 7; }));
}

BOOST_AUTO_TEST_CASE(test_fragment_iteration) {
    int count = 64*1024;

    bytes_ostream buf;
    append_sequence(buf, count);

    bytes_ostream buf2;
    for (bytes_view frag : buf.fragments()) {
        buf2.write(frag);
    }

    // If this fails, we will only have one fragment, and the test will be weak.
    // Bump up the 'count' if this is triggered.
    assert(!buf2.is_linearized());

    assert_sequence(buf2, count);
}

BOOST_AUTO_TEST_CASE(test_writing_empty_blobs) {
    bytes_ostream buf;

    bytes b;
    buf.write(b);

    BOOST_REQUIRE(buf.size() == 0);
    BOOST_REQUIRE(buf.linearize().empty());
}

BOOST_AUTO_TEST_CASE(test_retraction_to_initial_state) {
    bytes_ostream buf;

    auto pos = buf.pos();
    ser::serialize(buf, 1);

    buf.retract(pos);

    BOOST_REQUIRE(buf.size() == 0);
    BOOST_REQUIRE(buf.linearize().empty());
}

BOOST_AUTO_TEST_CASE(test_retraction_to_the_same_chunk) {
    bytes_ostream buf;

    ser::serialize(buf, 1);
    ser::serialize(buf, 2);
    auto pos = buf.pos();
    ser::serialize(buf, 3);
    ser::serialize(buf, 4);

    buf.retract(pos);

    BOOST_REQUIRE(buf.size() == sizeof(int) * 2);

    auto in = ser::as_input_stream(buf.view());
    BOOST_REQUIRE_EQUAL(ser::deserialize(in, boost::type<int>()), 1);
    BOOST_REQUIRE_EQUAL(ser::deserialize(in, boost::type<int>()), 2);
    BOOST_REQUIRE(in.size() == 0);
}

BOOST_AUTO_TEST_CASE(test_no_op_retraction) {
    bytes_ostream buf;

    ser::serialize(buf, 1);
    ser::serialize(buf, 2);
    auto pos = buf.pos();

    buf.retract(pos);

    BOOST_REQUIRE(buf.size() == sizeof(int) * 2);

    auto in = ser::as_input_stream(buf.view());
    BOOST_REQUIRE_EQUAL(ser::deserialize(in, boost::type<int>()), 1);
    BOOST_REQUIRE_EQUAL(ser::deserialize(in, boost::type<int>()), 2);
    BOOST_REQUIRE(in.size() == 0);
}

BOOST_AUTO_TEST_CASE(test_retraction_discarding_chunks) {
    bytes_ostream buf;

    ser::serialize(buf, 1);
    auto pos = buf.pos();
    append_sequence(buf, 64*1024);

    buf.retract(pos);

    BOOST_REQUIRE(buf.size() == sizeof(int));

    auto in = ser::as_input_stream(buf.view());
    BOOST_REQUIRE_EQUAL(ser::deserialize(in, boost::type<int>()), 1);
    BOOST_REQUIRE(in.size() == 0);
}

BOOST_AUTO_TEST_CASE(test_writing_placeholders) {
    bytes_ostream buf;

    auto ph = buf.write_place_holder<int>();
    ser::serialize(buf, 2);
    auto ph_stream = ph.get_stream();
    ser::serialize(ph_stream, 1);

    auto in = ser::as_input_stream(buf.view());
    BOOST_REQUIRE_EQUAL(ser::deserialize(in, boost::type<int>()), 1);
    BOOST_REQUIRE_EQUAL(ser::deserialize(in, boost::type<int>()), 2);
    BOOST_REQUIRE(in.size() == 0);
}

BOOST_AUTO_TEST_CASE(test_large_placeholder) {
    bytes_ostream::size_type size;
    try {
        for (size = 1; (int32_t)size > 0; size *= 2) {
            bytes_ostream buf;
            int8_t* ph;
            BOOST_TEST_MESSAGE(fmt::format("try size={}", size));
            ph = buf.write_place_holder(size);
            std::fill(ph, ph + size, 0);
        }
    } catch (const std::bad_alloc&) {
    }
    BOOST_REQUIRE(size >= bytes_ostream::max_chunk_size());
}

BOOST_AUTO_TEST_CASE(test_append_big_and_small_chunks) {
    bytes_ostream small;
    append_sequence(small, 12);

    bytes_ostream big;
    append_sequence(big, 513);

    bytes_ostream buf;
    buf.append(big);
    buf.append(small);
    buf.append(big);
    buf.append(small);
}

BOOST_AUTO_TEST_CASE(test_remove_suffix) {
    auto test = [] (size_t length, size_t suffix) {
        testlog.info("Testing buffer size {}  and suffix size {}", length, suffix);

        auto data = tests::random::get_bytes(length);
        bytes_view view = data;

        bytes_ostream bo;
        bo.write(data);

        bo.remove_suffix(suffix);
        view.remove_suffix(suffix);

        BOOST_REQUIRE(view == bytes_ostream(bo).linearize());
        for (bytes_view fragment : bo) {
            BOOST_REQUIRE_LE(fragment.size(), view.size());
            BOOST_REQUIRE(fragment == bytes_view(view.data(), fragment.size()));
            view.remove_prefix(fragment.size());
        }
        BOOST_REQUIRE_EQUAL(view.size(), 0);
    };

    test(0, 0);
    test(16, 0);
    test(1'000'000, 0);

    test(16, 16);
    test(1'000'000, 1'000'000);

    test(16, 1);
    test(16, 15);
    test(1'000'000, 1);
    test(1'000'000, 999'999);

    for (auto i = 0; i < 25; i++) {
        auto a = tests::random::get_int(128 * 1024);
        auto b = tests::random::get_int(128 * 1024);
        test(std::max(a, b), std::min(a, b));
    }
}
