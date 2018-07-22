/*
 * Copyright (C) 2018 ScyllaDB
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

#define BOOST_TEST_MODULE reusable_buffer
#include <boost/test/unit_test.hpp>

#include "utils/reusable_buffer.hh"

#include <boost/range/algorithm/copy.hpp>

#include "random-utils.hh"

BOOST_AUTO_TEST_CASE(test_get_linearized_view) {
    utils::reusable_buffer buffer;

    auto test = [&buffer] (size_t n) {
        BOOST_TEST_MESSAGE("Testing buffer size " << n);
        auto original = tests::random::get_bytes(n);

        bytes_ostream bo;
        bo.write(original);

        auto view = buffer.get_linearized_view(bo);
        BOOST_REQUIRE_EQUAL(view.size(), n);
        BOOST_REQUIRE(view == original);
        BOOST_REQUIRE(bo.linearize() == original);

        std::vector<temporary_buffer<char>> tbufs;
        bytes_view left = original;
        while (!left.empty()) {
            auto this_size = std::min<size_t>(left.size(), fragmented_temporary_buffer::default_fragment_size);
            tbufs.emplace_back(reinterpret_cast<const char*>(left.data()), this_size);
            left.remove_prefix(this_size);
        }

        auto fbuf = fragmented_temporary_buffer(std::move(tbufs), original.size());
        view = buffer.get_linearized_view(fragmented_temporary_buffer::view(fbuf));
        BOOST_REQUIRE_EQUAL(view.size(), n);
        BOOST_REQUIRE(view == original);
        BOOST_REQUIRE(linearized(fragmented_temporary_buffer::view(fbuf)) == original);
    };

    for (auto j = 0; j < 2; j++) {
        buffer.clear();

        test(0);
        test(1'000'000);
        test(1'000);
        test(100'000);

        for (auto i = 0; i < 25; i++) {
            test(tests::random::get_int(512 * 1024));
        }
    }
}

BOOST_AUTO_TEST_CASE(test_make_buffer) {
    utils::reusable_buffer buffer;

    auto test = [&buffer] (size_t maximum, size_t actual) {
        BOOST_TEST_MESSAGE("Testing maximum buffer size " << maximum << ", actual: " << actual);
        
        bytes original;
        auto make_buffer_fn = [&] (bytes_mutable_view view) {
            original = tests::random::get_bytes(actual);
            BOOST_REQUIRE_EQUAL(maximum, view.size());
            BOOST_REQUIRE_LE(actual, view.size());
            boost::range::copy(original, view.begin());
            return actual;
        };

        auto bo = buffer.make_buffer(maximum, make_buffer_fn);

        BOOST_REQUIRE_EQUAL(bo.size(), actual);
        BOOST_REQUIRE(bo.linearize() == original);

        auto fbuf = buffer.make_fragmented_temporary_buffer(maximum, fragmented_temporary_buffer::default_fragment_size, make_buffer_fn);
        auto view = fragmented_temporary_buffer::view(fbuf);

        BOOST_REQUIRE_EQUAL(view.size_bytes(), actual);
        BOOST_REQUIRE(linearized(view) == original);
    };

    for (auto j = 0; j < 2; j++) {
        buffer.clear();

        test(0, 0);
        test(100'000, 0);
        test(200'000, 200'000);
        test(400'000, 100'000);

        for (auto i = 0; i < 25; i++) {
            auto a = tests::random::get_int(512 * 1024);
            auto b = tests::random::get_int(512 * 1024);
            test(std::max(a, b), std::min(a, b));
        }
    }
}
