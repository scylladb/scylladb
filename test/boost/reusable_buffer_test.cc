/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "test/lib/random_utils.hh"
#include "test/lib/log.hh"

#include <boost/range/algorithm/copy.hpp>

#include "utils/assert.hh"
#include "utils/reusable_buffer.hh"
#include <seastar/core/manual_clock.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/later.hh>
#include <seastar/core/coroutine.hh>
#include <bit>

using namespace seastar;

SEASTAR_TEST_CASE(test_get_linearized_view) {
    auto test = [] (size_t n, utils::reusable_buffer<manual_clock>& buffer) {
        testlog.info("Testing buffer size {}", n);
        auto original = tests::random::get_bytes(n);

        bytes_ostream bo;
        bo.write(original);

        {
            auto bufguard = utils::reusable_buffer_guard(buffer);
            auto view = bufguard.get_linearized_view(bo);
            BOOST_REQUIRE_EQUAL(view.size(), n);
            BOOST_REQUIRE(view == original);
            BOOST_REQUIRE(bo.linearize() == original);
        }

        {
            std::vector<temporary_buffer<char>> tbufs;
            bytes_view left = original;
            while (!left.empty()) {
                auto this_size = std::min<size_t>(left.size(), fragmented_temporary_buffer::default_fragment_size);
                tbufs.emplace_back(reinterpret_cast<const char*>(left.data()), this_size);
                left.remove_prefix(this_size);
            }

            auto bufguard = utils::reusable_buffer_guard(buffer);
            auto fbuf = fragmented_temporary_buffer(std::move(tbufs), original.size());
            auto view = bufguard.get_linearized_view(fragmented_temporary_buffer::view(fbuf));
            BOOST_REQUIRE_EQUAL(view.size(), n);
            BOOST_REQUIRE(view == original);
            BOOST_REQUIRE(linearized(fragmented_temporary_buffer::view(fbuf)) == original);
        }
    };

    for (auto j = 0; j < 2; j++) {
        utils::reusable_buffer<manual_clock> buffer(std::chrono::milliseconds(1));

        test(0, buffer);
        test(1'000'000, buffer);
        test(1'000, buffer);
        test(100'000, buffer);

        for (auto i = 0; i < 25; i++) {
            test(tests::random::get_int(512 * 1024), buffer);
        }
    }

    return make_ready_future<>();
}

SEASTAR_TEST_CASE(test_make_buffer) {
    auto test = [] (size_t maximum, size_t actual, utils::reusable_buffer<manual_clock>& buffer) {
        testlog.info("Testing maximum buffer size {}, actual: {} ", maximum, actual);

        bytes original;
        auto make_buffer_fn = [&] (bytes_mutable_view view) {
            original = tests::random::get_bytes(actual);
            BOOST_REQUIRE_EQUAL(maximum, view.size());
            BOOST_REQUIRE_LE(actual, view.size());
            boost::range::copy(original, view.begin());
            return actual;
        };

        {
            auto bufguard = utils::reusable_buffer_guard(buffer);
            auto bo = bufguard.make_bytes_ostream(maximum, make_buffer_fn);

            BOOST_REQUIRE_EQUAL(bo.size(), actual);
            BOOST_REQUIRE(bo.linearize() == original);
        }

        {
            auto bufguard = utils::reusable_buffer_guard(buffer);
            auto fbuf = bufguard.make_fragmented_temporary_buffer(maximum, make_buffer_fn);
            auto view = fragmented_temporary_buffer::view(fbuf);

            BOOST_REQUIRE_EQUAL(view.size_bytes(), actual);
            BOOST_REQUIRE(linearized(view) == original);
        }
    };

    for (auto j = 0; j < 2; j++) {
        utils::reusable_buffer<manual_clock> buffer(std::chrono::milliseconds(1));

        test(0, 0, buffer);
        test(100'000, 0, buffer);
        test(200'000, 200'000, buffer);
        test(400'000, 100'000, buffer);

        for (auto i = 0; i < 25; i++) {
            auto a = tests::random::get_int(512 * 1024);
            auto b = tests::random::get_int(512 * 1024);
            test(std::max(a, b), std::min(a, b), buffer);
        }
    }

    return make_ready_future<>();
}

SEASTAR_TEST_CASE(test_decay) {
    using namespace std::chrono_literals;
    utils::reusable_buffer<manual_clock> buffer(1s);
    auto get_buffer = [&buffer] (size_t size) {
        auto bufguard = utils::reusable_buffer_guard(buffer);
        bufguard.get_temporary_buffer(size);
    };
    auto advance_clock = [] (manual_clock::duration d) {
        manual_clock::advance(d);
        return yield();
    };
    BOOST_REQUIRE(buffer.reallocs() == 0);
    get_buffer(1'000'000);
    get_buffer(1'000'001);
    get_buffer(1'000'000);
    get_buffer(1'000);
    BOOST_REQUIRE_EQUAL(buffer.reallocs(), 1);
    // It isn't strictly required from the implementation to use
    // power-of-2 sizes, just sizes coarse enough to limit the number
    // of allocations.
    // If the implementation is modified, this SCYLLA_ASSERT can be freely changed.
    BOOST_REQUIRE_EQUAL(buffer.size(), std::bit_ceil(size_t(1'000'001)));
    co_await advance_clock(1500ms);
    get_buffer(1'000);
    BOOST_REQUIRE_EQUAL(buffer.reallocs(), 1);
    co_await advance_clock(1000ms);
    BOOST_REQUIRE_EQUAL(buffer.reallocs(), 2);
    BOOST_REQUIRE_EQUAL(buffer.size(), std::bit_ceil(size_t(1'000)));
    co_await advance_clock(1000ms);
    BOOST_REQUIRE_EQUAL(buffer.reallocs(), 3);
    BOOST_REQUIRE_EQUAL(buffer.size(), 0);
}
