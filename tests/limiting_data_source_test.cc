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

#include "utils/limiting_data_source.hh"

#include <boost/test/unit_test.hpp>
#include <seastar/tests/test-utils.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/thread.hh>
#include <cstdint>

using namespace seastar;

namespace {

class test_data_source_impl : public data_source_impl {
    char _cur = 0;
    void advance(uint64_t n) {
        _cur = (_cur + n) % (std::numeric_limits<char>::max() + 1);
    }
public:
    static const unsigned chunk_limit = 10;
    test_data_source_impl() { }

    test_data_source_impl(test_data_source_impl&&) noexcept = default;
    test_data_source_impl& operator=(test_data_source_impl&&) noexcept = default;

    virtual future<temporary_buffer<char>> get() override {
        temporary_buffer<char> res(chunk_limit);
        for (unsigned i = 0; i < chunk_limit; ++i) {
            res.get_write()[i] = _cur;
            advance(1);
        }
        return make_ready_future<temporary_buffer<char>>(std::move(res));
    }
    virtual future<temporary_buffer<char>> skip(uint64_t n) override {
        advance(n);
        return get();
    }
};

data_source create_test_data_source() {
    return data_source{std::make_unique<test_data_source_impl>()};
}

void test_get(unsigned limit) {
    auto src = create_test_data_source();
    auto tested = make_limiting_data_source(std::move(src), [limit] { return limit; });
    char expected = 0;
    auto test_get = [&] {
        auto buf = tested.get().get0();
        BOOST_REQUIRE(buf.size() <= limit);
        for (unsigned i = 0; i < buf.size(); ++i) {
            BOOST_REQUIRE_EQUAL(expected++, buf[i]);
        }
    };
    test_get();
    test_get();
    test_get();
}

data_source prepare_test_skip() {
    auto src = create_test_data_source();
    auto tested = make_limiting_data_source(std::move(src), [] { return 1; });
    auto buf = tested.get().get0();
    BOOST_REQUIRE_EQUAL(1, buf.size());
    BOOST_REQUIRE_EQUAL(0, buf[0]);
    // At this point we have 9 chars buffered in limiting_data_source_impl
    return std::move(tested);
}

}

SEASTAR_THREAD_TEST_CASE(test_get_smaller_than_limit) {
    assert(test_data_source_impl::chunk_limit > 1);
    test_get(test_data_source_impl::chunk_limit - 1);
}

SEASTAR_THREAD_TEST_CASE(test_get_equal_to_limit) {
    test_get(test_data_source_impl::chunk_limit);
}

SEASTAR_THREAD_TEST_CASE(test_get_smaller_bigger_than_limit) {
    test_get(test_data_source_impl::chunk_limit + 1);
}

SEASTAR_THREAD_TEST_CASE(test_skip_smaller_than_buffered_data) {
    auto tested = prepare_test_skip();
    auto buf = tested.skip(8).get0();
    BOOST_REQUIRE_EQUAL(1, buf.size());
    BOOST_REQUIRE_EQUAL(9, buf[0]);
}

SEASTAR_THREAD_TEST_CASE(test_skip_equal_to_buffered_data) {
    auto tested = prepare_test_skip();
    auto buf = tested.skip(9).get0();
    BOOST_REQUIRE_EQUAL(1, buf.size());
    BOOST_REQUIRE_EQUAL(10, buf[0]);
}

SEASTAR_THREAD_TEST_CASE(test_skip_bigger_than_buffered_data) {
    auto tested = prepare_test_skip();
    auto buf = tested.skip(10).get0();
    BOOST_REQUIRE_EQUAL(1, buf.size());
    BOOST_REQUIRE_EQUAL(11, buf[0]);
}