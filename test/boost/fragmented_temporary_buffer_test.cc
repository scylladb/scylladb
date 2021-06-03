/*
 * Copyright (C) 2018-present ScyllaDB
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

#include <seastar/core/thread.hh>
#include <seastar/testing/thread_test_case.hh>

#include "utils/fragmented_temporary_buffer.hh"

#include "test/lib/random_utils.hh"

struct {
    [[noreturn]]
    static void throw_out_of_range(size_t a, size_t b) {
        throw size_t(a + b);
    }
} int_thrower;

std::tuple<std::vector<fragmented_temporary_buffer>, uint64_t, uint16_t> get_buffers()
{
    uint64_t value1 = 0x1234'5678'abcd'ef02ull;
    uint16_t value2 = 0xfedc;

    auto data = bytes(bytes::initialized_later(), sizeof(value1) + sizeof(value2));
    auto dst = std::copy_n(reinterpret_cast<const int8_t*>(&value1), sizeof(value1), data.begin());
    std::copy_n(reinterpret_cast<const int8_t*>(&value2), sizeof(value2), dst);

    std::vector<fragmented_temporary_buffer> buffers;

    // Everything in a single buffer
    {
        std::vector<temporary_buffer<char>> fragments;
        fragments.emplace_back(reinterpret_cast<char*>(data.data()), data.size());
        buffers.emplace_back(
            std::move(fragments),
            data.size()
        );
    }

    // One-byte buffers
    {
        std::vector<temporary_buffer<char>> fragments;
        for (auto i = 0u; i < data.size(); i++) {
            fragments.emplace_back(reinterpret_cast<char*>(data.data() + i), 1);
        }
        buffers.emplace_back(
            std::move(fragments),
            data.size()
        );
    }

    // Seven bytes and the rest
    {
        std::vector<temporary_buffer<char>> fragments;
        fragments.emplace_back(reinterpret_cast<char*>(data.data()), 7);
        fragments.emplace_back(reinterpret_cast<char*>(data.data() + 7), data.size() - 7);
        buffers.emplace_back(
            std::move(fragments),
            data.size()
        );
    }

    // 8 bytes and 2 bytes
    {
        std::vector<temporary_buffer<char>> fragments;
        fragments.emplace_back(reinterpret_cast<char*>(data.data()), sizeof(uint64_t));
        fragments.emplace_back(reinterpret_cast<char*>(data.data() + sizeof(uint64_t)), data.size() - sizeof(uint64_t));
        buffers.emplace_back(
            std::move(fragments),
            data.size()
        );
    }

    return { std::move(buffers), value1, value2 };
}

SEASTAR_THREAD_TEST_CASE(test_view) {
    auto [ buffers, value1, value2 ] = get_buffers();

    auto data = bytes(bytes::initialized_later(), sizeof(value1) + sizeof(value2));
    auto data_view = bytes_view(data);
    auto dst = std::copy_n(reinterpret_cast<const int8_t*>(&value1), sizeof(value1), data.begin());
    std::copy_n(reinterpret_cast<const int8_t*>(&value2), sizeof(value2), dst);

    auto test = [&] (fragmented_temporary_buffer::view view) {
        BOOST_CHECK_EQUAL(view.size_bytes(), data_view.size());
        BOOST_CHECK_EQUAL(view.empty(), data_view.empty());

        BOOST_CHECK_EQUAL(linearized(view), data_view);

        bool called = false;
        with_linearized(view, [&] (bytes_view value) {
            BOOST_CHECK_EQUAL(value, data_view);
            called = true;
        });
        BOOST_CHECK(called);

        auto data_it = data_view.begin();
        for (auto&& frag : view) {
            BOOST_CHECK_LE(frag.size(), data_view.end() - data_it);
            BOOST_CHECK(std::equal(frag.begin(), frag.end(), data_it));
            data_it += frag.size();
        }
        BOOST_CHECK(data_it == data_view.end());
    };

    for (auto& frag_buffer : buffers) {
        auto frag_view = fragmented_temporary_buffer::view(frag_buffer);
        test(frag_view);

        frag_view.remove_prefix(sizeof(value1) - 1);
        data_view.remove_prefix(sizeof(value1) - 1);
        test(frag_view);

        frag_view.remove_prefix(data_view.size());
        data_view.remove_prefix(data_view.size());
        test(frag_view);

        data_view = bytes_view(data);
        frag_view = fragmented_temporary_buffer::view(frag_buffer);

        frag_view.remove_suffix(sizeof(value2) - 1);
        data_view.remove_suffix(sizeof(value2) - 1);
        test(frag_view);

        frag_view.remove_suffix(data_view.size());
        data_view.remove_suffix(data_view.size());
        test(frag_view);

        data_view = bytes_view(data);
        frag_view = fragmented_temporary_buffer::view(frag_buffer);

        frag_view.remove_suffix(sizeof(value2) - 1);
        data_view.remove_suffix(sizeof(value2) - 1);
        test(frag_view);

        frag_view.remove_prefix(data_view.size());
        data_view.remove_prefix(data_view.size());
        test(frag_view);

        data_view = bytes_view(data);
        frag_view = fragmented_temporary_buffer::view(frag_buffer);

        frag_view.remove_prefix(sizeof(value2) - 1);
        data_view.remove_prefix(sizeof(value2) - 1);
        test(frag_view);

        frag_view.remove_suffix(data_view.size());
        data_view.remove_suffix(data_view.size());
        test(frag_view);

        data_view = bytes_view(data);
    }

    for (auto& frag_buffer : buffers) {
        test(fragmented_temporary_buffer::view(frag_buffer));

        frag_buffer.remove_prefix(sizeof(value1) - 1);
        data_view.remove_prefix(sizeof(value1) - 1);
        test(fragmented_temporary_buffer::view(frag_buffer));

        frag_buffer.remove_prefix(data_view.size());
        data_view.remove_prefix(data_view.size());
        test(fragmented_temporary_buffer::view(frag_buffer));

        data_view = bytes_view(data);
    }

    auto empty = fragmented_temporary_buffer();
    data_view = bytes_view();
    auto frag_view = fragmented_temporary_buffer::view(empty);
    test(frag_view);
    frag_view.remove_prefix(0);
    test(frag_view);
}

SEASTAR_THREAD_TEST_CASE(test_view_equality) {
    auto buffers = std::get<0>(get_buffers());

    for (auto& a : buffers) {
        for (auto& b : buffers) {
            auto av = fragmented_temporary_buffer::view(a);
            auto bv = fragmented_temporary_buffer::view(b);
            BOOST_CHECK_EQUAL(av.size_bytes(), bv.size_bytes());
            BOOST_CHECK(av == bv);
        }
    }

    auto empty = fragmented_temporary_buffer();
    auto empty_view = fragmented_temporary_buffer::view(empty);
    BOOST_CHECK(empty_view.empty());
    BOOST_CHECK(empty_view == empty_view);

    for (auto& buf : buffers) {
        BOOST_CHECK(empty_view != fragmented_temporary_buffer::view(buf));
        BOOST_CHECK(fragmented_temporary_buffer::view(buf) != empty_view);
    }
}

SEASTAR_THREAD_TEST_CASE(test_empty_istream) {
    auto fbuf = fragmented_temporary_buffer();
    auto in = fbuf.get_istream();

    auto linearization_buffer = bytes_ostream();
    BOOST_CHECK_EQUAL(in.bytes_left(), 0);
    BOOST_CHECK_THROW(in.read<char>(), std::out_of_range);
    BOOST_CHECK_THROW(in.read_view(1), std::out_of_range);
    BOOST_CHECK_THROW(in.read_bytes_view(1, linearization_buffer), std::out_of_range);
    BOOST_CHECK_EQUAL(in.read_bytes_view(0, linearization_buffer), bytes_view());
    BOOST_CHECK(linearization_buffer.empty());
}

SEASTAR_THREAD_TEST_CASE(test_read_pod) {
    auto test = [&] (auto expected_value1, auto expected_value2, fragmented_temporary_buffer& ftb) {
        using type1 = std::decay_t<decltype(expected_value1)>;
        using type2 = std::decay_t<decltype(expected_value2)>;
        static_assert(sizeof(type2) < sizeof(type1));

        auto in = ftb.get_istream();
        BOOST_CHECK_EQUAL(in.bytes_left(), sizeof(type1) + sizeof(type2));
        BOOST_CHECK_EQUAL(in.read<type1>(), expected_value1);
        BOOST_CHECK_EQUAL(in.bytes_left(), sizeof(type2));
        BOOST_CHECK_THROW(in.read<type1>(), std::out_of_range);
        BOOST_CHECK_EQUAL(in.bytes_left(), sizeof(type2));
        BOOST_CHECK_EQUAL(in.read<type2>(), expected_value2);
        BOOST_CHECK_EQUAL(in.bytes_left(), 0);
        BOOST_CHECK_THROW(in.read<type2>(), std::out_of_range);
        BOOST_CHECK_EQUAL(in.bytes_left(), 0);
        BOOST_CHECK_THROW(in.read<type1>(), std::out_of_range);
        BOOST_CHECK_EQUAL(in.bytes_left(), 0);
        BOOST_CHECK_THROW(in.read<char>(), std::out_of_range);
        BOOST_CHECK_EXCEPTION(in.read<char>(int_thrower), size_t, [] (size_t v) { return v == 1; });
        BOOST_CHECK_EQUAL(in.bytes_left(), 0);
    };

    auto [ buffers, value1, value2 ] = get_buffers();
    for (auto& frag_buffer : buffers) {
        test(value1, value2, frag_buffer);
    }
}

SEASTAR_THREAD_TEST_CASE(test_read_to) {
    auto test = [&] (bytes_view expected_value1, bytes_view expected_value2, fragmented_temporary_buffer& ftb) {
        assert(expected_value2.size() < expected_value1.size());

        bytes actual_value;

        auto in = ftb.get_istream();
        BOOST_CHECK_EQUAL(in.bytes_left(), expected_value1.size() + expected_value2.size());
        actual_value = bytes(bytes::initialized_later(), expected_value1.size());
        in.read_to(expected_value1.size(), actual_value.begin());
        BOOST_CHECK_EQUAL(actual_value, expected_value1);
        BOOST_CHECK_EQUAL(in.bytes_left(), expected_value2.size());
        BOOST_CHECK_THROW(in.read_to(expected_value1.size(), actual_value.begin()), std::out_of_range);
        BOOST_CHECK_EQUAL(in.bytes_left(), expected_value2.size());
        actual_value = bytes(bytes::initialized_later(), expected_value2.size());
        in.read_to(expected_value2.size(), actual_value.begin());
        BOOST_CHECK_EQUAL(actual_value, expected_value2);
        BOOST_CHECK_EQUAL(in.bytes_left(), 0);
        BOOST_CHECK_THROW(in.read_to(expected_value2.size(), actual_value.begin()), std::out_of_range);
        BOOST_CHECK_EQUAL(in.bytes_left(), 0);
        BOOST_CHECK_THROW(in.read_to(expected_value1.size(), actual_value.begin()), std::out_of_range);
        BOOST_CHECK_EQUAL(in.bytes_left(), 0);
        BOOST_CHECK_THROW(in.read_to(1, actual_value.begin()), std::out_of_range);
        BOOST_CHECK_EXCEPTION(in.read_to(1, actual_value.begin(), int_thrower), size_t, [] (size_t v) { return v == 1; });
        BOOST_CHECK_EQUAL(in.bytes_left(), 0);
    };


    auto [ buffers, value1, value2 ] = get_buffers();
    for (auto& frag_buffer : buffers) {
        auto value1_bv = bytes_view(reinterpret_cast<const bytes::value_type*>(&value1), sizeof(value1));
        auto value2_bv = bytes_view(reinterpret_cast<const bytes::value_type*>(&value2), sizeof(value2));
        test(value1_bv, value2_bv, frag_buffer);
    }
}

SEASTAR_THREAD_TEST_CASE(test_read_view) {
    auto test = [&] (bytes_view expected_value1, bytes_view expected_value2, fragmented_temporary_buffer& ftb) {
        assert(expected_value2.size() < expected_value1.size());

        auto in = ftb.get_istream();
        BOOST_CHECK_EQUAL(in.bytes_left(), expected_value1.size() + expected_value2.size());
        BOOST_CHECK_EQUAL(linearized(in.read_view(expected_value1.size())), expected_value1);
        BOOST_CHECK_EQUAL(in.bytes_left(), expected_value2.size());
        BOOST_CHECK_THROW(in.read_view(expected_value1.size()), std::out_of_range);
        BOOST_CHECK_EQUAL(in.bytes_left(), expected_value2.size());
        BOOST_CHECK_EQUAL(linearized(in.read_view(expected_value2.size())), expected_value2);
        BOOST_CHECK_EQUAL(in.bytes_left(), 0);
        BOOST_CHECK_THROW(in.read_view(expected_value2.size()), std::out_of_range);
        BOOST_CHECK_EQUAL(in.bytes_left(), 0);
        BOOST_CHECK_THROW(in.read_view(expected_value1.size()), std::out_of_range);
        BOOST_CHECK_EQUAL(in.bytes_left(), 0);
        BOOST_CHECK_THROW(in.read_view(1), std::out_of_range);
        BOOST_CHECK_EXCEPTION(in.read_view(1, int_thrower), size_t, [] (size_t v) { return v == 1; });
        BOOST_CHECK_EQUAL(in.bytes_left(), 0);
    };


    auto [ buffers, value1, value2 ] = get_buffers();
    for (auto& frag_buffer : buffers) {
        auto value1_bv = bytes_view(reinterpret_cast<const bytes::value_type*>(&value1), sizeof(value1));
        auto value2_bv = bytes_view(reinterpret_cast<const bytes::value_type*>(&value2), sizeof(value2));
        test(value1_bv, value2_bv, frag_buffer);
    }
}

SEASTAR_THREAD_TEST_CASE(test_read_bytes_view) {
    auto linearization_buffer = bytes_ostream();
    auto test = [&] (bytes_view expected_value1, bytes_view expected_value2, fragmented_temporary_buffer& ftb) {
        assert(expected_value2.size() < expected_value1.size());

        auto in = ftb.get_istream();
        BOOST_CHECK_EQUAL(in.read_bytes_view(0, linearization_buffer), bytes_view());
        BOOST_CHECK_EQUAL(in.bytes_left(), expected_value1.size() + expected_value2.size());
        BOOST_CHECK_EQUAL(in.read_bytes_view(expected_value1.size(), linearization_buffer), expected_value1);
        BOOST_CHECK_EQUAL(in.bytes_left(), expected_value2.size());
        BOOST_CHECK_THROW(in.read_bytes_view(expected_value1.size(), linearization_buffer), std::out_of_range);
        BOOST_CHECK_EQUAL(in.bytes_left(), expected_value2.size());
        BOOST_CHECK_EQUAL(in.read_bytes_view(expected_value2.size(), linearization_buffer), expected_value2);
        BOOST_CHECK_EQUAL(in.bytes_left(), 0);
        BOOST_CHECK_THROW(in.read_bytes_view(expected_value2.size(), linearization_buffer), std::out_of_range);
        BOOST_CHECK_EQUAL(in.bytes_left(), 0);
        BOOST_CHECK_THROW(in.read_bytes_view(expected_value1.size(), linearization_buffer), std::out_of_range);
        BOOST_CHECK_EQUAL(in.bytes_left(), 0);
        BOOST_CHECK_THROW(in.read_bytes_view(1, linearization_buffer), std::out_of_range);
        BOOST_CHECK_EXCEPTION(in.read_bytes_view(1, linearization_buffer, int_thrower), size_t, [] (size_t v) { return v == 1; });
        BOOST_CHECK_EQUAL(in.bytes_left(), 0);
        BOOST_CHECK_EQUAL(in.read_bytes_view(0, linearization_buffer), bytes_view());
    };


    auto [ buffers, value1, value2 ] = get_buffers();
    for (auto& frag_buffer : buffers) {
        auto value1_bv = bytes_view(reinterpret_cast<const bytes::value_type*>(&value1), sizeof(value1));
        auto value2_bv = bytes_view(reinterpret_cast<const bytes::value_type*>(&value2), sizeof(value2));
        test(value1_bv, value2_bv, frag_buffer);
    }
}

namespace {

class memory_data_source final : public data_source_impl {
private:
    using vector_type = std::vector<temporary_buffer<char>>;
    vector_type _buffers;
    vector_type::iterator _position;
public:
    explicit memory_data_source(std::vector<temporary_buffer<char>> buffers)
        : _buffers(std::move(buffers))
        , _position(_buffers.begin())
    { }

    virtual future<temporary_buffer<char>> get() override {
        if (_position == _buffers.end()) {
            return make_ready_future<temporary_buffer<char>>();
        }
        return make_ready_future<temporary_buffer<char>>(std::move(*_position++));
    }
};

}

SEASTAR_THREAD_TEST_CASE(test_read_fragmented_buffer) {
    using tuple_type = std::tuple<std::vector<temporary_buffer<char>>,
                                  bytes,
                                  bytes,
                                  bytes>;

    auto generate = [] (size_t n) {
        auto prefix = tests::random::get_bytes();
        auto data = tests::random::get_bytes(n);
        auto suffix = tests::random::get_bytes();

        auto linear = bytes(bytes::initialized_later(), prefix.size() + n + suffix.size());
        auto dst = linear.begin();
        dst = std::copy(prefix.begin(), prefix.end(), dst);
        dst = std::copy(data.begin(), data.end(), dst);
        std::copy(suffix.begin(), suffix.end(), dst);

        auto src = linear.begin();
        auto left = linear.size();

        std::vector<temporary_buffer<char>> buffers;
        while (left) {
            auto this_size = std::max<size_t>(left / 2, 1);
            buffers.emplace_back(reinterpret_cast<const char*>(src), this_size);
            left -= this_size;
            src += this_size;
        }

        return tuple_type { std::move(buffers), std::move(prefix),
                            std::move(data), std::move(suffix) };
    };

    auto test_cases = std::vector<tuple_type>();

    test_cases.emplace_back();
    test_cases.emplace_back(generate(0));
    test_cases.emplace_back(generate(1024));
    test_cases.emplace_back(generate(512 * 1024));
    for (auto i = 0; i < 16; i++) {
        test_cases.emplace_back(generate(tests::random::get_int(16, 16 * 1024)));
    }

    for (auto&& [ buffers, expected_prefix, expected_data, expected_suffix ] : test_cases) {
        auto prefix_size = expected_prefix.size();
        auto size = expected_data.size();
        auto suffix_size = expected_suffix.size();

        auto in = input_stream<char>(data_source(std::make_unique<memory_data_source>(std::move(buffers))));

        auto prefix = in.read_exactly(prefix_size).get0();
        BOOST_CHECK_EQUAL(prefix.size(), prefix_size);
        BOOST_CHECK_EQUAL(bytes_view(reinterpret_cast<const bytes::value_type*>(prefix.get()), prefix.size()),
                          expected_prefix);

        auto reader = fragmented_temporary_buffer::reader();
        auto fbuf = reader.read_exactly(in, size).get0();
        auto view = fragmented_temporary_buffer::view(fbuf);
        BOOST_CHECK_EQUAL(view.size_bytes(), size);
        BOOST_CHECK_EQUAL(linearized(view), expected_data);

        auto suffix = in.read_exactly(suffix_size).get0();
        BOOST_CHECK_EQUAL(suffix.size(), suffix_size);
        BOOST_CHECK_EQUAL(bytes_view(reinterpret_cast<const bytes::value_type*>(suffix.get()), suffix.size()),
                          expected_suffix);

        in.close().get();
    }
}

SEASTAR_THREAD_TEST_CASE(test_skip) {
    auto test = [&] (auto expected_value1, auto expected_value2, fragmented_temporary_buffer& ftb) {
        using type1 = std::decay_t<decltype(expected_value1)>;
        using type2 = std::decay_t<decltype(expected_value2)>;

        auto in = ftb.get_istream();
        BOOST_CHECK_EQUAL(in.bytes_left(), sizeof(type1) + sizeof(type2));
        in.skip(sizeof(type1));
        BOOST_CHECK_EQUAL(in.bytes_left(), sizeof(type2));
        BOOST_CHECK_EQUAL(in.read<type2>(), expected_value2);
        BOOST_CHECK_EQUAL(in.bytes_left(), 0);
        in.skip(sizeof(type2));
        BOOST_CHECK_EQUAL(in.bytes_left(), 0);
    };

    auto [ buffers, value1, value2 ] = get_buffers();
    for (auto& frag_buffer : buffers) {
        test(value1, value2, frag_buffer);
    }
}

SEASTAR_THREAD_TEST_CASE(test_remove_suffix) {
    auto test = [&] (auto expected_value1, auto expected_value2, fragmented_temporary_buffer& ftb) {
        using type1 = std::decay_t<decltype(expected_value1)>;
        using type2 = std::decay_t<decltype(expected_value2)>;

        BOOST_CHECK_EQUAL(ftb.size_bytes(), sizeof(type1) + sizeof(type2));
        ftb.remove_suffix(sizeof(type2));
        BOOST_CHECK_EQUAL(ftb.size_bytes(), sizeof(type1));

        auto in = ftb.get_istream();
        BOOST_CHECK_EQUAL(in.read<type1>(), expected_value1);
        BOOST_CHECK_EQUAL(in.bytes_left(), 0);
        BOOST_CHECK_THROW(in.read<char>(), std::out_of_range);

        ftb.remove_suffix(sizeof(type1) - 1);
        BOOST_CHECK_EQUAL(ftb.size_bytes(), 1);

        auto v = fragmented_temporary_buffer::view(ftb);
        v.remove_prefix(1);
        BOOST_CHECK(v.empty());

        ftb.remove_suffix(1);
        BOOST_CHECK_EQUAL(ftb.size_bytes(), 0);
    };

    auto [ buffers, value1, value2 ] = get_buffers();
    for (auto& frag_buffer : buffers) {
        test(value1, value2, frag_buffer);
    }
}

static void do_test_read_exactly_eof(size_t input_size) {
    std::vector<temporary_buffer<char>> data;
    if (input_size) {
        data.push_back(temporary_buffer<char>(input_size));
    }
    auto ds = data_source(std::make_unique<memory_data_source>(std::move(data)));
    auto is = input_stream<char>(std::move(ds));
    auto reader = fragmented_temporary_buffer::reader();
    auto result = reader.read_exactly(is, input_size + 1).get0();
    BOOST_CHECK_EQUAL(result.size_bytes(), size_t(0));
}

SEASTAR_THREAD_TEST_CASE(test_read_exactly_eof) {
    do_test_read_exactly_eof(0);
    do_test_read_exactly_eof(1);
}
