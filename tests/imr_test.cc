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

#define BOOST_TEST_MODULE imr
#include <boost/test/unit_test.hpp>

#include <algorithm>
#include <random>

#include <boost/range/irange.hpp>
#include <boost/range/algorithm/copy.hpp>
#include <boost/range/algorithm/generate.hpp>

#include <seastar/util/variant_utils.hh>

#include "imr/fundamental.hh"
#include "imr/compound.hh"

#include "random-utils.hh"

static constexpr auto random_test_iteration_count = 20;

class A;
class B;
class C;
class D;

BOOST_AUTO_TEST_SUITE(fundamental);

template<typename FillAB, typename FillBC>
struct generate_flags_type;

template<size_t... IdxAB, size_t... IdxBC>
struct generate_flags_type<std::index_sequence<IdxAB...>, std::index_sequence<IdxBC...>> {
    using type = imr::flags<A, std::integral_constant<size_t, IdxAB>...,
                            B, std::integral_constant<ssize_t, IdxBC>..., C>;
};

BOOST_AUTO_TEST_CASE(test_flags) {
    using flags_type = generate_flags_type<std::make_index_sequence<7>, std::make_index_sequence<8>>::type;
    static constexpr size_t expected_size = 3;

    BOOST_CHECK_EQUAL(flags_type::size_when_serialized(), expected_size);
    BOOST_CHECK_EQUAL(flags_type::size_when_serialized(imr::set_flag<A>(),
                                                       imr::set_flag<B>(),
                                                       imr::set_flag<C>()), expected_size);

    uint8_t buffer[expected_size];
    std::fill_n(buffer, expected_size, 0xbe);
    BOOST_CHECK_EQUAL(flags_type::serialize(buffer, imr::set_flag<B>()), expected_size);

    auto mview = flags_type::make_view(buffer);
    BOOST_CHECK(!mview.get<A>());
    BOOST_CHECK(mview.get<B>());
    BOOST_CHECK(!mview.get<C>());

    mview.set<A>();
    mview.set<B>(false);
    BOOST_CHECK(mview.get<A>());
    BOOST_CHECK(!mview.get<B>());
    BOOST_CHECK(!mview.get<C>());

    flags_type::view view = mview;
    mview.set<C>();
    BOOST_CHECK(view.get<A>());
    BOOST_CHECK(!view.get<B>());
    BOOST_CHECK(view.get<C>());

    BOOST_CHECK_EQUAL(flags_type::serialized_object_size(buffer), expected_size);

    int some_context;
    BOOST_CHECK_EQUAL(flags_type::serialized_object_size(buffer, some_context), expected_size);

    std::fill_n(buffer, expected_size, 0xff);
    BOOST_CHECK_EQUAL(flags_type::serialize(buffer), expected_size);
    BOOST_CHECK(!mview.get<A>());
    BOOST_CHECK(!mview.get<B>());
    BOOST_CHECK(!mview.get<C>());
}

struct test_pod_type {
    int32_t x;
    uint64_t y;

    friend bool operator==(const test_pod_type& a, const test_pod_type& b) {
        return a.x == b.x && a.y == b.y;
    }
    friend std::ostream& operator<<(std::ostream& os, const test_pod_type& obj) {
        return os << "test_pod_type { x: " << obj.x << ", y: " << obj.y << " }";
    }
};

BOOST_AUTO_TEST_CASE(test_pod) {
    auto generate_object = [] {
        std::uniform_int_distribution<decltype(test_pod_type::x)> dist_x;
        std::uniform_int_distribution<decltype(test_pod_type::y)> dist_y;
        return test_pod_type { dist_x(tests::random::gen), dist_y(tests::random::gen) };
    };
    using pod_type = imr::pod<test_pod_type>;

    uint8_t buffer[pod_type::size];
    for (auto i = 0; i < random_test_iteration_count; i++) {
        auto obj = generate_object();

        BOOST_CHECK_EQUAL(pod_type::size_when_serialized(obj), pod_type::size);
        BOOST_CHECK_EQUAL(pod_type::serialize(buffer, obj), pod_type::size);


        BOOST_CHECK_EQUAL(pod_type::serialized_object_size(buffer), pod_type::size);
        int some_context;
        BOOST_CHECK_EQUAL(pod_type::serialized_object_size(buffer, some_context), pod_type::size);

        auto mview = pod_type::make_view(buffer);
        pod_type::view view = mview;

        BOOST_CHECK_EQUAL(mview.load(), obj);
        BOOST_CHECK_EQUAL(view.load(), obj);

        auto obj2 = generate_object();
        mview.store(obj2);

        BOOST_CHECK_EQUAL(mview.load(), obj2);
        BOOST_CHECK_EQUAL(view.load(), obj2);
    }
}

class test_buffer_context {
    size_t _size;
public:
    explicit test_buffer_context(size_t sz) : _size(sz) { }

    template<typename Tag>
    size_t size_of() const noexcept;
};

template<>
size_t test_buffer_context::size_of<A>() const noexcept {
    return _size;
}

BOOST_AUTO_TEST_CASE(test_buffer) {
    using buffer_type = imr::buffer<A>;

    auto test = [] (auto serialize) {
        auto data = tests::random::get_bytes();
        auto size = data.size();

        auto buffer = std::make_unique<uint8_t[]>(size);

        serialize(buffer.get(), size, data);

        const auto ctx = test_buffer_context(size);
        BOOST_CHECK_EQUAL(buffer_type::serialized_object_size(buffer.get(), ctx), size);

        BOOST_CHECK(boost::range::equal(buffer_type::make_view(buffer.get(), ctx), data));
        BOOST_CHECK(boost::range::equal(buffer_type::make_view(const_cast<const uint8_t*>(buffer.get()), ctx), data));

        BOOST_CHECK_EQUAL(buffer_type::make_view(buffer.get(), ctx).size(), size);
    };

    for (auto i = 0; i < random_test_iteration_count; i++) {
        test([] (uint8_t* out, size_t size, const bytes& data) {
            BOOST_CHECK_EQUAL(buffer_type::size_when_serialized(data), size);
            BOOST_CHECK_EQUAL(buffer_type::serialize(out, data), size);
        });

        test([] (uint8_t* out, size_t size, const bytes& data) {
            auto serializer = [&data] (uint8_t* out) noexcept {
                boost::range::copy(data, out);
            };
            BOOST_CHECK_EQUAL(buffer_type::size_when_serialized(size, serializer), size);
            BOOST_CHECK_EQUAL(buffer_type::serialize(out, size, serializer), size);
        });
    }
}

BOOST_AUTO_TEST_SUITE_END();

BOOST_AUTO_TEST_SUITE(compound);

struct test_optional_context {
    template<typename Tag>
    bool is_present() const noexcept;

    template<typename Tag, typename... Args>
    decltype(auto) context_for(Args&&...) const noexcept { return *this; }
};
template<>
bool test_optional_context::is_present<A>() const noexcept {
    return true;
}
template<>
bool test_optional_context::is_present<B>() const noexcept {
    return false;
}

BOOST_AUTO_TEST_CASE(test_optional) {
    using optional_type1 = imr::optional<A, imr::pod<uint32_t>>;
    using optional_type2 = imr::optional<B, imr::pod<uint32_t>>;

    for (auto i = 0; i < random_test_iteration_count; i++) {
        auto value = tests::random::get_int<uint32_t>();
        auto expected_size = imr::pod<uint32_t>::size_when_serialized(value);

        auto buffer = std::make_unique<uint8_t[]>(expected_size);

        BOOST_CHECK_EQUAL(optional_type1::size_when_serialized(value), expected_size);
        BOOST_CHECK_EQUAL(optional_type1::serialize(buffer.get(), value), expected_size);

        BOOST_CHECK_EQUAL(optional_type1::serialized_object_size(buffer.get(), test_optional_context()), expected_size);
        BOOST_CHECK_EQUAL(optional_type2::serialized_object_size(buffer.get(), test_optional_context()), 0);

        auto view = optional_type1::make_view(buffer.get());
        BOOST_CHECK_EQUAL(view.get().load(), value);
    }
}


static constexpr auto data_size = 128;
using variant_type = imr::variant<A,
                                  imr::member<B, imr::pod<uint64_t>>,
                                  imr::member<C, imr::buffer<C>>,
                                  imr::member<D, imr::pod<int64_t>>>;

struct test_variant_context {
    unsigned _alternative_idx;
public:
    template<typename Tag>
    size_t size_of() const noexcept;

    template<typename Tag>
    auto active_alternative_of() const noexcept;

    template<typename Tag, typename... Args>
    decltype(auto) context_for(Args&&...) const noexcept { return *this; }
};

template<>
size_t test_variant_context::size_of<C>() const noexcept {
    return data_size;
}

template<>
auto test_variant_context::active_alternative_of<A>() const noexcept {
    switch (_alternative_idx) {
    case 0:
        return variant_type::index_for<B>();
    case 1:
        return variant_type::index_for<C>();
    case 2:
        return variant_type::index_for<D>();
    default:
        BOOST_FAIL("should not reach");
        abort();
    }
}

BOOST_AUTO_TEST_CASE(test_variant) {
    for (auto i = 0; i < random_test_iteration_count; i++) {
        unsigned alternative_idx = tests::random::get_int<unsigned>(2);

        uint64_t uinteger = tests::random::get_int<uint64_t>();
        int64_t integer = tests::random::get_int<int64_t>();
        bytes data = tests::random::get_bytes(data_size);

        const size_t expected_size = alternative_idx == 0
                                     ? imr::pod<uint64_t>::size_when_serialized(uinteger)
                                     : (alternative_idx == 1 ? data_size : sizeof(int64_t));

        auto buffer = std::make_unique<uint8_t[]>(expected_size);

        if (!alternative_idx) {
            BOOST_CHECK_EQUAL(variant_type::size_when_serialized<B>(uinteger), expected_size);
            BOOST_CHECK_EQUAL(variant_type::serialize<B>(buffer.get(), uinteger), expected_size);
        } else if (alternative_idx == 1) {
            BOOST_CHECK_EQUAL(variant_type::size_when_serialized<C>(data), expected_size);
            BOOST_CHECK_EQUAL(variant_type::serialize<C>(buffer.get(), data), expected_size);
        } else {
            BOOST_CHECK_EQUAL(variant_type::size_when_serialized<D>(integer), expected_size);
            BOOST_CHECK_EQUAL(variant_type::serialize<D>(buffer.get(), integer), expected_size);
        }

        auto ctx = test_variant_context { alternative_idx };

        BOOST_CHECK_EQUAL(variant_type::serialized_object_size(buffer.get(), ctx), expected_size);

        auto view = variant_type::make_view(buffer.get(), ctx);
        bool visitor_was_called = false;
        view.visit(make_visitor(
                [&] (imr::pod<uint64_t>::view val) {
                    visitor_was_called = true;
                    if (alternative_idx == 0) {
                        BOOST_CHECK_EQUAL(val.load(), uinteger);
                    } else {
                        BOOST_FAIL("wrong variant alternative (B)");
                    }
                },
                [&] (imr::buffer<C>::view buf) {
                    visitor_was_called = true;
                    if (alternative_idx == 1) {
                        BOOST_CHECK(boost::equal(data, buf));
                    } else {
                        BOOST_FAIL("wrong variant alternative (C)");
                    }
                },
                [&] (imr::pod<int64_t>::view val) {
                    visitor_was_called = true;
                    if (alternative_idx == 2) {
                        BOOST_CHECK_EQUAL(val.load(), integer);
                    } else {
                        BOOST_FAIL("wrong variant alternative (D)");
                    }
                }
        ), ctx);
        BOOST_CHECK(visitor_was_called);
    }
}

BOOST_AUTO_TEST_SUITE_END();
