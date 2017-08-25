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

#define BOOST_TEST_MODULE meta
#include <boost/test/unit_test.hpp>

#include <seastar/util/log.hh>

#include "utils/meta.hh"

namespace internal {

template<typename T>
struct check_constexpr {
    template<T N>
    struct check {
        enum : T {
            value = N,
        };
    };
};

template<typename T>
struct first_argument { };

template<typename R, typename T, typename... Ts>
struct first_argument<R(T, Ts...)> {
    using type = T;
};

}

#define INTERNAL_STATIC_CHECK_EQUAL(expected, actual, actual_str) \
    BOOST_CHECK_MESSAGE(internal::check_constexpr<std::decay_t<decltype(actual)>>::check<(actual)>::value == (expected), \
                        actual_str " expected to be equal " #expected " [actual: " << (actual) << ", expected: " << (expected) << "]")

#define INTERNAL_STATIC_CHECK_SAME(expr, expected, actual, actual_str) \
    BOOST_CHECK_MESSAGE(expr, actual_str " expected to be the same as " #expected \
                        " [actual: " << seastar::pretty_type_name(typeid(typename internal::first_argument<void(actual)>::type)) << ", expected: " \
                        << seastar::pretty_type_name(typeid(internal::first_argument<void(expected)>::type)) << "]")

#define STATIC_CHECK_EQUAL(expected, ...) \
    INTERNAL_STATIC_CHECK_EQUAL(expected, (__VA_ARGS__), #__VA_ARGS__)

#define STATIC_CHECK_SAME(expected, ...) \
    INTERNAL_STATIC_CHECK_SAME((std::is_same<__VA_ARGS__, typename internal::first_argument<void(expected)>::type>::value), expected, (__VA_ARGS__), #__VA_ARGS__)

class A { };
class B { };
class C { };
class D { };

BOOST_AUTO_TEST_CASE(find) {
    STATIC_CHECK_EQUAL(0, meta::find<A, A, B, C, D>);
    STATIC_CHECK_EQUAL(1, meta::find<B, A, B, C, D>);
    STATIC_CHECK_EQUAL(2, meta::find<C, A, B, C, D>);
    STATIC_CHECK_EQUAL(3, meta::find<D, A, B, C, D>);

    STATIC_CHECK_EQUAL(0, meta::find<A, A>);
    STATIC_CHECK_EQUAL(0, meta::find<A, A, A>);
    STATIC_CHECK_EQUAL(1, meta::find<A, B, A, A>);

    STATIC_CHECK_EQUAL(0, meta::find<A, meta::list<A, B, C, D>>);
    STATIC_CHECK_EQUAL(1, meta::find<B, meta::list<A, B, C, D>>);
    STATIC_CHECK_EQUAL(2, meta::find<C, meta::list<A, B, C, D>>);
    STATIC_CHECK_EQUAL(3, meta::find<D, meta::list<A, B, C, D>>);

    STATIC_CHECK_EQUAL(0, meta::find<A, meta::list<A>>);
    STATIC_CHECK_EQUAL(0, meta::find<A, meta::list<A, A>>);
    STATIC_CHECK_EQUAL(1, meta::find<A, meta::list<B, A, A>>);

    STATIC_CHECK_EQUAL(1, meta::find<meta::list<A>, meta::list<B>, meta::list<A>>);
    STATIC_CHECK_EQUAL(1, meta::find<meta::list<A>, meta::list<meta::list<B>, meta::list<A>>>);
}

BOOST_AUTO_TEST_CASE(get) {
    STATIC_CHECK_SAME(A, meta::get<0, A, B, C, D>);
    STATIC_CHECK_SAME(B, meta::get<1, A, B, C, D>);
    STATIC_CHECK_SAME(C, meta::get<2, A, B, C, D>);
    STATIC_CHECK_SAME(D, meta::get<3, A, B, C, D>);

    STATIC_CHECK_SAME(A, meta::get<0, meta::list<A, B, C, D>>);
    STATIC_CHECK_SAME(B, meta::get<1, meta::list<A, B, C, D>>);
    STATIC_CHECK_SAME(C, meta::get<2, meta::list<A, B, C, D>>);
    STATIC_CHECK_SAME(D, meta::get<3, meta::list<A, B, C, D>>);

    STATIC_CHECK_SAME(A, meta::get<0, meta::list<A>>);
    STATIC_CHECK_SAME(meta::list<A>, meta::get<0, meta::list<meta::list<A>>>);
}

BOOST_AUTO_TEST_CASE(take) {
    STATIC_CHECK_SAME(meta::list<A>, meta::take<1, A, B, C, D>);
    STATIC_CHECK_SAME((meta::list<A, B>), meta::take<2, A, B, C, D>);
    STATIC_CHECK_SAME((meta::list<A, B, C>), meta::take<3, A, B, C, D>);
    STATIC_CHECK_SAME((meta::list<A, B, C, D>), meta::take<4, A, B, C, D>);

    STATIC_CHECK_SAME(meta::list<A>, meta::take<1, meta::list<A, B, C, D>>);
    STATIC_CHECK_SAME((meta::list<A, B>), meta::take<2, meta::list<A, B, C, D>>);
    STATIC_CHECK_SAME((meta::list<A, B, C>), meta::take<3, meta::list<A, B, C, D>>);
    STATIC_CHECK_SAME((meta::list<A, B, C, D>), meta::take<4, meta::list<A, B, C, D>>);

    STATIC_CHECK_SAME(meta::list<A>, meta::take<1, meta::list<A>>);
    STATIC_CHECK_SAME(meta::list<meta::list<A>>, meta::take<1, meta::list<meta::list<A>>>);
    STATIC_CHECK_SAME((meta::list<meta::list<A, B>>), meta::take<1, meta::list<meta::list<A, B>>>);
}

BOOST_AUTO_TEST_CASE(size) {
    STATIC_CHECK_EQUAL(0, meta::size<>);
    STATIC_CHECK_EQUAL(1, meta::size<A>);
    STATIC_CHECK_EQUAL(2, meta::size<A, B>);
    STATIC_CHECK_EQUAL(3, meta::size<A, B, C>);
    STATIC_CHECK_EQUAL(4, meta::size<A, B, C, D>);

    STATIC_CHECK_EQUAL(0, meta::size<meta::list<>>);
    STATIC_CHECK_EQUAL(1, meta::size<meta::list<A>>);
    STATIC_CHECK_EQUAL(2, meta::size<meta::list<A, B>>);
    STATIC_CHECK_EQUAL(3, meta::size<meta::list<A, B, C>>);
    STATIC_CHECK_EQUAL(4, meta::size<meta::list<A, B, C, D>>);

    STATIC_CHECK_EQUAL(1, meta::size<meta::list<meta::list<A, B>>>);
    STATIC_CHECK_EQUAL(3, meta::size<meta::list<A, B>, C, D>);
    STATIC_CHECK_EQUAL(3, meta::size<meta::list<meta::list<A, B>, C, D>>);
}

class constexpr_count_all_fn {
    size_t _n = 0;
public:
    constexpr constexpr_count_all_fn() = default;
    template<typename T>
    constexpr void operator()(T) { _n++; }
    constexpr size_t get() { return _n; }
};

template<typename... Ts>
constexpr size_t constexpr_count_all()
{
    constexpr_count_all_fn constexpr_fn;
    meta::for_each<Ts...>(constexpr_fn);
    return constexpr_fn.get();
}

BOOST_AUTO_TEST_CASE(for_each) {
    STATIC_CHECK_EQUAL(0, constexpr_count_all<>());
    STATIC_CHECK_EQUAL(4, constexpr_count_all<A, B, C, D>());

    size_t n = 0;
    meta::for_each<A, B, C, D>([&] (auto&& ptr) {
        using type = std::remove_pointer_t<std::decay_t<decltype(ptr)>>;
        switch (n) {
        case 0: STATIC_CHECK_SAME(A, type); break;
        case 1: STATIC_CHECK_SAME(B, type); break;
        case 2: STATIC_CHECK_SAME(C, type); break;
        case 3: STATIC_CHECK_SAME(D, type); break;
        default: BOOST_FAIL("should not reach"); break;
        }
        n++;
    });
    BOOST_CHECK_EQUAL(4, n);

    STATIC_CHECK_EQUAL(0, constexpr_count_all<meta::list<>>());
    STATIC_CHECK_EQUAL(4, constexpr_count_all<meta::list<A, B, C, D>>());

    n = 0;
    meta::for_each<meta::list<A, B, C, D>>([&] (auto ptr) {
        using type = std::remove_pointer_t<decltype(ptr)>;
        switch (n) {
        case 0: STATIC_CHECK_SAME(A, type); break;
        case 1: STATIC_CHECK_SAME(B, type); break;
        case 2: STATIC_CHECK_SAME(C, type); break;
        case 3: STATIC_CHECK_SAME(D, type); break;
        default: BOOST_FAIL("should not reach"); break;
        }
        n++;
    });
    BOOST_CHECK_EQUAL(4, n);

    n = 0;
    meta::for_each<meta::take<2, A, B, C, D>>([&] (auto ptr) {
        using type = std::remove_pointer_t<decltype(ptr)>;
        switch (n) {
        case 0: STATIC_CHECK_SAME(A, type); break;
        case 1: STATIC_CHECK_SAME(B, type); break;
        default: BOOST_FAIL("should not reach"); break;
        }
        n++;
    });
    BOOST_CHECK_EQUAL(2, n);

    n = 0;
    using list = meta::list<A, B, C, D>;
    meta::for_each<meta::take<meta::size<list> - 1, list>>([&] (auto ptr) {
        using type = std::remove_pointer_t<decltype(ptr)>;
        switch (n) {
        case 0: STATIC_CHECK_SAME(A, type); break;
        case 1: STATIC_CHECK_SAME(B, type); break;
        case 2: STATIC_CHECK_SAME(C, type); break;
        default: BOOST_FAIL("should not reach"); break;
        }
        n++;
    });
    BOOST_CHECK_EQUAL(3, n);
}
