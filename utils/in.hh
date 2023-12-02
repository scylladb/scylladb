/*
 * Copyright (C) 2017-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <type_traits>
#include <vector>

#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/algorithm/copy.hpp>

/**
 * Adopted from
 * http://cpptruths.blogspot.se/2013/09/21-ways-of-passing-parameters-plus-one.html#inTidiom
 *
 * Helper type to somewhat help bridge std::initializer_list and move semantics
 *
 */
namespace utils {

template <typename T> class in;
template <typename T> struct is_in {
    static constexpr bool value = false;
};
template <typename T> struct is_in<in<T>> {
    static constexpr bool value = true;
};
template <typename T> struct is_in<const in<T>> {
    static constexpr bool value = true;
};

//
// Allows initializer lists of mixed rvalue/lvalues, where receive can
// use move semantics on provided value(s).
// Allows in-place conversion, _BUT_ (warning), a converted value
// is only valid until end-of-statement (;). So a call like:
//
//  void apa(initializer_list<in<std::string>>);
//  apa({ "ola", "kong" });
//
// is ok, but
//
//  initializer_list<in<std::string>> apa = { "ola", "kong" };
//  for (auto&x : apa) ...
//
// is not.
// So again. Only for calls.
template<typename T>
class in {
public:
    in(const T& l)
        : _value(l)
        , _is_rvalue(false)
    {}
    in(T&& r)
        : _value(r)
        , _is_rvalue(true)
    {}

    // Support for implicit conversion via perfect forwarding.
    //
    struct storage {
        storage(): created (false) {}
        ~storage() {
            if (created) {
                reinterpret_cast<T*> (&data)->~T ();
            }
        }

        bool created;
        typename std::aligned_storage<sizeof(T), alignof(T)>::type data;
    };

    // conversion constructor. See warning above.
    template<typename T1,
      typename std::enable_if<std::is_convertible<T1, T>::value
          && !is_in<typename std::remove_reference<T1>::type>::value,int>::type = 0>
    in(T1&& x, storage&& s = storage())
        : _value(*new (&s.data) T (std::forward<T1>(x)))
        , _is_rvalue(true)
    {
        s.created = true;
    }
    in(T& l)
        : _value(l)
        , _is_rvalue(false)
    {} // For T1&& becoming T1&.

    // Accessors.
    //
    bool lvalue() const { return !_is_rvalue; }
    bool rvalue() const { return _is_rvalue; }

    operator const T&() const { return get(); }
    const T& get() const { return _value; }
    T&& rget() const { return std::move (const_cast<T&> (_value)); }

    // Return a copy if lvalue.
    //
    T move() const {
        if (_is_rvalue) {
            return rget();
        }
        return _value;
    }

private:
    const T& _value;
    bool _is_rvalue;
};

template<typename T, typename Container>
Container make_from_in_list(std::initializer_list<in<T>> args) {
    return boost::copy_range<Container>(args | boost::adaptors::transformed([](const in<T>& v) {
        return v.move();
    }));
}

template<typename T, typename... Args>
std::vector<T, Args...> make_vector_from_in_list(std::initializer_list<in<T>> args) {
    return make_from_in_list<T, std::vector<T, Args...>>(args);
}

}
