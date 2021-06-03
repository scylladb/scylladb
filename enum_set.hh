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

#pragma once

#include <boost/iterator/transform_iterator.hpp>
#include <seastar/core/bitset-iter.hh>

#include <algorithm>
#include <cstddef>
#include <optional>
#include <stdexcept>
#include <type_traits>
#include <limits>

/**
 *
 * Allows to take full advantage of compile-time information when operating
 * on a set of enum values.
 *
 * Examples:
 *
 *   enum class x { A, B, C };
 *   using my_enum = super_enum<x, x::A, x::B, x::C>;
 *   using my_enumset = enum_set<my_enum>;
 *
 *   static_assert(my_enumset::frozen<x::A, x::B>::contains<x::A>(), "it should...");
 *
 *   assert(my_enumset::frozen<x::A, x::B>::contains(my_enumset::prepare<x::A>()));
 *
 *   assert(my_enumset::frozen<x::A, x::B>::contains(x::A));
 *
 */


template<typename EnumType, EnumType... Items>
struct super_enum {
    using enum_type = EnumType;

    template<enum_type... values>
    struct max {
        static constexpr enum_type max_of(enum_type a, enum_type b) {
            return a > b ? a : b;
        }

        template<enum_type first, enum_type second, enum_type... rest>
        static constexpr enum_type get() {
            return max_of(first, get<second, rest...>());
        }

        template<enum_type first>
        static constexpr enum_type get() { return first; }

        static constexpr enum_type value = get<values...>();
    };

    template<enum_type... values>
    struct min {
        static constexpr enum_type min_of(enum_type a, enum_type b) {
            return a < b ? a : b;
        }

        template<enum_type first, enum_type second, enum_type... rest>
        static constexpr enum_type get() {
            return min_of(first, get<second, rest...>());
        }

        template<enum_type first>
        static constexpr enum_type get() { return first; }

        static constexpr enum_type value = get<values...>();
    };

    using sequence_type = typename std::underlying_type<enum_type>::type;

    template <enum_type first, enum_type... rest>
    struct valid_sequence {
        static constexpr bool apply(sequence_type v) noexcept {
            return (v == static_cast<sequence_type>(first)) || valid_sequence<rest...>::apply(v);
        }
    };

    template <enum_type first>
    struct valid_sequence<first> {
        static constexpr bool apply(sequence_type v) noexcept {
            return v == static_cast<sequence_type>(first);
        }
    };

    static constexpr bool is_valid_sequence(sequence_type v) noexcept {
        return valid_sequence<Items...>::apply(v);
    }

    template<enum_type Elem>
    static constexpr sequence_type sequence_for() {
        return static_cast<sequence_type>(Elem);
    }

    static sequence_type sequence_for(enum_type elem) {
        return static_cast<sequence_type>(elem);
    }

    static constexpr sequence_type max_sequence = sequence_for<max<Items...>::value>();
    static constexpr sequence_type min_sequence = sequence_for<min<Items...>::value>();

    static_assert(min_sequence >= 0, "negative enum values unsupported");
};

class bad_enum_set_mask : public std::invalid_argument {
public:
    bad_enum_set_mask() : std::invalid_argument("Bit mask contains invalid enumeration indices.") {
    }
};

template<typename Enum>
class enum_set {
public:
    using mask_type = size_t; // TODO: use the smallest sufficient type
    using enum_type = typename Enum::enum_type;

private:
    static constexpr int mask_digits = std::numeric_limits<mask_type>::digits;
    using mask_iterator = seastar::bitsets::set_iterator<mask_digits>;

    mask_type _mask;
    constexpr enum_set(mask_type mask) : _mask(mask) {}

    template<enum_type Elem>
    static constexpr unsigned shift_for() {
        return Enum::template sequence_for<Elem>();
    }

    static auto make_iterator(mask_iterator iter) {
        return boost::make_transform_iterator(std::move(iter), [](typename Enum::sequence_type s) {
            return enum_type(s);
        });
    }

public:
    using iterator = std::invoke_result_t<decltype(&enum_set::make_iterator), mask_iterator>;

    constexpr enum_set() : _mask(0) {}

    /**
     * \throws \ref bad_enum_set_mask
     */
    static constexpr enum_set from_mask(mask_type mask) {
        const auto bit_range = seastar::bitsets::for_each_set(std::bitset<mask_digits>(mask));

        if (!std::all_of(bit_range.begin(), bit_range.end(), &Enum::is_valid_sequence)) {
            throw bad_enum_set_mask();
        }

        return enum_set(mask);
    }

    static constexpr mask_type full_mask() {
        return ~(std::numeric_limits<mask_type>::max() << (Enum::max_sequence + 1));
    }

    static constexpr enum_set full() {
        return enum_set(full_mask());
    }

    static inline mask_type mask_for(enum_type e) {
        return mask_type(1) << Enum::sequence_for(e);
    }

    template<enum_type Elem>
    static constexpr mask_type mask_for() {
        return mask_type(1) << shift_for<Elem>();
    }

    struct prepared {
        mask_type mask;
        bool operator==(const prepared& o) const {
            return mask == o.mask;
        }
    };

    static prepared prepare(enum_type e) {
        return {mask_for(e)};
    }

    template<enum_type e>
    static constexpr prepared prepare() {
        return {mask_for<e>()};
    }

    static_assert(std::numeric_limits<mask_type>::max() >= ((size_t)1 << Enum::max_sequence), "mask type too small");

    template<enum_type e>
    bool contains() const {
        return bool(_mask & mask_for<e>());
    }

    bool contains(enum_type e) const {
        return bool(_mask & mask_for(e));
    }

    template<enum_type e>
    void remove() {
        _mask &= ~mask_for<e>();
    }

    void remove(enum_type e) {
        _mask &= ~mask_for(e);
    }

    template<enum_type e>
    void set() {
        _mask |= mask_for<e>();
    }

    template<enum_type e>
    void set_if(bool condition) {
        _mask |= mask_type(condition) << shift_for<e>();
    }

    void set(enum_type e) {
        _mask |= mask_for(e);
    }

    void add(const enum_set& other) {
        _mask |= other._mask;
    }

    explicit operator bool() const {
        return bool(_mask);
    }

    mask_type mask() const {
        return _mask;
    }

    iterator begin() const {
        return make_iterator(mask_iterator(_mask));
    }

    iterator end() const {
        return make_iterator(mask_iterator(0));
    }

    template<enum_type... items>
    struct frozen {
        template<enum_type first>
        static constexpr mask_type make_mask() {
            return mask_for<first>();
        }

        static constexpr mask_type make_mask() {
            return 0;
        }

        template<enum_type first, enum_type second, enum_type... rest>
        static constexpr mask_type make_mask() {
            return mask_for<first>() | make_mask<second, rest...>();
        }

        static constexpr mask_type mask = make_mask<items...>();

        template<enum_type Elem>
        static constexpr bool contains() {
            return mask & mask_for<Elem>();
        }

        static bool contains(enum_type e) {
            return mask & mask_for(e);
        }

        static bool contains(prepared e) {
            return mask & e.mask;
        }

        static constexpr enum_set<Enum> unfreeze() {
            return enum_set<Enum>(mask);
        }
    };

    template<enum_type... items>
    static constexpr enum_set<Enum> of() {
        return frozen<items...>::unfreeze();
    }
};
