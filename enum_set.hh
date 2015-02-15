/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#pragma once

#include <cstddef>
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

template<typename Enum>
struct enum_set {
    using mask_type = size_t; // TODO: use the smallest sufficient type
    using enum_type = typename Enum::enum_type;

    static inline mask_type mask_for(enum_type e) {
        return mask_type(1) << Enum::sequence_for(e);
    }

    template<enum_type Elem>
    static constexpr mask_type mask_for() {
        // FIXME: for some reason Enum::sequence_for<Elem>() does not compile
        return mask_type(1) << static_cast<typename Enum::sequence_type>(Elem);
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
    };
};
