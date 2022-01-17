/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <variant>
#include <type_traits>

namespace utils {

// Given type T and an std::variant Variant, return std::true_type if T is a variant element
// This is also the recursion base case (empty variant), so return false.
template <typename T, typename Variant>
struct is_variant_element : std::false_type {
};

// Match - return true
template <typename T, typename... Elements>
struct is_variant_element<T, std::variant<T, Elements...>> : std::true_type {
};

// No match - recurse
template <typename T, typename U, typename... Elements>
struct is_variant_element<T, std::variant<U, Elements...>> : is_variant_element<T, std::variant<Elements...>> {
};

// Givent type T and std::variant, true if T is one of the variant elements.
template <typename T, typename Variant>
concept VariantElement = is_variant_element<T, Variant>::value;

}
