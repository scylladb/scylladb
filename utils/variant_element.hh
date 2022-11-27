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
template <class T, class Variant>
struct is_variant_element;

template <class T, class... Elements>
struct is_variant_element<T, std::variant<Elements...>> : std::bool_constant<(std::is_same_v<T, Elements> || ...)> {
};

// Givent type T and std::variant, true if T is one of the variant elements.
template <typename T, typename Variant>
concept VariantElement = is_variant_element<T, Variant>::value;

}
