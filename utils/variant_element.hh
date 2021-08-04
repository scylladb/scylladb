/*
 * Copyright (C) 2021-present ScyllaDB
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
