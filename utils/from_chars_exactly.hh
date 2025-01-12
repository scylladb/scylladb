// Copyright (C) 2025-present ScyllaDB
// SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

#pragma once

#include <charconv>
#include <concepts>
#include <string_view>

namespace utils {

// Parses a string into a number, throwing an exception if the entire string is not consumed
// or a conversion error happens. The exception is created by calling the provided callable.
template <typename T>
requires std::integral<T> || std::floating_point<T>
T
from_chars_exactly(std::string_view str, std::invocable<std::string_view> auto&& make_exception) {
    T result;
    auto [ptr, ec] = std::from_chars(str.data(), str.data() + str.size(), result);
    if (ptr != str.data() + str.size() || ec != std::errc{}) {
        throw make_exception(str);
    }
    return result;
}

}
