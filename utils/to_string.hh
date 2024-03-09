/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/sstring.hh>
#include <ranges>
#include <vector>
#include <sstream>
#include <unordered_set>
#include <set>
#include <optional>
#include <list>
#include <map>
#include <array>
#include <deque>

#include <fmt/format.h>

#include "seastarx.hh"

#include <boost/range/adaptor/transformed.hpp>

namespace utils {

template <std::ranges::range Range>
std::ostream& format_range(std::ostream& os, const Range& items, std::string_view paren = "{}") {
    fmt::print(os, "{}{}{}", paren.front(), fmt::join(items, ", "), paren.back());
    return os;
}

namespace internal {

template<bool NeedsComma, typename Printable>
struct print_with_comma {
    const Printable& v;
};

template<bool NeedsComma, typename Printable>
std::ostream& operator<<(std::ostream& os, const print_with_comma<NeedsComma, Printable>& x) {
    os << x.v;
    if (NeedsComma) {
        os << ", ";
    }
    return os;
}

} // namespace internal
} // namespace utils

namespace std {

template <typename K, typename V>
std::ostream& operator<<(std::ostream& os, const std::pair<K, V>& p) {
    os << "{" << p.first << ", " << p.second << "}";
    return os;
}

template<typename... T, size_t... I>
std::ostream& print_tuple(std::ostream& os, const std::tuple<T...>& p, std::index_sequence<I...>) {
    return ((os << "{" ) << ... << utils::internal::print_with_comma<I < sizeof...(I) - 1, T>{std::get<I>(p)}) << "}";
}

template <typename... T>
std::ostream& operator<<(std::ostream& os, const std::tuple<T...>& p) {
    return print_tuple(os, p, std::make_index_sequence<sizeof...(T)>());
}

// Vector-like ranges
template <std::ranges::range Range>
requires (
       std::same_as<Range, std::vector<std::ranges::range_value_t<Range>>>
    || std::same_as<Range, std::list<std::ranges::range_value_t<Range>>>
    || std::same_as<Range, std::initializer_list<std::ranges::range_value_t<Range>>>
    || std::same_as<Range, std::deque<std::ranges::range_value_t<Range>>>
)
std::ostream& operator<<(std::ostream& os, const Range& items) {
    return utils::format_range(os, items);
}

template <typename T, typename... Args>
std::ostream& operator<<(std::ostream& os, const std::set<T, Args...>& items) {
    return utils::format_range(os, items);
}

template <typename T, typename... Args>
std::ostream& operator<<(std::ostream& os, const std::unordered_set<T, Args...>& items) {
    return utils::format_range(os, items);
}

template <typename K, typename V, typename... Args>
std::ostream& operator<<(std::ostream& os, const std::map<K, V, Args...>& items) {
    return utils::format_range(os, items);
}

template <typename... Args>
std::ostream& operator<<(std::ostream& os, const boost::transformed_range<Args...>& items) {
    return utils::format_range(os, items);
}

template <typename T, std::size_t N>
std::ostream& operator<<(std::ostream& os, const std::array<T, N>& items) {
    return utils::format_range(os, items, "[]");
}

template <typename T>
std::ostream& operator<<(std::ostream& os, const std::optional<T>& opt) {
    if (opt) {
        os << "{" << *opt << "}";
    } else {
        os << "{}";
    }
    return os;
}

} // namespace std

template<typename T>
struct fmt::formatter<std::optional<T>> : fmt::formatter<std::string_view> {
    template <typename FormatContext>
    auto format(const std::optional<T>& opt, FormatContext& ctx) const {
        if (opt) {
            return fmt::format_to(ctx.out(), "{}", *opt);
        } else {
            return fmt::format_to(ctx.out(), "{{}}");
        }
     }
};

template <> struct fmt::formatter<std::strong_ordering> : fmt::formatter<std::string_view> {
    auto format(std::strong_ordering, fmt::format_context& ctx) const -> decltype(ctx.out());
};

template <> struct fmt::formatter<std::weak_ordering> : fmt::formatter<std::string_view> {
    auto format(std::weak_ordering, fmt::format_context& ctx) const -> decltype(ctx.out());
};

template <> struct fmt::formatter<std::partial_ordering> : fmt::formatter<std::string_view> {
    auto format(std::partial_ordering, fmt::format_context& ctx) const -> decltype(ctx.out());
};
