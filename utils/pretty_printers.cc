/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "pretty_printers.hh"
#include <fmt/ostream.h>
#include <tuple>

template <typename Suffixes>
static constexpr std::tuple<size_t, std::string_view, std::string_view>
do_format(size_t n, Suffixes suffixes, unsigned scale, bool bytes) {
    size_t factor = n;
    const char* suffix = "";
    for (auto next_suffix : suffixes) {
        size_t next_factor = factor / scale;
        if (next_factor == 0) {
            break;
        }
        factor = next_factor;
        suffix = next_suffix;
    }
    if (!bytes) {
        return {factor, suffix, ""};
    }
    if (factor == n) {
        if (n == 1) {
            return {factor, suffix, " byte"};
        } else {
            return {factor, suffix, " bytes"};
        }
    } else {
        return {factor, suffix, "B"};
    }
}

template <typename FormatContext>
auto fmt::formatter<utils::pretty_printed_data_size>::format(utils::pretty_printed_data_size data_size,
                                                             FormatContext& ctx) const -> decltype(ctx.out()) {
    if (_prefix == prefix_type::IEC) {
        // ISO/IEC units
        static constexpr auto suffixes = {"Ki", "Mi", "Gi", "Ti", "Pi"};
        auto [n, suffix, bytes] = do_format(data_size._size, suffixes, 1024, _bytes);
        return fmt::format_to(ctx.out(), "{}{}{}", n, suffix, bytes);
    } else {
        // SI units
        static constexpr auto suffixes = {"k", "M", "G", "T", "P"};
        auto [n, suffix, bytes] = do_format(data_size._size, suffixes, 1000, _bytes);
        return fmt::format_to(ctx.out(), "{}{}{}", n, suffix, bytes);
    }
}

template
auto fmt::formatter<utils::pretty_printed_data_size>::format<fmt::format_context>(
    utils::pretty_printed_data_size,
    fmt::format_context& ctx) const
    -> decltype(ctx.out());
template
auto fmt::formatter<utils::pretty_printed_data_size>::format<fmt::basic_format_context<std::back_insert_iterator<std::string>, char>>(
    utils::pretty_printed_data_size,
    fmt::basic_format_context<std::back_insert_iterator<std::string>, char>& ctx) const
    -> decltype(ctx.out());

template <typename FormatContext>
auto fmt::formatter<utils::pretty_printed_throughput>::format(const utils::pretty_printed_throughput& tp,
                                                              FormatContext& ctx) const -> decltype(ctx.out()) {
    uint64_t throughput = tp._duration.count() > 0 ? tp._size / tp._duration.count() : 0;
    auto out = size_formatter::format(utils::pretty_printed_data_size{throughput}, ctx);
    return fmt::format_to(out, "{}", "/s");
}

template
auto fmt::formatter<utils::pretty_printed_throughput>::format<fmt::format_context>(
    const utils::pretty_printed_throughput&,
    fmt::format_context& ctx) const
    -> decltype(ctx.out());
template
auto fmt::formatter<utils::pretty_printed_throughput>::format<fmt::basic_format_context<std::back_insert_iterator<std::string>, char>>(
    const utils::pretty_printed_throughput&,
    fmt::basic_format_context<std::back_insert_iterator<std::string>, char>& ctx) const
    -> decltype(ctx.out());

namespace utils {

std::ostream& operator<<(std::ostream& os, pretty_printed_data_size data) {
    fmt::print(os, "{}", data);
    return os;
}

std::ostream& operator<<(std::ostream& os, pretty_printed_throughput tp) {
    fmt::print(os, "{}", tp);
    return os;
}

}
