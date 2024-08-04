/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "utils/assert.hh"
#include "pretty_printers.hh"
#include <tuple>
#include <cassert>

template <typename Suffixes>
static constexpr std::tuple<size_t, std::string_view, std::string_view>
do_format(size_t n, Suffixes suffixes, unsigned scale, unsigned precision, bool bytes) {
    SCYLLA_ASSERT(scale < precision);
    size_t factor = n;
    const char* suffix = "";
    size_t remainder = 0;
    for (auto next_suffix : suffixes) {
        if (factor < precision && remainder == 0) {
            // If there is no remainder we go below precision because we don't
            // loose any.
            break;
        }
        if (factor < scale) {
            break;
        }
        remainder = factor % scale;
        factor /= scale;
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
        auto [n, suffix, bytes] = do_format(data_size._size, suffixes, 1024, 8192, _bytes);
        return fmt::format_to(ctx.out(), "{}{}{}", n, suffix, bytes);
    } else if (_prefix == prefix_type::IEC_SANS_I) {
        static constexpr auto suffixes = {"K", "M", "G", "T", "P"};
        auto [n, suffix, bytes] = do_format(data_size._size, suffixes, 1024, 8192, false);
        if (suffix.empty()) {
            bytes = "B";
        }
        return fmt::format_to(ctx.out(), "{}{}{}", n, suffix, bytes);
    } else {
        // SI units
        static constexpr auto suffixes = {"k", "M", "G", "T", "P"};
        auto [n, suffix, bytes] = do_format(data_size._size, suffixes, 1000, 10000, _bytes);
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
