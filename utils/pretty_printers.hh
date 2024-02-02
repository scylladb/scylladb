/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <chrono>
#include <fmt/format.h>

namespace utils {

class pretty_printed_data_size {
    uint64_t _size;
public:
    pretty_printed_data_size(uint64_t size) : _size(size) {}

    friend fmt::formatter<pretty_printed_data_size>;
};

class pretty_printed_throughput {
    uint64_t _size;
    std::chrono::duration<float> _duration;
public:
    pretty_printed_throughput(uint64_t size, std::chrono::duration<float> dur) : _size(size), _duration(std::move(dur)) {}

    friend fmt::formatter<pretty_printed_throughput>;
};


}

// print data_size using IEC or SI binary prefix annotation with optional "B"
// or " bytes" unit postfix.
//
// usage:
//   fmt::print("{}", 10'024); // prints "10kB", using SI and add the "B" unit
//                             // postfix by default
//   fmt::print("{:i}", 42);   // prints "42 bytes"
//   fmt::print("{:ib}", 10'024); // prints "10Ki", IEC unit is used, without
//                                // the " bytes" or "B" unit
//   fmt::print("{:I}", 1024); // prints "1K", IEC unit is used, but without
//                             // the "iB" postfix.
//   fmt::print("{:s}", 10); // prints "10 bytes", SI unit is used
//   fmt::print("{:sb}", 10'000); // prints "10k", SI unit is used, without
//                                // the unit postfix
template <>
struct fmt::formatter<utils::pretty_printed_data_size> {
    enum class prefix_type {
        SI,
        IEC,
        IEC_SANS_I,
    };
    prefix_type _prefix = prefix_type::SI;
    bool _bytes = true;
    constexpr auto parse(format_parse_context& ctx) {
        auto it = ctx.begin();
        auto end = ctx.end();
        if (it != end) {
            if (*it == 's') {
                _prefix = prefix_type::SI;
                ++it;
            } else if (*it == 'i') {
                _prefix = prefix_type::IEC;
                ++it;
            } else if (*it == 'I') {
                _prefix = prefix_type::IEC_SANS_I;
                ++it;
            }
            if (*it == 'b') {
                _bytes = false;
                ++it;
            }
        }
        if (it != end && *it != '}') {
            throw fmt::format_error("invalid format specifier");
        }
        return it;
    }
    template <typename FormatContext>
    auto format(utils::pretty_printed_data_size, FormatContext& ctx) const -> decltype(ctx.out());
};

template <>
struct fmt::formatter<utils::pretty_printed_throughput>
    : private fmt::formatter<utils::pretty_printed_data_size> {
    using size_formatter = fmt::formatter<utils::pretty_printed_data_size>;
public:
    using size_formatter::parse;
    template <typename FormatContext>
    auto format(const utils::pretty_printed_throughput&, FormatContext& ctx) const -> decltype(ctx.out());
};
