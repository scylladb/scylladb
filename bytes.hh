/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "seastarx.hh"
#include <fmt/format.h>
#include <seastar/core/sstring.hh>
#include "utils/hashing.hh"
#include <optional>
#include <iosfwd>
#include <functional>
#include <compare>
#include "utils/mutable_view.hh"
#include "utils/simple_hashers.hh"

using bytes = basic_sstring<int8_t, uint32_t, 31, false>;
using bytes_view = std::basic_string_view<int8_t>;
using bytes_mutable_view = basic_mutable_view<bytes_view::value_type>;
using bytes_opt = std::optional<bytes>;
using sstring_view = std::string_view;

inline bytes to_bytes(bytes&& b) {
    return std::move(b);
}

inline sstring_view to_sstring_view(bytes_view view) {
    return {reinterpret_cast<const char*>(view.data()), view.size()};
}

inline bytes_view to_bytes_view(sstring_view view) {
    return {reinterpret_cast<const int8_t*>(view.data()), view.size()};
}

struct fmt_hex {
    const bytes_view& v;
    fmt_hex(const bytes_view& v) noexcept : v(v) {}
};

bytes from_hex(sstring_view s);
sstring to_hex(bytes_view b);
sstring to_hex(const bytes& b);
sstring to_hex(const bytes_opt& b);

template <>
struct fmt::formatter<fmt_hex> {
    size_t _group_size_in_bytes = 0;
    char _delimiter = ' ';
public:
    // format_spec := [group_size[delimiter]]
    // group_size := a char from '0' to '9'
    // delimiter := a char other than '{'  or '}'
    //
    // by default, the given bytes are printed without delimiter, just
    // like a string. so a string view of {0x20, 0x01, 0x0d, 0xb8} is
    // printed like:
    // "20010db8".
    //
    // but the format specifier can be used to customize how the bytes
    // are printed. for instance, to print an bytes_view like IPv6. so
    // the format specfier would be "{:2:}", where
    // - "2": bytes are printed in groups of 2 bytes
    // - ":": each group is delimited by ":"
    // and the formatted output will look like:
    // "2001:0db8:0000"
    //
    // or we can mimic how the default format of used by hexdump using
    // "{:2 }", where
    // - "2": bytes are printed in group of 2 bytes
    // - " ": each group is delimited by " "
    // and the formatted output will look like:
    // "2001 0db8 0000"
    //
    // or we can just print each bytes and separate them by a dash using
    // "{:1-}"
    // and the formatted output will look like:
    // "20-01-0b-b8-00-00"
    constexpr auto parse(fmt::format_parse_context& ctx) {
        // get the delimiter if any
        auto it = ctx.begin();
        auto end = ctx.end();
        if (it != end && *it != '}') {
            int group_size = *it++ - '0';
            if (group_size < 0 ||
                static_cast<size_t>(group_size) > sizeof(uint64_t)) {
                throw format_error("invalid group_size");
            }
            _group_size_in_bytes = group_size;
            if (it != end) {
                // optional delimiter
                _delimiter = *it++;
            }
        }
        if (it != end && *it != '}') {
            throw format_error("invalid format");
        }
        return it;
    }
    template <typename FormatContext>
    auto format(const ::fmt_hex& s, FormatContext& ctx) const {
        auto out = ctx.out();
        const auto& v = s.v;
        if (_group_size_in_bytes > 0) {
            for (size_t i = 0, size = v.size(); i < size; i++) {
                if (i != 0 && i % _group_size_in_bytes == 0) {
                    fmt::format_to(out, "{}{:02x}", _delimiter, std::byte(v[i]));
                } else {
                    fmt::format_to(out, "{:02x}", std::byte(v[i]));
                }
            }
        } else {
            for (auto b : v) {
                fmt::format_to(out, "{:02x}", std::byte(b));
            }
        }
        return out;
    }
};

template <>
struct fmt::formatter<bytes> : fmt::formatter<fmt_hex> {
    template <typename FormatContext>
    auto format(const ::bytes& s, FormatContext& ctx) const {
        return fmt::formatter<::fmt_hex>::format(::fmt_hex(bytes_view(s)), ctx);
    }
};

namespace std {

// Must be in std:: namespace, or ADL fails
std::ostream& operator<<(std::ostream& os, const bytes_view& b);

}

template<>
struct appending_hash<bytes> {
    template<typename Hasher>
    void operator()(Hasher& h, const bytes& v) const {
        feed_hash(h, v.size());
        h.update(reinterpret_cast<const char*>(v.cbegin()), v.size() * sizeof(bytes::value_type));
    }
};

template<>
struct appending_hash<bytes_view> {
    template<typename Hasher>
    void operator()(Hasher& h, bytes_view v) const {
        feed_hash(h, v.size());
        h.update(reinterpret_cast<const char*>(v.begin()), v.size() * sizeof(bytes_view::value_type));
    }
};

using bytes_view_hasher = simple_xx_hasher;

namespace std {
template <>
struct hash<bytes_view> {
    size_t operator()(bytes_view v) const {
        bytes_view_hasher h;
        appending_hash<bytes_view>{}(h, v);
        return h.finalize();
    }
};
} // namespace std

inline std::strong_ordering compare_unsigned(bytes_view v1, bytes_view v2) {
  auto size = std::min(v1.size(), v2.size());
  if (size) {
    auto n = memcmp(v1.begin(), v2.begin(), size);
    if (n) {
        return n <=> 0;
    }
  }
    return v1.size() <=> v2.size();
}
