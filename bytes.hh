/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#pragma once

#include "core/sstring.hh"
#include <experimental/optional>
#include <iosfwd>
#include <functional>

using bytes = basic_sstring<int8_t, uint32_t, 31>;
using bytes_view = std::experimental::basic_string_view<int8_t>;
using bytes_opt = std::experimental::optional<bytes>;
using sstring_view = std::experimental::string_view;

namespace std {

template <>
struct hash<bytes_view> {
    size_t operator()(bytes_view v) const {
        return hash<sstring_view>()({reinterpret_cast<const char*>(v.begin()), v.size()});
    }
};

}

bytes from_hex(sstring_view s);
sstring to_hex(bytes_view b);
sstring to_hex(const bytes& b);
sstring to_hex(const bytes_opt& b);

std::ostream& operator<<(std::ostream& os, const bytes& b);
std::ostream& operator<<(std::ostream& os, const bytes_opt& b);

namespace std {

// Must be in std:: namespace, or ADL fails
std::ostream& operator<<(std::ostream& os, const bytes_view& b);

}

