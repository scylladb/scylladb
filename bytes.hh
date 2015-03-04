/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#pragma once

#include "core/sstring.hh"
#include <experimental/optional>
#include <iosfwd>

// FIXME: should be int8_t
using bytes = basic_sstring<char, uint32_t, 31>;
using bytes_view = std::experimental::string_view;
using bytes_opt = std::experimental::optional<bytes>;
using sstring_view = std::experimental::string_view;

bytes from_hex(sstring_view s);
sstring to_hex(const bytes& b);
sstring to_hex(const bytes_opt& b);

std::ostream& operator<<(std::ostream& os, const bytes& b);

