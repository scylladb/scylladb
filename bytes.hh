/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "seastarx.hh"
#include <seastar/core/sstring.hh>
#include "utils/hashing.hh"
#include <optional>
#include <iosfwd>
#include <functional>
#include <compare>
#include "utils/mutable_view.hh"
#include <xxhash.h>

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

std::ostream& operator<<(std::ostream& os, const fmt_hex& hex);

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

struct bytes_view_hasher : public hasher {
    XXH64_state_t _state;
    bytes_view_hasher(uint64_t seed = 0) noexcept {
        XXH64_reset(&_state, seed);
    }
    void update(const char* ptr, size_t length) noexcept {
        XXH64_update(&_state, ptr, length);
    }
    size_t finalize() {
        return static_cast<size_t>(XXH64_digest(&_state));
    }
};

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
