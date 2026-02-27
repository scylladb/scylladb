/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "utils/fragment_range.hh"
#include "utils/managed_bytes.hh"
#include "bytes_ostream.hh"

// A thin wrapper over `managed_bytes` representing a fragmented UTF-8 encoded string.
class managed_string {
private:
    managed_bytes _impl;

private:
    managed_string(managed_bytes mb) : _impl(std::move(mb)) {}

public:
    managed_string(managed_string&&) noexcept = default;
    managed_string(std::string_view sv)
        : _impl(bytes_view(reinterpret_cast<const int8_t*>(sv.data()), sv.size()))
    {}
    managed_string& operator=(managed_string&&) noexcept = default;

    // Precondition: the passed argument must represent a valid UTF-8 string.
    static managed_string from_managed_bytes_unsafe(managed_bytes mb) {
        return managed_string(std::move(mb));
    }

    bool operator==(const managed_string&) const = default;

    std::strong_ordering operator<=>(const managed_string& other) const {
        auto lv = managed_bytes_view(_impl);
        auto rv = managed_bytes_view(other._impl);
        return compare_unsigned(lv, rv);
    }

    template <typename Self>
    decltype(auto) as_managed_bytes(this Self&& self) {
        return std::forward_like<Self>(self._impl);
    }

    sstring linearize() const {
        sstring result(sstring::initialized_later{}, _impl.size());
        size_t offset = 0;

        for (auto&& fragment : fragment_range(managed_bytes_view(_impl))) {
            std::string_view char_view = to_string_view(fragment);
            std::ranges::copy(char_view, result.begin() + offset);
            offset += fragment.size();
        }

        return result;
    }
};

template <> struct fmt::formatter<managed_string> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const managed_string& b, FormatContext& ctx) const {
        auto view = managed_bytes_view(b.as_managed_bytes());
        auto out = ctx.out();

        for (auto&& fragment : fragment_range(view)) {
            std::string_view sv = to_string_view(fragment);
            out = fmt::format_to(out, "{}", sv);
        }

        return out;
    }
};
inline std::ostream& operator<<(std::ostream& os, const managed_string& b) {
    fmt::print(os, "{}", b);
    return os;
}

// A thin wrapper over `bytes_ostream` with a promise that it corresponds
// to actual UTF-8 characters, not just generic bytes.
class fragmented_ostringstream {
private:
    bytes_ostream _impl;

public:
    struct iter {
        fragmented_ostringstream& stream;

        iter& operator=(char c) {
            stream.write(c);
            return *this;
        }

        iter& operator*() { return *this; }
        iter& operator++() { return *this; }
        iter& operator++(int) { return *this; }
    };

public:
    [[gnu::always_inline]]
    void write(std::string_view sv) {
        _impl.write(sv.data(), sv.size());
    }
    [[gnu::always_inline]]
    void write(char c) {
        _impl.write(bytes_view(reinterpret_cast<typename bytes_ostream::value_type*>(&c), 1));
    }

    fragmented_ostringstream& operator<<(std::string sv) {
        write(sv);
        return *this;
    }
    fragmented_ostringstream& operator<<(char c) {
        write(c);
        return *this;
    }
    fragmented_ostringstream& operator<<(const managed_string& ms) {
        for (auto&& fragment : fragment_range(managed_bytes_view(ms.as_managed_bytes()))) {
            _impl.write(fragment);
        }
        return *this;
    }

    iter to_iter() noexcept {
        return iter {*this};
    }

    managed_string to_managed_string() && {
        return managed_string::from_managed_bytes_unsafe(std::move(_impl).to_managed_bytes());
    }
};
