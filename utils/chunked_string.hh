/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "utils/managed_bytes.hh"

namespace utils {

inline managed_bytes_view to_managed_bytes_view(std::string_view str) {
    return managed_bytes_view(bytes_view(reinterpret_cast<const signed char*>(str.data()), str.size()));
}

inline std::string_view to_string_view(bytes_view bv) {
    return std::string_view(reinterpret_cast<const char*>(bv.data()), bv.size());
}

class chunked_string_view;

/// Facade for managed_bytes for use-cases where the stored data is known to be text.
///
/// Hides the annoying reinterpret casts involved in converting between
/// string_view and bytes_view, and provides some convenient constructors and
/// operators for string literals and std::string_view.
class chunked_string {
    managed_bytes _data;

public:
    chunked_string() = default;

    explicit chunked_string(chunked_string_view o);
    explicit chunked_string(managed_bytes_view data) : _data(data) {}

    chunked_string(std::string_view data) : _data(to_managed_bytes_view(data)) {}
    chunked_string(sstring data) : chunked_string(std::string_view(data)) {}
    chunked_string(std::string data) : chunked_string(std::string_view(data)) {}
    chunked_string(const char* data) : chunked_string(std::string_view(data)) {}

    // prevent accidental implicit conversions from int and nullptr
    chunked_string(std::nullptr_t) = delete;
    chunked_string(int) = delete;

    chunked_string(chunked_string&&) = default;
    chunked_string(const chunked_string&) = default;

    chunked_string& operator=(chunked_string&&) = default;
    chunked_string& operator=(const chunked_string&) = default;

    chunked_string& operator=(chunked_string_view o);

    chunked_string& operator=(std::string_view o) {
        _data = managed_bytes(to_managed_bytes_view(o));
        return *this;
    }

    chunked_string& operator=(const char* o) {
        *this = std::string_view(o);
        return *this;
    }

    bool operator==(const chunked_string&) const = default;

    bool operator==(managed_bytes_view o) const {
        return _data.size() == o.size() && compare_unsigned(managed_bytes_view(_data), o) == 0;
    }

    bool operator==(std::string_view o) const {
        return *this == to_managed_bytes_view(o);
    }

    bool operator==(const char* o) const {
        return *this == std::string_view(o);
    }

    std::string_view::value_type operator[](size_t index) const {
        return _data[index];
    }

    bool empty() const {
        return _data.empty();
    }

    size_t size() const {
        return _data.size();
    }

    const managed_bytes& data() const {
        return _data;
    }

    /// Materialize the whole string in a single buffer, then invoke the given function on it.
    ///
    /// Think before using! It will possibly cause oversized allocations.
    template <std::invocable<std::string_view> Func>
    std::invoke_result_t<Func, std::string_view> with_linearized(Func&& func) const {
        return _data.with_linearized([&] (bytes_view bv) {
            return func(to_string_view(bv));
        });
    }

    /// Materialize the whole string in a single buffer.
    ///
    /// Think before using! It will possibly cause oversized allocations.
    sstring linearize() const {
        return _data.with_linearized([&] (bytes_view bv) {
            return sstring(to_string_view(bv));
        });
    }
};

class chunked_string_view {
    managed_bytes_view _data;

public:
    chunked_string_view() = default;
    chunked_string_view(const chunked_string& str) : _data(str.data()) {}
    chunked_string_view(std::string_view data) : _data(to_managed_bytes_view(data)) {}
    chunked_string_view(sstring data) : chunked_string_view(std::string_view(data)) {}
    chunked_string_view(std::string data) : chunked_string_view(std::string_view(data)) {}
    chunked_string_view(const char* data) : chunked_string_view(std::string_view(data)) {}

    // Explicit to require the caller to acknowledge that the raw data is indeed text.
    explicit chunked_string_view(managed_bytes_view data) : _data(data) {}

    // prevent accidental implicit conversions from int and nullptr
    chunked_string_view(std::nullptr_t) = delete;
    chunked_string_view(int) = delete;

    chunked_string_view(chunked_string_view&&) = default;
    chunked_string_view(const chunked_string_view&) = default;

    chunked_string_view& operator=(chunked_string_view&&) = default;
    chunked_string_view& operator=(const chunked_string_view&) = default;

    chunked_string_view& operator=(chunked_string o) {
        _data = o.data();
        return *this;
    }

    chunked_string_view& operator=(std::string_view o) {
        _data = to_managed_bytes_view(o);
        return *this;
    }

    chunked_string_view& operator=(const char* o) {
        *this = std::string_view(o);
        return *this;
    }

    bool operator==(const chunked_string_view&) const = default;

    bool operator==(std::string_view o) const {
        return *this == chunked_string_view(to_managed_bytes_view(o));
    }

    bool operator==(const char* o) const {
        return *this == std::string_view(o);
    }

    std::string_view::value_type operator[](size_t index) const {
        return _data[index];
    }

    bool empty() const {
        return _data.empty();
    }

    size_t size() const {
        return _data.size();
    }

    void remove_prefix(size_t n) {
        _data.remove_prefix(n);
    }

    // Return the first fragment of the string, with "..." appended if the string
    // is longer than that.
    // Useful for logging and error messages, which need a sample of the string
    // but don't want to cause oversized allocations by linearizing it.
    sstring ellipsize(size_t size = 100) const {
        if (_data.size() <= size) {
            return linearize();
        }
        return _data.substr(0, size).with_linearized([] (bytes_view bv) {
            const std::string_view ellipsis = "...";
            sstring str(sstring::initialized_later{}, bv.size() + ellipsis.size());
            std::ranges::copy(bv, str.begin());
            std::ranges::copy(ellipsis, str.begin() + bv.size());
            return str;
        });
    }

    managed_bytes_view data() const {
        return _data;
    }

    /// Materialize the whole string in a single buffer, then invoke the given function on it.
    ///
    /// Think before using! It will possibly cause oversized allocations.
    template <std::invocable<std::string_view> Func>
    std::invoke_result_t<Func, std::string_view> with_linearized(Func&& func) const {
        return _data.with_linearized([&] (bytes_view bv) {
            return func(to_string_view(bv));
        });
    }

    /// Materialize the whole string in a single buffer.
    ///
    /// Think before using! It will possibly cause oversized allocations.
    sstring linearize() const {
        return _data.with_linearized([&] (bytes_view bv) {
            return sstring(to_string_view(bv));
        });
    }
};

inline chunked_string::chunked_string(chunked_string_view o) : _data(o.data())
{ }

inline chunked_string& chunked_string::operator=(chunked_string_view o) {
    _data = managed_bytes(o.data());
    return *this;
}

} // namespace utils
