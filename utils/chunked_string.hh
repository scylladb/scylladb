/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
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

template <mutable_view> class chunked_string_basic_view;
using chunked_string_view = chunked_string_basic_view<mutable_view::no>;
using chunked_string_mutable_view = chunked_string_basic_view<mutable_view::yes>;

/// Facade for managed_bytes for use-cases where the stored data is known to be text.
///
/// Hides the annoying reinterpret casts involved in converting between
/// string_view and bytes_view, and provides some convenient constructors and
/// operators for string literals and std::string_view.
class chunked_string {
    managed_bytes _data;

public:
    chunked_string() = default;

    /// Tag type for constructing a chunked_string with an uninitialized buffer.
    struct initialized_later {};

    /// Allocate an uninitialized buffer of \p size bytes.
    /// The caller must fill every byte before the buffer is read.
    chunked_string(initialized_later, size_t size) : _data(managed_bytes::initialized_later{}, size) {}

    chunked_string(chunked_string_view o);
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

    bool operator==(std::string_view o) const {
        return _data.size() == o.size() && compare_unsigned(managed_bytes_view(_data), to_managed_bytes_view(o)) == 0;
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

    managed_bytes_mutable_view mutable_view() {
        return managed_bytes_mutable_view(_data);
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
    sstring linearize() const;
};

template <mutable_view is_mutable>
class chunked_string_basic_view {
    using bytes_view_type = std::conditional_t<is_mutable == mutable_view::yes, managed_bytes_mutable_view, managed_bytes_view>;
    bytes_view_type _data;

public:
    chunked_string_basic_view() = default;

    // Construct an immutable view from a chunked_string.
    chunked_string_basic_view(const chunked_string& str) requires (is_mutable == mutable_view::no)
        : _data(str.data()) {}

    // Construct a mutable view from a chunked_string.
    chunked_string_basic_view(chunked_string& str) requires (is_mutable == mutable_view::yes)
        : _data(str.mutable_view()) {}

    chunked_string_basic_view(std::string_view data) requires (is_mutable == mutable_view::no)
        : _data(to_managed_bytes_view(data)) {}
    chunked_string_basic_view(const sstring& data) requires (is_mutable == mutable_view::no)
        : chunked_string_basic_view(std::string_view(data)) {}
    chunked_string_basic_view(const std::string& data) requires (is_mutable == mutable_view::no)
        : chunked_string_basic_view(std::string_view(data)) {}
    chunked_string_basic_view(const char* data) requires (is_mutable == mutable_view::no)
        : chunked_string_basic_view(std::string_view(data)) {}

    // Explicit to require the caller to acknowledge that the raw data is indeed text.
    explicit chunked_string_basic_view(bytes_view_type data) : _data(data) {}

    // Implicit conversion from mutable to immutable.
    chunked_string_basic_view(const chunked_string_basic_view<mutable_view::yes>& o) requires (is_mutable == mutable_view::no)
        : _data(o.data()) {}

    // prevent accidental implicit conversions from int and nullptr
    chunked_string_basic_view(std::nullptr_t) = delete;
    chunked_string_basic_view(int) = delete;

    chunked_string_basic_view(chunked_string_basic_view&&) = default;
    chunked_string_basic_view(const chunked_string_basic_view&) = default;

    chunked_string_basic_view& operator=(chunked_string_basic_view&&) = default;
    chunked_string_basic_view& operator=(const chunked_string_basic_view&) = default;

    chunked_string_basic_view& operator=(std::string_view o) requires (is_mutable == mutable_view::no) {
        _data = to_managed_bytes_view(o);
        return *this;
    }

    chunked_string_basic_view& operator=(const char* o) requires (is_mutable == mutable_view::no) {
        *this = std::string_view(o);
        return *this;
    }

    bool operator==(const chunked_string_basic_view&) const = default;

    bool operator==(std::string_view o) const requires (is_mutable == mutable_view::no) {
        return *this == chunked_string_basic_view(to_managed_bytes_view(o));
    }

    bool operator==(const char* o) const requires (is_mutable == mutable_view::no) {
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

    chunked_string_basic_view substr(size_t pos, size_t count = std::string_view::npos) const {
        return chunked_string_basic_view(_data.substr(pos, count));
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
        return managed_bytes_view(_data).with_linearized([&] (bytes_view bv) {
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

    /// Byte-wise forward iterators yielding char values.
    ///
    /// Allows use with algorithms and APIs (such as boost::regex) that
    /// expect iterators over char.  char / int8_t are both single-byte
    /// integer types so the cast is always well-defined.
    using char_type = std::conditional_t<is_mutable == mutable_view::yes, char, const char>;
    using iterator = typename bytes_view_type::template byte_iterator<char_type>;
    using const_iterator = managed_bytes_view::byte_iterator<const char>;

    iterator begin() { return iterator(_data); }
    iterator end() { return iterator(); }
    const_iterator begin() const { return const_iterator(managed_bytes_view(_data)); }
    const_iterator end() const { return const_iterator(); }
};

using chunked_string_view = chunked_string_basic_view<mutable_view::no>;
using chunked_string_mutable_view = chunked_string_basic_view<mutable_view::yes>;

inline chunked_string::chunked_string(chunked_string_view o) : _data(o.data())
{ }

inline chunked_string& chunked_string::operator=(chunked_string_view o) {
    _data = managed_bytes(o.data());
    return *this;
}

/// Decode a hex-encoded fragmented string into a managed_bytes.
///
/// Equivalent to from_hex(std::string_view) but operates directly on a
/// chunked_string_view without requiring linearization.  Hex pairs that
/// straddle a fragment boundary are handled correctly via a carry-over nibble.
///
/// Throws std::invalid_argument if the total length is odd or the input
/// contains non-hex characters.
///
/// Place in global namespace, so it works as overload of existing from_hex() (from bytes.hh).
managed_bytes from_hex(utils::chunked_string_view view);

} // namespace utils
