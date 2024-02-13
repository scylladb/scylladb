/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/sstring.hh>

using namespace seastar;

namespace utils {

template <typename tag, typename char_type, typename Size, Size max_size, bool NulTerminate>
class basic_frozen_sstring {
    using basic_sstring =  basic_sstring<char_type, Size, max_size, NulTerminate>;
    using basic_string_view = std::basic_string_view<char_type>;
    using basic_string = std::basic_string<char_type>;

    basic_sstring _str;
    size_t _hash;

public:
    using value_type = basic_sstring::value_type;
    using traits_type = basic_sstring::traits_type;
    using allocator_type = basic_sstring::allocator_type;
    using reference = basic_sstring::const_reference;
    using const_reference = basic_sstring::const_reference;
    using pointer = basic_sstring::const_pointer;
    using const_pointer = basic_sstring::const_pointer;
    using iterator = basic_sstring::const_iterator;
    using const_iterator = basic_sstring::const_iterator;
    using difference_type = basic_sstring::difference_type;
    using size_type = basic_sstring::size_type;

    static constexpr size_type npos = basic_sstring::npos;

    basic_frozen_sstring() noexcept : _str(), _hash(0) {}
    basic_frozen_sstring(const basic_frozen_sstring& o) : _str(o._str) , _hash(o._hash) {}
    basic_frozen_sstring(basic_frozen_sstring&& o) noexcept : _str(std::move(o._str)), _hash(std::exchange(o._hash, 0)) {}

    explicit basic_frozen_sstring(const char_type* s) noexcept : _str(s), _hash(std::hash<basic_sstring>()(_str)) {}
    explicit basic_frozen_sstring(const char_type* s, size_type size) noexcept : _str(s, size), _hash(std::hash<basic_sstring>()(_str)) {}
    explicit basic_frozen_sstring(const basic_string_view& x) : _str(x), _hash(std::hash<basic_sstring>()(_str)) {}
    explicit basic_frozen_sstring(const basic_string& x) : _str(x), _hash(std::hash<basic_sstring>()(_str)) {}
    explicit basic_frozen_sstring(const basic_sstring& x) : _str(x), _hash(std::hash<basic_sstring>()(_str)) {}
    explicit basic_frozen_sstring(basic_string&& x) : _str(std::move(x)), _hash(std::hash<basic_sstring>()(_str)) {}
    explicit basic_frozen_sstring(basic_sstring&& x) : _str(std::move(x)), _hash(std::hash<basic_sstring>()(_str)) {}

    basic_frozen_sstring& operator=(const basic_frozen_sstring& x) {
        if (this != &x) {
            _str = x._str;
            _hash = x._hash;
        }
        return *this;
    }
    basic_frozen_sstring& operator=(basic_frozen_sstring&& x) noexcept {
        if (this != &x) {
            _str = std::move(x._str);
            _hash = std::exchange(x._hash, 0);
        }
        return *this;
    }

    bool operator==(const basic_frozen_sstring& x) const noexcept {
        return hash() == x.hash() && str() == x.str();
    }

    constexpr std::strong_ordering operator<=>(const basic_frozen_sstring& x) const noexcept {
        return str() <=> x.str();
    }

    const basic_sstring& str() const noexcept { return _str; }
    const_pointer c_str() const noexcept { return str().c_str(); }
    const_pointer data() const noexcept { return str().data(); }
    size_t hash() const noexcept { return _hash; }

    bool empty() const noexcept { return str().empty(); }
    size_t size() const noexcept { return str().size(); }

private:
    auto format(fmt::format_context& ctx) const {
        return fmt::formatter<basic_sstring>().format(str(), ctx);
    }

    friend fmt::formatter<basic_frozen_sstring>;
};

using frozen_sstring = basic_frozen_sstring<struct untagged_frozen_sstring, char, uint32_t, 15, true>;

} // namespace utils

template <typename tag, typename char_type, typename size_type, size_type max_size, bool NulTerminate>
struct fmt::formatter<utils::basic_frozen_sstring<tag, char_type, size_type, max_size, NulTerminate>> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const utils::basic_frozen_sstring<tag, char_type, size_type, max_size, NulTerminate>& s, fmt::format_context& ctx) const {
        return s.format(ctx);
    }
};

namespace std {

template <typename tag, typename char_type, typename size_type, size_type max_size, bool NulTerminate>
struct hash<utils::basic_frozen_sstring<tag, char_type, size_type, max_size, NulTerminate>> {
    size_t operator()(const utils::basic_frozen_sstring<tag, char_type, size_type, max_size, NulTerminate>& s) const {
        return s.hash();
    }
};

template <typename tag, typename char_type, typename size_type, size_type max_size, bool NulTerminate>
std::ostream& operator<<(std::ostream& os, const utils::basic_frozen_sstring<tag, char_type, size_type, max_size, NulTerminate>& s) {
    return os << s.str();
}

} // namespace std
