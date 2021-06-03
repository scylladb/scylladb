
/*
 * Copyright 2019-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */


#include "like_matcher.hh"

#include <boost/regex/icu.hpp>
#include <boost/locale/encoding.hpp>
#include <string>

namespace {

using std::wstring;

/// Processes a new pattern character, extending re with the equivalent regex pattern.
void process_char(wchar_t c, wstring& re, bool& escaping) {
    if (c == L'\\' && !escaping) {
        escaping = true;
        return;
    }
    switch (c) {
    case L'.':
    case L'[':
    case L'\\':
    case L'*':
    case L'^':
    case L'$':
        // These are meant to match verbatim in LIKE, but they'd be special characters in regex --
        // must escape them.
        re.push_back(L'\\');
        re.push_back(c);
        break;
    case L'_':
    case L'%':
        if (escaping) {
            re.push_back(c);
        } else { // LIKE wildcard.
            re.push_back(L'.');
            if (c == L'%') {
                re.push_back(L'*');
            }
        }
        break;
    default:
        re.push_back(c);
        break;
    }
    escaping = false;
}

/// Returns a regex string matching the given LIKE pattern.
wstring regex_from_pattern(bytes_view pattern) {
    if (pattern.empty()) {
        return L"^$"; // Like SQL, empty pattern matches only empty text.
    }
    using namespace boost::locale::conv;
    wstring wpattern = utf_to_utf<wchar_t>(pattern.begin(), pattern.end(), stop);
    if (wpattern.back() == L'\\') {
        // Add an extra backslash, in case that last character is unescaped.  (If it is escaped, the
        // extra backslash will be ignored.)
        wpattern += L'\\';
    }
    wstring re;
    re.reserve(wpattern.size() * 2); // Worst case: every element is a special character and must be escaped.
    bool escaping = false;
    for (const wchar_t c : wpattern) {
        process_char(c, re, escaping);
    }
    return re;
}

} // anonymous namespace

class like_matcher::impl {
    bytes _pattern;
    boost::u32regex _re; // Performs pattern matching.
  public:
    explicit impl(bytes_view pattern);
    bool operator()(bytes_view text) const;
    void reset(bytes_view pattern);
  private:
    void init_re() {
        _re = boost::make_u32regex(regex_from_pattern(_pattern), boost::u32regex::basic | boost::u32regex::optimize);
    }
};

like_matcher::impl::impl(bytes_view pattern) : _pattern(pattern) {
    init_re();
}

bool like_matcher::impl::operator()(bytes_view text) const {
    return boost::u32regex_match(text.begin(), text.end(), _re);
}

void like_matcher::impl::reset(bytes_view pattern) {
    if (pattern != _pattern) {
        _pattern = bytes(pattern);
        init_re();
    }
}

like_matcher::like_matcher(bytes_view pattern)
        : _impl(std::make_unique<impl>(pattern)) {
}

like_matcher::~like_matcher() = default;

like_matcher::like_matcher(like_matcher&& that) noexcept = default;

bool like_matcher::operator()(bytes_view text) const {
    return _impl->operator()(text);
}

void like_matcher::reset(bytes_view pattern) {
    return _impl->reset(pattern);
}
