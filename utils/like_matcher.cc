
/*
 * Copyright 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */


#include "like_matcher.hh"

#include <boost/regex/icu.hpp>
#include <boost/locale/encoding.hpp>
#include <functional>
#include <optional>
#include <span>
#include <string>
#include <vector>

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
    using searcher = std::boyer_moore_horspool_searcher<bytes_view::const_iterator>;

    bytes _pattern;
    // Set iff the pattern contains an unescaped '_', which needs code-point awareness.
    std::optional<boost::u32regex> _re;
    // Otherwise, the pattern is a list of literal segments separated by '%'.  The text must
    // start with the first segment, end with the last, and contain the ones in between, in
    // order and without overlap.  Since UTF-8 is self-synchronizing, bytewise comparison is
    // equivalent to comparing code points.
    bytes _literals; // Unescaped pattern characters; _segments point into it.
    std::vector<bytes_view> _segments;
    std::vector<searcher> _middle_searchers; // For non-empty segments other than first and last.
  public:
    explicit impl(bytes_view pattern);
    bool operator()(bytes_view text) const;
    void reset(bytes_view pattern);
  private:
    void init();
    bool match_segments(bytes_view text) const;
};

like_matcher::impl::impl(bytes_view pattern) : _pattern(pattern) {
    init();
}

void like_matcher::impl::init() {
    _re.reset();
    _middle_searchers.clear();
    _segments.clear();
    // Unescaping never lengthens the pattern, so _literals is never reallocated and views into it remain valid.
    _literals = bytes(bytes::initialized_later(), _pattern.size());
    auto out = _literals.begin();
    auto segment_start = out;
    auto end_segment = [&] {
        _segments.emplace_back(segment_start, out);
        segment_start = out;
    };
    bool escaping = false;
    for (const auto c : _pattern) {
        if (escaping) {
            *out++ = c;
            escaping = false;
        } else if (c == '\\') {
            escaping = true;
        } else if (c == '%') {
            end_segment();
        } else if (c == '_') {
            _segments.clear();
            _re = boost::make_u32regex(regex_from_pattern(_pattern), boost::u32regex::basic | boost::u32regex::optimize);
            return;
        } else {
            *out++ = c;
        }
    }
    if (escaping) {
        // Unescaped trailing backslash matches itself.
        *out++ = '\\';
    }
    end_segment();
    if (_segments.size() > 2) {
        _middle_searchers.reserve(_segments.size() - 2);
        for (auto seg : std::span(_segments).subspan(1, _segments.size() - 2)) {
            if (!seg.empty()) {
                _middle_searchers.emplace_back(seg.begin(), seg.end());
            }
        }
    }
}

bool like_matcher::impl::match_segments(bytes_view text) const {
    if (_segments.size() == 1) {
        return text == _segments.front();
    }
    const auto prefix = _segments.front();
    const auto suffix = _segments.back();
    if (text.size() < prefix.size() + suffix.size() || !text.starts_with(prefix) || !text.ends_with(suffix)) {
        return false;
    }
    auto it = text.begin() + prefix.size();
    const auto end = text.end() - suffix.size();
    // Matching each segment at its leftmost position leaves the most room for the ones that follow.
    for (const auto& search : _middle_searchers) {
        auto [found, found_end] = search(it, end);
        if (found == end) {
            return false;
        }
        it = found_end;
    }
    return true;
}

bool like_matcher::impl::operator()(bytes_view text) const {
    if (_re) {
        return boost::u32regex_match(text.begin(), text.end(), *_re);
    }
    return match_segments(text);
}

void like_matcher::impl::reset(bytes_view pattern) {
    if (pattern != _pattern) {
        _pattern = bytes(pattern);
        init();
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
