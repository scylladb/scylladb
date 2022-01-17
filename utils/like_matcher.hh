/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "bytes.hh"
#include <memory>

/// Implements <code>text LIKE pattern</code>.
///
/// The pattern is a string of characters with two wildcards:
/// - '_' matches any single character
/// - '%' matches any substring (including an empty string)
/// - '\' escapes the next pattern character, so it matches verbatim
/// - any other pattern character matches itself
///
/// The whole text must match the pattern; thus <code>'abc' LIKE 'a'</code> doesn't match, but
/// <code>'abc' LIKE 'a%'</code> matches.
class like_matcher {
    class impl;
    std::unique_ptr<impl> _impl;
public:
    /// Compiles \c pattern and stores the result.
    ///
    /// \param pattern UTF-8 encoded pattern with wildcards '_' and '%'.
    explicit like_matcher(bytes_view pattern);

    like_matcher(like_matcher&&) noexcept; // Must be defined in .cc, where class impl is known.

    ~like_matcher();

    /// Runs the compiled pattern on \c text.
    ///
    /// \return true iff text matches constructor's pattern.
    bool operator()(bytes_view text) const;

    /// Resets pattern if different from the current one.
    void reset(bytes_view pattern);
};
