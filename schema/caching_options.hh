/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once
#include <seastar/core/sstring.hh>
#include <map>
#include "seastarx.hh"

class schema;

class caching_options {
    // For Origin, the default value for the row is "NONE". However, since our
    // row_cache will cache both keys and rows, we will default to ALL.
    //
    // FIXME: We don't yet make any changes to our caching policies based on
    // this (and maybe we shouldn't)
    static constexpr auto default_key = "ALL";
    static constexpr auto default_row = "ALL";

    sstring _key_cache;
    sstring _row_cache;
    bool _enabled = true;
    caching_options(sstring k, sstring r, bool enabled);

    friend class schema;
    caching_options();
public:
    bool enabled() const {
        return _enabled;
    }

    std::map<sstring, sstring> to_map() const;

    sstring to_sstring() const;

    static caching_options get_disabled_caching_options();
    static caching_options from_map(const std::map<sstring, sstring>& map);
    static caching_options from_sstring(const sstring& str);

    bool operator==(const caching_options& other) const = default;
};
