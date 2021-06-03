/*
 * Copyright (C) 2015-present ScyllaDB
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

    bool operator==(const caching_options& other) const;
    bool operator!=(const caching_options& other) const;
};



