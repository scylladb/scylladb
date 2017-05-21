/*
 * Copyright (C) 2015 ScyllaDB
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
#include <core/sstring.hh>
#include <boost/lexical_cast.hpp>
#include "exceptions/exceptions.hh"
#include "json.hh"
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
    caching_options(sstring k, sstring r) : _key_cache(k), _row_cache(r) {
        if ((k != "ALL") && (k != "NONE")) {
            throw exceptions::configuration_exception("Invalid key value: " + k); 
        }

        if ((r == "ALL") || (r == "NONE")) {
            return;
        } else {
            try {
                boost::lexical_cast<unsigned long>(r);
            } catch (boost::bad_lexical_cast& e) {
                throw exceptions::configuration_exception("Invalid key value: " + r);
            }
        }
    }

    friend class schema;
    caching_options() : _key_cache(default_key), _row_cache(default_row) {}
public:

    std::map<sstring, sstring> to_map() const {
        return {{ "keys", _key_cache }, { "rows_per_partition", _row_cache }};
    }

    sstring to_sstring() const {
        return json::to_json(to_map());
    }

    template<typename Map>
    static caching_options from_map(const Map & map) {
        sstring k = default_key;
        sstring r = default_row;

        for (auto& p : map) {
            if (p.first == "keys") {
                k = p.second;
            } else if (p.first == "rows_per_partition") {
                r = p.second;
            } else {
                throw exceptions::configuration_exception("Invalid caching option: " + p.first);
            }
        }
        return caching_options(k, r);
    }
    static caching_options from_sstring(const sstring& str) {
        return from_map(json::to_map(str));
    }

    bool operator==(const caching_options& other) const {
        return _key_cache == other._key_cache && _row_cache == other._row_cache;
    }
    bool operator!=(const caching_options& other) const {
        return !(*this == other);
    }
};



