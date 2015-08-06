/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#pragma once
#include <core/sstring.hh>
#include <boost/lexical_cast.hpp>
#include "exceptions/exceptions.hh"
#include "json.hh"

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

        try {
            boost::lexical_cast<unsigned long>(r);
        } catch (boost::bad_lexical_cast& e) {
            if ((r != "ALL") && (r != "NONE")) {
                throw exceptions::configuration_exception("Invalid key value: " + k); 
            }
        }
    }

    friend class schema;
    caching_options() : _key_cache(default_key), _row_cache(default_row) {}
public:

    sstring to_sstring() const {
        return json::to_json(std::map<sstring, sstring>({{ "keys", _key_cache }, { "rows_per_partition", _row_cache }}));
    }

    static caching_options from_sstring(const sstring& str) {
        auto map = json::to_map(str);
        if (map.size() > 2) {
            throw exceptions::configuration_exception("Invalid map: " + str); 
        }
        sstring k;
        sstring r;
        if (map.count("keys")) {
            k = map.at("keys");
        } else {
            k = default_key;
        }

        if (map.count("rows_per_partition")) {
            r = map.at("rows_per_partition");
        } else {
            r = default_row;
        }
        return caching_options(k, r);
    }
};



