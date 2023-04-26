/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "caching_options.hh"
#include <boost/lexical_cast.hpp>
#include <map>
#include "exceptions/exceptions.hh"
#include "utils/rjson.hh"

caching_options::caching_options(sstring k, sstring r, bool enabled)
        : _key_cache(k), _row_cache(r), _enabled(enabled) {
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

caching_options::caching_options()
        : _key_cache(default_key), _row_cache(default_row) {
}

std::map<sstring, sstring>
caching_options::to_map() const {
    std::map<sstring, sstring> res = {{ "keys", _key_cache },
            { "rows_per_partition", _row_cache }};
    if (!_enabled) {
        res.insert({"enabled", "false"});
    }
    return res;
}

sstring
caching_options::to_sstring() const {
    return rjson::print(rjson::from_string_map(to_map()));
}

caching_options
caching_options::get_disabled_caching_options() {
    return caching_options("NONE", "NONE", false);
}

caching_options
caching_options::from_map(const std::map<sstring, sstring>& map) {
    sstring k = default_key;
    sstring r = default_row;
    bool e = true;

    for (auto& p : map) {
        if (p.first == "keys") {
            k = p.second;
        } else if (p.first == "rows_per_partition") {
            r = p.second;
        } else if (p.first == "enabled") {
            e = p.second == "true";
        } else {
            throw exceptions::configuration_exception(format("Invalid caching option: {}", p.first));
        }
    }
    return caching_options(k, r, e);
}

caching_options
caching_options::from_sstring(const sstring& str) {
    return from_map(rjson::parse_to_map<std::map<sstring, sstring>>(str));
}
