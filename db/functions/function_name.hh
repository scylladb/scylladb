/*
 * Copyright (C) 2022-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <seastar/core/sstring.hh>
#include "seastarx.hh"
#include <iosfwd>
#include <functional>

namespace db {

sstring system_keyspace_name();

namespace functions {

class function_name final {
public:
    sstring keyspace;
    sstring name;

    static function_name native_function(sstring name) {
        return function_name(db::system_keyspace_name(), name);
    }

    function_name() = default; // for ANTLR
    function_name(sstring keyspace, sstring name)
            : keyspace(std::move(keyspace)), name(std::move(name)) {
    }

    function_name as_native_function() const {
        return native_function(name);
    }

    bool has_keyspace() const {
        return !keyspace.empty();
    }

    bool operator==(const function_name& x) const {
        return keyspace == x.keyspace && name == x.name;
    }
};

inline
std::ostream& operator<<(std::ostream& os, const function_name& fn) {
    if (!fn.keyspace.empty()) {
        os << fn.keyspace << ".";
    }
    return os << fn.name;
}

}
}




namespace std {

template <>
struct hash<db::functions::function_name> {
    size_t operator()(const db::functions::function_name& x) const {
        return std::hash<sstring>()(x.keyspace) ^ std::hash<sstring>()(x.name);
    }
};

}
