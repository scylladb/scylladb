/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright 2014 Cloudius Systems
 *
 * Modified by Cloudius Systems
 */

#ifndef CQL3_FUNCTION_NAME_HH
#define CQL3_FUNCTION_NAME_HH

#include "core/sstring.hh"
#include "db/system_keyspace.hh"
#include <iostream>
#include <functional>

namespace cql3 {

namespace functions {

class function_name final {
public:
    sstring keyspace;
    sstring name;

    static function_name native_function(sstring name) {
        return function_name(db::system_keyspace::NAME, name);
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
struct hash<cql3::functions::function_name> {
    size_t operator()(const cql3::functions::function_name& x) const {
        return std::hash<sstring>()(x.keyspace) ^ std::hash<sstring>()(x.name);
    }
};

}

#endif
