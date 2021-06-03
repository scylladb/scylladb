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
 * Copyright (C) 2014-present ScyllaDB
 *
 * Modified by ScyllaDB
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
#include "seastarx.hh"
#include <iosfwd>
#include <functional>

namespace db {

sstring system_keyspace_name();

}

namespace cql3 {

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
struct hash<cql3::functions::function_name> {
    size_t operator()(const cql3::functions::function_name& x) const {
        return std::hash<sstring>()(x.keyspace) ^ std::hash<sstring>()(x.name);
    }
};

}
