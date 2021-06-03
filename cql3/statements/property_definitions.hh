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
 * Copyright (C) 2015-present ScyllaDB
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

#include <optional>
#include <cctype>
#include <map>
#include <set>
#include <variant>

#include "seastarx.hh"

namespace cql3 {

namespace statements {

class property_definitions {
public:
    using map_type = std::map<sstring, sstring>;
    using value_type = std::variant<sstring, map_type>;
protected:
#if 0
    protected static final Logger logger = LoggerFactory.getLogger(PropertyDefinitions.class);
#endif

    mutable std::unordered_map<sstring, value_type> _properties;

    property_definitions();
public:
    void add_property(const sstring& name, sstring value);

    void add_property(const sstring& name, const std::map<sstring, sstring>& value);

    void validate(const std::set<sstring>& keywords, const std::set<sstring>& exts = {}, const std::set<sstring>& obsolete = {}) const;

protected:
    std::optional<sstring> get_simple(const sstring& name) const;

    std::optional<std::map<sstring, sstring>> get_map(const sstring& name) const;

    void remove_from_map_if_exists(const sstring& name, const sstring& key) const;
public:
    bool has_property(const sstring& name) const;

    std::optional<value_type> get(const sstring& name) const;

    sstring get_string(sstring key, sstring default_value) const;

    // Return a property value, typed as a Boolean
    bool get_boolean(sstring key, bool default_value) const;

    // Return a property value, typed as a double
    double get_double(sstring key, double default_value) const;

    static double to_double(sstring key, std::optional<sstring> value, double default_value);

    // Return a property value, typed as an Integer
    int32_t get_int(sstring key, int32_t default_value) const;

    static int32_t to_int(sstring key, std::optional<sstring> value, int32_t default_value);

    static long to_long(sstring key, std::optional<sstring> value, long default_value);
};

}

}
