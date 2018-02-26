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
 * Copyright (C) 2015 ScyllaDB
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

#include "cql3/statements/property_definitions.hh"

namespace cql3 {

namespace statements {

property_definitions::property_definitions()
    : _properties{}
{ }

void property_definitions::add_property(const sstring& name, sstring value) {
    auto it = _properties.find(name);
    if (it != _properties.end()) {
        throw exceptions::syntax_exception(sprint("Multiple definition for property '%s'", name));
    }
    _properties.emplace(name, value);
}

void property_definitions::add_property(const sstring& name, const std::map<sstring, sstring>& value) {
    auto it = _properties.find(name);
    if (it != _properties.end()) {
        throw exceptions::syntax_exception(sprint("Multiple definition for property '%s'", name));
    }
    _properties.emplace(name, value);
}

void property_definitions::validate(const std::set<sstring>& keywords, const std::set<sstring>& exts, const std::set<sstring>& obsolete) {
    for (auto&& kv : _properties) {
        auto&& name = kv.first;
        if (keywords.count(name) || exts.count(name)) {
            continue;
        }
        if (obsolete.count(name)) {
#if 0
            logger.warn("Ignoring obsolete property {}", name);
#endif
        } else {
            throw exceptions::syntax_exception(sprint("Unknown property '%s'", name));
        }
    }
}

std::experimental::optional<sstring> property_definitions::get_simple(const sstring& name) const {
    auto it = _properties.find(name);
    if (it == _properties.end()) {
        return std::experimental::nullopt;
    }
    try {
        return std::get<sstring>(it->second);
    } catch (const std::bad_variant_access& e) {
        throw exceptions::syntax_exception(sprint("Invalid value for property '%s'. It should be a string", name));
    }
}

std::experimental::optional<std::map<sstring, sstring>> property_definitions::get_map(const sstring& name) const {
    auto it = _properties.find(name);
    if (it == _properties.end()) {
        return std::experimental::nullopt;
    }
    try {
        return std::get<map_type>(it->second);
    } catch (const std::bad_variant_access& e) {
        throw exceptions::syntax_exception(sprint("Invalid value for property '%s'. It should be a map.", name));
    }
}

bool property_definitions::has_property(const sstring& name) const {
    return _properties.find(name) != _properties.end();
}

sstring property_definitions::get_string(sstring key, sstring default_value) const {
    auto value = get_simple(key);
    if (value) {
        return value.value();
    } else {
        return default_value;
    }
}

// Return a property value, typed as a Boolean
bool property_definitions::get_boolean(sstring key, bool default_value) const {
    auto value = get_simple(key);
    if (value) {
        std::string s{value.value()};
        std::transform(s.begin(), s.end(), s.begin(), ::tolower);
        return s == "1" || s == "true" || s == "yes";
    } else {
        return default_value;
    }
}

// Return a property value, typed as a double
double property_definitions::get_double(sstring key, double default_value) const {
    auto value = get_simple(key);
    return to_double(key, value, default_value);
}

double property_definitions::to_double(sstring key, std::experimental::optional<sstring> value, double default_value) {
    if (value) {
        auto val = value.value();
        try {
            return std::stod(val);
        } catch (const std::exception& e) {
            throw exceptions::syntax_exception(sprint("Invalid double value %s for '%s'", val, key));
        }
    } else {
        return default_value;
    }
}

// Return a property value, typed as an Integer
int32_t property_definitions::get_int(sstring key, int32_t default_value) const {
    auto value = get_simple(key);
    return to_int(key, value, default_value);
}

int32_t property_definitions::to_int(sstring key, std::experimental::optional<sstring> value, int32_t default_value) {
    if (value) {
        auto val = value.value();
        try {
            return std::stoi(val);
        } catch (const std::exception& e) {
            throw exceptions::syntax_exception(sprint("Invalid integer value %s for '%s'", val, key));
        }
    } else {
        return default_value;
    }
}

long property_definitions::to_long(sstring key, std::experimental::optional<sstring> value, long default_value) {
    if (value) {
        auto val = value.value();
        try {
            return std::stol(val);
        } catch (const std::exception& e) {
            throw exceptions::syntax_exception(sprint("Invalid long value %s for '%s'", val, key));
        }
    } else {
        return default_value;
    }
}

void property_definitions::remove_from_map_if_exists(const sstring& name, const sstring& key)
{
    auto it = _properties.find(name);
    if (it == _properties.end()) {
        return;
    }
    try {
        auto map = std::get<map_type>(it->second);
        map.erase(key);
        _properties[name] = map;
    } catch (const std::bad_variant_access& e) {
        throw exceptions::syntax_exception(sprint("Invalid value for property '%s'. It should be a map.", name));
    }
}

}

}
