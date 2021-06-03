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

#include "cql3/statements/property_definitions.hh"
#include "exceptions/exceptions.hh"

namespace cql3 {

namespace statements {

property_definitions::property_definitions()
    : _properties{}
{ }

void property_definitions::add_property(const sstring& name, sstring value) {
    if (auto [ignored, added] = _properties.try_emplace(name, value); !added) {
        throw exceptions::syntax_exception(format("Multiple definition for property '{}'", name));
    }
}

void property_definitions::add_property(const sstring& name, const std::map<sstring, sstring>& value) {
    if (auto [ignored, added] = _properties.try_emplace(name, value); !added) {
        throw exceptions::syntax_exception(format("Multiple definition for property '{}'", name));
    }
}

void property_definitions::validate(const std::set<sstring>& keywords, const std::set<sstring>& exts, const std::set<sstring>& obsolete) const {
    for (auto&& kv : _properties) {
        auto&& name = kv.first;
        if (keywords.contains(name) || exts.contains(name)) {
            continue;
        }
        if (obsolete.contains(name)) {
#if 0
            logger.warn("Ignoring obsolete property {}", name);
#endif
        } else {
            throw exceptions::syntax_exception(format("Unknown property '{}'", name));
        }
    }
}

std::optional<sstring> property_definitions::get_simple(const sstring& name) const {
    auto it = _properties.find(name);
    if (it == _properties.end()) {
        return std::nullopt;
    }
    try {
        return std::get<sstring>(it->second);
    } catch (const std::bad_variant_access& e) {
        throw exceptions::syntax_exception(format("Invalid value for property '{}'. It should be a string", name));
    }
}

std::optional<std::map<sstring, sstring>> property_definitions::get_map(const sstring& name) const {
    auto it = _properties.find(name);
    if (it == _properties.end()) {
        return std::nullopt;
    }
    try {
        return std::get<map_type>(it->second);
    } catch (const std::bad_variant_access& e) {
        throw exceptions::syntax_exception(format("Invalid value for property '{}'. It should be a map.", name));
    }
}

bool property_definitions::has_property(const sstring& name) const {
    return _properties.contains(name);
}

std::optional<property_definitions::value_type> property_definitions::get(const sstring& name) const {
    if (auto it = _properties.find(name); it != _properties.end()) {
        return it->second;
    }
    return std::nullopt;
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

double property_definitions::to_double(sstring key, std::optional<sstring> value, double default_value) {
    if (value) {
        auto val = value.value();
        try {
            return std::stod(val);
        } catch (const std::exception& e) {
            throw exceptions::syntax_exception(format("Invalid double value {} for '{}'", val, key));
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

int32_t property_definitions::to_int(sstring key, std::optional<sstring> value, int32_t default_value) {
    if (value) {
        auto val = value.value();
        try {
            return std::stoi(val);
        } catch (const std::exception& e) {
            throw exceptions::syntax_exception(format("Invalid integer value {} for '{}'", val, key));
        }
    } else {
        return default_value;
    }
}

long property_definitions::to_long(sstring key, std::optional<sstring> value, long default_value) {
    if (value) {
        auto val = value.value();
        try {
            return std::stol(val);
        } catch (const std::exception& e) {
            throw exceptions::syntax_exception(format("Invalid long value {} for '{}'", val, key));
        }
    } else {
        return default_value;
    }
}

void property_definitions::remove_from_map_if_exists(const sstring& name, const sstring& key) const
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
        throw exceptions::syntax_exception(format("Invalid value for property '{}'. It should be a map.", name));
    }
}

}

}
