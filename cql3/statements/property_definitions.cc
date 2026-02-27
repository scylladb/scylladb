/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#include <ranges>

#include <seastar/core/format.hh>
#include <stdexcept>
#include "cql3/statements/property_definitions.hh"
#include "exceptions/exceptions.hh"
#include "utils/overloaded_functor.hh"

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

void property_definitions::add_property(const sstring& name, const extended_map_type& value) {
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

std::optional<property_definitions::extended_map_type> property_definitions::get_extended_map(const sstring& name) const {
    auto it = _properties.find(name);
    if (it == _properties.end()) {
        return std::nullopt;
    }
    try {
        return std::get<extended_map_type>(it->second);
    } catch (const std::bad_variant_access& e) {
        throw exceptions::syntax_exception(format("Invalid value for property '{}'. It should be a map.", name));
    }
}

std::optional<property_definitions::map_type> property_definitions::get_map(const sstring& name) const {
    auto xmap = get_extended_map(name);
    if (!xmap) {
        return std::nullopt;
    }
    return to_simple_map(std::move(*xmap));
}

property_definitions::map_type property_definitions::to_simple_map(const extended_map_type& xmap) {
    return xmap | std::views::transform([](const auto& x) {
        // Convert each pair to a string key and value
        try {
            return std::make_pair(x.first, std::get<sstring>(x.second));
        } catch (const std::bad_variant_access& e) {
            throw exceptions::syntax_exception(seastar::format("Invalid map value '{}' for key '{}'. It should be a simple string.", std::get<list_type>(x.second), x.first));
        }
    }) | std::ranges::to<map_type>();
}

property_definitions::extended_map_type property_definitions::to_extended_map(const map_type& map) {
    return map | std::ranges::to<extended_map_type>();
}

bool property_definitions::has_property(const sstring& name) const {
    return _properties.contains(name);
}

property_definitions::value_type property_definitions::extract_property(const sstring& name) {
    auto it = _properties.find(name);

    if (it == _properties.end()) {
        throw std::out_of_range{std::format("No property of name '{}'", std::string_view(name))};
    }

    value_type result = std::move(it->second);
    _properties.erase(it);
    return result;
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

void property_definitions::remove_property(const sstring& name) const {
    _properties.erase(name);
}

void property_definitions::remove_from_map_if_exists(const sstring& name, const sstring& key) const
{
    auto it = _properties.find(name);
    if (it == _properties.end()) {
        return;
    }
    try {
        auto map = std::get<extended_map_type>(it->second);
        map.erase(key);
        _properties[name] = map;
    } catch (const std::bad_variant_access& e) {
        throw exceptions::syntax_exception(format("Invalid value for property '{}'. It should be a map.", name));
    }
}

/// Converts extended map into a flat map.
///
/// Values which are lists are represented as multiple entries in the map
/// with the list index appended to the key, with ':' as a separator.
/// Empty list is represented as a single entry with index -1 and empty string as value.
///
/// For example:
///
///    {'dc1': '3', 'dc2': ['rack1', 'rack2'], 'dc3': []}
///
/// has a flattened representation of:
///
///   {'dc1': '3', 'dc2:0': 'rack1', 'dc2:1': 'rack2', 'dc3:-1': ''}
///
property_definitions::map_type to_flattened_map(const property_definitions::extended_map_type& in) {
    property_definitions::map_type out;
    for (const auto& [in_key, in_value]: in) {
        if (in_key.find(':') != sstring::npos) {
            throw std::invalid_argument(fmt::format("key '{}' contains reserved character ':'", in_key));
        }
        std::visit(overloaded_functor{
            [&] (const sstring& value) {
                out[in_key] = value;
            },
            [&] (const std::vector<sstring>& list) {
                if (list.empty()) {
                    out[fmt::format("{}:{}", in_key, -1)] = "";
                } else {
                    // flatten the rack list in multiple entries
                    for (size_t i = 0; i < list.size(); ++i) {
                        const auto& v = list[i];
                        out[fmt::format("{}:{}", in_key, i)] = v;
                    }
                }
            }
        }, in_value);
    }
    return out;
}

property_definitions::extended_map_type from_flattened_map(const property_definitions::map_type& in) {
    property_definitions::extended_map_type out;
    for (const auto& [key, value] : in) {
        auto pos = key.find(':');
        if (pos == sstring::npos) {
            out.emplace(key, value);
        } else {
            auto dc = key.substr(0, pos);
            auto index = std::stol(key.substr(pos + 1));
            auto [it, empty] = out.try_emplace(dc, std::vector<sstring>());
            auto& vec = std::get<std::vector<sstring>>(it->second);
            if (index >= 0) {
                if (vec.size() <= size_t(index)) {
                    vec.resize(index + 1);
                }
                vec[index] = value;
            }
        }
    }
    return out;
}

}

}
