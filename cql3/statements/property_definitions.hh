/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
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
    using list_type = std::vector<sstring>;
    using extended_map_type = std::map<sstring, std::variant<sstring, list_type>>;
    using value_type = std::variant<sstring, extended_map_type>;
    using properties_map_type = std::unordered_map<sstring, value_type>;
protected:
#if 0
    protected static final Logger logger = LoggerFactory.getLogger(PropertyDefinitions.class);
#endif

    mutable properties_map_type _properties;

    property_definitions();
public:
    void add_property(const sstring& name, sstring value);

    void add_property(const sstring& name, const map_type& value) {
        add_property(name, to_extended_map(value));
    }

    void add_property(const sstring& name, const extended_map_type& value);

    void validate(const std::set<sstring>& keywords, const std::set<sstring>& exts = {}, const std::set<sstring>& obsolete = {}) const;

protected:
    std::optional<sstring> get_simple(const sstring& name) const;

    void remove_from_map_if_exists(const sstring& name, const sstring& key) const;
public:
    bool has_property(const sstring& name) const;

    std::optional<value_type> get(const sstring& name) const;

    std::optional<extended_map_type> get_extended_map(const sstring& name) const;
    std::optional<map_type> get_map(const sstring& name) const;

    static map_type to_simple_map(const extended_map_type&);
    static extended_map_type to_extended_map(const map_type&);

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

    size_t count() const {
        return _properties.size();
    }
};

}

}
