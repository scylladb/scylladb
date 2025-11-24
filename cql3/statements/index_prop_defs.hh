/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#pragma once

#include "cql3/statements/view_prop_defs.hh"
#include "property_definitions.hh"
#include <seastar/core/sstring.hh>
#include "schema/schema_fwd.hh"

#include <unordered_map>
#include <optional>

typedef std::unordered_map<sstring, sstring> index_options_map;

namespace cql3 {

namespace statements {

class index_specific_prop_defs : public property_definitions {
public:
    static constexpr auto KW_OPTIONS = "options";

    bool is_custom = false;
    std::optional<sstring> custom_class;
    // The only assumption about the value of `index_version` should be that it is different for every index.
    std::optional<table_schema_version> index_version;

    void validate() const;
    index_options_map get_raw_options() const;
    index_options_map get_options() const;
};

struct index_prop_defs : public view_prop_defs {
    /// Extract all of the index-specific properties to `target`.
    ///
    /// If there's a property at an index-specific key, and if `target` already has
    /// a value at that key, that value will be replaced.
    void extract_index_specific_properties_to(index_specific_prop_defs& target);

    /// Turns this object into an object of type `view_prop_defs`, as if moved.
    ///
    /// Precondition: the object MUST NOT contain any index-specific property.
    view_prop_defs into_view_prop_defs() &&;
};

}
}

