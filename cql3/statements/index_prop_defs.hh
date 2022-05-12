/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "property_definitions.hh"
#include <seastar/core/sstring.hh>

#include <unordered_map>
#include <optional>

typedef std::unordered_map<sstring, sstring> index_options_map;

namespace cql3 {

namespace statements {

class index_prop_defs : public property_definitions {
public:
    static constexpr auto KW_OPTIONS = "options";

    bool is_custom = false;
    std::optional<sstring> custom_class;

    void validate();
    index_options_map get_raw_options();
    index_options_map get_options();
};

}
}

