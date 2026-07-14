/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <seastar/core/sstring.hh>

#include <map>
#include <vector>
#include <variant>

#include "seastarx.hh"

// Shape shared by cql3::statements::property_definitions and
// locator::replication_strategy_config_option: neither module may depend
// on the other's headers, so the common type is defined here once instead
// of being duplicated in both places.
namespace utils {

using property_string_list = std::vector<sstring>;
using property_string_map = std::map<sstring, sstring>;
using property_value = std::variant<sstring, property_string_list>;
using extended_property_map = std::map<sstring, property_value>;

}
