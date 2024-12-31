/*
 * Copyright 2025-present ScyllaDB
 */
/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <cstdlib>

#include "exceptions/exceptions.hh"
#include "db/tablet_options.hh"
#include "utils/log.hh"

extern logging::logger dblog;

namespace db {

tablet_options::tablet_options(const map_type& map) {
    for (auto& [key, value_str] : map) {
        switch (tablet_options::from_string(key)) {
        case tablet_option_type::min_tablet_count:
            if (auto value = std::atol(value_str.c_str())) {
                min_tablet_count.emplace(value);
            }
            break;
        case tablet_option_type::min_per_shard_tablet_count:
            if (auto value = std::atof(value_str.c_str())) {
                min_per_shard_tablet_count.emplace(value);
            }
            break;
        case tablet_option_type::expected_data_size_in_gb:
            if (auto value = std::atol(value_str.c_str())) {
                expected_data_size_in_gb.emplace(value);
            }
            break;
        }
    }
}

sstring tablet_options::to_string(tablet_option_type hint) {
    switch (hint) {
    case tablet_option_type::min_tablet_count: return "min_tablet_count";
    case tablet_option_type::min_per_shard_tablet_count: return "min_per_shard_tablet_count";
    case tablet_option_type::expected_data_size_in_gb: return "expected_data_size_in_gb";
    }
}

tablet_option_type tablet_options::from_string(sstring hint_desc) {
    if (hint_desc == "min_tablet_count") {
        return tablet_option_type::min_tablet_count;
    } else if (hint_desc == "min_per_shard_tablet_count") {
        return tablet_option_type::min_per_shard_tablet_count;
    } else if (hint_desc == "expected_data_size_in_gb") {
        return tablet_option_type::expected_data_size_in_gb;
    } else {
        throw exceptions::syntax_exception(fmt::format("Unknown tablet hint '{}'", hint_desc));
    }
}

std::map<sstring, sstring> tablet_options::to_map() const {
    std::map<sstring, sstring> res;
    if (min_tablet_count) {
        res[to_string(tablet_option_type::min_tablet_count)] = fmt::to_string(*min_tablet_count);
    }
    if (min_per_shard_tablet_count) {
        res[to_string(tablet_option_type::min_per_shard_tablet_count)] = fmt::to_string(*min_per_shard_tablet_count);
    }
    if (expected_data_size_in_gb) {
        res[to_string(tablet_option_type::expected_data_size_in_gb)] = fmt::to_string(*expected_data_size_in_gb);
    }
    return res;
}

void tablet_options::validate(const map_type& map) {
    for (auto& [key, value_str] : map) {
        switch (tablet_options::from_string(key)) {
        case tablet_option_type::min_tablet_count:
            if (auto value = std::atol(value_str.c_str()); value < 0) {
                throw exceptions::configuration_exception(format("Invalid value '{}' for min_tablet_count", value));
            }
            break;
        case tablet_option_type::min_per_shard_tablet_count:
            if (auto value = std::atof(value_str.c_str()); value < 0) {
                throw exceptions::configuration_exception(format("Invalid value '{}' for min_per_shard_tablet_count", value));
            }
            break;
        case tablet_option_type::expected_data_size_in_gb:
            if (auto value = std::atol(value_str.c_str()); value < 0) {
                throw exceptions::configuration_exception(format("Invalid value '{}' for expected_data_size_in_gb", value));
            }
            break;
        }
    }
}

} // namespace db
