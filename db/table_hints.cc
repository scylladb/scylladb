/*
 * Copyright 2025-present ScyllaDB
 */
/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <cstdlib>

#include "exceptions/exceptions.hh"
#include "db/table_hints_extension.hh"

extern logging::logger dblog;

namespace db {

table_hints::table_hints(const map_type& map) {
    for (auto& [key, value_str] : map) {
        switch (table_hints::from_string(key)) {
        case table_hint_type::min_tablet_count:
            if (size_t value = std::atol(value_str.c_str())) {
                min_tablet_count.emplace(value);
            }
            break;
        case table_hint_type::min_per_shard_tablet_count:
            if (size_t value = std::atol(value_str.c_str())) {
                min_per_shard_tablet_count.emplace(value);
            }
            break;
        case table_hint_type::expected_data_size_in_gb:
            if (size_t value = std::atol(value_str.c_str())) {
                expected_data_size_in_gb.emplace(value);
            }
            break;
        case table_hint_type::hot_table:
            if (value_str == "true") {
                hot_table.emplace(true);
            } else if (value_str == "false") {
                // leave unset
            } else {
                throw exceptions::syntax_exception(fmt::format("Invalid boolean value '{}'", value_str));
            }
            break;
        }
    }
}

sstring table_hints::to_string(table_hint_type hint) {
    switch (hint) {
    case table_hint_type::min_tablet_count: return "min_tablet_count";
    case table_hint_type::min_per_shard_tablet_count: return "min_per_shard_tablet_count";
    case table_hint_type::expected_data_size_in_gb: return "expected_data_size_in_gb";
    case table_hint_type::hot_table: return "hot_table";
    }
}

table_hint_type table_hints::from_string(sstring hint_desc) {
    if (hint_desc == "min_tablet_count") {
        return table_hint_type::min_tablet_count;
    } else if (hint_desc == "min_per_shard_tablet_count") {
        return table_hint_type::min_per_shard_tablet_count;
    } else if (hint_desc == "expected_data_size_in_gb") {
        return table_hint_type::expected_data_size_in_gb;
    } else if (hint_desc == "hot_table") {
        return table_hint_type::hot_table;
    } else {
        throw exceptions::syntax_exception(fmt::format("Unknown table hint '{}'", hint_desc));
    }
}

std::map<sstring, sstring> table_hints::to_map() const {
    std::map<sstring, sstring> res;
    if (min_tablet_count) {
        res[to_string(table_hint_type::min_tablet_count)] = fmt::to_string(*min_tablet_count);
    }
    if (min_per_shard_tablet_count) {
        res[to_string(table_hint_type::min_per_shard_tablet_count)] = fmt::to_string(*min_per_shard_tablet_count);
    }
    if (expected_data_size_in_gb) {
        res[to_string(table_hint_type::expected_data_size_in_gb)] = fmt::to_string(*expected_data_size_in_gb);
    }
    if (hot_table) {
        res[to_string(table_hint_type::hot_table)] = *hot_table ? "true" : "false";
    }
    return res;
}

} // namespace db
