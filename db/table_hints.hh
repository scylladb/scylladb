/*
 * Copyright 2025-present ScyllaDB
 */
/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <map>

#include <seastar/core/sstring.hh>

using namespace seastar;

namespace db {

// Per-table schema hints
enum class table_hint_type {
    min_tablet_count,
    min_per_shard_tablet_count,
    expected_data_size_in_gb,
    hot_table,
};

struct table_hints {
    using map_type = std::map<sstring, sstring>;

    std::optional<ssize_t> min_tablet_count;
    std::optional<ssize_t> min_per_shard_tablet_count;
    std::optional<ssize_t> expected_data_size_in_gb;
    std::optional<bool> hot_table;

    table_hints() = default;
    explicit table_hints(const map_type& map);

    operator bool() const noexcept {
        return min_tablet_count || min_per_shard_tablet_count || expected_data_size_in_gb || hot_table;
    }

    map_type to_map() const;

    static sstring to_string(table_hint_type hint);
    static table_hint_type from_string(sstring hint_desc);
};

} // namespace db
