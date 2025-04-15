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

// Per-table tablet options
enum class tablet_option_type {
    min_tablet_count,
    min_per_shard_tablet_count,
    expected_data_size_in_gb,
};

struct tablet_options {
    using map_type = std::map<sstring, sstring>;

    std::optional<ssize_t> min_tablet_count;
    std::optional<double> min_per_shard_tablet_count;
    std::optional<ssize_t> expected_data_size_in_gb;

    tablet_options() = default;
    explicit tablet_options(const map_type& map);

    operator bool() const noexcept {
        return min_tablet_count || min_per_shard_tablet_count || expected_data_size_in_gb;
    }

    map_type to_map() const;

    static sstring to_string(tablet_option_type hint);
    static tablet_option_type from_string(sstring hint_desc);
    static void validate(const map_type& map);
};

} // namespace db
