/*
 * Copyright 2025-present ScyllaDB
 */
/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <map>

#include <seastar/core/sstring.hh>

using namespace seastar;

namespace gms { class feature_service; }

namespace db {

// Per-table tablet options
enum class tablet_option_type {
    min_tablet_count,
    max_tablet_count,
    min_per_shard_tablet_count,
    expected_data_size_in_gb,
    pow2_count,
};

struct tablet_options {
    // System-wide default for pow2_count if the option is not set.
    static const bool default_pow2_count = true;

    using map_type = std::map<sstring, sstring>;

    std::optional<ssize_t> min_tablet_count;
    std::optional<ssize_t> max_tablet_count;
    std::optional<double> min_per_shard_tablet_count;
    std::optional<ssize_t> expected_data_size_in_gb;
    std::optional<bool> pow2_count;

    tablet_options() = default;
    explicit tablet_options(const map_type& map);

    operator bool() const noexcept {
        return min_tablet_count || max_tablet_count || min_per_shard_tablet_count || expected_data_size_in_gb || pow2_count;
    }

    map_type to_map() const;

    static sstring to_string(tablet_option_type hint);
    static tablet_option_type from_string(sstring hint_desc);
    static void validate(const map_type& map, const gms::feature_service& features);
};

} // namespace db
