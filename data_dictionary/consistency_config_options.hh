/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <cstdint>
#include <optional>
#include <seastar/core/sstring.hh>

namespace data_dictionary {
enum class consistency_config_option : uint8_t {
    eventual,
    local,
    global
};

consistency_config_option consistency_config_option_from_string(const seastar::sstring& str);
seastar::sstring consistency_config_option_to_string(consistency_config_option option);

inline bool is_strongly_consistent(consistency_config_option option) {
    return option != consistency_config_option::eventual;
}

// A keyspace without the consistency option is eventually consistent.
inline bool is_strongly_consistent(std::optional<consistency_config_option> option) {
    return option && is_strongly_consistent(*option);
}

}
