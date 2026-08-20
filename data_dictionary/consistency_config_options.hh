/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <cstdint>
#include <map>
#include <seastar/core/sstring.hh>

namespace data_dictionary {
enum class consistency_config_option : uint8_t {
    eventual,
    local,
    global
};

consistency_config_option consistency_config_option_from_string(const seastar::sstring& str);
seastar::sstring consistency_config_option_to_string(consistency_config_option option);

struct consistency_config {
    consistency_config_option type = consistency_config_option::eventual;
    std::map<seastar::sstring, seastar::sstring> dedicated_rack;

    bool has_dedicated_rack() const {
        return !dedicated_rack.empty();
    }

    bool operator==(const consistency_config&) const = default;
};

}
