/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <cstdint>
#include <seastar/core/sstring.hh>

namespace data_dictionary {
enum class consistency_config_option : uint8_t {
    eventual,
    local,
    global
};

consistency_config_option consistency_config_option_from_string(const seastar::sstring& str);
seastar::sstring consistency_config_option_to_string(consistency_config_option option);
}
