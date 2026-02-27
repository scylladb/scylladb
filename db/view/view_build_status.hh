/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <string_view>
#include <seastar/core/sstring.hh>

namespace db {

namespace view {

enum class build_status {
    STARTED,
    SUCCESS,
};

build_status build_status_from_string(std::string_view str);
seastar::sstring build_status_to_sstring(build_status status);

}

}
