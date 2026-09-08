/*
 * Copyright (C) 2026-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <unordered_map>
#include <seastar/core/sstring.hh>
#include "seastarx.hh"
#include "db/consistency_level_type.hh"

namespace db {

struct consistency_level_restriction_t {
    static std::unordered_map<sstring, db::consistency_level> map(); // for enum_option<>
};

} // namespace db
