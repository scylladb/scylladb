/*
 * Modified by ScyllaDB
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "schema/schema_fwd.hh"
#include <vector>

namespace db {

namespace system_auth_keyspace {
    enum class version_t: int64_t {
        v1 = 1,
        v2 = 2,
    };
    static constexpr auto NAME = "system_auth_v2";
    // tables
    static constexpr auto ROLES = "roles";

    std::vector<schema_ptr> all_tables();
}; // namespace system_auth_keyspace

} // namespace db
