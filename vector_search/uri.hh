/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once
#include <seastar/core/sstring.hh>
#include <cstdint>

namespace vector_search {

struct uri {
    enum class schema_type { http, https };

    schema_type schema;
    seastar::sstring host;
    std::uint16_t port;
};

} // namespace vector_search
