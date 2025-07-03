/*
 * Copyright (C) 2025-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "bytes.hh"

namespace resources {

struct resource {
    const char* name;
    const char* content_type;
    bytes_view content;
};

} // namespace resources
