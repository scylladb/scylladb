/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <cstddef>

namespace cql3 {

// Memory limits for query_processor's prepared statement caches. Kept apart
// from query_processor.hh so that configuration code can name it without
// pulling in the whole query processor.
struct query_processor_memory_config {
    size_t prepared_statment_cache_size = 0;
    size_t authorized_prepared_cache_size = 0;
};

}
