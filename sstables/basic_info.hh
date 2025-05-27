/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/core/sstring.hh>
#include "sstables/generation_type.hh"

namespace sstables {

struct basic_info {
    generation_type generation;
    sstring origin;
    int64_t size;
};

}
