/*
 * Copyright (C) 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <cstdint>

namespace query {

enum class digest_algorithm : uint8_t {
    none = 0,  // digest not required
    xxHash = 3, // default algorithm
};

}
