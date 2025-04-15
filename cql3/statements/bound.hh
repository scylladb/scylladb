/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#include <cstdint>

#pragma once

namespace cql3 {

namespace statements {

enum class bound : int32_t { START = 0, END };

inline
int32_t get_idx(bound b) {
    return (int32_t)b;
}

inline
bound reverse(bound b) {
    return bound((int32_t)b ^ 1);
}

inline
bool is_start(bound b) {
    return b == bound::START;
}

inline
bool is_end(bound b) {
    return b == bound::END;
}

}

}
