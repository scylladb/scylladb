/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include <cstdint>

#pragma once

namespace cql3 {

namespace statements {

enum class bound : int32_t { START = 0, END };

static inline
int32_t get_idx(bound b) {
    return (int32_t)b;
}

static inline
bound reverse(bound b) {
    return bound((int32_t)b ^ 1);
}

static inline
bool is_start(bound b) {
    return b == bound::START;
}

static inline
bool is_end(bound b) {
    return b == bound::END;
}

}

}
