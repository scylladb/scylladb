/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/thread.hh>
#include <seastar/util/bool_class.hh>

namespace utils {

class can_yield_tag;
using can_yield = seastar::bool_class<can_yield_tag>;

inline void maybe_yield(can_yield can_yield) {
    if (can_yield) {
        seastar::thread::maybe_yield();
    }
}

} // namespace utils
