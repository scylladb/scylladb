/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <concepts>
#include <limits>
#include "utils/assert.hh"

template <std::integral Divdend, std::integral Divisor>
inline auto
div_ceil(Divdend dividend, Divisor divisor) {
    SCYLLA_ASSERT(dividend >= 0);
    SCYLLA_ASSERT(divisor > 0);
    Divisor max_remainder = divisor - 1;
    using common_t = std::common_type_t<Divdend, Divisor>;
    // check for overflow in (dividend + divisor - 1)
    SCYLLA_ASSERT(static_cast<common_t>(dividend) <=
                  std::numeric_limits<common_t>::max() - max_remainder);
    return (dividend + max_remainder) / divisor;
}
