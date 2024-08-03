/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <concepts>

inline auto
div_ceil(std::integral auto dividend, std::integral auto divisor) {
    return (dividend + divisor - 1) / divisor;
}
