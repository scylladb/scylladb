/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <concepts>

inline auto
div_ceil(std::integral auto dividend, std::integral auto divisor) {
    return (dividend + divisor - 1) / divisor;
}
