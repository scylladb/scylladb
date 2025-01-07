/*
 * Optimized ASCII string validation.
 *
 * Copyright (c) 2018, Arm Limited.
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <cstdint>
#include "bytes_fwd.hh"

namespace utils {

namespace ascii {

bool validate(const uint8_t *data, size_t len);

inline bool validate(bytes_view string) {
    const uint8_t *data = reinterpret_cast<const uint8_t*>(string.data());
    size_t len = string.size();

    return validate(data, len);
}

} // namespace ascii

} // namespace utils
