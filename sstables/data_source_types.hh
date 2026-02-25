/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <cstdint>

namespace sstables {

template<bool check_digest>
struct digest_members {
    bool can_calculate_digest;
    uint32_t expected_digest;
    uint32_t actual_digest;
};

template<>
struct digest_members<false> {};

}