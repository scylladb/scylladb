/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <cstdint>

namespace sstables {

template<bool check_digest>
struct digest_members {
    uint32_t expected_digest;
    uint32_t actual_digest;
};

template<>
struct digest_members<false> {};

}