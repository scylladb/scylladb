/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <cstdint>

namespace service {

// This the default target size of tablets.
static constexpr uint64_t default_target_tablet_size = 5UL * 1024 * 1024 * 1024;

class tablet_allocator_impl;

class tablet_allocator;

}
