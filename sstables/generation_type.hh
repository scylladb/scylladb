/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <cstdint>

namespace sstables {
using generation_type = int64_t;
constexpr int64_t generation_value(generation_type generation) { return generation; }
constexpr generation_type generation_from_value(int64_t value) { return value; }
}
