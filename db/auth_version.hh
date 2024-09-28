// Copyright (C) 2024-present ScyllaDB
// SPDX-License-Identifier: AGPL-3.0-or-later

#pragma once

#include <cstdint>

namespace db {

enum class auth_version_t: int64_t {
    v1 = 1,
    v2 = 2,
};

}
