/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <cstdint>
#include <iosfwd>

namespace db {

enum class operation_type : uint8_t {
    read = 0,
    write = 1
};

std::ostream& operator<<(std::ostream& os, operation_type op_type);

}
