/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <cstdint>
#include <ostream>

namespace streaming {

enum class stream_reason : uint8_t {
    unspecified,
    bootstrap,
    decommission,
    removenode,
    rebuild,
    repair,
    replace,
};

std::ostream& operator<<(std::ostream& out, stream_reason r);

}
