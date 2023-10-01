/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <iosfwd>
#include <fmt/ostream.h>

namespace db {

/// CQL consistency levels.
///
/// Values are guaranteed to be dense and in the tight range [MIN_VALUE, MAX_VALUE].
enum class consistency_level {
    ANY, MIN_VALUE = ANY,
    ONE,
    TWO,
    THREE,
    QUORUM,
    ALL,
    LOCAL_QUORUM,
    EACH_QUORUM,
    SERIAL,
    LOCAL_SERIAL,
    LOCAL_ONE, MAX_VALUE = LOCAL_ONE
};

std::ostream& operator<<(std::ostream& os, consistency_level cl);

}

template <> struct fmt::formatter<db::consistency_level> : fmt::ostream_formatter {};
