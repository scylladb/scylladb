/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <assert.h>
#include <cstdint>
#include <iosfwd>
#include <fmt/ostream.h>

namespace db {

enum class write_type : uint8_t {
    SIMPLE,
    BATCH,
    UNLOGGED_BATCH,
    COUNTER,
    BATCH_LOG,
    CAS,
    VIEW,
};

std::ostream& operator<<(std::ostream& os, const write_type& t);

}

template <> struct fmt::formatter<db::write_type> : fmt::ostream_formatter {};
