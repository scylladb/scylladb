/*
 * Copyright (C) 2021-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <unordered_map>

#include <seastar/core/sstring.hh>

#include "seastarx.hh"
#include "utils/enum_option.hh"

namespace db {

/// A restriction that can be in three modes: true (the operation is
/// restricted), false (the operation is unrestricted), or warn (the
/// operation is unrestricted but produces some warning (in the response,
/// log and/or metric) when the operation would have been forbidden in
/// "true" mode.
struct tri_mode_restriction_t {
    enum class mode { FALSE, TRUE, WARN };
    static std::unordered_map<sstring, mode> map(); // for enum_option<>
};
using tri_mode_restriction = enum_option<tri_mode_restriction_t>;

} // namespace db