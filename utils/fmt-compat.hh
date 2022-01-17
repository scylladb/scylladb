/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <fmt/core.h>

// compatibility between fmt < 8 (that doesn't have fmt::runtime())
// and fmt 8 (that requires it)

#if FMT_VERSION < 8'00'00

namespace fmt {

// fmt 8 requires that non-constant format strings be wrapped with
// fmt::runtime(), supply a nop-op version for older fmt
auto runtime(auto fmt_string) {
    return fmt_string;
}

}

#endif
