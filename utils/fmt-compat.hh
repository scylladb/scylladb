/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
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
