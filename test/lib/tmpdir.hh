/*
 * Copyright (C) 2015-present ScyllaDB
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

#include <fmt/format.h>

#include <seastar/util/std-compat.hh>

#include "utils/UUID.hh"

namespace fs = std::filesystem;

// Creates a new empty directory with arbitrary name, which will be removed
// automatically when tmpdir object goes out of scope.
class tmpdir {
    fs::path _path;

private:
    void remove() noexcept;

public:
    tmpdir();

    tmpdir(tmpdir&& other) noexcept;
    tmpdir(const tmpdir&) = delete;
    void operator=(tmpdir&& other) noexcept;
    void operator=(const tmpdir&) = delete;

    ~tmpdir();

    const fs::path& path() const noexcept { return _path; }
};
