/*
 * Copyright (C) 2015 ScyllaDB
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
    void remove() {
        if (!_path.empty()) {
            fs::remove_all(_path);
        }
    }

public:
    tmpdir() : _path(tmpdir::temp_directory_path()) {
        fs::create_directories(_path);
    }

    tmpdir(tmpdir&& other) noexcept : _path(std::exchange(other._path, {})) { }
    tmpdir(const tmpdir&) = delete;
    void operator=(tmpdir&& other) noexcept {
        remove();
        _path = std::exchange(other._path, {});
    }
    void operator=(const tmpdir&) = delete;

    ~tmpdir() {
        remove();
    }

    const fs::path& path() const & noexcept { return _path; }
    // tmpdir().path() must not be allowed, use tmpdir::temp_directory_path() for this.
    const fs::path& path() const && = delete;

    static const fs::path temp_directory_path() {
      return fs::temp_directory_path() /
             fs::path(fmt::format(FMT_STRING("scylla-{}"), utils::make_random_uuid()));
    }
};
