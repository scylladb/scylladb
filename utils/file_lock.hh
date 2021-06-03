/*
 * Copyright (C) 2014-present ScyllaDB
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

#include <memory>
#include <ostream>
#include <filesystem>
#include <seastar/core/sstring.hh>
#include <seastar/core/future.hh>

#include "seastarx.hh"

namespace fs = std::filesystem;

namespace utils {
    class file_lock {
    public:
        file_lock() = delete;
        file_lock(const file_lock&) = delete;
        file_lock(file_lock&&) noexcept;
        ~file_lock();

        file_lock& operator=(file_lock&&) = default;

        static future<file_lock> acquire(fs::path);

        fs::path path() const;
        sstring to_string() const {
            return path().native();
        }
    private:
        class impl;
        file_lock(fs::path);
        std::unique_ptr<impl> _impl;
    };

    std::ostream& operator<<(std::ostream& out, const file_lock& f);
}

